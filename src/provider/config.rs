//! Provider registry (~/.kestrel/providers.toml): the single layer allowed to
//! define providers/keys (codex 纪律 1 — project 层禁入由 `assert_no_provider_definitions`
//! 钉住). Key values are three-form: literal / `$ENV` / `!command`
//! (pi resolve-config-value 语义; `!cmd` 结果进程内 memoize).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use anyhow::Context;
use serde::Deserialize;

use super::wire::{CompatFlags, CostPerMTok, ModelMeta, WireId};

#[derive(Debug, Deserialize)]
pub struct Registry {
    pub providers: BTreeMap<String, ProviderDef>,
}

#[derive(Debug, Deserialize)]
pub struct ProviderDef {
    #[serde(default)]
    pub name: Option<String>,
    pub base_url: String,
    pub api: WireId,
    pub key: String,
    /// The currency the `cost` tables of this provider are written in. Declared
    /// per provider because that is the unit the **vendor** publishes: a Chinese
    /// price list stays 元, a USD one stays $, and no FX arithmetic is hidden in
    /// the kernel (design §9.44). Absent => `usd`, the unit the ledger has always
    /// printed. A run whose priced peers disagree is refused, not averaged.
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub models: Vec<ConfigModel>,
}

#[derive(Debug, Deserialize)]
pub struct ConfigModel {
    pub id: String,
    pub context_window: Option<u32>,
    pub max_out: Option<u32>,
    #[serde(default)]
    pub cost: Option<CostPerMTok>,
    #[serde(default)]
    pub input_image: bool,
    #[serde(default)]
    pub reasoning: bool,
    #[serde(default)]
    pub compat: Option<CompatFlags>,
}

impl Registry {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| {
            format!(
                "read provider registry {} — providers/keys live only in the user layer file",
                path.display()
            )
        })?;
        toml::from_str(&text).with_context(|| format!("parse provider registry {}", path.display()))
    }

    /// Resolve (provider, model) -> full ModelMeta. Unknown model ids get
    /// pi-style defaults: 128k context / 16k out / zero cost.
    pub fn model_meta(&self, provider_id: &str, model_id: &str) -> anyhow::Result<ModelMeta> {
        let def = self
            .providers
            .get(provider_id)
            .ok_or_else(|| anyhow::anyhow!("unknown provider: {provider_id}"))?;
        let configured = def.models.iter().find(|m| m.id == model_id);
        let meta = ModelMeta {
            id: model_id.to_string(),
            wire: def.api,
            base_url: def.base_url.clone(),
            context_window: configured.and_then(|m| m.context_window).unwrap_or(128_000),
            max_out: configured.and_then(|m| m.max_out).unwrap_or(16_384),
            input_image: configured.map(|m| m.input_image).unwrap_or(false),
            reasoning: configured.map(|m| m.reasoning).unwrap_or(false),
            // Absent rate stays `None`: "nobody declared a price" is not the
            // same claim as "this model is free", and the budget ledger bills
            // differently for the two.
            cost: configured.and_then(|m| m.cost),
            compat: configured
                .and_then(|m| m.compat.clone())
                .unwrap_or_default(),
        };
        Ok(meta)
    }

    /// The declared rate for (provider, model), if the user layer has one.
    /// Unknown provider / undeclared model / an all-zero table all come back as
    /// `None` — the ledger then books a nominal charge instead of `0.00`, so a
    /// missing price can never silently disarm the budget stop-loss.
    pub fn model_cost(&self, provider_id: &str, model_id: &str) -> Option<CostPerMTok> {
        let cost = self
            .providers
            .get(provider_id)?
            .models
            .iter()
            .find(|m| m.id == model_id)
            .and_then(|m| m.cost)?;
        cost.is_declared().then_some(cost)
    }

    /// The currency a provider's rates are written in (`None` = not declared,
    /// which callers read as `usd`). Compared case-insensitively by the ledger.
    pub fn provider_currency(&self, provider_id: &str) -> Option<&str> {
        self.providers.get(provider_id)?.currency.as_deref()
    }

    pub fn headers(&self, provider_id: &str) -> Vec<(String, String)> {
        self.providers
            .get(provider_id)
            .map(|d| {
                d.headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn api(&self, provider_id: &str) -> Option<WireId> {
        self.providers.get(provider_id).map(|d| d.api)
    }
}

/// Resolved key cache: `!cmd` results memoized per command string.
#[derive(Default)]
pub struct KeyResolver {
    memo: Mutex<HashMap<String, String>>,
}

impl KeyResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// `value` | `$VAR` | `$VAR:-default` | `!command` (pi 三态; cmd memoized).
    pub fn resolve(&self, spec: &str) -> anyhow::Result<String> {
        if let Some(cmd) = spec.strip_prefix('!') {
            if let Some(hit) = self.memo.lock().unwrap().get(cmd) {
                return Ok(hit.clone());
            }
            let out = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(cmd)
                .output()
                .map_err(|e| anyhow::anyhow!("key command failed: {e}"))?;
            if !out.status.success() {
                anyhow::bail!("key command exited {}: {}", out.status, cmd);
            }
            let key = String::from_utf8_lossy(&out.stdout).trim().to_string();
            self.memo
                .lock()
                .unwrap()
                .insert(cmd.to_string(), key.clone());
            Ok(key)
        } else if let Some(rest) = spec.strip_prefix('$') {
            let (name, default) = match rest.split_once(":-") {
                Some((n, d)) => (n, Some(d)),
                None => (rest, None),
            };
            match (std::env::var(name).ok(), default) {
                (Some(v), _) => Ok(v),
                (None, Some(d)) => Ok(d.to_string()),
                (None, None) => anyhow::bail!("env var {name} is not set"),
            }
        } else {
            Ok(spec.to_string())
        }
    }
}

/// codex 纪律 1: project-layer config must not define providers.
pub fn assert_no_provider_definitions(content: &str) -> anyhow::Result<()> {
    let v: toml::Value = toml::from_str(content)?;
    if v.get("providers").is_some() {
        anyhow::bail!(
            "project config defines [providers]: provider/key definitions are \
             only allowed in ~/.kestrel/providers.toml (user layer)"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry_toml() -> &'static str {
        r#"
[providers.deepseek-anthropic]
base_url = "https://api.deepseek.com/anthropic"
api = "anthropic-messages"
key = "$DEEPSEEK_KEY"

[[providers.deepseek-anthropic.models]]
id = "deepseek-v4-flash"
context_window = 128000
max_out = 8192

[providers.zhipu-anthropic]
base_url = "https://open.bigmodel.cn/api/anthropic"
api = "anthropic-messages"
key = "!echo cmd-key-123"

[[providers.zhipu-anthropic.models]]
id = "glm-5.3-flash"
input_image = true
reasoning = true

[providers.zhipu-anthropic.models.cost]
input = 0.5
output = 1.0

[providers.local-llama]
base_url = "http://127.0.0.1:8080"
api = "openai-completions"
key = "sk-local"

[[providers.local-llama.models]]
id = "llama-x"

[providers.local-llama.models.compat]
max_tokens_field = "max_completion_tokens"
tolerate_unknown_finish = true
stream_include_usage = true

# the third v1 wire: the Responses API is a different `api` value, not a flag
[providers.oai-responses]
base_url = "https://api.openai.com"
api = "openai-responses"
key = "sk-resp"

[[providers.oai-responses.models]]
id = "resp-model"

# a rate that is present but empty: "nobody filled this in", not "free"
[providers.free-loader]
base_url = "https://example.invalid"
api = "openai-completions"
key = "sk-none"

[[providers.free-loader.models]]
id = "free-model"

[providers.free-loader.models.cost]
input = 0.0
output = 0.0
"#
    }

    fn registry_named(tag: &str) -> Registry {
        let dir = std::env::temp_dir().join(format!("kestrel-reg-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("providers.toml");
        std::fs::write(&p, registry_toml()).unwrap();
        Registry::load(&p).unwrap()
    }

    #[test]
    fn load_and_default_model_meta() {
        let r = registry_named("load");
        let m = r
            .model_meta("deepseek-anthropic", "deepseek-v4-flash")
            .unwrap();
        assert_eq!(m.wire, WireId::AnthropicMessages);
        assert_eq!(m.context_window, 128000);
        assert_eq!(m.max_out, 8192);
        // undeclared model gets pi-style defaults: 128k / 16k / cost 0
        let m2 = r
            .model_meta("deepseek-anthropic", "undeclared-model")
            .unwrap();
        assert_eq!(m2.context_window, 128_000);
        assert_eq!(m2.max_out, 16_384);
        // no rate declared => no rate, not a claim of "free"
        assert_eq!(m2.cost, None);
        // cost + compat from config
        let g = r.model_meta("zhipu-anthropic", "glm-5.3-flash").unwrap();
        assert!(g.input_image && g.reasoning);
        assert_eq!(g.cost, Some(CostPerMTok::simple(0.5, 1.0)));
        let l = r.model_meta("local-llama", "llama-x").unwrap();
        assert_eq!(l.wire, WireId::OpenAiCompletions);
        assert_eq!(
            l.compat.max_tokens_field,
            Some(super::super::wire::MaxTokensField::MaxCompletionTokens)
        );
        assert_eq!(l.compat.tolerate_unknown_finish, Some(true));
        assert_eq!(l.compat.stream_include_usage, Some(true));
        // `api = "openai-responses"` resolves to the Responses wire, and the
        // session header string follows it
        let rr = r.model_meta("oai-responses", "resp-model").unwrap();
        assert_eq!(rr.wire, WireId::OpenAiResponses);
        assert_eq!(rr.wire_id_str(), "openai-responses");
        assert_eq!(rr.base_url, "https://api.openai.com");
    }

    /// The ledger asks one question: what does this model cost? A model with no
    /// rate, a provider that does not exist, and a rate filled in as zeros all
    /// answer `None` — the three cases where booking `0.00` would quietly turn
    /// the budget stop-loss into a no-op.
    #[test]
    fn model_cost_only_answers_for_declared_rates() {
        let r = registry_named("cost");
        assert_eq!(
            r.model_cost("zhipu-anthropic", "glm-5.3-flash"),
            Some(CostPerMTok::simple(0.5, 1.0))
        );
        assert_eq!(
            r.model_cost("deepseek-anthropic", "deepseek-v4-flash"),
            None
        );
        assert_eq!(r.model_cost("zhipu-anthropic", "undeclared"), None);
        assert_eq!(r.model_cost("no-such-provider", "glm-5.3-flash"), None);
        // an explicit but empty table is still "no rate"
        assert_eq!(r.model_cost("free-loader", "free-model"), None);
    }

    #[test]
    fn unknown_provider_errors() {
        let r = registry_named("unknown");
        assert!(r.model_meta("nope", "m").is_err());
    }

    #[test]
    fn key_sources_resolve() {
        let resolver = KeyResolver::new();
        // literal
        assert_eq!(resolver.resolve("sk-literal").unwrap(), "sk-literal");
        // env
        // SAFETY: single-threaded test process; no concurrent env readers.
        unsafe { std::env::set_var("KESTREL_TEST_KEY", "env-key-1") };
        assert_eq!(resolver.resolve("$KESTREL_TEST_KEY").unwrap(), "env-key-1");
        assert_eq!(
            resolver
                .resolve("$KESTREL_TEST_KEY_MISSING:-fallback")
                .unwrap(),
            "fallback"
        );
        // cmd + memoize (single execution)
        // The counter lives under `env::temp_dir()` (which the sandbox may mount
        // read-only for `/tmp`, and which `TMPDIR` redirects) and carries the
        // pid, so concurrent runners cannot step on each other's file.
        let count_file =
            std::env::temp_dir().join(format!("kestrel-cmd-count-{}-resolve", std::process::id()));
        let counting = format!("!printf x >>{}; printf cmd-key", count_file.display());
        std::fs::write(&count_file, "0").unwrap();
        let r1 = resolver.resolve(&counting).unwrap();
        let r2 = resolver.resolve(&counting).unwrap();
        assert_eq!(r1, "cmd-key");
        assert_eq!(r2, "cmd-key");
        let counter = std::fs::read_to_string(&count_file).unwrap();
        assert_eq!(counter, "0x", "cmd executed exactly once (memoized)");
        std::fs::remove_file(&count_file).ok();
    }

    #[test]
    fn project_layer_providers_banned() {
        assert!(assert_no_provider_definitions("[providers.x]\nbase_url = \"x\"\n").is_err());
        assert!(assert_no_provider_definitions("[llm]\nprovider = \"x\"\n").is_ok());
    }

    /// A missing/misplaced registry file must say which file it tried to read
    /// and where providers are actually allowed (smoke 2026-10-26 finding H).
    #[test]
    fn registry_error_names_the_file() {
        let err = Registry::load(std::path::Path::new("/tmp/kestrel-missing-providers.toml"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("read provider registry") && err.contains("user layer"),
            "{err}"
        );
    }
}
