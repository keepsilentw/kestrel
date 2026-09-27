//! Static discipline gates (plan §5 质量闸门):
//! - every `tokio::spawn` in engine code is wrapped in AbortOnDropHandle,
//!   except lines explicitly marked as fire-and-forget.

use std::fs;

const SCAN_DIRS: &[&str] = &["src/core", "src/tools", "src/provider"];

#[test]
fn spawns_are_wrapped_or_marked_fire_and_forget() {
    let mut checked = 0;
    for dir in SCAN_DIRS {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
        for entry in std::fs::read_dir(&root).expect("dir readable") {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let text = fs::read_to_string(&path).unwrap();
            for (i, line) in text.lines().enumerate() {
                if line.contains("tokio::spawn(") {
                    checked += 1;
                    let wrapped = line.contains("AbortOnDropHandle");
                    // marked fire-and-forget / managed: marker on this line, the
                    // one above, or the one below (comment may sit next to the spawn)
                    let lines: Vec<&str> = text.lines().collect();
                    let marked = [i.saturating_sub(1), i, (i + 1).min(lines.len() - 1)]
                        .iter()
                        .any(|&j| {
                            let l = lines[j].to_lowercase();
                            l.contains("fire-and-forget") || l.contains("managed:")
                        });
                    assert!(
                        wrapped || marked,
                        "{path:?}:{} bare tokio::spawn without AbortOnDropHandle or \
                         fire-and-forget/managed marker",
                        i + 1
                    );
                }
            }
        }
    }
    assert!(
        checked >= 3,
        "gate scanned too few spawns ({checked}); did sources move?"
    );
}
