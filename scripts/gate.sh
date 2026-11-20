#!/usr/bin/env bash
# The repo's one-command gate: fmt + clippy (-D warnings) + full tests.
#
# This repository has no CI by design (design §8, §9.19⑥): every change is
# supposed to pass this gate before it counts as done. Forgetting to run it is
# how clippy 1.98 broke the build once, so the three commands live here as one.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

step() { printf '\n== %s ==\n' "$*"; }

step "cargo fmt --check"
cargo fmt --check

step "cargo clippy --all-targets -- -D warnings"
cargo clippy --all-targets -- -D warnings

step "cargo test"
cargo test

printf '\ngate: clean\n'
