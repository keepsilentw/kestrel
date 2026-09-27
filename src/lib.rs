//! kestrel — minimal Rust harness kernel for a star-topology multi-agent
//! coding harness. See docs/ for design docs; the research corpus lives in
//! the CC repo's docs/design/rust-agent/.

pub mod bus;
pub mod cli;
pub mod config;
pub mod core;
pub mod journal;
pub mod permissions;
pub mod provider;
pub mod sandbox;
pub mod session;
pub mod tools;
