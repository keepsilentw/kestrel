//! Bus hub process (`kestrel hub`): binds the UDS socket and routes frames
//! between peers. The hub is deliberately dumb (design §2): Hello registration,
//! Notify/Request routing, sender-identity enforcement — no auth, no reconnect
//! replay (write-ahead in the journal is the backstop).

use std::path::PathBuf;

use crate::bus::rpc::BusHub;

/// Bind the socket and serve until Ctrl-C.
pub async fn run_hub(socket: PathBuf) -> anyhow::Result<()> {
    let hub = BusHub::bind(&socket)?;
    eprintln!("[hub] listening on {}", hub.path().display());
    tokio::signal::ctrl_c().await?;
    eprintln!("[hub] shutting down");
    Ok(())
}
