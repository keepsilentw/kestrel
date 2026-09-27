//! Bus layer: 哑 RPC 总线 + sender identity + write-ahead 配套 (M4, plan §2.8).

pub mod frame;
pub mod identity;
pub mod rpc;
