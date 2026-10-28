//! Kubernetes CSI driver and 9P2000.L file server on top of jetstream.
//!
//! The server accepts 9P over QUIC (mTLS) and iroh. The kernel 9p client only
//! speaks TCP or unix sockets, so the node plugin runs a local proxy per mount
//! that carries the 9P byte stream over QUIC/iroh.

pub mod csi;
pub mod server;
pub mod tls;
pub mod transport;

/// ALPN for 9P streams, both on QUIC and on iroh (`Protocol::NAME` of jetstream_ufs).
pub const ALPN_9P: &str = "9p";
pub const DEFAULT_QUIC_PORT: u16 = 5641;
pub const DEFAULT_IROH_PORT: u16 = 5642;
