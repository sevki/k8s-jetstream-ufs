//! Client side: dial a 9P server over QUIC or iroh and proxy kernel connections to it.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use jetstream_iroh::iroh;
use tokio::{
    io::{copy_bidirectional, AsyncRead, AsyncWrite},
    net::UnixListener,
    sync::{Mutex, OnceCell},
};

use crate::ALPN_9P;

/// Where a volume's 9P server lives.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    /// QUIC server; `host` is also the TLS server name.
    Quic { host: String, port: u16 },
    /// iroh node id, plus optional `host:port` direct addresses (otherwise iroh discovery is used).
    Iroh { node: String, addrs: Vec<String> },
}

#[derive(Debug, Clone)]
pub struct ClientTls {
    pub ca: PathBuf,
    /// Client certificate and key, for servers that require mTLS.
    pub identity: Option<(PathBuf, PathBuf)>,
}

pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

enum Conn {
    Quic(quinn::Connection),
    Iroh(iroh::endpoint::Connection),
}

pub struct Dialer {
    tls: Option<ClientTls>,
    quic: OnceCell<jetstream_quic::Client>,
    iroh: OnceCell<iroh::Endpoint>,
    conns: Mutex<HashMap<Target, Conn>>,
}

async fn resolve(hostport: &str) -> Result<Vec<SocketAddr>> {
    let addrs: Vec<_> = tokio::net::lookup_host(hostport)
        .await
        .with_context(|| format!("resolve {hostport}"))?
        .collect();
    anyhow::ensure!(!addrs.is_empty(), "{hostport} has no address");
    Ok(addrs)
}

impl Dialer {
    pub fn new(tls: Option<ClientTls>) -> Self {
        crate::tls::install_crypto_provider();
        Self {
            tls,
            quic: OnceCell::new(),
            iroh: OnceCell::new(),
            conns: Mutex::new(HashMap::new()),
        }
    }

    async fn quic_client(&self) -> Result<&jetstream_quic::Client> {
        self.quic
            .get_or_try_init(|| async {
                let tls = self
                    .tls
                    .as_ref()
                    .context("QUIC needs --tls-ca (and --tls-cert/--tls-key for mTLS)")?;
                let ca = crate::tls::load_certs(&tls.ca)?.remove(0);
                let alpn = vec![ALPN_9P.as_bytes().to_vec()];
                let bind: SocketAddr = "0.0.0.0:0".parse()?;
                match &tls.identity {
                    Some((cert, key)) => jetstream_quic::Client::new_with_mtls(
                        ca,
                        crate::tls::load_certs(cert)?.remove(0),
                        crate::tls::load_key(key)?,
                        alpn,
                        bind,
                    ),
                    None => jetstream_quic::Client::new(ca, alpn, bind),
                }
                .map_err(|e| anyhow!("quic client: {e}"))
            })
            .await
    }

    async fn iroh_endpoint(&self) -> Result<&iroh::Endpoint> {
        self.iroh
            .get_or_try_init(|| async {
                Ok(jetstream_iroh::endpoint_builder::<jetstream_ufs::Server>()
                    .bind()
                    .await?)
            })
            .await
    }

    async fn connect(&self, target: &Target) -> Result<Conn> {
        match target {
            Target::Quic { host, port } => {
                let addrs = resolve(&format!("{host}:{port}")).await?;
                let addr = addrs.iter().find(|a| a.is_ipv4()).unwrap_or(&addrs[0]);
                let conn = self
                    .quic_client()
                    .await?
                    .connect(*addr, host)
                    .await
                    .map_err(|e| anyhow!("quic connect {host}:{port}: {e}"))?;
                Ok(Conn::Quic(conn))
            }
            Target::Iroh { node, addrs } => {
                let id: iroh::NodeId = node
                    .parse()
                    .map_err(|e| anyhow!("iroh node id {node}: {e}"))?;
                let mut direct = Vec::new();
                for a in addrs {
                    direct.extend(resolve(a).await?);
                }
                let addr = iroh::NodeAddr::new(id).with_direct_addresses(direct);
                let conn = self
                    .iroh_endpoint()
                    .await?
                    .connect(addr, ALPN_9P.as_bytes())
                    .await
                    .map_err(|e| anyhow!("iroh connect {node}: {e}"))?;
                Ok(Conn::Iroh(conn))
            }
        }
    }

    async fn try_open(conn: &Conn) -> Result<Box<dyn Io>> {
        Ok(match conn {
            Conn::Quic(c) => {
                let (send, recv) = c.open_bi().await?;
                Box::new(tokio::io::join(recv, send))
            }
            Conn::Iroh(c) => {
                let (send, recv) = c.open_bi().await?;
                Box::new(tokio::io::join(recv, send))
            }
        })
    }

    /// Opens a new 9P stream to `target`, (re)connecting if the cached connection is dead.
    pub async fn open(&self, target: &Target) -> Result<Box<dyn Io>> {
        let mut conns = self.conns.lock().await;
        if let Some(conn) = conns.get(target) {
            if let Ok(io) = Self::try_open(conn).await {
                return Ok(io);
            }
            conns.remove(target);
        }
        let conn = self.connect(target).await?;
        let io = Self::try_open(&conn).await?;
        conns.insert(target.clone(), conn);
        Ok(io)
    }
}

/// Accepts connections from the kernel 9p client on `listener` and forwards each to a new stream.
pub async fn proxy(listener: UnixListener, dialer: Arc<Dialer>, target: Target) {
    while let Ok((mut local, _)) = listener.accept().await {
        let dialer = dialer.clone();
        let target = target.clone();
        tokio::spawn(async move {
            match dialer.open(&target).await {
                Ok(mut remote) => {
                    let _ = copy_bidirectional(&mut local, &mut remote).await;
                }
                Err(e) => tracing::error!("dial {target:?}: {e:#}"),
            }
        });
    }
}
