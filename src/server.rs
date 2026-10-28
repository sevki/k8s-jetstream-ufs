//! 9P2000.L file server: exports a directory over QUIC, iroh and (optionally) a unix socket.
//!
//! jetstream's `run` loop only accepts unix-socket transports, so every
//! incoming byte stream is bridged onto an in-process unix socket pair.

use std::{
    collections::BTreeMap,
    io,
    path::{Component, Path, PathBuf},
};

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use jetstream_rpc::server::{run, ServerCodec};
use tokio::{
    io::{copy_bidirectional, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
use tokio_util::codec::Framed;

fn serve_conn(root: PathBuf, stream: UnixStream) {
    tokio::spawn(async move {
        let mut service = match jetstream_ufs::Server::new(root, BTreeMap::new(), BTreeMap::new()) {
            Ok(s) => s,
            Err(e) => return tracing::error!("create 9p server: {e}"),
        };
        let codec: ServerCodec<jetstream_ufs::Server> = Default::default();
        if let Err(e) = run(&mut service, Framed::new(stream, codec)).await {
            tracing::warn!("9p session ended: {e}");
        }
    });
}

const TVERSION: u8 = 100;
const TATTACH: u8 = 104;
const RLERROR: u8 = 7;
const MAX_MSIZE: u32 = 64 * 1024 + 24;
const EINVAL: u32 = 22;
const ENOENT: u32 = 2;

/// Resolves a client-supplied subpath (a 9P attach name or a volume name) to a
/// directory strictly inside `root`.
pub fn resolve_subpath(root: &Path, name: &str) -> io::Result<PathBuf> {
    let mut path = root.to_path_buf();
    for c in Path::new(name).components() {
        match c {
            Component::Normal(c) => path.push(c),
            Component::RootDir | Component::CurDir => {}
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }
    let path = path.canonicalize()?;
    if !path.starts_with(root) || !path.is_dir() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    Ok(path)
}

async fn read_frame(r: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut size = [0u8; 4];
    r.read_exact(&mut size).await?;
    let n = u32::from_le_bytes(size) as usize;
    if !(7..=MAX_MSIZE as usize).contains(&n) {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let mut f = size.to_vec();
    f.resize(n, 0);
    r.read_exact(&mut f[4..]).await?;
    Ok(f)
}

fn rlerror(tag: [u8; 2], errno: u32) -> Vec<u8> {
    let mut f = 11u32.to_le_bytes().to_vec();
    f.push(RLERROR);
    f.extend(tag);
    f.extend(errno.to_le_bytes());
    f
}

/// Attach name of a Tattach frame: size[4] type[1] tag[2] fid[4] afid[4] uname[s] aname[s] ...
fn attach_aname(f: &[u8]) -> Option<String> {
    let mut o = 15;
    let str_at = |o: usize| -> Option<(&[u8], usize)> {
        let n = u16::from_le_bytes(f.get(o..o + 2)?.try_into().ok()?) as usize;
        Some((f.get(o + 2..o + 2 + n)?, o + 2 + n))
    };
    o = str_at(o)?.1;
    String::from_utf8(str_at(o)?.0.to_vec()).ok()
}

/// Negotiates the version, waits for the Tattach to learn which subdirectory the
/// client wants, then bridges the connection to a ufs server rooted there.
pub async fn handle_conn<T>(mut client: T, root: PathBuf) -> io::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut tversion = None;
    loop {
        let f = read_frame(&mut client).await?;
        let tag = [f[5], f[6]];
        match f[4] {
            TVERSION if f.len() >= 13 => {
                let msize = u32::from_le_bytes(f[7..11].try_into().unwrap());
                let mut r = vec![0; 4];
                r.push(TVERSION + 1);
                r.extend(tag);
                r.extend(msize.min(MAX_MSIZE).to_le_bytes());
                let v = b"9P2000.L";
                if f[11..].starts_with(&[8, 0]) && f[13..].starts_with(v) {
                    r.extend(8u16.to_le_bytes());
                    r.extend(v);
                } else {
                    r.extend(7u16.to_le_bytes());
                    r.extend(b"unknown");
                }
                let n = r.len() as u32;
                r[..4].copy_from_slice(&n.to_le_bytes());
                client.write_all(&r).await?;
                tversion = Some(f);
            }
            TATTACH if tversion.is_some() => {
                let dir = attach_aname(&f)
                    .ok_or(io::ErrorKind::InvalidData)
                    .map_err(io::Error::from)
                    .and_then(|a| resolve_subpath(&root, &a));
                let dir = match dir {
                    Ok(d) => d,
                    Err(e) => {
                        let errno = match e.kind() {
                            io::ErrorKind::NotFound => ENOENT,
                            _ => EINVAL,
                        };
                        client.write_all(&rlerror(tag, errno)).await?;
                        continue;
                    }
                };
                let (mut bridge, inner) = UnixStream::pair()?;
                serve_conn(dir, inner);
                // Replay the handshake; the Rversion was already sent to the client.
                bridge.write_all(&tversion.take().unwrap()).await?;
                read_frame(&mut bridge).await?;
                bridge.write_all(&f).await?;
                copy_bidirectional(&mut client, &mut bridge).await?;
                return Ok(());
            }
            _ => client.write_all(&rlerror(tag, EINVAL)).await?,
        }
    }
}

pub struct QuicOpts {
    pub listen: SocketAddr,
    pub cert: PathBuf,
    pub key: PathBuf,
    /// When set, clients must present a certificate signed by this CA (mTLS).
    pub client_ca: Option<PathBuf>,
}

pub struct IrohOpts {
    pub listen: SocketAddr,
    /// File holding the hex-encoded 32-byte node secret key; created if missing.
    pub secret_file: PathBuf,
}

#[derive(Default)]
pub struct Listeners {
    pub quic: Option<QuicOpts>,
    pub iroh: Option<IrohOpts>,
    pub socket: Option<PathBuf>,
}

struct QuicHandler {
    root: PathBuf,
}

#[async_trait::async_trait]
impl jetstream_quic::QuicHandler for QuicHandler {
    fn alpns(&self) -> Vec<String> {
        vec![crate::ALPN_9P.into()]
    }

    async fn accept(&self, ctx: jetstream_rpc::context::Context, conn: quinn::Connection) {
        while let Ok((send, recv)) = conn.accept_bi().await {
            let root = self.root.clone();
            tracing::info!("quic 9p stream from {ctx}");
            tokio::spawn(async move {
                if let Err(e) = handle_conn(tokio::io::join(recv, send), root).await {
                    tracing::debug!("quic stream closed: {e}");
                }
            });
        }
    }
}

fn start_quic(opts: &QuicOpts, root: &Path) -> Result<jetstream_quic::Server> {
    let certs = crate::tls::load_certs(&opts.cert)?;
    let key = crate::tls::load_key(&opts.key)?;
    let mut router = jetstream_quic::QuicRouter::new();
    router.register(Arc::new(QuicHandler { root: root.into() }));
    Ok(match &opts.client_ca {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for c in crate::tls::load_certs(ca)? {
                roots.add(c)?;
            }
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
            jetstream_quic::Server::new_with_mtls(certs, key, verifier, opts.listen, router)
        }
        None => jetstream_quic::Server::new_with_addr(certs, key, opts.listen, router),
    })
}

#[derive(Debug)]
struct IrohHandler {
    root: PathBuf,
}

impl jetstream_iroh::iroh::protocol::ProtocolHandler for IrohHandler {
    async fn accept(
        &self,
        connection: jetstream_iroh::iroh::endpoint::Connection,
    ) -> Result<(), jetstream_iroh::iroh::protocol::AcceptError> {
        while let Ok((send, recv)) = connection.accept_bi().await {
            let root = self.root.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(tokio::io::join(recv, send), root).await {
                    tracing::debug!("iroh stream closed: {e}");
                }
            });
        }
        Ok(())
    }
}

/// Reads the iroh secret key from `path`, generating it first if it does not exist.
pub fn load_or_create_iroh_key(path: &Path) -> Result<jetstream_iroh::iroh::SecretKey> {
    use std::io::Read;
    let bytes: [u8; 32] = match std::fs::read_to_string(path) {
        Ok(hex) => {
            let hex = hex.trim();
            anyhow::ensure!(hex.len() == 64, "{path:?}: expected 64 hex characters");
            let mut b = [0u8; 32];
            for (i, byte) in b.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
                    .context("bad hex in secret key")?;
            }
            b
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut b = [0u8; 32];
            std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            std::fs::write(path, hex).with_context(|| format!("write {path:?}"))?;
            b
        }
        Err(e) => return Err(e).with_context(|| format!("read {path:?}")),
    };
    Ok(jetstream_iroh::iroh::SecretKey::from_bytes(&bytes))
}

pub async fn serve(root: PathBuf, listeners: Listeners) -> Result<()> {
    anyhow::ensure!(
        listeners.quic.is_some() || listeners.iroh.is_some() || listeners.socket.is_some(),
        "enable at least one of --quic-listen, --iroh-listen, --socket"
    );
    crate::tls::install_crypto_provider();
    let root = root
        .canonicalize()
        .with_context(|| format!("root {root:?}"))?;
    let mut tasks = tokio::task::JoinSet::new();

    if let Some(opts) = &listeners.quic {
        let server = start_quic(opts, &root)?;
        tracing::info!("exporting {root:?} over QUIC on {}", opts.listen);
        tasks.spawn(async move { server.run().await });
    }

    // Keep the router alive for as long as we serve.
    let _iroh = match &listeners.iroh {
        Some(opts) => {
            let key = load_or_create_iroh_key(&opts.secret_file)?;
            let endpoint = jetstream_iroh::endpoint_builder::<jetstream_ufs::Server>()
                .secret_key(key)
                .bind_addr_v4(match opts.listen {
                    SocketAddr::V4(a) => a,
                    SocketAddr::V6(_) => anyhow::bail!("--iroh-listen must be an IPv4 address"),
                })
                .bind()
                .await?;
            tracing::info!(
                "exporting {root:?} over iroh, node id {}, port {}",
                endpoint.node_id(),
                opts.listen.port()
            );
            Some(
                jetstream_iroh::iroh::protocol::Router::builder(endpoint)
                    .accept(
                        crate::ALPN_9P.as_bytes(),
                        IrohHandler { root: root.clone() },
                    )
                    .spawn(),
            )
        }
        None => None,
    };

    // Bound last so its existence doubles as a readiness marker.
    if let Some(path) = &listeners.socket {
        let _ = std::fs::remove_file(path);
        let unix = UnixListener::bind(path).context("bind unix")?;
        tracing::info!("exporting {root:?} on unix socket {path:?}");
        let root = root.clone();
        tasks.spawn(async move {
            while let Ok((stream, _)) = unix.accept().await {
                let root = root.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, root).await;
                });
            }
        });
    }

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
        _ = tasks.join_next(), if !tasks.is_empty() => {}
    }
    Ok(())
}
