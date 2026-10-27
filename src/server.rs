//! 9P2000.L file server: exports a directory over TCP (and optionally a unix socket).
//!
//! jetstream's `run` loop only accepts unix-socket transports, so every TCP
//! connection is bridged onto an in-process unix socket pair.

use std::{
    collections::BTreeMap,
    io,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result};
use jetstream_rpc::server::{run, ServerCodec};
use tokio::{
    io::{copy_bidirectional, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
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

async fn read_frame(r: &mut (impl AsyncReadExt + Unpin)) -> io::Result<Vec<u8>> {
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
async fn handle_tcp(mut client: TcpStream, root: PathBuf) -> io::Result<()> {
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

pub async fn serve(root: PathBuf, listen: String, socket: Option<PathBuf>) -> Result<()> {
    let root = root
        .canonicalize()
        .with_context(|| format!("root {root:?}"))?;
    let tcp = TcpListener::bind(&listen).await.context("bind tcp")?;
    tracing::info!("exporting {root:?} on {}", tcp.local_addr()?);

    if let Some(path) = socket {
        let _ = std::fs::remove_file(&path);
        let unix = UnixListener::bind(&path).context("bind unix")?;
        let root = root.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = unix.accept().await {
                serve_conn(root.clone(), stream);
            }
        });
    }

    loop {
        let (client, peer) = tcp.accept().await?;
        client.set_nodelay(true).ok();
        tracing::info!("connection from {peer}");
        let root = root.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_tcp(client, root).await {
                tracing::debug!("connection {peer} closed: {e}");
            }
        });
    }
}
