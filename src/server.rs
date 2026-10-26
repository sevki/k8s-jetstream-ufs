//! 9P2000.L file server: exports a directory over TCP (and optionally a unix socket).
//!
//! jetstream's `run` loop only accepts unix-socket transports, so every TCP
//! connection is bridged onto an in-process unix socket pair.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result};
use jetstream_rpc::server::{run, ServerCodec};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, UnixListener, UnixStream},
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
        let (mut client, peer) = tcp.accept().await?;
        client.set_nodelay(true).ok();
        tracing::info!("connection from {peer}");
        let (mut bridge, inner) = UnixStream::pair()?;
        serve_conn(root.clone(), inner);
        tokio::spawn(async move {
            let _ = copy_bidirectional(&mut client, &mut bridge).await;
        });
    }
}
