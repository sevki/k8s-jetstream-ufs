//! Minimal CSI node plugin: mounts a jetstream 9P export with the kernel v9fs client.

use std::{
    net::{IpAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[allow(clippy::all)]
pub mod pb {
    tonic::include_proto!("csi.v1");
}
use pb::{identity_server::*, node_server::*, *};

pub const DRIVER_NAME: &str = "ufs.jetstream.rs";
const DEFAULT_PORT: u16 = 5640;
const MSIZE: u32 = 65536;

/// Parsed and validated `volume_context` of a volume.
#[derive(Debug, PartialEq, Eq)]
pub struct Export {
    pub addr: IpAddr,
    pub port: u16,
}

impl Export {
    pub fn from_context(ctx: &std::collections::HashMap<String, String>) -> Result<Self, Status> {
        let server = ctx
            .get("server")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Status::invalid_argument("volume_context.server is required"))?;
        let port = match ctx.get("port") {
            Some(p) => p
                .parse()
                .map_err(|_| Status::invalid_argument("volume_context.port is not a port"))?,
            None => DEFAULT_PORT,
        };
        // The kernel only accepts IP literals, so resolve names here.
        let addr = (server.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| Status::invalid_argument(format!("resolve {server}: {e}")))?
            .map(|a| a.ip())
            .find(IpAddr::is_ipv4)
            .or_else(|| {
                (server.as_str(), port)
                    .to_socket_addrs()
                    .ok()?
                    .next()
                    .map(|a| a.ip())
            })
            .ok_or_else(|| Status::invalid_argument(format!("{server} has no address")))?;
        Ok(Export { addr, port })
    }
}

pub fn mount_options(port: u16, read_only: bool) -> String {
    let mut o = format!("trans=tcp,port={port},version=9p2000.L,msize={MSIZE},access=any");
    if read_only {
        o.push_str(",ro");
    }
    o
}

fn is_mounted(target: &Path) -> Result<bool, Status> {
    let info = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| Status::internal(format!("read mountinfo: {e}")))?;
    let target = target.to_string_lossy();
    Ok(info.lines().any(|l| l.split(' ').nth(4) == Some(&*target)))
}

fn exec(cmd: &mut Command) -> Result<(), Status> {
    let out = cmd
        .output()
        .map_err(|e| Status::internal(format!("exec {cmd:?}: {e}")))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(Status::internal(format!(
            "{cmd:?}: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

#[derive(Clone)]
struct Plugin {
    node_id: String,
}

#[tonic::async_trait]
impl Identity for Plugin {
    async fn get_plugin_info(
        &self,
        _: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER_NAME.into(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![],
        }))
    }

    async fn probe(&self, _: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}

#[tonic::async_trait]
impl Node for Plugin {
    async fn node_publish_volume(
        &self,
        req: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = req.into_inner();
        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id is required"));
        }
        if req.target_path.is_empty() {
            return Err(Status::invalid_argument("target_path is required"));
        }
        if req.volume_capability.is_none() {
            return Err(Status::invalid_argument("volume_capability is required"));
        }
        let export = Export::from_context(&req.volume_context)?;
        let target = PathBuf::from(&req.target_path);
        std::fs::create_dir_all(&target)
            .map_err(|e| Status::internal(format!("mkdir {target:?}: {e}")))?;
        if is_mounted(&target)? {
            return Ok(Response::new(NodePublishVolumeResponse {}));
        }
        tracing::info!("mounting {}:{} on {target:?}", export.addr, export.port);
        exec(
            Command::new("mount")
                .args(["-t", "9p", "-o"])
                .arg(mount_options(export.port, req.readonly))
                .arg(export.addr.to_string())
                .arg(&target),
        )?;
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = req.into_inner();
        if req.volume_id.is_empty() || req.target_path.is_empty() {
            return Err(Status::invalid_argument(
                "volume_id and target_path are required",
            ));
        }
        let target = PathBuf::from(&req.target_path);
        if is_mounted(&target)? {
            tracing::info!("unmounting {target:?}");
            exec(Command::new("umount").arg(&target))?;
        }
        let _ = std::fs::remove_dir(&target);
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self,
        _: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![],
        }))
    }

    async fn node_get_info(
        &self,
        _: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            ..Default::default()
        }))
    }

    async fn node_stage_volume(
        &self,
        _: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Err(Status::unimplemented("staging not supported"))
    }

    async fn node_unstage_volume(
        &self,
        _: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Err(Status::unimplemented("staging not supported"))
    }

    async fn node_get_volume_stats(
        &self,
        _: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("volume stats not supported"))
    }

    async fn node_expand_volume(
        &self,
        _: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("expansion not supported"))
    }
}

pub async fn run(endpoint: PathBuf, node_id: String) -> Result<()> {
    if let Some(dir) = endpoint.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let _ = std::fs::remove_file(&endpoint);
    let listener = UnixListener::bind(&endpoint).context("bind csi socket")?;
    tracing::info!("csi {DRIVER_NAME} listening on {endpoint:?}");
    let plugin = Plugin { node_id };
    tonic::transport::Server::builder()
        .add_service(IdentityServer::new(plugin.clone()))
        .add_service(NodeServer::new(plugin))
        .serve_with_incoming(UnixListenerStream::new(listener))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_export() {
        let e = Export::from_context(&ctx(&[("server", "10.0.0.7"), ("port", "9000")])).unwrap();
        assert_eq!(
            e,
            Export {
                addr: "10.0.0.7".parse().unwrap(),
                port: 9000
            }
        );
        let e = Export::from_context(&ctx(&[("server", "127.0.0.1")])).unwrap();
        assert_eq!(e.port, DEFAULT_PORT);
    }

    #[test]
    fn rejects_bad_context() {
        assert!(Export::from_context(&ctx(&[])).is_err());
        assert!(Export::from_context(&ctx(&[("server", "127.0.0.1"), ("port", "x")])).is_err());
    }

    #[test]
    fn options() {
        assert!(mount_options(5640, true).ends_with(",ro"));
        assert!(!mount_options(5640, false).contains("ro"));
    }
}
