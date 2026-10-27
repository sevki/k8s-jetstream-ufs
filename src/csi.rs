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
use pb::{controller_server::*, identity_server::*, node_server::*, *};

pub const DRIVER_NAME: &str = "ufs.jetstream.rs";
const DEFAULT_PORT: u16 = 5640;
const MSIZE: u32 = 65536;

/// Volume names become directory names under the export root.
pub fn valid_volume_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
}

/// Parsed and validated `volume_context` of a volume.
#[derive(Debug, PartialEq, Eq)]
pub struct Export {
    pub addr: IpAddr,
    pub port: u16,
    /// Directory below the server's export root; becomes the 9P attach name.
    pub subpath: String,
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
        let subpath = ctx.get("subpath").cloned().unwrap_or_else(|| "/".into());
        Ok(Export {
            addr,
            port,
            subpath,
        })
    }
}

pub fn mount_options(port: u16, subpath: &str, read_only: bool) -> String {
    let mut o =
        format!("trans=tcp,port={port},version=9p2000.L,msize={MSIZE},access=any,aname={subpath}");
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
    /// Set when running as the controller (the server's export directory).
    export_root: Option<PathBuf>,
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
            capabilities: self.plugin_capabilities(),
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
                .arg(mount_options(export.port, &export.subpath, req.readonly))
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

#[tonic::async_trait]
impl Controller for Plugin {
    async fn create_volume(
        &self,
        req: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = req.into_inner();
        let root = self.root()?;
        if !valid_volume_name(&req.name) {
            return Err(Status::invalid_argument("invalid volume name"));
        }
        if req.volume_capabilities.is_empty() {
            return Err(Status::invalid_argument("volume_capabilities is required"));
        }
        let server = req
            .parameters
            .get("server")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Status::invalid_argument("StorageClass parameter `server` is required")
            })?;
        let dir = root.join(&req.name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| Status::internal(format!("mkdir {dir:?}: {e}")))?;
        // Pods run as arbitrary uids; the volume is the isolation boundary.
        std::fs::set_permissions(&dir, std::os::unix::fs::PermissionsExt::from_mode(0o777))
            .map_err(|e| Status::internal(format!("chmod {dir:?}: {e}")))?;
        let mut volume_context = std::collections::HashMap::from([
            ("server".to_string(), server.clone()),
            ("subpath".to_string(), req.name.clone()),
        ]);
        if let Some(port) = req.parameters.get("port") {
            volume_context.insert("port".into(), port.clone());
        }
        tracing::info!("created volume {}", req.name);
        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id: req.name,
                capacity_bytes: req.capacity_range.map(|r| r.required_bytes).unwrap_or(0),
                volume_context,
                ..Default::default()
            }),
        }))
    }

    async fn delete_volume(
        &self,
        req: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let id = req.into_inner().volume_id;
        let root = self.root()?;
        if !valid_volume_name(&id) {
            return Err(Status::invalid_argument("invalid volume id"));
        }
        match std::fs::remove_dir_all(root.join(&id)) {
            Ok(()) => tracing::info!("deleted volume {id}"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Status::internal(format!("remove {id}: {e}"))),
        }
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_get_capabilities(
        &self,
        _: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![ControllerServiceCapability {
                r#type: Some(controller_service_capability::Type::Rpc(
                    controller_service_capability::Rpc {
                        r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                    },
                )),
            }],
        }))
    }

    async fn validate_volume_capabilities(
        &self,
        req: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = req.into_inner();
        let root = self.root()?;
        if !valid_volume_name(&req.volume_id) || !root.join(&req.volume_id).is_dir() {
            return Err(Status::not_found("volume not found"));
        }
        Ok(Response::new(ValidateVolumeCapabilitiesResponse {
            confirmed: Some(validate_volume_capabilities_response::Confirmed {
                volume_context: req.volume_context,
                volume_capabilities: req.volume_capabilities,
                parameters: req.parameters,
                ..Default::default()
            }),
            message: String::new(),
        }))
    }

    async fn controller_publish_volume(
        &self,
        _: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("attach not required"))
    }

    async fn controller_unpublish_volume(
        &self,
        _: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("attach not required"))
    }

    async fn list_volumes(
        &self,
        _: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_capacity(
        &self,
        _: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn create_snapshot(
        &self,
        _: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn delete_snapshot(
        &self,
        _: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn list_snapshots(
        &self,
        _: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn controller_expand_volume(
        &self,
        _: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn controller_get_volume(
        &self,
        _: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn controller_modify_volume(
        &self,
        _: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

impl Plugin {
    fn plugin_capabilities(&self) -> Vec<PluginCapability> {
        if self.export_root.is_none() {
            return vec![];
        }
        vec![PluginCapability {
            r#type: Some(plugin_capability::Type::Service(
                plugin_capability::Service {
                    r#type: plugin_capability::service::Type::ControllerService as i32,
                },
            )),
        }]
    }

    fn root(&self) -> Result<&Path, Status> {
        self.export_root
            .as_deref()
            .ok_or_else(|| Status::unimplemented("not running in controller mode"))
    }
}

/// Runs the node service, or the controller service when `export_root` is set.
pub async fn run(endpoint: PathBuf, node_id: String, export_root: Option<PathBuf>) -> Result<()> {
    if let Some(dir) = endpoint.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let _ = std::fs::remove_file(&endpoint);
    let listener = UnixListener::bind(&endpoint).context("bind csi socket")?;
    tracing::info!("csi {DRIVER_NAME} listening on {endpoint:?}");
    let controller = export_root.is_some();
    let plugin = Plugin {
        node_id,
        export_root,
    };
    let mut server = tonic::transport::Server::builder();
    let router = server.add_service(IdentityServer::new(plugin.clone()));
    let router = if controller {
        router.add_service(ControllerServer::new(plugin))
    } else {
        router.add_service(NodeServer::new(plugin))
    };
    router
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
                port: 9000,
                subpath: "/".into()
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
        assert!(mount_options(5640, "/", true).ends_with(",ro"));
        assert!(!mount_options(5640, "/", false).contains(",ro"));
        assert!(mount_options(5640, "vol-1", false).contains("aname=vol-1"));
    }

    #[test]
    fn controller_mode_reports_controller_service() {
        let node = Plugin {
            node_id: "n".into(),
            export_root: None,
        };
        assert!(node.plugin_capabilities().is_empty());
        let ctl = Plugin {
            node_id: "n".into(),
            export_root: Some("/export".into()),
        };
        assert_eq!(ctl.plugin_capabilities().len(), 1);
    }

    #[test]
    fn volume_names() {
        assert!(valid_volume_name("pvc-0f3a-1.x_y"));
        for bad in ["", ".", "..", ".hidden", "a/b", "../x", "a b"] {
            assert!(!valid_volume_name(bad), "{bad}");
        }
    }
}
