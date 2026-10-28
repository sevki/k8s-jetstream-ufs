//! Minimal CSI node plugin: mounts a jetstream 9P export with the kernel v9fs client.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use tokio::{net::UnixListener, task::JoinHandle};

use crate::transport::{proxy, ClientTls, Dialer, Target};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[allow(clippy::all)]
pub mod pb {
    tonic::include_proto!("csi.v1");
}
use pb::{controller_server::*, identity_server::*, node_server::*, *};

pub const DRIVER_NAME: &str = "ufs.jetstream.rs";
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
    pub target: Target,
    /// Directory below the server's export root; becomes the 9P attach name.
    pub subpath: String,
}

impl Export {
    /// Validates the context without touching the network (names are resolved at dial time).
    pub fn from_context(ctx: &HashMap<String, String>) -> Result<Self, Status> {
        let get = |k: &str| ctx.get(k).filter(|v| !v.is_empty());
        let target = match get("transport").map(String::as_str).unwrap_or("quic") {
            "quic" => {
                let host = get("server").ok_or_else(|| {
                    Status::invalid_argument("server is required for transport quic")
                })?;
                let port = match get("port") {
                    Some(p) => p
                        .parse()
                        .map_err(|_| Status::invalid_argument("port is not a port"))?,
                    None => crate::DEFAULT_QUIC_PORT,
                };
                Target::Quic {
                    host: host.clone(),
                    port,
                }
            }
            "iroh" => {
                let node = get("iroh-node").ok_or_else(|| {
                    Status::invalid_argument("iroh-node is required for transport iroh")
                })?;
                let addrs = get("iroh-addrs")
                    .map(|a| a.split(',').map(|s| s.trim().to_string()).collect())
                    .unwrap_or_default();
                Target::Iroh {
                    node: node.clone(),
                    addrs,
                }
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown transport {other:?} (want quic or iroh)"
                )))
            }
        };
        let subpath = ctx.get("subpath").cloned().unwrap_or_else(|| "/".into());
        Ok(Export { target, subpath })
    }
}

pub fn mount_options(subpath: &str, read_only: bool) -> String {
    let mut o = format!("trans=unix,version=9p2000.L,msize={MSIZE},access=any,aname={subpath}");
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
    /// Node mode: dials servers, and keeps one proxy per mounted target path.
    dialer: Arc<Dialer>,
    socket_dir: PathBuf,
    proxies: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl Plugin {
    fn new(
        node_id: String,
        export_root: Option<PathBuf>,
        tls: Option<ClientTls>,
        socket_dir: PathBuf,
    ) -> Self {
        Plugin {
            node_id,
            export_root,
            dialer: Arc::new(Dialer::new(tls)),
            socket_dir,
            proxies: Default::default(),
        }
    }

    fn socket_for(&self, target: &Path) -> PathBuf {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        target.hash(&mut h);
        self.socket_dir.join(format!("{:016x}.sock", h.finish()))
    }

    fn stop_proxy(&self, target: &Path) {
        if let Some(task) = self
            .proxies
            .lock()
            .unwrap()
            .remove(target.to_str().unwrap_or(""))
        {
            task.abort();
        }
        let _ = std::fs::remove_file(self.socket_for(target));
    }
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
        tracing::info!("mounting {:?} on {target:?}", export.target);
        // The kernel 9p client only does tcp/unix; it talks to a local proxy that
        // carries the stream over QUIC/iroh.
        std::fs::create_dir_all(&self.socket_dir)
            .map_err(|e| Status::internal(format!("mkdir {:?}: {e}", self.socket_dir)))?;
        let sock = self.socket_for(&target);
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock)
            .map_err(|e| Status::internal(format!("bind {sock:?}: {e}")))?;
        let task = tokio::spawn(proxy(listener, self.dialer.clone(), export.target));
        self.proxies
            .lock()
            .unwrap()
            .insert(req.target_path.clone(), task);
        let mount = tokio::task::spawn_blocking({
            let opts = mount_options(&export.subpath, req.readonly);
            let target = target.clone();
            move || {
                exec(
                    Command::new("mount")
                        .args(["-t", "9p", "-o"])
                        .arg(opts)
                        .arg(&sock)
                        .arg(&target),
                )
            }
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        if let Err(e) = mount {
            self.stop_proxy(&target);
            return Err(e);
        }
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
            // Unmounting makes the kernel talk to our own proxy (final clunk), so it must not
            // block a runtime worker the proxy tasks may need.
            let t = target.clone();
            tokio::task::spawn_blocking(move || exec(Command::new("umount").arg(&t)))
                .await
                .map_err(|e| Status::internal(e.to_string()))??;
        }
        self.stop_proxy(&target);
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
        let dir = root.join(&req.name);
        // Everything but the CSI-internal keys becomes the node's volume_context;
        // validate it now so bad StorageClasses fail at provisioning, not at mount.
        let mut volume_context: HashMap<String, String> = req
            .parameters
            .iter()
            .filter(|(k, _)| !k.starts_with("csi.storage.k8s.io/"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        volume_context.insert("subpath".into(), req.name.clone());
        Export::from_context(&volume_context)?;
        std::fs::create_dir_all(&dir)
            .map_err(|e| Status::internal(format!("mkdir {dir:?}: {e}")))?;
        // Pods run as arbitrary uids; the volume is the isolation boundary.
        std::fs::set_permissions(&dir, std::os::unix::fs::PermissionsExt::from_mode(0o777))
            .map_err(|e| Status::internal(format!("chmod {dir:?}: {e}")))?;
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
pub async fn run(
    endpoint: PathBuf,
    node_id: String,
    export_root: Option<PathBuf>,
    tls: Option<ClientTls>,
    socket_dir: PathBuf,
) -> Result<()> {
    if let Some(dir) = endpoint.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let _ = std::fs::remove_file(&endpoint);
    let listener = UnixListener::bind(&endpoint).context("bind csi socket")?;
    tracing::info!("csi {DRIVER_NAME} listening on {endpoint:?}");
    let controller = export_root.is_some();
    let plugin = Plugin::new(node_id, export_root, tls, socket_dir);
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
    fn parses_quic_export() {
        let e = Export::from_context(&ctx(&[("server", "ufs.svc"), ("port", "9000")])).unwrap();
        assert_eq!(
            e,
            Export {
                target: Target::Quic {
                    host: "ufs.svc".into(),
                    port: 9000
                },
                subpath: "/".into()
            }
        );
        let e = Export::from_context(&ctx(&[("server", "ufs.svc"), ("subpath", "vol-1")])).unwrap();
        assert_eq!(
            e.target,
            Target::Quic {
                host: "ufs.svc".into(),
                port: crate::DEFAULT_QUIC_PORT
            }
        );
        assert_eq!(e.subpath, "vol-1");
    }

    #[test]
    fn parses_iroh_export() {
        let e = Export::from_context(&ctx(&[
            ("transport", "iroh"),
            ("iroh-node", "abc"),
            ("iroh-addrs", "a:1, b:2"),
        ]))
        .unwrap();
        assert_eq!(
            e.target,
            Target::Iroh {
                node: "abc".into(),
                addrs: vec!["a:1".into(), "b:2".into()]
            }
        );
    }

    #[test]
    fn rejects_bad_context() {
        assert!(Export::from_context(&ctx(&[])).is_err());
        assert!(Export::from_context(&ctx(&[("server", "h"), ("port", "x")])).is_err());
        assert!(Export::from_context(&ctx(&[("transport", "iroh")])).is_err());
        assert!(Export::from_context(&ctx(&[("transport", "tcp"), ("server", "h")])).is_err());
    }

    #[test]
    fn options() {
        assert!(mount_options("/", true).ends_with(",ro"));
        assert!(!mount_options("/", false).contains(",ro"));
        assert!(mount_options("vol-1", false).contains("aname=vol-1"));
        assert!(mount_options("/", false).contains("trans=unix"));
    }

    #[test]
    fn controller_mode_reports_controller_service() {
        let node = Plugin::new("n".into(), None, None, "/tmp".into());
        assert!(node.plugin_capabilities().is_empty());
        let ctl = Plugin::new("n".into(), Some("/export".into()), None, "/tmp".into());
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
