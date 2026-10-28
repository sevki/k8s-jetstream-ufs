use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use k8s_jetstream_ufs::{csi, server, transport::ClientTls, DEFAULT_IROH_PORT, DEFAULT_QUIC_PORT};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Export a directory over 9P2000.L (jetstream_ufs) via QUIC and/or iroh.
    Serve {
        #[arg(long)]
        root: PathBuf,
        /// QUIC listen address, e.g. 0.0.0.0:5641. Needs --tls-cert and --tls-key.
        #[arg(long)]
        quic_listen: Option<SocketAddr>,
        #[arg(long, requires = "quic_listen")]
        tls_cert: Option<PathBuf>,
        #[arg(long, requires = "quic_listen")]
        tls_key: Option<PathBuf>,
        /// Require client certificates signed by this CA (mTLS).
        #[arg(long, requires = "quic_listen")]
        client_ca: Option<PathBuf>,
        /// iroh listen address (IPv4), e.g. 0.0.0.0:5642. Needs --iroh-secret-file.
        #[arg(long)]
        iroh_listen: Option<SocketAddr>,
        /// Hex-encoded iroh secret key; created if missing. `iroh-keygen` makes one.
        #[arg(long, requires = "iroh_listen")]
        iroh_secret_file: Option<PathBuf>,
        /// Also listen on this unix socket (also a readiness marker).
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Print the iroh node id for a secret key file, creating the file if needed.
    IrohKeygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Run the CSI node plugin (or controller, with --controller-export-root).
    Csi {
        #[arg(long, default_value = "/csi/csi.sock")]
        endpoint: PathBuf,
        #[arg(long, env = "NODE_ID", default_value = "controller")]
        node_id: String,
        /// Run the controller service (create/delete volumes as directories here)
        /// instead of the node service. Must be the server's export directory.
        #[arg(long)]
        controller_export_root: Option<PathBuf>,
        /// CA that signed the server certificates (QUIC volumes).
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        /// Client certificate/key presented to servers that require mTLS.
        #[arg(long, requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        #[arg(long, requires = "tls_cert")]
        tls_key: Option<PathBuf>,
        /// Directory for the per-mount proxy sockets.
        #[arg(long, default_value = "/run/ufs-csi")]
        socket_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match Cli::parse().cmd {
        Cmd::Serve {
            root,
            quic_listen,
            tls_cert,
            tls_key,
            client_ca,
            iroh_listen,
            iroh_secret_file,
            socket,
        } => {
            let quic = match quic_listen {
                Some(listen) => Some(server::QuicOpts {
                    listen,
                    cert: tls_cert.ok_or_else(|| anyhow::anyhow!("--tls-cert is required"))?,
                    key: tls_key.ok_or_else(|| anyhow::anyhow!("--tls-key is required"))?,
                    client_ca,
                }),
                None => None,
            };
            let iroh = match iroh_listen {
                Some(listen) => Some(server::IrohOpts {
                    listen,
                    secret_file: iroh_secret_file
                        .ok_or_else(|| anyhow::anyhow!("--iroh-secret-file is required"))?,
                }),
                None => None,
            };
            server::serve(root, server::Listeners { quic, iroh, socket }).await
        }
        Cmd::IrohKeygen { out } => {
            let key = server::load_or_create_iroh_key(&out)?;
            println!("{}", key.public());
            Ok(())
        }
        Cmd::Csi {
            endpoint,
            node_id,
            controller_export_root,
            tls_ca,
            tls_cert,
            tls_key,
            socket_dir,
        } => {
            let tls = tls_ca.map(|ca| ClientTls {
                ca,
                identity: tls_cert.zip(tls_key),
            });
            csi::run(endpoint, node_id, controller_export_root, tls, socket_dir).await
        }
    }
}

#[allow(dead_code)]
const _: (u16, u16) = (DEFAULT_QUIC_PORT, DEFAULT_IROH_PORT);
