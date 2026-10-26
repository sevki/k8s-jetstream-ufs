mod csi;
mod server;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Export a directory over 9P2000.L (jetstream_ufs).
    Serve {
        #[arg(long)]
        root: PathBuf,
        #[arg(long, default_value = "0.0.0.0:5640")]
        listen: String,
        /// Also listen on this unix socket.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Run the CSI node plugin that mounts 9P exports with the kernel v9fs client.
    Csi {
        #[arg(long, default_value = "/csi/csi.sock")]
        endpoint: PathBuf,
        #[arg(long, env = "NODE_ID")]
        node_id: String,
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
            listen,
            socket,
        } => server::serve(root, listen, socket).await,
        Cmd::Csi { endpoint, node_id } => csi::run(endpoint, node_id).await,
    }
}
