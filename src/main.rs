mod attrs;
mod builder;
mod config;
mod tree;
mod vfs;
mod watcher;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use rand::Rng;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::vfs::FusionFs;
use crate::watcher::Watcher;

#[derive(Parser)]
#[command(name = "fusion", about = "Read-only virtual NFS for media libraries")]
struct Cli {
    /// Path to config YAML.
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let config = Arc::new(Config::load(&cli.config).context("loading config")?);

    let server_id: u64 = rand::thread_rng().gen();
    info!(server_id, "building tree");
    let tree = builder::build(&config, server_id).context("building initial tree")?;
    let watch_roots = watcher::collect_roots(&config, &tree);
    let tree = Arc::new(RwLock::new(tree));

    let _watcher = Watcher::start(config.clone(), tree.clone(), watch_roots)
        .context("starting watcher")?;

    let bind = config.server.bind.clone();
    info!(%bind, "starting NFS server");
    let listener = NFSTcpListener::bind(&bind, FusionFs::new(tree, server_id)).await?;
    listener.handle_forever().await?;
    Ok(())
}
