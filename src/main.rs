#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use rand::Rng;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::EnvFilter;

use fusion::builder;
use fusion::config::Config;
use fusion::vfs::{new_file_cache, FusionFs};
use fusion::watcher::{self, Watcher};

#[derive(Parser)]
#[command(name = "fusion", about = "Read-only virtual NFS for media libraries")]
struct Cli {
    /// Path to config YAML.
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

/// Self-documenting starter template written on first run when no config
/// exists. Everything is commented out so the file parses as YAML null —
/// `prepare_config` detects that and prints an "edit me" message rather
/// than failing with a deserialization error.
const STARTER_CONFIG: &str = "\
# fusion configuration. Edit this file and re-run `fusion`.
#
# Define one or more shares. A share name maps to a path or list of
# paths that fusion exposes as a single NFS export. Examples:
#
#   shares:
#     Movies: /path/to/your/movies
#     TV:
#       - /mnt/disk1/TV
#       - /mnt/disk2/TV
#
# See config.advanced.yaml in the repo or
# https://github.com/nchapman/fusion#configure for every option.
";

/// First-run UX. Returns `Ok(true)` if we already handled the situation
/// (wrote a starter template, or detected an unedited template) and main
/// should exit cleanly. Returns `Ok(false)` to proceed to `Config::load`.
fn prepare_config(path: &Path) -> Result<bool> {
    if !path.exists() {
        match write_starter_template(path) {
            Ok(()) => {
                eprintln!("Wrote starter config to {}.", path.display());
                eprintln!("Edit it (define at least one share) and re-run `fusion`.");
                return Ok(true);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Lost a race with another process / external write that
                // landed between `path.exists()` and `create_new`. Whatever
                // is there now will be picked up on the next run; tell the
                // user clearly rather than bail with the contradictory
                // "no config found AND already exists" message.
                eprintln!(
                    "Config appeared at {} while starting. Re-run `fusion` to load it.",
                    path.display()
                );
                return Ok(true);
            }
            Err(e) => {
                anyhow::bail!(
                    "no config found at {} and couldn't write a starter template ({}). \
                     Create one yourself — see config.example.yaml in the repo or \
                     https://github.com/nchapman/fusion#configure. \
                     If you're running the Docker image, mount a config: \
                     `-v /path/to/config.yaml:/etc/fusion/config.yaml`",
                    path.display(),
                    e
                );
            }
        }
    }
    if config_is_yaml_null(path)? {
        eprintln!(
            "{} exists but defines nothing (all comments).",
            path.display()
        );
        eprintln!("Edit it (define at least one share) and re-run `fusion`.");
        return Ok(true);
    }
    Ok(false)
}

fn write_starter_template(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    // `create_new` ensures we never overwrite an existing file (TOCTOU-safe
    // against the `path.exists()` check above).
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(STARTER_CONFIG.as_bytes())?;
    Ok(())
}

/// True if the YAML at `path` parses to a null document — empty file,
/// only comments, or `~`. The signature for "user wrote the starter
/// template but hasn't edited yet."
///
/// Returns `false` (not `true`) when YAML is *malformed*: a partially-
/// edited config with a syntax error has real content and deserves
/// `Config::load`'s actual error message, not a misleading "all comments"
/// hint. An empty `{}` mapping also returns `false` — it's a valid
/// (though useless) document and gets handled by `Config::load`'s
/// "no shares" validation.
fn config_is_yaml_null(path: &Path) -> Result<bool> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    match serde_yml::from_str::<serde_yml::Value>(&text) {
        Ok(v) => Ok(matches!(v, serde_yml::Value::Null)),
        Err(_) => Ok(false),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    if prepare_config(&cli.config)? {
        return Ok(());
    }
    let config = Arc::new(Config::load(&cli.config).context("loading config")?);

    let server_id: u64 = rand::thread_rng().gen();
    info!(server_id, "building tree");
    let tree = builder::build(&config, server_id).context("building initial tree")?;
    let watch_roots = watcher::collect_roots(&config, &tree);
    let tree = Arc::new(RwLock::new(tree));

    let file_cache = new_file_cache();

    let _watcher = Watcher::start(
        config.clone(),
        tree.clone(),
        watch_roots,
        file_cache.clone(),
    )
    .context("starting watcher")?;

    let bind = config.server.bind.clone();
    info!(%bind, "starting NFS server");
    let listener = NFSTcpListener::bind(&bind, FusionFs::new(tree, server_id, file_cache)).await?;
    listener.handle_forever().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn prepare_config_writes_template_when_missing_and_returns_true() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        let handled = prepare_config(&path).unwrap();
        assert!(handled, "first run should be handled by prepare_config");
        let written = std::fs::read_to_string(&path).unwrap();
        // Template is all comments + trailing newlines → parses to YAML null.
        assert!(written.contains("Edit this file"));
        assert!(config_is_yaml_null(&path).unwrap());
    }

    #[test]
    fn prepare_config_detects_unedited_template_on_rerun() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        prepare_config(&path).unwrap(); // first run writes template
        let handled = prepare_config(&path).unwrap(); // second run sees null
        assert!(handled, "unedited template must short-circuit");
    }

    #[test]
    fn prepare_config_passes_through_for_real_config() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "shares:\n  Movies: /tmp\n").unwrap();
        let handled = prepare_config(&path).unwrap();
        assert!(!handled, "real config must be handled by Config::load");
    }

    #[test]
    fn prepare_config_passes_through_for_empty_mapping() {
        // `{}` is a valid (but useless) YAML mapping, not Null. It should
        // fall through to Config::load — the user wrote *something*, even
        // if it's empty, and they deserve Config::load's "no shares" error
        // rather than the all-comments hint.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "{}\n").unwrap();
        let handled = prepare_config(&path).unwrap();
        assert!(!handled);
    }

    #[test]
    fn prepare_config_passes_through_for_malformed_yaml() {
        // A partially-edited config with a syntax error has real content;
        // pretending it's "all comments" would mislead the user. Fall
        // through so Config::load reports the actual parse error.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "shares:\n  Movies: [unclosed\n").unwrap();
        let handled = prepare_config(&path).unwrap();
        assert!(!handled, "malformed YAML must reach Config::load");
    }

    #[test]
    fn prepare_config_does_not_overwrite_existing() {
        // Even an empty file (which would parse to null and trigger the
        // unedited-template branch) must NOT be overwritten — we'd lose
        // the user's blank-but-intentional file.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "").unwrap();
        let handled = prepare_config(&path).unwrap();
        assert!(handled, "empty existing file should hit the null branch");
        // Confirm we didn't replace the empty file with the template.
        let after = std::fs::read_to_string(&path).unwrap();
        assert_eq!(after, "", "must not overwrite existing file");
    }
}
