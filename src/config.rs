use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub shares: BTreeMap<String, ShareConfig>,
    #[serde(default)]
    pub options: Options,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { bind: default_bind() }
    }
}

fn default_bind() -> String {
    "0.0.0.0:2049".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct ShareConfig {
    /// Roots that union into the share root.
    #[serde(default)]
    pub merge: Vec<PathBuf>,

    /// Roots that mount as named subdirectories of the share.
    #[serde(default)]
    pub mount: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct Options {
    #[serde(default = "default_true")]
    pub hide_dotfiles: bool,
    #[serde(default)]
    pub hide_patterns: Vec<String>,
    /// If true, scan follows symbolic links and exposes their targets as
    /// regular files. Off by default because a symlink inside a media root
    /// to e.g. `/etc/passwd` would otherwise be served over NFS.
    #[serde(default)]
    pub follow_symlinks: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            hide_dotfiles: true,
            hide_patterns: Vec::new(),
            follow_symlinks: false,
        }
    }
}

fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()?;
        cfg.canonicalize_roots();
        Ok(cfg)
    }

    /// Resolve symlinks in every root path. macOS reports FSEvents under the
    /// real (e.g. /private/tmp) path, so unless we canonicalize at load time,
    /// our `path_index` keys won't match the events we receive. We do this
    /// once, eagerly — missing roots stay un-canonicalized and surface as
    /// scan warnings later.
    fn canonicalize_roots(&mut self) {
        for share in self.shares.values_mut() {
            for p in share.merge.iter_mut() {
                if let Ok(c) = p.canonicalize() {
                    *p = c;
                }
            }
            for p in share.mount.values_mut() {
                if let Ok(c) = p.canonicalize() {
                    *p = c;
                }
            }
        }
    }

    fn validate(&self) -> Result<()> {
        if self.shares.is_empty() {
            anyhow::bail!("config has no shares");
        }
        for (name, share) in &self.shares {
            if name.is_empty() || name.contains('/') {
                anyhow::bail!("invalid share name {name:?}");
            }
            if share.merge.is_empty() && share.mount.is_empty() {
                anyhow::bail!("share {name} has no merge or mount roots");
            }
            for p in &share.merge {
                if !p.is_absolute() {
                    anyhow::bail!("share {name} merge root {} must be absolute", p.display());
                }
            }
            for (mname, p) in &share.mount {
                if mname.is_empty() || mname.contains('/') {
                    anyhow::bail!("share {name} has invalid mount name {mname:?}");
                }
                if !p.is_absolute() {
                    anyhow::bail!(
                        "share {name} mount {mname} root {} must be absolute",
                        p.display()
                    );
                }
            }
        }
        Ok(())
    }

    pub fn is_hidden(&self, name: &str) -> bool {
        if self.options.hide_dotfiles && name.starts_with('.') {
            return true;
        }
        // Patterns are simple case-insensitive substrings for v1.
        let lower = name.to_lowercase();
        for pat in &self.options.hide_patterns {
            if lower.contains(&pat.to_lowercase()) {
                return true;
            }
        }
        false
    }
}
