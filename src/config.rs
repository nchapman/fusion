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
        Self {
            bind: default_bind(),
        }
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
    /// Periodic full rescan interval, in seconds. Safety net behind notify
    /// in case events get dropped (kernel queue saturation, debouncer bugs,
    /// FUSE/SMB mounts that don't surface events at all). 0 disables.
    ///
    /// Default 86400 (24h): media libraries change in clustered bursts and
    /// usually live on disks that spin down — rescanning more often wakes
    /// the disk for no benefit. Lower this if you're on flash and want
    /// stronger correctness guarantees.
    #[serde(default = "default_rescan_interval")]
    pub rescan_interval_secs: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            hide_dotfiles: true,
            hide_patterns: Vec::new(),
            follow_symlinks: false,
            rescan_interval_secs: default_rescan_interval(),
        }
    }
}

fn default_rescan_interval() -> u64 {
    86_400
}

fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config = serde_yml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()?;
        cfg.canonicalize_roots();
        // Pre-lowercase hide patterns so `is_hidden` (called once per
        // directory entry on every scan) doesn't re-allocate per call.
        for pat in &mut cfg.options.hide_patterns {
            *pat = pat.to_lowercase();
        }
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
            if !is_valid_path_segment(name) {
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
                if !is_valid_path_segment(mname) {
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
        // Patterns are pre-lowercased in `Config::load`.
        let lower = name.to_lowercase();
        for pat in &self.options.hide_patterns {
            if lower.contains(pat) {
                return true;
            }
        }
        false
    }
}

/// Names that become directory entries served over NFS. Reject empty,
/// path-component characters, control chars, and surrounding whitespace
/// — all of which produce surprising client-side behaviour.
fn is_valid_path_segment(s: &str) -> bool {
    !s.is_empty() && !s.contains('/') && s.trim() == s && !s.chars().any(|c| c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(shares: BTreeMap<String, ShareConfig>, options: Options) -> Config {
        Config {
            server: ServerConfig::default(),
            shares,
            options,
        }
    }

    fn share(merge: &[&str], mount: &[(&str, &str)]) -> ShareConfig {
        ShareConfig {
            merge: merge.iter().map(PathBuf::from).collect(),
            mount: mount
                .iter()
                .map(|(k, v)| (k.to_string(), PathBuf::from(v)))
                .collect(),
        }
    }

    fn one_share(name: &str, s: ShareConfig) -> BTreeMap<String, ShareConfig> {
        let mut m = BTreeMap::new();
        m.insert(name.to_string(), s);
        m
    }

    #[test]
    fn defaults_hide_dotfiles_and_skip_symlinks() {
        let opts = Options::default();
        assert!(opts.hide_dotfiles);
        assert!(!opts.follow_symlinks);
        assert!(opts.hide_patterns.is_empty());
    }

    #[test]
    fn default_bind_is_all_interfaces_2049() {
        assert_eq!(ServerConfig::default().bind, "0.0.0.0:2049");
    }

    #[test]
    fn is_hidden_dotfiles() {
        let cfg = cfg_with(BTreeMap::new(), Options::default());
        assert!(cfg.is_hidden(".DS_Store"));
        assert!(cfg.is_hidden(".hidden"));
        assert!(!cfg.is_hidden("Movie.mkv"));
    }

    #[test]
    fn is_hidden_dotfiles_disabled() {
        let cfg = cfg_with(
            BTreeMap::new(),
            Options {
                hide_dotfiles: false,
                ..Options::default()
            },
        );
        assert!(!cfg.is_hidden(".DS_Store"));
    }

    #[test]
    fn is_hidden_patterns_are_case_insensitive_substrings() {
        let cfg = cfg_with(
            BTreeMap::new(),
            Options {
                hide_dotfiles: false,
                // Patterns are pre-lowercased by `Config::load`; tests
                // constructing `Config` directly must match that contract.
                hide_patterns: vec!["thumbs.db".into(), "@eadir".into()],
                ..Options::default()
            },
        );
        assert!(cfg.is_hidden("Thumbs.db"));
        assert!(cfg.is_hidden("THUMBS.DB"));
        assert!(cfg.is_hidden("My@eadirCache"));
        assert!(!cfg.is_hidden("Movie.mkv"));
    }

    /// Assert `validate()` fails and the error message contains `needle`
    /// (case-insensitive). Naming the specific constraint that fired prevents
    /// a regression where the wrong rule rejects an input.
    fn assert_validate_err(cfg: &Config, needle: &str) {
        let err = cfg.validate().expect_err("expected validation error");
        let msg = format!("{err}").to_lowercase();
        assert!(
            msg.contains(&needle.to_lowercase()),
            "error message {msg:?} should mention {needle:?}"
        );
    }

    #[test]
    fn validate_rejects_empty_shares_map() {
        let cfg = cfg_with(BTreeMap::new(), Options::default());
        assert_validate_err(&cfg, "no shares");
    }

    #[test]
    fn validate_rejects_share_with_no_sources() {
        let cfg = cfg_with(one_share("Movies", share(&[], &[])), Options::default());
        assert_validate_err(&cfg, "no merge or mount");
    }

    #[test]
    fn validate_rejects_share_name_with_slash() {
        let cfg = cfg_with(one_share("a/b", share(&["/m"], &[])), Options::default());
        assert_validate_err(&cfg, "invalid share name");
    }

    #[test]
    fn validate_rejects_relative_merge_root() {
        let cfg = cfg_with(
            one_share("Movies", share(&["relative/path"], &[])),
            Options::default(),
        );
        assert_validate_err(&cfg, "absolute");
    }

    #[test]
    fn validate_rejects_relative_mount_root() {
        let cfg = cfg_with(
            one_share("Movies", share(&[], &[("Archive", "rel")])),
            Options::default(),
        );
        assert_validate_err(&cfg, "absolute");
    }

    #[test]
    fn validate_rejects_mount_name_with_slash() {
        let cfg = cfg_with(
            one_share("Movies", share(&[], &[("a/b", "/m")])),
            Options::default(),
        );
        assert_validate_err(&cfg, "invalid mount name");
    }

    #[test]
    fn validate_accepts_mount_only_share() {
        let cfg = cfg_with(
            one_share("Movies", share(&[], &[("Archive", "/m")])),
            Options::default(),
        );
        cfg.validate().expect("mount-only should be valid");
    }

    #[test]
    fn load_parses_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "shares:\n  Movies:\n    merge:\n      - /tmp\n").unwrap();
        let cfg = Config::load(&path).expect("load");
        assert!(cfg.shares.contains_key("Movies"));
        assert_eq!(cfg.server.bind, "0.0.0.0:2049");
        assert!(cfg.options.hide_dotfiles);
    }
}
