use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::Deserialize;
use tracing::warn;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(deserialize_with = "deserialize_shares")]
    pub shares: BTreeMap<String, ShareConfig>,
    #[serde(default)]
    pub options: Options,
    /// Compiled `options.hide_patterns`. Built once at load. Use `is_hidden`
    /// to query — the field is non-public so callers can't observe a `None`
    /// state mid-construction.
    #[serde(skip)]
    hide_set: Option<GlobSet>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Bind address for the RPC portmap responder. Clients like Infuse don't
    /// expose a port override and discover NFS via portmap on the well-known
    /// port 111. Default `0.0.0.0:111`; set to `null` (YAML) to disable.
    #[serde(default = "default_portmap_bind")]
    pub portmap_bind: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            portmap_bind: default_portmap_bind(),
        }
    }
}

fn default_bind() -> String {
    // Non-privileged port so first-run doesn't EACCES on Linux. Production
    // deployments that want 2049 set it explicitly (see README).
    "0.0.0.0:11111".to_string()
}

fn default_portmap_bind() -> Option<String> {
    Some("0.0.0.0:111".to_string())
}

#[derive(Debug, Default)]
pub struct ShareConfig {
    /// Roots that union into the share root.
    pub merge: Vec<PathBuf>,
    /// Named subdirectories of this share. Each subdir is itself a
    /// share-shaped config — it can have its own `merge` roots, its own
    /// `dedupe_depth`, and (recursively) further subdirs. The minimal form
    /// (a single path) is sugar for `{ merge: [path] }`.
    pub subdirs: BTreeMap<String, ShareConfig>,
    /// Folder-level dedupe depth for `merge` roots. `None` (default) recurses
    /// forever so directory trees fully union. `Some(N)` stops merging at
    /// depth N: a directory whose name was first claimed by an earlier root
    /// shadows later roots' copy of that directory entirely (subtree and all),
    /// matching the existing first-root-wins file-collision rule. Useful when
    /// the same logical item exists in multiple roots (e.g. resolution tiers)
    /// and recursive union would interleave their contents.
    ///
    /// Caveat: dedupe is enforced at *build time* (initial scan + full
    /// rescans), not in incremental watcher applies. If the winning root's
    /// copy of a deduped folder is later deleted from disk, the next
    /// watcher event from a losing root will promote that root's copy.
    /// Conversely, if the winning root *adds* a deduped folder name after
    /// a losing root already claimed it, the two roots' contents will
    /// interleave for that folder until the next full rescan. This is
    /// fine for the design intent (resolution tiers stay stable while all
    /// roots are present); document it for users with more dynamic libraries.
    pub dedupe_depth: Option<usize>,
}

/// User-facing share schema. Three forms accepted:
///   `Movies: /mnt/movies`                   → single merge root
///   `Movies: [/mnt/d1, /mnt/d2]`            → multiple merge roots
///   `Movies: { merge: [...], subdirs: {…} }` → full form
///
/// The schema is recursive: each value inside `subdirs` accepts the same
/// three forms, so a subdir can itself be a multi-root merge with its own
/// `dedupe_depth`.
///
/// `mount` is captured to produce a clear migration error — pre-rename
/// configs would otherwise silently parse as an empty share.
#[derive(Deserialize)]
#[serde(untagged)]
enum ShareSpec {
    Single(PathBuf),
    Many(Vec<PathBuf>),
    Full {
        #[serde(default)]
        merge: Vec<PathBuf>,
        #[serde(default)]
        subdirs: BTreeMap<String, ShareSpec>,
        #[serde(default)]
        dedupe_depth: Option<usize>,
        #[serde(default, rename = "mount")]
        mount_deprecated: Option<serde::de::IgnoredAny>,
    },
}

impl ShareSpec {
    fn into_config(self, share_name: &str) -> Result<ShareConfig> {
        match self {
            ShareSpec::Single(p) => Ok(ShareConfig {
                merge: vec![p],
                ..Default::default()
            }),
            ShareSpec::Many(v) => Ok(ShareConfig {
                merge: v,
                ..Default::default()
            }),
            ShareSpec::Full {
                mount_deprecated: Some(_),
                ..
            } => anyhow::bail!(
                "share {share_name}: `mount:` was renamed to `subdirs:`; \
                 update your config (see config.advanced.yaml)"
            ),
            ShareSpec::Full {
                merge,
                subdirs,
                dedupe_depth,
                ..
            } => {
                let subdirs = subdirs
                    .into_iter()
                    .map(|(k, v)| {
                        let qualified = format!("{share_name}/{k}");
                        v.into_config(&qualified).map(|cfg| (k, cfg))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                Ok(ShareConfig {
                    merge,
                    subdirs,
                    dedupe_depth,
                })
            }
        }
    }
}

fn deserialize_shares<'de, D>(d: D) -> std::result::Result<BTreeMap<String, ShareConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let raw: BTreeMap<String, ShareSpec> = BTreeMap::deserialize(d)?;
    raw.into_iter()
        .map(|(k, v)| {
            v.into_config(&k)
                .map(|cfg| (k, cfg))
                .map_err(D::Error::custom)
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    /// Globs (case-insensitive) matched against directory entry names. Default
    /// hides dotfiles and a few platform metadata files. Set to `[]` to show
    /// everything.
    #[serde(default = "default_hide_patterns")]
    pub hide_patterns: Vec<String>,
    /// If true, scan follows symbolic links and exposes their targets as
    /// regular files. Off by default because a symlink inside a media root
    /// to e.g. `/etc/passwd` would otherwise be served over NFS.
    #[serde(default)]
    pub follow_symlinks: bool,
    /// Periodic full rescan interval (e.g. `"24h"`, `"30m"`, `"0s"` to disable).
    /// Safety net behind notify in case events get dropped.
    ///
    /// Default 24h: media libraries change in clustered bursts and usually
    /// live on disks that spin down — rescanning more often wakes the disk
    /// for no benefit. Lower this if you're on flash and want stronger
    /// correctness guarantees.
    #[serde(default = "default_rescan_interval", with = "humantime_serde")]
    pub rescan_interval: Duration,
    /// Captures the pre-rename key so we can emit a guiding error rather than
    /// silently using the default 24h. `deny_unknown_fields` would also reject
    /// it, but with a generic "unknown field" message.
    #[serde(default, rename = "rescan_interval_secs")]
    pub(crate) rescan_interval_secs_deprecated: Option<serde::de::IgnoredAny>,
    /// Pre-rename key — `hide_dotfiles` was folded into `hide_patterns`.
    #[serde(default, rename = "hide_dotfiles")]
    pub(crate) hide_dotfiles_deprecated: Option<serde::de::IgnoredAny>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            hide_patterns: default_hide_patterns(),
            follow_symlinks: false,
            rescan_interval: default_rescan_interval(),
            rescan_interval_secs_deprecated: None,
            hide_dotfiles_deprecated: None,
        }
    }
}

fn default_hide_patterns() -> Vec<String> {
    vec![
        ".*".to_string(), // dotfiles (.DS_Store, .AppleDouble, ...)
        "Thumbs.db".to_string(),
        "@eaDir".to_string(), // Synology metadata dirs
    ]
}

fn default_rescan_interval() -> Duration {
    Duration::from_secs(86_400)
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config = serde_yml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.reject_deprecated_keys()?;
        // Canonicalize before validate: overlap checks and absolute-path checks
        // must run on resolved paths, not their pre-symlink-resolution form.
        cfg.canonicalize_roots();
        cfg.validate()?;
        cfg.warn_on_missing_roots();
        cfg.warn_on_unusual_share_options();
        cfg.hide_set = Some(build_hide_set(&cfg.options.hide_patterns)?);
        Ok(cfg)
    }

    fn reject_deprecated_keys(&self) -> Result<()> {
        if self.options.rescan_interval_secs_deprecated.is_some() {
            anyhow::bail!(
                "`options.rescan_interval_secs` was renamed to `rescan_interval` \
                 and now takes a humantime string (e.g. `\"24h\"`, `\"30m\"`, `\"0s\"`)"
            );
        }
        if self.options.hide_dotfiles_deprecated.is_some() {
            anyhow::bail!(
                "`options.hide_dotfiles` was removed; dotfiles are hidden by the \
                 default `hide_patterns: [\".*\"]`. Set `hide_patterns: []` to show them."
            );
        }
        Ok(())
    }

    /// Resolve symlinks in every root path. macOS reports FSEvents under the
    /// real (e.g. /private/tmp) path, so unless we canonicalize at load time,
    /// our `path_index` keys won't match the events we receive. Missing roots
    /// stay un-canonicalized — they're surfaced by `warn_on_missing_roots`.
    fn canonicalize_roots(&mut self) {
        fn walk(share: &mut ShareConfig) {
            for p in share.merge.iter_mut() {
                if let Ok(c) = p.canonicalize() {
                    *p = c;
                }
            }
            for sub in share.subdirs.values_mut() {
                walk(sub);
            }
        }
        for share in self.shares.values_mut() {
            walk(share);
        }
    }

    /// Loud-but-non-fatal warning for roots that don't exist on disk. A typo'd
    /// path used to silently produce an empty share that "ran but showed
    /// nothing"; this surfaces it. We don't `bail!` because disks legitimately
    /// come and go (USB media, network mounts) and the watcher will pick up
    /// the path when it appears.
    fn warn_on_missing_roots(&self) {
        fn walk(label: &str, share: &ShareConfig) {
            for p in &share.merge {
                if !p.exists() {
                    warn!(
                        share = label,
                        path = %p.display(),
                        "merge root does not exist; share will be empty until path appears"
                    );
                }
            }
            for (sname, sub) in &share.subdirs {
                walk(&format!("{label}/{sname}"), sub);
            }
        }
        for (name, share) in &self.shares {
            walk(name, share);
        }
    }

    /// Non-fatal warnings about share options that are well-formed but won't
    /// do anything useful. Kept separate from `validate` (which bails) and
    /// `warn_on_missing_roots` (which is about disk state).
    fn warn_on_unusual_share_options(&self) {
        fn walk(label: &str, share: &ShareConfig) {
            if share.dedupe_depth.is_some() && share.merge.len() < 2 {
                warn!(
                    share = label,
                    "dedupe_depth has no effect with fewer than two merge roots; ignoring"
                );
            }
            for (sname, sub) in &share.subdirs {
                walk(&format!("{label}/{sname}"), sub);
            }
        }
        for (name, share) in &self.shares {
            walk(name, share);
        }
    }

    fn validate(&self) -> Result<()> {
        if self.shares.is_empty() {
            anyhow::bail!("config has no shares");
        }
        fn walk(label: &str, share: &ShareConfig) -> Result<()> {
            if share.merge.is_empty() && share.subdirs.is_empty() {
                anyhow::bail!("share {label} has no merge or subdirs roots");
            }
            for p in &share.merge {
                if !p.is_absolute() {
                    anyhow::bail!("share {label} merge root {} must be absolute", p.display());
                }
            }
            if let Some(0) = share.dedupe_depth {
                anyhow::bail!(
                    "share {label} has dedupe_depth: 0; use 1 or higher \
                     (1 dedupes at the share's top-level folders)"
                );
            }
            // Reject overlapping merge roots within a share. The union is
            // ambiguous (which root "wins" a file inside the overlap?) and
            // the watcher would double-fire on every event under the inner
            // path. Subdirs are checked recursively in their own scope —
            // overlap *across* a share and one of its subdirs is allowed
            // (subdirs win at their slot by design).
            for (i, a) in share.merge.iter().enumerate() {
                for b in share.merge.iter().skip(i + 1) {
                    if a == b {
                        anyhow::bail!("share {label} merge root {} listed twice", a.display());
                    }
                    if a.starts_with(b) || b.starts_with(a) {
                        anyhow::bail!(
                            "share {label} merge roots overlap: {} and {}",
                            a.display(),
                            b.display()
                        );
                    }
                }
            }
            for (sname, sub) in &share.subdirs {
                if !is_valid_path_segment(sname) {
                    anyhow::bail!("share {label} has invalid subdir name {sname:?}");
                }
                walk(&format!("{label}/{sname}"), sub)?;
            }
            Ok(())
        }
        for (name, share) in &self.shares {
            if !is_valid_path_segment(name) {
                anyhow::bail!("invalid share name {name:?}");
            }
            walk(name, share)?;
        }
        Ok(())
    }

    pub fn is_hidden(&self, name: &str) -> bool {
        // `hide_set` is populated by `Config::load` / `from_parts`. A `None`
        // here means a `Config` was built by some other path (e.g. raw serde
        // deserialization in a future refactor) — better to fail loudly than
        // silently expose entries that should have been hidden over NFS.
        self.hide_set
            .as_ref()
            .expect("hide_set not built; construct Config via load() or from_parts()")
            .is_match(name)
    }
}

fn build_hide_set(patterns: &[String]) -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for pat in patterns {
        let glob = GlobBuilder::new(pat)
            .case_insensitive(true)
            .literal_separator(true)
            .build()
            .with_context(|| format!("invalid hide pattern {pat:?}"))?;
        b.add(glob);
    }
    b.build().context("building hide pattern set")
}

/// Names that become directory entries served over NFS. Reject empty,
/// path-component characters, control chars, and surrounding whitespace
/// — all of which produce surprising client-side behaviour.
fn is_valid_path_segment(s: &str) -> bool {
    !s.is_empty() && !s.contains('/') && s.trim() == s && !s.chars().any(|c| c.is_control())
}

impl Config {
    /// Construct a `Config` directly, compiling the hide-pattern set. Used by
    /// tests and benchmarks that bypass `Config::load`. Production code paths
    /// should use `Config::load`. Returns `Err` if any hide pattern is an
    /// invalid glob.
    pub fn from_parts(
        server: ServerConfig,
        shares: BTreeMap<String, ShareConfig>,
        options: Options,
    ) -> Result<Self> {
        let hide_set = build_hide_set(&options.hide_patterns)?;
        Ok(Self {
            server,
            shares,
            options,
            hide_set: Some(hide_set),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(shares: BTreeMap<String, ShareConfig>, options: Options) -> Config {
        Config::from_parts(ServerConfig::default(), shares, options).expect("test config")
    }

    fn share(merge: &[&str], subdirs: &[(&str, &str)]) -> ShareConfig {
        ShareConfig {
            merge: merge.iter().map(PathBuf::from).collect(),
            subdirs: subdirs
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        ShareConfig {
                            merge: vec![PathBuf::from(v)],
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            dedupe_depth: None,
        }
    }

    fn one_share(name: &str, s: ShareConfig) -> BTreeMap<String, ShareConfig> {
        let mut m = BTreeMap::new();
        m.insert(name.to_string(), s);
        m
    }

    #[test]
    fn defaults_skip_symlinks_and_hide_dotfiles_by_default() {
        let opts = Options::default();
        assert!(!opts.follow_symlinks);
        assert!(opts.hide_patterns.iter().any(|p| p == ".*"));
    }

    #[test]
    fn default_bind_is_unprivileged_port() {
        // Default must be unprivileged so first-run doesn't fail with EACCES.
        assert_eq!(ServerConfig::default().bind, "0.0.0.0:11111");
    }

    #[test]
    fn default_portmap_bind_is_111() {
        // Clients without a port override (e.g. Infuse) discover NFS via
        // portmap on 111; default must match the well-known port.
        assert_eq!(
            ServerConfig::default().portmap_bind.as_deref(),
            Some("0.0.0.0:111")
        );
    }

    #[test]
    fn portmap_bind_can_be_disabled_via_null() {
        let yaml = "server:\n  bind: 0.0.0.0:2049\n  portmap_bind: ~\nshares:\n  M: /tmp\n";
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.yaml");
        std::fs::write(&p, yaml).unwrap();
        let cfg = Config::load(&p).unwrap();
        assert_eq!(cfg.server.portmap_bind, None);
    }

    #[test]
    fn is_hidden_dotfiles_by_default() {
        let cfg = cfg_with(BTreeMap::new(), Options::default());
        assert!(cfg.is_hidden(".DS_Store"));
        assert!(cfg.is_hidden(".hidden"));
        assert!(!cfg.is_hidden("Movie.mkv"));
    }

    #[test]
    fn is_hidden_can_be_disabled_with_empty_patterns() {
        let cfg = cfg_with(
            BTreeMap::new(),
            Options {
                hide_patterns: vec![],
                ..Options::default()
            },
        );
        assert!(!cfg.is_hidden(".DS_Store"));
    }

    #[test]
    fn is_hidden_globs_are_case_insensitive() {
        let cfg = cfg_with(
            BTreeMap::new(),
            Options {
                hide_patterns: vec!["thumbs.db".into(), "@eaDir".into(), "*.tmp".into()],
                ..Options::default()
            },
        );
        assert!(cfg.is_hidden("Thumbs.db"));
        assert!(cfg.is_hidden("THUMBS.DB"));
        assert!(cfg.is_hidden("@eadir"));
        assert!(cfg.is_hidden("scratch.tmp"));
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
        assert_validate_err(&cfg, "no merge or subdirs");
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
    fn validate_rejects_relative_subdir_root() {
        let cfg = cfg_with(
            one_share("Movies", share(&[], &[("Archive", "rel")])),
            Options::default(),
        );
        assert_validate_err(&cfg, "absolute");
    }

    #[test]
    fn validate_rejects_subdir_name_with_slash() {
        let cfg = cfg_with(
            one_share("Movies", share(&[], &[("a/b", "/m")])),
            Options::default(),
        );
        assert_validate_err(&cfg, "invalid subdir name");
    }

    #[test]
    fn validate_accepts_subdir_only_share() {
        let cfg = cfg_with(
            one_share("Movies", share(&[], &[("Archive", "/m")])),
            Options::default(),
        );
        cfg.validate().expect("subdir-only should be valid");
    }

    #[test]
    fn validate_rejects_overlapping_merge_roots() {
        // /a and /a/b within the same share would double-fire watcher events
        // and have ambiguous union semantics.
        let cfg = cfg_with(
            one_share("Movies", share(&["/a", "/a/b"], &[])),
            Options::default(),
        );
        assert_validate_err(&cfg, "overlap");
    }

    #[test]
    fn validate_rejects_duplicate_merge_roots() {
        let cfg = cfg_with(
            one_share("Movies", share(&["/a", "/a"], &[])),
            Options::default(),
        );
        assert_validate_err(&cfg, "twice");
    }

    #[test]
    fn load_parses_minimal_yaml_with_string_shorthand() {
        // The killer DX feature: one-line share config.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "shares:\n  Movies: /tmp\n").unwrap();
        let cfg = Config::load(&path).expect("load");
        let movies = cfg.shares.get("Movies").expect("share Movies");
        assert_eq!(movies.merge.len(), 1);
        assert!(movies.subdirs.is_empty());
    }

    #[test]
    fn load_parses_list_shorthand() {
        let dir = tempfile::tempdir().unwrap();
        let d1 = dir.path().join("d1");
        let d2 = dir.path().join("d2");
        std::fs::create_dir_all(&d1).unwrap();
        std::fs::create_dir_all(&d2).unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            format!(
                "shares:\n  Movies:\n    - {}\n    - {}\n",
                d1.display(),
                d2.display()
            ),
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.shares["Movies"].merge.len(), 2);
    }

    #[test]
    fn load_parses_full_form() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  Movies:\n    merge:\n      - /tmp\n    subdirs:\n      Archive: /tmp\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.shares["Movies"].merge.len(), 1);
        assert!(cfg.shares["Movies"].subdirs.contains_key("Archive"));
    }

    #[test]
    fn load_parses_humantime_rescan_interval() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  Movies: /tmp\noptions:\n  rescan_interval: 30m\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.options.rescan_interval, Duration::from_secs(1800));
    }

    /// Pre-rename configs must fail with a guiding error, not silently parse
    /// as an empty share / default interval.
    fn assert_load_err(yaml: &str, needle: &str) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, yaml).unwrap();
        let err = Config::load(&path).expect_err("expected load to fail");
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains(&needle.to_lowercase()),
            "error {msg:?} should mention {needle:?}"
        );
    }

    #[test]
    fn load_rejects_deprecated_mount_key() {
        assert_load_err(
            "shares:\n  Movies:\n    mount:\n      Archive: /tmp\n",
            "subdirs",
        );
    }

    #[test]
    fn load_rejects_deprecated_rescan_interval_secs_key() {
        assert_load_err(
            "shares:\n  Movies: /tmp\noptions:\n  rescan_interval_secs: 60\n",
            "rescan_interval",
        );
    }

    #[test]
    fn load_rejects_deprecated_hide_dotfiles_key() {
        assert_load_err(
            "shares:\n  Movies: /tmp\noptions:\n  hide_dotfiles: false\n",
            "hide_patterns",
        );
    }

    #[test]
    fn load_rejects_unknown_options_key() {
        assert_load_err(
            "shares:\n  Movies: /tmp\noptions:\n  totally_made_up: 1\n",
            "unknown",
        );
    }

    #[test]
    fn load_parses_dedupe_depth() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  Movies:\n    merge:\n      - /tmp\n    dedupe_depth: 1\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.shares["Movies"].dedupe_depth, Some(1));
    }

    #[test]
    fn load_parses_subdir_with_merge_and_dedupe_depth() {
        // The Infuse-friendly shape: an outer wrapper share whose subdir
        // is itself a multi-root merge with folder-level dedupe.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  \
             Library:\n    \
             subdirs:\n      \
             Movies:\n        \
             merge:\n          \
             - /tmp/a\n          \
             - /tmp/b\n        \
             dedupe_depth: 1\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        let library = &cfg.shares["Library"];
        assert!(library.merge.is_empty());
        let movies = &library.subdirs["Movies"];
        assert_eq!(movies.merge.len(), 2);
        assert_eq!(movies.dedupe_depth, Some(1));
    }

    #[test]
    fn load_parses_subdir_path_shorthand() {
        // The legacy shorthand — a bare path under `subdirs:` — must still
        // parse to a single-merge ShareConfig.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  Library:\n    subdirs:\n      TV: /tmp/tv\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        let tv = &cfg.shares["Library"].subdirs["TV"];
        assert_eq!(tv.merge, vec![PathBuf::from("/tmp/tv")]);
        assert!(tv.subdirs.is_empty());
        assert_eq!(tv.dedupe_depth, None);
    }

    #[test]
    fn load_parses_subdir_list_shorthand() {
        // List form `Movies: [/p1, /p2]` works inside subdirs same as it
        // does at the top level, sugar for `{ merge: [/p1, /p2] }`.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  Library:\n    subdirs:\n      Movies:\n        - /tmp/a\n        - /tmp/b\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        let movies = &cfg.shares["Library"].subdirs["Movies"];
        assert_eq!(
            movies.merge,
            vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]
        );
        assert_eq!(movies.dedupe_depth, None);
    }

    #[test]
    fn load_parses_three_level_nested_subdirs() {
        // No depth limit on `subdirs:` recursion — verify three levels
        // round-trip cleanly.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "shares:\n  L1:\n    subdirs:\n      L2:\n        subdirs:\n          L3: /tmp/leaf\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        let l3 = &cfg.shares["L1"].subdirs["L2"].subdirs["L3"];
        assert_eq!(l3.merge, vec![PathBuf::from("/tmp/leaf")]);
    }

    #[test]
    fn validate_rejects_dedupe_depth_zero_on_nested_subdir() {
        // The dedupe-depth-0 ban applies recursively, and the error must
        // identify the nested location with `parent/child` notation so
        // users can find the offending key.
        assert_load_err(
            "shares:\n  Library:\n    subdirs:\n      Movies:\n        merge:\n          - /tmp/a\n        dedupe_depth: 0\n",
            "library/movies",
        );
    }

    #[test]
    fn validate_rejects_empty_nested_subdir() {
        // A subdir with neither `merge:` nor its own `subdirs:` is empty
        // and almost certainly a mistake — surface it loudly.
        assert_load_err(
            "shares:\n  Library:\n    subdirs:\n      Movies: {}\n",
            "library/movies",
        );
    }

    #[test]
    fn validate_rejects_invalid_subdir_name_at_depth() {
        // Subdir names are path components served over NFS. Names with
        // slashes break readdir/lookup; reject at any depth, not just the
        // top level.
        assert_load_err(
            "shares:\n  Library:\n    subdirs:\n      \"bad/name\":\n        merge: [/tmp/a]\n",
            "bad/name",
        );
    }

    #[test]
    fn validate_rejects_overlapping_merge_roots_in_nested_subdir() {
        // Overlap detection must run recursively. The watcher would
        // double-fire under the inner path otherwise.
        assert_load_err(
            "shares:\n  Library:\n    subdirs:\n      Movies:\n        merge:\n          - /tmp/parent\n          - /tmp/parent/child\n",
            "overlap",
        );
    }

    #[test]
    fn validate_accepts_nested_relative_path_at_top_but_rejects_at_depth() {
        // Absolute-path requirement applies recursively. Use a relative
        // path under a nested subdir and confirm the validator catches it.
        assert_load_err(
            "shares:\n  Library:\n    subdirs:\n      Movies:\n        merge:\n          - relative/path\n",
            "must be absolute",
        );
    }

    #[test]
    fn validate_accepts_subdir_only_share_with_no_top_level_merge() {
        // The Infuse-friendly shape: outer share has no `merge:`, only
        // `subdirs:`. Must validate cleanly (this used to be the case for
        // the top-level path-only `subdirs` form too).
        let yaml = "shares:\n  \
                    Library:\n    \
                    subdirs:\n      \
                    Movies: /tmp\n      \
                    TV: /tmp/tv\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.yaml");
        std::fs::write(&path, yaml).unwrap();
        Config::load(&path).expect("load should succeed");
    }

    #[test]
    fn dedupe_depth_attaches_to_the_correct_nesting_level() {
        // Outer share has dedupe_depth: 2; a subdir has dedupe_depth: 1;
        // an inner-inner subdir has none. Each must round-trip to its own
        // ShareConfig, not bleed across levels.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.yaml");
        std::fs::write(
            &path,
            "shares:\n  Outer:\n    merge: [/tmp/o1, /tmp/o2]\n    dedupe_depth: 2\n    subdirs:\n      Mid:\n        merge: [/tmp/m1, /tmp/m2]\n        dedupe_depth: 1\n        subdirs:\n          Leaf: /tmp/leaf\n",
        )
        .unwrap();
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.shares["Outer"].dedupe_depth, Some(2));
        assert_eq!(cfg.shares["Outer"].subdirs["Mid"].dedupe_depth, Some(1));
        assert_eq!(
            cfg.shares["Outer"].subdirs["Mid"].subdirs["Leaf"].dedupe_depth,
            None
        );
    }

    #[test]
    fn validate_rejects_dedupe_depth_zero() {
        // Depth 0 would dedupe at the share root itself, which makes the
        // second root contribute nothing — almost certainly a config error.
        let mut s = share(&["/a", "/b"], &[]);
        s.dedupe_depth = Some(0);
        let cfg = cfg_with(one_share("Movies", s), Options::default());
        assert_validate_err(&cfg, "dedupe_depth");
    }

    #[test]
    fn load_default_bind_is_unprivileged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "shares:\n  Movies: /tmp\n").unwrap();
        let cfg = Config::load(&path).expect("load");
        assert_eq!(cfg.server.bind, "0.0.0.0:11111");
    }
}
