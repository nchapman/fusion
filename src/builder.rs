//! Build the in-memory tree by walking physical roots on startup.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::tree::{CachedAttrs, DirSources, NodeKind, NodeId, Tree, ROOT_ID};

/// Cap on recursive descent during scans. Protects against symlink loops
/// when `follow_symlinks` is enabled.
const MAX_SCAN_DEPTH: usize = 64;

/// Stat a path, respecting `config.options.follow_symlinks`. Returns `None`
/// for entries we should skip (broken symlinks, or symlinks when follow is
/// disabled). Files and directories return their target metadata.
fn stat_entry(path: &Path, config: &Config) -> Option<std::fs::Metadata> {
    let lstat = std::fs::symlink_metadata(path).ok()?;
    if lstat.file_type().is_symlink() {
        if !config.options.follow_symlinks {
            debug!(path=%path.display(), "skipping symlink (follow_symlinks=false)");
            return None;
        }
        // Follow.
        std::fs::metadata(path).ok()
    } else {
        Some(lstat)
    }
}

/// In-memory snapshot of a physical directory subtree, produced by reading
/// the disk *without* holding the tree lock. The watcher's drainer reads the
/// snapshot on a blocking thread, then takes the write lock briefly to apply
/// it via `apply_snapshot`. This keeps NFS readers from stalling for the
/// duration of a multi-second disk walk.
#[derive(Debug)]
pub struct DirSnapshot {
    pub physical: PathBuf,
    pub attrs: CachedAttrs,
    pub children: HashMap<String, EntrySnapshot>,
}

#[derive(Debug)]
pub enum EntrySnapshot {
    File { path: PathBuf, attrs: CachedAttrs },
    Dir(Box<DirSnapshot>),
}

/// Read a physical directory tree into a `DirSnapshot`. Pure disk I/O — safe
/// to call from a `spawn_blocking` task without holding any lock. Returns
/// `None` if the directory itself doesn't exist (caller should treat that as
/// "directory deleted" and drop the corresponding source).
pub fn snapshot_dir(physical: &Path, config: &Config, depth: usize) -> Option<DirSnapshot> {
    if depth > MAX_SCAN_DEPTH {
        warn!(path=%physical.display(), "max scan depth exceeded; symlink loop?");
        return None;
    }
    let dir_md = std::fs::metadata(physical).ok()?;
    let attrs = CachedAttrs::from_metadata(&dir_md);

    let entries = match std::fs::read_dir(physical) {
        Ok(it) => it,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(path=%physical.display(), error=%e, "snapshot read_dir failed");
            return None;
        }
    };

    let mut children = HashMap::new();
    for entry in entries.flatten() {
        let name = match entry.file_name().into_string() {
            Ok(s) => s,
            Err(_) => continue,
        };
        if config.is_hidden(&name) {
            continue;
        }
        let path = entry.path();
        let Some(md) = stat_entry(&path, config) else { continue };
        if md.is_dir() {
            if let Some(sub) = snapshot_dir(&path, config, depth + 1) {
                children.insert(name, EntrySnapshot::Dir(Box::new(sub)));
            }
        } else if md.is_file() {
            let attrs = CachedAttrs::from_metadata(&md);
            children.insert(name, EntrySnapshot::File { path, attrs });
        }
    }

    Some(DirSnapshot {
        physical: physical.to_path_buf(),
        attrs,
        children,
    })
}

/// Apply a `DirSnapshot` to the in-memory tree. All work is in-memory — no
/// disk I/O. Caller holds the write lock. Mirrors the diff logic that
/// `rescan_dir` does inline, but without read_dir/metadata calls.
pub fn apply_snapshot(tree: &mut Tree, virtual_id: NodeId, snap: &DirSnapshot) {
    tree.extend_dir_sources(virtual_id, snap.physical.clone());

    // We only need the child ids to iterate; names + types come from
    // `tree.get(child_id)` per-step. Cloning the (String, NodeId) Vec
    // would burst thousands of String allocations under the write lock.
    let virtual_child_ids: Vec<NodeId> = match tree.get(virtual_id) {
        Some(node) => match &node.kind {
            NodeKind::Directory { ordered, .. } => {
                ordered.iter().map(|(_, id)| *id).collect()
            }
            _ => return,
        },
        None => return,
    };

    let mut handled_names: HashSet<String> = HashSet::with_capacity(virtual_child_ids.len());

    for child_id in &virtual_child_ids {
        let Some(child) = tree.get(*child_id) else { continue };
        let name = child.name.clone(); // single clone per child, not full Vec
        handled_names.insert(name.clone());
        match &child.kind {
            NodeKind::File { backing } => {
                if !backing.starts_with(&snap.physical) {
                    continue;
                }
                match snap.children.get(&name) {
                    Some(EntrySnapshot::File { path, attrs }) if path == backing => {
                        if let Some(node) = tree.get_mut(*child_id) {
                            node.attrs = attrs.clone();
                        }
                    }
                    _ => {
                        info!(name=%name, backing=%backing.display(), "removing stale file node");
                        tree.remove_recursive(*child_id);
                    }
                }
            }
            NodeKind::Directory { sources, .. } => {
                let child_phys = snap.physical.join(&name);
                let backed_here = match sources {
                    DirSources::Physical(paths) => paths.iter().any(|p| p == &child_phys),
                    DirSources::Synthetic => false,
                };
                if !backed_here {
                    continue;
                }
                match snap.children.get(&name) {
                    Some(EntrySnapshot::Dir(sub)) => {
                        apply_snapshot(tree, *child_id, sub);
                    }
                    _ => {
                        let now_empty = tree.drop_dir_source(*child_id, &child_phys);
                        if now_empty {
                            info!(name=%name, "removing stale directory node (no sources left)");
                            tree.remove_recursive(*child_id);
                        }
                    }
                }
            }
        }
    }

    for (name, entry) in &snap.children {
        if handled_names.contains(name) {
            continue;
        }
        match entry {
            EntrySnapshot::Dir(sub) => {
                if let Some(child_id) = tree.add_child(
                    virtual_id,
                    name.clone(),
                    NodeKind::empty_dir(),
                    sub.attrs.clone(),
                ) {
                    apply_snapshot(tree, child_id, sub);
                }
            }
            EntrySnapshot::File { path, attrs } => {
                let kind = NodeKind::File { backing: path.clone() };
                if let Some(child_id) =
                    tree.add_child(virtual_id, name.clone(), kind, attrs.clone())
                {
                    tree.index_file(path.clone(), child_id);
                }
            }
        }
    }
}

/// Merge a `DirSnapshot` into the tree with **additive, first-root-wins**
/// semantics. Used during initial build, where multiple `merge:` roots feed
/// the same virtual share and conflicts are resolved by config order.
///
/// Dir-name collision: if a dir of this name already exists, descend into
/// it (recursive merge of subtrees). File-name collision: keep the existing
/// (earlier) entry; log a warning unless the existing's backing is the same
/// path (which means we're re-applying our own work).
pub fn merge_snapshot(tree: &mut Tree, virtual_id: NodeId, snap: &DirSnapshot) {
    tree.extend_dir_sources(virtual_id, snap.physical.clone());
    tree.mark_unsorted(virtual_id);

    for (name, entry) in &snap.children {
        match entry {
            EntrySnapshot::Dir(sub) => {
                let existing = tree.child(virtual_id, name);
                let child_id = if let Some(eid) = existing {
                    if !tree.get(eid).map(|n| n.is_dir()).unwrap_or(false) {
                        warn!(name=%name, "directory shadowed by earlier file with same name");
                        continue;
                    }
                    eid
                } else {
                    match tree.add_child(
                        virtual_id,
                        name.clone(),
                        NodeKind::empty_dir(),
                        sub.attrs.clone(),
                    ) {
                        Some(id) => id,
                        None => continue,
                    }
                };
                merge_snapshot(tree, child_id, sub);
            }
            EntrySnapshot::File { path, attrs } => {
                let kind = NodeKind::File { backing: path.clone() };
                if let Some(child_id) =
                    tree.add_child(virtual_id, name.clone(), kind, attrs.clone())
                {
                    tree.index_file(path.clone(), child_id);
                } else {
                    let already_same = tree
                        .child(virtual_id, name)
                        .and_then(|cid| tree.get(cid))
                        .map(|n| matches!(&n.kind, NodeKind::File { backing } if backing == path))
                        .unwrap_or(false);
                    if !already_same {
                        warn!(
                            name = %name,
                            new_path = %path.display(),
                            "duplicate file shadowed by earlier root"
                        );
                    }
                }
            }
        }
    }
}

pub fn build(config: &Config, server_id: u64) -> Result<Tree> {
    let mut tree = Tree::new(server_id);

    // Phase 1: lay out share + mount virtual nodes (sequential, RAM-only).
    // We collect a flat job list of (target_virtual_id, physical_path,
    // is_mount) so the disk-bound phase can fan out without touching the
    // tree.
    struct ScanJob {
        target_id: NodeId,
        physical: PathBuf,
        is_mount: bool,
        label: String, // for logs
    }
    let mut jobs: Vec<ScanJob> = Vec::new();
    // Per-share mount names. The doc'd contract is that a mount shadows any
    // top-level merge entry of the same name (and the collision is logged).
    // Without this set the recursive merge would descend into the mount's
    // virtual dir and pollute it with files from the merge root.
    let mut mount_names_per_share: HashMap<NodeId, HashSet<String>> = HashMap::new();

    for (share_name, share) in &config.shares {
        let share_id = tree
            .add_child(
                ROOT_ID,
                share_name.clone(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .ok_or_else(|| anyhow::anyhow!("duplicate share name {share_name}"))?;
        info!(share = %share_name, id = share_id, "created share");

        // Mounts get virtual node first so their names take precedence over
        // any same-named entries in merge roots.
        for (mount_name, root) in &share.mount {
            if let Some(mount_id) = tree.add_child(
                share_id,
                mount_name.clone(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            ) {
                jobs.push(ScanJob {
                    target_id: mount_id,
                    physical: root.clone(),
                    is_mount: true,
                    label: format!("{share_name}:mount:{mount_name}"),
                });
                mount_names_per_share
                    .entry(share_id)
                    .or_default()
                    .insert(mount_name.clone());
            } else {
                warn!(share=%share_name, mount=%mount_name, "mount name conflicts; skipping");
            }
        }

        for root in &share.merge {
            if !root.exists() {
                warn!(share=%share_name, root=%root.display(), "merge root missing; skipping");
                continue;
            }
            jobs.push(ScanJob {
                target_id: share_id,
                physical: root.clone(),
                is_mount: false,
                label: format!("{share_name}:merge:{}", root.display()),
            });
        }
    }

    // Phase 2: snapshot every root in parallel. Each worker only needs
    // `&Config` and the physical path; no tree access, no lock. On spinning
    // disks this gives ≈N_disks speedup over the previous serial scan.
    let scan_start = std::time::Instant::now();
    let snapshots: Vec<(NodeId, PathBuf, Option<DirSnapshot>, bool, String)> =
        std::thread::scope(|s| {
            let cfg = config;
            let handles: Vec<_> = jobs
                .into_iter()
                .map(|j| {
                    s.spawn(move || {
                        let snap = snapshot_dir(&j.physical, cfg, 0);
                        (j.target_id, j.physical, snap, j.is_mount, j.label)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("scan worker panicked"))
                .collect()
        });
    let scan_elapsed = scan_start.elapsed();

    // Phase 3: apply snapshots sequentially. Mounts first (already won
    // their share-root slot in phase 1), then merge roots in config order
    // for first-root-wins on file conflicts.
    let (mounts, merges): (Vec<_>, Vec<_>) =
        snapshots.into_iter().partition(|(_, _, _, is_mount, _)| *is_mount);

    for (vid, path, snap, is_mount, label) in mounts.into_iter().chain(merges.into_iter()) {
        match snap {
            Some(mut s) => {
                if !is_mount {
                    if let Some(shadowed) = mount_names_per_share.get(&vid) {
                        s.children.retain(|name, _| {
                            if shadowed.contains(name) {
                                warn!(
                                    share_id = vid,
                                    name = %name,
                                    "merge entry shadowed by mount of same name"
                                );
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
                info!(target=%label, root=%path.display(), "applying scan");
                merge_snapshot(&mut tree, vid, &s);
            }
            None => warn!(target=%label, path=%path.display(), "scan returned no data; skipping"),
        }
    }

    tree.finalize_sort();
    info!(
        nodes = tree.node_count(),
        scan_ms = scan_elapsed.as_millis() as u64,
        "tree built"
    );
    Ok(tree)
}

// (`scan_into` / `merge_into` removed; both replaced by `snapshot_dir` +
// `merge_snapshot`. The single-phase `rescan_path` is also gone — the
// watcher uses `snapshot_dir` + `apply_snapshot` so disk I/O stays outside
// the tree write lock.)

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Options, ServerConfig, ShareConfig};
    use crate::tree::{NodeKind, ROOT_ID};
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::TempDir;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::File::create(path).unwrap();
    }

    fn cfg_for(shares: BTreeMap<String, ShareConfig>) -> Config {
        Config {
            server: ServerConfig::default(),
            shares,
            options: Options::default(),
        }
    }

    fn cfg_with_options(shares: BTreeMap<String, ShareConfig>, options: Options) -> Config {
        Config {
            server: ServerConfig::default(),
            shares,
            options,
        }
    }

    fn child_names(tree: &Tree, dir: NodeId) -> Vec<String> {
        match &tree.get(dir).unwrap().kind {
            NodeKind::Directory { ordered, .. } => {
                ordered.iter().map(|(n, _)| n.clone()).collect()
            }
            _ => panic!("not a dir"),
        }
    }

    fn file_backing(tree: &Tree, parent: NodeId, name: &str) -> PathBuf {
        let id = tree.child(parent, name).expect("child exists");
        match &tree.get(id).unwrap().kind {
            NodeKind::File { backing } => backing.clone(),
            _ => panic!("not a file"),
        }
    }

    // ---------- snapshot_dir ----------

    #[test]
    fn snapshot_dir_returns_none_for_missing_path() {
        let cfg = cfg_for(BTreeMap::new());
        let snap = snapshot_dir(Path::new("/nonexistent/definitely/not/here"), &cfg, 0);
        assert!(snap.is_none());
    }

    #[test]
    fn snapshot_dir_captures_files_and_subdirs() {
        let dir = TempDir::new().unwrap();
        touch(&dir.path().join("a.mkv"));
        touch(&dir.path().join("sub/b.mkv"));
        let snap = snapshot_dir(dir.path(), &cfg_for(BTreeMap::new()), 0).unwrap();
        assert!(snap.children.contains_key("a.mkv"));
        match snap.children.get("sub").unwrap() {
            EntrySnapshot::Dir(sub) => assert!(sub.children.contains_key("b.mkv")),
            _ => panic!("sub should be a dir"),
        }
    }

    #[test]
    fn snapshot_dir_filters_dotfiles() {
        let dir = TempDir::new().unwrap();
        touch(&dir.path().join("Movie.mkv"));
        touch(&dir.path().join(".DS_Store"));
        let snap = snapshot_dir(dir.path(), &cfg_for(BTreeMap::new()), 0).unwrap();
        assert!(snap.children.contains_key("Movie.mkv"));
        assert!(!snap.children.contains_key(".DS_Store"));
    }

    #[test]
    fn snapshot_dir_filters_hide_patterns() {
        let dir = TempDir::new().unwrap();
        touch(&dir.path().join("Movie.mkv"));
        touch(&dir.path().join("Thumbs.db"));
        let cfg = cfg_with_options(
            BTreeMap::new(),
            Options {
                hide_dotfiles: true,
                hide_patterns: vec!["thumbs.db".into()],
                follow_symlinks: false,
            },
        );
        let snap = snapshot_dir(dir.path(), &cfg, 0).unwrap();
        assert!(snap.children.contains_key("Movie.mkv"));
        assert!(!snap.children.contains_key("Thumbs.db"));
    }

    #[test]
    #[cfg(unix)]
    fn snapshot_dir_skips_symlinks_when_follow_disabled() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().unwrap();
        touch(&dir.path().join("real.mkv"));
        symlink("/etc/passwd", dir.path().join("evil")).unwrap();
        let snap = snapshot_dir(dir.path(), &cfg_for(BTreeMap::new()), 0).unwrap();
        assert!(snap.children.contains_key("real.mkv"));
        assert!(
            !snap.children.contains_key("evil"),
            "symlink must be skipped when follow_symlinks=false"
        );
    }

    #[test]
    #[cfg(unix)]
    fn snapshot_dir_follows_symlinks_when_enabled() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();
        touch(&target_dir.path().join("inside.mkv"));
        symlink(target_dir.path(), dir.path().join("link")).unwrap();
        let cfg = cfg_with_options(
            BTreeMap::new(),
            Options {
                hide_dotfiles: true,
                hide_patterns: vec![],
                follow_symlinks: true,
            },
        );
        let snap = snapshot_dir(dir.path(), &cfg, 0).unwrap();
        match snap.children.get("link") {
            Some(EntrySnapshot::Dir(sub)) => assert!(sub.children.contains_key("inside.mkv")),
            _ => panic!("symlinked dir should appear as a Dir entry"),
        }
    }

    // ---------- merge_snapshot (first-root-wins) ----------

    #[test]
    fn merge_snapshot_first_root_wins_on_file_conflict() {
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie.mkv"));
        touch(&r2.path().join("Movie.mkv"));

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "Movies".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();

        let s1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        let s2 = snapshot_dir(r2.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &s1);
        merge_snapshot(&mut tree, share, &s2);
        tree.finalize_sort();

        let backing = file_backing(&tree, share, "Movie.mkv");
        assert!(
            backing.starts_with(r1.path()),
            "first root must win: backing={}",
            backing.display()
        );
    }

    #[test]
    fn merge_snapshot_unions_directories_recursively() {
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Show/s1e1.mkv"));
        touch(&r2.path().join("Show/s1e2.mkv"));

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "TV".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();

        let s1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        let s2 = snapshot_dir(r2.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &s1);
        merge_snapshot(&mut tree, share, &s2);
        tree.finalize_sort();

        let show = tree.child(share, "Show").expect("Show present");
        let mut names = child_names(&tree, show);
        names.sort();
        assert_eq!(names, vec!["s1e1.mkv".to_string(), "s1e2.mkv".to_string()]);
    }

    // ---------- apply_snapshot (reconciling diff) ----------

    #[test]
    fn apply_snapshot_removes_deleted_files_and_adds_new_ones() {
        let root = TempDir::new().unwrap();
        touch(&root.path().join("keep.mkv"));
        touch(&root.path().join("gone.mkv"));

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "Movies".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let snap1 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &snap1);
        tree.finalize_sort();
        assert!(tree.child(share, "gone.mkv").is_some());

        // Mutate disk: delete one, add another.
        fs::remove_file(root.path().join("gone.mkv")).unwrap();
        touch(&root.path().join("new.mkv"));
        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot(&mut tree, share, &snap2);

        assert!(tree.child(share, "keep.mkv").is_some());
        assert!(tree.child(share, "gone.mkv").is_none());
        assert!(tree.child(share, "new.mkv").is_some());
    }

    // ---------- build() end-to-end ----------

    #[test]
    fn build_produces_share_with_merged_and_mounted_roots() {
        let m1 = TempDir::new().unwrap();
        let m2 = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        touch(&m1.path().join("A.mkv"));
        touch(&m2.path().join("B.mkv"));
        touch(&archive.path().join("Old.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Movies".to_string(),
            ShareConfig {
                merge: vec![m1.path().to_path_buf(), m2.path().to_path_buf()],
                mount: {
                    let mut m = BTreeMap::new();
                    m.insert("Archive".to_string(), archive.path().to_path_buf());
                    m
                },
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let movies = tree.child(ROOT_ID, "Movies").expect("Movies share");
        let mut names = child_names(&tree, movies);
        names.sort();
        assert_eq!(
            names,
            vec!["A.mkv".to_string(), "Archive".to_string(), "B.mkv".to_string()]
        );
        let archive_id = tree.child(movies, "Archive").unwrap();
        assert!(tree.child(archive_id, "Old.mkv").is_some());
    }

    #[test]
    fn build_mount_takes_precedence_over_merge_with_same_name() {
        // A merge root contains a directory called "Archive"; a mount also
        // named "Archive" should win and the merge entry should be ignored.
        let merge_root = TempDir::new().unwrap();
        touch(&merge_root.path().join("Archive/from_merge.mkv"));
        let mount_root = TempDir::new().unwrap();
        touch(&mount_root.path().join("from_mount.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Movies".to_string(),
            ShareConfig {
                merge: vec![merge_root.path().to_path_buf()],
                mount: {
                    let mut m = BTreeMap::new();
                    m.insert("Archive".to_string(), mount_root.path().to_path_buf());
                    m
                },
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let movies = tree.child(ROOT_ID, "Movies").unwrap();
        let archive = tree.child(movies, "Archive").expect("mount Archive");
        let names = child_names(&tree, archive);
        assert!(
            names.iter().any(|n| n == "from_mount.mkv"),
            "mount content should be present: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "from_merge.mkv"),
            "mount must shadow merge entry of same name: {names:?}"
        );
    }

    #[test]
    fn build_skips_missing_merge_root_with_warning() {
        let real = TempDir::new().unwrap();
        touch(&real.path().join("real.mkv"));
        let mut shares = BTreeMap::new();
        shares.insert(
            "Movies".to_string(),
            ShareConfig {
                merge: vec![
                    real.path().to_path_buf(),
                    PathBuf::from("/definitely/not/here"),
                ],
                mount: BTreeMap::new(),
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build should succeed despite missing root");
        let movies = tree.child(ROOT_ID, "Movies").unwrap();
        assert!(tree.child(movies, "real.mkv").is_some());
    }
}
