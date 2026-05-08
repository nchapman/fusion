//! Build the in-memory tree by walking physical roots on startup.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::tree::{CachedAttrs, DirSources, FastMap, NodeKind, NodeId, Tree, ROOT_ID};

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
pub fn apply_snapshot(
    tree: &mut Tree,
    virtual_id: NodeId,
    snap: &DirSnapshot,
    config: &Config,
) {
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
                        apply_snapshot(tree, *child_id, sub, config);
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
                    empty_dir_kind(),
                    sub.attrs.clone(),
                ) {
                    apply_snapshot(tree, child_id, sub, config);
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
pub fn merge_snapshot(
    tree: &mut Tree,
    virtual_id: NodeId,
    snap: &DirSnapshot,
    config: &Config,
) {
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
                        empty_dir_kind(),
                        sub.attrs.clone(),
                    ) {
                        Some(id) => id,
                        None => continue,
                    }
                };
                merge_snapshot(tree, child_id, sub, config);
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

/// Build an empty directory NodeKind. Empty == trivially sorted, so
/// `add_child` on this dir will maintain the sort invariant. Bulk-build
/// paths (`scan_into`, `merge_into`) flip it back to unsorted with
/// `mark_unsorted` so they can append in O(1) and we sort once at the end.
fn empty_dir_kind() -> NodeKind {
    NodeKind::Directory {
        by_name: FastMap::default(),
        ordered: Vec::new(),
        sorted: true,
        subdir_count: 0,
        sources: DirSources::Synthetic,
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

    for (share_name, share) in &config.shares {
        let share_id = tree
            .add_child(
                ROOT_ID,
                share_name.clone(),
                empty_dir_kind(),
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
                empty_dir_kind(),
                CachedAttrs::synthetic_dir(),
            ) {
                jobs.push(ScanJob {
                    target_id: mount_id,
                    physical: root.clone(),
                    is_mount: true,
                    label: format!("{share_name}:mount:{mount_name}"),
                });
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

    for (vid, path, snap, _, label) in mounts.into_iter().chain(merges.into_iter()) {
        match snap {
            Some(s) => {
                info!(target=%label, root=%path.display(), "applying scan");
                merge_snapshot(&mut tree, vid, &s, config);
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
