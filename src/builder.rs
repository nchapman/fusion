//! Build the in-memory tree by walking physical roots on startup.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::Result;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::tree::{
    CachedAttrs, DirSources, NodeId, NodeKind, ShadowDir, ShadowFile, Tree, ROOT_ID,
};

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
        let Some(md) = stat_entry(&path, config) else {
            continue;
        };
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

/// A directory-shadow promotion the apply phase wants to install but
/// has deferred so its `snapshot_dir` disk read can happen *outside*
/// the tree write lock. The drainer re-acquires the lock and calls
/// `install_pending_promotions` to finish them.
#[derive(Debug)]
pub struct PendingPromotion {
    pub parent: NodeId,
    pub name: String,
    pub shadow: ShadowDir,
}

/// Apply a `DirSnapshot` to the in-memory tree. RAM-only: the snapshot
/// has already been read from disk by the caller, and any directory-shadow
/// promotions needed to keep the cache always-correct are *deferred* into
/// `pending` rather than executed inline (since they would otherwise
/// require `snapshot_dir` disk reads under the write lock). The drainer
/// runs `install_pending_promotions` after releasing the lock.
///
/// File-shadow promotions are not deferred — installing them is RAM-only
/// (the loser's attrs were captured at the time of shadowing).
///
/// `root_priority` is the index of the source root in its share's `merge`
/// list (lower = higher precedence). It controls two things:
///   1. when a winner disappears, the highest-priority shadow at that name
///      is promoted in its place;
///   2. when a name appears in this snapshot but is already taken, the
///      existing winner is demoted (becomes a shadow) iff the incoming
///      root has higher priority than the current owner.
///
/// `config` is plumbed through for parity with promotion install paths
/// even though `apply_snapshot` itself does no disk I/O — keeps the
/// signature stable across the inline and deferred call sites.
pub fn apply_snapshot(
    tree: &mut Tree,
    virtual_id: NodeId,
    snap: &DirSnapshot,
    root_priority: usize,
    config: &Config,
    pending: &mut Vec<PendingPromotion>,
) {
    tree.extend_dir_sources(virtual_id, snap.physical.clone());

    // Refresh the virtual dir's own attrs so getattr reflects the latest
    // on-disk mtime/ctime (clients use this as a dentry-cache freshness key).
    if let Some(node) = tree.get_mut(virtual_id) {
        node.attrs = snap.attrs.clone();
    }

    // We only need the child ids to iterate; names + types come from
    // `tree.get(child_id)` per-step. Cloning the (String, NodeId) Vec
    // would burst thousands of String allocations under the write lock.
    let virtual_child_ids: Vec<NodeId> = match tree.get(virtual_id) {
        Some(node) => match &node.kind {
            NodeKind::Directory { ordered, .. } => ordered.iter().map(|(_, id)| *id).collect(),
            _ => return,
        },
        None => return,
    };

    let mut handled_names: HashSet<String> = HashSet::with_capacity(virtual_child_ids.len());
    // Tracks only the in-place attrs-changed case; add/remove paths bump the
    // parent's mtime themselves inside `Tree::add_child` / `remove_recursive`.
    let mut attrs_changed_in_place = false;

    for child_id in &virtual_child_ids {
        let Some(child) = tree.get(*child_id) else {
            continue;
        };
        let name = child.name.clone(); // single clone per child, not full Vec
        match &child.kind {
            NodeKind::File { backing } => {
                // A file "belongs" to this snapshot only if its backing lives
                // immediately inside `snap.physical`. `starts_with` alone
                // would also match files in nested subdir roots (e.g. a deeper
                // subdir whose path is a sub-path of this merge root), causing
                // their nodes to be incorrectly removed during this rescan.
                if backing.parent() != Some(snap.physical.as_path()) {
                    // Another merge root owns this file. We deliberately
                    // leave `name` out of `handled_names`: if the current
                    // snap also has an entry by this name, the add-new pass
                    // will hit `add_child`'s dup-name rejection (returns
                    // `None`) and the cross-root file stays untouched.
                    continue;
                }
                match snap.children.get(&name) {
                    Some(EntrySnapshot::File { path, attrs }) if path == backing => {
                        handled_names.insert(name.clone());
                        if let Some(node) = tree.get_mut(*child_id) {
                            // Patch attrs in place (size/mtime/mode may have
                            // changed). Bump the parent dir's mtime below so
                            // Linux NFS clients revalidate the dentry cache
                            // even for in-place file replacement (including
                            // mode-only changes from `chmod`, which would
                            // otherwise be invisible until the periodic
                            // rescan).
                            let attrs_changed = node.attrs.differs_visibly(attrs);
                            node.attrs = attrs.clone();
                            if attrs_changed {
                                attrs_changed_in_place = true;
                            }
                        }
                    }
                    _ => {
                        // The file is gone, OR the disk entry of this name is
                        // now a directory (file→dir collision). Remove and
                        // leave the name unhandled so the add-new pass below
                        // can replace it with the new entry type.
                        info!(name=%name, backing=%backing.display(), "removing stale file node");
                        tree.remove_recursive(*child_id);
                        // Always-correct cache: a losing root may have been
                        // shadowing this name. Promote it now so the user
                        // sees the loser without waiting for the periodic
                        // rescan. Marks the name handled so the add-new
                        // pass won't double-install.
                        if try_promote_shadow(tree, virtual_id, &name, pending) {
                            handled_names.insert(name.clone());
                        }
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
                        handled_names.insert(name.clone());
                        // Capture only the visible attr fields (24 bytes,
                        // not a 56-byte CachedAttrs clone) before recursing:
                        // the recursive call updates the child dir's own
                        // attrs but doesn't surface mode-only changes back
                        // here. Without this comparison, `chmod 700 child/`
                        // wouldn't bump our parent mtime and Linux NFS
                        // clients would skip readdir revalidation.
                        let old_visible = tree
                            .get(*child_id)
                            .map(|n| (n.attrs.size, n.attrs.mtime, n.attrs.mode));
                        apply_snapshot(tree, *child_id, sub, root_priority, config, pending);
                        if let (Some(old), Some(node)) = (old_visible, tree.get(*child_id)) {
                            let new = (node.attrs.size, node.attrs.mtime, node.attrs.mode);
                            if old != new {
                                attrs_changed_in_place = true;
                            }
                        }
                    }
                    _ => {
                        // Disk no longer has a directory at this name — drop
                        // our source. If other physical roots still back this
                        // virtual dir, it survives and shadows whatever new
                        // entry the snap may have for this name (so we mark
                        // it handled). If we were the last source, the dir
                        // is removed and the name is left unhandled so the
                        // add-new pass can install the replacement (handles
                        // the dir→file collision case).
                        let now_empty = tree.drop_dir_source(*child_id, &child_phys);
                        if now_empty {
                            info!(name=%name, "removing stale directory node (no sources left)");
                            tree.remove_recursive(*child_id);
                            // Promote a shadowed loser if any (mirrors the
                            // file-removal branch above).
                            if try_promote_shadow(tree, virtual_id, &name, pending) {
                                handled_names.insert(name.clone());
                            }
                        } else {
                            handled_names.insert(name.clone());
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
                    tree.set_winner_priority(child_id, Some(root_priority));
                    apply_snapshot(tree, child_id, sub, root_priority, config, pending);
                } else {
                    // Name already owned. Compare priorities: if we outrank
                    // the current owner, demote them and install ourselves.
                    // Otherwise, record ourselves as a shadow so the user
                    // gets us if the winner later disappears.
                    handle_dir_collision(
                        tree,
                        virtual_id,
                        name,
                        sub,
                        root_priority,
                        config,
                        pending,
                    );
                }
            }
            EntrySnapshot::File { path, attrs } => {
                let kind = NodeKind::File {
                    backing: path.clone(),
                };
                if let Some(child_id) =
                    tree.add_child(virtual_id, name.clone(), kind, attrs.clone())
                {
                    tree.index_file(path.clone(), child_id);
                    tree.set_winner_priority(child_id, Some(root_priority));
                } else {
                    handle_file_collision(
                        tree,
                        virtual_id,
                        name,
                        path,
                        attrs,
                        root_priority,
                        config,
                    );
                }
            }
        }
    }

    // Prune dead shadows owned by this root: if a name we previously
    // recorded as a shadow at our priority is no longer in our snapshot,
    // the loser-root copy was deleted while shadowed. Drop the entry now;
    // otherwise it would accumulate forever and be discovered only when
    // the current winner is removed (where `try_promote_shadow` would
    // pop dead shadows one at a time, paying a `snapshot_dir` per dead
    // entry). Snapshot child names into a Vec to avoid borrowing the
    // shadow maps while mutating them.
    let dead_shadow_names: Vec<String> = match tree.get(virtual_id).map(|n| &n.kind) {
        Some(NodeKind::Directory { shadows, .. }) => match shadows.as_deref() {
            Some(s) => s
                .files
                .iter()
                .filter(|(name, list)| {
                    !snap.children.contains_key(*name)
                        && list.iter().any(|x| x.priority == root_priority)
                })
                .map(|(name, _)| name.clone())
                .chain(
                    s.dirs
                        .iter()
                        .filter(|(name, list)| {
                            !snap.children.contains_key(*name)
                                && list.iter().any(|x| x.priority == root_priority)
                        })
                        .map(|(name, _)| name.clone()),
                )
                .collect(),
            None => Vec::new(),
        },
        _ => Vec::new(),
    };
    for name in dead_shadow_names {
        tree.remove_shadows_for_priority(virtual_id, &name, root_priority);
    }

    if attrs_changed_in_place {
        if let Some(node) = tree.get_mut(virtual_id) {
            let now = SystemTime::now();
            node.attrs.mtime = now;
            node.attrs.ctime = now;
        }
    }
}

/// Pop the highest-priority shadow at `name`. File shadows install inline
/// (RAM-only). Directory shadows are *deferred* into `pending` so the
/// caller can run their `snapshot_dir` outside the tree write lock; the
/// slot stays empty until `install_pending_promotions` fills it. Returns
/// `true` if a promotion was installed or scheduled.
fn try_promote_shadow(
    tree: &mut Tree,
    parent: NodeId,
    name: &str,
    pending: &mut Vec<PendingPromotion>,
) -> bool {
    loop {
        // Peek both shadow lists' heads to decide which type to promote.
        // Lower priority value wins; ties prefer files (RAM-only, cheaper
        // and equally correct since priorities are unique per root).
        let (file_pri, dir_pri) = match tree.get(parent).map(|n| &n.kind) {
            Some(NodeKind::Directory { shadows, .. }) => match shadows.as_deref() {
                Some(s) => (
                    s.files
                        .get(name)
                        .and_then(|v| v.first())
                        .map(|x| x.priority),
                    s.dirs.get(name).and_then(|v| v.first()).map(|x| x.priority),
                ),
                None => return false,
            },
            _ => return false,
        };
        let pop_file = match (file_pri, dir_pri) {
            (Some(fp), Some(dp)) => fp <= dp,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return false,
        };
        if pop_file {
            if let Some(shadow) = tree.pop_shadow_file(parent, name) {
                let kind = NodeKind::File {
                    backing: shadow.backing.clone(),
                };
                if let Some(id) = tree.add_child(parent, name.to_string(), kind, shadow.attrs) {
                    info!(name=%name, backing=%shadow.backing.display(), priority=shadow.priority,
                          "promoting shadowed file");
                    tree.index_file(shadow.backing, id);
                    tree.set_winner_priority(id, Some(shadow.priority));
                    return true;
                }
                // add_child shouldn't fail at an unowned name; if it does,
                // try the next shadow rather than infinite-loop.
                continue;
            }
        } else if let Some(shadow) = tree.pop_shadow_dir(parent, name) {
            // Defer dir-shadow promotion: installing requires `snapshot_dir`
            // (disk I/O) and the caller currently holds the tree write
            // lock. The drainer releases the lock, runs snapshot_dir, then
            // re-acquires for `install_pending_promotions`.
            info!(name=%name, root=%shadow.physical.display(), priority=shadow.priority,
                  "scheduling shadowed directory promotion (deferred snapshot)");
            pending.push(PendingPromotion {
                parent,
                name: name.to_string(),
                shadow,
            });
            return true;
        }
        return false;
    }
}

/// Convenience wrapper: run `apply_snapshot` and immediately install any
/// deferred directory-shadow promotions inline. Use this in tests and
/// other contexts that don't need the watcher's two-phase lock-release
/// semantics. Production code (the watcher's drainer) should call
/// `apply_snapshot` and `install_pending_promotions` separately so the
/// disk I/O for promotions happens with the tree write lock released.
pub fn apply_snapshot_inline(
    tree: &mut Tree,
    virtual_id: NodeId,
    snap: &DirSnapshot,
    root_priority: usize,
    config: &Config,
) {
    let mut pending = Vec::new();
    apply_snapshot(tree, virtual_id, snap, root_priority, config, &mut pending);
    while !pending.is_empty() {
        let snapshotted = snapshot_pending_promotions(pending, config);
        pending = install_pending_promotions(tree, snapshotted);
    }
}

/// Phase A of the two-phase install: do the disk reads (`snapshot_dir`)
/// for each deferred promotion, with NO tree lock held by the caller.
/// Returns each pending paired with its freshly-read snapshot (or `None`
/// if the loser path has also vanished).
pub fn snapshot_pending_promotions(
    pending: Vec<PendingPromotion>,
    config: &Config,
) -> Vec<(PendingPromotion, Option<DirSnapshot>)> {
    pending
        .into_iter()
        .map(|p| {
            let snap = snapshot_dir(&p.shadow.physical, config, 0);
            (p, snap)
        })
        .collect()
}

/// Phase B: install previously-snapshotted promotions. RAM-only — caller
/// holds the tree write lock. Each slot is re-checked before installing:
/// another concurrent apply may have already filled the name, or the
/// parent itself may have been removed; both cases are handled.
///
/// If a promotion's shadow was a directory whose path turned out to be
/// gone too (`snap` is `None`), this function pops the *next* shadow
/// for the same name from the tree and returns it as a retry. The caller
/// should re-snapshot the retries (no lock needed) and call this again,
/// looping until the retry list is empty. This preserves the original
/// "try shadows in priority order until one installs" semantics that
/// the two-phase split would otherwise have lost.
#[must_use = "retries must be re-snapshotted and re-installed; otherwise stale shadows leak"]
pub fn install_pending_promotions(
    tree: &mut Tree,
    snapshotted: Vec<(PendingPromotion, Option<DirSnapshot>)>,
) -> Vec<PendingPromotion> {
    let mut retries = Vec::new();
    for (p, snap) in snapshotted {
        // Parent must still exist. add_child also defends against this,
        // but checking here lets us short-circuit cleanly and log.
        if tree.get(p.parent).is_none() {
            info!(name=%p.name, "skipping deferred promotion — parent node removed");
            continue;
        }
        // Slot may have been filled while we did disk I/O for the
        // snapshot phase (e.g. a higher-priority root rescan that came
        // in while we were releasing the lock between phases).
        if tree.child(p.parent, &p.name).is_some() {
            continue;
        }
        match snap {
            Some(snap) => {
                let attrs = snap.attrs.clone();
                if let Some(id) =
                    tree.add_child(p.parent, p.name.clone(), NodeKind::empty_dir(), attrs)
                {
                    info!(name=%p.name, root=%p.shadow.physical.display(), priority=p.shadow.priority,
                          "installing deferred directory promotion");
                    tree.set_winner_priority(id, Some(p.shadow.priority));
                    merge_snapshot(tree, id, &snap, None, p.shadow.priority);
                }
            }
            None => {
                // Shadowed root path is gone. Pop the next-best shadow
                // (file or dir) and either install it inline (file:
                // RAM-only) or queue it as a retry (dir: needs another
                // round of disk I/O outside the lock).
                if let Some(next) = pop_next_shadow_for_promotion(tree, p.parent, &p.name) {
                    match next {
                        NextShadow::File(s) => {
                            let kind = NodeKind::File {
                                backing: s.backing.clone(),
                            };
                            if let Some(id) =
                                tree.add_child(p.parent, p.name.clone(), kind, s.attrs)
                            {
                                info!(name=%p.name, backing=%s.backing.display(), priority=s.priority,
                                      "promoting next shadow (file) after dir-shadow path vanished");
                                tree.index_file(s.backing, id);
                                tree.set_winner_priority(id, Some(s.priority));
                            }
                        }
                        NextShadow::Dir(s) => {
                            info!(name=%p.name, root=%s.physical.display(), priority=s.priority,
                                  "scheduling retry — previous dir-shadow path vanished");
                            retries.push(PendingPromotion {
                                parent: p.parent,
                                name: p.name.clone(),
                                shadow: s,
                            });
                        }
                    }
                } else {
                    info!(name=%p.name, "no further shadows; slot stays empty");
                }
            }
        }
    }
    retries
}

enum NextShadow {
    File(ShadowFile),
    Dir(ShadowDir),
}

/// Pop the highest-priority remaining shadow at `name`, choosing whichever
/// of file/dir has lower priority value (ties prefer file — RAM-only).
fn pop_next_shadow_for_promotion(
    tree: &mut Tree,
    parent: NodeId,
    name: &str,
) -> Option<NextShadow> {
    let (file_pri, dir_pri) = match tree.get(parent).map(|n| &n.kind) {
        Some(NodeKind::Directory { shadows, .. }) => match shadows.as_deref() {
            Some(s) => (
                s.files
                    .get(name)
                    .and_then(|v| v.first())
                    .map(|x| x.priority),
                s.dirs.get(name).and_then(|v| v.first()).map(|x| x.priority),
            ),
            None => return None,
        },
        _ => return None,
    };
    let pop_file = match (file_pri, dir_pri) {
        (Some(fp), Some(dp)) => fp <= dp,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => return None,
    };
    if pop_file {
        tree.pop_shadow_file(parent, name).map(NextShadow::File)
    } else {
        tree.pop_shadow_dir(parent, name).map(NextShadow::Dir)
    }
}

/// An incoming directory's name is already taken. Decide whether the
/// incoming root outranks the existing owner; if so, demote the owner
/// to a shadow and install the incoming dir. Otherwise, record the
/// incoming as a shadow.
fn handle_dir_collision(
    tree: &mut Tree,
    parent: NodeId,
    name: &str,
    sub: &DirSnapshot,
    root_priority: usize,
    config: &Config,
    pending: &mut Vec<PendingPromotion>,
) {
    let Some(existing_id) = tree.child(parent, name) else {
        return;
    };
    let existing_priority = tree.get(existing_id).and_then(|n| n.winner_priority);
    let outranks = matches!(existing_priority, Some(ep) if root_priority < ep);
    if outranks {
        let demote = capture_for_demotion(tree, existing_id, existing_priority.expect("checked"));
        let Some(demote) = demote else {
            // Existing is synthetic or multi-source — can't demote it.
            // Record the incoming as a shadow so a future winner-removal
            // event can promote us if applicable.
            tree.add_shadow_dir(
                parent,
                name,
                ShadowDir {
                    priority: root_priority,
                    physical: sub.physical.clone(),
                },
            );
            return;
        };
        info!(name=%name, demoted_priority=?existing_priority, new_priority=root_priority,
              "demoting existing winner; higher-priority root taking over");
        tree.remove_recursive(existing_id);
        match demote {
            DemotedShadow::File(s) => tree.add_shadow_file(parent, name, s),
            DemotedShadow::Dir(s) => tree.add_shadow_dir(parent, name, s),
        }
        if let Some(child_id) = tree.add_child(
            parent,
            name.to_string(),
            NodeKind::empty_dir(),
            sub.attrs.clone(),
        ) {
            tree.set_winner_priority(child_id, Some(root_priority));
            apply_snapshot(tree, child_id, sub, root_priority, config, pending);
        }
    } else {
        // Incoming loses. Record as shadow (or update an existing shadow
        // entry from this same root — `add_shadow_dir` dedupes by priority).
        tree.add_shadow_dir(
            parent,
            name,
            ShadowDir {
                priority: root_priority,
                physical: sub.physical.clone(),
            },
        );
    }
}

/// Symmetric to `handle_dir_collision` for incoming files. Demotes the
/// existing owner if the incoming root outranks it; else records incoming
/// as a shadow.
fn handle_file_collision(
    tree: &mut Tree,
    parent: NodeId,
    name: &str,
    path: &Path,
    attrs: &CachedAttrs,
    root_priority: usize,
    _config: &Config,
) {
    let Some(existing_id) = tree.child(parent, name) else {
        return;
    };
    // Same-path no-op: re-applying our own snapshot against an unchanged file.
    if let Some(existing) = tree.get(existing_id) {
        if let NodeKind::File { backing } = &existing.kind {
            if backing == path {
                return;
            }
        }
    }
    let existing_priority = tree.get(existing_id).and_then(|n| n.winner_priority);
    let outranks = matches!(existing_priority, Some(ep) if root_priority < ep);
    if outranks {
        let demote = capture_for_demotion(tree, existing_id, existing_priority.expect("checked"));
        let Some(demote) = demote else {
            tree.add_shadow_file(
                parent,
                name,
                ShadowFile {
                    priority: root_priority,
                    backing: path.to_path_buf(),
                    attrs: attrs.clone(),
                },
            );
            return;
        };
        info!(name=%name, demoted_priority=?existing_priority, new_priority=root_priority,
              "demoting existing winner; higher-priority file taking over");
        tree.remove_recursive(existing_id);
        match demote {
            DemotedShadow::File(s) => tree.add_shadow_file(parent, name, s),
            DemotedShadow::Dir(s) => tree.add_shadow_dir(parent, name, s),
        }
        let kind = NodeKind::File {
            backing: path.to_path_buf(),
        };
        if let Some(child_id) = tree.add_child(parent, name.to_string(), kind, attrs.clone()) {
            tree.index_file(path.to_path_buf(), child_id);
            tree.set_winner_priority(child_id, Some(root_priority));
        }
    } else {
        tree.add_shadow_file(
            parent,
            name,
            ShadowFile {
                priority: root_priority,
                backing: path.to_path_buf(),
                attrs: attrs.clone(),
            },
        );
    }
}

/// What we captured from an existing winner that we're about to demote
/// to a shadow. Synthetic and multi-source dirs aren't demotable, so the
/// capture function returns `None` for those cases instead of encoding
/// the impossibility as a variant.
enum DemotedShadow {
    File(ShadowFile),
    Dir(ShadowDir),
}

/// Capture the demote-shadow info for an existing winner at `id` whose
/// current owner-priority is `priority`. Returns `None` for nodes that
/// can't be demoted (synthetic dirs, multi-source merged dirs).
fn capture_for_demotion(tree: &Tree, id: NodeId, priority: usize) -> Option<DemotedShadow> {
    let node = tree.get(id)?;
    match &node.kind {
        NodeKind::File { backing } => Some(DemotedShadow::File(ShadowFile {
            priority,
            backing: backing.clone(),
            attrs: node.attrs.clone(),
        })),
        NodeKind::Directory {
            sources: DirSources::Physical(paths),
            ..
        } if paths.len() == 1 => Some(DemotedShadow::Dir(ShadowDir {
            priority,
            physical: paths[0].clone(),
        })),
        _ => None,
    }
}

/// Merge a `DirSnapshot` into the tree with **additive, first-root-wins**
/// semantics. Used during initial build, where multiple `merge:` roots feed
/// the same virtual share and conflicts are resolved by config order.
///
/// `dedupe_depth` controls folder-level shadowing: `None` recurses forever
/// (fully union directory trees). `Some(0)` shadows colliding directories at
/// the *current* level — an earlier root's directory wins entirely and the
/// incoming subtree is dropped. The depth decrements on each recursion.
///
/// Dir-name collision below dedupe depth: if a dir of this name already
/// exists, descend into it (recursive merge of subtrees). File-name
/// collision: keep the existing (earlier) entry; log a warning unless the
/// existing's backing is the same path (which means we're re-applying our
/// own work).
pub fn merge_snapshot(
    tree: &mut Tree,
    virtual_id: NodeId,
    snap: &DirSnapshot,
    dedupe_depth: Option<usize>,
    root_priority: usize,
) {
    tree.extend_dir_sources(virtual_id, snap.physical.clone());
    tree.mark_unsorted(virtual_id);

    for (name, entry) in &snap.children {
        match entry {
            EntrySnapshot::Dir(sub) => {
                let existing = tree.child(virtual_id, name);
                let child_id = if let Some(eid) = existing {
                    if !tree.get(eid).map(|n| n.is_dir()).unwrap_or(false) {
                        // Cross-type collision: an earlier root placed a file
                        // at this name. Record the incoming dir as a shadow so
                        // it can be promoted if the file is later removed.
                        warn!(name=%name, "incoming directory shadowed by earlier file with same name");
                        tree.add_shadow_dir(
                            virtual_id,
                            name,
                            ShadowDir {
                                priority: root_priority,
                                physical: sub.physical.clone(),
                            },
                        );
                        continue;
                    }
                    if dedupe_depth == Some(0) {
                        // Folder-level dedupe: an earlier root owns this dir.
                        // Stash the loser as a shadow so a future delete of
                        // the winning root's copy promotes us in.
                        warn!(
                            name = %name,
                            losing_root = %sub.physical.display(),
                            "directory shadowed by earlier root (dedupe_depth)"
                        );
                        tree.add_shadow_dir(
                            virtual_id,
                            name,
                            ShadowDir {
                                priority: root_priority,
                                physical: sub.physical.clone(),
                            },
                        );
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
                        Some(id) => {
                            // First root contributes this dir name → claim
                            // ownership at our priority. `extend_dir_sources`
                            // will clear this back to None if a later root
                            // adds a second source (merged dirs aren't owned
                            // by one root).
                            tree.set_winner_priority(id, Some(root_priority));
                            id
                        }
                        None => continue,
                    }
                };
                merge_snapshot(
                    tree,
                    child_id,
                    sub,
                    dedupe_depth.map(|d| d.saturating_sub(1)),
                    root_priority,
                );
            }
            EntrySnapshot::File { path, attrs } => {
                let kind = NodeKind::File {
                    backing: path.clone(),
                };
                if let Some(child_id) =
                    tree.add_child(virtual_id, name.clone(), kind, attrs.clone())
                {
                    tree.index_file(path.clone(), child_id);
                    tree.set_winner_priority(child_id, Some(root_priority));
                } else {
                    let already_same = tree
                        .child(virtual_id, name)
                        .and_then(|cid| tree.get(cid))
                        .map(|n| matches!(&n.kind, NodeKind::File { backing } if backing == path))
                        .unwrap_or(false);
                    if !already_same {
                        // Lost the name to an earlier root (file or dir).
                        // Record as a file shadow so a future removal of the
                        // winner promotes this loser into the slot.
                        warn!(
                            name = %name,
                            new_path = %path.display(),
                            "duplicate file shadowed by earlier root"
                        );
                        tree.add_shadow_file(
                            virtual_id,
                            name,
                            ShadowFile {
                                priority: root_priority,
                                backing: path.clone(),
                                attrs: attrs.clone(),
                            },
                        );
                    }
                }
            }
        }
    }
}

pub fn build(config: &Config, server_id: u64) -> Result<Tree> {
    let mut tree = Tree::new(server_id);

    // Phase 1: lay out share + subdir virtual nodes (sequential, RAM-only).
    // We collect a flat job list of (target_virtual_id, physical_path,
    // is_subdir) so the disk-bound phase can fan out without touching the
    // tree.
    struct ScanJob {
        target_id: NodeId,
        physical: PathBuf,
        is_subdir: bool,
        label: String, // for logs
        // Internal dedupe counter passed to `merge_snapshot`. The invariant:
        // when `merge_snapshot` is called with `Some(0)`, dir-collisions in
        // the *current call's child loop* are deduped; recursion into a
        // surviving child decrements (saturating). User-facing
        // `dedupe_depth = N` therefore enters as `Some(N - 1)`: at depth=1
        // the top-level child loop dedupes immediately; at depth=2 we
        // recurse once before the trigger fires. `None` disables dedupe.
        // Subdirs always get `None` — dedupe is a merge-roots concept.
        dedupe_remaining: Option<usize>,
        /// Index of this root in its share's `merge` list (lower wins).
        /// Drives shadow-list ordering and demote-on-add at watcher time.
        /// Subdirs use 0 — they have no peers to compete with.
        root_priority: usize,
    }
    let mut jobs: Vec<ScanJob> = Vec::new();
    // Subdir names per parent virtual id. The doc'd contract is that a
    // subdir shadows any same-named entry contributed by the parent's merge
    // roots (collision is logged at apply time). Without this set the
    // recursive merge would descend into the subdir's virtual dir and
    // pollute it with files from the merge root.
    let mut subdir_names_per_share: HashMap<NodeId, HashSet<String>> = HashMap::new();

    /// Walk one share-shaped config rooted at `parent_id`, populating jobs
    /// and the subdir-shadow set. Recurses through nested `subdirs:`.
    fn build_share_jobs(
        tree: &mut Tree,
        parent_id: NodeId,
        label: &str,
        share: &crate::config::ShareConfig,
        jobs: &mut Vec<ScanJob>,
        subdir_names_per_share: &mut HashMap<NodeId, HashSet<String>>,
    ) {
        // Subdirs first so their names take precedence over same-named
        // entries in merge roots at this level.
        for (subdir_name, sub_cfg) in &share.subdirs {
            let Some(subdir_id) = tree.add_child(
                parent_id,
                subdir_name.clone(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            ) else {
                warn!(parent = %label, subdir = %subdir_name, "subdir name conflicts; skipping");
                continue;
            };
            subdir_names_per_share
                .entry(parent_id)
                .or_default()
                .insert(subdir_name.clone());

            // Backwards-compatible single-path subdir: one merge root, no
            // dedupe, no nested subdirs. Tag the ScanJob with `is_subdir`
            // so phase 3 schedules it ahead of its parent's merge entries.
            // Subdirs that themselves declare `merge:` or `subdirs:` are
            // recursed into and contribute their own (non-`is_subdir`)
            // jobs at the subdir's id.
            let subdir_label = format!("{label}/{subdir_name}");
            let only_one_merge_no_nesting = sub_cfg.merge.len() == 1
                && sub_cfg.subdirs.is_empty()
                && sub_cfg.dedupe_depth.is_none();
            if only_one_merge_no_nesting {
                let root = &sub_cfg.merge[0];
                if root.exists() {
                    jobs.push(ScanJob {
                        target_id: subdir_id,
                        physical: root.clone(),
                        is_subdir: true,
                        label: format!("{subdir_label}:subdir:{}", root.display()),
                        dedupe_remaining: None,
                        root_priority: 0,
                    });
                } else {
                    warn!(share=%subdir_label, root=%root.display(), "subdir root missing; skipping");
                }
            } else {
                build_share_jobs(
                    tree,
                    subdir_id,
                    &subdir_label,
                    sub_cfg,
                    jobs,
                    subdir_names_per_share,
                );
            }
        }

        for (idx, root) in share.merge.iter().enumerate() {
            if !root.exists() {
                warn!(share=%label, root=%root.display(), "merge root missing; skipping");
                continue;
            }
            jobs.push(ScanJob {
                target_id: parent_id,
                physical: root.clone(),
                is_subdir: false,
                label: format!("{label}:merge:{}", root.display()),
                dedupe_remaining: share.dedupe_depth.map(|d| d.saturating_sub(1)),
                // Priority 0 is reserved for subdirs (which always win at
                // their parent's slot over a merge entry of the same name).
                // Merge roots start at 1 so the priority spaces don't
                // overlap and a tied collision between merge[0] and a
                // subdir name is impossible.
                root_priority: idx + 1,
            });
        }
    }

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
        build_share_jobs(
            &mut tree,
            share_id,
            share_name,
            share,
            &mut jobs,
            &mut subdir_names_per_share,
        );
    }

    // Phase 2: snapshot every root in parallel. Each worker only needs
    // `&Config` and the physical path; no tree access, no lock. On spinning
    // disks this gives ≈N_disks speedup over the previous serial scan.
    struct ScanResult {
        target_id: NodeId,
        physical: PathBuf,
        snapshot: Option<DirSnapshot>,
        is_subdir: bool,
        label: String,
        dedupe_remaining: Option<usize>,
        root_priority: usize,
    }
    let scan_start = std::time::Instant::now();
    let snapshots: Vec<ScanResult> = std::thread::scope(|s| {
        let cfg = config;
        let handles: Vec<_> = jobs
            .into_iter()
            .map(|j| {
                s.spawn(move || ScanResult {
                    target_id: j.target_id,
                    snapshot: snapshot_dir(&j.physical, cfg, 0),
                    physical: j.physical,
                    is_subdir: j.is_subdir,
                    label: j.label,
                    dedupe_remaining: j.dedupe_remaining,
                    root_priority: j.root_priority,
                })
            })
            .collect();
        // Joining handles in spawn order (i.e. config order) — threads may
        // *complete* in any order, but `snapshots[i]` corresponds to
        // `jobs[i]`, which is what first-root-wins / dedupe semantics
        // depend on at apply time.
        handles
            .into_iter()
            .map(|h| h.join().expect("scan worker panicked"))
            .collect()
    });
    let scan_elapsed = scan_start.elapsed();

    // Phase 3: apply snapshots sequentially. Subdirs first (already won
    // their share-root slot in phase 1), then merge roots in config order
    // for first-root-wins on file conflicts.
    let (subdirs, merges): (Vec<_>, Vec<_>) = snapshots.into_iter().partition(|r| r.is_subdir);

    for r in subdirs.into_iter().chain(merges) {
        let ScanResult {
            target_id,
            physical,
            snapshot,
            is_subdir,
            label,
            dedupe_remaining,
            root_priority,
        } = r;
        match snapshot {
            Some(mut s) => {
                if !is_subdir {
                    if let Some(shadowed) = subdir_names_per_share.get(&target_id) {
                        s.children.retain(|name, _| {
                            if shadowed.contains(name) {
                                warn!(
                                    share_id = target_id,
                                    name = %name,
                                    "merge entry shadowed by subdir of same name"
                                );
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
                info!(target=%label, root=%physical.display(), "applying scan");
                merge_snapshot(&mut tree, target_id, &s, dedupe_remaining, root_priority);
            }
            None => {
                warn!(target=%label, path=%physical.display(), "scan returned no data; skipping")
            }
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
        Config::from_parts(ServerConfig::default(), shares, Options::default()).unwrap()
    }

    fn cfg_with_options(shares: BTreeMap<String, ShareConfig>, options: Options) -> Config {
        Config::from_parts(ServerConfig::default(), shares, options).unwrap()
    }

    fn child_names(tree: &Tree, dir: NodeId) -> Vec<String> {
        match &tree.get(dir).unwrap().kind {
            NodeKind::Directory { ordered, .. } => ordered.iter().map(|(n, _)| n.clone()).collect(),
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
                hide_patterns: vec!["thumbs.db".into()],
                ..Options::default()
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
                follow_symlinks: true,
                ..Options::default()
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
        merge_snapshot(&mut tree, share, &s1, None, 0);
        merge_snapshot(&mut tree, share, &s2, None, 1);
        tree.finalize_sort();

        let backing = file_backing(&tree, share, "Movie.mkv");
        assert!(
            backing.starts_with(r1.path()),
            "first root must win: backing={}",
            backing.display()
        );
    }

    #[test]
    fn merge_snapshot_dedupe_depth_one_drops_colliding_top_level_dir() {
        // The headline case: two roots both contain `Inception (2010)/...`.
        // With dedupe_depth=1 (internally Some(0) at top level), the second
        // root's copy of the folder must be dropped entirely — none of its
        // files should leak into the first root's folder.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Inception (2010)/r1.mkv"));
        touch(&r1.path().join("Inception (2010)/extras/r1-extra.mkv"));
        touch(&r2.path().join("Inception (2010)/r2.mkv"));
        touch(&r2.path().join("Inception (2010)/extras/r2-extra.mkv"));
        touch(&r2.path().join("OnlyInR2/file.mkv"));

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
        // dedupe_depth=1 (user-facing) → Some(0) internally.
        merge_snapshot(&mut tree, share, &s1, Some(0), 0);
        merge_snapshot(&mut tree, share, &s2, Some(0), 1);
        tree.finalize_sort();

        // The deduped folder is fully owned by r1 — no r2 leakage anywhere.
        let movie = tree.child(share, "Inception (2010)").expect("present");
        let mut top = child_names(&tree, movie);
        top.sort();
        assert_eq!(
            top,
            vec!["extras".to_string(), "r1.mkv".to_string()],
            "top-level contents of deduped folder should come from r1 only"
        );
        let extras = tree.child(movie, "extras").unwrap();
        assert_eq!(
            child_names(&tree, extras),
            vec!["r1-extra.mkv".to_string()],
            "subtree of deduped folder must come from r1 only"
        );

        // Names not present in r1 are still added by r2 (dedupe doesn't
        // mean r2 contributes nothing — it just can't shadow-merge).
        assert!(tree.child(share, "OnlyInR2").is_some());
    }

    #[test]
    fn merge_snapshot_dedupe_depth_two_dedupes_one_level_deeper() {
        // dedupe_depth=2: top-level folders merge normally; the *second*
        // level (e.g. seasons inside a show) is deduped. Tests that the
        // depth counter actually decrements on recursion.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Show/Season 1/r1-ep1.mkv"));
        touch(&r2.path().join("Show/Season 1/r2-ep2.mkv")); // shadowed
        touch(&r2.path().join("Show/Season 2/r2-ep1.mkv")); // not shadowed (new)

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

        // dedupe_depth=2 (user-facing) → Some(1) internally.
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(1),
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(1),
            1,
        );
        tree.finalize_sort();

        let show = tree.child(share, "Show").expect("Show merged");
        let mut seasons = child_names(&tree, show);
        seasons.sort();
        assert_eq!(
            seasons,
            vec!["Season 1".to_string(), "Season 2".to_string()],
            "Show should union both roots' season folders"
        );
        // Season 1 is shadowed by r1: only r1's episode is present.
        let s1 = tree.child(show, "Season 1").unwrap();
        assert_eq!(
            child_names(&tree, s1),
            vec!["r1-ep1.mkv".to_string()],
            "Season 1 contents must come from r1 only (depth-2 dedupe)"
        );
        // Season 2 was added by r2 (no collision).
        let s2 = tree.child(show, "Season 2").unwrap();
        assert_eq!(child_names(&tree, s2), vec!["r2-ep1.mkv".to_string()]);
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
        merge_snapshot(&mut tree, share, &s1, None, 0);
        merge_snapshot(&mut tree, share, &s2, None, 1);
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
        merge_snapshot(&mut tree, share, &snap1, None, 0);
        tree.finalize_sort();
        assert!(tree.child(share, "gone.mkv").is_some());

        // Mutate disk: delete one, add another.
        fs::remove_file(root.path().join("gone.mkv")).unwrap();
        touch(&root.path().join("new.mkv"));
        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap2, 0, &cfg);

        assert!(tree.child(share, "keep.mkv").is_some());
        assert!(tree.child(share, "gone.mkv").is_none());
        assert!(tree.child(share, "new.mkv").is_some());
    }

    #[test]
    fn apply_snapshot_in_place_file_change_bumps_parent_mtime() {
        // Linux NFS uses parent dir mtime as the dentry-cache freshness
        // key. If a file is overwritten in place (size or mtime change with
        // no name add/remove), the parent must still be marked dirty.
        let root = TempDir::new().unwrap();
        let path = root.path().join("Movie.mkv");
        std::fs::write(&path, b"v1").unwrap();

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
        merge_snapshot(&mut tree, share, &snap1, None, 0);
        tree.finalize_sort();

        // Backdate parent mtime to a sentinel; if apply_snapshot's
        // attrs_changed_in_place branch fires it will overwrite this with
        // `now()`. Size changes (`v1` → 17 bytes) guarantee `attrs_changed`
        // is true, so we don't depend on filesystem mtime granularity.
        tree.get_mut(share).unwrap().attrs.mtime = SystemTime::UNIX_EPOCH;
        std::fs::write(&path, b"v2-longer-content").unwrap();
        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap2, 0, &cfg);

        let mtime_after = tree.get(share).unwrap().attrs.mtime;
        assert!(
            mtime_after > SystemTime::UNIX_EPOCH,
            "in-place file change must bump parent dir mtime"
        );
    }

    // ---------- build() end-to-end ----------

    #[test]
    fn build_produces_share_with_merged_and_subdir_roots() {
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
                subdirs: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "Archive".to_string(),
                        ShareConfig {
                            merge: vec![archive.path().to_path_buf()],
                            ..Default::default()
                        },
                    );
                    m
                },
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let movies = tree.child(ROOT_ID, "Movies").expect("Movies share");
        let mut names = child_names(&tree, movies);
        names.sort();
        assert_eq!(
            names,
            vec![
                "A.mkv".to_string(),
                "Archive".to_string(),
                "B.mkv".to_string()
            ]
        );
        let archive_id = tree.child(movies, "Archive").unwrap();
        assert!(tree.child(archive_id, "Old.mkv").is_some());
    }

    #[test]
    fn build_subdir_takes_precedence_over_merge_with_same_name() {
        // A merge root contains a directory called "Archive"; a subdir also
        // named "Archive" should win and the merge entry should be ignored.
        let merge_root = TempDir::new().unwrap();
        touch(&merge_root.path().join("Archive/from_merge.mkv"));
        let subdir_root = TempDir::new().unwrap();
        touch(&subdir_root.path().join("from_subdir.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Movies".to_string(),
            ShareConfig {
                merge: vec![merge_root.path().to_path_buf()],
                subdirs: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "Archive".to_string(),
                        ShareConfig {
                            merge: vec![subdir_root.path().to_path_buf()],
                            ..Default::default()
                        },
                    );
                    m
                },
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let movies = tree.child(ROOT_ID, "Movies").unwrap();
        let archive = tree.child(movies, "Archive").expect("subdir Archive");
        // Subdir fully shadows the merge entry — only subdir content is visible.
        assert_eq!(
            child_names(&tree, archive),
            vec!["from_subdir.mkv".to_string()]
        );
    }

    #[test]
    fn build_subdir_supports_nested_merge_and_dedupe_depth() {
        // A subdir is itself a share-shaped config: it can have multiple
        // merge roots and its own `dedupe_depth`. Models the Infuse-friendly
        // shape where one outer "Library" share groups inner "Movies" /
        // "TV" subdirs that each merge across resolution-tier roots and
        // dedupe at folder level.
        let bluray = TempDir::new().unwrap();
        let remux = TempDir::new().unwrap();
        let p1080 = TempDir::new().unwrap();
        // Same movie folder name in all three tiers — dedupe must keep
        // only the highest-priority (Bluray) copy's contents.
        touch(&bluray.path().join("Inception/main.mkv"));
        touch(&remux.path().join("Inception/extras.mkv"));
        touch(&p1080.path().join("Inception/lo.mkv"));
        // Movie only in 1080p — must still appear, sourced from 1080p.
        touch(&p1080.path().join("Heat/main.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Library".to_string(),
            ShareConfig {
                merge: vec![],
                subdirs: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "Movies".to_string(),
                        ShareConfig {
                            merge: vec![
                                bluray.path().to_path_buf(),
                                remux.path().to_path_buf(),
                                p1080.path().to_path_buf(),
                            ],
                            subdirs: BTreeMap::new(),
                            dedupe_depth: Some(1),
                        },
                    );
                    m
                },
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let library = tree.child(ROOT_ID, "Library").unwrap();
        let movies = tree.child(library, "Movies").unwrap();
        let mut names = child_names(&tree, movies);
        names.sort();
        assert_eq!(names, vec!["Heat".to_string(), "Inception".to_string()]);
        // Inception came from Bluray (highest priority); the other tiers
        // are shadowed entirely, so only `main.mkv` is present.
        let inception = tree.child(movies, "Inception").unwrap();
        assert_eq!(child_names(&tree, inception), vec!["main.mkv".to_string()]);
        // Heat exists only in 1080p; it's promoted as the winner there.
        let heat = tree.child(movies, "Heat").unwrap();
        assert_eq!(child_names(&tree, heat), vec!["main.mkv".to_string()]);
    }

    #[test]
    fn build_three_level_nested_subdirs_each_get_their_own_node() {
        // A grand-subdir should appear as a real directory at the right
        // path with the right contents — i.e. recursion in the builder
        // doesn't lose levels or attach contents to the wrong parent.
        let leaf = TempDir::new().unwrap();
        touch(&leaf.path().join("inner.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "L1".to_string(),
            ShareConfig {
                merge: vec![],
                subdirs: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "L2".to_string(),
                        ShareConfig {
                            merge: vec![],
                            subdirs: {
                                let mut m2 = BTreeMap::new();
                                m2.insert(
                                    "L3".to_string(),
                                    ShareConfig {
                                        merge: vec![leaf.path().to_path_buf()],
                                        ..Default::default()
                                    },
                                );
                                m2
                            },
                            dedupe_depth: None,
                        },
                    );
                    m
                },
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let l1 = tree.child(ROOT_ID, "L1").unwrap();
        let l2 = tree.child(l1, "L2").unwrap();
        let l3 = tree.child(l2, "L3").unwrap();
        assert_eq!(child_names(&tree, l3), vec!["inner.mkv".to_string()]);
    }

    #[test]
    fn build_dedupe_depth_inside_subdir_is_independent_of_parent() {
        // Outer share has no dedupe; inner subdir has dedupe_depth=1.
        // Verify the inner dedupe fires (one folder kept) without leaking
        // into the outer level.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie/r1.mkv"));
        touch(&r2.path().join("Movie/r2.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Library".to_string(),
            ShareConfig {
                merge: vec![],
                subdirs: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "Movies".to_string(),
                        ShareConfig {
                            merge: vec![r1.path().to_path_buf(), r2.path().to_path_buf()],
                            subdirs: BTreeMap::new(),
                            dedupe_depth: Some(1),
                        },
                    );
                    m
                },
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let library = tree.child(ROOT_ID, "Library").unwrap();
        let movies = tree.child(library, "Movies").unwrap();
        let movie = tree.child(movies, "Movie").unwrap();
        // Dedupe at the inner Movies level kept only r1's copy; r2's
        // contents were shadowed entirely.
        assert_eq!(child_names(&tree, movie), vec!["r1.mkv".to_string()]);
    }

    #[test]
    fn build_subdir_of_subdir_shadows_parent_merge_entry_of_same_name() {
        // The "subdir wins over a merge entry of the same name" rule must
        // apply at every level, not just the share-root level.
        let outer_merge = TempDir::new().unwrap();
        // Create nested folder structure that would otherwise contribute
        // a name colliding with the inner subdir.
        touch(&outer_merge.path().join("Wrapper/Special/from_merge.mkv"));
        let inner = TempDir::new().unwrap();
        touch(&inner.path().join("from_subdir.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Top".to_string(),
            ShareConfig {
                merge: vec![outer_merge.path().to_path_buf()],
                subdirs: {
                    let mut m = BTreeMap::new();
                    m.insert(
                        "Wrapper".to_string(),
                        ShareConfig {
                            merge: vec![],
                            subdirs: {
                                let mut m2 = BTreeMap::new();
                                m2.insert(
                                    "Special".to_string(),
                                    ShareConfig {
                                        merge: vec![inner.path().to_path_buf()],
                                        ..Default::default()
                                    },
                                );
                                m2
                            },
                            dedupe_depth: None,
                        },
                    );
                    m
                },
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let top = tree.child(ROOT_ID, "Top").unwrap();
        let wrapper = tree.child(top, "Wrapper").unwrap();
        let special = tree.child(wrapper, "Special").unwrap();
        // The subdir at depth 2 wins; the merge root's `Special/` is shadowed.
        assert_eq!(
            child_names(&tree, special),
            vec!["from_subdir.mkv".to_string()]
        );
    }

    #[test]
    fn apply_snapshot_preserves_sorted_order_when_adding_entries() {
        // The drainer relies on `apply_snapshot` keeping each touched
        // directory's `sorted=true` invariant — `finalize_sort` is no longer
        // called after a watcher batch. If `add_child`'s binary-search
        // insertion regresses, this test catches it; readdir would return
        // entries in insertion order instead of sorted order.
        let root = TempDir::new().unwrap();
        std::fs::write(root.path().join("b.mkv"), b"").unwrap();

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "S".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let snap1 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &snap1, None, 0);
        tree.finalize_sort();

        // Add three files that interleave alphabetically with the existing
        // entry — `apply_snapshot` must binary-insert each one into place.
        std::fs::write(root.path().join("a.mkv"), b"").unwrap();
        std::fs::write(root.path().join("c.mkv"), b"").unwrap();
        std::fs::write(root.path().join("aa.mkv"), b"").unwrap();
        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap2, 0, &cfg);

        assert_eq!(
            child_names(&tree, share),
            vec![
                "a.mkv".to_string(),
                "aa.mkv".to_string(),
                "b.mkv".to_string(),
                "c.mkv".to_string(),
            ],
            "apply_snapshot must preserve sorted order on add"
        );
    }

    #[test]
    fn apply_snapshot_file_to_dir_collision_replaces_node() {
        // A file `entry` becomes a directory `entry/` on disk between
        // snapshots. The old file node must go and a directory node must
        // appear in its place — within a single apply, not deferred to a
        // later watcher tick.
        let root = TempDir::new().unwrap();
        let entry = root.path().join("entry");
        std::fs::write(&entry, b"old").unwrap();

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "S".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let snap1 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &snap1, None, 0);
        tree.finalize_sort();

        std::fs::remove_file(&entry).unwrap();
        std::fs::create_dir(&entry).unwrap();
        std::fs::write(entry.join("inner.txt"), b"x").unwrap();

        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap2, 0, &cfg);

        let entry_id = tree.child(share, "entry").expect("entry still present");
        assert!(
            tree.get(entry_id).unwrap().is_dir(),
            "entry must now be a dir"
        );
        assert!(tree.child(entry_id, "inner.txt").is_some());
    }

    #[test]
    fn apply_snapshot_does_not_leak_into_dir_owned_by_another_root() {
        // Sets up a deduped initial build then rescans the losing root.
        // The protection that keeps r2's contents out of r1's deduped folder
        // is *not* dedupe-specific — it's `apply_snapshot`'s generic
        // source-ownership check (the dir's `sources` only lists r1's path,
        // so r2's snapshot is `!backed_here` and skipped). The dir-level
        // analogue of `apply_snapshot_does_not_remove_files_from_other_merge_roots`.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Inception (2010)/r1.mkv"));
        touch(&r2.path().join("Inception (2010)/r2.mkv"));

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
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(0),
            1,
        );
        tree.finalize_sort();

        // Now simulate a watcher event on r2: a new file appears inside the
        // shadowed folder, and r2's full root is rescanned.
        touch(&r2.path().join("Inception (2010)/late-arrival.mkv"));
        let snap_r2 = snapshot_dir(r2.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r2, 1, &cfg);

        let movie = tree.child(share, "Inception (2010)").unwrap();
        assert_eq!(
            child_names(&tree, movie),
            vec!["r1.mkv".to_string()],
            "rescan of losing root must not leak files into deduped folder"
        );
    }

    #[test]
    fn apply_snapshot_dir_to_file_collision_replaces_node() {
        // Inverse of the above: a directory becomes a regular file. The dir
        // (and any descendants) must be removed and a file node must take
        // its place.
        let root = TempDir::new().unwrap();
        let entry = root.path().join("entry");
        std::fs::create_dir(&entry).unwrap();
        std::fs::write(entry.join("inner.txt"), b"x").unwrap();

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "S".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let snap1 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &snap1, None, 0);
        tree.finalize_sort();

        std::fs::remove_dir_all(&entry).unwrap();
        std::fs::write(&entry, b"now a file").unwrap();

        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap2, 0, &cfg);

        let entry_id = tree.child(share, "entry").expect("entry still present");
        let node = tree.get(entry_id).unwrap();
        assert!(!node.is_dir(), "entry must now be a file");
        match &node.kind {
            NodeKind::File { backing } => assert_eq!(backing, &entry),
            _ => panic!("expected File"),
        }
    }

    #[test]
    fn apply_snapshot_does_not_remove_files_from_other_merge_roots() {
        // Two merge roots union into one share. When we apply a snapshot of
        // root1 alone, files contributed by root2 (whose backing path lives
        // in root2, not root1) must NOT be removed: the `backing.parent()`
        // ownership check at builder.rs:139 prevents a rescan of one root
        // from clobbering another root's contributions.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        std::fs::write(r1.path().join("from_r1.mkv"), b"").unwrap();
        std::fs::write(r2.path().join("from_r2.mkv"), b"").unwrap();

        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "S".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            None,
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            None,
            1,
        );
        tree.finalize_sort();
        assert!(tree.child(share, "from_r2.mkv").is_some());

        // Re-snapshot r1 alone: from_r2.mkv is not in r1's snap.children.
        // Without the parent() check, the apply loop would treat it as a
        // stale file and call remove_recursive.
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r1, 0, &cfg);

        assert!(
            tree.child(share, "from_r1.mkv").is_some(),
            "r1 file should still be present after rescanning r1"
        );
        assert!(
            tree.child(share, "from_r2.mkv").is_some(),
            "r2 file must not be removed by a rescan of r1"
        );
    }

    #[test]
    fn build_with_dedupe_depth_drops_colliding_top_level_dir() {
        // End-to-end: dedupe_depth in ShareConfig is wired through `build()`
        // (the `dedupe_depth.saturating_sub(1)` translation at the call site)
        // so user-facing depth=1 actually shadows top-level folders.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Inception (2010)/r1.mkv"));
        touch(&r2.path().join("Inception (2010)/r2.mkv"));
        touch(&r2.path().join("Tenet (2020)/r2.mkv"));

        let mut shares = BTreeMap::new();
        shares.insert(
            "Movies".to_string(),
            ShareConfig {
                merge: vec![r1.path().to_path_buf(), r2.path().to_path_buf()],
                subdirs: BTreeMap::new(),
                dedupe_depth: Some(1),
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let movies = tree.child(ROOT_ID, "Movies").unwrap();
        let inception = tree.child(movies, "Inception (2010)").unwrap();
        // r1 wins entirely — only r1.mkv inside.
        assert_eq!(
            child_names(&tree, inception),
            vec!["r1.mkv".to_string()],
            "deduped folder must not contain r2 contents"
        );
        // r2's unique movie is still added.
        assert!(tree.child(movies, "Tenet (2020)").is_some());
    }

    #[test]
    fn build_with_dedupe_depth_two_dedupes_one_level_deeper() {
        // End-to-end check that the build()-site `saturating_sub(1)`
        // translation works for a non-trivial depth (catches regressions
        // where someone "fixes" the off-by-one and shifts everyone's
        // semantics by 1).
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Show/Season 1/r1-ep1.mkv"));
        touch(&r2.path().join("Show/Season 1/r2-ep2.mkv")); // shadowed
        touch(&r2.path().join("Show/Season 2/r2-ep1.mkv")); // unique, kept

        let mut shares = BTreeMap::new();
        shares.insert(
            "TV".to_string(),
            ShareConfig {
                merge: vec![r1.path().to_path_buf(), r2.path().to_path_buf()],
                subdirs: BTreeMap::new(),
                dedupe_depth: Some(2),
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build");
        let tv = tree.child(ROOT_ID, "TV").unwrap();
        let show = tree.child(tv, "Show").unwrap();
        let s1 = tree.child(show, "Season 1").unwrap();
        assert_eq!(
            child_names(&tree, s1),
            vec!["r1-ep1.mkv".to_string()],
            "depth=2 must dedupe Season 1 (r1 wins, r2 dropped)"
        );
        // Season 2 only exists in r2 → still added.
        assert!(tree.child(show, "Season 2").is_some());
    }

    #[test]
    fn apply_snapshot_winner_deletion_promotes_loser_on_next_rescan() {
        // Documented behavior: dedupe is enforced at build time, not
        // persistently. If the winning root's deduped folder is later
        // deleted, the next watcher rescan that re-adds the folder from
        // a losing root will succeed. This test pins down the behavior
        // so a future change can't silently shift it.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Inception (2010)/r1.mkv"));
        touch(&r2.path().join("Inception (2010)/r2.mkv"));

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
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(0),
            1,
        );
        tree.finalize_sort();

        // r1 deletes its copy of Inception entirely.
        std::fs::remove_dir_all(r1.path().join("Inception (2010)")).unwrap();
        // Watcher rescan of r1: snapshot has no "Inception (2010)" entry.
        // Shadow tracking now lets the loser (r2) get promoted in place,
        // so we don't need a follow-up r2 scan to fully heal — but we'll
        // still do one to assert idempotency of the apply.
        apply_snapshot_inline(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            0,
            &cfg,
        );
        apply_snapshot_inline(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            1,
            &cfg,
        );

        let movie = tree
            .child(share, "Inception (2010)")
            .expect("loser root should be promoted after winner's copy is removed");
        assert_eq!(
            child_names(&tree, movie),
            vec!["r2.mkv".to_string()],
            "after winner deletion, loser's contents should be visible"
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
                subdirs: BTreeMap::new(),
                dedupe_depth: None,
            },
        );
        let cfg = cfg_for(shares);
        let tree = build(&cfg, 0).expect("build should succeed despite missing root");
        let movies = tree.child(ROOT_ID, "Movies").unwrap();
        assert!(tree.child(movies, "real.mkv").is_some());
    }

    // ---------- shadow tracking: promote on remove, demote on add ----------

    /// Build a deduped two-root setup and return a (tree, share, cfg) triple
    /// for tests that exercise shadow promotion/demotion.
    fn deduped_two_root_setup(r1: &TempDir, r2: &TempDir) -> (Tree, NodeId, Config) {
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
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(0),
            1,
        );
        tree.finalize_sort();
        (tree, share, cfg)
    }

    #[test]
    fn shadow_promotes_loser_dir_immediately_on_winner_delete() {
        // The headline scenario: r1 wins a deduped folder, then r1's copy
        // is deleted from disk. A *single* watcher rescan of r1 should
        // promote r2's shadowed copy in place — no need to wait for a
        // separate r2 event or the periodic rescan.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Inception (2010)/r1.mkv"));
        touch(&r2.path().join("Inception (2010)/r2.mkv"));
        let (mut tree, share, cfg) = deduped_two_root_setup(&r1, &r2);

        std::fs::remove_dir_all(r1.path().join("Inception (2010)")).unwrap();
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r1, 0, &cfg);

        let movie = tree
            .child(share, "Inception (2010)")
            .expect("loser must be promoted in same apply that removed winner");
        assert_eq!(
            child_names(&tree, movie),
            vec!["r2.mkv".to_string()],
            "promoted dir must be backed by r2's contents"
        );
    }

    #[test]
    fn shadow_drops_dir_when_loser_path_also_gone() {
        // Edge case: r1 wins, r2 is shadowed, then BOTH roots' copies are
        // deleted before the watcher fires. apply_snapshot of r1 finds no
        // dir and tries to promote r2's shadow; snapshot_dir(r2's path)
        // returns None, so promotion fails and the dir cleanly disappears.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Inception (2010)/r1.mkv"));
        touch(&r2.path().join("Inception (2010)/r2.mkv"));
        let (mut tree, share, cfg) = deduped_two_root_setup(&r1, &r2);

        std::fs::remove_dir_all(r1.path().join("Inception (2010)")).unwrap();
        std::fs::remove_dir_all(r2.path().join("Inception (2010)")).unwrap();
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r1, 0, &cfg);

        assert!(
            tree.child(share, "Inception (2010)").is_none(),
            "with both copies gone, promotion must fail and the dir disappear"
        );
    }

    #[test]
    fn shadow_higher_priority_root_demotes_existing_dir() {
        // The "I added Inception to /mnt/4k" scenario. r2 (lower priority)
        // contributed `Lone (2024)` first; later r1 (higher priority) gets
        // a copy too. apply_snapshot of r1 must demote r2 and install r1.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r2.path().join("Lone (2024)/r2.mkv"));
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
        // Initial: only r2 has the folder.
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(0),
            1,
        );
        tree.finalize_sort();
        // r1 receives the same folder name.
        touch(&r1.path().join("Lone (2024)/r1.mkv"));
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r1, 0, &cfg);

        let movie = tree.child(share, "Lone (2024)").expect("present");
        assert_eq!(
            child_names(&tree, movie),
            vec!["r1.mkv".to_string()],
            "higher-priority root must take over the folder"
        );
        // Sanity: removing r1 again should now promote r2 back.
        std::fs::remove_dir_all(r1.path().join("Lone (2024)")).unwrap();
        let snap_r1b = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r1b, 0, &cfg);
        let movie = tree.child(share, "Lone (2024)").expect("r2 promoted back");
        assert_eq!(child_names(&tree, movie), vec!["r2.mkv".to_string()]);
    }

    #[test]
    fn shadow_lower_priority_arrival_does_not_displace_winner() {
        // Symmetric to the previous test: r1 already owns the folder; r2
        // (lower priority) arrives with the same name. The winner stays
        // and r2 is recorded as a shadow.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie/r1.mkv"));
        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "M".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            0,
        );
        tree.finalize_sort();
        // r2 contributes the same name later.
        touch(&r2.path().join("Movie/r2.mkv"));
        let snap_r2 = snapshot_dir(r2.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r2, 1, &cfg);

        let movie = tree.child(share, "Movie").unwrap();
        assert_eq!(
            child_names(&tree, movie),
            vec!["r1.mkv".to_string()],
            "lower-priority arrival must not displace existing winner"
        );
        // ...but r2 should be recorded as a shadow so deleting r1's copy
        // promotes r2 immediately.
        std::fs::remove_dir_all(r1.path().join("Movie")).unwrap();
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap_r1, 0, &cfg);
        let movie = tree.child(share, "Movie").expect("r2 promoted from shadow");
        assert_eq!(child_names(&tree, movie), vec!["r2.mkv".to_string()]);
    }

    #[test]
    fn shadow_promotes_file_chain_three_way() {
        // Three roots all contribute the same filename. r1 wins; deleting
        // r1's copy promotes r2; deleting r2's copy promotes r3; deleting
        // r3's copy leaves nothing.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        let r3 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie.mkv"));
        touch(&r2.path().join("Movie.mkv"));
        touch(&r3.path().join("Movie.mkv"));
        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "M".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            None,
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            None,
            1,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r3.path(), &cfg, 0).unwrap(),
            None,
            2,
        );
        tree.finalize_sort();
        assert!(file_backing(&tree, share, "Movie.mkv").starts_with(r1.path()));

        // r1 deletes → r2 promoted.
        std::fs::remove_file(r1.path().join("Movie.mkv")).unwrap();
        apply_snapshot_inline(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            0,
            &cfg,
        );
        assert!(
            file_backing(&tree, share, "Movie.mkv").starts_with(r2.path()),
            "after r1 delete, r2 must own the name"
        );

        // r2 deletes → r3 promoted.
        std::fs::remove_file(r2.path().join("Movie.mkv")).unwrap();
        apply_snapshot_inline(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            1,
            &cfg,
        );
        assert!(
            file_backing(&tree, share, "Movie.mkv").starts_with(r3.path()),
            "after r2 delete, r3 must own the name"
        );

        // r3 deletes → name fully gone.
        std::fs::remove_file(r3.path().join("Movie.mkv")).unwrap();
        apply_snapshot_inline(
            &mut tree,
            share,
            &snapshot_dir(r3.path(), &cfg, 0).unwrap(),
            2,
            &cfg,
        );
        assert!(tree.child(share, "Movie.mkv").is_none());
    }

    #[test]
    fn apply_snapshot_prunes_stale_shadow_when_loser_deletes_its_copy() {
        // r2 is the shadow at "Movie.mkv" (r1 wins). Then r2 deletes its
        // own copy. apply_snapshot of r2's now-empty root must drop the
        // stale shadow entry — otherwise it accumulates forever and
        // a subsequent winner-removal would try to promote a dead path.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie.mkv"));
        touch(&r2.path().join("Movie.mkv"));
        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "M".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            None,
            0,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            None,
            1,
        );
        tree.finalize_sort();

        // Loser deletes its copy.
        std::fs::remove_file(r2.path().join("Movie.mkv")).unwrap();
        apply_snapshot_inline(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            1,
            &cfg,
        );

        // Confirm shadow is gone: peek inside the parent dir.
        match &tree.get(share).unwrap().kind {
            NodeKind::Directory { shadows, .. } => {
                assert!(
                    shadows.is_none(),
                    "stale shadow at Movie.mkv must be pruned after loser deletes its copy"
                );
            }
            _ => panic!("share should be a dir"),
        }
        // Sanity: r1 is still the visible owner.
        assert!(file_backing(&tree, share, "Movie.mkv").starts_with(r1.path()));
    }

    #[test]
    fn two_phase_promotion_skips_slot_filled_between_phases() {
        // Simulates the watcher race: apply_snapshot defers a dir-shadow
        // promotion (winner removed, snapshot needs disk I/O), then before
        // the install phase runs, another apply fills the slot. The
        // install must skip the deferred promotion rather than clobber.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie/r1.mkv"));
        touch(&r2.path().join("Movie/r2.mkv"));
        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "M".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            1,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(0),
            2,
        );
        tree.finalize_sort();

        // r1 deletes its Movie folder. apply_snapshot defers a promotion
        // for r2's shadow into `pending` — install hasn't happened yet.
        std::fs::remove_dir_all(r1.path().join("Movie")).unwrap();
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        let mut pending = Vec::new();
        apply_snapshot(&mut tree, share, &snap_r1, 1, &cfg, &mut pending);
        assert_eq!(pending.len(), 1, "promotion should have been deferred");

        // Simulate concurrent slot fill: between apply and install, some
        // other path inserts a file at the deferred name. The install
        // must skip rather than try to install a duplicate.
        let usurper_path = r1.path().join("usurper.mkv");
        std::fs::write(&usurper_path, b"").unwrap();
        let usurper_attrs = CachedAttrs::from_metadata(&std::fs::metadata(&usurper_path).unwrap());
        tree.add_child(
            share,
            "Movie".into(),
            NodeKind::File {
                backing: usurper_path.clone(),
            },
            usurper_attrs,
        )
        .expect("slot is free; add must succeed");

        // Run snapshot phase + install phase — the install should skip.
        let snapshotted = snapshot_pending_promotions(pending, &cfg);
        let retries = install_pending_promotions(&mut tree, snapshotted);
        assert!(retries.is_empty(), "no retries expected");

        // The usurper is still the owner.
        let id = tree.child(share, "Movie").unwrap();
        match &tree.get(id).unwrap().kind {
            NodeKind::File { backing } => assert_eq!(*backing, usurper_path),
            _ => panic!("usurper should still own the slot"),
        }
    }

    #[test]
    fn two_phase_promotion_falls_through_to_next_shadow_when_path_vanished() {
        // Three-root deduped folder: r1 wins, r2 and r3 are both shadows.
        // When r1 deletes AND r2's path is also gone, install should
        // return retries that cause r3 to be promoted on the next round.
        let r1 = TempDir::new().unwrap();
        let r2 = TempDir::new().unwrap();
        let r3 = TempDir::new().unwrap();
        touch(&r1.path().join("Movie/r1.mkv"));
        touch(&r2.path().join("Movie/r2.mkv"));
        touch(&r3.path().join("Movie/r3.mkv"));
        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "M".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r1.path(), &cfg, 0).unwrap(),
            Some(0),
            1,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r2.path(), &cfg, 0).unwrap(),
            Some(0),
            2,
        );
        merge_snapshot(
            &mut tree,
            share,
            &snapshot_dir(r3.path(), &cfg, 0).unwrap(),
            Some(0),
            3,
        );
        tree.finalize_sort();

        // Both r1 and r2 are gone before the watcher fires.
        std::fs::remove_dir_all(r1.path().join("Movie")).unwrap();
        std::fs::remove_dir_all(r2.path().join("Movie")).unwrap();

        // Drive the loop manually: apply r1's empty snap, then snapshot
        // and install in repeating phases.
        let snap_r1 = snapshot_dir(r1.path(), &cfg, 0).unwrap();
        let mut pending = Vec::new();
        apply_snapshot(&mut tree, share, &snap_r1, 1, &cfg, &mut pending);
        let mut iterations = 0;
        while !pending.is_empty() {
            iterations += 1;
            assert!(iterations < 10, "promotion loop should terminate quickly");
            let snapshotted = snapshot_pending_promotions(pending, &cfg);
            pending = install_pending_promotions(&mut tree, snapshotted);
        }

        // r3 should now own the slot; r2 was tried and skipped.
        let movie = tree
            .child(share, "Movie")
            .expect("r3 must be promoted after r2's path vanished");
        assert_eq!(child_names(&tree, movie), vec!["r3.mkv".to_string()]);
    }

    #[test]
    fn apply_snapshot_mode_change_on_file_bumps_parent_mtime() {
        // chmod-only on a file (size + mtime unchanged) must still bump the
        // parent dir mtime so Linux NFS clients revalidate the listing.
        // Without `mode` in the attrs-changed check, this would be invisible
        // to clients until the periodic 24h rescan or another event.
        let root = TempDir::new().unwrap();
        let path = root.path().join("Movie.mkv");
        std::fs::write(&path, b"v1").unwrap();
        // Force a stable mtime so size+mtime equal between snaps; only mode
        // changes. (`set_permissions` does not bump mtime on Linux.)
        let cfg = cfg_for(BTreeMap::new());
        let mut tree = Tree::new(0);
        let share = tree
            .add_child(
                ROOT_ID,
                "M".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let snap1 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        merge_snapshot(&mut tree, share, &snap1, None, 0);
        tree.finalize_sort();
        // Backdate parent mtime to a sentinel.
        tree.get_mut(share).unwrap().attrs.mtime = SystemTime::UNIX_EPOCH;

        // Flip mode bits.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(&path, perms).unwrap();
        }

        let snap2 = snapshot_dir(root.path(), &cfg, 0).unwrap();
        apply_snapshot_inline(&mut tree, share, &snap2, 0, &cfg);

        let mtime_after = tree.get(share).unwrap().attrs.mtime;
        assert!(
            mtime_after > SystemTime::UNIX_EPOCH,
            "mode-only file change must bump parent dir mtime"
        );
    }
}
