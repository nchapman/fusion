//! Build the in-memory tree by walking physical roots on startup.

use std::collections::{HashMap, HashSet};
use std::fs::Metadata;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::tree::{CachedAttrs, DirSources, NodeKind, NodeId, Tree, ROOT_ID};

/// Cap on recursive descent during scans. Protects against symlink loops
/// (since we follow symlinks) without needing inode tracking.
const MAX_SCAN_DEPTH: usize = 64;

/// Build an empty directory NodeKind. Empty == trivially sorted, so
/// `add_child` on this dir will maintain the sort invariant. Bulk-build
/// paths (`scan_into`, `merge_into`) flip it back to unsorted with
/// `mark_unsorted` so they can append in O(1) and we sort once at the end.
fn empty_dir_kind() -> NodeKind {
    NodeKind::Directory {
        by_name: HashMap::new(),
        ordered: Vec::new(),
        sorted: true,
        sources: DirSources::Synthetic,
    }
}

pub fn build(config: &Config, server_id: u64) -> Result<Tree> {
    let mut tree = Tree::new(server_id);

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

        // Mounts go in first — they win over merge collisions.
        for (mount_name, root) in &share.mount {
            if let Some(mount_id) = tree.add_child(
                share_id,
                mount_name.clone(),
                empty_dir_kind(),
                CachedAttrs::synthetic_dir(),
            ) {
                info!(share=%share_name, mount=%mount_name, root=%root.display(), "mounting");
                scan_into(&mut tree, mount_id, root, config, 0);
            } else {
                warn!(share=%share_name, mount=%mount_name, "mount name conflicts; skipping");
            }
        }

        // Then merge roots union into the share root.
        for root in &share.merge {
            if !root.exists() {
                warn!(share=%share_name, root=%root.display(), "merge root missing; skipping");
                continue;
            }
            info!(share=%share_name, root=%root.display(), "merging");
            merge_into(&mut tree, share_id, root, config, 0);
        }
    }

    tree.finalize_sort();
    info!(nodes = tree.node_count(), "tree built");
    Ok(tree)
}

/// Walk `physical` and create a strict mirror under `virtual_id`. Used for
/// `mount:` entries — no merging.
fn scan_into(
    tree: &mut Tree,
    virtual_id: NodeId,
    physical: &Path,
    config: &Config,
    depth: usize,
) {
    if depth > MAX_SCAN_DEPTH {
        warn!(path=%physical.display(), "max scan depth exceeded; symlink loop?");
        return;
    }
    tree.extend_dir_sources(virtual_id, physical.to_path_buf());
    // Bulk path: append children unsorted, finalize_sort handles the rest.
    tree.mark_unsorted(virtual_id);

    let entries = match std::fs::read_dir(physical) {
        Ok(it) => it,
        Err(e) => {
            warn!(path=%physical.display(), error=%e, "read_dir failed");
            return;
        }
    };

    for entry in entries.flatten() {
        let name = match entry.file_name().into_string() {
            Ok(s) => s,
            Err(_) => {
                warn!(path=%entry.path().display(), "non-utf8 filename, skipping");
                continue;
            }
        };
        if config.is_hidden(&name) {
            continue;
        }
        let path = entry.path();
        // Use std::fs::metadata (which follows symlinks) — DirEntry::metadata
        // is `lstat` on Unix and would classify symlinks as their own type.
        let md = match std::fs::metadata(&entry.path()) {
            Ok(m) => m,
            Err(e) => {
                warn!(path=%path.display(), error=%e, "stat failed");
                continue;
            }
        };
        let attrs = CachedAttrs::from_metadata(&md);

        if md.is_dir() {
            if let Some(child_id) =
                tree.add_child(virtual_id, name.clone(), empty_dir_kind(), attrs)
            {
                // scan_into will register `path` as the directory's source
                // and index it so watcher events under it match precisely.
                scan_into(tree, child_id, &path, config, depth + 1);
            }
        } else if md.is_file() {
            let kind = NodeKind::File {
                backing: path.clone(),
            };
            if let Some(child_id) = tree.add_child(virtual_id, name, kind, attrs) {
                tree.path_index.insert(path, child_id);
            }
        } else {
            // Symlink, socket, fifo, etc. — skip in v1.
            debug!(path=%path.display(), "skipping non-regular entry");
        }
    }
}

/// Merge a physical root into a virtual dir. Files first-root-wins; dirs
/// recursively merge by name.
fn merge_into(
    tree: &mut Tree,
    virtual_id: NodeId,
    physical: &Path,
    config: &Config,
    depth: usize,
) {
    if depth > MAX_SCAN_DEPTH {
        warn!(path=%physical.display(), "max scan depth exceeded; symlink loop?");
        return;
    }
    tree.extend_dir_sources(virtual_id, physical.to_path_buf());
    tree.mark_unsorted(virtual_id);

    let entries = match std::fs::read_dir(physical) {
        Ok(it) => it,
        Err(e) => {
            warn!(path=%physical.display(), error=%e, "read_dir failed");
            return;
        }
    };

    for entry in entries.flatten() {
        let name = match entry.file_name().into_string() {
            Ok(s) => s,
            Err(_) => {
                warn!(path=%entry.path().display(), "non-utf8 filename, skipping");
                continue;
            }
        };
        if config.is_hidden(&name) {
            continue;
        }
        let path = entry.path();
        // Use std::fs::metadata (which follows symlinks) — DirEntry::metadata
        // is `lstat` on Unix and would classify symlinks as their own type.
        let md = match std::fs::metadata(&entry.path()) {
            Ok(m) => m,
            Err(e) => {
                warn!(path=%path.display(), error=%e, "stat failed");
                continue;
            }
        };
        let attrs = CachedAttrs::from_metadata(&md);

        if md.is_dir() {
            // If a child dir of this name already exists, descend (merge).
            // Otherwise create a new one.
            let existing = tree.child(virtual_id, &name);
            let child_id = if let Some(eid) = existing {
                // Merging into an existing dir is fine; if the existing is a
                // file we lose to the earlier root and skip.
                if !tree.get(eid).map(|n| n.is_dir()).unwrap_or(false) {
                    warn!(path=%path.display(), "shadowed by earlier file with same name");
                    continue;
                }
                eid
            } else {
                tree.add_child(virtual_id, name.clone(), empty_dir_kind(), attrs)
                    .expect("just verified absence")
            };
            merge_into(tree, child_id, &path, config, depth + 1);
        } else if md.is_file() {
            let kind = NodeKind::File {
                backing: path.clone(),
            };
            if let Some(child_id) = tree.add_child(virtual_id, name.clone(), kind, attrs) {
                tree.path_index.insert(path, child_id);
            } else {
                // Already-present-with-same-path (rescan case) is silent;
                // genuine cross-root shadowing is logged.
                let already_same = tree
                    .child(virtual_id, &name)
                    .and_then(|cid| tree.get(cid))
                    .map(|n| matches!(&n.kind, NodeKind::File { backing } if backing == &path))
                    .unwrap_or(false);
                if !already_same {
                    warn!(
                        name = %name,
                        new_path = %path.display(),
                        "duplicate file shadowed by earlier root"
                    );
                }
            }
        } else {
            debug!(path=%path.display(), "skipping non-regular entry");
        }
    }
}

/// Re-scan a single physical root into the tree. Used by the watcher.
/// The `virtual_parent` is the virtual dir that this physical path feeds.
/// Re-scan a single physical root and reconcile the tree against it.
///
/// Unlike the initial-build merge, this is destructive: virtual children
/// whose backing path lived under `physical` and is gone from disk are
/// removed; surviving children get fresh attrs; new on-disk entries are
/// added. Children backed by *other* physical roots in the same union are
/// left alone — they aren't this rescan's responsibility.
pub fn rescan_path(
    tree: &mut Tree,
    virtual_id: NodeId,
    physical: &PathBuf,
    config: &Config,
) {
    rescan_dir(tree, virtual_id, physical, config, 0);
    tree.finalize_sort();
}

fn rescan_dir(
    tree: &mut Tree,
    virtual_id: NodeId,
    physical: &Path,
    config: &Config,
    depth: usize,
) {
    if depth > MAX_SCAN_DEPTH {
        warn!(path=%physical.display(), "max scan depth exceeded; symlink loop?");
        return;
    }

    // Make sure this physical path is registered as a source for the dir.
    tree.extend_dir_sources(virtual_id, physical.to_path_buf());

    // Read the on-disk entries up front so we can iterate the virtual side
    // without holding the disk handle.
    let on_disk: HashMap<String, (PathBuf, Metadata)> = match std::fs::read_dir(physical) {
        Ok(it) => it
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                if config.is_hidden(&name) {
                    return None;
                }
                let path = e.path();
                let md = std::fs::metadata(&path).ok()?; // follows symlinks
                Some((name, (path, md)))
            })
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Directory itself is gone — drop our source for it. If it has no
            // remaining sources we remove the virtual node entirely.
            let path = physical.to_path_buf();
            let now_empty = tree.drop_dir_source(virtual_id, &path);
            if now_empty && virtual_id != ROOT_ID {
                info!(path=%physical.display(), "removing virtual dir; underlying directory deleted");
                tree.remove_recursive(virtual_id);
            }
            return;
        }
        Err(e) => {
            warn!(path=%physical.display(), error=%e, "rescan read_dir failed");
            return;
        }
    };

    // Snapshot the virtual children once so we can mutate the tree while
    // diffing. Cheap: a single Vec clone of (name, NodeId) pairs.
    let virtual_children: Vec<(String, NodeId)> = match tree.get(virtual_id) {
        Some(node) => match &node.kind {
            NodeKind::Directory { ordered, .. } => ordered.clone(),
            _ => return,
        },
        None => return,
    };

    let virtual_names: HashSet<&str> = virtual_children
        .iter()
        .map(|(n, _)| n.as_str())
        .collect();
    let virtual_names: HashSet<String> = virtual_names.iter().map(|s| s.to_string()).collect();

    for (name, child_id) in &virtual_children {
        let Some(child) = tree.get(*child_id) else { continue };
        match &child.kind {
            NodeKind::File { backing } => {
                let backed_here = backing.starts_with(physical);
                if !backed_here {
                    continue; // From another root — leave alone.
                }
                match on_disk.get(name) {
                    Some((path, md)) if md.is_file() && path == backing => {
                        let new_attrs = CachedAttrs::from_metadata(md);
                        if let Some(node) = tree.get_mut(*child_id) {
                            node.attrs = new_attrs;
                        }
                    }
                    _ => {
                        info!(name=%name, backing=%backing.display(), "removing stale file node");
                        tree.remove_recursive(*child_id);
                    }
                }
            }
            NodeKind::Directory { sources, .. } => {
                let child_phys = physical.join(name);
                let backed_here = match sources {
                    DirSources::Physical(paths) => paths.iter().any(|p| p == &child_phys),
                    DirSources::Synthetic => false,
                };
                if !backed_here {
                    continue;
                }
                match on_disk.get(name) {
                    Some((path, md)) if md.is_dir() => {
                        rescan_dir(tree, *child_id, path, config, depth + 1);
                    }
                    _ => {
                        // Disk dir gone (or replaced by a non-dir). Drop our
                        // source from the union; if no sources remain, the
                        // dir is fully gone and we remove it.
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

    // Add anything on disk that the virtual tree didn't already have.
    for (name, (path, md)) in on_disk {
        if virtual_names.contains(&name) {
            // Already handled above (refresh / remove / recurse).
            continue;
        }
        let attrs = CachedAttrs::from_metadata(&md);
        if md.is_dir() {
            if let Some(child_id) =
                tree.add_child(virtual_id, name.clone(), empty_dir_kind(), attrs)
            {
                rescan_dir(tree, child_id, &path, config, depth + 1);
            }
        } else if md.is_file() {
            let kind = NodeKind::File { backing: path.clone() };
            if let Some(child_id) = tree.add_child(virtual_id, name, kind, attrs) {
                tree.path_index.insert(path, child_id);
            }
        }
    }
}
