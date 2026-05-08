//! Filesystem watcher.
//!
//! v1 strategy: per-root debounced full rescan.
//!
//! We map each watched physical path back to the virtual directory it feeds,
//! debounce events for ~2 seconds, then re-merge that root into the tree.
//! The rescan is non-destructive (existing nodes are preserved by name) and
//! the next watcher event for a removed file will clean up the stale node.
//!
//! When the watcher reports an unrecoverable error or a queue overflow, we
//! schedule a full rescan of every share root.
//!
//! Concurrency model: the notify callback is sync (runs on notify's thread).
//! It pushes dirty (physical_root, virtual_id) pairs onto an unbounded mpsc
//! channel. A single dedicated drainer task receives, coalesces everything
//! currently queued into a HashSet, takes the tree write lock once, and
//! rescans each unique root. This bounds write-lock contention even if the
//! filesystem is being hammered (e.g. rsync) — backpressure is the channel
//! buffer; lock churn stays at one acquisition per drain cycle.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use notify_debouncer_full::{new_debouncer, DebounceEventResult, Debouncer, FileIdMap};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::tree::{NodeId, NodeKind, Tree, ROOT_ID};

/// Information about a watched root: which virtual directory it feeds.
#[derive(Clone)]
pub struct WatchRoot {
    pub physical: PathBuf,
    pub virtual_id: NodeId,
}

/// Collect every (physical_root, virtual_id) pair to watch. Call this against
/// the freshly-built tree, before wrapping it in the async lock.
pub fn collect_roots(config: &Config, tree: &Tree) -> Vec<WatchRoot> {
    let mut out = Vec::new();
    let Some(root) = tree.get(ROOT_ID) else { return out };
    let NodeKind::Directory { by_name: shares, .. } = &root.kind else {
        return out;
    };
    for (share_name, share_id) in shares {
        let Some(share_cfg) = config.shares.get(share_name) else { continue };
        let share_id = *share_id;
        for r in &share_cfg.merge {
            out.push(WatchRoot { physical: r.clone(), virtual_id: share_id });
        }
        let Some(share_node) = tree.get(share_id) else { continue };
        let NodeKind::Directory { by_name, .. } = &share_node.kind else {
            continue;
        };
        for (mount_name, r) in &share_cfg.mount {
            if let Some(mount_id) = by_name.get(mount_name) {
                out.push(WatchRoot { physical: r.clone(), virtual_id: *mount_id });
            }
        }
    }
    out
}

pub struct Watcher {
    _debouncer: Debouncer<RecommendedWatcher, FileIdMap>,
}

/// What to rescan: a (physical_path, virtual_id) pair. Sent on the dirty
/// channel and coalesced by the drainer.
type DirtyRoot = (PathBuf, NodeId);

/// Send a path event to the drainer, picking the deepest virtual node we know
/// about. We first ask the live tree's `path_index` (consulted under a read
/// lock) for a virtual id corresponding to any ancestor of the event path —
/// this routes to the changed directory itself rather than always falling
/// back to the share root. Only on miss do we degrade to the configured
/// watch root.
async fn route_event(
    path: &Path,
    roots: &[WatchRoot],
    tree: &Arc<RwLock<Tree>>,
) -> Option<DirtyRoot> {
    {
        let tree = tree.read().await;
        let mut p = path;
        loop {
            if let Some(&vid) = tree.path_index.get(p) {
                // Only route to directory nodes — rescan_path expects a dir.
                // path_index also contains file paths; for those we keep
                // walking up to the containing directory.
                if let Some(node) = tree.get(vid) {
                    if node.is_dir() {
                        return Some((p.to_path_buf(), vid));
                    }
                }
            }
            match p.parent() {
                Some(parent) if parent != p => p = parent,
                _ => break,
            }
        }
    }
    match_root(roots, path).map(|r| (r.physical.clone(), r.virtual_id))
}

impl Watcher {
    pub fn start(
        config: Arc<Config>,
        tree: Arc<RwLock<Tree>>,
        roots: Vec<WatchRoot>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<DirtyRoot>();

        // Capture the current runtime handle so the notify callback (which
        // runs on its own non-tokio thread) can still spawn tasks.
        let rt = Handle::current();

        // Spawn the drainer. It owns the receiver, coalesces queued events,
        // and is the only place that takes the tree write lock for rescans.
        rt.spawn(drain_dirty(rx, config.clone(), tree.clone()));

        let roots_for_handler = roots.clone();
        let tree_for_handler = tree.clone();
        let tx_events = tx.clone();
        let tx_errors = tx.clone();
        let rt_cb = rt.clone();

        let mut debouncer = new_debouncer(
            Duration::from_secs(2),
            None,
            move |res: DebounceEventResult| match res {
                Ok(events) => {
                    // Hand routing off to an async task so we can consult the
                    // tree's path_index without blocking the notify thread.
                    let mut paths: Vec<PathBuf> = Vec::new();
                    for ev in events {
                        for path in &ev.paths {
                            paths.push(path.clone());
                        }
                    }
                    let roots = roots_for_handler.clone();
                    let tree = tree_for_handler.clone();
                    let tx = tx_events.clone();
                    rt_cb.spawn(async move {
                        let mut dirty: HashSet<DirtyRoot> = HashSet::new();
                        for path in &paths {
                            match route_event(path, &roots, &tree).await {
                                Some(d) => { dirty.insert(d); }
                                None => debug!(path=%path.display(), "event did not match any watched root"),
                            }
                        }
                        for d in dirty {
                            let _ = tx.send(d);
                        }
                    });
                }
                Err(errors) => {
                    for e in errors {
                        error!(error=?e, "watcher error; falling back to full rescan");
                    }
                    for r in &roots_for_handler {
                        let _ = tx_errors.send((r.physical.clone(), r.virtual_id));
                    }
                }
            },
        )?;

        for r in &roots {
            if let Err(e) = debouncer
                .watcher()
                .watch(&r.physical, RecursiveMode::Recursive)
            {
                warn!(path=%r.physical.display(), error=%e, "failed to watch path");
            }
        }
        info!(count = roots.len(), "watching roots");

        Ok(Self {
            _debouncer: debouncer,
        })
    }
}

async fn drain_dirty(
    mut rx: mpsc::UnboundedReceiver<DirtyRoot>,
    config: Arc<Config>,
    tree: Arc<RwLock<Tree>>,
) {
    while let Some(first) = rx.recv().await {
        // Coalesce: drain everything currently queued into a single set, so
        // a flurry of events resolves to one rescan pass per affected root.
        let mut dirty: HashSet<DirtyRoot> = HashSet::new();
        dirty.insert(first);
        while let Ok(extra) = rx.try_recv() {
            dirty.insert(extra);
        }

        let mut tree_guard = tree.write().await;
        for (path, vid) in &dirty {
            info!(root=%path.display(), virtual_id=vid, "rescanning after watcher event");
            crate::builder::rescan_path(&mut tree_guard, *vid, path, &config);
        }
        // Lock dropped here; NFS readers can resume between drain cycles.
    }
}

fn match_root<'a>(roots: &'a [WatchRoot], path: &Path) -> Option<&'a WatchRoot> {
    // Pick the longest prefix match.
    let mut best: Option<&WatchRoot> = None;
    let mut best_len = 0;
    for r in roots {
        if path.starts_with(&r.physical) {
            let len = r.physical.components().count();
            if len > best_len {
                best_len = len;
                best = Some(r);
            }
        }
    }
    best
}

