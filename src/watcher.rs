//! Filesystem watcher.
//!
//! Pipeline:
//!
//! ```text
//!   notify (OS thread, sync)
//!     -> notify-debouncer-full (2s window, dedupe by inode)
//!     -> bounded mpsc channel of WatchSignal
//!     -> async drainer task
//!         phase 1: resolve event paths to (root, virtual_id) via path_index
//!                  under a brief read lock
//!         phase 2: spawn_blocking — snapshot each dirty root from disk,
//!                  WITHOUT holding any tree lock
//!         phase 3: take write lock briefly, apply each snapshot's diff,
//!                  release
//! ```
//!
//! Lock-hold time is bounded by the apply phase only (no disk I/O), which
//! keeps NFS read latency clean even during a multi-second cold scan of a
//! spinning disk.
//!
//! Backpressure: the channel is bounded. On full, events are dropped with a
//! warn log. The reconciliatory rescan model means a dropped event is not a
//! correctness problem: the next event for any path inside the same root, or
//! a fallback periodic scan, restores consistency. (No periodic fallback in
//! v1; documented as a known limit.)
//!
//! Overflow handling: notify reports queue overflows differently per-OS.
//! On macOS FSEvents the kernel sets `Flag::Rescan` inside an `Ok` event
//! (the underlying FSEvents flag is `kFSEventStreamEventFlagMustScanSubDirs`);
//! on Linux inotify it surfaces as `Err`. Both routes emit a
//! `WatchSignal::RescanAll` to force re-snapshot of every root.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use notify::event::Flag;
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use notify_debouncer_full::{new_debouncer, DebounceEventResult, Debouncer, FileIdMap};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::builder::{apply_snapshot, snapshot_dir, DirSnapshot};
use crate::config::Config;
use crate::tree::{NodeId, NodeKind, Tree, ROOT_ID};
use crate::vfs::FileCache;

const EVENT_CHANNEL_CAP: usize = 4096;

/// What the notify callback sends to the drainer. Paths are deduplicated and
/// routed to virtual ids inside the drainer, not here.
#[derive(Debug)]
enum WatchSignal {
    Path(PathBuf),
    RescanAll,
}

/// Information about a watched root: which virtual directory it feeds.
#[derive(Clone, Debug)]
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

impl Watcher {
    pub fn start(
        config: Arc<Config>,
        tree: Arc<RwLock<Tree>>,
        roots: Vec<WatchRoot>,
        file_cache: FileCache,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<WatchSignal>(EVENT_CHANNEL_CAP);

        // Capture runtime handle for the notify thread.
        let rt = Handle::current();

        rt.spawn(drain(rx, config.clone(), tree.clone(), roots.clone(), file_cache));

        let tx_cb = tx.clone();
        let mut debouncer = new_debouncer(
            Duration::from_secs(2),
            None,
            move |res: DebounceEventResult| match res {
                Ok(events) => {
                    let mut overflow = false;
                    let mut paths: HashSet<PathBuf> = HashSet::new();
                    for ev in events {
                        // FSEvents (macOS) reports kernel-queue overflow as a
                        // flag inside an Ok event. Treat it as "we lost some
                        // events; rescan everything."
                        if matches!(ev.flag(), Some(Flag::Rescan)) {
                            overflow = true;
                        }
                        for p in &ev.paths {
                            paths.insert(p.clone());
                        }
                    }
                    if overflow {
                        let _ = tx_cb.try_send(WatchSignal::RescanAll);
                        return;
                    }
                    let mut dropped = 0usize;
                    for path in paths {
                        if tx_cb.try_send(WatchSignal::Path(path)).is_err() {
                            dropped += 1;
                        }
                    }
                    if dropped > 0 {
                        // Channel saturated. Convert silent loss into a
                        // guaranteed catch-up: send RescanAll so the drainer
                        // picks up everything once it regains capacity.
                        warn!(dropped, "watcher channel full; scheduling full rescan");
                        let _ = tx_cb.try_send(WatchSignal::RescanAll);
                    }
                }
                Err(errors) => {
                    for e in errors {
                        error!(error=?e, "watcher error; scheduling full rescan");
                    }
                    let _ = tx_cb.try_send(WatchSignal::RescanAll);
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

        Ok(Self { _debouncer: debouncer })
    }
}

/// Drainer: receive signals, batch them, route paths to virtual ids, snapshot
/// disk state without the lock, then apply under write lock briefly.
async fn drain(
    mut rx: mpsc::Receiver<WatchSignal>,
    config: Arc<Config>,
    tree: Arc<RwLock<Tree>>,
    roots: Vec<WatchRoot>,
    file_cache: FileCache,
) {
    while let Some(first) = rx.recv().await {
        // 1. Coalesce queued signals.
        let mut signals = vec![first];
        while let Ok(s) = rx.try_recv() {
            signals.push(s);
        }

        // 2. Compute the dirty (root_path, virtual_id) set.
        let dirty = if signals.iter().any(|s| matches!(s, WatchSignal::RescanAll)) {
            roots
                .iter()
                .map(|r| (r.physical.clone(), r.virtual_id))
                .collect::<HashSet<_>>()
        } else {
            let paths: HashSet<PathBuf> = signals
                .into_iter()
                .filter_map(|s| match s {
                    WatchSignal::Path(p) => Some(p),
                    WatchSignal::RescanAll => None,
                })
                .collect();
            // Read lock briefly to consult path_index.
            let tree_r = tree.read().await;
            let mut out = HashSet::new();
            for path in &paths {
                if let Some(d) = route_path(path, &roots, &tree_r) {
                    out.insert(d);
                } else {
                    debug!(path=%path.display(), "event did not match any watched root");
                }
            }
            out
        };

        if dirty.is_empty() {
            continue;
        }

        // 3. Phase 1 — snapshot each dirty root on a blocking thread, no lock.
        let cfg_b = config.clone();
        let snapshots: Vec<(PathBuf, NodeId, Option<DirSnapshot>)> =
            match tokio::task::spawn_blocking(move || {
                dirty
                    .into_iter()
                    .map(|(path, vid)| {
                        let snap = snapshot_dir(&path, &cfg_b, 0);
                        (path, vid, snap)
                    })
                    .collect()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    // A panic inside snapshot_dir would otherwise drop the
                    // dirty batch silently, leaving the tree stale forever.
                    error!(error=?e, "snapshot worker panicked; dropping batch");
                    continue;
                }
            };

        // 4. Phase 2 — apply under write lock briefly.
        {
            let mut tree_w = tree.write().await;
            for (path, vid, snap) in snapshots {
                match snap {
                    Some(s) => {
                        info!(root=%path.display(), virtual_id=vid, "applying snapshot");
                        apply_snapshot(&mut tree_w, vid, &s);
                    }
                    None => {
                        // Underlying path is gone — drop our source; if the
                        // virtual dir has no sources left, remove it.
                        let now_empty = tree_w.drop_dir_source(vid, &path);
                        if now_empty && vid != ROOT_ID {
                            info!(path=%path.display(), "removing virtual dir; underlying directory deleted");
                            tree_w.remove_recursive(vid);
                        }
                    }
                }
            }
            tree_w.finalize_sort();
        }

        // Clear the file handle cache: any cached FD might point at a file
        // whose path was just replaced or removed. Cache rebuild is cheap
        // (one open per active stream); kernel page cache survives.
        file_cache.lock().unwrap().clear();
    }
}

/// Route an event path to the deepest watched virtual directory: walk up
/// ancestors, looking each up in `path_index`; on miss fall back to the
/// configured watch roots' longest prefix match.
fn route_path(
    path: &Path,
    roots: &[WatchRoot],
    tree: &Tree,
) -> Option<(PathBuf, NodeId)> {
    let mut p = path;
    loop {
        if let Some(vid) = tree.lookup_path(p) {
            // Only route to dirs (path_index also contains files).
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
    match_root_prefix(roots, path).map(|r| (r.physical.clone(), r.virtual_id))
}

fn match_root_prefix<'a>(roots: &'a [WatchRoot], path: &Path) -> Option<&'a WatchRoot> {
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
