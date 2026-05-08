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
//! warn log and a `WatchSignal::RescanAll` is queued so the drainer catches
//! up once it regains capacity.
//!
//! Periodic rescan: a long-interval safety net (default 24h, configurable via
//! `options.rescan_interval_secs`). Catches the cases notify can miss
//! entirely — kernel-queue saturation that wasn't surfaced as `Flag::Rescan`,
//! filesystems that don't generate events at all (some FUSE/SMB mounts), or
//! events that fire before the watcher is fully attached.
//!
//! Overflow handling: notify reports queue overflows differently per-OS.
//! On macOS FSEvents the kernel sets `Flag::Rescan` inside an `Ok` event
//! (the underlying FSEvents flag is `kFSEventStreamEventFlagMustScanSubDirs`);
//! on Linux inotify it surfaces as `Err`. Both routes emit a
//! `WatchSignal::RescanAll` to force re-snapshot of every root.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use notify::event::Flag;
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use notify_debouncer_full::{new_debouncer, DebounceEventResult, Debouncer, FileIdMap};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
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
    let Some(root) = tree.get(ROOT_ID) else {
        return out;
    };
    let NodeKind::Directory {
        by_name: shares, ..
    } = &root.kind
    else {
        return out;
    };
    for (share_name, share_id) in shares {
        let Some(share_cfg) = config.shares.get(share_name) else {
            continue;
        };
        let share_id = *share_id;
        for r in &share_cfg.merge {
            out.push(WatchRoot {
                physical: r.clone(),
                virtual_id: share_id,
            });
        }
        let Some(share_node) = tree.get(share_id) else {
            continue;
        };
        let NodeKind::Directory { by_name, .. } = &share_node.kind else {
            continue;
        };
        for (mount_name, r) in &share_cfg.mount {
            if let Some(mount_id) = by_name.get(mount_name) {
                out.push(WatchRoot {
                    physical: r.clone(),
                    virtual_id: *mount_id,
                });
            }
        }
    }
    out
}

/// Owns the OS watcher and the optional periodic-rescan task. Dropping the
/// `Watcher` aborts the periodic task and stops the debouncer; the drainer
/// then exits cleanly once the channel closes.
pub struct Watcher {
    _debouncer: Debouncer<RecommendedWatcher, FileIdMap>,
    periodic: Option<JoinHandle<()>>,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        if let Some(h) = self.periodic.take() {
            // The periodic task otherwise holds a `tx` that keeps the channel
            // open forever, so the drainer can't observe sender-closed and
            // would never exit.
            h.abort();
        }
    }
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

        rt.spawn(drain(
            rx,
            config.clone(),
            tree.clone(),
            roots.clone(),
            file_cache,
        ));

        // Optional periodic full-rescan task. `interval_secs == 0` disables.
        let periodic = if config.options.rescan_interval_secs > 0 {
            let interval_secs = config.options.rescan_interval_secs;
            let tx_periodic = tx.clone();
            let h = rt.spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
                // Default `Burst` would queue every missed tick during a stall
                // (e.g. drainer backed up on `tx.send().await`) and fire them
                // back-to-back on recovery — N consecutive full rescans for an
                // N-tick stall. We only ever need one catch-up, so skip past
                // missed ticks instead.
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // The first tick fires immediately. Skip it — we just finished
                // the initial build, so an immediate rescan would be redundant.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    // Awaiting send is appropriate here: the periodic task is
                    // not latency-sensitive, and waiting for capacity beats
                    // dropping the safety-net signal. Send-error means the
                    // drainer's rx has been dropped → time to exit.
                    if tx_periodic.send(WatchSignal::RescanAll).await.is_err() {
                        break;
                    }
                }
            });
            info!(interval_secs, "periodic rescan enabled");
            Some(h)
        } else {
            None
        };

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

        Ok(Self {
            _debouncer: debouncer,
            periodic,
        })
    }
}

/// Drainer: receive signals, batch them, route paths to virtual ids, snapshot
/// disk state in parallel without the lock, then apply each per-root under a
/// brief write lock — releasing between roots so concurrent NFS reads can
/// interleave instead of stalling for the whole batch.
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
        let was_rescan_all = signals.iter().any(|s| matches!(s, WatchSignal::RescanAll));

        // 2. Compute the dirty (root_path, virtual_id) set.
        let dirty: Vec<(PathBuf, NodeId)> = if was_rescan_all {
            roots
                .iter()
                .map(|r| (r.physical.clone(), r.virtual_id))
                .collect()
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
            let mut out: HashSet<(PathBuf, NodeId)> = HashSet::new();
            for path in &paths {
                if let Some(d) = route_path(path, &roots, &tree_r) {
                    out.insert(d);
                } else {
                    debug!(path=%path.display(), "event did not match any watched root");
                }
            }
            out.into_iter().collect()
        };

        if dirty.is_empty() {
            continue;
        }
        let root_count = dirty.len();

        // 3. Phase 1 — snapshot every dirty root *in parallel* on blocking
        // threads, with no tree lock held. Mirrors `build()`'s thread::scope
        // pattern: on a multi-disk library each spinning disk does its own
        // I/O concurrently, turning a serial N-disk scan into ~1-disk wall
        // time.
        let snap_start = Instant::now();
        let cfg_b = config.clone();
        let snapshots: Vec<(PathBuf, NodeId, Option<DirSnapshot>)> =
            match tokio::task::spawn_blocking(move || {
                let cfg: &Config = &cfg_b;
                std::thread::scope(|s| {
                    let handles: Vec<_> = dirty
                        .into_iter()
                        .map(|(path, vid)| {
                            s.spawn(move || {
                                let snap = snapshot_dir(&path, cfg, 0);
                                (path, vid, snap)
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| h.join().expect("snapshot worker panicked"))
                        .collect()
                })
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    // A panic inside the scope would otherwise drop the dirty
                    // batch silently, leaving the tree stale forever.
                    error!(error=?e, "snapshot worker panicked; dropping batch");
                    continue;
                }
            };
        let snap_ms = snap_start.elapsed().as_millis() as u64;

        // 4. Phase 2 — apply per-root, taking and releasing the write lock
        // each time. Holding one lock across all roots would stall every NFS
        // metadata RPC for the full apply duration; per-root release lets
        // readers interleave at the cost of a partially-rescanned tree being
        // briefly visible (acceptable for media — no transactional rescan).
        let apply_start = Instant::now();
        for (path, vid, snap) in snapshots {
            let mut tree_w = tree.write().await;
            match snap {
                Some(s) => apply_snapshot(&mut tree_w, vid, &s),
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
            // tree_w drops here; the next iteration re-acquires.
        }
        let apply_ms = apply_start.elapsed().as_millis() as u64;

        if was_rescan_all {
            info!(
                roots = root_count,
                snap_ms, apply_ms, "full rescan complete"
            );
        } else {
            debug!(
                roots = root_count,
                snap_ms, apply_ms, "watcher batch applied"
            );
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
fn route_path(path: &Path, roots: &[WatchRoot], tree: &Tree) -> Option<(PathBuf, NodeId)> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{build, snapshot_dir};
    use crate::config::{Options, ServerConfig, ShareConfig};
    use crate::vfs::new_file_cache;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn cfg_with(shares: BTreeMap<String, ShareConfig>) -> Config {
        Config {
            server: ServerConfig::default(),
            shares,
            options: Options::default(),
        }
    }

    fn one_merge_share(name: &str, root: &Path) -> BTreeMap<String, ShareConfig> {
        let mut m = BTreeMap::new();
        m.insert(
            name.to_string(),
            ShareConfig {
                merge: vec![root.to_path_buf()],
                mount: BTreeMap::new(),
            },
        );
        m
    }

    // ---------- collect_roots ----------

    #[test]
    fn collect_roots_returns_one_per_merge_and_mount() {
        let m1 = TempDir::new().unwrap();
        let m2 = TempDir::new().unwrap();
        let archive = TempDir::new().unwrap();
        std::fs::write(m1.path().join("a"), b"").unwrap();
        std::fs::write(m2.path().join("b"), b"").unwrap();
        std::fs::write(archive.path().join("old"), b"").unwrap();

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
        let cfg = cfg_with(shares);
        let tree = build(&cfg, 0).unwrap();

        let roots = collect_roots(&cfg, &tree);
        assert_eq!(roots.len(), 3, "two merge + one mount = 3 watch roots");

        let physicals: Vec<_> = roots.iter().map(|r| r.physical.clone()).collect();
        assert!(physicals.contains(&m1.path().to_path_buf()));
        assert!(physicals.contains(&m2.path().to_path_buf()));
        assert!(physicals.contains(&archive.path().to_path_buf()));

        // Mount root targets the mount node, not the share node — otherwise
        // events on the mount would be applied at the share level.
        let movies = tree.child(ROOT_ID, "Movies").unwrap();
        let archive_id = tree.child(movies, "Archive").unwrap();
        let archive_root = roots.iter().find(|r| r.physical == archive.path()).unwrap();
        assert_eq!(archive_root.virtual_id, archive_id);
    }

    #[test]
    fn collect_roots_returns_empty_for_share_with_no_sources() {
        let cfg = cfg_with(BTreeMap::new());
        let tree = Tree::new(0);
        assert!(collect_roots(&cfg, &tree).is_empty());
    }

    // ---------- match_root_prefix ----------

    #[test]
    fn match_root_prefix_picks_longest_match() {
        // When watch roots overlap (e.g. `/m` and `/m/sub`), an event at
        // `/m/sub/x` must route to the deeper root so the apply is scoped
        // to the smaller subtree.
        let roots = vec![
            WatchRoot {
                physical: PathBuf::from("/m"),
                virtual_id: 10,
            },
            WatchRoot {
                physical: PathBuf::from("/m/sub"),
                virtual_id: 20,
            },
        ];
        let r = match_root_prefix(&roots, Path::new("/m/sub/x")).unwrap();
        assert_eq!(r.virtual_id, 20);
    }

    #[test]
    fn match_root_prefix_returns_none_for_unrelated_path() {
        let roots = vec![WatchRoot {
            physical: PathBuf::from("/m"),
            virtual_id: 10,
        }];
        assert!(match_root_prefix(&roots, Path::new("/other/x")).is_none());
    }

    // ---------- route_path ----------

    #[test]
    fn route_path_resolves_indexed_directory_directly() {
        // The deepest indexed ancestor wins — a snapshot covers a single
        // physical directory, so we want the apply scoped as tightly as
        // possible.
        let m = TempDir::new().unwrap();
        std::fs::create_dir_all(m.path().join("sub")).unwrap();
        std::fs::write(m.path().join("sub/file"), b"").unwrap();

        let cfg = cfg_with(one_merge_share("S", m.path()));
        let tree = build(&cfg, 0).unwrap();
        let roots = collect_roots(&cfg, &tree);

        // Event on `m/sub/file` walks ancestors: file path is indexed but
        // not a dir, so we walk up. `m/sub` is indexed as a Physical dir
        // and wins.
        let routed = route_path(&m.path().join("sub/file"), &roots, &tree).unwrap();
        let expected_phys = m.path().join("sub");
        assert_eq!(routed.0, expected_phys);
        let virtual_dir = tree.lookup_path(&expected_phys).unwrap();
        assert_eq!(routed.1, virtual_dir);
    }

    #[test]
    fn route_path_skips_file_index_entries() {
        // The reverse path index also contains files. They must not be
        // returned from route_path — apply expects a directory virtual id.
        let m = TempDir::new().unwrap();
        std::fs::write(m.path().join("a.mkv"), b"").unwrap();

        let cfg = cfg_with(one_merge_share("S", m.path()));
        let tree = build(&cfg, 0).unwrap();
        let roots = collect_roots(&cfg, &tree);

        let routed = route_path(&m.path().join("a.mkv"), &roots, &tree).unwrap();
        // Resolves to the parent (the watch root), not the file's own id.
        assert_eq!(routed.0, m.path().to_path_buf());
        let parent = tree.lookup_path(m.path()).unwrap();
        assert_eq!(routed.1, parent);
    }

    #[test]
    fn route_path_falls_back_to_prefix_for_unindexed_paths() {
        // A new sub-path the watcher hasn't seen yet (no path_index entry).
        // We must still route it to the configured root via prefix match,
        // otherwise the very first event for a new directory would be
        // dropped silently.
        let m = TempDir::new().unwrap();
        let cfg = cfg_with(one_merge_share("S", m.path()));
        let tree = build(&cfg, 0).unwrap();
        let roots = collect_roots(&cfg, &tree);

        let new_path = m.path().join("brand/new/dir");
        let routed = route_path(&new_path, &roots, &tree).unwrap();
        assert_eq!(routed.0, m.path().to_path_buf());
    }

    #[test]
    fn route_path_returns_none_for_path_outside_all_roots() {
        let m = TempDir::new().unwrap();
        let cfg = cfg_with(one_merge_share("S", m.path()));
        let tree = build(&cfg, 0).unwrap();
        let roots = collect_roots(&cfg, &tree);

        assert!(route_path(Path::new("/some/unrelated/place"), &roots, &tree).is_none());
    }

    // ---------- drain (integration) ----------

    /// Build a tree + config wired to one merge root and return a drainer
    /// task plus a sender into its channel. The drainer runs until the
    /// sender is dropped.
    fn spawn_drainer(
        m: &TempDir,
    ) -> (
        mpsc::Sender<WatchSignal>,
        Arc<RwLock<Tree>>,
        FileCache,
        tokio::task::JoinHandle<()>,
    ) {
        let cfg = Arc::new(cfg_with(one_merge_share("S", m.path())));
        let tree = build(&cfg, 0).unwrap();
        let roots = collect_roots(&cfg, &tree);
        let tree = Arc::new(RwLock::new(tree));
        let cache = new_file_cache();
        let (tx, rx) = mpsc::channel::<WatchSignal>(64);
        let drainer = tokio::spawn(drain(rx, cfg, tree.clone(), roots, cache.clone()));
        (tx, tree, cache, drainer)
    }

    /// Wait until `predicate` is true under the read lock, polling briefly.
    /// The drainer is async and its work crosses two task hops
    /// (spawn_blocking + write lock), so we can't observe synchronously.
    async fn await_tree<F>(tree: &Arc<RwLock<Tree>>, mut predicate: F)
    where
        F: FnMut(&Tree) -> bool,
    {
        for _ in 0..200 {
            if predicate(&*tree.read().await) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("tree predicate did not converge within 2s");
    }

    #[tokio::test]
    async fn drain_applies_path_signal_to_add_new_file() {
        let m = TempDir::new().unwrap();
        let (tx, tree, _cache, drainer) = spawn_drainer(&m);

        std::fs::write(m.path().join("appeared.mkv"), b"").unwrap();
        tx.send(WatchSignal::Path(m.path().join("appeared.mkv")))
            .await
            .unwrap();

        await_tree(&tree, |t| {
            let s = t.child(ROOT_ID, "S").unwrap();
            t.child(s, "appeared.mkv").is_some()
        })
        .await;

        drop(tx);
        drainer.await.unwrap();
    }

    #[tokio::test]
    async fn drain_rescan_all_picks_up_changes_on_every_root() {
        let m = TempDir::new().unwrap();
        let (tx, tree, _cache, drainer) = spawn_drainer(&m);

        // Add files via RescanAll, which must rescan every configured root
        // even though no per-path signals were sent.
        std::fs::write(m.path().join("a.mkv"), b"").unwrap();
        std::fs::write(m.path().join("b.mkv"), b"").unwrap();
        tx.send(WatchSignal::RescanAll).await.unwrap();

        await_tree(&tree, |t| {
            let s = t.child(ROOT_ID, "S").unwrap();
            t.child(s, "a.mkv").is_some() && t.child(s, "b.mkv").is_some()
        })
        .await;

        drop(tx);
        drainer.await.unwrap();
    }

    #[tokio::test]
    async fn drain_clears_file_cache_after_apply() {
        let m = TempDir::new().unwrap();
        let path = m.path().join("a.mkv");
        std::fs::write(&path, b"x").unwrap();

        let (tx, tree, cache, drainer) = spawn_drainer(&m);

        // Pre-populate the cache as if a read had happened.
        let bogus = Arc::new(std::fs::File::open(&path).unwrap());
        cache.lock().unwrap().put(99_999, bogus);
        assert_eq!(cache.lock().unwrap().len(), 1);

        // Trigger any apply.
        std::fs::write(m.path().join("trigger.mkv"), b"").unwrap();
        tx.send(WatchSignal::Path(m.path().join("trigger.mkv")))
            .await
            .unwrap();

        await_tree(&tree, |t| {
            let s = t.child(ROOT_ID, "S").unwrap();
            t.child(s, "trigger.mkv").is_some()
        })
        .await;
        // The cache clear happens after the write-lock release; wait for it.
        for _ in 0..200 {
            if cache.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            cache.lock().unwrap().is_empty(),
            "file cache must be cleared after every apply (otherwise a stale fd survives a rescan)"
        );

        drop(tx);
        drainer.await.unwrap();
    }

    #[tokio::test]
    async fn drain_routes_unrelated_path_to_no_dirty_set() {
        // Events that don't match any watched root should be silently
        // ignored (not panic, not poison, not retry forever).
        let m = TempDir::new().unwrap();
        std::fs::write(m.path().join("baseline.mkv"), b"").unwrap();
        let (tx, tree, _cache, drainer) = spawn_drainer(&m);

        // Confirm the share starts with exactly the one initial-build entry.
        let baseline_count = {
            let t = tree.read().await;
            let sid = t.child(ROOT_ID, "S").unwrap();
            t.child(sid, "baseline.mkv").is_some() as usize
        };
        assert_eq!(baseline_count, 1);

        tx.send(WatchSignal::Path(PathBuf::from("/some/unrelated/place")))
            .await
            .unwrap();

        // Drainer must not panic and the tree must remain unchanged.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        {
            let t = tree.read().await;
            let sid = t.child(ROOT_ID, "S").unwrap();
            assert!(
                t.child(sid, "baseline.mkv").is_some(),
                "unrelated path must not trigger any apply"
            );
        }

        drop(tx);
        drainer.await.unwrap();
    }

    /// `apply_snapshot` is the diff applied to the in-memory tree. Verify
    /// that re-using it via the drainer reflects file *deletion* (not just
    /// addition), which exercises the same code path watcher relies on for
    /// renames and removals.
    #[tokio::test]
    async fn drain_applies_file_deletion() {
        let m = TempDir::new().unwrap();
        std::fs::write(m.path().join("doomed.mkv"), b"").unwrap();
        let (tx, tree, _cache, drainer) = spawn_drainer(&m);

        // Confirm initial presence.
        {
            let t = tree.read().await;
            let s = t.child(ROOT_ID, "S").unwrap();
            assert!(t.child(s, "doomed.mkv").is_some());
        }

        std::fs::remove_file(m.path().join("doomed.mkv")).unwrap();
        tx.send(WatchSignal::Path(m.path().join("doomed.mkv")))
            .await
            .unwrap();

        await_tree(&tree, |t| {
            let s = t.child(ROOT_ID, "S").unwrap();
            t.child(s, "doomed.mkv").is_none()
        })
        .await;

        drop(tx);
        drainer.await.unwrap();
    }

    /// Use snapshot_dir directly to confirm the helper used by drain treats
    /// a vanished directory the same way the drainer does — returns None,
    /// which the drainer interprets as "drop the source".
    #[test]
    fn snapshot_dir_returns_none_for_deleted_path() {
        let m = TempDir::new().unwrap();
        let path = m.path().to_path_buf();
        drop(m); // delete the tempdir
        let cfg = cfg_with(BTreeMap::new());
        assert!(snapshot_dir(&path, &cfg, 0).is_none());
    }

    // ---------- periodic rescan ----------

    /// Build a Watcher with a short rescan interval; tests below use the
    /// full Watcher::start path so the periodic spawn + abort-on-drop
    /// pipeline is exercised, not just `drain` in isolation.
    async fn build_watcher_with_interval(
        m: &TempDir,
        interval_secs: u64,
    ) -> (Arc<RwLock<Tree>>, Watcher) {
        let mut cfg_inner = cfg_with(one_merge_share("S", m.path()));
        cfg_inner.options.rescan_interval_secs = interval_secs;
        let cfg = Arc::new(cfg_inner);
        let tree = Arc::new(RwLock::new(build(&cfg, 0).unwrap()));
        let roots = collect_roots(&cfg, &*tree.read().await);
        let cache = new_file_cache();
        let watcher = Watcher::start(cfg, tree.clone(), roots, cache).unwrap();
        (tree, watcher)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_rescan_picks_up_change_made_before_watcher_started() {
        // Sanity check that the periodic safety net actually runs end-to-end:
        // we add a file *before* notify is watching (so notify's first event
        // is when the periodic rescan fires), then verify it appears in the
        // tree within a few interval ticks.
        let m = TempDir::new().unwrap();
        let mut cfg_inner = cfg_with(one_merge_share("S", m.path()));
        cfg_inner.options.rescan_interval_secs = 1;
        let cfg = Arc::new(cfg_inner);
        let tree = Arc::new(RwLock::new(build(&cfg, 0).unwrap()));
        let roots = collect_roots(&cfg, &*tree.read().await);
        let cache = new_file_cache();

        // Mutate disk *before* attaching the watcher. notify only surfaces
        // events from after `watch()` is called, so absent the periodic
        // rescan, this file would never be picked up.
        std::fs::write(m.path().join("preexisting.mkv"), b"").unwrap();

        let _watcher = Watcher::start(cfg, tree.clone(), roots, cache).unwrap();

        // Periodic interval is 1s; first effective tick lands at ~T+1s. The
        // generous 15s ceiling absorbs slow CI runners (macOS GitHub Actions
        // is routinely 4-5x slower than local) — local runs typically pass
        // in ~1s.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            {
                let t = tree.read().await;
                let s = t.child(ROOT_ID, "S").unwrap();
                if t.child(s, "preexisting.mkv").is_some() {
                    return;
                }
            }
            if Instant::now() >= deadline {
                panic!("periodic rescan did not pick up file within 15s");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watcher_drop_aborts_periodic_task_cleanly() {
        // Without explicit abort, the periodic task would keep its `tx`
        // clone alive forever, the channel would never close, and the
        // drainer task would leak. Drop must terminate cleanly without
        // hanging the test.
        let m = TempDir::new().unwrap();
        let (_tree, watcher) = build_watcher_with_interval(&m, 1).await;
        // Implicit drop here is the test.
        drop(watcher);
        // If Drop deadlocked, the test runner would time out.
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watcher_with_zero_interval_does_not_spawn_periodic_task() {
        // 0 disables the safety net. We can't easily inspect the task list,
        // but if `rescan_interval_secs == 0` had erroneously spawned an
        // interval(0) ticker, that would either panic at construction or
        // burn CPU in a tight loop. Just constructing without panic is the
        // assertion.
        let m = TempDir::new().unwrap();
        let (_tree, watcher) = build_watcher_with_interval(&m, 0).await;
        drop(watcher);
    }
}
