//! Read-side latency under writer contention.
//!
//! Tests the architectural claim that watcher applies don't stall NFS reads.
//! Three configurations:
//!
//!   - **baseline**: no concurrent writer
//!   - **under_per_root_apply**: writer simulates the current drain code
//!     by taking and releasing the write lock once per applied root
//!     (multiple acquire/release cycles per "batch")
//!   - **under_batched_apply**: writer simulates the *previous* drain code
//!     where the write lock was held across every root in a batch — useful
//!     to validate that per-root release actually buys reader latency
//!
//! `under_per_root_apply` should be substantially faster than `under_batched_apply`
//! when the batch covers more than one root; that difference *is* the win
//! from the per-root lock release optimization.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nfsserve::nfs::filename3;
use nfsserve::vfs::NFSFileSystem;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use fusion::builder::{apply_snapshot, merge_snapshot, snapshot_dir, DirSnapshot};
use fusion::config::{Config, Options, ServerConfig, ShareConfig};
use fusion::tree::{CachedAttrs, NodeId, NodeKind, Tree, ROOT_ID};
use fusion::vfs::{new_file_cache, FusionFs};

/// Files in each test directory. Realistic for a media library; large
/// enough that an apply walks a non-trivial number of nodes.
const DIR_FILES: usize = 1_000;

/// Number of physical roots feeding the share. Mimics a media library with
/// several merge roots — exactly the case where per-root lock release is
/// supposed to help.
const ROOT_COUNT: usize = 4;

fn cfg() -> Config {
    let mut shares = BTreeMap::new();
    shares.insert(
        "S".to_string(),
        ShareConfig {
            merge: vec![PathBuf::from("/unused")],
            subdirs: BTreeMap::new(),
            dedupe_depth: None,
        },
    );
    Config::from_parts(ServerConfig::default(), shares, Options::default()).expect("bench config")
}

struct BenchSetup {
    fs: FusionFs,
    tree: Arc<RwLock<Tree>>,
    share: NodeId,
    /// One snapshot per physical root. The writer applies all of them per
    /// "batch" — i.e. each batch represents one full RescanAll pass.
    snapshots: Vec<Arc<DirSnapshot>>,
    needle: filename3,
    fid: NodeId,
    cfg: Arc<Config>,
}

/// Build a tree backed by `ROOT_COUNT` physical directories, each holding
/// `DIR_FILES` empty files, all merged into one share. Returns one snapshot
/// per root — the writer cycles through these to simulate a full rescan.
fn setup() -> BenchSetup {
    let cfg = cfg();
    let mut snapshots = Vec::with_capacity(ROOT_COUNT);
    let mut roots = Vec::with_capacity(ROOT_COUNT);

    for ri in 0..ROOT_COUNT {
        let td = TempDir::new().unwrap();
        let dir = td.path().to_path_buf();
        // Disjoint filenames per root so the merge unions cleanly without
        // first-root-wins shadowing affecting the bench.
        for i in 0..DIR_FILES {
            std::fs::write(dir.join(format!("r{ri}_f{i:05}.mkv")), b"").unwrap();
        }
        let snap = snapshot_dir(&dir, &cfg, 0).unwrap();
        snapshots.push(snap);
        // Leak tempdir; bench process exits when criterion finishes.
        std::mem::forget(td);
        roots.push(dir);
    }

    let mut tree = Tree::new(0);
    let share = tree
        .add_child(
            ROOT_ID,
            "S".into(),
            NodeKind::empty_dir(),
            CachedAttrs::synthetic_dir(),
        )
        .unwrap();
    for (priority, snap) in snapshots.iter().enumerate() {
        merge_snapshot(&mut tree, share, snap, None, priority);
    }
    tree.finalize_sort();

    // Pick a needle from root 0 so it stays present through both
    // batched and per-root variants of the writer.
    let needle_name = format!("r0_f{:05}.mkv", DIR_FILES / 2);
    let fid = tree.child(share, &needle_name).unwrap();

    let tree = Arc::new(RwLock::new(tree));
    let fs = FusionFs::new(tree.clone(), 0, new_file_cache());
    let needle = filename3::from(needle_name.into_bytes());

    BenchSetup {
        fs,
        tree,
        share,
        snapshots: snapshots.into_iter().map(Arc::new).collect(),
        needle,
        fid,
        cfg: Arc::new(cfg),
    }
}

/// Per-root release: the lock is acquired and dropped once per applied
/// root. Mirrors the current `drain()` after the optimization.
fn spawn_writer_per_root(
    rt: &Runtime,
    tree: Arc<RwLock<Tree>>,
    share: NodeId,
    snapshots: Vec<Arc<DirSnapshot>>,
    stop: Arc<AtomicBool>,
    cfg: Arc<Config>,
) -> tokio::task::JoinHandle<u64> {
    rt.spawn(async move {
        let mut applies: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            for (priority, snap) in snapshots.iter().enumerate() {
                let mut tw = tree.write().await;
                apply_snapshot(&mut tw, share, snap, priority, &cfg);
                drop(tw);
                applies += 1;
            }
            tokio::task::yield_now().await;
        }
        applies
    })
}

/// Batched: the lock is held across every applied root in a batch. Mirrors
/// the *pre*-optimization `drain()` so we can quantify the win.
fn spawn_writer_batched(
    rt: &Runtime,
    tree: Arc<RwLock<Tree>>,
    share: NodeId,
    snapshots: Vec<Arc<DirSnapshot>>,
    stop: Arc<AtomicBool>,
    cfg: Arc<Config>,
) -> tokio::task::JoinHandle<u64> {
    rt.spawn(async move {
        let mut applies: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            let mut tw = tree.write().await;
            for (priority, snap) in snapshots.iter().enumerate() {
                apply_snapshot(&mut tw, share, snap, priority, &cfg);
                applies += 1;
            }
            drop(tw);
            tokio::task::yield_now().await;
        }
        applies
    })
}

fn make_runtime() -> Runtime {
    // Multi-threaded so writer and reader run on different worker threads
    // — we want real lock contention, not cooperative-yield ordering.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn bench_lookup(c: &mut Criterion) {
    let rt = make_runtime();
    let BenchSetup {
        fs,
        tree,
        share,
        snapshots,
        needle,
        cfg,
        ..
    } = setup();
    let needle = Arc::new(needle);
    let fs = Arc::new(fs);

    let mut group = c.benchmark_group("contention_lookup");

    {
        let fs = fs.clone();
        let needle = needle.clone();
        group.bench_function("baseline", |b| {
            b.to_async(&rt).iter(|| {
                let fs = fs.clone();
                let needle = needle.clone();
                async move { black_box(fs.lookup(share, &needle).await.unwrap()) }
            });
        });
    }

    // Per-root release (current production code path).
    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer_per_root(
        &rt,
        tree.clone(),
        share,
        snapshots.clone(),
        stop.clone(),
        cfg.clone(),
    );
    {
        let fs = fs.clone();
        let needle = needle.clone();
        group.bench_function("under_per_root_apply", |b| {
            b.to_async(&rt).iter(|| {
                let fs = fs.clone();
                let needle = needle.clone();
                async move { black_box(fs.lookup(share, &needle).await.unwrap()) }
            });
        });
    }
    stop.store(true, Ordering::Relaxed);
    let applies_pr = rt.block_on(async { writer.await.unwrap() });

    // Batched (the pre-optimization shape — for comparison only).
    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer_batched(&rt, tree, share, snapshots, stop.clone(), cfg);
    {
        let fs = fs.clone();
        let needle = needle.clone();
        group.bench_function("under_batched_apply", |b| {
            b.to_async(&rt).iter(|| {
                let fs = fs.clone();
                let needle = needle.clone();
                async move { black_box(fs.lookup(share, &needle).await.unwrap()) }
            });
        });
    }
    stop.store(true, Ordering::Relaxed);
    let applies_b = rt.block_on(async { writer.await.unwrap() });
    eprintln!(
        "contention_lookup: per_root applies={applies_pr}, batched applies={applies_b} (across {ROOT_COUNT} roots)"
    );

    group.finish();
}

fn bench_getattr(c: &mut Criterion) {
    let rt = make_runtime();
    let BenchSetup {
        fs,
        tree,
        share,
        snapshots,
        fid,
        cfg,
        ..
    } = setup();
    let fs = Arc::new(fs);

    let mut group = c.benchmark_group("contention_getattr");

    {
        let fs = fs.clone();
        group.bench_function("baseline", |b| {
            b.to_async(&rt).iter(|| {
                let fs = fs.clone();
                async move { black_box(fs.getattr(fid).await.unwrap()) }
            });
        });
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer_per_root(
        &rt,
        tree.clone(),
        share,
        snapshots.clone(),
        stop.clone(),
        cfg.clone(),
    );
    {
        let fs = fs.clone();
        group.bench_function("under_per_root_apply", |b| {
            b.to_async(&rt).iter(|| {
                let fs = fs.clone();
                async move { black_box(fs.getattr(fid).await.unwrap()) }
            });
        });
    }
    stop.store(true, Ordering::Relaxed);
    let applies_pr = rt.block_on(async { writer.await.unwrap() });

    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer_batched(&rt, tree, share, snapshots, stop.clone(), cfg);
    {
        let fs = fs.clone();
        group.bench_function("under_batched_apply", |b| {
            b.to_async(&rt).iter(|| {
                let fs = fs.clone();
                async move { black_box(fs.getattr(fid).await.unwrap()) }
            });
        });
    }
    stop.store(true, Ordering::Relaxed);
    let applies_b = rt.block_on(async { writer.await.unwrap() });
    eprintln!(
        "contention_getattr: per_root applies={applies_pr}, batched applies={applies_b} (across {ROOT_COUNT} roots)"
    );

    group.finish();
}

criterion_group!(benches, bench_lookup, bench_getattr);
criterion_main!(benches);
