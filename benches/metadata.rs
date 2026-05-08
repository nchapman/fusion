//! NFS metadata-op microbenchmarks against a pre-built in-memory tree.
//! Exercises the hot-path lock + hashmap costs for lookup/getattr/readdir.

use std::path::PathBuf;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nfsserve::nfs::filename3;
use nfsserve::vfs::NFSFileSystem;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use fusion::tree::{CachedAttrs, NodeKind, Tree, ROOT_ID};
use fusion::vfs::{new_file_cache, FusionFs};

/// Populate root with `n` file children. Returns (fs, child_names).
fn fs_with_n_files(n: usize) -> (FusionFs, Vec<String>) {
    let mut tree = Tree::new(0);
    let mut names = Vec::with_capacity(n);
    for i in 0..n {
        let name = format!("file_{i:06}.mkv");
        tree.add_child(
            ROOT_ID,
            name.clone(),
            NodeKind::File {
                backing: PathBuf::from("/dev/null"),
            },
            CachedAttrs::synthetic_file(0),
        )
        .unwrap();
        names.push(name);
    }
    let fs = FusionFs::new(Arc::new(RwLock::new(tree)), 0, new_file_cache());
    (fs, names)
}

fn bench_lookup(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lookup");
    for &n in &[100usize, 1_000, 10_000] {
        let (fs, names) = fs_with_n_files(n);
        // Pick a needle near the middle so it's not a best-case hit.
        let needle = filename3::from(names[n / 2].as_bytes().to_vec());
        // A name guaranteed to miss — Infuse probes for `.DS_Store` etc.,
        // which always come back NOENT in production.
        let miss = filename3::from(b"missing-needle-zzz".to_vec());
        group.bench_with_input(BenchmarkId::new("hit", n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let id = fs.lookup(ROOT_ID, &needle).await.unwrap();
                black_box(id);
            });
        });
        group.bench_with_input(BenchmarkId::new("miss", n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let r = fs.lookup(ROOT_ID, &miss).await;
                black_box(r.unwrap_err());
            });
        });
    }
    group.finish();
}

fn bench_getattr(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // Build a tree with several directories under root so subdir_count work
    // is non-trivial; getattr is essentially read-lock + hash + format.
    let mut tree = Tree::new(0);
    for i in 0..50 {
        tree.add_child(
            ROOT_ID,
            format!("d_{i}"),
            NodeKind::empty_dir(),
            CachedAttrs::synthetic_dir(),
        )
        .unwrap();
    }
    let fs = FusionFs::new(Arc::new(RwLock::new(tree)), 0, new_file_cache());
    c.bench_function("getattr_root", |b| {
        b.to_async(&rt)
            .iter(|| async { black_box(fs.getattr(ROOT_ID).await.unwrap()) });
    });
}

fn bench_readdir(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("readdir_full");
    for &n in &[100usize, 1_000, 10_000] {
        let (fs, _) = fs_with_n_files(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                // Paginate the entire directory in 64-entry chunks.
                let mut cookie = 0u64;
                loop {
                    let r = fs.readdir(ROOT_ID, cookie, 64).await.unwrap();
                    // Force the compiler to keep the page allocation + each
                    // entry's filename3/fattr3 alive — without this LLVM can
                    // elide the per-entry work and time only the lock+hash.
                    black_box(&r);
                    if r.end {
                        break;
                    }
                    cookie = r.entries.last().unwrap().fileid;
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_lookup, bench_getattr, bench_readdir);
criterion_main!(benches);
