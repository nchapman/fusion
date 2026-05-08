//! Read throughput against a real backing file. Compares warm cache (open(2)
//! amortized across many RPCs, the Infuse playback case) against cold cache
//! (cleared every iteration, simulating a watcher rescan dropping FDs).

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nfsserve::vfs::NFSFileSystem;
use rand::RngCore;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use fusion::tree::{CachedAttrs, NodeKind, Tree, ROOT_ID};
use fusion::vfs::{new_file_cache, FileCache, FusionFs};

const FILE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
const READ_SIZE: u32 = 1024 * 1024; // 1 MiB — Infuse playback chunk size

fn write_random(path: &Path, len: usize) {
    let mut buf = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut buf);
    std::fs::File::create(path)
        .unwrap()
        .write_all(&buf)
        .unwrap();
}

fn fs_with_one_file(td: &TempDir) -> (FusionFs, FileCache, u64) {
    let path = td.path().join("payload.bin");
    write_random(&path, FILE_SIZE);

    let mut tree = Tree::new(0);
    let fid = tree
        .add_child(
            ROOT_ID,
            "payload.bin".into(),
            NodeKind::File { backing: path },
            CachedAttrs::synthetic_file(FILE_SIZE as u64),
        )
        .unwrap();
    let cache = new_file_cache();
    let fs = FusionFs::new(Arc::new(RwLock::new(tree)), 0, cache.clone());
    (fs, cache, fid)
}

/// Pick a random offset that leaves room for a full 1 MiB read. Computed
/// outside the timed region so RNG cost doesn't pollute the measurement.
fn random_offset() -> u64 {
    (rand::random::<u32>() as u64) % (FILE_SIZE as u64 - READ_SIZE as u64)
}

fn bench_read_warm(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let td = TempDir::new().unwrap();
    let (fs, _cache, fid) = fs_with_one_file(&td);

    // Prime the cache so the very first iter sees an open()-amortized state.
    rt.block_on(async {
        let _ = fs.read(fid, 0, READ_SIZE).await.unwrap();
    });

    let mut group = c.benchmark_group("read_1mib");
    group.throughput(Throughput::Bytes(READ_SIZE as u64));
    let fs_ref = &fs;
    group.bench_function("warm_cache", |b| {
        // iter_custom so the offset RNG and Instant overhead stay outside
        // the measured `fs.read` call.
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let off = random_offset();
                let t = Instant::now();
                let (buf, _) = fs_ref.read(fid, off, READ_SIZE).await.unwrap();
                total += t.elapsed();
                black_box(buf);
            }
            total
        });
    });
    group.finish();
}

fn bench_read_cold(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let td = TempDir::new().unwrap();
    let (fs, cache, fid) = fs_with_one_file(&td);

    let mut group = c.benchmark_group("read_1mib");
    group.throughput(Throughput::Bytes(READ_SIZE as u64));
    let fs_ref = &fs;
    let cache_ref = &cache;
    group.bench_function("cold_cache", |b| {
        // Cache eviction must run *outside* the timed region — otherwise we
        // can't tell what fraction of the sample is `open(2)` versus the
        // mutex-and-Arc-drop work of clearing the LRU.
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                cache_ref.lock().unwrap().clear();
                let off = random_offset();
                let t = Instant::now();
                let (buf, _) = fs_ref.read(fid, off, READ_SIZE).await.unwrap();
                total += t.elapsed();
                black_box(buf);
            }
            total
        });
    });
    group.finish();
}

criterion_group!(benches, bench_read_warm, bench_read_cold);
criterion_main!(benches);
