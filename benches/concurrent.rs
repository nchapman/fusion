//! Read throughput under N concurrent client streams.
//!
//! The architectural claim is that fusion's read path has no shared mutable
//! state that serializes concurrent readers — `pread` is positional, the
//! tree is behind a `RwLock` so multiple read locks coexist, the file LRU
//! is `Mutex`'d but only briefly during get/put, and reads happen on
//! `spawn_blocking` workers so they don't share an event loop.
//!
//! This bench fans out N concurrent reader tasks doing 1 MiB reads at
//! random offsets across many backing files (one Infuse playback ≈ one
//! reader; many devices in a household ≈ many readers). If aggregate
//! throughput scales roughly linearly with N until CPU/disk saturates,
//! the design holds. If it plateaus early, there's a hidden lock or
//! contention point worth finding.
//!
//! Run with `cargo bench --bench concurrent`. The `same_file` group
//! amplifies the shared-fd case (one file, N readers) so any
//! per-file serialization would show up; the `many_files` group is the
//! realistic case (each reader hits a different file).

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

use fusion::tree::{CachedAttrs, NodeId, NodeKind, Tree, ROOT_ID};
use fusion::vfs::{new_file_cache, FusionFs};

const FILE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
const READ_SIZE: u32 = 1024 * 1024; // 1 MiB — Infuse playback chunk
const FILE_COUNT: usize = 64; // matches the FileCache capacity

/// Concurrency levels to sweep. 1 is the baseline; production hardware with
/// even modest core counts should scale through 16 cleanly. 64 stresses the
/// tokio runtime more than the read path.
const N_VALUES: &[usize] = &[1, 4, 16, 64];

fn write_random(path: &Path, len: usize) {
    let mut buf = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut buf);
    std::fs::File::create(path)
        .unwrap()
        .write_all(&buf)
        .unwrap();
}

/// Build a tree with `FILE_COUNT` real files (each `FILE_SIZE` bytes of
/// random data) under one share. Returns the FusionFs plus the list of
/// fileids for the bench harness to pick from.
fn fs_with_many_files(td: &TempDir) -> (Arc<FusionFs>, Vec<NodeId>) {
    let mut tree = Tree::new(0);
    let mut fids = Vec::with_capacity(FILE_COUNT);
    for i in 0..FILE_COUNT {
        let path = td.path().join(format!("f{i:04}.bin"));
        write_random(&path, FILE_SIZE);
        let fid = tree
            .add_child(
                ROOT_ID,
                format!("f{i:04}.bin"),
                NodeKind::File { backing: path },
                CachedAttrs::synthetic_file(FILE_SIZE as u64),
            )
            .unwrap();
        fids.push(fid);
    }
    let fs = FusionFs::new(Arc::new(RwLock::new(tree)), 0, new_file_cache());
    (Arc::new(fs), fids)
}

/// Pick a random in-bounds 1 MiB offset.
fn random_offset() -> u64 {
    (rand::random::<u32>() as u64) % (FILE_SIZE as u64 - READ_SIZE as u64)
}

/// Multi-threaded runtime; we want real OS-thread concurrency so the
/// `spawn_blocking` reads can actually run in parallel. 8 worker threads
/// is enough to oversubscribe to the N values we sweep without thrashing
/// scheduler decisions.
fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap()
}

/// Each criterion sample fans out `n` reader tasks and waits for all to
/// finish; the elapsed wall time is the per-iter duration. Throughput is
/// reported as `n * READ_SIZE` so criterion prints aggregate MiB/s — that's
/// what scales (or fails to scale) with N.
fn bench_concurrent(c: &mut Criterion) {
    let rt = make_runtime();
    let td = TempDir::new().unwrap();
    let (fs, fids) = fs_with_many_files(&td);

    // Prime the LRU so we measure steady-state read throughput, not the
    // first-touch open(2) of every file.
    rt.block_on(async {
        for &fid in &fids {
            let _ = fs.read(fid, 0, READ_SIZE).await.unwrap();
        }
    });

    // ---- many_files: each reader picks a different file. The realistic
    // multi-client case. Validates pread + tree RwLock + spawn_blocking
    // pool all scale with reader count.
    let mut group = c.benchmark_group("concurrent_read_many_files");
    for &n in N_VALUES {
        group.throughput(Throughput::Bytes(n as u64 * READ_SIZE as u64));
        let fs_ref = fs.clone();
        let fids_ref = fids.clone();
        group.bench_function(format!("n_{n}"), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let fs = fs_ref.clone();
                let fids = fids_ref.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut handles = Vec::with_capacity(n);
                        let t = Instant::now();
                        for i in 0..n {
                            let fs = fs.clone();
                            // Stride through the file list so no two
                            // concurrent readers hit the same fid; this
                            // isolates "many files in flight" from
                            // "same-fd contention" (covered separately).
                            let fid = fids[i % fids.len()];
                            let off = random_offset();
                            handles.push(tokio::spawn(async move {
                                fs.read(fid, off, READ_SIZE).await.unwrap()
                            }));
                        }
                        for h in handles {
                            black_box(h.await.unwrap());
                        }
                        total += t.elapsed();
                    }
                    total
                }
            });
        });
    }
    group.finish();

    // ---- same_file: every reader hammers the SAME fid. Stresses the
    // shared-fd path (Arc<File> + pread). Should still scale because
    // pread is positional — if it doesn't, we've found per-fd
    // serialization (e.g. an accidental Mutex<File>).
    let mut group = c.benchmark_group("concurrent_read_same_file");
    let same_fid = fids[0];
    for &n in N_VALUES {
        group.throughput(Throughput::Bytes(n as u64 * READ_SIZE as u64));
        let fs_ref = fs.clone();
        group.bench_function(format!("n_{n}"), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let fs = fs_ref.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut handles = Vec::with_capacity(n);
                        let t = Instant::now();
                        for _ in 0..n {
                            let fs = fs.clone();
                            let off = random_offset();
                            handles.push(tokio::spawn(async move {
                                fs.read(same_fid, off, READ_SIZE).await.unwrap()
                            }));
                        }
                        for h in handles {
                            black_box(h.await.unwrap());
                        }
                        total += t.elapsed();
                    }
                    total
                }
            });
        });
    }
    group.finish();

    // ---- mixed_metadata: lookup + getattr from N concurrent tasks.
    // Validates the read-locked metadata path under load. NFS clients
    // typically issue a lot more metadata RPCs than reads (READDIRPLUS
    // bursts on directory navigation) so this is the workload-shape
    // most likely to surface a tree-lock bottleneck.
    let mut group = c.benchmark_group("concurrent_metadata");
    let names: Vec<nfsserve::nfs::filename3> = (0..FILE_COUNT)
        .map(|i| nfsserve::nfs::filename3::from(format!("f{i:04}.bin").into_bytes()))
        .collect();
    let names = Arc::new(names);
    for &n in N_VALUES {
        // One metadata op per task in the inner fan-out.
        group.throughput(Throughput::Elements(n as u64));
        let fs_ref = fs.clone();
        let names_ref = names.clone();
        let fids_ref = fids.clone();
        group.bench_function(format!("n_{n}"), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let fs = fs_ref.clone();
                let names = names_ref.clone();
                let fids = fids_ref.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut handles = Vec::with_capacity(n);
                        let t = Instant::now();
                        for i in 0..n {
                            let fs = fs.clone();
                            let names = names.clone();
                            let fids = fids.clone();
                            // Alternate lookup vs getattr so neither path
                            // dominates the measurement.
                            let do_lookup = i % 2 == 0;
                            let idx = i % FILE_COUNT;
                            handles.push(tokio::spawn(async move {
                                if do_lookup {
                                    fs.lookup(ROOT_ID, &names[idx]).await.unwrap();
                                } else {
                                    fs.getattr(fids[idx]).await.unwrap();
                                }
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                        total += t.elapsed();
                    }
                    total
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_concurrent);
criterion_main!(benches);
