//! End-to-end build() throughput against a synthesized media tree on a
//! tempdir. Tests the parallel scan + merge_snapshot + finalize_sort path.
//!
//! Setup (mkdir/touch) happens outside `iter`; only `build()` is timed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;

use fusion::builder;
use fusion::config::{Config, Options, ServerConfig, ShareConfig};

/// Build a layout of `dirs` subdirs each containing `files_per_dir` empty
/// files. Returns the tempdir (kept alive while the bench runs).
fn make_tree(dirs: usize, files_per_dir: usize) -> TempDir {
    let td = TempDir::new().unwrap();
    populate(td.path(), dirs, files_per_dir);
    td
}

fn populate(root: &Path, dirs: usize, files_per_dir: usize) {
    for d in 0..dirs {
        let dir = root.join(format!("Show_{d:04}"));
        fs::create_dir_all(&dir).unwrap();
        for f in 0..files_per_dir {
            fs::File::create(dir.join(format!("S01E{f:02}.mkv"))).unwrap();
        }
    }
}

fn cfg_for(roots: Vec<PathBuf>) -> Config {
    let mut shares = BTreeMap::new();
    shares.insert(
        "Media".to_string(),
        ShareConfig {
            merge: roots,
            subdirs: BTreeMap::new(),
            dedupe_depth: None,
        },
    );
    Config::from_parts(ServerConfig::default(), shares, Options::default()).expect("bench config")
}

fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("build");
    // (label, dirs, files_per_dir) — total file count = dirs * files_per_dir.
    for &(label, dirs, files) in &[("small_200", 20, 10), ("medium_5000", 100, 50)] {
        let td = make_tree(dirs, files);
        let cfg = cfg_for(vec![td.path().to_path_buf()]);
        let total_files = (dirs * files) as u64;
        group.throughput(Throughput::Elements(total_files));
        group.bench_with_input(BenchmarkId::from_parameter(label), &cfg, |b, cfg| {
            b.iter(|| builder::build(cfg, 0).expect("build"));
        });
    }
    group.finish();
}

fn bench_build_parallel_scan(c: &mut Criterion) {
    // Multiple merge roots stress the parallel-scan thread::scope path.
    let mut group = c.benchmark_group("build_parallel");
    let roots: Vec<TempDir> = (0..4).map(|_| make_tree(25, 25)).collect();
    let paths: Vec<PathBuf> = roots.iter().map(|t| t.path().to_path_buf()).collect();
    let cfg = cfg_for(paths);
    group.throughput(Throughput::Elements(4 * 25 * 25));
    group.bench_function("4_roots_625_files_each", |b| {
        b.iter(|| builder::build(&cfg, 0).expect("build"));
    });
    group.finish();
}

criterion_group!(benches, bench_build, bench_build_parallel_scan);
criterion_main!(benches);
