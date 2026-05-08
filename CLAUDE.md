# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`fusion` is a read-only NFSv3 server (built on `nfsserve` 0.11) that exposes a virtual filesystem composed of multiple physical media roots. It's designed for serving media libraries to clients like Infuse over NFS. See `README.md` for user-facing setup; this file covers internals.

## Common commands

```bash
cargo build --release          # release binary at target/release/fusion
cargo run -- --config config.yaml
cargo check                    # fast type-check
cargo clippy --all-targets -- -D warnings
cargo fmt
cargo test                     # ~90 unit tests across all modules
cargo bench                    # criterion benches: build, metadata, read, contention

# Docker
docker compose up --build      # uses host networking (NFS+macOS-Infuse compatibility)
```

Default bind is `0.0.0.0:11111` (non-privileged). Port 2049 is privileged on Linux: grant `CAP_NET_BIND_SERVICE` or set `sysctl net.ipv4.ip_unprivileged_port_start=2049` if you need it.

`RUST_LOG` controls tracing (`info` default; `fusion=debug` is useful when diagnosing watcher/scan issues).

## Architecture

Five modules in `src/`, all wired together in `main.rs`:

- **`config.rs`** — YAML config. A share value can be a single path, a list of paths (unioned), or `{ merge: [paths], subdirs: { name: path } }`. Plus `options`: `hide_patterns` (case-insensitive globs via `globset`, compiled once at load into `Config.hide_set`), `follow_symlinks`, `rescan_interval` (humantime, e.g. `"24h"`). Roots are canonicalized at load time so they match the paths reported by macOS FSEvents (`/private/...`); missing roots warn-but-don't-fail (disks come and go). Validation rejects overlapping merge roots within a share. Tests/benches construct via `Config::from_parts`; production must use `Config::load` (which is what compiles `hide_set`).
- **`tree.rs`** — In-memory virtual filesystem. Flat `Vec<Option<Node>>` where the index *is* the NFS `fileid3`. **Index 0 is reserved (NFS forbids fileid 0); index 1 is always root.** NodeIds are never recycled — keeping them stable across mutations is what makes NFS readdir cookies remain valid. Directories track `subdir_count` (used for NFS `nlink = 2 + subdirs`; over-counting makes macOS `find` skip dirs). Two source kinds: `Synthetic` (root, share roots, intermediate union dirs) and `Physical(Vec<PathBuf>)` (one or more disk paths union into the same virtual dir).
- **`builder.rs`** — Initial tree construction and the snapshot/apply primitives reused by the watcher.
  - `snapshot_dir` does pure disk I/O into a `DirSnapshot` (no tree access, safe in `spawn_blocking`).
  - `merge_snapshot` is **first-root-wins** semantics for initial build (file conflicts: earlier root wins; dir collisions: descend and merge).
  - `apply_snapshot` is the reconciling diff used by the watcher.
  - Initial build is three phases: lay out virtual nodes → fan out `snapshot_dir` per root in `std::thread::scope` → apply sequentially. Directories are kept unsorted during bulk insert and sorted once at the end via `finalize_sort` (avoids O(n²)).
- **`watcher.rs`** — `notify` + `notify-debouncer-full` (2s window) → bounded mpsc → async drainer. The drainer **never holds the tree lock during disk I/O**: it routes events to dirty (root, virtual_id) pairs under a brief read lock, then `spawn_blocking`s `snapshot_dir`, then takes the write lock briefly to apply. Channel-full and OS overflow events both fall back to a full `RescanAll`. After every apply, the file-handle cache is cleared.
- **`vfs.rs`** — `NFSFileSystem` impl. All metadata served from RAM. Only `read` touches disk. An LRU `FileCache` (capacity 64) keyed by NodeId amortizes `open(2)` across Infuse's many ~1 MiB sequential reads per stream; reads use `pread` (`FileExt::read_at`) inside `spawn_blocking` so concurrent readers of the same fd don't serialize. `hint_sequential` issues `posix_fadvise(SEQUENTIAL)` (Linux) / `fcntl(F_RDAHEAD)` (macOS) on first open. All write-ish ops return `NFS3ERR_ROFS`.
- **`attrs.rs`** — `Metadata` ↔ `CachedAttrs` ↔ `fattr3` conversion.

## Things to know before editing

- **Don't recycle NodeIds.** Anywhere you remove a node, leave a `None` in the slot. The id-stability invariant is load-bearing for readdir cookies.
- **Don't hold the tree write lock across disk I/O.** The watcher's snapshot/apply split exists specifically to keep NFS read latency clean during multi-second cold scans. Apply phases must be RAM-only.
- **`subdir_count` must count directories only, not files.** macOS `find` uses `nlink-2` to short-circuit and will skip real subdirs if files are counted.
- **`readdir` cookies are NodeIds.** When `start_after` no longer maps to a child (entry deleted between RPCs), return `NFS3ERR_BAD_COOKIE`, not `end:true` — Linux clients silently truncate listings on the latter.
- **Bump parent `mtime` on add/remove.** Linux NFS uses parent dir mtime as the dentry-cache freshness key; without the bump, `ls` serves stale listings.
- **`follow_symlinks: false` is the safe default.** A symlink inside a media root pointing at `/etc/passwd` would otherwise be served over NFS.
- **`subdirs` was previously called `mount`.** Renamed because "mount" is what the *client* does in NFS-land — using it for a server-side concept is a perpetual source of confusion. Builder/watcher internals (`subdir_names_per_share`, `is_subdir`, log labels) follow the new name.
- **macOS path canonicalization.** FSEvents reports paths under `/private/...`; the config loader canonicalizes roots at startup so `path_index` lookups match. Don't bypass `Config::load`.
- **jemalloc** is the global allocator on non-Windows targets — glibc's mmap/munmap churn on per-RPC 1 MiB read buffers was a measurable hit. Don't remove without re-benchmarking.
