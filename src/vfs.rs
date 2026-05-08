//! `NFSFileSystem` impl backed by the in-memory `Tree`.
//!
//! All directory metadata operations are served from RAM. Only `read` opens
//! and reads the backing physical file.

use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lru::LruCache;
use tokio::sync::RwLock;
use tracing::warn;

use nfsserve::nfs::{
    cookieverf3, fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3,
    specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};

use crate::attrs::fattr3_for;
use crate::tree::{NodeId, NodeKind, Tree, ROOT_ID};

/// LRU cache of opened backing files keyed by virtual NodeId.
///
/// Sustained Infuse playback issues many ~1 MiB READ RPCs per file; without a
/// cache, every RPC pays open + (implicit fstat) + close. With a cache, the
/// open cost amortizes across the entire playback. We use `pread` (via
/// `FileExt::read_at`) inside `spawn_blocking` so concurrent reads of the
/// same file don't serialize on a per-file mutex — the kernel handles
/// concurrency.
///
/// The cache is shared with the watcher: after every reconciliatory rescan
/// it's cleared, evicting any entries whose backing file may have been
/// replaced or removed. Cache rebuild costs one open per active stream;
/// the kernel page cache survives, so this is cheap.
pub type FileCache = Arc<Mutex<LruCache<NodeId, Arc<std::fs::File>>>>;

const FILE_CACHE_CAP: usize = 64;

pub fn new_file_cache() -> FileCache {
    Arc::new(Mutex::new(LruCache::new(
        NonZeroUsize::new(FILE_CACHE_CAP).expect("nonzero"),
    )))
}

/// Tell the kernel to expect sequential reads from this fd, biasing its
/// readahead toward larger windows. Linux: `posix_fadvise(SEQUENTIAL)`.
/// macOS: `fcntl(F_RDAHEAD, 1)`. Best-effort; ignore the return code.
fn hint_sequential(file: &std::fs::File) {
    let fd = file.as_raw_fd();
    #[cfg(target_os = "linux")]
    unsafe {
        libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
    #[cfg(target_os = "macos")]
    unsafe {
        libc::fcntl(fd, libc::F_RDAHEAD, 1);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let _ = fd;
}

pub struct FusionFs {
    pub tree: Arc<RwLock<Tree>>,
    server_id: u64,
    file_cache: FileCache,
}

impl FusionFs {
    pub fn new(tree: Arc<RwLock<Tree>>, server_id: u64, file_cache: FileCache) -> Self {
        Self { tree, server_id, file_cache }
    }

    /// Get a cached `File` or open it and cache it. The returned Arc is safe
    /// to use across threads concurrently because `pread` is positional.
    ///
    /// We open *outside* the cache mutex so a slow `open(2)` (e.g. spinning
    /// disk seek) doesn't block other lookups. The cost is a benign race:
    /// two concurrent first-readers of the same file may both call `open`.
    /// We resolve by re-checking the cache after open and preferring the
    /// already-cached entry — the loser's `File` is dropped, closing its
    /// fd. Bounded to one duplicate per concurrent first-access burst.
    fn open_cached(&self, id: NodeId, path: &std::path::Path) -> std::io::Result<Arc<std::fs::File>> {
        if let Some(f) = self.file_cache.lock().unwrap().get(&id).cloned() {
            return Ok(f);
        }
        let file = std::fs::File::open(path)?;
        // Hint the kernel to prefetch aggressively. Infuse playback is
        // sequential 1 MiB chunks; with this hint, spinning disks roughly
        // double effective throughput by letting the kernel readahead
        // pull large windows ahead of our reads. Best-effort — failures
        // are non-fatal.
        hint_sequential(&file);
        let file = Arc::new(file);
        let mut cache = self.file_cache.lock().unwrap();
        if let Some(existing) = cache.get(&id).cloned() {
            // Lost the race. Drop our `file` (closes its fd) and use the
            // entry already in the cache.
            return Ok(existing);
        }
        cache.put(id, file.clone());
        Ok(file)
    }
}

fn name_to_str(name: &filename3) -> Option<String> {
    // filename3 derefs to bytes via its inner Vec<u8>.
    std::str::from_utf8(name.as_ref()).ok().map(str::to_string)
}

#[async_trait]
impl NFSFileSystem for FusionFs {
    fn root_dir(&self) -> fileid3 {
        ROOT_ID
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = name_to_str(filename).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let tree = self.tree.read().await;
        // Special pseudo-entries.
        if name == "." {
            tree.get(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
            return Ok(dirid);
        }
        if name == ".." {
            let node = tree.get(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
            return Ok(node.parent.unwrap_or(ROOT_ID));
        }
        tree.child(dirid, &name).ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let tree = self.tree.read().await;
        let node = tree.get(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
        Ok(fattr3_for(node, tree.server_id))
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        // Read backing path and the cached size from the tree under a read
        // lock briefly. We use the cached size (rather than fstat) so we
        // don't pay an extra syscall per RPC.
        //
        // Tradeoff: `file_size` reflects the last watcher rescan. If the
        // file grows between rescans, we'll early-return EOF for offsets
        // beyond the stale size. This is fine for media libraries (files
        // are static once written; growth happens during a download which
        // a watcher event will pick up). Not safe for live-growing files
        // like log tails — out of scope for this server.
        let (backing, file_size) = {
            let tree = self.tree.read().await;
            let node = tree.get(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
            match &node.kind {
                NodeKind::File { backing } => (backing.clone(), node.attrs.size),
                NodeKind::Directory { .. } => return Err(nfsstat3::NFS3ERR_ISDIR),
            }
        };

        if offset >= file_size {
            return Ok((Vec::new(), true));
        }

        let file = self.open_cached(id, &backing).map_err(|e| {
            // Don't include host path — a misbehaving client could enumerate
            // by hammering stale fileids. fileid is enough.
            warn!(fileid = id, error = %e, "read open failed");
            io_to_nfs(&e)
        })?;

        // Cap to 1 MiB per RPC.
        const MAX_READ: u32 = 1 << 20;
        let count = count.min(MAX_READ);
        let want = (count as u64).min(file_size - offset) as usize;

        // pread (positional read) on a blocking thread — concurrent readers
        // of the same file don't serialize.
        let read_result = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
            let mut buf: Vec<u8> = Vec::with_capacity(want);
            // Loop in case pread returns short.
            let mut total = 0usize;
            while total < want {
                let spare = buf.spare_capacity_mut();
                let dst: &mut [u8] = unsafe {
                    std::slice::from_raw_parts_mut(
                        spare.as_mut_ptr() as *mut u8,
                        spare.len(),
                    )
                };
                let n = file.read_at(dst, offset + total as u64)?;
                if n == 0 {
                    break;
                }
                unsafe { buf.set_len(total + n) };
                total += n;
            }
            Ok(buf)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let buf = match read_result {
            Ok(b) => b,
            Err(e) => {
                // Cached FD may be stale (file replaced under us); evict.
                self.file_cache.lock().unwrap().pop(&id);
                warn!(fileid = id, error = %e, "read failed; evicting cache entry");
                return Err(io_to_nfs(&e));
            }
        };
        let eof = offset + buf.len() as u64 >= file_size;
        Ok((buf, eof))
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let tree = self.tree.read().await;
        let node = tree.get(dirid).ok_or(nfsstat3::NFS3ERR_STALE)?;
        let ordered = match &node.kind {
            NodeKind::Directory { ordered, .. } => ordered,
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        };

        // `start_after` is the cookie: 0 means "from the beginning", otherwise
        // it's the fileid of the last entry the client received. NodeIds are
        // stable for the process lifetime so this resolves uniquely.
        //
        // If the cookie no longer matches an entry in this directory (because
        // the file was deleted between RPCs), RFC 1813 §3.3.16 requires we
        // return NFS3ERR_BAD_COOKIE so the client restarts pagination from
        // the beginning. Returning `end:true` here would silently truncate
        // the listing on Linux clients mid-stream.
        let start = if start_after == 0 {
            0
        } else {
            match ordered.iter().position(|(_, id)| *id == start_after) {
                Some(p) => p + 1,
                None => return Err(nfsstat3::NFS3ERR_BAD_COOKIE),
            }
        };

        let mut entries = Vec::with_capacity(max_entries.min(ordered.len() - start));
        for (name, child_id) in ordered.iter().skip(start) {
            if entries.len() >= max_entries {
                break;
            }
            let Some(child) = tree.get(*child_id) else { continue };
            entries.push(DirEntry {
                fileid: *child_id,
                name: filename3::from(name.as_bytes().to_vec()),
                attr: fattr3_for(child, tree.server_id),
            });
        }
        let end = start + entries.len() >= ordered.len();
        Ok(ReadDirResult { entries, end })
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        // No symlinks in v1.
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    fn serverid(&self) -> cookieverf3 {
        // Stable across the process lifetime; randomized at startup.
        self.server_id.to_be_bytes()
    }

    // ---- Read-only stubs ----

    async fn setattr(&self, _id: fileid3, _attr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
}

fn io_to_nfs(e: &std::io::Error) -> nfsstat3 {
    use std::io::ErrorKind::*;
    match e.kind() {
        NotFound => nfsstat3::NFS3ERR_NOENT,
        PermissionDenied => nfsstat3::NFS3ERR_ACCES,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

// Suppress unused-import warning for types used only in trait bounds.
#[allow(dead_code)]
fn _types(_a: ftype3, _b: nfstime3, _c: specdata3) {}
