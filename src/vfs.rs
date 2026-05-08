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

use nfsserve::nfs::{cookieverf3, fattr3, fileid3, filename3, nfspath3, nfsstat3, sattr3};
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
        Self {
            tree,
            server_id,
            file_cache,
        }
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
    fn open_cached(
        &self,
        id: NodeId,
        path: &std::path::Path,
    ) -> std::io::Result<Arc<std::fs::File>> {
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

fn name_to_str(name: &filename3) -> Option<&str> {
    // filename3 derefs to bytes via its inner Vec<u8>.
    std::str::from_utf8(name.as_ref()).ok()
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
        tree.child(dirid, name).ok_or(nfsstat3::NFS3ERR_NOENT)
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
                    std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len())
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
        // EOF if we've reached the cached file size, OR if pread returned
        // fewer bytes than asked (hit physical end-of-file). Using stale
        // `file_size` alone could mis-report eof on a file that grew between
        // rescans.
        let eof = offset + buf.len() as u64 >= file_size || buf.len() < want;
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
            let Some(child) = tree.get(*child_id) else {
                continue;
            };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{CachedAttrs, NodeKind, Tree};
    use nfsserve::nfs::ftype3;
    use std::io::Write;
    use std::path::PathBuf;

    fn fs_with(tree: Tree) -> FusionFs {
        let server_id = tree.server_id;
        FusionFs::new(Arc::new(RwLock::new(tree)), server_id, new_file_cache())
    }

    fn fs_with_cache(tree: Tree, cache: FileCache) -> FusionFs {
        let server_id = tree.server_id;
        FusionFs::new(Arc::new(RwLock::new(tree)), server_id, cache)
    }

    /// Add a real file to the tree backed by `path` with the given size.
    fn add_file(tree: &mut Tree, name: &str, path: PathBuf, size: u64) -> NodeId {
        tree.add_child(
            ROOT_ID,
            name.into(),
            NodeKind::File { backing: path },
            CachedAttrs::synthetic_file(size),
        )
        .unwrap()
    }

    fn name(s: &str) -> filename3 {
        filename3::from(s.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn root_dir_returns_root_id() {
        let fs = fs_with(Tree::new(7));
        assert_eq!(fs.root_dir(), ROOT_ID);
    }

    #[tokio::test]
    async fn capabilities_is_read_only() {
        let fs = fs_with(Tree::new(0));
        assert!(matches!(fs.capabilities(), VFSCapabilities::ReadOnly));
    }

    #[tokio::test]
    async fn lookup_dot_and_dotdot() {
        let mut tree = Tree::new(0);
        let child = tree
            .add_child(
                ROOT_ID,
                "sub".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let fs = fs_with(tree);

        assert_eq!(fs.lookup(child, &name(".")).await.unwrap(), child);
        assert_eq!(fs.lookup(child, &name("..")).await.unwrap(), ROOT_ID);
        // Root's parent is root (RFC convention).
        assert_eq!(fs.lookup(ROOT_ID, &name("..")).await.unwrap(), ROOT_ID);
    }

    #[tokio::test]
    async fn lookup_missing_returns_noent() {
        let fs = fs_with(Tree::new(0));
        let err = fs.lookup(ROOT_ID, &name("nope")).await.unwrap_err();
        assert!(matches!(err, nfsstat3::NFS3ERR_NOENT));
    }

    #[tokio::test]
    async fn lookup_on_stale_dirid_returns_stale() {
        let fs = fs_with(Tree::new(0));
        let err = fs.lookup(9999, &name(".")).await.unwrap_err();
        assert!(matches!(err, nfsstat3::NFS3ERR_STALE));
    }

    #[tokio::test]
    async fn getattr_dir_nlink_is_two_plus_subdirs() {
        let mut tree = Tree::new(0);
        let parent = tree
            .add_child(
                ROOT_ID,
                "p".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        for n in ["a", "b", "c"] {
            tree.add_child(
                parent,
                n.into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        }
        // Add a file too — must NOT count toward nlink.
        tree.add_child(
            parent,
            "f".into(),
            NodeKind::File {
                backing: PathBuf::from("/x"),
            },
            CachedAttrs::synthetic_dir(),
        )
        .unwrap();
        let fs = fs_with(tree);
        let attr = fs.getattr(parent).await.unwrap();
        assert!(matches!(attr.ftype, ftype3::NF3DIR));
        assert_eq!(attr.nlink, 2 + 3);
    }

    #[tokio::test]
    async fn getattr_file_nlink_is_one() {
        let mut tree = Tree::new(0);
        let f = tree
            .add_child(
                ROOT_ID,
                "f".into(),
                NodeKind::File {
                    backing: PathBuf::from("/x"),
                },
                CachedAttrs::synthetic_file(0),
            )
            .unwrap();
        let fs = fs_with(tree);
        let attr = fs.getattr(f).await.unwrap();
        assert!(matches!(attr.ftype, ftype3::NF3REG));
        assert_eq!(attr.nlink, 1);
    }

    #[tokio::test]
    async fn readdir_paginates_with_node_id_cookies() {
        let mut tree = Tree::new(0);
        for n in ["a", "b", "c", "d"] {
            tree.add_child(
                ROOT_ID,
                n.into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        }
        let fs = fs_with(tree);

        fn names(r: &ReadDirResult) -> Vec<String> {
            r.entries
                .iter()
                .map(|e| std::str::from_utf8(e.name.as_ref()).unwrap().to_string())
                .collect()
        }

        let first = fs.readdir(ROOT_ID, 0, 2).await.unwrap();
        assert_eq!(names(&first), vec!["a", "b"]);
        assert!(!first.end);
        let last_cookie = first.entries.last().unwrap().fileid;

        let second = fs.readdir(ROOT_ID, last_cookie, 10).await.unwrap();
        assert_eq!(names(&second), vec!["c", "d"]);
        assert!(second.end);
    }

    #[tokio::test]
    async fn readdir_returns_bad_cookie_for_stale_start_after() {
        let mut tree = Tree::new(0);
        let a = tree
            .add_child(
                ROOT_ID,
                "a".into(),
                NodeKind::empty_dir(),
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        tree.add_child(
            ROOT_ID,
            "b".into(),
            NodeKind::empty_dir(),
            CachedAttrs::synthetic_dir(),
        )
        .unwrap();
        // Delete `a`. Its NodeId is now stale as a cookie.
        tree.remove_recursive(a);
        let fs = fs_with(tree);
        let err = fs.readdir(ROOT_ID, a, 10).await.unwrap_err();
        assert!(
            matches!(err, nfsstat3::NFS3ERR_BAD_COOKIE),
            "expected BAD_COOKIE; got {err:?}"
        );
    }

    #[tokio::test]
    async fn readdir_on_file_returns_notdir() {
        let mut tree = Tree::new(0);
        let f = tree
            .add_child(
                ROOT_ID,
                "f".into(),
                NodeKind::File {
                    backing: PathBuf::from("/x"),
                },
                CachedAttrs::synthetic_dir(),
            )
            .unwrap();
        let fs = fs_with(tree);
        let err = fs.readdir(f, 0, 10).await.unwrap_err();
        assert!(matches!(err, nfsstat3::NFS3ERR_NOTDIR));
    }

    #[tokio::test]
    async fn read_on_dir_returns_isdir() {
        let fs = fs_with(Tree::new(0));
        let err = fs.read(ROOT_ID, 0, 10).await.unwrap_err();
        assert!(matches!(err, nfsstat3::NFS3ERR_ISDIR));
    }

    #[tokio::test]
    async fn read_returns_file_content_and_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        let payload = b"hello fusion world";
        std::fs::File::create(&path)
            .unwrap()
            .write_all(payload)
            .unwrap();

        let mut tree = Tree::new(0);
        let mut attrs = CachedAttrs::synthetic_dir();
        attrs.size = payload.len() as u64;
        let fid = tree
            .add_child(
                ROOT_ID,
                "data.bin".into(),
                NodeKind::File {
                    backing: path.clone(),
                },
                attrs,
            )
            .unwrap();
        let fs = fs_with(tree);

        let (buf, eof) = fs.read(fid, 0, 1024).await.unwrap();
        assert_eq!(&buf, payload);
        assert!(eof);

        // Offset >= size: empty + EOF.
        let (buf, eof) = fs.read(fid, payload.len() as u64, 1024).await.unwrap();
        assert!(buf.is_empty());
        assert!(eof);

        // Partial read short of EOF.
        let (buf, eof) = fs.read(fid, 0, 5).await.unwrap();
        assert_eq!(&buf, &payload[..5]);
        assert!(!eof);
    }

    #[tokio::test]
    async fn readlink_is_unsupported() {
        let fs = fs_with(Tree::new(0));
        let err = fs.readlink(ROOT_ID).await.unwrap_err();
        assert!(matches!(err, nfsstat3::NFS3ERR_NOTSUPP));
    }

    #[tokio::test]
    async fn all_mutating_ops_return_rofs() {
        let fs = fs_with(Tree::new(0));
        let n = name("x");
        let blank_sattr = sattr3::default();

        assert!(matches!(
            fs.setattr(ROOT_ID, blank_sattr).await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        assert!(matches!(
            fs.write(ROOT_ID, 0, b"x").await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        assert!(matches!(
            fs.create(ROOT_ID, &n, blank_sattr).await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        assert!(matches!(
            fs.create_exclusive(ROOT_ID, &n).await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        assert!(matches!(
            fs.mkdir(ROOT_ID, &n).await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        assert!(matches!(
            fs.remove(ROOT_ID, &n).await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        assert!(matches!(
            fs.rename(ROOT_ID, &n, ROOT_ID, &n).await.unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
        let blank_path = nfspath3::from(b"/x".to_vec());
        assert!(matches!(
            fs.symlink(ROOT_ID, &n, &blank_path, &blank_sattr)
                .await
                .unwrap_err(),
            nfsstat3::NFS3ERR_ROFS
        ));
    }

    #[tokio::test]
    async fn read_caps_count_at_one_mib() {
        // Even if a client requests more than 1 MiB, we serve at most that
        // per RPC. Build a 2 MiB file, ask for 4 MiB, verify capping.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.bin");
        let payload = vec![0xABu8; 2 * 1024 * 1024];
        std::fs::write(&path, &payload).unwrap();

        let mut tree = Tree::new(0);
        let fid = add_file(&mut tree, "big.bin", path, payload.len() as u64);
        let fs = fs_with(tree);

        let (buf, eof) = fs.read(fid, 0, 4 * 1024 * 1024).await.unwrap();
        assert_eq!(buf.len(), 1024 * 1024, "read must cap at 1 MiB");
        assert!(!eof, "more data remains after the capped read");
    }

    #[tokio::test]
    async fn read_concurrent_on_same_file_both_succeed() {
        // pread is positional and we deliberately don't serialize concurrent
        // readers of the same fd — Infuse playback often issues overlapping
        // RPCs against the same file.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.bin");
        let payload: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
        std::fs::write(&path, &payload).unwrap();

        let mut tree = Tree::new(0);
        let fid = add_file(&mut tree, "concurrent.bin", path, payload.len() as u64);
        let fs = Arc::new(fs_with(tree));

        let (a, b) = tokio::join!(
            {
                let fs = fs.clone();
                async move { fs.read(fid, 0, 1024).await }
            },
            {
                let fs = fs.clone();
                async move { fs.read(fid, 2048, 1024).await }
            },
        );
        let (a_buf, _) = a.unwrap();
        let (b_buf, _) = b.unwrap();
        assert_eq!(a_buf, payload[..1024]);
        assert_eq!(b_buf, payload[2048..3072]);
    }

    #[tokio::test]
    async fn read_caches_open_fd_across_calls() {
        // First read populates the cache; subsequent reads reuse the same
        // entry. Without caching, sustained playback would pay open(2) per
        // RPC — measurable on spinning disks.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cached.bin");
        std::fs::write(&path, b"hello").unwrap();

        let cache = new_file_cache();
        let mut tree = Tree::new(0);
        let fid = add_file(&mut tree, "cached.bin", path, 5);
        let fs = fs_with_cache(tree, cache.clone());

        assert_eq!(cache.lock().unwrap().len(), 0);
        fs.read(fid, 0, 5).await.unwrap();
        assert_eq!(cache.lock().unwrap().len(), 1);
        // Hit the same fid again — len should not grow.
        fs.read(fid, 0, 5).await.unwrap();
        assert_eq!(cache.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn read_evicts_cache_entry_on_io_error() {
        // If pread returns Err (e.g. backing file replaced under us), the
        // stale fd must be evicted so the next read re-opens.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vanish.bin");
        std::fs::write(&path, b"data").unwrap();

        let cache = new_file_cache();
        let mut tree = Tree::new(0);
        // Cache the open fd via a successful first read.
        let fid = add_file(&mut tree, "vanish.bin", path.clone(), 4096);
        let fs = fs_with_cache(tree, cache.clone());
        fs.read(fid, 0, 4).await.unwrap();
        assert_eq!(cache.lock().unwrap().len(), 1);

        // Inject a bogus fd into the cache: open `/` (a directory), which
        // makes `pread` fail with EISDIR. The next read for `fid` will use
        // this entry, fail, and evict.
        let bad = Arc::new(std::fs::File::open("/").unwrap());
        cache.lock().unwrap().put(fid, bad);

        let err = fs.read(fid, 0, 4).await.unwrap_err();
        assert!(matches!(
            err,
            nfsstat3::NFS3ERR_IO | nfsstat3::NFS3ERR_ACCES
        ));
        assert_eq!(
            cache.lock().unwrap().len(),
            0,
            "bad fd must be evicted from cache after read error"
        );
    }

    #[tokio::test]
    async fn file_cache_is_bounded_by_capacity() {
        // The LRU drops the oldest entry once capacity is exceeded — without
        // this, a long-running server with many briefly-touched files would
        // leak fds.
        let dir = tempfile::tempdir().unwrap();
        let cache = new_file_cache();
        let mut tree = Tree::new(0);

        // Capacity is 64; create 65 distinct files and read each once.
        let mut fids = Vec::new();
        for i in 0..65 {
            let p = dir.path().join(format!("f{i}.bin"));
            std::fs::write(&p, b"x").unwrap();
            fids.push(add_file(&mut tree, &format!("f{i}.bin"), p, 1));
        }
        let fs = fs_with_cache(tree, cache.clone());

        for fid in &fids {
            fs.read(*fid, 0, 1).await.unwrap();
        }

        assert_eq!(
            cache.lock().unwrap().len(),
            FILE_CACHE_CAP,
            "cache must never exceed capacity"
        );
        // The first-touched fid is the LRU and must have been evicted by the
        // 65th insertion.
        assert!(
            !cache.lock().unwrap().contains(&fids[0]),
            "least-recently-used entry must have been evicted"
        );
    }

    #[tokio::test]
    async fn lookup_with_non_utf8_filename_returns_noent() {
        // filename3 carries arbitrary bytes; we treat un-decodable bytes as
        // "no such file" rather than 500-style errors.
        let fs = fs_with(Tree::new(0));
        let bad = filename3::from(vec![0xFFu8, 0xFE, 0xFD]);
        let err = fs.lookup(ROOT_ID, &bad).await.unwrap_err();
        assert!(matches!(err, nfsstat3::NFS3ERR_NOENT));
    }

    #[tokio::test]
    async fn serverid_matches_constructor_arg() {
        let tree = Tree::new(0xdead_beef_cafe_babe);
        let fs = fs_with(tree);
        assert_eq!(fs.serverid(), 0xdead_beef_cafe_babe_u64.to_be_bytes());
    }
}
