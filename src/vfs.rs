//! `NFSFileSystem` impl backed by the in-memory `Tree`.
//!
//! All directory metadata operations are served from RAM. Only `read` opens
//! and reads the backing physical file.

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;
use tracing::warn;

use nfsserve::nfs::{
    cookieverf3, fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3,
    specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};

use crate::attrs::fattr3_for;
use crate::tree::{NodeKind, Tree, ROOT_ID};

pub struct FusionFs {
    pub tree: Arc<RwLock<Tree>>,
    server_id: u64,
}

impl FusionFs {
    pub fn new(tree: Arc<RwLock<Tree>>, server_id: u64) -> Self {
        Self { tree, server_id }
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
        let backing: PathBuf = {
            let tree = self.tree.read().await;
            let node = tree.get(id).ok_or(nfsstat3::NFS3ERR_STALE)?;
            match &node.kind {
                NodeKind::File { backing } => backing.clone(),
                NodeKind::Directory { .. } => return Err(nfsstat3::NFS3ERR_ISDIR),
            }
        };

        let mut file = tokio::fs::File::open(&backing).await.map_err(|e| {
            // Don't include the host path in client-triggered logs — a
            // misbehaving client can enumerate by hammering stale fileids.
            // The fileid is enough to correlate with the tree if needed.
            warn!(fileid = id, error = %e, "read open failed");
            io_to_nfs(&e)
        })?;
        let metadata = file.metadata().await.map_err(|e| io_to_nfs(&e))?;
        let file_len = metadata.len();

        if offset >= file_len {
            return Ok((Vec::new(), true));
        }
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| io_to_nfs(&e))?;

        // Cap to 1 MiB to bound per-request memory; clients that ask for more
        // will simply make multiple READ RPCs.
        const MAX_READ: u32 = 1 << 20;
        let count = count.min(MAX_READ);
        let want = (count as u64).min(file_len - offset) as usize;

        // Allocate uninitialized — we'll fill and `set_len` only after the
        // bytes are actually written by `read`. Avoids the per-RPC zero-fill
        // of (up to) 1 MiB during playback.
        let mut buf: Vec<u8> = Vec::with_capacity(want);
        let mut total = 0usize;
        while total < want {
            // Safety: we treat `&mut [MaybeUninit<u8>]` slice via the spare
            // capacity, then advance `set_len` only by `n` bytes that the
            // kernel reported it filled.
            let spare = buf.spare_capacity_mut();
            // tokio::io::AsyncReadExt::read takes &mut [u8]; we need to
            // expose the uninit tail as initialized-typed bytes. Use
            // ReadBuf via tokio's AsyncReadExt directly:
            let dst: &mut [u8] = unsafe {
                std::slice::from_raw_parts_mut(
                    spare.as_mut_ptr() as *mut u8,
                    spare.len(),
                )
            };
            let n = file.read(dst).await.map_err(|e| io_to_nfs(&e))?;
            if n == 0 {
                break;
            }
            // Safety: the kernel just wrote `n` bytes into the spare
            // capacity beginning at offset `total`.
            unsafe { buf.set_len(total + n) };
            total += n;
        }
        let eof = offset + total as u64 >= file_len;
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
