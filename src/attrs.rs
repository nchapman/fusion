//! Conversion between `std::fs::Metadata`, our `CachedAttrs`, and NFS `fattr3`.

use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nfsserve::nfs::{fattr3, ftype3, nfstime3, specdata3};

use crate::tree::{CachedAttrs, Node, NodeKind};

impl CachedAttrs {
    pub fn from_metadata(md: &Metadata) -> Self {
        Self {
            size: md.len(),
            mtime: md.modified().unwrap_or_else(|_| SystemTime::now()),
            ctime: ctime_from(md),
            atime: md.accessed().unwrap_or_else(|_| SystemTime::now()),
            mode: md.mode() & 0o7777,
        }
    }
}

fn ctime_from(md: &Metadata) -> SystemTime {
    // ctime is "status change time" on Unix; not exposed by std, use the raw
    // unix value.
    let secs = md.ctime();
    let nsecs = md.ctime_nsec() as u32;
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH
    }
}

fn to_nfstime(t: SystemTime) -> nfstime3 {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    nfstime3 {
        seconds: dur.as_secs() as u32,
        nseconds: dur.subsec_nanos(),
    }
}

pub fn fattr3_for(node: &Node, server_id: u64) -> fattr3 {
    let (ftype, mode_default, nlink) = match &node.kind {
        NodeKind::Directory { subdir_count, .. } => {
            // Unix convention: nlink = 2 (`.` + parent's `..` to here) + one
            // per subdirectory's `..`. Files are NOT counted. macOS `find`
            // uses `nlink - 2` to short-circuit traversal, so over-counting
            // makes find skip real subdirectories.
            (ftype3::NF3DIR, 0o555, 2 + *subdir_count)
        }
        NodeKind::File { .. } => (ftype3::NF3REG, 0o444, 1),
    };
    let mode = if node.attrs.mode == 0 {
        mode_default
    } else {
        node.attrs.mode & 0o7777
    };
    fattr3 {
        ftype,
        mode: mode as u32,
        nlink,
        uid: 0,
        gid: 0,
        size: node.attrs.size,
        used: node.attrs.size,
        rdev: specdata3 { specdata1: 0, specdata2: 0 },
        fsid: server_id,
        fileid: node.id,
        atime: to_nfstime(node.attrs.atime),
        mtime: to_nfstime(node.attrs.mtime),
        ctime: to_nfstime(node.attrs.ctime),
    }
}
