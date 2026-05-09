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

#[cfg(test)]
fn ctime_from_for_test(md: &Metadata) -> SystemTime {
    ctime_from(md)
}

#[cfg(test)]
fn to_nfstime_for_test(t: SystemTime) -> nfstime3 {
    to_nfstime(t)
}

pub fn fattr3_for(node: &Node, server_id: u64) -> fattr3 {
    let (ftype, nlink) = match &node.kind {
        NodeKind::Directory { subdir_count, .. } => {
            // Unix convention: nlink = 2 (`.` + parent's `..` to here) + one
            // per subdirectory's `..`. Files are NOT counted. macOS `find`
            // uses `nlink - 2` to short-circuit traversal, so over-counting
            // makes find skip real subdirectories.
            (ftype3::NF3DIR, 2 + *subdir_count)
        }
        NodeKind::File { .. } => (ftype3::NF3REG, 1),
    };
    // `attrs.mode` is always populated: synthetic constructors set 0o555/0o444,
    // `from_metadata` propagates the on-disk mode. No fallback needed.
    let mode = node.attrs.mode & 0o7777;
    fattr3 {
        ftype,
        mode,
        nlink,
        uid: 0,
        gid: 0,
        size: node.attrs.size,
        used: node.attrs.size,
        rdev: specdata3 {
            specdata1: 0,
            specdata2: 0,
        },
        fsid: server_id,
        fileid: node.id,
        atime: to_nfstime(node.attrs.atime),
        mtime: to_nfstime(node.attrs.mtime),
        ctime: to_nfstime(node.attrs.ctime),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{CachedAttrs, DirSources, FastMap, Node, NodeKind, ROOT_ID};
    use std::path::PathBuf;
    use std::time::Duration;

    fn dir_node(id: u64, subdir_count: u32) -> Node {
        Node {
            id,
            parent: Some(ROOT_ID),
            name: "d".into(),
            kind: NodeKind::Directory {
                by_name: FastMap::default(),
                ordered: Vec::new(),
                sorted: true,
                subdir_count,
                sources: DirSources::Synthetic,
                shadows: None,
            },
            attrs: CachedAttrs::synthetic_dir(),
            winner_priority: None,
        }
    }

    fn file_node(id: u64, size: u64) -> Node {
        let mut attrs = CachedAttrs::synthetic_file(size);
        // High bits should get masked off by fattr3_for.
        attrs.mode = 0o100_644;
        Node {
            id,
            parent: Some(ROOT_ID),
            name: "f".into(),
            kind: NodeKind::File {
                backing: PathBuf::from("/x"),
            },
            attrs,
            winner_priority: None,
        }
    }

    #[test]
    fn from_metadata_pulls_size_and_mode_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        std::fs::write(&path, b"twelve bytes").unwrap();
        let md = std::fs::metadata(&path).unwrap();

        let attrs = CachedAttrs::from_metadata(&md);
        assert_eq!(attrs.size, 12);
        // Mode is post-masked: only permission/sticky/setuid bits remain.
        assert_eq!(attrs.mode & !0o7777, 0);
        // The mtime read from the file must round-trip back to its
        // SystemTime representation (i.e. we didn't drop sub-second info).
        assert_eq!(attrs.mtime, md.modified().unwrap());
    }

    #[test]
    fn fattr3_for_dir_nlink_is_two_plus_subdirs() {
        let node = dir_node(5, 3);
        let attr = fattr3_for(&node, 7);
        assert!(matches!(attr.ftype, ftype3::NF3DIR));
        assert_eq!(attr.nlink, 5);
        assert_eq!(attr.fileid, 5);
        assert_eq!(attr.fsid, 7);
    }

    #[test]
    fn fattr3_for_dir_with_no_subdirs_has_nlink_two() {
        let node = dir_node(2, 0);
        assert_eq!(fattr3_for(&node, 0).nlink, 2);
    }

    #[test]
    fn fattr3_for_file_is_nf3reg_with_nlink_one() {
        let node = file_node(9, 4096);
        let attr = fattr3_for(&node, 0);
        assert!(matches!(attr.ftype, ftype3::NF3REG));
        assert_eq!(attr.nlink, 1);
        assert_eq!(attr.size, 4096);
        // `used` mirrors size — we don't track sparse-file allocation.
        assert_eq!(attr.used, 4096);
    }

    #[test]
    fn fattr3_for_masks_high_mode_bits() {
        // file_node sets mode = 0o100_644 (regular-file type bit + 0o644).
        // fattr3 carries permission bits only; the type lives in `ftype`.
        let node = file_node(1, 0);
        let attr = fattr3_for(&node, 0);
        assert_eq!(attr.mode, 0o644);
    }

    #[test]
    fn fattr3_for_uid_gid_and_rdev_are_zero() {
        // Files are read-only and not character/block devices; the server
        // attributes everything to root with no special device numbers.
        let attr = fattr3_for(&file_node(1, 0), 0);
        assert_eq!(attr.uid, 0);
        assert_eq!(attr.gid, 0);
        assert_eq!(attr.rdev.specdata1, 0);
        assert_eq!(attr.rdev.specdata2, 0);
    }

    #[test]
    fn to_nfstime_round_trips_seconds_and_nanoseconds() {
        let t = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let n = to_nfstime_for_test(t);
        assert_eq!(n.seconds, 1_700_000_000);
        assert_eq!(n.nseconds, 123_456_789);
    }

    #[test]
    fn to_nfstime_for_unix_epoch_is_zero() {
        let n = to_nfstime_for_test(UNIX_EPOCH);
        assert_eq!(n.seconds, 0);
        assert_eq!(n.nseconds, 0);
    }

    #[test]
    fn to_nfstime_for_pre_epoch_clamps_to_zero() {
        // `duration_since(UNIX_EPOCH)` errors on pre-epoch SystemTimes;
        // we fall back to Duration::default() rather than panicking.
        let pre = UNIX_EPOCH - Duration::from_secs(1);
        let n = to_nfstime_for_test(pre);
        assert_eq!(n.seconds, 0);
        assert_eq!(n.nseconds, 0);
    }

    #[test]
    fn ctime_from_real_file_is_at_or_after_epoch() {
        // We can't construct a Metadata with a negative ctime from safe
        // code, but we can verify the happy path: a file created right
        // now has a ctime that's strictly after the Unix epoch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"").unwrap();
        let md = std::fs::metadata(&path).unwrap();
        let ct = ctime_from_for_test(&md);
        assert!(ct >= UNIX_EPOCH);
    }
}
