//! In-memory virtual filesystem tree.
//!
//! Nodes are stored in a flat `Vec<Option<Node>>` indexed by `NodeId`.
//! `NodeId` doubles as the NFS `fileid3`. Index 0 is reserved (NFS forbids
//! fileid 0); index 1 is always the root.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// HashMap with ahash. Names and paths hash 3–5× faster than the std SipHash
/// default for our key sizes; not DoS-resistant, which is fine for an
/// internal data structure (no client-controlled keys reach these maps).
pub type FastMap<K, V> = HashMap<K, V, ahash::RandomState>;

pub type NodeId = u64;

pub const ROOT_ID: NodeId = 1;

#[derive(Debug, Clone)]
pub struct CachedAttrs {
    pub size: u64,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub atime: SystemTime,
    pub mode: u32,
}

impl CachedAttrs {
    /// True iff the fields a NFS client can observe and revalidate against
    /// differ. NFSv3 clients key dentry-cache freshness on mtime and act on
    /// size + mode for getattr; ctime/atime are not load-bearing for cache
    /// invalidation here. Centralizing the comparison keeps the in-place
    /// attrs-update branches in `apply_snapshot` from drifting (e.g. by
    /// silently ignoring `chmod`).
    pub fn differs_visibly(&self, other: &CachedAttrs) -> bool {
        self.size != other.size || self.mtime != other.mtime || self.mode != other.mode
    }

    pub fn synthetic_dir() -> Self {
        let now = SystemTime::now();
        Self {
            size: 4096,
            mtime: now,
            ctime: now,
            atime: now,
            mode: 0o555,
        }
    }

    pub fn synthetic_file(size: u64) -> Self {
        let now = SystemTime::now();
        Self {
            size,
            mtime: now,
            ctime: now,
            atime: now,
            mode: 0o444,
        }
    }
}

#[derive(Debug)]
pub enum NodeKind {
    Directory {
        /// Name → child id, for O(1) lookup by name.
        by_name: FastMap<String, NodeId>,
        /// Children for stable readdir pagination. Sorted by name once the
        /// directory is finalized (`sorted == true`); during initial bulk
        /// build it's unordered to avoid O(n²) insertion.
        ordered: Vec<(String, NodeId)>,
        sorted: bool,
        /// Number of *directory* children. Maintained incrementally so that
        /// NFS `nlink` (`2 + subdirs`, the Unix convention) doesn't require
        /// an O(children) scan per `getattr`. macOS `find` uses `nlink-2` to
        /// short-circuit traversal, so over-counting (e.g. counting files)
        /// makes `find` miss subdirectories.
        subdir_count: u32,
        sources: DirSources,
        /// Losers at each name: files and directories that didn't win the
        /// initial collision but should be promoted if the current winner
        /// disappears. Boxed + lazily allocated so directories without any
        /// collisions (the common case) pay only one pointer of overhead,
        /// and the `Directory` variant stays a similar size to `File`.
        shadows: Option<Box<DirShadows>>,
    },
    File {
        backing: PathBuf,
    },
}

/// Per-directory shadow data. Allocated only when a name actually has a
/// shadow recorded; cleared back to `None` once the last shadow is popped.
#[derive(Debug, Default)]
pub struct DirShadows {
    /// Sorted ascending by priority (position 0 = highest precedence).
    pub files: FastMap<String, Vec<ShadowFile>>,
    pub dirs: FastMap<String, Vec<ShadowDir>>,
}

/// A losing file at a name in a virtual directory. Records enough to install
/// the file as a winner if the current winner disappears.
#[derive(Debug, Clone)]
pub struct ShadowFile {
    /// Index of the source root in its share's `merge` list. Lower wins.
    pub priority: usize,
    pub backing: PathBuf,
    pub attrs: CachedAttrs,
}

/// A losing directory at a name in a virtual directory. Stores only the
/// physical path of the loser root — the subtree is re-snapshotted from
/// disk if/when the shadow is promoted.
#[derive(Debug, Clone)]
pub struct ShadowDir {
    pub priority: usize,
    pub physical: PathBuf,
}

#[derive(Debug, Clone)]
pub enum DirSources {
    /// No physical backing — root, share roots, and intermediate union dirs
    /// that exist purely as namespace.
    Synthetic,
    /// One or more physical directories that union into this virtual dir.
    Physical(Vec<PathBuf>),
}

impl NodeKind {
    /// Empty directory with `Synthetic` sources and `sorted=true` invariant.
    /// Bulk-build paths flip `sorted` to false via `mark_unsorted` so they
    /// can append in O(1) and sort once at the end.
    pub fn empty_dir() -> Self {
        Self::Directory {
            by_name: FastMap::default(),
            ordered: Vec::new(),
            sorted: true,
            subdir_count: 0,
            sources: DirSources::Synthetic,
            shadows: None,
        }
    }
}

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub name: String,
    pub kind: NodeKind,
    pub attrs: CachedAttrs,
    /// Priority of the source root that owns this node, if applicable.
    /// `None` for synthetic dirs (root, share roots, intermediate union
    /// dirs) and for multi-source merged directories where the entry is
    /// genuinely shared between roots — demotion makes no sense in that
    /// case. `Some(p)` for files (always single-owner) and for single-
    /// source physical dirs (deduped or only-one-root contributed).
    /// `apply_snapshot` consults this to decide whether an incoming
    /// higher-priority root should demote the current owner.
    pub winner_priority: Option<usize>,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        matches!(self.kind, NodeKind::Directory { .. })
    }
}

pub struct Tree {
    nodes: Vec<Option<Node>>,
    /// Reverse index from physical path → virtual node id, for watcher
    /// lookups. Both file and directory paths are registered. Mutated only
    /// via `index_file` and the directory-source helpers; readers go
    /// through `lookup_path`.
    path_index: FastMap<PathBuf, NodeId>,
    /// Stable verifier returned to clients; changes only on process restart.
    pub server_id: u64,
}

impl Tree {
    pub fn new(server_id: u64) -> Self {
        let root = Node {
            id: ROOT_ID,
            parent: None,
            name: String::new(),
            kind: NodeKind::Directory {
                by_name: FastMap::default(),
                ordered: Vec::new(),
                sorted: true,
                subdir_count: 0,
                sources: DirSources::Synthetic,
                shadows: None,
            },
            attrs: CachedAttrs::synthetic_dir(),
            winner_priority: None,
        };
        Self {
            nodes: vec![None, Some(root)],
            path_index: FastMap::default(),
            server_id,
        }
    }

    pub fn get(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(id as usize).and_then(|o| o.as_ref())
    }

    pub fn get_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id as usize).and_then(|o| o.as_mut())
    }

    /// Allocate a fresh, never-reused NodeId. Stable for the process lifetime,
    /// which keeps NFS readdir cookies (which are fileids in nfsserve) valid
    /// across watcher mutations.
    fn alloc_id(&mut self) -> NodeId {
        let id = self.nodes.len() as NodeId;
        self.nodes.push(None);
        id
    }

    /// Allocate and insert a new child under `parent`. Returns the new id, or
    /// `None` if the parent already has a child with that name (caller decides
    /// what to do — log shadow for files, descend-to-merge for dirs).
    ///
    /// If the parent's `ordered` is currently sorted, maintains the invariant
    /// (O(n) insertion). If not, appends and leaves `sorted=false` for the
    /// caller to finalize later.
    pub fn add_child(
        &mut self,
        parent: NodeId,
        name: String,
        kind: NodeKind,
        attrs: CachedAttrs,
    ) -> Option<NodeId> {
        match &self.get(parent)?.kind {
            NodeKind::Directory { by_name, .. } => {
                if by_name.contains_key(&name) {
                    return None;
                }
            }
            _ => return None,
        }
        let id = self.alloc_id();
        let child_is_dir = matches!(kind, NodeKind::Directory { .. });
        let node = Node {
            id,
            parent: Some(parent),
            name: name.clone(),
            kind,
            attrs,
            winner_priority: None,
        };
        self.nodes[id as usize] = Some(node);

        let parent_node = self.get_mut(parent).expect("parent disappeared");
        if let NodeKind::Directory {
            by_name,
            ordered,
            sorted,
            subdir_count,
            ..
        } = &mut parent_node.kind
        {
            by_name.insert(name.clone(), id);
            if *sorted {
                let pos = ordered
                    .binary_search_by(|(n, _)| n.as_str().cmp(name.as_str()))
                    .unwrap_or_else(|p| p);
                ordered.insert(pos, (name, id));
            } else {
                ordered.push((name, id));
            }
            if child_is_dir {
                *subdir_count += 1;
            }
        }
        // Linux NFS clients use parent mtime as the dentry-cache freshness
        // key (RFC 1813 §2): without this bump, `ls` may serve stale listings
        // until the cache TTL expires.
        let now = SystemTime::now();
        parent_node.attrs.mtime = now;
        parent_node.attrs.ctime = now;
        Some(id)
    }

    /// Mark a directory as needing a sort before it's served. Used during
    /// bulk build to defer sorting until the directory is fully populated.
    pub fn mark_unsorted(&mut self, id: NodeId) {
        if let Some(NodeKind::Directory { sorted, .. }) = self.get_mut(id).map(|n| &mut n.kind) {
            *sorted = false;
        }
    }

    /// Sort all directories that were marked unsorted. Call once after the
    /// initial scan to restore the readdir invariant.
    pub fn finalize_sort(&mut self) {
        for slot in self.nodes.iter_mut() {
            let Some(node) = slot else { continue };
            if let NodeKind::Directory {
                ordered, sorted, ..
            } = &mut node.kind
            {
                if !*sorted {
                    ordered.sort_by(|(a, _), (b, _)| a.cmp(b));
                    *sorted = true;
                }
            }
        }
    }

    /// Look up a child by name within a directory.
    pub fn child(&self, parent: NodeId, name: &str) -> Option<NodeId> {
        match &self.get(parent)?.kind {
            NodeKind::Directory { by_name, .. } => by_name.get(name).copied(),
            _ => None,
        }
    }

    /// Recursively remove a node and its descendants. Returns the number of
    /// nodes removed. Also clears `path_index` entries for files removed.
    pub fn remove_recursive(&mut self, id: NodeId) -> usize {
        if id == ROOT_ID {
            return 0;
        }
        let mut removed = 0;
        let mut stack = vec![id];
        let mut to_drop: Vec<NodeId> = Vec::new();
        while let Some(nid) = stack.pop() {
            if let Some(node) = self.get(nid) {
                if let NodeKind::Directory { ordered, .. } = &node.kind {
                    for (_, cid) in ordered {
                        stack.push(*cid);
                    }
                }
                to_drop.push(nid);
            }
        }
        // Detach from parent (only for the top-level id).
        let removed_is_dir = self.get(id).map(|n| n.is_dir()).unwrap_or(false);
        let parent_id_opt = self.get(id).and_then(|n| n.parent);
        if let Some(parent_id) = parent_id_opt {
            let name = self.get(id).map(|n| n.name.clone());
            if let Some(name) = name {
                if let Some(parent) = self.get_mut(parent_id) {
                    if let NodeKind::Directory {
                        by_name,
                        ordered,
                        subdir_count,
                        ..
                    } = &mut parent.kind
                    {
                        by_name.remove(&name);
                        if let Some(pos) = ordered.iter().position(|(n, _)| n == &name) {
                            ordered.remove(pos);
                        }
                        if removed_is_dir && *subdir_count > 0 {
                            *subdir_count -= 1;
                        }
                    }
                }
            }
            // Bump parent mtime — see add_child for rationale.
            let now = std::time::SystemTime::now();
            if let Some(p) = self.get_mut(parent_id) {
                p.attrs.mtime = now;
                p.attrs.ctime = now;
            }
        }
        for nid in to_drop {
            if let Some(node) = self.nodes.get_mut(nid as usize).and_then(|o| o.take()) {
                if let NodeKind::File { backing } = &node.kind {
                    self.path_index.remove(backing);
                } else if let NodeKind::Directory {
                    sources: DirSources::Physical(paths),
                    ..
                } = &node.kind
                {
                    for p in paths {
                        if self.path_index.get(p) == Some(&nid) {
                            self.path_index.remove(p);
                        }
                    }
                }
                // NodeId is intentionally NOT recycled — keeping ids stable
                // for the process lifetime keeps NFS readdir cookies valid.
                removed += 1;
            }
        }
        removed
    }

    /// Append an additional physical source to a Physical directory (used when
    /// a second merge root contributes to an existing union dir). No-op if
    /// `id` no longer exists in the tree (e.g. caller passed a stale id from
    /// a snapshot taken before another apply removed the subtree).
    ///
    /// As soon as a directory has more than one source, its
    /// `winner_priority` is cleared: a merged dir is genuinely shared and
    /// has no single owner to demote.
    pub fn extend_dir_sources(&mut self, id: NodeId, path: PathBuf) {
        let Some(node) = self.get_mut(id) else { return };
        let NodeKind::Directory { sources, .. } = &mut node.kind else {
            return;
        };
        match sources {
            DirSources::Synthetic => {
                *sources = DirSources::Physical(vec![path.clone()]);
            }
            DirSources::Physical(v) => {
                if !v.contains(&path) {
                    v.push(path.clone());
                    if v.len() > 1 {
                        node.winner_priority = None;
                    }
                }
            }
        }
        // Index only after the node is confirmed present — otherwise we'd
        // leave a `path_index` entry pointing at a `None` slot.
        self.path_index.insert(path, id);
    }

    /// Set the owner-priority of a node (typically right after `add_child`).
    /// `None` is appropriate for synthetic nodes and merged dirs.
    pub fn set_winner_priority(&mut self, id: NodeId, priority: Option<usize>) {
        if let Some(node) = self.get_mut(id) {
            node.winner_priority = priority;
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_some()).count()
    }

    /// Register a physical path → file node mapping. Use this rather than
    /// poking `path_index` directly so that the (file_node, path_index entry)
    /// invariant lives in one place.
    pub fn index_file(&mut self, path: PathBuf, id: NodeId) {
        self.path_index.insert(path, id);
    }

    /// Look up the virtual node id for a physical path. Returns the *deepest*
    /// match: see [`Watcher`] routing for ancestor-walk semantics.
    pub fn lookup_path(&self, path: &Path) -> Option<NodeId> {
        self.path_index.get(path).copied()
    }

    /// Remove `path` from a directory's physical source list. Returns true if
    /// the directory is now sourceless (caller should remove it). Also clears
    /// the path_index entry that pointed `path` at this directory.
    pub fn drop_dir_source(&mut self, id: NodeId, path: &Path) -> bool {
        let now_empty = if let Some(node) = self.get_mut(id) {
            match &mut node.kind {
                NodeKind::Directory {
                    sources: DirSources::Physical(paths),
                    ..
                } => {
                    paths.retain(|p| p != path);
                    paths.is_empty()
                }
                _ => false,
            }
        } else {
            false
        };
        if self.path_index.get(path) == Some(&id) {
            self.path_index.remove(path);
        }
        now_empty
    }

    /// Record a losing file under `name` at directory `parent`. Insert is
    /// stable-sorted by ascending priority (position 0 = highest priority).
    /// Existing entries from the same root are replaced rather than
    /// duplicated, so a re-applied merge of the same root doesn't grow the
    /// shadow list unboundedly.
    pub fn add_shadow_file(&mut self, parent: NodeId, name: &str, shadow: ShadowFile) {
        let Some(node) = self.get_mut(parent) else {
            return;
        };
        let NodeKind::Directory { shadows, .. } = &mut node.kind else {
            return;
        };
        let list = shadows
            .get_or_insert_with(|| Box::new(DirShadows::default()))
            .files
            .entry(name.to_string())
            .or_default();
        list.retain(|s| s.priority != shadow.priority);
        let pos = list
            .binary_search_by(|s| s.priority.cmp(&shadow.priority))
            .unwrap_or_else(|p| p);
        list.insert(pos, shadow);
    }

    /// Record a losing directory under `name` at directory `parent`. Same
    /// dedup-by-priority semantics as `add_shadow_file`.
    pub fn add_shadow_dir(&mut self, parent: NodeId, name: &str, shadow: ShadowDir) {
        let Some(node) = self.get_mut(parent) else {
            return;
        };
        let NodeKind::Directory { shadows, .. } = &mut node.kind else {
            return;
        };
        let list = shadows
            .get_or_insert_with(|| Box::new(DirShadows::default()))
            .dirs
            .entry(name.to_string())
            .or_default();
        list.retain(|s| s.priority != shadow.priority);
        let pos = list
            .binary_search_by(|s| s.priority.cmp(&shadow.priority))
            .unwrap_or_else(|p| p);
        list.insert(pos, shadow);
    }

    /// Pop the highest-priority file shadow for `name` (if any). Used when a
    /// winner is removed and we want to promote a loser into its place.
    pub fn pop_shadow_file(&mut self, parent: NodeId, name: &str) -> Option<ShadowFile> {
        let node = self.get_mut(parent)?;
        let NodeKind::Directory { shadows, .. } = &mut node.kind else {
            return None;
        };
        let s = shadows.as_deref_mut()?;
        let list = s.files.get_mut(name)?;
        let popped = if list.is_empty() {
            None
        } else {
            Some(list.remove(0))
        };
        if list.is_empty() {
            s.files.remove(name);
        }
        if s.files.is_empty() && s.dirs.is_empty() {
            *shadows = None;
        }
        popped
    }

    /// Pop the highest-priority directory shadow for `name` (if any).
    pub fn pop_shadow_dir(&mut self, parent: NodeId, name: &str) -> Option<ShadowDir> {
        let node = self.get_mut(parent)?;
        let NodeKind::Directory { shadows, .. } = &mut node.kind else {
            return None;
        };
        let s = shadows.as_deref_mut()?;
        let list = s.dirs.get_mut(name)?;
        let popped = if list.is_empty() {
            None
        } else {
            Some(list.remove(0))
        };
        if list.is_empty() {
            s.dirs.remove(name);
        }
        if s.files.is_empty() && s.dirs.is_empty() {
            *shadows = None;
        }
        popped
    }

    /// Remove all shadow entries (file and dir) at `name` matching `priority`.
    /// Used when a losing root deletes its own copy of a shadowed name.
    pub fn remove_shadows_for_priority(&mut self, parent: NodeId, name: &str, priority: usize) {
        let Some(node) = self.get_mut(parent) else {
            return;
        };
        let NodeKind::Directory { shadows, .. } = &mut node.kind else {
            return;
        };
        let Some(s) = shadows.as_deref_mut() else {
            return;
        };
        if let Some(list) = s.files.get_mut(name) {
            list.retain(|x| x.priority != priority);
            if list.is_empty() {
                s.files.remove(name);
            }
        }
        if let Some(list) = s.dirs.get_mut(name) {
            list.retain(|x| x.priority != priority);
            if list.is_empty() {
                s.dirs.remove(name);
            }
        }
        if s.files.is_empty() && s.dirs.is_empty() {
            *shadows = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn file_kind(path: &str) -> NodeKind {
        NodeKind::File {
            backing: PathBuf::from(path),
        }
    }

    fn dir_attrs() -> CachedAttrs {
        CachedAttrs::synthetic_dir()
    }

    fn subdir_count(tree: &Tree, id: NodeId) -> u32 {
        match &tree.get(id).unwrap().kind {
            NodeKind::Directory { subdir_count, .. } => *subdir_count,
            _ => panic!("not a dir"),
        }
    }

    fn ordered_names(tree: &Tree, id: NodeId) -> Vec<String> {
        match &tree.get(id).unwrap().kind {
            NodeKind::Directory { ordered, .. } => ordered.iter().map(|(n, _)| n.clone()).collect(),
            _ => panic!("not a dir"),
        }
    }

    #[test]
    fn root_exists_and_is_synthetic() {
        let tree = Tree::new(42);
        let root = tree.get(ROOT_ID).expect("root present");
        assert!(root.is_dir());
        assert_eq!(root.parent, None);
        match &root.kind {
            NodeKind::Directory { sources, .. } => {
                assert!(matches!(sources, DirSources::Synthetic));
            }
            _ => panic!("root should be a dir"),
        }
    }

    #[test]
    fn add_child_inserts_and_indexes_by_name() {
        let mut tree = Tree::new(0);
        let id = tree
            .add_child(ROOT_ID, "Movies".into(), NodeKind::empty_dir(), dir_attrs())
            .expect("inserted");
        assert_eq!(tree.child(ROOT_ID, "Movies"), Some(id));
        assert_eq!(ordered_names(&tree, ROOT_ID), vec!["Movies".to_string()]);
    }

    #[test]
    fn add_child_returns_none_on_name_collision() {
        let mut tree = Tree::new(0);
        let first = tree
            .add_child(ROOT_ID, "dup".into(), NodeKind::empty_dir(), dir_attrs())
            .expect("inserted");
        let next_id_before = tree.nodes.len() as NodeId;
        let collision = tree.add_child(ROOT_ID, "dup".into(), NodeKind::empty_dir(), dir_attrs());
        assert_eq!(collision, None);
        // Failed insert must not consume an id slot.
        assert_eq!(tree.nodes.len() as NodeId, next_id_before);
        assert_eq!(tree.child(ROOT_ID, "dup"), Some(first));
    }

    #[test]
    fn subdir_count_tracks_directories_only() {
        let mut tree = Tree::new(0);
        tree.add_child(ROOT_ID, "a".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        tree.add_child(ROOT_ID, "b".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        tree.add_child(ROOT_ID, "f.txt".into(), file_kind("/x/f.txt"), dir_attrs())
            .unwrap();
        assert_eq!(subdir_count(&tree, ROOT_ID), 2);
    }

    #[test]
    fn remove_recursive_decrements_subdir_count() {
        let mut tree = Tree::new(0);
        let a = tree
            .add_child(ROOT_ID, "a".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        tree.add_child(ROOT_ID, "b".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        assert_eq!(subdir_count(&tree, ROOT_ID), 2);
        tree.remove_recursive(a);
        assert_eq!(subdir_count(&tree, ROOT_ID), 1);
        assert_eq!(tree.child(ROOT_ID, "a"), None);
    }

    #[test]
    fn remove_recursive_does_not_recycle_node_ids() {
        let mut tree = Tree::new(0);
        let a = tree
            .add_child(ROOT_ID, "a".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        tree.remove_recursive(a);
        assert!(tree.get(a).is_none(), "removed node must be gone");
        let b = tree
            .add_child(ROOT_ID, "b".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        assert_ne!(a, b, "ids must not be recycled");
        assert!(b > a, "freshly allocated id must be strictly greater");
    }

    #[test]
    fn remove_recursive_clears_path_index_for_files() {
        let mut tree = Tree::new(0);
        let dir = tree
            .add_child(ROOT_ID, "d".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        let file = tree
            .add_child(dir, "f".into(), file_kind("/disk/f"), dir_attrs())
            .unwrap();
        tree.index_file(PathBuf::from("/disk/f"), file);
        assert_eq!(tree.lookup_path(&PathBuf::from("/disk/f")), Some(file));
        tree.remove_recursive(dir);
        assert_eq!(tree.lookup_path(&PathBuf::from("/disk/f")), None);
    }

    #[test]
    fn remove_recursive_root_is_noop() {
        let mut tree = Tree::new(0);
        let removed = tree.remove_recursive(ROOT_ID);
        assert_eq!(removed, 0);
        assert!(tree.get(ROOT_ID).is_some());
    }

    #[test]
    fn finalize_sort_sorts_unsorted_dirs() {
        let mut tree = Tree::new(0);
        let d = tree
            .add_child(ROOT_ID, "d".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        tree.mark_unsorted(d);
        for n in ["c", "a", "b"] {
            tree.add_child(d, n.into(), NodeKind::empty_dir(), dir_attrs())
                .unwrap();
        }
        // Before finalize, `sorted=false` so order reflects insertion.
        assert_eq!(ordered_names(&tree, d), vec!["c", "a", "b"]);
        tree.finalize_sort();
        assert_eq!(ordered_names(&tree, d), vec!["a", "b", "c"]);
    }

    #[test]
    fn add_child_into_sorted_dir_inserts_in_order() {
        let mut tree = Tree::new(0);
        let d = tree
            .add_child(ROOT_ID, "d".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        // Default is sorted=true, so each insert must binary-search into place.
        for n in ["c", "a", "b"] {
            tree.add_child(d, n.into(), NodeKind::empty_dir(), dir_attrs())
                .unwrap();
        }
        assert_eq!(ordered_names(&tree, d), vec!["a", "b", "c"]);
    }

    #[test]
    fn drop_dir_source_signals_emptiness_and_clears_path_index() {
        let mut tree = Tree::new(0);
        let d = tree
            .add_child(ROOT_ID, "d".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        let p1 = PathBuf::from("/m1/d");
        let p2 = PathBuf::from("/m2/d");
        tree.extend_dir_sources(d, p1.clone());
        tree.extend_dir_sources(d, p2.clone());
        assert_eq!(tree.lookup_path(&p1), Some(d));
        assert!(!tree.drop_dir_source(d, &p1), "still has p2");
        assert_eq!(tree.lookup_path(&p1), None);
        assert!(tree.drop_dir_source(d, &p2), "now empty");
        assert_eq!(tree.lookup_path(&p2), None);
    }

    #[test]
    fn extend_dir_sources_promotes_synthetic_to_physical() {
        let mut tree = Tree::new(0);
        let d = tree
            .add_child(ROOT_ID, "d".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();
        tree.extend_dir_sources(d, PathBuf::from("/m/d"));
        match &tree.get(d).unwrap().kind {
            NodeKind::Directory {
                sources: DirSources::Physical(v),
                ..
            } => {
                assert_eq!(v.len(), 1);
            }
            _ => panic!("expected physical sources"),
        }
    }

    #[test]
    fn add_and_remove_bump_parent_mtime() {
        // Backdate the parent's mtime to a sentinel before each mutation,
        // then assert it was overwritten. Avoids racing wall-clock
        // granularity (`SystemTime::now()` resolution varies per platform).
        let mut tree = Tree::new(0);
        let d = tree
            .add_child(ROOT_ID, "d".into(), NodeKind::empty_dir(), dir_attrs())
            .unwrap();

        tree.get_mut(d).unwrap().attrs.mtime = SystemTime::UNIX_EPOCH;
        let f = tree
            .add_child(d, "f".into(), file_kind("/x"), dir_attrs())
            .unwrap();
        assert!(
            tree.get(d).unwrap().attrs.mtime > SystemTime::UNIX_EPOCH,
            "add_child must bump parent mtime"
        );

        tree.get_mut(d).unwrap().attrs.mtime = SystemTime::UNIX_EPOCH;
        tree.remove_recursive(f);
        assert!(
            tree.get(d).unwrap().attrs.mtime > SystemTime::UNIX_EPOCH,
            "remove_recursive must bump parent mtime"
        );
    }

    #[test]
    fn child_lookup_returns_none_for_missing_or_non_dir() {
        let mut tree = Tree::new(0);
        let f = tree
            .add_child(ROOT_ID, "f".into(), file_kind("/x"), dir_attrs())
            .unwrap();
        assert_eq!(tree.child(ROOT_ID, "missing"), None);
        // Looking up a child within a file node returns None.
        assert_eq!(tree.child(f, "anything"), None);
    }
}
