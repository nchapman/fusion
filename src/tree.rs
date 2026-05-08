//! In-memory virtual filesystem tree.
//!
//! Nodes are stored in a flat `Vec<Option<Node>>` indexed by `NodeId`.
//! `NodeId` doubles as the NFS `fileid3`. Index 0 is reserved (NFS forbids
//! fileid 0); index 1 is always the root.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

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
}

#[derive(Debug)]
pub enum NodeKind {
    Directory {
        /// Name → child id, for O(1) lookup by name.
        by_name: HashMap<String, NodeId>,
        /// Children for stable readdir pagination. Sorted by name once the
        /// directory is finalized (`sorted == true`); during initial bulk
        /// build it's unordered to avoid O(n²) insertion.
        ordered: Vec<(String, NodeId)>,
        sorted: bool,
        sources: DirSources,
    },
    File {
        backing: PathBuf,
    },
}

#[derive(Debug, Clone)]
pub enum DirSources {
    /// No physical backing — root, share roots, and intermediate union dirs
    /// that exist purely as namespace.
    Synthetic,
    /// One or more physical directories that union into this virtual dir.
    Physical(Vec<PathBuf>),
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Node {
    pub id: NodeId,
    pub parent: Option<NodeId>,
    pub name: String,
    pub kind: NodeKind,
    pub attrs: CachedAttrs,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        matches!(self.kind, NodeKind::Directory { .. })
    }

    #[allow(dead_code)]
    pub fn is_file(&self) -> bool {
        matches!(self.kind, NodeKind::File { .. })
    }
}

pub struct Tree {
    nodes: Vec<Option<Node>>,
    /// Reverse index from physical path → virtual node, for watcher lookups.
    /// Files and directories both map here.
    pub path_index: HashMap<PathBuf, NodeId>,
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
                by_name: HashMap::new(),
                ordered: Vec::new(),
                sorted: true,
                sources: DirSources::Synthetic,
            },
            attrs: CachedAttrs::synthetic_dir(),
        };
        Self {
            nodes: vec![None, Some(root)],
            path_index: HashMap::new(),
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
        let node = Node {
            id,
            parent: Some(parent),
            name: name.clone(),
            kind,
            attrs,
        };
        self.nodes[id as usize] = Some(node);

        let parent_node = self.get_mut(parent).expect("parent disappeared");
        if let NodeKind::Directory {
            by_name,
            ordered,
            sorted,
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
        }
        Some(id)
    }

    /// Mark a directory as needing a sort before it's served. Used during
    /// bulk build to defer sorting until the directory is fully populated.
    pub fn mark_unsorted(&mut self, id: NodeId) {
        if let Some(NodeKind::Directory { sorted, .. }) =
            self.get_mut(id).map(|n| &mut n.kind)
        {
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
        if let Some(parent_id) = self.get(id).and_then(|n| n.parent) {
            let name = self.get(id).map(|n| n.name.clone());
            if let Some(name) = name {
                if let Some(parent) = self.get_mut(parent_id) {
                    if let NodeKind::Directory {
                        by_name, ordered, ..
                    } = &mut parent.kind
                    {
                        by_name.remove(&name);
                        if let Some(pos) =
                            ordered.iter().position(|(n, _)| n == &name)
                        {
                            ordered.remove(pos);
                        }
                    }
                }
            }
        }
        for nid in to_drop {
            if let Some(node) = self.nodes.get_mut(nid as usize).and_then(|o| o.take()) {
                if let NodeKind::File { backing } = &node.kind {
                    self.path_index.remove(backing);
                } else if let NodeKind::Directory { sources, .. } = &node.kind {
                    if let DirSources::Physical(paths) = sources {
                        for p in paths {
                            if self.path_index.get(p) == Some(&nid) {
                                self.path_index.remove(p);
                            }
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
    /// a second merge root contributes to an existing union dir).
    pub fn extend_dir_sources(&mut self, id: NodeId, path: PathBuf) {
        if let Some(node) = self.get_mut(id) {
            if let NodeKind::Directory { sources, .. } = &mut node.kind {
                match sources {
                    DirSources::Synthetic => {
                        *sources = DirSources::Physical(vec![path.clone()]);
                    }
                    DirSources::Physical(v) => {
                        if !v.contains(&path) {
                            v.push(path.clone());
                        }
                    }
                }
            }
        }
        self.path_index.insert(path, id);
    }

    pub fn node_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_some()).count()
    }

    /// Remove `path` from a directory's physical source list. Returns true if
    /// the directory is now sourceless (caller should remove it). Also clears
    /// the path_index entry that pointed `path` at this directory.
    pub fn drop_dir_source(&mut self, id: NodeId, path: &PathBuf) -> bool {
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
}
