use super::*;

impl ContractGraph {
    pub fn add_node(&mut self, mut node: ContractNode) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        node.id = id;

        self.name_to_nodes
            .entry(node.name.clone())
            .or_default()
            .push(id);

        if !node.package.is_empty() {
            self.package_to_nodes
                .entry(node.package.clone())
                .or_default()
                .push(id);
        }

        self.file_to_nodes
            .entry(node.file_path.clone())
            .or_default()
            .push(id);

        if node.kind == NodeKind::GrpcMethod
            || node.kind == NodeKind::GrpcService
            || !node.package.is_empty()
        {
            let fqcn = CompactStr::new(format!("{}/{}", node.package, node.name));
            self.fqcn_to_node.entry(fqcn).or_default().push(id);
        }

        if node.kind == NodeKind::KafkaTopic
            || node.kind == NodeKind::EventStream
            || node.kind == NodeKind::Queue
        {
            let topic_key = node.name.to_lowercase();
            self.name_to_nodes
                .entry(CompactStr::new(&topic_key))
                .or_default()
                .push(id);
        }

        self.nodes.insert(id, node);
        id
    }

    pub fn add_edge(&mut self, edge: ContractEdge) {
        match edge.kind {
            EdgeKind::Produces => {
                if let Some(to_node) = self.nodes.get(&edge.to) {
                    let key = CompactStr::new(to_node.name.to_lowercase());
                    self.topic_producers.entry(key).or_default().push(edge.from);
                }
            }
            EdgeKind::Consumes => {
                if let Some(to_node) = self.nodes.get(&edge.to) {
                    let key = CompactStr::new(to_node.name.to_lowercase());
                    self.topic_consumers.entry(key).or_default().push(edge.from);
                }
            }
            _ => {}
        }
        self.edges.push(edge);
    }

    /// Records the raw fact "`consumer_node_id` imports `imported_target`". This is
    /// the *only* place that fact is stored — no placeholder edge is created here.
    /// `reconcile_edges` derives every `Imports` edge fresh, every time it runs, by
    /// reading `reverse_deps` directly; that is what lets an incremental reload of
    /// some *other* file still produce the correct edge for this (unchanged)
    /// consumer without `consumer_node_id` itself being re-folded (idempotence
    /// invariants I2/I3 — see the note on `reconcile_edges`).
    pub fn add_dependency(&mut self, consumer_node_id: NodeId, imported_target: &str) {
        let key = CompactStr::new(imported_target);
        self.reverse_deps
            .entry(key)
            .or_default()
            .push(consumer_node_id);
    }

    pub fn add_producer(&mut self, producer_node_id: NodeId, topic: &str) {
        let key = CompactStr::new(normalize_topic_key(topic));
        self.topic_producers
            .entry(key)
            .or_default()
            .push(producer_node_id);
    }

    pub fn add_consumer(&mut self, consumer_node_id: NodeId, topic: &str) {
        let key = CompactStr::new(normalize_topic_key(topic));
        self.topic_consumers
            .entry(key)
            .or_default()
            .push(consumer_node_id);
    }

    /// Marks a producer / consumer node whose topic link is heuristic.
    pub fn mark_heuristic_topic_node(&mut self, node_id: NodeId) {
        self.heuristic_topic_nodes.insert(node_id);
    }

    /// Edge confidence of a registered producer / consumer node.
    pub(super) fn topic_confidence(&self, node_id: NodeId) -> EdgeConfidence {
        if self.heuristic_topic_nodes.contains(&node_id) {
            EdgeConfidence::Heuristic
        } else {
            EdgeConfidence::Exact
        }
    }

    pub fn add_rpc_call(&mut self, caller_node_id: NodeId, target_rpc: &str) {
        self.rpc_calls
            .push((caller_node_id, CompactStr::new(target_rpc)));
    }

    pub fn patch_file(&mut self, file_path: &Path) {
        self.patch_files(std::iter::once(file_path));
    }

    /// Purges every node declared in any of `file_paths` in a single pass over the
    /// secondary indices, instead of one full `retain` sweep per file.
    pub fn patch_files<'a>(&mut self, file_paths: impl IntoIterator<Item = &'a Path>) {
        let mut stale_set: HashSet<NodeId> = HashSet::new();
        for path in file_paths {
            if let Some(ids) = self.file_to_nodes.remove(path) {
                stale_set.extend(ids);
            }
        }
        if stale_set.is_empty() {
            return;
        }

        self.remove_nodes(&stale_set);

        // Indices keyed by *target* (not node) need one sweep — done once for the whole batch.
        for map in [
            &mut self.reverse_deps,
            &mut self.topic_producers,
            &mut self.topic_consumers,
        ] {
            map.retain(|_, ids| {
                ids.retain(|id| !stale_set.contains(id));
                !ids.is_empty()
            });
        }
        self.rpc_calls.retain(|(id, _)| !stale_set.contains(id));
        self.heuristic_topic_nodes
            .retain(|id| !stale_set.contains(id));
        self.edges
            .retain(|e| !stale_set.contains(&e.from) && !stale_set.contains(&e.to));
    }

    /// Removes every node in `stale` from `self.nodes` and from each index keyed
    /// by a node attribute (name, lowercase stream name, package, FQCN, file),
    /// grouped by key: one `retain` per touched key rather than one per removed
    /// node. Per-node removal was O(k·N) when k removed nodes share a hot key —
    /// every file declaring `new`/`handle` lands in the same `name_to_nodes`
    /// bucket, and every synthetic topic hub shares the `event-bus` file entry.
    pub(super) fn remove_nodes(&mut self, stale: &HashSet<NodeId>) {
        let mut by_name: HashSet<CompactStr> = HashSet::new();
        let mut by_package: HashSet<CompactStr> = HashSet::new();
        let mut by_fqcn: HashSet<CompactStr> = HashSet::new();
        let mut by_file: HashSet<FilePath> = HashSet::new();
        for id in stale {
            let Some(node) = self.nodes.remove(id) else {
                continue;
            };
            if node.kind == NodeKind::KafkaTopic
                || node.kind == NodeKind::EventStream
                || node.kind == NodeKind::Queue
            {
                by_name.insert(CompactStr::new(node.name.to_lowercase()));
            }
            if !node.package.is_empty() {
                by_package.insert(node.package.clone());
            }
            if node.kind == NodeKind::GrpcMethod
                || node.kind == NodeKind::GrpcService
                || !node.package.is_empty()
            {
                by_fqcn.insert(CompactStr::new(format!("{}/{}", node.package, node.name)));
            }
            by_name.insert(node.name);
            by_file.insert(node.file_path);
        }
        for (index, keys) in [
            (&mut self.name_to_nodes, by_name),
            (&mut self.package_to_nodes, by_package),
            (&mut self.fqcn_to_node, by_fqcn),
        ] {
            for key in keys {
                Self::retain_bucket(index, &key, stale);
            }
        }
        for file in by_file {
            Self::retain_bucket(&mut self.file_to_nodes, &file, stale);
        }
    }

    fn retain_bucket<K: std::hash::Hash + Eq>(
        index: &mut HashMap<K, Vec<NodeId>>,
        key: &K,
        stale: &HashSet<NodeId>,
    ) {
        if let Some(ids) = index.get_mut(key) {
            ids.retain(|x| !stale.contains(x));
            if ids.is_empty() {
                index.remove(key);
            }
        }
    }
}
