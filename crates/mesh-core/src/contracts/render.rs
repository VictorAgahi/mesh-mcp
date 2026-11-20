use super::*;

impl ContractGraph {
    /// Canonical, `NodeId`-independent form of the graph, used to compare two
    /// builds: one JSON line per node, edge and recorded relation intent, the
    /// whole list sorted. The same facts give the same lines whatever the
    /// insertion order, thread count or id numbering; duplicates are kept so a
    /// file indexed twice stays visible.
    ///
    /// File paths are written as-is (absolute); see
    /// [`Self::canonical_lines_rooted`] for the checkout-independent form.
    pub fn canonical_lines(&self) -> Vec<String> {
        self.canonical_lines_rooted(&[])
    }

    /// [`Self::canonical_lines`] with every file path written relative to its
    /// workspace root (`roots[node.repo_id]`, see
    /// [`crate::state::root_relative_path`]), so the same content checked out
    /// in two directories gives the same lines (plan 4.12f).
    pub fn canonical_lines_rooted(&self, roots: &[std::path::PathBuf]) -> Vec<String> {
        let key_of = |id: NodeId| -> String {
            self.get_node(id).map_or_else(
                || String::from("<missing>"),
                |n| Self::canonical_node_key(n, roots),
            )
        };

        let mut lines = Vec::with_capacity(
            self.nodes.len() + self.edges.len() + self.rpc_calls.len() + self.reverse_deps.len(),
        );
        lines.extend(
            self.nodes
                .values()
                .map(|n| format!("node {}", Self::canonical_node_key(n, roots))),
        );
        for edge in &self.edges {
            let line = serde_json::json!([
                edge.kind,
                key_of(edge.from),
                key_of(edge.to),
                edge.metadata,
                edge.confidence
            ]);
            lines.push(format!("edge {line}"));
        }
        for (label, index) in [
            ("dep", &self.reverse_deps),
            ("produces", &self.topic_producers),
            ("consumes", &self.topic_consumers),
        ] {
            for (target, ids) in index {
                for &id in ids {
                    lines.push(format!(
                        "{label} {}",
                        serde_json::json!([target, key_of(id)])
                    ));
                }
            }
        }
        for node in self.callable_nodes() {
            lines.push(format!("node {}", Self::canonical_node_key(node, roots)));
        }
        for edge in self.call_edges() {
            let line = serde_json::json!([
                edge.kind,
                key_of(edge.from),
                key_of(edge.to),
                edge.metadata,
                edge.confidence
            ]);
            lines.push(format!("edge {line}"));
        }
        // One digest per file, not the facts themselves: the full JSON of every
        // file's facts made `graph --format fingerprint` hold several times their
        // size at once (vscode: +170 MB peak).
        for (path, facts) in &self.call_facts {
            let digest = serde_json::to_vec(&facts.facts)
                .map(|bytes| crate::audit::AuditLogger::compute_sha256(&bytes))
                .unwrap_or_default();
            lines.push(format!(
                "call_facts {}",
                serde_json::json!([
                    crate::state::root_relative_path(path, roots, Some(facts.repo_id)),
                    digest
                ])
            ));
        }
        for (caller, target) in &self.rpc_calls {
            lines.push(format!(
                "rpc_call {}",
                serde_json::json!([key_of(*caller), target])
            ));
        }

        lines.sort_unstable();
        lines
    }

    /// SHA-256 (hex) of [`Self::canonical_lines`].
    pub fn fingerprint(&self) -> String {
        crate::audit::AuditLogger::compute_sha256(self.canonical_lines().join("\n").as_bytes())
    }

    /// Every observable field of a node except its `NodeId`.
    fn canonical_node_key(node: &ContractNode, roots: &[std::path::PathBuf]) -> String {
        serde_json::json!([
            crate::state::root_relative_path(&node.file_path, roots, Some(node.repo_id)),
            node.line_start,
            node.line_end,
            node.kind,
            node.name,
            node.package,
            node.repo_id,
            node.signature,
            node.docstring
        ])
        .to_string()
    }
}
