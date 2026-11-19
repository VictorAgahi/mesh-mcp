use super::resolve::contains_ignore_ascii_case;
use super::*;

impl ContractGraph {
    /// O(1) in-memory reverse dependency resolution. `target` may be either a
    /// literal import-path/package identifier (as it appears in another
    /// file's `import`/`use` statement) or a declared contract symbol name
    /// (e.g. a `GrpcService`'s name) — the latter is bridged to the package(s)
    /// it's declared in before falling back to a raw substring scan.
    pub fn find_dependents(&self, target: &str) -> Vec<&ContractNode> {
        self.find_dependents_matched(target).0
    }

    /// Shortest target the last-resort substring fallback of
    /// [`Self::find_dependents_matched`] runs for: `find_dependents("a")` used to
    /// return every dependency edge in the workspace.
    pub const SUBSTRING_FALLBACK_MIN_CHARS: usize = 3;

    /// [`Self::find_dependents`], each node at most once (a file importing the
    /// same module on several lines used to list its declarations once per
    /// line), plus *how* the result was matched, so a caller can say when it is
    /// only a substring heuristic.
    pub fn find_dependents_matched(&self, target: &str) -> (Vec<&ContractNode>, DependentsMatch) {
        let (nodes, how) = self.find_dependents_raw(target);
        let mut seen: HashSet<NodeId> = HashSet::with_capacity(nodes.len());
        let nodes = nodes.into_iter().filter(|n| seen.insert(n.id)).collect();
        (nodes, how)
    }

    fn find_dependents_raw(&self, target: &str) -> (Vec<&ContractNode>, DependentsMatch) {
        let mut result = Vec::new();
        let target_key = CompactStr::new(target);

        // 1. Direct match in reverse_deps: `target` is itself the literal
        // import-path/package string other files reference — or a subpath of it
        // (`@scope/pkg/testing`, `github.com/org/mod/pkg`): importing part of a
        // package is depending on it. Sorted keys keep the order deterministic (I5).
        if let Some(node_ids) = self.reverse_deps.get(&target_key) {
            for &id in node_ids {
                if let Some(node) = self.nodes.get(&id) {
                    result.push(node);
                }
            }
        }
        if !target.is_empty() && !target.ends_with('/') {
            let prefix = format!("{target}/");
            let mut subpaths: Vec<&CompactStr> = self
                .reverse_deps
                .keys()
                .filter(|k| k.starts_with(prefix.as_str()))
                .collect();
            subpaths.sort();
            for key in subpaths {
                for &id in &self.reverse_deps[key] {
                    if let Some(node) = self.nodes.get(&id) {
                        result.push(node);
                    }
                }
            }
        }
        if !result.is_empty() {
            return (result, DependentsMatch::Import);
        }

        // 2. Symbol bridge: `target` may instead be a declared contract name
        // (what the tool's own schema example — "Target contract name (ex:
        // 'UserAuthRequest')" — invites callers to pass) rather than the raw
        // import string. Resolve it to its declaring node(s)' own package
        // identity, then re-query reverse_deps with THAT — a declaring node
        // always knows its own package even when nothing calls it by that
        // exact literal string anywhere else.
        if result.is_empty() {
            let mut seen_packages: HashSet<&CompactStr> = HashSet::new();
            let declaring_ids = self
                .name_to_nodes
                .get(&target_key)
                .into_iter()
                .flatten()
                .chain(self.fqcn_to_node.get(&target_key).into_iter().flatten());
            for &id in declaring_ids {
                let Some(node) = self.nodes.get(&id) else {
                    continue;
                };
                if node.package.is_empty() || !seen_packages.insert(&node.package) {
                    continue;
                }
                if let Some(node_ids) = self.reverse_deps.get(&node.package) {
                    for &dep_id in node_ids {
                        if let Some(dep_node) = self.nodes.get(&dep_id) {
                            result.push(dep_node);
                        }
                    }
                }
            }
            if !result.is_empty() {
                return (result, DependentsMatch::DeclaringPackage);
            }
        }

        // 2b. Graph-edge bridge: a cross-service RPC caller is linked by a
        // real `CallsRpc`/`Implements` edge (built in `reconcile_edges`), not
        // by an import string at all — a Go client, for instance, constructs
        // `pb.NewFooServiceClient(conn)` and never imports anything literally
        // named "FooService". Resolve `target` to its declaring node(s) (the
        // service itself, plus every other node declared in the same
        // package — covers a proto method node living alongside its service),
        // then collect the `from` side of any edge pointing at one of them.
        if result.is_empty() {
            let mut edge_targets: HashSet<NodeId> = HashSet::new();
            let declaring_ids = self
                .name_to_nodes
                .get(&target_key)
                .into_iter()
                .flatten()
                .chain(self.fqcn_to_node.get(&target_key).into_iter().flatten());
            for &id in declaring_ids {
                edge_targets.insert(id);
                if let Some(node) = self.nodes.get(&id) {
                    if !node.package.is_empty() {
                        if let Some(sibling_ids) = self.package_to_nodes.get(&node.package) {
                            edge_targets.extend(sibling_ids.iter().copied());
                        }
                    }
                }
            }
            let mut seen_ids: HashSet<NodeId> = HashSet::new();
            for edge in &self.edges {
                if !matches!(edge.kind, EdgeKind::CallsRpc | EdgeKind::Implements) {
                    continue;
                }
                if !edge_targets.contains(&edge.to) {
                    continue;
                }
                if let Some(caller) = self.nodes.get(&edge.from) {
                    if seen_ids.insert(caller.id) {
                        result.push(caller);
                    }
                }
            }
            if !result.is_empty() {
                return (result, DependentsMatch::GraphEdge);
            }
        }

        // 3. Last-resort fallback: unscoped substring match across every
        // recorded dependency string. Can span multiple, unrelated services
        // that happen to share a locally-aliased package name (e.g. every
        // service in a polyglot monorepo vendoring its own `genproto`
        // package) — callers should group results by `ContractNode::repo_id`
        // before presenting this to a human/agent rather than treat it as a
        // single flat, disambiguated answer.
        if target.chars().count() >= Self::SUBSTRING_FALLBACK_MIN_CHARS {
            // `reverse_deps` is a `HashMap`: iterated directly, its per-process
            // random hash seed would make this fallback return a different
            // node order on every run for identical input, violating I5.
            // Sorted by key first, so the order depends only on content.
            let mut matching_pkgs: Vec<&CompactStr> = self
                .reverse_deps
                .keys()
                .filter(|pkg| pkg.contains(target))
                .collect();
            matching_pkgs.sort();
            for pkg in matching_pkgs {
                for &id in &self.reverse_deps[pkg] {
                    if let Some(node) = self.nodes.get(&id) {
                        result.push(node);
                    }
                }
            }
        }

        let how = if result.is_empty() {
            DependentsMatch::None
        } else {
            DependentsMatch::Substring
        };
        (result, how)
    }

    /// Synchronous end-to-end gRPC trace resolution
    pub fn analyze_grpc(&self, target: &str) -> GrpcTrace<'_> {
        let norm_target = target.trim();
        let mut proto_definition = None;
        // Falls back to the exact-name-matched service/method node itself
        // when no `.proto` declaration is indexed (e.g. `proto_dirs` isn't
        // configured, or the workspace root simply wasn't crawled) — a
        // language's own service-implementation node (Go's
        // `RegisterFooServiceServer`-detected node, TS's `@GrpcService`
        // class, ...) is a perfectly good anchor for resolving callers via
        let mut anchors: Vec<NodeId> = Vec::new();
        let mut client_stubs: Vec<(&ContractNode, EdgeConfidence)> = Vec::new();
        let mut server_handlers: Vec<(&ContractNode, EdgeConfidence)> = Vec::new();

        let (matches, related_rpcs) = self.grpc_anchor_matches(norm_target);
        for (node, confidence, is_symbol_match) in matches {
            if Self::is_proto_file(&node.file_path) {
                if confidence == EdgeConfidence::Exact || proto_definition.is_none() {
                    proto_definition = Some(node);
                }
                anchors.push(node.id);
            } else if is_symbol_match {
                anchors.push(node.id);
                // Exact or bare-name match: this node represents the target declaration/implementation.
                // It defaults straight to server_handlers without relying on directory naming heuristics.
                if node.name.as_str().starts_with("rpc:") {
                    client_stubs.push((node, confidence));
                } else {
                    server_handlers.push((node, confidence));
                }
            }
        }

        for anchor_id in &anchors {
            for edge in &self.edges {
                if edge.to != *anchor_id {
                    continue;
                }
                match edge.kind {
                    EdgeKind::Implements => {
                        if let Some(handler) = self.nodes.get(&edge.from) {
                            if let Some(seen) =
                                server_handlers.iter_mut().find(|(h, _)| h.id == handler.id)
                            {
                                // The symbol-name scan above only ever sees the raw
                                // decorator text; an exact edge is stronger evidence.
                                if edge.confidence == EdgeConfidence::Exact {
                                    seen.1 = EdgeConfidence::Exact;
                                }
                            } else {
                                server_handlers.push((handler, edge.confidence));
                            }
                        }
                    }
                    EdgeKind::CallsRpc => {
                        if let Some(client) = self.nodes.get(&edge.from) {
                            if !client_stubs.iter().any(|(c, _)| c.id == client.id) {
                                client_stubs.push((client, edge.confidence));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Always computed, not only when no method-level call resolved: 7.0.6
        // said "Client Stubs (1 found)" for CreateEvent while a second caller,
        // whose call went through an inherited field, sat unresolved. A file
        // that already has a resolved call is not repeated here.
        let mut service_level_clients: Vec<(&ContractNode, EdgeConfidence)> = Vec::new();
        {
            let mut resolved_files: HashSet<&FilePath> =
                client_stubs.iter().map(|(c, _)| &c.file_path).collect();
            let services: HashSet<NodeId> = anchors
                .iter()
                .filter_map(|id| self.nodes.get(id))
                .filter(|m| m.kind == NodeKind::GrpcMethod)
                .filter_map(|m| self.enclosing_grpc_service(m))
                .collect();
            // A file that binds the service's client to a typed field has every
            // call through that field recorded method by method (`=S.m`, or `#`
            // for a base class whose subclasses call through it): if none of
            // them is this method, the file does not call it. Only clients
            // held some other way stay "may call".
            let service_names: Vec<&str> = services
                .iter()
                .filter_map(|id| self.nodes.get(id))
                .map(|s| s.name.as_str())
                .collect();
            for (caller_id, recorded) in &self.rpc_calls {
                let service = if let Some(t) = recorded.strip_prefix(RPC_EXACT_METHOD_PREFIX) {
                    t.rsplit_once('.').map(|(s, _)| s)
                } else if let Some(b) = recorded.strip_prefix(CLIENT_FIELD_BINDING_PREFIX) {
                    b.split_once('=').map(|(_, s)| s)
                } else {
                    None
                };
                if service.is_some_and(|s| service_names.iter().any(|n| n.eq_ignore_ascii_case(s)))
                {
                    if let Some(caller) = self.nodes.get(caller_id) {
                        resolved_files.insert(&caller.file_path);
                    }
                }
            }
            let mut edges: Vec<&ContractEdge> = self
                .edges
                .iter()
                .filter(|e| e.kind == EdgeKind::CallsRpc && services.contains(&e.to))
                .collect();
            edges.sort_by_key(|e| e.from);
            for edge in edges {
                if let Some(client) = self.nodes.get(&edge.from) {
                    if !resolved_files.contains(&client.file_path)
                        && !service_level_clients.iter().any(|(c, _)| c.id == client.id)
                    {
                        service_level_clients.push((client, edge.confidence));
                    }
                }
            }
        }

        let handler_rpc_names = server_handlers
            .iter()
            .filter_map(|(h, _)| {
                self.handler_rpc_names
                    .get(&h.id)
                    .map(|name| (h.id, name.clone()))
            })
            .collect();
        let handler_declared_as = self
            .rpc_calls
            .iter()
            .filter(|(id, _)| self.handler_rpc_names.contains_key(id))
            .filter(|(id, _)| server_handlers.iter().any(|(h, _)| h.id == *id))
            .filter_map(|(id, recorded)| {
                let reference = recorded.strip_prefix(HANDLER_REF_PREFIX)?;
                Some((*id, Self::declared_as(reference)?))
            })
            .collect();
        GrpcTrace {
            target: CompactStr::new(norm_target),
            proto_definition,
            client_stubs,
            server_handlers,
            handler_rpc_names,
            handler_declared_as,
            service_level_clients,
            related_rpcs,
            files_scanned: self.file_to_nodes.len(),
        }
    }

    /// Call sites recorded for `service.method`, whether or not they resolved
    /// to a declared RPC. A caller of an RPC that no longer exists has no
    /// `CallsRpc` edge (there is no node left to link it to), so `analyze_grpc`
    /// cannot list it: `diff_mesh` asks here instead.
    ///
    /// - A method-level record (`=Service.Method`, or `~Service.Method` when the
    ///   service was inferred from a field name, heuristic) may name the service
    ///   with its proto package (`=demo.v1.UserService.DeleteUser`). The package
    ///   is stripped: the same as `package` (the `.proto`'s own, when known) keeps
    ///   the record's confidence; no known package makes it heuristic; another
    ///   package makes it ambiguous (a homonym service, or the same one renamed).
    /// - A service-level record (a client constructed for the service, without
    ///   the method resolved: Go `NewUserServiceClient`, a Python stub, a Java
    ///   `newBlockingStub`) is a [`RecordedCaller::ServiceClient`] that may call
    ///   the method, heuristic, listed only for a file with no method-level call.
    ///
    /// Names compare with [`fold_rpc_name`]. Sorted by location, each node once.
    pub fn recorded_rpc_callers(
        &self,
        service: &str,
        method: &str,
        package: Option<&str>,
    ) -> Vec<(&ContractNode, EdgeConfidence, RecordedCaller)> {
        let (want_service, want_method) = (fold_rpc_name(service), fold_rpc_name(method));
        // Splits `[pkg.]Service`, comparing the service; the package decides
        // the confidence.
        let service_confidence = |qualified: &str, recorded: EdgeConfidence| {
            let (pkg, bare) = match qualified.rsplit_once('.') {
                Some((pkg, bare)) => (Some(pkg), bare),
                None => (None, qualified),
            };
            if fold_rpc_name(bare) != want_service {
                return None;
            }
            Some(match (pkg, package) {
                (None, _) => recorded,
                (Some(p), Some(want)) if fold_rpc_name(p) == fold_rpc_name(want) => recorded,
                (Some(_), Some(_)) => EdgeConfidence::Ambiguous,
                (Some(_), None) => EdgeConfidence::Heuristic,
            })
        };
        let strength = |c: EdgeConfidence| match c {
            EdgeConfidence::Exact => 2u8,
            EdgeConfidence::Heuristic => 1,
            EdgeConfidence::Ambiguous => 0,
        };
        let mut methods: Vec<(&ContractNode, EdgeConfidence, RecordedCaller)> = Vec::new();
        let mut clients: Vec<(&ContractNode, EdgeConfidence, RecordedCaller)> = Vec::new();
        for (caller_id, recorded) in &self.rpc_calls {
            let (found, kind) = if let Some(t) = recorded
                .strip_prefix(RPC_EXACT_METHOD_PREFIX)
                .map(|t| (t, EdgeConfidence::Exact))
                .or_else(|| {
                    recorded
                        .strip_prefix(RPC_INFERRED_METHOD_PREFIX)
                        .map(|t| (t, EdgeConfidence::Heuristic))
                }) {
                let (target, recorded_confidence) = t;
                let Some((s, m)) = target.rsplit_once('.') else {
                    continue;
                };
                if fold_rpc_name(m) != want_method {
                    continue;
                }
                (
                    service_confidence(s, recorded_confidence),
                    RecordedCaller::Method,
                )
            } else if recorded
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                && recorded
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            {
                (
                    service_confidence(recorded, EdgeConfidence::Heuristic).map(|c| match c {
                        EdgeConfidence::Exact => EdgeConfidence::Heuristic,
                        weaker => weaker,
                    }),
                    RecordedCaller::ServiceClient,
                )
            } else {
                continue;
            };
            let (Some(confidence), Some(node)) = (found, self.nodes.get(caller_id)) else {
                continue;
            };
            let list = match kind {
                RecordedCaller::Method => &mut methods,
                RecordedCaller::ServiceClient => &mut clients,
            };
            match list.iter_mut().find(|(n, _, _)| n.id == node.id) {
                Some(seen) if strength(confidence) > strength(seen.1) => seen.1 = confidence,
                Some(_) => {}
                None => list.push((node, confidence, kind)),
            }
        }
        let called: HashSet<&FilePath> = methods.iter().map(|(n, _, _)| &n.file_path).collect();
        let mut seen: HashSet<NodeId> = methods.iter().map(|(n, _, _)| n.id).collect();
        let clients: Vec<_> = clients
            .into_iter()
            .filter(|(n, _, _)| !called.contains(&n.file_path) && seen.insert(n.id))
            .collect();
        let mut out = methods;
        out.extend(clients);
        out.sort_by(|(a, _, _), (b, _, _)| {
            (&a.file_path, a.line_start, a.id).cmp(&(&b.file_path, b.line_start, b.id))
        });
        out
    }

    /// Asynchronous causal impact flow resolution
    pub fn analyze_impact(&self, target: &str) -> ImpactFlow<'_> {
        let mut norm_target = normalize_topic_key(target);
        // The value a constant holds (`'event.created'`) is how people often
        // name an event, while the index keys on the member (`event_created`):
        // the tool description itself cited `event.created`, which 7.0.6 did
        // not match. Tried only when the spelling as written matches no key.
        let any_key = |key: &str| {
            self.topic_producers
                .keys()
                .chain(self.topic_consumers.keys())
                .any(|k| k.contains(key))
        };
        if !any_key(&norm_target) {
            let alt = norm_target.replace(['.', ':', '-', '/'], "_");
            if alt != norm_target && any_key(&alt) {
                norm_target = alt;
            }
        }
        // A topic whose key *is* the target wins over topics merely containing
        // it: `EVENT_CREATED` must not pull in `WS_EVENT_CREATED_FEEDBACK`.
        // Substring matching remains the fallback when no key is exact.
        let exact = self.topic_producers.contains_key(norm_target.as_str())
            || self.topic_consumers.contains_key(norm_target.as_str());
        let key_matches = |key: &str| {
            if exact {
                key == norm_target
            } else {
                key.contains(norm_target.as_str())
            }
        };
        let mut upstream_producers = Vec::new();
        let mut topics = Vec::new();
        let mut downstream_consumers = Vec::new();
        let mut related_sagas = Vec::new();

        // 1. Check topic registry. `topic_producers`/`topic_consumers` are
        // `HashMap`s: iterated directly, their per-process random hash seed
        // would reorder `upstream_producers`/`downstream_consumers` on every
        // run for identical input, violating I5. Sorted by topic name first.
        let mut producer_topics: Vec<&CompactStr> = self.topic_producers.keys().collect();
        producer_topics.sort();
        for topic_name in producer_topics {
            if key_matches(topic_name) {
                upstream_producers.extend(
                    self.topic_producers[topic_name]
                        .iter()
                        .filter_map(|id| self.nodes.get(id)),
                );
            }
        }

        let mut consumer_topics: Vec<&CompactStr> = self.topic_consumers.keys().collect();
        consumer_topics.sort();
        for topic_name in consumer_topics {
            if key_matches(topic_name) {
                downstream_consumers.extend(
                    self.topic_consumers[topic_name]
                        .iter()
                        .filter_map(|id| self.nodes.get(id)),
                );
            }
        }

        // 2. Check nodes for topics and sagas
        for node in self.nodes.values() {
            let interesting = matches!(
                node.kind,
                NodeKind::KafkaTopic
                    | NodeKind::EventStream
                    | NodeKind::Queue
                    | NodeKind::Saga
                    | NodeKind::PostProcessor
            );
            if !interesting {
                continue;
            }
            let name = node.name.as_str();
            let pattern_topic = name
                .strip_prefix("produce:")
                .or_else(|| name.strip_prefix("consume:"));
            // Named after its topic: a stream-like node, or a custom-pattern
            // `produce:<topic>` / `consume:<topic>` node of any kind.
            let is_topic_like = pattern_topic.is_some()
                || matches!(
                    node.kind,
                    NodeKind::KafkaTopic | NodeKind::EventStream | NodeKind::Queue
                );
            let matched = if exact && is_topic_like {
                normalize_topic_key(pattern_topic.unwrap_or(name)) == norm_target
            } else {
                contains_ignore_ascii_case(node.name.as_str(), &norm_target)
            };
            if !matched {
                continue;
            }
            match node.kind {
                NodeKind::KafkaTopic | NodeKind::EventStream | NodeKind::Queue => topics.push(node),
                NodeKind::Saga => related_sagas.push(node),
                _ => downstream_consumers.push(node),
            }
        }

        ImpactFlow {
            target: CompactStr::new(target),
            key: CompactStr::new(&norm_target),
            upstream_producers,
            topics,
            downstream_consumers,
            related_sagas,
            downstream_producers: Vec::new(),
            unexpanded: Vec::new(),
            chain_end: Vec::new(),
            files_scanned: self.file_to_nodes.len(),
        }
    }

    /// Same causal blast-radius report as [`Self::analyze_impact`], extended with
    /// real graph traversal (BFS over `Produces`/`Consumes` edges, not another
    /// substring pass) for every hop past the first: a transitive consumer that
    /// itself produces onto another topic pulls in *that* topic's consumers too,
    /// up to `depth` hops. `depth <= 1` is exactly `analyze_impact`'s direct-only
    /// result, unchanged.
    ///
    /// `visited_topics`/`visited_nodes` bound the walk against cycles (a saga that
    /// re-produces onto a topic upstream of it) — each node and topic is expanded
    /// at most once, so the traversal always terminates even on a graph with a
    /// causal loop, matching the cycle-detection requirement for bounded C-FFI/AST
    /// walks elsewhere in this crate (see `AstGuard`).
    pub fn analyze_impact_with_depth(&self, target: &str, depth: usize) -> ImpactFlow<'_> {
        let mut flow = self.analyze_impact(target);
        let depth = depth.clamp(1, Self::MAX_IMPACT_DEPTH);

        // Total order over content with the node id as the last tiebreaker, so
        // two nodes sharing file, line and name never swap between runs.
        let by_location = |nodes: &mut Vec<&'_ ContractNode>| {
            nodes.sort_by(|a, b| {
                (&a.file_path, a.line_start, &a.name, a.id).cmp(&(
                    &b.file_path,
                    b.line_start,
                    &b.name,
                    b.id,
                ))
            });
            nodes.dedup_by_key(|n| n.id);
        };

        if depth <= 1 {
            // Reached, not expanded: no claim about what lies past them.
            let mut unexpanded: Vec<&ContractNode> = flow
                .downstream_consumers
                .iter()
                .chain(flow.related_sagas.iter())
                .copied()
                .collect();
            by_location(&mut unexpanded);
            flow.unexpanded = unexpanded;
            return flow;
        }
        let mut expanded: Vec<NodeId> = Vec::new();

        let mut visited_topics: HashSet<NodeId> = flow.topics.iter().map(|n| n.id).collect();
        let mut visited_nodes: HashSet<NodeId> = flow
            .downstream_consumers
            .iter()
            .chain(flow.related_sagas.iter())
            .map(|n| n.id)
            .collect();
        let mut frontier: HashSet<NodeId> = visited_nodes.clone();
        let mut visited_producers: HashSet<NodeId> =
            flow.upstream_producers.iter().map(|n| n.id).collect();

        for _hop in 2..=depth {
            if frontier.is_empty() {
                break;
            }
            // A consumer rarely carries the `Produces` edge itself: the
            // re-emission is a separate producer node (a `produce:` pattern
            // call site, a `client.emit` in another method) in the same file.
            // 7.0.6 walked only the consumer's own edges, so `depth: 2` gave
            // the depth-1 answer on Volontariapp's post-processors, silently.
            expanded.extend(frontier.iter().copied());
            let frontier_files: HashSet<&FilePath> = frontier
                .iter()
                .filter_map(|id| self.nodes.get(id))
                .map(|n| &n.file_path)
                .collect();

            // Every topic a node in the current frontier produces onto, in
            // stable edge-insertion order (not `HashSet` order) so the result
            // stays deterministic across runs (I5). A single pass over
            // `self.edges` — not one pass per frontier node — keeps one hop
            // O(|edges|) regardless of how wide the frontier is.
            let mut new_topics: Vec<NodeId> = Vec::new();
            let mut new_topics_set: HashSet<NodeId> = HashSet::new();
            for edge in &self.edges {
                if edge.kind != EdgeKind::Produces {
                    continue;
                }
                let own = frontier.contains(&edge.from);
                let same_file = !own
                    && self
                        .nodes
                        .get(&edge.from)
                        .is_some_and(|p| frontier_files.contains(&p.file_path));
                if !(own || same_file) {
                    continue;
                }
                if same_file && visited_producers.insert(edge.from) {
                    if let Some(producer) = self.nodes.get(&edge.from) {
                        flow.downstream_producers.push(producer);
                    }
                }
                if visited_topics.insert(edge.to) {
                    new_topics.push(edge.to);
                    new_topics_set.insert(edge.to);
                }
            }
            for topic_id in &new_topics {
                if let Some(topic_node) = self.nodes.get(topic_id) {
                    flow.topics.push(topic_node);
                }
            }

            // Consumers of *any* newly-discovered topic, again in one pass
            // over `self.edges` rather than one pass per topic — keeps this
            // hop O(|edges|) too instead of O(new_topics × |edges|).
            //
            // `reconcile_edges` emits `Consumes` as topic -> consumer (the
            // direction events flow, also what the graph renderers draw), while
            // `add_edge` indexes it as consumer -> topic. The walk used to
            // accept only the latter, so on an indexed workspace no hop past
            // the first ever reached a consumer. The topic end is whichever end
            // was just discovered; the consumer is the other one.
            let mut next_frontier: HashSet<NodeId> = HashSet::new();
            for edge in &self.edges {
                if edge.kind != EdgeKind::Consumes {
                    continue;
                }
                let consumer_id = if new_topics_set.contains(&edge.from) {
                    edge.to
                } else if new_topics_set.contains(&edge.to) {
                    edge.from
                } else {
                    continue;
                };
                if !visited_nodes.insert(consumer_id) {
                    continue;
                }
                let Some(consumer) = self.nodes.get(&consumer_id) else {
                    continue;
                };
                next_frontier.insert(consumer_id);
                match consumer.kind {
                    NodeKind::Saga => flow.related_sagas.push(consumer),
                    _ => flow.downstream_consumers.push(consumer),
                }
            }
            frontier = next_frontier;
        }

        // Left in `frontier` when the hops ran out: reached, not expanded.
        let mut unexpanded: Vec<&ContractNode> = frontier
            .iter()
            .filter_map(|id| self.nodes.get(id))
            .collect();
        by_location(&mut unexpanded);
        flow.unexpanded = unexpanded;

        // Expanded consumers with no resolved onward publish (a `Produces` edge
        // of their own or of any node in their file, the file-linked rule the
        // walk uses). The edge scan runs only when there is something to judge.
        if !expanded.is_empty() {
            let mut producing_nodes: HashSet<NodeId> = HashSet::new();
            let mut producing_files: HashSet<&FilePath> = HashSet::new();
            for edge in &self.edges {
                if edge.kind != EdgeKind::Produces {
                    continue;
                }
                producing_nodes.insert(edge.from);
                if let Some(p) = self.nodes.get(&edge.from) {
                    producing_files.insert(&p.file_path);
                }
            }
            let mut chain_end: Vec<&ContractNode> = expanded
                .iter()
                .filter_map(|id| self.nodes.get(id))
                .filter(|n| {
                    !producing_nodes.contains(&n.id) && !producing_files.contains(&n.file_path)
                })
                .collect();
            by_location(&mut chain_end);
            flow.chain_end = chain_end;
        }

        flow
    }

    /// Impact matrix of a change to `target` (plan 4 step 4.6a): the gRPC
    /// handlers and clients of the proto method/service it names (resolved like
    /// [`Self::analyze_grpc`]) plus the async producers, topics, consumers and
    /// sagas of the events it names ([`Self::analyze_impact_with_depth`]), each
    /// classified `Internal` or `External`.
    ///
    /// **Owner rule.** The graph has no service field, so a service is a root
    /// (`repo_id`). The owner of a proto contract is the set of roots of the
    /// handlers that implement it (`Implements` edges); with no handler, the
    /// root of the `.proto` itself. A row linked through a contract (`via`) is
    /// `Internal` when its root is in that contract's owner set, else
    /// `External`. Rows with no `via` (direct name matches with no edge) are
    /// classified against the union of every contract's owners. The owner of an
    /// async event is the set of roots of its producers, else of its topics.
    ///
    /// **Edge cases.** A node with no root (`RepoId::MAX`: the synthetic
    /// cross-repo topic hubs) is never `Internal` and never an owner. A client
    /// whose `CallsRpc` edges fan out to several homonymous contracts
    /// (`Ambiguous`) gets one row per candidate contract, each classified
    /// against that candidate's owners. A service-level client (bound to the
    /// method's service, not to the method) is capped at `Heuristic`: the edge
    /// proves the binding, not that this method is called.
    ///
    /// **Dedup.** One row per `(node, role, via)`; when the same key is reached
    /// by several paths the strongest confidence wins (exact > heuristic >
    /// ambiguous). A via-less direct-match row is dropped when the same
    /// `(node, role)` also has a row linked through a contract.
    ///
    /// **Order.** External first, then role, root-qualified path, line, name,
    /// `via` (path, line, name), confidence — a total order over content, not
    /// over `NodeId`s or hash iteration.
    pub fn impact_matrix(&self, target: &str, depth: usize) -> ImpactMatrix<'_> {
        self.impact_matrix_visible(target, depth, &|_| false)
    }

    /// [`Self::impact_matrix`] seen through `hide` (the caller's test-file rule,
    /// judged on root-relative paths): hidden nodes are left out of the rows
    /// (counted in `hidden_rows`) and every note (no producer / consumer, depth
    /// limit, chain end) is computed from the same visible set as the rows.
    pub fn impact_matrix_visible(
        &self,
        target: &str,
        depth: usize,
        hide: &dyn Fn(&ContractNode) -> bool,
    ) -> ImpactMatrix<'_> {
        let norm_target = target.trim();

        // ── gRPC: anchors, then their handlers/clients through real edges ──
        let mut anchors: Vec<&ContractNode> = Vec::new();
        // (node, role, confidence, via); the owner anchor is kept alongside.
        let mut grpc: Vec<(
            &ContractNode,
            ImpactRole,
            EdgeConfidence,
            Option<&ContractNode>,
        )> = Vec::new();
        for (node, confidence, is_symbol_match) in self.grpc_anchor_matches(norm_target).0 {
            if Self::is_proto_file(&node.file_path) {
                anchors.push(node);
            } else if is_symbol_match {
                anchors.push(node);
                let role = if node.name.as_str().starts_with("rpc:") {
                    ImpactRole::Client
                } else {
                    ImpactRole::Handler
                };
                grpc.push((node, role, confidence, None));
            }
        }

        // Edge target -> (anchor the row is classified against, via node, is service-level).
        let mut edge_targets: HashMap<NodeId, Vec<(NodeId, bool)>> = HashMap::new();
        for anchor in &anchors {
            edge_targets
                .entry(anchor.id)
                .or_default()
                .push((anchor.id, false));
            // A proto method's own service: clients bind to the service
            // (`NewFooServiceClient`, `getService('FooService')`), not to a
            // method, and `reconcile_edges` points that `CallsRpc` edge at the
            // `.proto` service or, when one is indexed, at a server-side
            // implementation of it (Go `RegisterFooServiceServer`, a tonic
            // `impl FooService for …`). Both count: the `.proto` one only in
            // the method's own file, a code one by its exact service name.
            if anchor.kind == NodeKind::GrpcMethod && Self::is_proto_file(&anchor.file_path) {
                if let Some((service_name, _)) = anchor.name.rsplit_once('.') {
                    let same_name = self
                        .name_to_nodes
                        .get(service_name)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    for sid in same_name {
                        let Some(service) = self.nodes.get(sid) else {
                            continue;
                        };
                        let in_scope = !Self::is_proto_file(&service.file_path)
                            || service.file_path == anchor.file_path;
                        if service.kind == NodeKind::GrpcService && in_scope {
                            edge_targets
                                .entry(service.id)
                                .or_default()
                                .push((anchor.id, true));
                        }
                    }
                }
            }
        }
        // (row index in `grpc`) -> owner anchor id; via-less rows have none.
        let mut owner_anchor: Vec<Option<NodeId>> = vec![None; grpc.len()];
        for edge in &self.edges {
            let role = match edge.kind {
                EdgeKind::Implements => ImpactRole::Handler,
                EdgeKind::CallsRpc => ImpactRole::Client,
                _ => continue,
            };
            let Some(targets) = edge_targets.get(&edge.to) else {
                continue;
            };
            let (Some(from), Some(via)) = (self.nodes.get(&edge.from), self.nodes.get(&edge.to))
            else {
                continue;
            };
            for &(anchor_id, service_level) in targets {
                // Only a client can bind to the service; an `Implements` edge
                // to the service is not an implementation of this method.
                if service_level && role != ImpactRole::Client {
                    continue;
                }
                let confidence = if service_level {
                    Self::weakest(edge.confidence, EdgeConfidence::Heuristic)
                } else {
                    edge.confidence
                };
                grpc.push((from, role, confidence, Some(via)));
                owner_anchor.push(Some(anchor_id));
            }
        }

        // Owner roots per anchor: roots of its handlers, else its own root —
        // except a client call site (`rpc:`), which never owns the contract.
        let mut owners_of: HashMap<NodeId, Vec<RepoId>> = HashMap::new();
        for ((node, role, _, _), anchor) in grpc.iter().zip(&owner_anchor) {
            if let (ImpactRole::Handler, Some(anchor)) = (role, anchor) {
                if node.repo_id != RepoId::MAX {
                    owners_of.entry(*anchor).or_default().push(node.repo_id);
                }
            }
        }
        for anchor in &anchors {
            let owners = owners_of.entry(anchor.id).or_default();
            if owners.is_empty()
                && anchor.repo_id != RepoId::MAX
                && !anchor.name.starts_with("rpc:")
            {
                owners.push(anchor.repo_id);
            }
            owners.sort_unstable();
            owners.dedup();
        }
        let mut grpc_owners: Vec<RepoId> = owners_of.values().flatten().copied().collect();
        grpc_owners.extend(
            grpc.iter()
                .zip(&owner_anchor)
                .filter(|((n, role, _, _), a)| {
                    a.is_none() && *role == ImpactRole::Handler && n.repo_id != RepoId::MAX
                })
                .map(|((n, _, _, _), _)| n.repo_id),
        );
        grpc_owners.sort_unstable();
        grpc_owners.dedup();

        // Via-less rows are dropped when the same (node, role) is also linked
        // through a contract.
        let linked: HashSet<(NodeId, ImpactRole)> = grpc
            .iter()
            .filter(|(_, _, _, via)| via.is_some())
            .map(|(n, role, _, _)| (n.id, *role))
            .collect();

        let mut rows: Vec<ImpactRow<'_>> = Vec::new();
        for ((node, role, confidence, via), anchor) in grpc.into_iter().zip(owner_anchor) {
            if via.is_none() && linked.contains(&(node.id, role)) {
                continue;
            }
            let owners = match anchor {
                Some(a) => owners_of.get(&a).map(Vec::as_slice).unwrap_or(&[]),
                None => grpc_owners.as_slice(),
            };
            rows.push(ImpactRow {
                node,
                role,
                scope: Self::scope_of(node, owners),
                confidence,
                via,
            });
        }

        // ── Async: producers / topics / consumers / sagas ──
        let flow = self.analyze_impact_with_depth(norm_target, depth);
        let target_lc = flow.key.to_lowercase();
        let file_linked_hops = !flow.downstream_producers.is_empty();
        let event_matched = !flow.topics.is_empty()
            || !flow.upstream_producers.is_empty()
            || !flow.downstream_consumers.is_empty();
        let shown = |nodes: &[&ContractNode]| nodes.iter().any(|n| !hide(n));
        // The synthetic cross-repo topic hub (no root) is not a site in any file.
        let shown_in_a_root =
            |nodes: &[&ContractNode]| nodes.iter().any(|n| n.repo_id != RepoId::MAX && !hide(n));
        let event_only_in_tests = event_matched
            && !shown_in_a_root(&flow.topics)
            && !shown_in_a_root(&flow.upstream_producers)
            && !shown_in_a_root(&flow.downstream_consumers);
        let no_producer_resolved = event_matched
            && !shown(&flow.upstream_producers)
            && !flow
                .topics
                .iter()
                .any(|n| n.name.starts_with("produce:") && !hide(n));
        let no_consumer_resolved = event_matched
            && !flow
                .downstream_consumers
                .iter()
                .chain(
                    flow.topics
                        .iter()
                        .filter(|n| n.name.starts_with("consume:")),
                )
                .any(|n| !hide(n));
        let mut async_owners: Vec<RepoId> = flow
            .upstream_producers
            .iter()
            .map(|n| n.repo_id)
            .filter(|r| *r != RepoId::MAX)
            .collect();
        if async_owners.is_empty() {
            async_owners.extend(
                flow.topics
                    .iter()
                    .map(|n| n.repo_id)
                    .filter(|r| *r != RepoId::MAX),
            );
        }
        async_owners.sort_unstable();
        async_owners.dedup();
        for (nodes, role) in [
            (&flow.upstream_producers, ImpactRole::Producer),
            (&flow.topics, ImpactRole::Topic),
            (&flow.downstream_consumers, ImpactRole::Consumer),
            (&flow.related_sagas, ImpactRole::Saga),
            (&flow.downstream_producers, ImpactRole::Producer),
        ] {
            for node in nodes {
                // `analyze_impact`'s node scan lists a pattern-declared
                // `produce:`/`consume:` call site among the topics; in the
                // matrix it is what its name says it is.
                let role = if node.name.starts_with("produce:") {
                    ImpactRole::Producer
                } else if node.name.starts_with("consume:") {
                    ImpactRole::Consumer
                } else {
                    role
                };
                rows.push(ImpactRow {
                    node,
                    role,
                    scope: Self::scope_of(node, &async_owners),
                    confidence: self.async_match_confidence(node, &target_lc),
                    via: None,
                });
            }
        }

        // ── Dedup (strongest confidence per key), then a total content order ──
        // The same key reached through several anchors keeps its strongest
        // confidence, then `External` (the conservative answer) on a tie.
        rows.sort_by_key(|r| {
            (
                r.node.id,
                r.role,
                r.via.map(|v| v.id),
                Self::confidence_rank(r.confidence),
                r.scope,
            )
        });
        rows.dedup_by(|later, first| {
            later.node.id == first.node.id
                && later.role == first.role
                && later.via.map(|v| v.id) == first.via.map(|v| v.id)
        });
        rows.sort_by(|a, b| Self::row_key(a).cmp(&Self::row_key(b)));

        let before = rows.len();
        rows.retain(|r| !hide(r.node));
        let hidden_rows = before - rows.len();

        let mut owner_roots = grpc_owners;
        owner_roots.extend(async_owners);
        owner_roots.sort_unstable();
        owner_roots.dedup();

        let mut contracts: Vec<&ContractNode> = anchors
            .into_iter()
            .filter(|n| Self::is_proto_file(&n.file_path))
            .collect();
        contracts.sort_by(|a, b| {
            (&a.file_path, a.line_start, &a.name).cmp(&(&b.file_path, b.line_start, &b.name))
        });

        ImpactMatrix {
            target: CompactStr::new(norm_target),
            contracts,
            owner_roots,
            rows,
            no_producer_resolved,
            no_consumer_resolved,
            file_linked_hops,
            depth: depth.clamp(1, Self::MAX_IMPACT_DEPTH),
            unexpanded: flow
                .unexpanded
                .iter()
                .copied()
                .filter(|n| !hide(n))
                .collect(),
            chain_end: flow
                .chain_end
                .iter()
                .copied()
                .filter(|n| !hide(n))
                .collect(),
            event_only_in_tests,
            hidden_rows,
        }
    }

    /// `Internal` when `node` has a root and it is one of `owners`.
    fn scope_of(node: &ContractNode, owners: &[RepoId]) -> ImpactScope {
        if node.repo_id != RepoId::MAX && owners.contains(&node.repo_id) {
            ImpactScope::Internal
        } else {
            ImpactScope::External
        }
    }

    /// Exact (0) is the strongest, ambiguous (2) the weakest.
    fn confidence_rank(c: EdgeConfidence) -> u8 {
        match c {
            EdgeConfidence::Exact => 0,
            EdgeConfidence::Heuristic => 1,
            EdgeConfidence::Ambiguous => 2,
        }
    }

    fn weakest(a: EdgeConfidence, b: EdgeConfidence) -> EdgeConfidence {
        if Self::confidence_rank(a) >= Self::confidence_rank(b) {
            a
        } else {
            b
        }
    }

    /// Content sort key of an impact row (rows are already unique per
    /// `(node, role, via)`). The `NodeId`s come last and only separate two
    /// distinct nodes with identical content.
    #[allow(clippy::type_complexity)]
    fn row_key<'a>(
        row: &'a ImpactRow<'_>,
    ) -> (
        ImpactScope,
        ImpactRole,
        &'a Path,
        usize,
        &'a str,
        Option<(&'a Path, usize, &'a str)>,
        NodeId,
        Option<NodeId>,
    ) {
        (
            row.scope,
            row.role,
            &row.node.file_path,
            row.node.line_start,
            row.node.name.as_str(),
            row.via
                .map(|v| (&*v.file_path, v.line_start, v.name.as_str())),
            row.node.id,
            row.via.map(|v| v.id),
        )
    }

    /// Exact (or heuristic, for a node registered by a heuristic source) when
    /// `node` is registered on exactly the target topic, or its
    /// name (minus a `produce:`/`consume:` prefix) is the target; otherwise the
    /// row came from a substring match or a transitive hop: heuristic.
    fn async_match_confidence(&self, node: &ContractNode, target_lc: &str) -> EdgeConfidence {
        let registered = |index: &HashMap<CompactStr, Vec<NodeId>>| {
            index
                .get(target_lc)
                .is_some_and(|ids| ids.contains(&node.id))
        };
        let name = node.name.as_str();
        let bare = name
            .strip_prefix("produce:")
            .or_else(|| name.strip_prefix("consume:"))
            .unwrap_or(name);
        if registered(&self.topic_producers)
            || registered(&self.topic_consumers)
            || bare.eq_ignore_ascii_case(target_lc)
        {
            // A registration made by a heuristic source (declarative patterns by
            // default) stays heuristic in the matrix as it does on the edge.
            self.topic_confidence(node.id)
        } else {
            EdgeConfidence::Heuristic
        }
    }

    /// Case-insensitive substring search over symbol declarations, restricted to
    /// files under `scope_filter` when given. Exact-name hits come first.
    pub fn search_symbols(&self, query: &str, scope_filter: Option<&Path>) -> Vec<&ContractNode> {
        let mut matches: Vec<&ContractNode> = Vec::new();
        let mut seen: HashSet<NodeId> = HashSet::new();

        // Fast path: exact symbol name via the O(1) index.
        if let Some(ids) = self.name_to_nodes.get(query) {
            for id in ids {
                if let Some(node) = self.nodes.get(id) {
                    if node.kind != NodeKind::Module
                        && scope_filter.is_none_or(|s| node.file_path.starts_with(s))
                        && seen.insert(node.id)
                    {
                        matches.push(node);
                    }
                }
            }
        }

        // Substring path: walk only the files inside the scope instead of every node.
        // `file_to_nodes` is a `HashMap`, so the walk itself is in hash-seed order;
        // only the (usually few) *matches* are sorted afterwards, by content, so the
        // result order is still independent of the per-process seed (I5) without
        // sorting every in-scope path on every query.
        let exact = matches.len();
        let in_scope = self
            .file_to_nodes
            .iter()
            .filter(|(path, _)| scope_filter.is_none_or(|s| path.starts_with(s)))
            .flat_map(|(_, ids)| ids.iter());
        for id in in_scope {
            if seen.contains(id) {
                continue;
            }
            if let Some(node) = self.nodes.get(id) {
                if node.kind != NodeKind::Module
                    && (contains_ignore_ascii_case(node.name.as_str(), query)
                        || Self::bare_names_match(node.name.as_str(), query))
                {
                    seen.insert(node.id);
                    matches.push(node);
                }
            }
        }
        matches[exact..].sort_by(|a, b| {
            a.file_path
                .cmp(&b.file_path)
                .then_with(|| a.line_start.cmp(&b.line_start))
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.id.cmp(&b.id))
        });

        matches
    }
}
