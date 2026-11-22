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

    /// The resolver of recorded RPC call records against this graph's client
    /// field bindings and declared RPCs (see [`RpcRecordResolver`]).
    pub fn rpc_record_resolver(&self) -> RpcRecordResolver<'_> {
        let mut declared: HashMap<(String, String), Vec<CompactStr>> = HashMap::new();
        for (_, name) in self.proto_methods() {
            let Some((service, method)) = name.rsplit_once('.') else {
                continue;
            };
            let bare = service.rsplit('.').next().unwrap_or(service);
            let names = declared
                .entry((fold_rpc_name(bare), fold_rpc_name(method)))
                .or_default();
            if !names.contains(&name) {
                names.push(name.clone());
            }
        }
        RpcRecordResolver {
            class_fields: self.class_fields(),
            declared,
            consts: self.const_values(),
        }
    }

    /// Call sites recorded for `service.method`, whether or not they resolved
    /// to a declared RPC. A caller of an RPC that no longer exists has no
    /// `CallsRpc` edge (there is no node left to link it to), so `analyze_grpc`
    /// cannot list it: `diff_mesh` asks here instead.
    ///
    /// - Every call record is resolved as `reconcile_edges` resolves it
    ///   ([`RpcRecordResolver`]): `=Service.Method` (a client binding, exact),
    ///   `~Service.Method` (the service inferred from a field name, heuristic)
    ///   and `^Base#field.method` (a field inherited from `Base`, exact through
    ///   `Base`'s binding, heuristic when only the field name tells). A method
    ///   may name the service with its proto package
    ///   (`=demo.v1.UserService.DeleteUser`): the same as `package` (the
    ///   `.proto`'s own, when known) keeps the confidence; no known package
    ///   makes it heuristic; another package makes it ambiguous (a homonym
    ///   service, or the same one renamed). Reported at the calling node.
    /// - A class binding a client field to the service (`#field=Service`) with
    ///   no resolved call of the method in it, nor through `^` records naming
    ///   it as their base, is a [`RecordedCaller::BoundClient`], heuristic.
    /// - A service-level record (a client constructed for the service, without
    ///   the method resolved: Go `NewUserServiceClient`, a Python stub, a Java
    ///   `newBlockingStub`) is a [`RecordedCaller::ServiceClient`] that may call
    ///   the method, heuristic, listed only for a file with no method-level call
    ///   and no client field binding of the service (the binding's class
    ///   answers for it).
    ///
    /// Names compare with [`fold_rpc_name`]. Sorted by location, each node once.
    /// `resolver` is this graph's [`Self::rpc_record_resolver`], built once by
    /// a caller asking about several RPCs.
    pub fn recorded_rpc_callers<'g>(
        &'g self,
        resolver: &RpcRecordResolver<'g>,
        service: &str,
        method: &str,
        package: Option<&str>,
    ) -> Vec<(&'g ContractNode, EdgeConfidence, RecordedCaller)> {
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
        let at_most_heuristic = |c: EdgeConfidence| match c {
            EdgeConfidence::Exact => EdgeConfidence::Heuristic,
            weaker => weaker,
        };
        type Row<'n> = (&'n ContractNode, EdgeConfidence, RecordedCaller);
        fn keep_strongest<'n>(list: &mut Vec<Row<'n>>, row: Row<'n>) {
            let strength = |c: EdgeConfidence| match c {
                EdgeConfidence::Exact => 2u8,
                EdgeConfidence::Heuristic => 1,
                EdgeConfidence::Ambiguous => 0,
            };
            match list.iter_mut().find(|(n, _, _)| n.id == row.0.id) {
                Some(seen) if strength(row.1) > strength(seen.1) => seen.1 = row.1,
                Some(_) => {}
                None => list.push(row),
            }
        }
        let mut methods: Vec<Row<'_>> = Vec::new();
        let mut clients: Vec<Row<'_>> = Vec::new();
        let mut bound: Vec<Row<'_>> = Vec::new();
        // Base classes a `^Base#field.method` record calls the method through,
        // by (repository, class name): the repositories whose binding of the
        // field the resolution used (the caller's own first, as
        // `resolve_inherited_call` does), else the caller's.
        let mut called_bases: HashSet<(RepoId, &str)> = HashSet::new();
        // Files binding a client field to the service.
        let mut binding_files: HashSet<&FilePath> = HashSet::new();
        for (caller_id, recorded) in &self.rpc_calls {
            let Some(node) = self.nodes.get(caller_id) else {
                continue;
            };
            if let Some(binding) = recorded.strip_prefix(CLIENT_FIELD_BINDING_PREFIX) {
                let Some((_, bound_service)) = binding.split_once('=') else {
                    continue;
                };
                if bound_service.is_empty() {
                    continue;
                }
                if let Some(c) = service_confidence(bound_service, EdgeConfidence::Heuristic) {
                    binding_files.insert(&node.file_path);
                    keep_strongest(
                        &mut bound,
                        (node, at_most_heuristic(c), RecordedCaller::BoundClient),
                    );
                }
                continue;
            }
            let Some(resolved) = resolver.resolve(Some(node.repo_id), recorded) else {
                continue;
            };
            if resolved.method_level {
                let Some((s, m)) = resolved.target.rsplit_once('.') else {
                    continue;
                };
                if fold_rpc_name(m) != want_method {
                    continue;
                }
                let Some(confidence) = service_confidence(s, resolved.confidence) else {
                    continue;
                };
                if let Some((base, field)) = recorded
                    .strip_prefix(RPC_INHERITED_METHOD_PREFIX)
                    .and_then(|r| r.split_once('#'))
                    .and_then(|(base, rest)| rest.split_once('.').map(|(f, _)| (base, f)))
                {
                    let repos: Vec<RepoId> = resolver
                        .class_fields
                        .get(&(base, field))
                        .map(|b| b.iter().map(|(r, _)| *r).collect())
                        .unwrap_or_default();
                    if repos.is_empty() || repos.contains(&node.repo_id) {
                        called_bases.insert((node.repo_id, base));
                    } else {
                        called_bases.extend(repos.into_iter().map(|r| (r, base)));
                    }
                }
                keep_strongest(&mut methods, (node, confidence, RecordedCaller::Method));
            } else if resolved
                .target
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                && resolved
                    .target
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            {
                if let Some(c) = service_confidence(&resolved.target, EdgeConfidence::Heuristic) {
                    keep_strongest(
                        &mut clients,
                        (node, at_most_heuristic(c), RecordedCaller::ServiceClient),
                    );
                }
            }
        }
        let called: HashSet<&FilePath> = methods.iter().map(|(n, _, _)| &n.file_path).collect();
        let mut seen: HashSet<NodeId> = methods.iter().map(|(n, _, _)| n.id).collect();
        // A class whose own body, or a subclass through the inherited field,
        // calls the method is answered by those call rows.
        let calls_inside = |class: &ContractNode| {
            methods.iter().any(|(n, _, _)| {
                n.file_path == class.file_path
                    && n.line_start >= class.line_start
                    && n.line_start <= class.line_end.max(class.line_start)
            })
        };
        let bound: Vec<Row<'_>> = bound
            .into_iter()
            .filter(|(class, _, _)| {
                !called_bases.contains(&(class.repo_id, class.name.as_str()))
                    && !calls_inside(class)
                    && seen.insert(class.id)
            })
            .collect();
        let clients: Vec<Row<'_>> = clients
            .into_iter()
            .filter(|(n, _, _)| {
                !called.contains(&n.file_path)
                    && !binding_files.contains(&n.file_path)
                    && seen.insert(n.id)
            })
            .collect();
        let mut out = methods;
        out.extend(bound);
        out.extend(clients);
        out.sort_by(|(a, _, _), (b, _, _)| {
            (&a.file_path, a.line_start, a.id).cmp(&(&b.file_path, b.line_start, b.id))
        });
        out
    }

    /// Server handlers whose `@GrpcMethod(service, method)` arguments name
    /// `service.method`, resolved through string constants as
    /// `reconcile_edges` resolves them but without the RPC having to be
    /// declared: the handlers a removed RPC leaves orphaned. A handler that
    /// resolved to a declared RPC (`Service.Method` still declared somewhere)
    /// is not one. Exact when both arguments reduce to literals naming the
    /// RPC (the folded service equal once its package qualifier is stripped;
    /// a qualifier must equal `package` when both are known). When the method
    /// does but the service argument is empty or does not reduce to a literal
    /// (a constant of an unindexed package), the row is heuristic and its
    /// third element is `false` (service not resolved). Sorted by location.
    pub fn handlers_naming_rpc<'g>(
        &'g self,
        resolver: &RpcRecordResolver<'g>,
        service: &str,
        method: &str,
        package: Option<&str>,
    ) -> Vec<(&'g ContractNode, EdgeConfidence, bool)> {
        let (want_service, want_method) = (fold_rpc_name(service), fold_rpc_name(method));
        let refs: Vec<(NodeId, &str)> = self
            .rpc_calls
            .iter()
            .filter(|(id, _)| !self.handler_rpc_names.contains_key(id))
            .filter_map(|(id, recorded)| {
                recorded.strip_prefix(HANDLER_REF_PREFIX).map(|r| (*id, r))
            })
            .collect();
        if refs.is_empty() {
            return Vec::new();
        }
        let consts = &resolver.consts;
        let mut out: Vec<(&ContractNode, EdgeConfidence, bool)> = Vec::new();
        for (id, reference) in refs {
            let (Some(node), Some(parsed)) =
                (self.nodes.get(&id), Self::parse_handler_ref(reference))
            else {
                continue;
            };
            let Some(m) = Self::resolve_literal_expr(parsed.method, node.repo_id, consts) else {
                continue;
            };
            if fold_rpc_name(m.rsplit('.').next().unwrap_or(m)) != want_method {
                continue;
            }
            let (confidence, resolved) =
                match Self::resolve_literal_expr(parsed.service, node.repo_id, consts) {
                    Some(s) => {
                        let (qualifier, bare) = match s.rsplit_once('.') {
                            Some((q, b)) => (Some(q), b),
                            None => (None, s),
                        };
                        if fold_rpc_name(bare) != want_service {
                            continue;
                        }
                        match (qualifier, package) {
                            (Some(q), Some(p)) if q != p => continue,
                            _ => (EdgeConfidence::Exact, true),
                        }
                    }
                    None => (EdgeConfidence::Heuristic, false),
                };
            if !out.iter().any(|(n, _, _)| n.id == node.id) {
                out.push((node, confidence, resolved));
            }
        }
        out.sort_by(|(a, _, _), (b, _, _)| {
            (&a.file_path, a.line_start, a.id).cmp(&(&b.file_path, b.line_start, b.id))
        });
        out
    }

    /// The literal key behind a qualified `target` (`rabbitmq:orders`) that is also the
    /// `rabbitmq` channel of `orders`: a key written so in source (a Kafka topic literally named
    /// `rabbitmq:orders`) while RabbitMQ also uses `orders`. Such a target is ambiguous and
    /// [`Self::analyze_impact`] lists both readings.
    pub fn target_literal_clash(&self, target: &str) -> Option<CompactStr> {
        let norm = self.topic_target_key(target);
        let (broker, bare) = split_qualified_topic_key(&norm)?;
        let literal = self.topic_producers.contains_key(norm.as_str())
            || self.topic_consumers.contains_key(norm.as_str());
        (literal && self.known_brokers(bare) & broker.bit() != 0).then(|| CompactStr::new(&norm))
    }

    /// The registry key a query `target` names: [`normalize_topic_key`], except that an enum
    /// member spelled as written in code (`UserQueue.USER`, or the `${UserQueue.USER:user}` an
    /// extractor records) names the topic its string constant is, when no topic is keyed by the
    /// member itself and the workspace declares exactly one value for it (9.0 T14).
    pub fn topic_target_key(&self, target: &str) -> String {
        let written = member_ref_of_placeholder(target).unwrap_or(target);
        let norm = normalize_topic_key(written);
        let known = |key: &str| {
            self.topic_producers.contains_key(key) || self.topic_consumers.contains_key(key)
        };
        let expr = written.trim().trim_matches(['\'', '"', '`']).trim();
        let expr = expr.strip_prefix("typeof ").map_or(expr, str::trim);
        if known(&norm) || !is_member_ref(expr) {
            return norm;
        }
        // Distinct values only: copies of one enum that agree are one value, two that disagree
        // are not a guess.
        let mut values: Vec<String> = self
            .rpc_calls
            .iter()
            .filter_map(|(_, r)| r.strip_prefix(CONST_VALUE_PREFIX))
            .filter_map(|c| c.split_once('='))
            .filter(|(path, _)| *path == expr)
            .map(|(_, value)| normalize_topic_key(value))
            .collect();
        values.sort_unstable();
        values.dedup();
        match values.as_slice() {
            [only] if known(only) => only.clone(),
            _ => norm,
        }
    }

    /// Asynchronous causal impact flow resolution
    pub fn analyze_impact(&self, target: &str) -> ImpactFlow<'_> {
        let norm = self.topic_target_key(target);
        // A qualified target (`kafka:orders`, the name of a broker hub) selects one broker's
        // channel of the name: its sites, the unknown-broker ones included (they join every
        // channel of the name). A key that literally is the target (a Redis key spelled
        // `kafka:orders`) is read as that key; when the broker also has sites for the bare
        // name the target is ambiguous and both readings are listed.
        let literal = self.topic_producers.contains_key(norm.as_str())
            || self.topic_consumers.contains_key(norm.as_str());
        let Some((broker, bare)) = split_qualified_topic_key(&norm) else {
            return self.impact_for(target, norm, None);
        };
        if !literal {
            return self.impact_for(target, bare.to_string(), Some(broker));
        }
        if self.known_brokers(bare) & broker.bit() == 0 {
            return self.impact_for(target, norm, None);
        }
        let mut flow = self.impact_for(target, norm.clone(), None);
        let channel = self.impact_for(target, bare.to_string(), Some(broker));
        for (mine, theirs) in [
            (&mut flow.upstream_producers, channel.upstream_producers),
            (&mut flow.topics, channel.topics),
            (&mut flow.downstream_consumers, channel.downstream_consumers),
            (&mut flow.related_sagas, channel.related_sagas),
        ] {
            let seen: HashSet<NodeId> = mine.iter().map(|n| n.id).collect();
            mine.extend(theirs.into_iter().filter(|n| !seen.contains(&n.id)));
        }
        flow
    }

    /// [`Self::analyze_impact`] for one reading of the target: `norm_target` is its key, read
    /// as `broker_filter`'s channel of that name when given.
    fn impact_for(
        &self,
        target: &str,
        mut norm_target: String,
        broker_filter: Option<Broker>,
    ) -> ImpactFlow<'_> {
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
                        .filter(|id| {
                            broker_filter
                                .is_none_or(|b| !self.site_excluded_from(true, topic_name, **id, b))
                        })
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
                        .filter(|id| {
                            broker_filter.is_none_or(|b| {
                                !self.site_excluded_from(false, topic_name, **id, b)
                            })
                        })
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
            // A broker hub is named `kafka:orders` for display; it is the topic `orders`.
            let hub = Self::is_hub_like(node);
            if let Some(b) = broker_filter {
                if hub && Self::hub_broker(node).is_some_and(|nb| nb != b) {
                    continue;
                }
                if !hub && self.node_excluded_from(&norm_target, node.id, b) {
                    continue;
                }
            }
            let name = if hub {
                Self::hub_key(node)
            } else {
                node.name.as_str()
            };
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
                contains_ignore_ascii_case(name, &norm_target)
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
            call_linked_producers: Vec::new(),
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
        // Hops past the first come from the chain engine (`flow`), the same one
        // `trace_flow` renders: from each consumer, the resolved calls to every
        // publish they reach; producers declared in the consumer's own file only
        // when no call path reaches a publish (the 8.x same-file rule, heuristic).
        // `depth` counts the direct hop, so the engine follows `depth - 1` levels.
        // No fan-out cap: the matrix lists every row and pages them.
        let starts: Vec<&ContractNode> = flow
            .downstream_consumers
            .iter()
            .chain(flow.related_sagas.iter())
            .copied()
            .collect();
        let seen_topics: HashSet<NodeId> = flow.topics.iter().map(|n| n.id).collect();
        let trace = self.trace_from(
            starts,
            super::flow::FlowStepKind::Consume,
            Some(flow.key.clone()),
            seen_topics,
            super::flow::FlowDirection::Forward,
            super::flow::FlowBounds {
                call_depth: super::flow::DEFAULT_CALL_DEPTH,
                event_depth: depth - 1,
                fan_out: usize::MAX,
            },
            &|_| false,
        );
        let mut seen_nodes: HashSet<NodeId> = flow
            .downstream_consumers
            .iter()
            .chain(flow.related_sagas.iter())
            .map(|n| n.id)
            .collect();
        for consumer in &trace.reached {
            if seen_nodes.insert(consumer.id) {
                match consumer.kind {
                    NodeKind::Saga => flow.related_sagas.push(consumer),
                    _ => flow.downstream_consumers.push(consumer),
                }
            }
        }
        flow.topics.extend(trace.topics.iter().copied());
        let mut visited_producers: HashSet<NodeId> =
            flow.upstream_producers.iter().map(|n| n.id).collect();
        let mut call_linked: Vec<NodeId> = Vec::new();
        for hop in &trace.hops {
            let (Some(first), Some(last)) = (hop.steps.first(), hop.steps.last()) else {
                continue;
            };
            // A consumer that publishes itself is not listed again as a producer.
            if last.kind != super::flow::FlowStepKind::Produce || last.node.id == first.node.id {
                continue;
            }
            if visited_producers.insert(last.node.id) {
                flow.downstream_producers.push(last.node);
            }
            if !hop.fallback {
                call_linked.push(last.node.id);
            }
        }
        call_linked.sort_unstable();
        call_linked.dedup();
        flow.call_linked_producers = call_linked;
        flow.unexpanded = trace.unexpanded;
        flow.chain_end = trace.chain_end;
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
        let file_linked_hops = flow
            .downstream_producers
            .iter()
            .any(|p| flow.call_linked_producers.binary_search(&p.id).is_err());
        let call_linked_hops = !flow.call_linked_producers.is_empty();
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
                // A call-site producer (9.0) is named after its topic: the row
                // names the declaration that publishes through `via`.
                let via = (role == ImpactRole::Producer)
                    .then(|| self.enclosing_declaration(node))
                    .flatten();
                rows.push(ImpactRow {
                    node,
                    role,
                    scope: Self::scope_of(node, &async_owners),
                    confidence: self.async_match_confidence(node, &target_lc),
                    via,
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
            call_linked_hops,
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
        let name = if Self::is_hub_like(node) {
            Self::hub_key(node)
        } else {
            node.name.as_str()
        };
        let bare = name
            .strip_prefix("produce:")
            .or_else(|| name.strip_prefix("consume:"))
            .unwrap_or(name);
        if registered(&self.topic_producers)
            || registered(&self.topic_consumers)
            || bare.eq_ignore_ascii_case(target_lc)
        {
            // A site of unknown broker on a name two brokers share joins both channels,
            // so its link is a guess; a registration made by a heuristic source
            // (declarative patterns by default) stays heuristic in the matrix as it
            // does on the edge.
            if (registered(&self.topic_producers) || registered(&self.topic_consumers))
                && self.known_brokers(target_lc).count_ones() >= 2
                && [true, false]
                    .into_iter()
                    .all(|is_producer| self.site_mask(is_producer, target_lc, node.id) == 0)
            {
                return EdgeConfidence::Heuristic;
            }
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

impl RpcRecordResolver<'_> {
    /// Whether some class named `base` binds `field` to a gRPC service
    /// (`#field=Service`, the service written): the inherited field of a
    /// `^base#field.method` record is a client even when the resolution
    /// cannot tell which one.
    pub fn base_binds_client_field(&self, base: &str, field: &str) -> bool {
        self.class_fields
            .get(&(base, field))
            .is_some_and(|b| b.iter().any(|(_, service)| !service.is_empty()))
    }

    /// One `rpc_calls` record of a caller in `caller_repo`, resolved as
    /// `reconcile_edges` resolves it (`ContractGraph::rpc_call_target`): `None`
    /// for a record that is not a call (a binding, a constant, a handler
    /// reference) or an inherited call that resolves to nothing.
    pub fn resolve(
        &self,
        caller_repo: Option<RepoId>,
        recorded: &str,
    ) -> Option<ResolvedRpcRecord> {
        let (target, method_level, inferred, inherited) =
            ContractGraph::rpc_record_target(caller_repo, recorded, &self.class_fields)?;
        let target = inherited.unwrap_or_else(|| target.to_string());
        let declared = if method_level {
            target.rsplit_once('.').and_then(|(service, method)| {
                let bare = service.rsplit('.').next().unwrap_or(service);
                match self
                    .declared
                    .get(&(fold_rpc_name(bare), fold_rpc_name(method)))
                    .map(Vec::as_slice)
                {
                    Some([only]) => Some(only.clone()),
                    _ => None,
                }
            })
        } else {
            None
        };
        Some(ResolvedRpcRecord {
            target,
            method_level,
            confidence: if inferred {
                EdgeConfidence::Heuristic
            } else {
                EdgeConfidence::Exact
            },
            declared,
        })
    }
}
