use super::resolve::{is_script_path, HandlerIndex, ImportMemo, ResolvedKey};
use super::*;

impl ContractGraph {
    /// Reconciles causal edges across microservices:
    /// 1. Derives `Imports` edges fresh from the raw `reverse_deps` facts.
    /// 2. Links topic producers & consumers to canonical Kafka/Event topic nodes.
    /// 3. Creates direct causal dispatch edges from producers to downstream consumers.
    /// 4. Links service handlers to protobuf RPC declarations (EdgeKind::Implements).
    /// 5. Links RPC client calls to proto methods (EdgeKind::CallsRpc).
    ///
    /// Every lookup goes through the O(1) indices and edge de-duplication uses a
    /// `HashSet`, so the whole pass is linear in nodes + edges (was O(E²)).
    ///
    /// The whole edge set is **cleared and rebuilt from scratch** on every call,
    /// rather than mutating whatever `self.edges` already held: an incremental
    /// reload only re-folds the *changed* files' facts (`WorkspaceIndexer::reload`),
    /// so an unchanged file's edge to a just-reindexed target would otherwise stay
    /// stale (pointing at a node `patch_files` already removed) or vanish entirely
    /// — silently, since that file was never touched this cycle to notice. Rebuilding
    /// from the raw fact indices (`reverse_deps`, `topic_producers`, `topic_consumers`,
    /// `rpc_calls`, and the node-kind scans below), which `patch_files` keeps current
    /// for every *surviving* node regardless of what changed, makes one full rebuild
    /// and any incremental reload converge to the same graph (idempotence invariants
    /// I2 and I3: `derive(derive(g)) == derive(g)`).
    pub fn reconcile_edges(&mut self) {
        self.edges.clear();
        let mut edge_set: HashSet<(NodeId, NodeId, EdgeKind)> = HashSet::new();

        // 1. Imports: resolve every (target, importer) raw fact fresh. Sorted so the
        // edge push order — irrelevant to `canonical_lines()`, which sorts its own
        // output, but a stale node id surviving in `reverse_deps` past its own
        // removal (shouldn't happen; `patch_files` prunes it) can't panic either way
        // since resolution only ever looks up ids still present in `self.nodes`.
        let mut import_facts: Vec<(NodeId, &CompactStr)> = self
            .reverse_deps
            .iter()
            .flat_map(|(target, ids)| ids.iter().map(move |&id| (id, target)))
            .collect();
        import_facts.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));
        let mut import_edges = Vec::new();
        // `resolve_import_targets` depends on the importer only through its repo
        // (its signature takes nothing else), and each call can scan a whole
        // `name_to_nodes` bucket — which for a hot name is O(N). Memoized per
        // (target, importer repo), N importers of one hot name cost one scan
        // instead of N (was O(importers × bucket)).
        // A relative import from a JavaScript/TypeScript file also depends on the
        // importer's directory (see `module_path_matches`), so that directory joins
        // the memo key for those, and only those.
        let mut resolved: HashMap<ResolvedKey, Vec<(NodeId, EdgeConfidence)>> = HashMap::new();
        let mut memo = ImportMemo::default();
        for (importer_id, target) in import_facts {
            let Some(importer) = self.nodes.get(&importer_id) else {
                continue;
            };
            let importer_repo = importer.repo_id;
            let importer_dir = (target.starts_with('.') && is_script_path(&importer.file_path))
                .then(|| importer.file_path.parent())
                .flatten();
            let targets = resolved
                .entry((target, importer_repo, importer_dir))
                .or_insert_with(|| {
                    self.resolve_import_targets(
                        Some(importer_repo),
                        importer_dir,
                        target.as_str(),
                        &mut memo,
                    )
                });
            for &(to_id, confidence) in targets.iter() {
                if edge_set.insert((importer_id, to_id, EdgeKind::Imports)) {
                    import_edges.push(ContractEdge {
                        from: importer_id,
                        to: to_id,
                        kind: EdgeKind::Imports,
                        metadata: Some(target.clone()),
                        confidence,
                    });
                }
            }
        }
        self.edges.extend(import_edges);

        // 2 & 3. Topic hubs and causal dispatch.
        let mut all_topics: Vec<CompactStr> = self
            .topic_producers
            .keys()
            .chain(self.topic_consumers.keys())
            .cloned()
            .collect();
        all_topics.sort_unstable();
        all_topics.dedup();

        // A synthetic hub node from a *previous* reconcile call whose topic no
        // longer has any producer or consumer fact (the file that used to publish
        // it was edited to use a different topic, or deleted) must not linger
        // forever: a full rebuild from the current facts alone would never create
        // it, so leaving it in place would make an incremental reload permanently
        // diverge from a full rebuild (idempotence invariant I2).
        let live_topics: HashSet<&CompactStr> = all_topics.iter().collect();
        let stale_hubs: HashSet<NodeId> = self
            .nodes
            .values()
            .filter(|n| {
                n.package == "event-bus"
                    && matches!(
                        n.kind,
                        NodeKind::EventStream | NodeKind::KafkaTopic | NodeKind::Queue
                    )
                    && !live_topics.contains(&n.name)
            })
            .map(|n| n.id)
            .collect();
        self.remove_nodes(&stale_hubs);

        // Producers of concrete subjects, in a fixed order, built on the first wildcard
        // subscription only.
        let mut concrete_producers: Option<Vec<(CompactStr, Vec<NodeId>)>> = None;

        for topic_key in all_topics {
            // `add_node` indexes stream-like nodes under their lowercase name, and
            // `topic_key` is already lowercase.
            let existing_topic_id = self.name_to_nodes.get(&topic_key).and_then(|ids| {
                ids.iter().copied().find(|id| {
                    self.nodes.get(id).is_some_and(|n| {
                        n.package == "event-bus" && n.kind == NodeKind::EventStream
                    })
                })
            });

            let topic_id = match existing_topic_id {
                Some(id) => id,
                None => {
                    // Declarative `[[engines.contracts.patterns]]` hits are transport-agnostic
                    // (Kafka, Redis Streams, BullMQ, SQS, ...) — label them EventStream, not
                    // KafkaTopic. Real Kafka usage is still tagged NodeKind::KafkaTopic by the
                    // language-specific extractors that actually detect it (e.g. Spring @KafkaListener).
                    let node = ContractNode {
                        id: 0,
                        name: topic_key.clone(),
                        kind: NodeKind::EventStream,
                        file_path: FilePath::from(Path::new("event-bus")),
                        line_start: 1,
                        line_end: 1,
                        package: CompactStr::new("event-bus"),
                        // This node is a synthetic cross-repo hub, not something
                        // scanned from any configured root — `repo_id: 0` would
                        // silently alias it to whichever repo happens to be
                        // first in `[workspace] roots`, misattributing every
                        // event/topic in the mesh to that one repo. `RepoId::MAX`
                        // is a sentinel no real root index can ever reach.
                        repo_id: RepoId::MAX,
                        signature: Some(CompactStr::new(format!("topic://{topic_key}"))),
                        docstring: None,
                    };
                    self.add_node(node)
                }
            };

            let producers = self
                .topic_producers
                .get(&topic_key)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let consumers = self
                .topic_consumers
                .get(&topic_key)
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            for &prod_id in producers {
                if edge_set.insert((prod_id, topic_id, EdgeKind::Produces)) {
                    self.edges.push(ContractEdge {
                        from: prod_id,
                        to: topic_id,
                        kind: EdgeKind::Produces,
                        metadata: Some(topic_key.clone()),
                        // Structural: derived directly from producer registration,
                        // unless the registering source is itself heuristic.
                        confidence: self.topic_confidence(prod_id),
                    });
                }
            }

            for &cons_id in consumers {
                if edge_set.insert((topic_id, cons_id, EdgeKind::Consumes)) {
                    self.edges.push(ContractEdge {
                        from: topic_id,
                        to: cons_id,
                        kind: EdgeKind::Consumes,
                        metadata: Some(topic_key.clone()),
                        // Structural: derived directly from consumer registration,
                        // unless the registering source is itself heuristic.
                        confidence: self.topic_confidence(cons_id),
                    });
                }
            }

            // A wildcard subscription (`orders.*`, `sensors/+/temp`) also hears the producers
            // of every concrete subject it matches: heuristic, since the match is by pattern.
            // Direct edges come first so an exact edge is never shadowed by a heuristic one.
            // Kafka topics and queues have no subject wildcards: a pattern read only by such
            // consumers (`orders.*` as a Kafka regex or a Redis glob) is a literal key.
            let subject_consumer = consumers.iter().any(|id| {
                self.nodes
                    .get(id)
                    .is_none_or(|n| !matches!(n.kind, NodeKind::KafkaTopic | NodeKind::Queue))
            });
            if is_subject_pattern(&topic_key) && subject_consumer {
                let concrete = concrete_producers.get_or_insert_with(|| {
                    let mut v: Vec<(CompactStr, Vec<NodeId>)> = self
                        .topic_producers
                        .iter()
                        .filter(|(k, _)| !is_subject_pattern(k))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                    v
                });
                let mut linked = 0usize;
                'subjects: for (subject, prods) in concrete.iter() {
                    if !subject_matches(&topic_key, subject) {
                        continue;
                    }
                    for &prod_id in prods {
                        // A relay that consumes the pattern is not its own cause.
                        if consumers.contains(&prod_id) {
                            continue;
                        }
                        if linked >= MAX_WILDCARD_PRODUCERS {
                            tracing::debug!(
                                pattern = %topic_key,
                                cap = MAX_WILDCARD_PRODUCERS,
                                "wildcard subscription truncated"
                            );
                            break 'subjects;
                        }
                        if edge_set.insert((prod_id, topic_id, EdgeKind::Produces)) {
                            linked += 1;
                            self.edges.push(ContractEdge {
                                from: prod_id,
                                to: topic_id,
                                kind: EdgeKind::Produces,
                                metadata: Some(subject.clone()),
                                confidence: EdgeConfidence::Heuristic,
                            });
                        }
                    }
                }
            }

            // Direct `Produces` and `Consumes` edges carry topic routing via a
            // two-hop producer -> topic -> consumer walk with linear O(producers + consumers)
            // edge complexity, rather than quadratic O(producers * consumers) edge growth.
        }

        // 4. Reconcile gRPC Proto Definitions <-> Service Handlers (Implements)
        let proto_methods: Vec<(NodeId, CompactStr)> = self
            .nodes
            .values()
            .filter(|n| n.kind == NodeKind::GrpcMethod && Self::is_proto_file(&n.file_path))
            .map(|n| (n.id, n.name.clone()))
            .collect();

        // Only nodes the language extractors actually tagged as a gRPC handler
        // (e.g. TS `@GrpcMethod`/`@GrpcService`) may implement an RPC. Without
        // this, any same-named REST handler, factory method, or unrelated
        // function matches by bare name alone. Collected once, not per proto method.
        struct Handler<'a> {
            id: NodeId,
            name: &'a str,
            pascal_name: String,
            signature: Option<&'a str>,
        }
        // A handler declared with an enum member / constant instead of literals
        // (`@GrpcMethod(USER_SERVICE_NAME, USER_COMMAND_METHODS.ADD_BADGE_TO_USER)`)
        // is matched by the literal those resolve to, when it names exactly one
        // declared proto RPC; unresolved or ambiguous, it keeps its raw name.
        let resolved_handlers = self.resolve_handler_rpc_names(&proto_methods);
        let handlers: Vec<Handler<'_>> = self
            .nodes
            .values()
            .filter(|n| {
                (n.kind == NodeKind::GrpcMethod || n.kind == NodeKind::GrpcService)
                    && !Self::is_proto_file(&n.file_path)
                    && !n.name.starts_with("rpc:")
                    && !n.name.starts_with("produce:")
                    && !n.name.starts_with("consume:")
            })
            .map(|n| {
                let name = resolved_handlers
                    .get(&n.id)
                    .map_or(n.name.as_str(), CompactStr::as_str);
                Handler {
                    id: n.id,
                    name,
                    pascal_name: crate::types::to_pascal_case(name),
                    signature: n.signature.as_deref(),
                }
            })
            .collect();

        // Handlers indexed by every key the match predicate below can succeed on,
        // so each proto method looks up its few candidates instead of testing
        // every handler (was O(proto methods × handlers)). Candidates are then
        // re-checked with the exact original predicate, so the edge set is
        // unchanged — the index only prunes pairs that could never match.
        let handler_index = HandlerIndex::build(
            handlers
                .iter()
                .map(|h| (h.name, h.pascal_name.as_str(), h.signature)),
        );

        let mut new_edges = Vec::new();
        let mut candidates: Vec<usize> = Vec::new();
        for (proto_id, method_fqcn) in &proto_methods {
            let bare = method_fqcn
                .split('.')
                .next_back()
                .unwrap_or(method_fqcn.as_str());

            candidates.clear();
            handler_index.candidates(method_fqcn.as_str(), bare, &mut candidates);
            for &i in &candidates {
                let h = &handlers[i];
                // Only an exact match against the full FQCN is trustworthy;
                // case-folding, PascalCase normalization, and substring
                // signature scraping are all bare-name heuristics that can
                // collide across unrelated same-named symbols.
                let confidence = if h.name == method_fqcn.as_str() {
                    Some(EdgeConfidence::Exact)
                } else if h.name.eq_ignore_ascii_case(bare)
                    || h.pascal_name == bare
                    || Self::bare_names_match(h.name, bare)
                    || h.signature
                        .is_some_and(|s| Self::signature_contains_bare(s, bare))
                {
                    Some(EdgeConfidence::Heuristic)
                } else {
                    None
                };

                let Some(confidence) = confidence else {
                    continue;
                };
                if edge_set.insert((h.id, *proto_id, EdgeKind::Implements)) {
                    new_edges.push(ContractEdge {
                        from: h.id,
                        to: *proto_id,
                        kind: EdgeKind::Implements,
                        metadata: Some(method_fqcn.clone()),
                        confidence,
                    });
                }
            }
        }
        self.edges.extend(new_edges);
        self.handler_rpc_names = resolved_handlers;

        // 5. Reconcile RPC Client Calls (CallsRpc) — proto methods indexed by
        // lowercase full and bare name; first declaration wins, as before.
        //
        // Also indexed here: every declared `GrpcService` node, proto-anchored
        // or not. A client-side call site (e.g. Go's `pb.NewFooServiceClient(conn)`
        // construction) records the *service* it talks to, not a specific proto
        // method FQCN — there is no proto method to name at that call site, only
        // the service. Matching directly against the service's own name (its
        // .proto declaration when indexed, or a language extractor's own
        // service-implementation node — e.g. Go's `RegisterFooServiceServer`-
        // detected node — when it isn't) is what lets `find_dependents`/
        // `analyze_grpc` resolve a real cross-service caller even when the
        // repo's `.proto` sources aren't in `roots` at all.
        // Both indexed by every matching candidate (not just the first found) —
        // a bare service name like "AdminService" is exactly the shape two
        // unrelated proto packages can legitimately both declare, and picking
        // only the first one `self.nodes.values()` (or file processing order)
        // happened to insert used to be a silent, order-dependent wrong answer
        // (idempotence invariant I1). Real ties are surfaced via
        // `pick_or_ambiguous_by_package` below instead.
        let mut proto_by_fqcn: HashMap<String, Vec<NodeId>> =
            HashMap::with_capacity(proto_methods.len() * 2);
        let mut proto_by_bare: HashMap<String, Vec<NodeId>> =
            HashMap::with_capacity(proto_methods.len());

        for (id, name) in &proto_methods {
            proto_by_fqcn
                .entry(name.as_str().to_lowercase())
                .or_default()
                .push(*id);
            if let Some(bare) = name.split('.').next_back() {
                proto_by_bare
                    .entry(bare.to_lowercase())
                    .or_default()
                    .push(*id);
            }
        }
        for node in self.nodes.values() {
            if node.kind == NodeKind::GrpcService {
                proto_by_fqcn
                    .entry(node.name.as_str().to_lowercase())
                    .or_default()
                    .push(node.id);
            }
        }

        // Each candidate list grouped by declared package, once: the per-call
        // package filter `pick_or_ambiguous_by_package` did was O(candidates) per
        // call — O(calls × homonyms) when N services each declare their own
        // `Get` and each one's client calls it.
        let proto_by_fqcn = self.group_by_package(proto_by_fqcn);
        let proto_by_bare = self.group_by_package(proto_by_bare);

        // Client fields each class binds (`#field=Service` records), keyed by
        // (class name, field), so a subclass in another file resolves
        // `this.<inherited field>.<method>()`. 7.0.6 missed all 75 Volontariapp
        // gateway calls made through `BaseEventGrpcController`'s fields.
        let mut class_fields: HashMap<(&str, &str), Vec<(RepoId, &str)>> = HashMap::new();
        for (class_id, recorded) in &self.rpc_calls {
            let Some(binding) = recorded.strip_prefix(CLIENT_FIELD_BINDING_PREFIX) else {
                continue;
            };
            let (Some(class), Some((field, service))) =
                (self.nodes.get(class_id), binding.split_once('='))
            else {
                continue;
            };
            class_fields
                .entry((class.name.as_str(), field))
                .or_default()
                .push((class.repo_id, service));
        }

        let handler_methods = self.handler_served_methods();
        let mut rpc_edges = Vec::new();
        for (caller_id, recorded) in &self.rpc_calls {
            if recorded.starts_with([
                CLIENT_FIELD_BINDING_PREFIX,
                CONST_VALUE_PREFIX,
                HANDLER_REF_PREFIX,
            ]) {
                continue;
            }
            // Method-level call sites (`this.users.signUp(…)`) are recorded with
            // a prefix: `=` resolves by exact `Service.Method` only — never by
            // bare method name, which would link `this.users.findAll()` to any
            // other service's `FindAll` — and `~` does the same with heuristic
            // confidence (the service was inferred from the field's name).
            // `^` (inherited field) becomes one of the two once the base
            // class's binding is looked up.
            let inherited: String;
            let (target_str, method_level, inferred) =
                if let Some(t) = recorded.strip_prefix(RPC_EXACT_METHOD_PREFIX) {
                    (t, true, false)
                } else if let Some(t) = recorded.strip_prefix(RPC_INFERRED_METHOD_PREFIX) {
                    (t, true, true)
                } else if let Some(t) = recorded.strip_prefix(RPC_INHERITED_METHOD_PREFIX) {
                    let caller_repo = self.nodes.get(caller_id).map(|n| n.repo_id);
                    let Some((resolved, inferred)) =
                        Self::resolve_inherited_call(t, caller_repo, &class_fields)
                    else {
                        continue;
                    };
                    inherited = resolved;
                    (inherited.as_str(), true, inferred)
                } else {
                    (recorded.as_str(), false, false)
                };
            let target_rpc = CompactStr::new(target_str);
            // The standard gRPC health-checking protocol is infrastructure every
            // server exposes, not a service-to-service dependency: an edge to
            // it (a readiness probe in a test, a `NewHealthClient` in a
            // sidecar) is noise in every blast-radius answer (Plan 4 step 4.5:
            // otel-demo's spurious `checkout -> health`).
            if Self::is_grpc_health_fqcn(target_str) {
                continue;
            }
            // A name-based guess made inside the handler of the RPC it names is
            // delegation to an application service, not a client call.
            if inferred && method_level {
                let (called_service, called) =
                    target_str.rsplit_once('.').unwrap_or(("", target_str));
                if handler_methods
                    .get(caller_id)
                    .is_some_and(|(service, method)| {
                        *method == fold_rpc_name(called)
                            && service.as_deref().is_some_and(|s| {
                                s.eq_ignore_ascii_case(
                                    called_service.rsplit('.').next().unwrap_or(called_service),
                                )
                            })
                    })
                {
                    continue;
                }
            }
            let caller_package = self.nodes.get(caller_id).map(|n| n.package.clone());
            if method_level {
                let Some(candidates) = proto_by_fqcn.get(&target_str.to_lowercase()) else {
                    continue;
                };
                let confidence = if inferred {
                    EdgeConfidence::Heuristic
                } else {
                    EdgeConfidence::Exact
                };
                let picked = match caller_package.as_ref() {
                    Some(pkg) => candidates.pick_or_ambiguous(pkg, confidence),
                    None => candidates.ids.iter().map(|&id| (id, confidence)).collect(),
                };
                for (target_id, confidence) in picked {
                    let confidence = if inferred && confidence == EdgeConfidence::Exact {
                        EdgeConfidence::Heuristic
                    } else {
                        confidence
                    };
                    if edge_set.insert((*caller_id, target_id, EdgeKind::CallsRpc)) {
                        rpc_edges.push(ContractEdge {
                            from: *caller_id,
                            to: target_id,
                            kind: EdgeKind::CallsRpc,
                            metadata: Some(target_rpc.clone()),
                            confidence,
                        });
                    }
                }
                continue;
            }
            let mut matches = Self::resolve_rpc_target(
                target_str,
                caller_package.as_ref(),
                &proto_by_fqcn,
                &proto_by_bare,
            );
            // A generated client names its service either way: ts-proto emits
            // `AdServiceClient` for `service AdService` but `AdClient` for
            // `service Ad`, and a caller may well spell the latter's target as
            // `Ad` while the graph declares `AdService`. One direction only
            // (`<X>` -> `<X>Service`). Tried only when the literal target
            // resolves to nothing, and only against services/methods actually
            // declared in the graph — never a way to invent an edge to an
            // undeclared name. Never for `Health`: that is the stock health
            // client (`NewHealthClient`, `HealthClient`), which a repo's own
            // `service HealthService` is not.
            if matches.is_empty() {
                let bare = target_str.split('.').next_back().unwrap_or(target_str);
                if !bare.is_empty()
                    && !bare.to_ascii_lowercase().ends_with("service")
                    && !bare.eq_ignore_ascii_case("health")
                {
                    let with_suffix = format!("{target_str}Service");
                    matches = Self::resolve_rpc_target(
                        &with_suffix,
                        caller_package.as_ref(),
                        &proto_by_fqcn,
                        &proto_by_bare,
                    );
                }
            }
            matches.retain(|(id, _)| {
                self.nodes
                    .get(id)
                    .is_none_or(|n| !Self::is_grpc_health_service(n))
            });

            for (target_id, confidence) in matches {
                if edge_set.insert((*caller_id, target_id, EdgeKind::CallsRpc)) {
                    rpc_edges.push(ContractEdge {
                        from: *caller_id,
                        to: target_id,
                        kind: EdgeKind::CallsRpc,
                        metadata: Some(target_rpc.clone()),
                        confidence,
                    });
                }
            }
        }
        self.edges.extend(rpc_edges);
    }
}
