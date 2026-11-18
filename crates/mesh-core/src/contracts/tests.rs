use super::resolve::HandlerIndex;
use super::*;

#[test]
fn subject_wildcards_match_by_token() {
    assert!(subject_matches("orders.*", "orders.created"));
    assert!(!subject_matches("orders.*", "orders.created.eu"));
    assert!(subject_matches("orders.>", "orders.created.eu"));
    assert!(!subject_matches("orders.>", "orders"));
    assert!(subject_matches("sensors/#", "sensors"));
    assert!(!subject_matches(">", ""));
    assert!(!subject_matches("orders.>", "orders."));
    // Dialects do not mix, and a wildcard only ends a pattern.
    assert!(!is_subject_pattern("cache.+") && !is_subject_pattern("src/*/x"));
    assert!(!is_subject_pattern("a.>.b"));
    assert!(subject_matches("sensors/+/temp", "sensors/a/temp"));
    assert!(!subject_matches("sensors/+/temp", "sensors/a/b/temp"));
    assert!(subject_matches("sensors/#", "sensors/a/b"));
    assert!(!subject_matches("orders.*", "billing.created"));
    assert!(is_subject_pattern("a.*.c") && !is_subject_pattern("a.b.c"));
}

#[test]
fn a_wildcard_consumer_hears_matching_producers() {
    let mut g = ContractGraph::new();
    let p = matrix_node(&mut g, "pub", NodeKind::ServiceClass, "a/p.go", "", 0);
    let c = matrix_node(&mut g, "sub", NodeKind::ServiceClass, "b/c.go", "", 1);
    g.add_producer(p, "orders.created");
    g.add_consumer(c, "orders.*");
    g.reconcile_edges();
    let hub = g
        .all_edges()
        .iter()
        .find(|e| e.kind == EdgeKind::Consumes && e.to == c)
        .map(|e| e.from)
        .expect("consume edge");
    assert!(g
        .all_edges()
        .iter()
        .any(|e| e.kind == EdgeKind::Produces && e.from == p && e.to == hub));
}

#[test]
fn a_relay_is_not_linked_to_its_own_wildcard_subscription() {
    let mut g = ContractGraph::new();
    let relay = matrix_node(&mut g, "relay", NodeKind::ServiceClass, "a/r.go", "", 0);
    g.add_producer(relay, "orders.created");
    g.add_consumer(relay, "orders.*");
    g.reconcile_edges();
    assert!(!g.all_edges().iter().any(|e| e.kind == EdgeKind::Produces
        && e.from == relay
        && e.confidence == EdgeConfidence::Heuristic));
}

#[test]
fn wildcard_edges_follow_the_producers_across_reconciles() {
    let mut g = ContractGraph::new();
    let p = matrix_node(&mut g, "pub", NodeKind::ServiceClass, "a/p.go", "", 0);
    let c = matrix_node(&mut g, "sub", NodeKind::ServiceClass, "b/c.go", "", 1);
    g.add_producer(p, "orders.created");
    g.add_consumer(c, "orders.*");
    g.reconcile_edges();
    g.reconcile_edges();
    let heuristic = |g: &ContractGraph| {
        g.all_edges()
            .iter()
            .filter(|e| e.confidence == EdgeConfidence::Heuristic)
            .count()
    };
    assert_eq!(heuristic(&g), 1, "idempotent");
    g.topic_producers.clear();
    g.reconcile_edges();
    assert_eq!(heuristic(&g), 0, "no stale wildcard edge");
}

#[test]
fn a_wildcard_read_only_by_kafka_or_queue_consumers_stays_literal() {
    let mut g = ContractGraph::new();
    let p = matrix_node(&mut g, "pub", NodeKind::ServiceClass, "a/p.go", "", 0);
    let k = matrix_node(&mut g, "kafka-sub", NodeKind::KafkaTopic, "b/c.ts", "", 1);
    g.add_producer(p, "orders.created");
    g.add_consumer(k, "orders.*");
    g.reconcile_edges();
    assert!(!g
        .all_edges()
        .iter()
        .any(|e| e.confidence == EdgeConfidence::Heuristic));
}

#[test]
fn normalize_topic_key_reduces_member_references_only() {
    for (raw, key) in [
        ("Streams.EVENT_CREATED", "event_created"),
        ("EventEventMessagingType.EVENT_CREATED", "event_created"),
        ("typeof EventMessagingType.EVENT_CREATED", "event_created"),
        ("'Topics.ORDER_PLACED_V2'", "order_placed_v2"),
        ("orders.created", "orders.created"),
        ("Order.Created", "order.created"),
        ("event.created", "event.created"),
        ("EVENT_CREATED", "event_created"),
        ("config.topics.ORDERS", "config.topics.orders"),
    ] {
        assert_eq!(normalize_topic_key(raw), key, "{raw}");
    }
}
use std::path::PathBuf;

/// `HandlerIndex` only prunes: every (handler, proto method) pair the
/// original all-pairs predicate accepts must be among its candidates.
#[test]
fn handler_index_never_drops_a_predicate_match() {
    let names = [
        "GetUser",
        "getUser",
        "get_user",
        "GET_USER",
        "svc.GetUser",
        "a.b.get_user",
        "GetUserById",
        "ListUsers",
        "",
        "x.",
        "Ünïcode",
        "_",
    ];
    let sigs = [
        None,
        Some("fn get_user(&self)"),
        Some("@GrpcMethod('UserService', 'GetUser')"),
        Some("def getUserById(self): pass"),
        Some("func (s *S) GetUser(ctx) error"),
        Some("\"Get-User\" handler"),
        Some("@Ünïcode"),
    ];
    let bares = [
        ("pkg.UserService.GetUser", "GetUser"),
        ("GetUser", "GetUser"),
        ("x.getuser", "getuser"),
        ("x.GetUserBy", "GetUserBy"),
        ("x.Get-User", "Get-User"),
        ("x.", ""),
        ("x.Ünïcode", "Ünïcode"),
        ("x.ListUsers", "ListUsers"),
    ];
    let handlers: Vec<(String, String, Option<&str>)> = names
        .iter()
        .flat_map(|n| {
            sigs.iter()
                .map(move |s| (n.to_string(), crate::types::to_pascal_case(n), *s))
        })
        .collect();
    let index = HandlerIndex::build(
        handlers
            .iter()
            .map(|(n, p, s)| (n.as_str(), p.as_str(), *s)),
    );
    let mut cands = Vec::new();
    for (fqcn, bare) in bares {
        cands.clear();
        index.candidates(fqcn, bare, &mut cands);
        for (i, (name, pascal, sig)) in handlers.iter().enumerate() {
            let matches = name == fqcn
                || name.eq_ignore_ascii_case(bare)
                || pascal == bare
                || ContractGraph::bare_names_match(name, bare)
                || sig.is_some_and(|s| ContractGraph::signature_contains_bare(s, bare));
            if matches {
                assert!(
                    cands.contains(&i),
                    "index dropped handler {name:?}/{sig:?} for {fqcn:?}"
                );
            }
        }
    }
}

/// Randomized companion to `handler_index_never_drops_a_predicate_match`:
/// names, signatures and proto FQCNs assembled from the fragments the
/// predicate is sensitive to (case, `_`, `.`, non-ASCII, signature
/// prefixes, non-identifier characters, empty pieces).
#[test]
fn handler_index_never_drops_a_predicate_match_fuzzed() {
    const PIECES: [&str; 16] = [
        "get", "Get", "GET", "_", ".", "user", "User", "é", "É", "-", "@", "'", "fn ", "def ",
        "x1", "",
    ];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut word = |max: u64| -> String {
        let len = next() % max;
        (0..len)
            .map(|_| PIECES[(next() % PIECES.len() as u64) as usize])
            .collect()
    };
    let handlers: Vec<(String, String, Option<String>)> = (0..600)
        .map(|i| {
            let name = word(5);
            let pascal = crate::types::to_pascal_case(&name);
            let sig = (i % 3 != 0).then(|| word(8));
            (name, pascal, sig)
        })
        .collect();
    let fqcns: Vec<String> = (0..400).map(|_| word(6)).collect();
    let index = HandlerIndex::build(
        handlers
            .iter()
            .map(|(n, p, s)| (n.as_str(), p.as_str(), s.as_deref())),
    );
    let mut cands = Vec::new();
    for fqcn in &fqcns {
        let bare = fqcn.split('.').next_back().unwrap_or(fqcn.as_str());
        cands.clear();
        index.candidates(fqcn, bare, &mut cands);
        for (i, (name, pascal, sig)) in handlers.iter().enumerate() {
            let matches = name == fqcn
                || name.eq_ignore_ascii_case(bare)
                || pascal == bare
                || ContractGraph::bare_names_match(name, bare)
                || sig
                    .as_deref()
                    .is_some_and(|s| ContractGraph::signature_contains_bare(s, bare));
            if matches {
                assert!(
                    cands.binary_search(&i).is_ok(),
                    "index dropped handler {name:?}/{sig:?} for {fqcn:?}"
                );
            }
        }
    }
}

fn memo_node(name: &str, pkg: &str, file: &str, repo: RepoId, kind: NodeKind) -> ContractNode {
    ContractNode {
        id: 0,
        name: CompactStr::new(name),
        kind,
        file_path: FilePath::from(Path::new(file)),
        line_start: 1,
        line_end: 1,
        package: CompactStr::new(pkg),
        repo_id: repo,
        signature: None,
        docstring: None,
    }
}

fn edges_from(g: &ContractGraph, from: NodeId, kind: EdgeKind) -> Vec<(NodeId, EdgeConfidence)> {
    let mut out: Vec<_> = g
        .all_edges()
        .iter()
        .filter(|e| e.from == from && e.kind == kind)
        .map(|e| (e.to, e.confidence))
        .collect();
    out.sort_by_key(|(id, _)| *id);
    out
}

/// Volontariapp 7.0.6 report, defect 7: `import * as path from 'path'`
/// resolved to a `const path = require(…)` in another repo. A built-in
/// module name resolves within the importer's repo only.
#[test]
fn node_builtin_modules_never_resolve_across_repos() {
    let mut g = ContractGraph::new();
    g.add_node(memo_node(
        "path",
        "",
        "app/scripts/setup-env.js",
        1,
        NodeKind::ServiceClass,
    ));
    let local_events = g.add_node(memo_node(
        "events",
        "",
        "svc/src/events.ts",
        0,
        NodeKind::ServiceClass,
    ));
    let importer = g.add_node(memo_node(
        "main",
        "",
        "svc/src/main.ts",
        0,
        NodeKind::ServiceClass,
    ));
    for m in ["path", "node:fs", "fs/promises", "events"] {
        g.add_dependency(importer, m);
    }
    g.reconcile_edges();
    let targets: Vec<NodeId> = g
        .all_edges()
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports && e.from == importer)
        .map(|e| e.to)
        .collect();
    assert_eq!(targets, vec![local_events], "{:?}", g.all_edges());
}

/// The per-bucket memos in `reconcile_edges` (import strategies 1 and 3,
/// RPC package grouping) answer exactly what the per-call scans did: first
/// node of the package in bucket order, same-repo stem matches, and the
/// unique / same-package-tie / no-match tiers of the package tiebreak.
#[test]
fn reconcile_memos_keep_per_call_semantics() {
    use NodeKind::{GrpcMethod, ServiceClass};
    let mut g = ContractGraph::new();
    // Strategy 1: two `p.C` in bucket order, plus `q.C`.
    let pc1 = g.add_node(memo_node("C", "p", "p/C1.java", 0, ServiceClass));
    let pc2 = g.add_node(memo_node("C", "p", "p/C2.java", 0, ServiceClass));
    let qc = g.add_node(memo_node("C", "q", "q/C.java", 1, ServiceClass));
    let imp_p = g.add_node(memo_node("UseP", "x", "x/UseP.java", 0, ServiceClass));
    let imp_q = g.add_node(memo_node("UseQ", "x", "x/UseQ.java", 1, ServiceClass));
    g.add_dependency(imp_p, "p.C");
    g.add_dependency(imp_q, "q.C");
    // Strategy 3: an `index` in each repo, reached by distinct relative paths.
    let idx0 = g.add_node(memo_node("index", "", "r0/index.ts", 0, ServiceClass));
    let idx1 = g.add_node(memo_node("index", "", "r1/index.ts", 1, ServiceClass));
    let rel0 = g.add_node(memo_node("a", "", "r0/a.ts", 0, ServiceClass));
    let rel0b = g.add_node(memo_node("b", "", "r0/b.ts", 0, ServiceClass));
    let rel1 = g.add_node(memo_node("c", "", "r1/c.ts", 1, ServiceClass));
    g.add_dependency(rel0, "./index");
    g.add_dependency(rel0b, "../r0/index");
    g.add_dependency(rel1, "./index");
    // RPC: bare `Get` declared twice in package a, once in b.
    let a1 = g.add_node(memo_node("a.S1.Get", "a", "a1.proto", 0, GrpcMethod));
    let a2 = g.add_node(memo_node("a.S2.Get", "a", "a2.proto", 0, GrpcMethod));
    let b1 = g.add_node(memo_node("b.S.Get", "b", "b.proto", 0, GrpcMethod));
    let call_a = g.add_node(memo_node("ca", "a", "ca.go", 0, ServiceClass));
    let call_b = g.add_node(memo_node("cb", "b", "cb.go", 0, ServiceClass));
    let call_c = g.add_node(memo_node("cc", "c", "cc.go", 0, ServiceClass));
    let call_fq = g.add_node(memo_node("cf", "z", "cf.go", 0, ServiceClass));
    for c in [call_a, call_b, call_c] {
        g.add_rpc_call(c, "Get");
    }
    g.add_rpc_call(call_fq, "b.S.Get");
    g.reconcile_edges();

    use EdgeConfidence::{Ambiguous, Exact, Heuristic};
    assert_eq!(edges_from(&g, imp_p, EdgeKind::Imports), [(pc1, Exact)]);
    assert_eq!(edges_from(&g, imp_q, EdgeKind::Imports), [(qc, Exact)]);
    assert_ne!(pc1, pc2);
    assert_eq!(edges_from(&g, rel0, EdgeKind::Imports), [(idx0, Heuristic)]);
    assert_eq!(
        edges_from(&g, rel0b, EdgeKind::Imports),
        [(idx0, Heuristic)]
    );
    assert_eq!(edges_from(&g, rel1, EdgeKind::Imports), [(idx1, Heuristic)]);
    assert_eq!(
        edges_from(&g, call_a, EdgeKind::CallsRpc),
        [(a1, Ambiguous), (a2, Ambiguous)]
    );
    assert_eq!(
        edges_from(&g, call_b, EdgeKind::CallsRpc),
        [(b1, Heuristic)]
    );
    assert_eq!(
        edges_from(&g, call_c, EdgeKind::CallsRpc),
        [(a1, Ambiguous), (a2, Ambiguous), (b1, Ambiguous)]
    );
    assert_eq!(edges_from(&g, call_fq, EdgeKind::CallsRpc), [(b1, Exact)]);
}

/// Stale synthetic topic hubs are removed from every index in one batch,
/// including the `event-bus` file entry they all share.
#[test]
fn stale_topic_hubs_leave_no_index_entries() {
    let mut g = ContractGraph::new();
    let mut files = Vec::new();
    for i in 0..50 {
        let file = format!("t{i}.ts");
        let id = g.add_node(memo_node("p", "", &file, 0, NodeKind::ServiceClass));
        g.add_producer(id, &format!("topic{i}"));
        files.push(PathBuf::from(file));
    }
    g.reconcile_edges();
    assert_eq!(g.get_nodes_for_file(Path::new("event-bus")).len(), 50);
    g.patch_files(files.iter().map(PathBuf::as_path));
    g.reconcile_edges();
    assert_eq!(g.node_count(), 0);
    assert!(g.get_nodes_for_file(Path::new("event-bus")).is_empty());
    assert!(g.search_symbols("topic7", None).is_empty());
}

/// Scaling guard for the paths above (hot-name imports, RPC homonyms, mass
/// hub removal): each was O(N²) — 9–11 s at N = 16k — and is now ~linear.
/// Timing-based, so opt-in: `cargo test --release -p mesh-core -- --ignored`.
#[test]
#[ignore]
fn reconcile_scales_linearly_on_hot_buckets() {
    let n = 16_000usize;
    let mut g = ContractGraph::new();
    for i in 0..n {
        let id = g.add_node(memo_node(
            "Constants",
            &format!("p{i}"),
            &format!("p{i}/C.java"),
            0,
            NodeKind::ServiceClass,
        ));
        g.add_dependency(id, &format!("p{i}.Constants"));
        g.add_node(memo_node(
            &format!("s{i}.Svc.Get"),
            &format!("s{i}"),
            &format!("s{i}.proto"),
            0,
            NodeKind::GrpcMethod,
        ));
        let caller = g.add_node(memo_node(
            "call",
            &format!("s{i}"),
            &format!("c{i}.go"),
            0,
            NodeKind::ServiceClass,
        ));
        g.add_rpc_call(caller, "Get");
        g.add_producer(caller, &format!("topic{i}"));
    }
    g.reconcile_edges();
    let files: Vec<PathBuf> = (0..n).map(|i| PathBuf::from(format!("c{i}.go"))).collect();
    g.patch_files(files.iter().map(PathBuf::as_path));
    let t = std::time::Instant::now();
    g.reconcile_edges();
    let elapsed = t.elapsed();
    assert!(elapsed.as_secs_f64() < 2.0, "reconcile took {elapsed:?}");
}

#[test]
fn test_reverse_dependency_graph() {
    let mut graph = ContractGraph::new();
    let consumer_node = ContractNode {
        id: 0,
        name: CompactStr::new("AuthConsumer"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/auth/AuthConsumer.java").into(),
        line_start: 10,
        line_end: 50,
        package: CompactStr::new("com.mesh.auth"),
        repo_id: 1,
        signature: None,
        docstring: None,
    };
    let cid = graph.add_node(consumer_node);
    graph.add_dependency(cid, "UserAuthRequest");

    let dependents = graph.find_dependents("UserAuthRequest");
    assert_eq!(dependents.len(), 1);
    assert_eq!(dependents[0].name.as_str(), "AuthConsumer");
}

#[test]
fn test_analyze_grpc_flow() {
    let mut graph = ContractGraph::new();
    let proto_node = ContractNode {
        id: 0,
        name: CompactStr::new("AuthenticateUser"),
        kind: NodeKind::GrpcMethod,
        file_path: Path::new("proto-registry/auth.proto").into(),
        line_start: 20,
        line_end: 25,
        package: CompactStr::new("auth.v1"),
        repo_id: 0,
        signature: Some(CompactStr::new(
            "rpc AuthenticateUser (AuthRequest) returns (AuthResponse);",
        )),
        docstring: None,
    };
    graph.add_node(proto_node);

    let trace = graph.analyze_grpc("AuthenticateUser");
    assert!(trace.proto_definition.is_some());
    assert_eq!(
        trace.proto_definition.unwrap().name.as_str(),
        "AuthenticateUser"
    );
}

/// A node that only matches the target via a substring hit inside its own
/// name (with no exact match and no real Implements edge to a proto definition)
/// must not be bucketed into server_handlers.
#[test]
fn analyze_grpc_does_not_bucket_heuristic_name_matches_by_path_alone() {
    let mut graph = ContractGraph::new();
    let vendored_stub = ContractNode {
        id: 0,
        name: CompactStr::new("CheckoutServiceServicer"),
        kind: NodeKind::GrpcService,
        file_path: Path::new("src/emailservice/demo_pb2_grpc.py").into(),
        line_start: 709,
        line_end: 715,
        package: CompactStr::new("emailservice"),
        repo_id: 2,
        signature: None,
        docstring: None,
    };
    graph.add_node(vendored_stub);

    let trace = graph.analyze_grpc("CheckoutService");
    assert!(
        trace
            .server_handlers
            .iter()
            .all(|(h, _)| h.name.as_str() != "CheckoutServiceServicer"),
        "a heuristic substring-only name match with no exact match and \
             no Implements edge must not appear as a server handler, got: {:?}",
        trace.server_handlers
    );
}

/// An exact-name-matched GrpcService node is bucketed as a server
/// handler regardless of file path naming.
#[test]
fn analyze_grpc_buckets_exact_match_as_server_handler_regardless_of_path_naming() {
    let mut graph = ContractGraph::new();
    let service_node = ContractNode {
        id: 0,
        name: CompactStr::new("CheckoutService"),
        kind: NodeKind::GrpcService,
        // Deliberately no "service"/"handler"/"controller" substring
        // anywhere in this path.
        file_path: Path::new("src/checkout/main.go").into(),
        line_start: 142,
        line_end: 142,
        package: CompactStr::new("checkout"),
        repo_id: 0,
        signature: None,
        docstring: None,
    };
    graph.add_node(service_node);

    let trace = graph.analyze_grpc("CheckoutService");
    assert!(
        trace
            .server_handlers
            .iter()
            .any(|(h, _)| h.name.as_str() == "CheckoutService"),
        "an exact-name GrpcService match must be a server handler \
             regardless of directory naming, got server_handlers={:?} \
             client_stubs={:?}",
        trace.server_handlers,
        trace.client_stubs
    );
    assert!(
        trace
            .client_stubs
            .iter()
            .all(|(c, _)| c.name.as_str() != "CheckoutService"),
        "must not also appear in client_stubs"
    );
}

/// Regression test: a synthetic node created by the generic
/// custom-pattern engine's `PatternKind::Rpc` (name prefixed `"rpc:"`)
/// explicitly represents a client call site, not a declaration — it must
/// still route to client_stubs, not server_handlers, even under an
/// exact-name match.
#[test]
fn analyze_grpc_routes_custom_pattern_rpc_node_to_client_stubs() {
    let mut graph = ContractGraph::new();
    let rpc_call_node = ContractNode {
        id: 0,
        name: CompactStr::new("rpc:CheckoutService"),
        kind: NodeKind::GrpcMethod,
        file_path: Path::new("src/some-service/handler.ts").into(),
        line_start: 10,
        line_end: 12,
        package: CompactStr::new("some-service"),
        repo_id: 0,
        signature: Some(CompactStr::new("RPC call: CheckoutService")),
        docstring: None,
    };
    graph.add_node(rpc_call_node);

    let trace = graph.analyze_grpc("rpc:CheckoutService");
    assert!(
        trace
            .client_stubs
            .iter()
            .any(|(c, _)| c.name.as_str() == "rpc:CheckoutService"),
        "a custom-pattern RPC call-site node must be a client stub, got: {:?}",
        trace
    );
    assert!(trace
        .server_handlers
        .iter()
        .all(|(h, _)| h.name.as_str() != "rpc:CheckoutService"));
}

/// Regression test: `analyze_grpc`'s client/server edge resolution must
/// not require a `.proto` file to be indexed at all — a language's own
/// exact-name service node (e.g. Go's `RegisterFooServiceServer`-detected
/// node) is a perfectly good anchor for a real `CallsRpc` edge.
#[test]
fn analyze_grpc_resolves_client_stubs_via_edge_without_a_proto_file() {
    let mut graph = ContractGraph::new();

    let service_node = ContractNode {
        id: 0,
        name: CompactStr::new("CheckoutService"),
        kind: NodeKind::GrpcService,
        file_path: Path::new("src/checkoutservice/main.go").into(),
        line_start: 142,
        line_end: 142,
        package: CompactStr::new("checkoutservice"),
        repo_id: 0,
        signature: None,
        docstring: None,
    };
    let service_id = graph.add_node(service_node);

    let caller_node = ContractNode {
        id: 0,
        name: CompactStr::new("placeOrderHandler"),
        kind: NodeKind::HttpEndpoint,
        file_path: Path::new("src/frontend/handlers.go").into(),
        line_start: 320,
        line_end: 401,
        package: CompactStr::new("frontend"),
        repo_id: 1,
        signature: None,
        docstring: None,
    };
    let caller_id = graph.add_node(caller_node);
    graph.add_rpc_call(caller_id, "CheckoutService");
    graph.reconcile_edges();

    let trace = graph.analyze_grpc("CheckoutService");
    assert!(trace.proto_definition.is_none());
    assert_eq!(
        trace
            .server_handlers
            .iter()
            .find(|(h, _)| h.id == service_id)
            .map(|_| ()),
        Some(()),
        "the service node itself must still be its own server handler entry"
    );
    assert!(
        trace
            .client_stubs
            .iter()
            .any(|(c, _)| c.name.as_str() == "placeOrderHandler"),
        "expected the real CallsRpc caller to appear in client_stubs even \
             with no .proto file indexed, got: {:?}",
        trace.client_stubs
    );
}

/// Resolves a cross-service gRPC caller via CallsRpc edge: client construction
/// links caller to GrpcService through reconcile_edges.
#[test]
fn find_dependents_resolves_a_real_cross_service_grpc_caller_via_reconcile_edges() {
    let mut graph = ContractGraph::new();

    let service_node = ContractNode {
        id: 0,
        name: CompactStr::new("CheckoutService"),
        kind: NodeKind::GrpcService,
        file_path: Path::new("src/checkoutservice/main.go").into(),
        line_start: 142,
        line_end: 142,
        package: CompactStr::new("checkoutservice"),
        repo_id: 0,
        signature: None,
        docstring: None,
    };
    graph.add_node(service_node);

    let caller_node = ContractNode {
        id: 0,
        name: CompactStr::new("placeOrderHandler"),
        kind: NodeKind::HttpEndpoint,
        file_path: Path::new("src/frontend/handlers.go").into(),
        line_start: 320,
        line_end: 401,
        package: CompactStr::new("frontend"),
        repo_id: 1,
        signature: None,
        docstring: None,
    };
    let caller_id = graph.add_node(caller_node);

    // The raw signal a language extractor records at a
    // `pb.NewCheckoutServiceClient(conn)` call site — no import-path
    // relationship involved at all.
    graph.add_rpc_call(caller_id, "CheckoutService");
    graph.reconcile_edges();

    let dependents = graph.find_dependents("CheckoutService");
    assert_eq!(
        dependents.len(),
        1,
        "a real CallsRpc-edge caller must resolve even though no file \
             imports anything named 'CheckoutService', got: {dependents:?}"
    );
    assert_eq!(dependents[0].name.as_str(), "placeOrderHandler");
}

/// Regression test for querying `find_dependents` by a declared symbol's
/// own name (a GrpcService, the exact shape the tool's own schema
/// example invites — "Target contract name (ex: 'UserAuthRequest')")
/// which must not silently return nothing just because no other file
/// happens to reference that literal string — `frontend` depends on
/// `checkoutservice`'s *package*, not on the string "CheckoutService".
#[test]
fn find_dependents_resolves_by_declared_symbol_name() {
    let mut graph = ContractGraph::new();
    let service_node = ContractNode {
        id: 0,
        name: CompactStr::new("CheckoutService"),
        kind: NodeKind::GrpcService,
        file_path: Path::new("src/checkoutservice/main.go").into(),
        line_start: 142,
        line_end: 142,
        package: CompactStr::new("checkoutservice"),
        repo_id: 0,
        signature: None,
        docstring: None,
    };
    graph.add_node(service_node);

    let caller_node = ContractNode {
        id: 0,
        name: CompactStr::new("placeOrderHandler"),
        kind: NodeKind::HttpEndpoint,
        file_path: Path::new("src/frontend/handlers.go").into(),
        line_start: 320,
        line_end: 401,
        package: CompactStr::new("frontend"),
        repo_id: 1,
        signature: None,
        docstring: None,
    };
    let caller_id = graph.add_node(caller_node);
    // The caller's recorded dependency is on the *package*, never the
    // literal gRPC service name.
    graph.add_dependency(caller_id, "checkoutservice");

    let dependents = graph.find_dependents("CheckoutService");
    assert_eq!(
        dependents.len(),
        1,
        "querying by the declared symbol name must resolve via its \
             package, not return an empty (false-negative) result"
    );
    assert_eq!(dependents[0].name.as_str(), "placeOrderHandler");
}

/// Regression test for the second half of the same finding: once results
/// span multiple services that happen to share a locally-aliased package
/// name (every service vendoring its own `genproto`), each result still
/// carries enough info (`repo_id`) for a caller to group/disambiguate
/// them, rather than a single flattened, unscoped list.
#[test]
fn find_dependents_substring_fallback_preserves_repo_id_for_grouping() {
    let mut graph = ContractGraph::new();

    let svc_a_caller = ContractNode {
        id: 0,
        name: CompactStr::new("loadCatalog"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("src/productcatalogservice/catalog_loader.go").into(),
        line_start: 33,
        line_end: 42,
        package: CompactStr::new("productcatalogservice"),
        repo_id: 0,
        signature: None,
        docstring: None,
    };
    let a_id = graph.add_node(svc_a_caller);
    graph.add_dependency(a_id, "genproto");

    let svc_b_caller = ContractNode {
        id: 0,
        name: CompactStr::new("placeOrderHandler"),
        kind: NodeKind::HttpEndpoint,
        file_path: Path::new("src/frontend/handlers.go").into(),
        line_start: 320,
        line_end: 401,
        package: CompactStr::new("frontend"),
        repo_id: 1,
        signature: None,
        docstring: None,
    };
    let b_id = graph.add_node(svc_b_caller);
    graph.add_dependency(b_id, "genproto");

    let dependents = graph.find_dependents("genproto");
    assert_eq!(dependents.len(), 2);
    let repo_ids: HashSet<RepoId> = dependents.iter().map(|n| n.repo_id).collect();
    assert_eq!(
        repo_ids.len(),
        2,
        "results from unrelated services sharing a locally-aliased \
             package name must retain distinct repo_id so a caller can \
             group them instead of treating this as one disambiguated match"
    );
}

/// The substring fallback in `find_dependents` (step 3, no exact/symbol/edge
/// match found) used to iterate `reverse_deps` — a `HashMap` — directly,
/// so the returned node order depended on the process's random hash seed
/// instead of on the workspace's content (idempotence invariant I5). It
/// must come back sorted by the matched package name, deterministically,
/// regardless of node/registration order.
#[test]
fn find_dependents_substring_fallback_is_ordered_by_matched_package() {
    let mut graph = ContractGraph::new();
    // Registered in an order that does not match the expected (sorted)
    // output order, so a HashMap-order regression would be caught.
    for (pkg, name, path) in [
        ("zzz-genproto-shared", "zCaller", "z.go"),
        ("aaa-genproto-shared", "aCaller", "a.go"),
        ("mmm-genproto-shared", "mCaller", "m.go"),
    ] {
        let id = graph.add_node(ContractNode {
            id: 0,
            name: CompactStr::new(name),
            kind: NodeKind::ServiceClass,
            file_path: Path::new(path).into(),
            line_start: 1,
            line_end: 1,
            package: CompactStr::new(pkg),
            repo_id: 0,
            signature: None,
            docstring: None,
        });
        graph.add_dependency(id, pkg);
    }

    let dependents = graph.find_dependents("genproto");
    let packages: Vec<&str> = dependents.iter().map(|n| n.package.as_str()).collect();
    assert_eq!(
        packages,
        vec![
            "aaa-genproto-shared",
            "mmm-genproto-shared",
            "zzz-genproto-shared"
        ],
        "substring fallback must be sorted by matched package name, not HashMap order"
    );
}

/// `analyze_impact`'s topic-registry pass used to iterate `topic_producers`/
/// `topic_consumers` — both `HashMap`s — directly, so which producer/
/// consumer topic matched first (and thus its node order in the result)
/// depended on the process's random hash seed rather than content (I5).
fn matrix_node(
    g: &mut ContractGraph,
    name: &str,
    kind: NodeKind,
    path: &str,
    package: &str,
    repo_id: RepoId,
) -> NodeId {
    g.add_node(ContractNode {
        id: 0,
        name: CompactStr::new(name),
        kind,
        file_path: Path::new(path).into(),
        line_start: 1,
        line_end: 1,
        package: CompactStr::new(package),
        repo_id,
        signature: None,
        docstring: None,
    })
}

fn matrix_edge(g: &mut ContractGraph, from: NodeId, to: NodeId, kind: EdgeKind, c: EdgeConfidence) {
    g.add_edge(ContractEdge {
        from,
        to,
        kind,
        metadata: None,
        confidence: c,
    });
}

/// (scope, role, name, via name, confidence) of every row, in order.
fn matrix_rows(
    m: &ImpactMatrix<'_>,
) -> Vec<(ImpactScope, ImpactRole, String, String, EdgeConfidence)> {
    m.rows
        .iter()
        .map(|r| {
            (
                r.scope,
                r.role,
                r.node.name.to_string(),
                r.via.map(|v| v.name.to_string()).unwrap_or_default(),
                r.confidence,
            )
        })
        .collect()
}

/// `depth` must follow the edges `reconcile_edges` really builds:
/// `Produces` producer -> topic hub, `Consumes` topic hub -> consumer.
/// producer -> t1 -> relay (consumes t1, produces t2) -> t2 -> sink.
#[test]
fn analyze_impact_with_depth_follows_reconciled_edge_directions() {
    let mut g = ContractGraph::new();
    let producer = matrix_node(&mut g, "producer", NodeKind::ServiceClass, "a/p.go", "", 0);
    let relay = matrix_node(&mut g, "relay", NodeKind::ServiceClass, "b/r.go", "", 1);
    let sink = matrix_node(&mut g, "sink", NodeKind::ServiceClass, "c/s.go", "", 2);
    g.add_producer(producer, "orders.t1");
    g.add_consumer(relay, "orders.t1");
    g.add_producer(relay, "billing.t2");
    g.add_consumer(sink, "billing.t2");
    g.reconcile_edges();
    assert!(
        g.all_edges()
            .iter()
            .any(|e| e.kind == EdgeKind::Consumes && e.to == relay),
        "reconcile_edges emits Consumes as topic -> consumer: {:?}",
        g.all_edges()
    );

    let names = |f: &ImpactFlow<'_>| -> Vec<String> {
        f.downstream_consumers
            .iter()
            .map(|n| n.name.to_string())
            .collect()
    };
    let direct = g.analyze_impact_with_depth("orders.t1", 1);
    assert_eq!(names(&direct), vec!["relay".to_string()]);
    let deep = g.analyze_impact_with_depth("orders.t1", 3);
    assert_eq!(
        names(&deep),
        vec!["relay".to_string(), "sink".to_string()],
        "depth 3 must reach the second-hop consumer"
    );
    assert!(deep.topics.iter().any(|t| t.name == "billing.t2"));
}

/// Plan 4 step 4.6a: a client whose `CallsRpc` edges fan out to two
/// homonymous contracts (`Ambiguous`) gets one row per candidate, each
/// classified against that candidate's own owner root.
#[test]
fn impact_matrix_ambiguous_fan_out_is_one_row_per_candidate() {
    use EdgeConfidence::*;
    use ImpactRole::*;
    use ImpactScope::*;
    let mut g = ContractGraph::new();
    let a = matrix_node(
        &mut g,
        "AdminService.Ping",
        NodeKind::GrpcMethod,
        "a/admin.proto",
        "a.v1",
        0,
    );
    let b = matrix_node(
        &mut g,
        "AdminService.Ping",
        NodeKind::GrpcMethod,
        "b/admin.proto",
        "b.v1",
        1,
    );
    let ha = matrix_node(
        &mut g,
        "pingA",
        NodeKind::GrpcMethod,
        "svc-a/server.go",
        "",
        2,
    );
    let hb = matrix_node(
        &mut g,
        "pingB",
        NodeKind::GrpcMethod,
        "svc-b/server.go",
        "",
        3,
    );
    let client = matrix_node(
        &mut g,
        "callPing",
        NodeKind::ServiceClass,
        "svc-a/client.go",
        "",
        2,
    );
    matrix_edge(&mut g, ha, a, EdgeKind::Implements, Exact);
    matrix_edge(&mut g, hb, b, EdgeKind::Implements, Exact);
    matrix_edge(&mut g, client, a, EdgeKind::CallsRpc, Ambiguous);
    matrix_edge(&mut g, client, b, EdgeKind::CallsRpc, Ambiguous);

    let m = g.impact_matrix("Ping", 1);
    assert_eq!(m.owner_roots, vec![2, 3]);
    assert_eq!(
        matrix_rows(&m),
        vec![
            (
                External,
                Client,
                "callPing".into(),
                "AdminService.Ping".into(),
                Ambiguous
            ),
            (
                Internal,
                Handler,
                "pingA".into(),
                "AdminService.Ping".into(),
                Exact
            ),
            (
                Internal,
                Handler,
                "pingB".into(),
                "AdminService.Ping".into(),
                Exact
            ),
            (
                Internal,
                Client,
                "callPing".into(),
                "AdminService.Ping".into(),
                Ambiguous
            ),
        ]
    );
    // The EXTERNAL client row is the one linked through `b` (owned by root 3).
    assert_eq!(m.rows[0].via.map(|v| v.id), Some(b));
    assert_eq!(m.rows[3].via.map(|v| v.id), Some(a));
    // Deterministic: the same graph gives the same rows.
    assert_eq!(matrix_rows(&g.impact_matrix("Ping", 1)), matrix_rows(&m));
}

/// Owner = handlers' roots, else the `.proto` root; a rootless node is
/// never INTERNAL; a node reached by a direct name match and by an edge is
/// one row (the edge's).
#[test]
fn impact_matrix_owner_fallback_rootless_and_dedup() {
    use EdgeConfidence::*;
    use ImpactRole::*;
    use ImpactScope::*;
    let mut g = ContractGraph::new();
    let m_id = matrix_node(
        &mut g,
        "PingService.Ping",
        NodeKind::GrpcMethod,
        "proto/ping.proto",
        "p.v1",
        0,
    );
    let local = matrix_node(
        &mut g,
        "localCaller",
        NodeKind::ServiceClass,
        "proto/tools/cli.go",
        "",
        0,
    );
    let rootless = matrix_node(
        &mut g,
        "busCaller",
        NodeKind::ServiceClass,
        "event-bus",
        "",
        RepoId::MAX,
    );
    matrix_edge(&mut g, local, m_id, EdgeKind::CallsRpc, Exact);
    matrix_edge(&mut g, rootless, m_id, EdgeKind::CallsRpc, Exact);

    // No handler: the `.proto` root owns the contract.
    let m = g.impact_matrix("PingService.Ping", 1);
    assert_eq!(m.owner_roots, vec![0]);
    assert_eq!(
        matrix_rows(&m),
        vec![
            (
                External,
                Client,
                "busCaller".into(),
                "PingService.Ping".into(),
                Exact
            ),
            (
                Internal,
                Client,
                "localCaller".into(),
                "PingService.Ping".into(),
                Exact
            ),
        ]
    );

    // A handler named like the method (a direct symbol match) that also
    // implements it through an edge: one row, the edge's confidence, and
    // its root becomes the owner.
    let h = matrix_node(&mut g, "Ping", NodeKind::GrpcMethod, "svc/ping.go", "", 1);
    matrix_edge(&mut g, h, m_id, EdgeKind::Implements, Heuristic);
    let m = g.impact_matrix("Ping", 1);
    assert_eq!(m.owner_roots, vec![1]);
    assert_eq!(
        matrix_rows(&m),
        vec![
            (
                External,
                Client,
                "busCaller".into(),
                "PingService.Ping".into(),
                Exact
            ),
            (
                External,
                Client,
                "localCaller".into(),
                "PingService.Ping".into(),
                Exact
            ),
            (
                Internal,
                Handler,
                "Ping".into(),
                "PingService.Ping".into(),
                Heuristic
            ),
        ]
    );
}

#[test]
fn analyze_impact_orders_producers_by_matched_topic_name() {
    let mut graph = ContractGraph::new();
    for (topic, name, path) in [
        ("orders.zzz", "zProducer", "z.go"),
        ("orders.aaa", "aProducer", "a.go"),
        ("orders.mmm", "mProducer", "m.go"),
    ] {
        let id = graph.add_node(ContractNode {
            id: 0,
            name: CompactStr::new(name),
            kind: NodeKind::ServiceClass,
            file_path: Path::new(path).into(),
            line_start: 1,
            line_end: 1,
            package: CompactStr::new("shop"),
            repo_id: 0,
            signature: None,
            docstring: None,
        });
        graph.add_producer(id, topic);
    }

    let flow = graph.analyze_impact("orders");
    let names: Vec<&str> = flow
        .upstream_producers
        .iter()
        .map(|n| n.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["aProducer", "mProducer", "zProducer"],
        "producers must be ordered by their matched topic name, not HashMap order"
    );
}

/// Two consumers sharing file, line and name are two nodes: both are listed
/// as not expanded, ordered by node id, identically on every call.
#[test]
fn unexpanded_consumers_are_ordered_by_id_when_location_ties() {
    let mut graph = ContractGraph::new();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let c = graph.add_node(memo_node(
            "consume:Streams.TIE",
            "",
            "pp/tie.ts",
            1,
            NodeKind::PostProcessor,
        ));
        graph.add_consumer(c, "Streams.TIE");
        ids.push(c);
    }
    graph.reconcile_edges();
    let first = graph.analyze_impact_with_depth("TIE", 1);
    let got: Vec<NodeId> = first.unexpanded.iter().map(|n| n.id).collect();
    let mut want = ids.clone();
    want.sort_unstable();
    assert_eq!(got, want);
    for _ in 0..5 {
        let again = graph.analyze_impact_with_depth("TIE", 1);
        assert_eq!(
            again.unexpanded.iter().map(|n| n.id).collect::<Vec<_>>(),
            got
        );
    }
}

/// Volontariapp 7.0.6 report, defects 5, 6 and 13: the re-emission of a
/// post-processor is a separate `produce:` node in the consumer's file, so
/// `depth: 2` returned the depth-1 answer; a topic with no resolved
/// producer was not said; the value spelling `event.created` matched
/// nothing.
#[test]
fn impact_depth_follows_same_file_producers_and_reports_gaps() {
    let mut graph = ContractGraph::new();
    let origin = graph.add_node(memo_node(
        "produce:Streams.EVENT_CREATED",
        "",
        "domain/repo.ts",
        0,
        NodeKind::EventStream,
    ));
    graph.add_producer(origin, "Streams.EVENT_CREATED");
    let pp = graph.add_node(memo_node(
        "EventCreatedPostProcessor",
        "",
        "pp/event-created.post-processor.ts",
        1,
        NodeKind::PostProcessor,
    ));
    graph.add_consumer(pp, "Streams.EVENT_CREATED");
    let reemit = graph.add_node(memo_node(
        "produce:Streams.WS_FEEDBACK",
        "",
        "pp/event-created.post-processor.ts",
        1,
        NodeKind::EventStream,
    ));
    graph.add_producer(reemit, "Streams.WS_FEEDBACK");
    let ws = graph.add_node(memo_node(
        "consume:Streams.WS_FEEDBACK",
        "",
        "ws/ws-feedback.options.ts",
        2,
        NodeKind::PostProcessor,
    ));
    graph.add_consumer(ws, "Streams.WS_FEEDBACK");
    let orphan = graph.add_node(memo_node(
        "consume:Streams.USER_CREATED",
        "",
        "ws/ws-user-created.options.ts",
        2,
        NodeKind::PostProcessor,
    ));
    graph.add_consumer(orphan, "Streams.USER_CREATED");
    graph.reconcile_edges();

    let names = |flow: &ImpactFlow| -> Vec<String> {
        flow.downstream_consumers
            .iter()
            .map(|n| n.name.to_string())
            .collect()
    };
    let direct = graph.analyze_impact_with_depth("EVENT_CREATED", 1);
    assert!(!names(&direct).contains(&"consume:Streams.WS_FEEDBACK".to_string()));
    let two = graph.analyze_impact_with_depth("EVENT_CREATED", 2);
    assert!(
        names(&two).contains(&"consume:Streams.WS_FEEDBACK".to_string()),
        "{:?}",
        names(&two)
    );
    assert_eq!(
        two.downstream_producers
            .iter()
            .map(|n| n.id)
            .collect::<Vec<_>>(),
        vec![reemit]
    );

    let by_value = graph.analyze_impact("event.created");
    assert_eq!(by_value.key.as_str(), "event_created");
    assert!(by_value.downstream_consumers.iter().any(|n| n.id == pp));

    let matrix = graph.impact_matrix("EVENT_CREATED", 2);
    assert!(matrix.file_linked_hops);
    assert!(!matrix.no_producer_resolved && !matrix.no_consumer_resolved);
    let orphan_matrix = graph.impact_matrix("USER_CREATED", 1);
    assert!(orphan_matrix.no_producer_resolved, "{orphan_matrix:?}");
    assert!(!orphan_matrix.no_consumer_resolved);
}

/// Builds `orders.created` -[Consumes]-> `billingHandler` -[Produces]-> `payment.settled`
/// -[Consumes]-> `ledgerHandler`, so a depth-1 query only ever sees `billingHandler`
/// while depth-2 pulls in `payment.settled` and `ledgerHandler` too — proving the
/// traversal follows real edges rather than re-running the substring pass.
#[test]
fn analyze_impact_with_depth_follows_produces_consumes_edges_transitively() {
    let mut graph = ContractGraph::new();
    let topic_orders = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("orders.created"),
        kind: NodeKind::KafkaTopic,
        file_path: Path::new("infra/topics.yaml").into(),
        line_start: 1,
        line_end: 1,
        package: CompactStr::new(""),
        repo_id: 0,
        signature: None,
        docstring: None,
    });
    let billing_handler = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("billingHandler"),
        kind: NodeKind::PostProcessor,
        file_path: Path::new("services/billing/handler.go").into(),
        line_start: 10,
        line_end: 20,
        package: CompactStr::new("billing"),
        repo_id: 1,
        signature: None,
        docstring: None,
    });
    let topic_settled = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("payment.settled"),
        kind: NodeKind::KafkaTopic,
        file_path: Path::new("infra/topics.yaml").into(),
        line_start: 2,
        line_end: 2,
        package: CompactStr::new(""),
        repo_id: 0,
        signature: None,
        docstring: None,
    });
    let ledger_handler = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("ledgerHandler"),
        kind: NodeKind::PostProcessor,
        file_path: Path::new("services/ledger/handler.go").into(),
        line_start: 5,
        line_end: 15,
        package: CompactStr::new("ledger"),
        repo_id: 2,
        signature: None,
        docstring: None,
    });

    graph.add_edge(ContractEdge {
        from: billing_handler,
        to: topic_orders,
        kind: EdgeKind::Consumes,
        metadata: None,
        confidence: EdgeConfidence::Exact,
    });
    graph.add_edge(ContractEdge {
        from: billing_handler,
        to: topic_settled,
        kind: EdgeKind::Produces,
        metadata: None,
        confidence: EdgeConfidence::Exact,
    });
    graph.add_edge(ContractEdge {
        from: ledger_handler,
        to: topic_settled,
        kind: EdgeKind::Consumes,
        metadata: None,
        confidence: EdgeConfidence::Exact,
    });
    // Cycle: the ledger handler re-produces onto the original topic. Without
    // visited-set cycle detection this would loop forever re-discovering
    // `billingHandler`/`topic_settled` at every hop.
    graph.add_edge(ContractEdge {
        from: ledger_handler,
        to: topic_orders,
        kind: EdgeKind::Produces,
        metadata: None,
        confidence: EdgeConfidence::Exact,
    });

    let direct = graph.analyze_impact_with_depth("orders.created", 1);
    let direct_names: HashSet<&str> = direct
        .downstream_consumers
        .iter()
        .map(|n| n.name.as_str())
        .collect();
    assert_eq!(
        direct_names,
        HashSet::from(["billingHandler"]),
        "depth 1 must match plain analyze_impact: direct consumers only"
    );
    assert!(
        direct
            .topics
            .iter()
            .all(|t| t.name.as_str() != "payment.settled"),
        "depth 1 must not pull in a topic two hops away"
    );

    let transitive = graph.analyze_impact_with_depth("orders.created", 2);
    let transitive_names: HashSet<&str> = transitive
        .downstream_consumers
        .iter()
        .map(|n| n.name.as_str())
        .collect();
    assert_eq!(
        transitive_names,
        HashSet::from(["billingHandler", "ledgerHandler"]),
        "depth 2 must include the consumer of the topic billingHandler produces onto"
    );
    assert!(
        transitive
            .topics
            .iter()
            .any(|t| t.name.as_str() == "payment.settled"),
        "depth 2 must surface the transitively-produced topic"
    );

    // The cycle back to `orders.created` must not re-add it as a "new"
    // downstream discovery, and higher depths must terminate rather than
    // looping forever rediscovering the same two nodes.
    let deep = graph.analyze_impact_with_depth("orders.created", 5);
    assert_eq!(
        deep.downstream_consumers.len(),
        transitive.downstream_consumers.len(),
        "cycle back to the origin topic must not manufacture duplicate consumers at higher depth"
    );

    let over_cap = graph.analyze_impact_with_depth("orders.created", 200);
    assert_eq!(
        over_cap.downstream_consumers.len(),
        deep.downstream_consumers.len(),
        "requested depth must be clamped to MAX_IMPACT_DEPTH, not iterate 200 hops"
    );
}

/// A bare-name import (ambiguous across packages) must not be reported with
/// the same confidence as an exact fully-qualified import.
#[test]
fn test_import_resolution_confidence_distinguishes_fqcn_from_bare_name() {
    let mut graph = ContractGraph::new();

    // Two unrelated `Invoice` types in different packages — the classic
    // name-collision the roadmap calls out.
    let acme_invoice = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("Invoice"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/billing/acme/Invoice.java").into(),
        line_start: 1,
        line_end: 10,
        package: CompactStr::new("com.acme.billing"),
        repo_id: 1,
        signature: None,
        docstring: None,
    });
    graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("Invoice"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/billing/other/Invoice.java").into(),
        line_start: 1,
        line_end: 10,
        package: CompactStr::new("com.other.billing"),
        repo_id: 2,
        signature: None,
        docstring: None,
    });

    let bare_importer = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("BareImporter"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/consumer/BareImporter.java").into(),
        line_start: 1,
        line_end: 5,
        package: CompactStr::new("com.mesh.consumer"),
        repo_id: 3,
        signature: None,
        docstring: None,
    });
    let fqcn_importer = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("FqcnImporter"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/consumer/FqcnImporter.java").into(),
        line_start: 1,
        line_end: 5,
        package: CompactStr::new("com.mesh.consumer"),
        repo_id: 3,
        signature: None,
        docstring: None,
    });
    let rust_importer = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("RustImporter"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/consumer/rust_importer.rs").into(),
        line_start: 1,
        line_end: 5,
        package: CompactStr::new("com.mesh.consumer"),
        repo_id: 3,
        signature: None,
        docstring: None,
    });

    // Bare, unqualified import — ambiguous, must resolve as Heuristic.
    graph.add_dependency(bare_importer, "Invoice");
    // Fully-qualified Java-style import — unambiguous, must resolve as Exact
    // and must land specifically on the `com.acme.billing` node.
    graph.add_dependency(fqcn_importer, "com.acme.billing.Invoice");
    // Fully-qualified Rust-style path — same requirement, `::`-separated.
    graph.add_dependency(rust_importer, "com::acme::billing::Invoice");

    graph.reconcile_edges();

    let find_import_edge = |from: NodeId| {
        graph
            .all_edges()
            .iter()
            .find(|e| e.kind == EdgeKind::Imports && e.from == from)
            .expect("resolved import edge should exist")
    };

    let fqcn_edge = find_import_edge(fqcn_importer);
    let rust_edge = find_import_edge(rust_importer);

    assert_eq!(fqcn_edge.confidence, EdgeConfidence::Exact);
    assert_eq!(rust_edge.confidence, EdgeConfidence::Exact);

    // The whole point: the FQCN/`::` matches must resolve to the exact
    // node their qualifier names, not merely "a" node sharing the bare name.
    assert_eq!(fqcn_edge.to, acme_invoice);
    assert_eq!(rust_edge.to, acme_invoice);

    // The bare import is a genuine tie: neither `Invoice` candidate shares
    // `bare_importer`'s repo, so the graph must not silently pick one — it
    // must emit an edge to *both*, tagged `Ambiguous`, rather than a single
    // confident-looking `Heuristic` edge to whichever candidate happened to
    // be inserted first.
    let bare_edges: Vec<_> = graph
        .all_edges()
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports && e.from == bare_importer)
        .collect();
    assert_eq!(
        bare_edges.len(),
        2,
        "a genuine bare-name tie must fan out to every candidate, got: {bare_edges:?}"
    );
    assert!(bare_edges
        .iter()
        .all(|e| e.confidence == EdgeConfidence::Ambiguous));
    let bare_targets: std::collections::HashSet<NodeId> = bare_edges.iter().map(|e| e.to).collect();
    assert!(bare_targets.contains(&acme_invoice));

    // Bug-in-the-test guard: if every match ends up the same confidence, the
    // field is decorative, not signal.
    assert_ne!(bare_edges[0].confidence, fqcn_edge.confidence);
}

/// The `Implements` reconciliation likewise must not claim `Exact`
/// confidence for a handler that only matches the proto method by a
/// case-insensitive / PascalCase heuristic rather than the literal FQCN.
#[test]
fn test_implements_reconciliation_confidence_distinguishes_heuristic_match() {
    let mut graph = ContractGraph::new();

    let proto_method = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("auth.v1.AuthenticateUser"),
        kind: NodeKind::GrpcMethod,
        file_path: Path::new("proto-registry/auth.proto").into(),
        line_start: 20,
        line_end: 25,
        package: CompactStr::new("auth.v1"),
        repo_id: 0,
        signature: Some(CompactStr::new(
            "rpc AuthenticateUser (AuthRequest) returns (AuthResponse);",
        )),
        docstring: None,
    });

    // Handler whose name is only a case-insensitive match to the bare
    // method name ("authenticateuser" vs "AuthenticateUser") — heuristic.
    let heuristic_handler = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("authenticateuser"),
        kind: NodeKind::GrpcMethod,
        file_path: Path::new("services/auth/AuthController.java").into(),
        line_start: 15,
        line_end: 30,
        package: CompactStr::new("com.mesh.auth"),
        repo_id: 1,
        signature: None,
        docstring: None,
    });

    // Handler whose name is the literal FQCN — exact.
    let exact_handler = graph.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("auth.v1.AuthenticateUser"),
        kind: NodeKind::GrpcMethod,
        file_path: Path::new("services/auth/AuthServiceImpl.java").into(),
        line_start: 15,
        line_end: 30,
        package: CompactStr::new("com.mesh.auth"),
        repo_id: 1,
        signature: None,
        docstring: None,
    });
    graph.reconcile_edges();

    let implements_edges: Vec<_> = graph
        .all_edges()
        .iter()
        .filter(|e| e.kind == EdgeKind::Implements && e.to == proto_method)
        .collect();

    let heuristic_edge = implements_edges
        .iter()
        .find(|e| e.from == heuristic_handler)
        .expect("heuristic handler should still be linked");
    let exact_edge = implements_edges
        .iter()
        .find(|e| e.from == exact_handler)
        .expect("exact handler should be linked");

    assert_eq!(heuristic_edge.confidence, EdgeConfidence::Heuristic);
    assert_eq!(exact_edge.confidence, EdgeConfidence::Exact);
    assert_ne!(heuristic_edge.confidence, exact_edge.confidence);
}

#[test]
fn test_large_scale_repo_indexing() {
    let mut graph = ContractGraph::new();

    // Register 500 microservices (exceeding 255 u8 limit)
    for repo_idx in 0..500u16 {
        let node = ContractNode {
            id: 0,
            name: CompactStr::new(format!("ServiceHandler{repo_idx}")),
            kind: NodeKind::ServiceClass,
            file_path: PathBuf::from(format!("services/service_{repo_idx}/Handler.go")).into(),
            line_start: 1,
            line_end: 100,
            package: CompactStr::new(format!("service.{repo_idx}")),
            repo_id: repo_idx,
            signature: None,
            docstring: None,
        };
        let nid = graph.add_node(node);
        if repo_idx % 2 == 0 {
            graph.add_dependency(nid, "SharedEnterpriseContract");
        }
    }

    assert_eq!(graph.node_count(), 500);

    let dependents = graph.find_dependents("SharedEnterpriseContract");
    assert_eq!(dependents.len(), 250);

    // Verify high repo_id (e.g. repo 498) is accurately preserved without overflow
    let high_repo_node = dependents.iter().find(|n| n.repo_id == 498);
    assert!(high_repo_node.is_some());
    assert_eq!(high_repo_node.unwrap().name.as_str(), "ServiceHandler498");
}

/// A hub topic with producers and consumers generates linear O(P + C)
/// Produces/Consumes edges, rather than quadratic O(P * C) edges.
#[test]
fn test_hub_topic_edge_growth_is_linear_not_quadratic() {
    let mut graph = ContractGraph::new();

    const FANOUT: usize = 50;
    let mut producer_ids = Vec::with_capacity(FANOUT);
    let mut consumer_ids = Vec::with_capacity(FANOUT);

    for i in 0..FANOUT {
        let producer = ContractNode {
            id: 0,
            name: CompactStr::new(format!("Producer{i}")),
            kind: NodeKind::ServiceClass,
            file_path: Path::new(&format!("services/producer_{i}/Handler.go")).into(),
            line_start: 1,
            line_end: 10,
            package: CompactStr::new(format!("producer.{i}")),
            repo_id: i as RepoId,
            signature: None,
            docstring: None,
        };
        let pid = graph.add_node(producer);
        graph.add_producer(pid, "hub-topic");
        producer_ids.push(pid);

        let consumer = ContractNode {
            id: 0,
            name: CompactStr::new(format!("Consumer{i}")),
            kind: NodeKind::ServiceClass,
            file_path: Path::new(&format!("services/consumer_{i}/Handler.go")).into(),
            line_start: 1,
            line_end: 10,
            package: CompactStr::new(format!("consumer.{i}")),
            repo_id: (FANOUT + i) as RepoId,
            signature: None,
            docstring: None,
        };
        let cid = graph.add_node(consumer);
        graph.add_consumer(cid, "hub-topic");
        consumer_ids.push(cid);
    }

    graph.reconcile_edges();

    let produces_count = graph
        .edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Produces)
        .count();
    let consumes_count = graph
        .edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Consumes)
        .count();
    let dispatches_count = graph
        .edges
        .iter()
        .filter(|e| e.kind == EdgeKind::DispatchesTo)
        .count();

    // Bounded: one Produces edge per producer, one Consumes edge per
    // consumer, and no direct producer->consumer edges at all — never the
    // 2,500 (50*50) pairs a quadratic pass would have generated.
    assert_eq!(produces_count, FANOUT);
    assert_eq!(consumes_count, FANOUT);
    assert_eq!(dispatches_count, 0);

    let total_topic_edges = produces_count + consumes_count + dispatches_count;
    assert_eq!(total_topic_edges, FANOUT * 2);
    assert!(total_topic_edges < FANOUT * FANOUT);
}

#[test]
fn test_grpc_enum_decorator_end_to_end_reconciliation() {
    let mut graph = ContractGraph::new();

    // 1. Proto RPC declaration node
    let proto_node = ContractNode {
        id: 0,
        name: CompactStr::new("volontariapp.user.v1.UserService.SignUp"),
        kind: NodeKind::GrpcMethod,
        file_path: Path::new("proto/user.proto").into(),
        line_start: 10,
        line_end: 15,
        package: CompactStr::new("volontariapp.user.v1"),
        repo_id: 1,
        signature: Some(CompactStr::new(
            "rpc SignUp(SignUpRequest) returns (SignUpResponse)",
        )),
        docstring: None,
    };
    graph.add_node(proto_node);

    // 2. TS Controller Handler node extracted from @GrpcMethod(USER_SERVICE_NAME, UserCommandMethod.SIGN_UP)
    let ts_handler = ContractNode {
            id: 0,
            name: CompactStr::new("USER_SERVICE_NAME.SIGN_UP"),
            kind: NodeKind::GrpcMethod,
            file_path: Path::new(
                "ms-user/src/modules/user/controllers/command/user.command.controller.ts",
            )
            .into(),
            line_start: 47,
            line_end: 50,
            package: CompactStr::new("ms-user"),
            repo_id: 1,
            signature: Some(CompactStr::new(
                "@GrpcMethod(USER_SERVICE_NAME, UserCommandMethod.SIGN_UP) async signUp(data: SignUpCommandDTO)",
            )),
            docstring: None,
        };
    graph.add_node(ts_handler);

    // Reconcile graph edges
    graph.reconcile_edges();

    // Query analyze_grpc("SignUp")
    let trace = graph.analyze_grpc("SignUp");
    assert!(
        trace.proto_definition.is_some(),
        "Proto definition for SignUp should be found"
    );
    assert!(
        !trace.server_handlers.is_empty(),
        "Server handlers for SignUp must not be empty"
    );
    assert_eq!(
        trace.server_handlers[0].0.name.as_str(),
        "USER_SERVICE_NAME.SIGN_UP"
    );

    // Query analyze_grpc("UserService")
    let trace_svc = graph.analyze_grpc("UserService");
    assert!(!trace_svc.server_handlers.is_empty());

    // Query search_symbols("signUp")
    let symbols = graph.search_symbols("signUp", None);
    assert!(symbols
        .iter()
        .any(|n| n.name.as_str() == "USER_SERVICE_NAME.SIGN_UP"));
}

#[test]
fn test_analyze_grpc_multi_service_anchors_resolves_all_callers() {
    let mut graph = ContractGraph::new();

    // Service v1 in auth
    let svc_v1 = ContractNode {
        id: 0,
        name: CompactStr::new("AuthService"),
        kind: NodeKind::GrpcService,
        file_path: Path::new("services/auth-v1/server.go").into(),
        line_start: 10,
        line_end: 20,
        package: CompactStr::new("auth.v1"),
        repo_id: 1,
        signature: None,
        docstring: None,
    };
    // Service v2 in new auth
    let svc_v2 = ContractNode {
        id: 0,
        name: CompactStr::new("AuthService"),
        kind: NodeKind::GrpcService,
        file_path: Path::new("services/auth-v2/server.go").into(),
        line_start: 10,
        line_end: 20,
        package: CompactStr::new("auth.v2"),
        repo_id: 2,
        signature: None,
        docstring: None,
    };
    // Caller A targeting v1
    let caller_a = ContractNode {
        id: 0,
        name: CompactStr::new("callAuthV1"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/gateway/client1.go").into(),
        line_start: 5,
        line_end: 15,
        package: CompactStr::new("gateway"),
        repo_id: 3,
        signature: None,
        docstring: None,
    };
    // Caller B targeting v2
    let caller_b = ContractNode {
        id: 0,
        name: CompactStr::new("callAuthV2"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("services/billing/client2.go").into(),
        line_start: 5,
        line_end: 15,
        package: CompactStr::new("billing"),
        repo_id: 4,
        signature: None,
        docstring: None,
    };

    let id_v1 = graph.add_node(svc_v1);
    let id_v2 = graph.add_node(svc_v2);
    let id_caller_a = graph.add_node(caller_a);
    let id_caller_b = graph.add_node(caller_b);

    graph.add_edge(ContractEdge {
        from: id_caller_a,
        to: id_v1,
        kind: EdgeKind::CallsRpc,
        metadata: Some(CompactStr::new("AuthService")),
        confidence: EdgeConfidence::Exact,
    });
    graph.add_edge(ContractEdge {
        from: id_caller_b,
        to: id_v2,
        kind: EdgeKind::CallsRpc,
        metadata: Some(CompactStr::new("AuthService")),
        confidence: EdgeConfidence::Exact,
    });

    let trace = graph.analyze_grpc("AuthService");
    assert_eq!(
        trace.server_handlers.len(),
        2,
        "both v1 and v2 AuthService must be server handlers"
    );
    assert_eq!(
        trace.client_stubs.len(),
        2,
        "both callers to v1 and v2 must be resolved without anchor overwrite"
    );
    assert!(trace
        .client_stubs
        .iter()
        .any(|(c, _)| c.name.as_str() == "callAuthV1"));
    assert!(trace
        .client_stubs
        .iter()
        .any(|(c, _)| c.name.as_str() == "callAuthV2"));
}

fn fp_node(name: &str, file: &str, line: usize, kind: NodeKind) -> ContractNode {
    ContractNode {
        id: 0,
        name: CompactStr::new(name),
        kind,
        file_path: Path::new(file).into(),
        line_start: line,
        line_end: line + 2,
        package: CompactStr::new("pkg"),
        repo_id: 0,
        signature: Some(CompactStr::new(format!("sig {name}"))),
        docstring: None,
    }
}

/// The fingerprint must depend on graph content only: inserting the same
/// nodes and relations in a different order renumbers every `NodeId` but
/// must not change it.
#[test]
fn fingerprint_ignores_insertion_order_and_node_ids() {
    let build = |reversed: bool| {
        let mut graph = ContractGraph::new();
        let mut specs = vec![
            ("Widget", "a/widget.go", 1, NodeKind::ServiceClass),
            ("Use", "b/use.go", 4, NodeKind::ServiceClass),
            ("Emit", "c/emit.go", 7, NodeKind::ServiceClass),
        ];
        if reversed {
            specs.reverse();
        }
        let mut ids = HashMap::new();
        for (name, file, line, kind) in specs {
            ids.insert(name, graph.add_node(fp_node(name, file, line, kind)));
        }
        graph.add_dependency(ids["Use"], "Widget");
        graph.add_producer(ids["Emit"], "orders");
        graph.add_consumer(ids["Use"], "orders");
        graph.reconcile_edges();
        graph
    };

    let forward = build(false);
    let reversed = build(true);
    assert_eq!(forward.canonical_lines(), reversed.canonical_lines());
    assert_eq!(forward.fingerprint(), reversed.fingerprint());
}

/// Any content difference — here a single line number — must change it.
#[test]
fn fingerprint_changes_when_content_changes() {
    let mut a = ContractGraph::new();
    a.add_node(fp_node("Widget", "a/widget.go", 1, NodeKind::ServiceClass));
    let mut b = ContractGraph::new();
    b.add_node(fp_node("Widget", "a/widget.go", 2, NodeKind::ServiceClass));
    assert_ne!(a.fingerprint(), b.fingerprint());
}

/// A node indexed twice (e.g. through two overlapping roots) must stay
/// visible in the canonical form rather than collapse into one line.
#[test]
fn canonical_lines_keep_duplicate_nodes() {
    let mut graph = ContractGraph::new();
    graph.add_node(fp_node("Widget", "a/widget.go", 1, NodeKind::ServiceClass));
    graph.add_node(fp_node("Widget", "a/widget.go", 1, NodeKind::ServiceClass));
    let node_lines = graph
        .canonical_lines()
        .into_iter()
        .filter(|l| l.starts_with("node "))
        .count();
    assert_eq!(node_lines, 2);
}

fn calls_rpc_targets(g: &ContractGraph, from: NodeId) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = g
        .all_edges()
        .iter()
        .filter(|e| e.from == from && e.kind == EdgeKind::CallsRpc)
        .map(|e| e.to)
        .collect();
    out.sort_unstable();
    out
}

/// Plan 4 step 4.5: the stock gRPC health service is infrastructure,
/// never a service-to-service edge — whether it is inferred from code
/// (Go's `RegisterHealthServer`, no `.proto` behind it), declared by a
/// vendored `grpc/health/v1/health.proto`, or named by its full proto
/// name. A `service Health` the repo declares in its own proto package
/// is a real service and keeps its edge.
#[test]
fn grpc_health_service_is_filtered_as_infrastructure() {
    use NodeKind::{GrpcMethod, GrpcService, ServiceClass};
    let mut g = ContractGraph::new();
    let inferred = g.add_node(memo_node(
        "Health",
        "checkout",
        "src/checkout/main.go",
        0,
        GrpcService,
    ));
    let caller = g.add_node(memo_node(
        "TestStartup",
        "checkout",
        "src/checkout/main_test.go",
        0,
        ServiceClass,
    ));
    g.add_rpc_call(caller, "Health");
    let vendored = g.add_node(memo_node(
        "Health.Check",
        "grpc.health.v1",
        "vendor/grpc/health/v1/health.proto",
        1,
        GrpcMethod,
    ));
    let probe = g.add_node(memo_node(
        "probe",
        "ops",
        "src/ops/probe.go",
        1,
        ServiceClass,
    ));
    g.add_rpc_call(probe, "Health.Check");
    let fq = g.add_node(memo_node("fq", "ops", "src/ops/fq.go", 1, ServiceClass));
    g.add_rpc_call(fq, "grpc.health.v1.Health");
    g.reconcile_edges();
    assert!(
        calls_rpc_targets(&g, caller).is_empty(),
        "inferred Health: {:?}",
        g.all_edges()
    );
    assert!(
        calls_rpc_targets(&g, probe).is_empty(),
        "vendored health.proto: {:?}",
        g.all_edges()
    );
    assert!(calls_rpc_targets(&g, fq).is_empty());
    let _ = (inferred, vendored);

    // A repo's own `service Health` in its own proto package is kept.
    let mut g = ContractGraph::new();
    let own = g.add_node(memo_node(
        "Health",
        "acme.clinic",
        "proto/clinic.proto",
        0,
        GrpcService,
    ));
    let caller = g.add_node(memo_node(
        "book",
        "frontend",
        "src/frontend/book.ts",
        0,
        ServiceClass,
    ));
    g.add_rpc_call(caller, "Health");
    g.reconcile_edges();
    assert_eq!(calls_rpc_targets(&g, caller), vec![own]);
}

/// `<X>` resolves to a declared `<X>Service` when `<X>` itself is not
/// declared (ts-proto's `AdClient` for `service Ad` vs. a caller naming
/// `AdService`), and never to an undeclared name.
#[test]
fn rpc_target_falls_back_to_service_suffix_only_against_declared_services() {
    use NodeKind::{GrpcService, ServiceClass};
    let mut g = ContractGraph::new();
    let ad = g.add_node(memo_node(
        "AdService",
        "oteldemo",
        "pb/demo.proto",
        0,
        GrpcService,
    ));
    let cart = g.add_node(memo_node(
        "Cart",
        "oteldemo",
        "pb/demo.proto",
        0,
        GrpcService,
    ));
    let cart_svc = g.add_node(memo_node(
        "CartService",
        "oteldemo",
        "pb/other.proto",
        0,
        GrpcService,
    ));
    let c_ad = g.add_node(memo_node(
        "ads",
        "frontend",
        "src/frontend/a.ts",
        0,
        ServiceClass,
    ));
    let c_cart = g.add_node(memo_node(
        "cart",
        "frontend",
        "src/frontend/c.ts",
        0,
        ServiceClass,
    ));
    let c_foo = g.add_node(memo_node(
        "foo",
        "frontend",
        "src/frontend/f.ts",
        0,
        ServiceClass,
    ));
    g.add_rpc_call(c_ad, "Ad");
    // `Cart` is itself declared: the literal target wins, no fallback.
    g.add_rpc_call(c_cart, "Cart");
    g.add_rpc_call(c_foo, "Foo");
    g.reconcile_edges();
    assert_eq!(calls_rpc_targets(&g, c_ad), vec![ad]);
    assert_eq!(calls_rpc_targets(&g, c_cart), vec![cart]);
    assert!(calls_rpc_targets(&g, c_foo).is_empty());
    let _ = cart_svc;
}

/// The stock health client (`healthpb.NewHealthClient`, ts `HealthClient`)
/// names `Health`, never the repo's own `HealthService`: with no `Health`
/// in the graph (a client-only readiness probe, no `RegisterHealthServer`,
/// no vendored `health.proto`), the `<X>` -> `<X>Service` fallback must not
/// turn it into an edge to an unrelated `service HealthService`.
#[test]
fn stock_health_client_does_not_fall_back_to_a_health_service() {
    use NodeKind::{GrpcService, ServiceClass};
    let mut g = ContractGraph::new();
    let _own = g.add_node(memo_node(
        "HealthService",
        "acme.clinic",
        "proto/clinic.proto",
        0,
        GrpcService,
    ));
    let probe = g.add_node(memo_node(
        "waitReady",
        "ops",
        "src/ops/ready.go",
        0,
        ServiceClass,
    ));
    g.add_rpc_call(probe, "Health");
    g.reconcile_edges();
    assert!(
        calls_rpc_targets(&g, probe).is_empty(),
        "stock health client linked to the repo's HealthService: {:?}",
        g.all_edges()
    );
}
