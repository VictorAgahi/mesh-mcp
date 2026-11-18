//! `reconcile_delta` against its oracle: after every change, the graph a delta
//! ends in must be the one a full `reconcile_edges` of the same patched graph
//! ends in, edge order and resolved handler names included.

use super::*;

/// One indexed file: its nodes and the facts recorded against them, the way a
/// language extractor's fragment folds into the graph.
#[derive(Clone, Default)]
struct FileSpec {
    path: String,
    repo: RepoId,
    nodes: Vec<NodeSpec>,
    /// Records the nodes' RPC calls round-robin after every node, so one
    /// caller's calls are interleaved with another's in `rpc_calls`.
    interleave_rpc: bool,
}

#[derive(Clone)]
struct NodeSpec {
    name: &'static str,
    kind: NodeKind,
    package: &'static str,
    signature: Option<&'static str>,
    deps: Vec<&'static str>,
    produces: Vec<&'static str>,
    consumes: Vec<&'static str>,
    rpc: Vec<&'static str>,
    heuristic: bool,
}

fn node(name: &'static str, kind: NodeKind, package: &'static str) -> NodeSpec {
    NodeSpec {
        name,
        kind,
        package,
        signature: None,
        deps: Vec::new(),
        produces: Vec::new(),
        consumes: Vec::new(),
        rpc: Vec::new(),
        heuristic: false,
    }
}

impl NodeSpec {
    fn deps(mut self, deps: &[&'static str]) -> Self {
        self.deps = deps.to_vec();
        self
    }
    fn produces(mut self, topics: &[&'static str]) -> Self {
        self.produces = topics.to_vec();
        self
    }
    fn consumes(mut self, topics: &[&'static str]) -> Self {
        self.consumes = topics.to_vec();
        self
    }
    fn rpc(mut self, records: &[&'static str]) -> Self {
        self.rpc = records.to_vec();
        self
    }
    fn signature(mut self, sig: &'static str) -> Self {
        self.signature = Some(sig);
        self
    }
}

fn file(path: &str, repo: RepoId, nodes: Vec<NodeSpec>) -> FileSpec {
    FileSpec {
        path: path.to_string(),
        repo,
        nodes,
        interleave_rpc: false,
    }
}

fn fold(g: &mut ContractGraph, f: &FileSpec) {
    let mut ids = Vec::new();
    for (i, n) in f.nodes.iter().enumerate() {
        let id = g.add_node(ContractNode {
            id: 0,
            name: CompactStr::new(n.name),
            kind: n.kind,
            file_path: Path::new(&f.path).into(),
            line_start: i + 1,
            line_end: i + 1,
            package: CompactStr::new(n.package),
            repo_id: f.repo,
            signature: n.signature.map(CompactStr::new),
            docstring: None,
        });
        for d in &n.deps {
            g.add_dependency(id, d);
        }
        for t in &n.produces {
            g.add_producer(id, t);
        }
        for t in &n.consumes {
            g.add_consumer(id, t);
        }
        if !f.interleave_rpc {
            for r in &n.rpc {
                g.add_rpc_call(id, r);
            }
        }
        if n.heuristic {
            g.mark_heuristic_topic_node(id);
        }
        ids.push(id);
    }
    if f.interleave_rpc {
        let longest = f.nodes.iter().map(|n| n.rpc.len()).max().unwrap_or(0);
        for k in 0..longest {
            for (n, id) in f.nodes.iter().zip(&ids) {
                if let Some(r) = n.rpc.get(k) {
                    g.add_rpc_call(*id, r);
                }
            }
        }
    }
}

/// An incremental reload of one file: purge, then re-fold its new content
/// (`None`: the file was deleted).
fn reload(g: &mut ContractGraph, path: &str, new: Option<&FileSpec>) {
    g.patch_files(std::iter::once(Path::new(path)));
    if let Some(f) = new {
        fold(g, f);
    }
}

type EdgeRow = (EdgeKind, NodeId, NodeId, Option<CompactStr>, EdgeConfidence);

fn edge_rows(g: &ContractGraph) -> Vec<EdgeRow> {
    g.all_edges()
        .iter()
        .map(|e| (e.kind, e.from, e.to, e.metadata.clone(), e.confidence))
        .collect()
}

/// Reconciles `g` both ways, asserts they agree, and returns the delta's graph
/// (so a sequence of steps keeps exercising delta-after-delta) with its mode.
fn delta_vs_full(g: &ContractGraph) -> (ContractGraph, ReconcileMode) {
    let mut delta = g.clone();
    let mode = delta.reconcile_delta_within(1000);
    let mut full = g.clone();
    full.reconcile_edges();
    let (d, f) = (delta.canonical_lines(), full.canonical_lines());
    let only = |a: &[String], b: &[String]| -> Vec<String> {
        a.iter().filter(|l| !b.contains(l)).cloned().collect()
    };
    assert!(
        d == f,
        "canonical lines diverge ({mode:?})\nonly in delta: {:#?}\nonly in full: {:#?}",
        only(&d, &f),
        only(&f, &d)
    );
    assert_eq!(
        edge_rows(&delta),
        edge_rows(&full),
        "edge order diverges ({mode:?})"
    );
    assert_eq!(
        delta.handler_rpc_names, full.handler_rpc_names,
        "resolved handler names diverge ({mode:?})"
    );
    (delta, mode)
}

/// Applies `edits` one after the other, checking delta against full after each,
/// and asserting each one was reconciled incrementally as `expect` says.
fn run_steps(initial: &[FileSpec], edits: &[(&str, Option<FileSpec>, ReconcileMode)]) {
    let mut g = ContractGraph::new();
    for f in initial {
        fold(&mut g, f);
    }
    g.reconcile_edges();
    for (step, (path, new, expect)) in edits.iter().enumerate() {
        reload(&mut g, path, new.as_ref());
        let (next, mode) = delta_vs_full(&g);
        assert_eq!(mode, *expect, "step {step} ({path})");
        g = next;
    }
}

const DELTA: ReconcileMode = ReconcileMode::Delta {
    grpc_full: false,
    calls_full: false,
};
const DELTA_GRPC_FULL: ReconcileMode = ReconcileMode::Delta {
    grpc_full: true,
    calls_full: false,
};
const DELTA_CALLS_FULL: ReconcileMode = ReconcileMode::Delta {
    grpc_full: false,
    calls_full: true,
};

fn has_edge(g: &ContractGraph, kind: EdgeKind, from: &str, to: &str) -> bool {
    let name = |id: NodeId| g.get_node(id).map(|n| n.name.as_str());
    g.all_edges()
        .iter()
        .any(|e| e.kind == kind && name(e.from) == Some(from) && name(e.to) == Some(to))
}

#[test]
fn delta_tracks_a_declaration_others_import_being_removed_added_and_renamed() {
    let shared = |name: &'static str| {
        file(
            "/ws/a/Shared.java",
            0,
            vec![node(name, NodeKind::ServiceClass, "com.acme")],
        )
    };
    let importer = file(
        "/ws/b/Billing.java",
        1,
        vec![
            node("Billing", NodeKind::ServiceClass, "com.billing").deps(&[
                "com.acme.Shared",
                "Shared",
                "com.acme",
            ]),
        ],
    );
    let ts_lib = file(
        "/ws/c/lib/index.ts",
        2,
        vec![node("index", NodeKind::Module, "")],
    );
    let ts_user = file(
        "/ws/c/app.ts",
        2,
        vec![node("App", NodeKind::ServiceClass, "").deps(&["./lib/index", "./lib"])],
    );
    run_steps(
        &[shared("Shared"), importer.clone(), ts_lib.clone(), ts_user],
        &[
            // Removed: the importer's edges to it must go.
            ("/ws/a/Shared.java", Some(shared("Other")), DELTA),
            // Added back: they must come back.
            ("/ws/a/Shared.java", Some(shared("Shared")), DELTA),
            // A homonym in another repo makes the bare import ambiguous.
            (
                "/ws/d/Shared.go",
                Some(file(
                    "/ws/d/Shared.go",
                    3,
                    vec![node("Shared", NodeKind::ServiceClass, "")],
                )),
                DELTA,
            ),
            ("/ws/d/Shared.go", None, DELTA),
            // A relative import's target file deleted, then recreated.
            ("/ws/c/lib/index.ts", None, DELTA),
            ("/ws/c/lib/index.ts", Some(ts_lib), DELTA),
            // The importer itself edited: its facts change.
            (
                "/ws/b/Billing.java",
                Some(file(
                    "/ws/b/Billing.java",
                    1,
                    vec![node("Billing", NodeKind::ServiceClass, "com.billing").deps(&["Shared"])],
                )),
                DELTA,
            ),
            ("/ws/b/Billing.java", Some(importer), DELTA),
        ],
    );
}

#[test]
fn delta_tracks_producers_gaining_and_losing_topics() {
    let producer = |topics: &[&'static str]| {
        file(
            "/ws/a/pub.go",
            0,
            vec![node("Publisher", NodeKind::ServiceClass, "").produces(topics)],
        )
    };
    let consumer = file(
        "/ws/b/sub.go",
        1,
        vec![node("Subscriber", NodeKind::ServiceClass, "").consumes(&["orders.created"])],
    );
    let wildcard = file(
        "/ws/c/audit.go",
        2,
        vec![node("Audit", NodeKind::ServiceClass, "").consumes(&["orders.*"])],
    );
    let mut heuristic = node("Pattern", NodeKind::ServiceClass, "").produces(&["orders.created"]);
    heuristic.heuristic = true;
    let steps = vec![
        // Gains a topic only a wildcard subscription hears, then loses every
        // topic (its hubs must be removed), then gets them back.
        (
            "/ws/a/pub.go",
            Some(producer(&["orders.created", "orders.shipped"])),
            DELTA,
        ),
        ("/ws/a/pub.go", Some(producer(&[])), DELTA),
        ("/ws/a/pub.go", Some(producer(&["orders.created"])), DELTA),
        // The only consumer goes: the wildcard hub too.
        ("/ws/c/audit.go", None, DELTA),
        ("/ws/c/audit.go", Some(wildcard.clone()), DELTA),
        // A heuristic producer of the same topic, and an event-bus stream node
        // declared by a file (it becomes the topic's node).
        (
            "/ws/d/pattern.yml",
            Some(file("/ws/d/pattern.yml", 3, vec![heuristic])),
            DELTA,
        ),
        (
            "/ws/e/bus.ts",
            Some(file(
                "/ws/e/bus.ts",
                3,
                vec![node("Orders.Created", NodeKind::EventStream, "event-bus")],
            )),
            DELTA,
        ),
        ("/ws/e/bus.ts", None, DELTA),
    ];
    run_steps(&[producer(&["orders.created"]), consumer, wildcard], &steps);
}

#[test]
fn delta_tracks_an_rpc_call_target_appearing_and_disappearing() {
    let server = file(
        "/ws/a/server.go",
        0,
        vec![node("UserService", NodeKind::GrpcService, "demo.user")],
    );
    let caller = file(
        "/ws/b/client.go",
        1,
        vec![node("Client", NodeKind::ServiceClass, "demo.gw").rpc(&["UserService", "Ad"])],
    );
    let base = |service: &'static str| {
        file(
            "/ws/c/base.ts",
            2,
            vec![node("BaseCtrl", NodeKind::ServiceClass, "").rpc(&[service])],
        )
    };
    let sub = file(
        "/ws/d/sub.ts",
        2,
        vec![node("Ctrl", NodeKind::ServiceClass, "").rpc(&["^BaseCtrl#users.signUp"])],
    );
    let ad = file(
        "/ws/e/ad.go",
        3,
        vec![node("AdService", NodeKind::GrpcService, "demo.ad")],
    );
    run_steps(
        &[server.clone(), caller, base("#users=UserService"), sub],
        &[
            ("/ws/a/server.go", None, DELTA),
            ("/ws/a/server.go", Some(server), DELTA),
            // `Ad` resolves through the `<X>Service` retry once `AdService` exists.
            ("/ws/e/ad.go", Some(ad), DELTA),
            ("/ws/e/ad.go", None, DELTA),
            // The base class binding an inherited field changes, then goes.
            ("/ws/c/base.ts", Some(base("#users=AdService")), DELTA),
            ("/ws/c/base.ts", None, DELTA),
        ],
    );
}

#[test]
fn delta_tracks_a_handler_moving_files_and_its_constants_changing() {
    let proto = file(
        "/ws/p/user.proto",
        0,
        vec![
            node("UserService", NodeKind::GrpcService, "demo.user"),
            node("UserService.SignUp", NodeKind::GrpcMethod, "demo.user"),
            node("UserService.GetUser", NodeKind::GrpcMethod, "demo.user"),
        ],
    );
    let consts = |method: &'static str| {
        file(
            "/ws/a/consts.ts",
            1,
            vec![node("METHODS", NodeKind::ServiceClass, "").rpc(&["$SVC=UserService", method])],
        )
    };
    let handler = |path: &'static str| {
        file(
            path,
            1,
            vec![node("addUser", NodeKind::GrpcMethod, "")
                .signature("@GrpcMethod(SVC, METHODS.ADD)")
                .rpc(&["@SVC|METHODS.ADD", "~UserService.SignUp"])],
        )
    };
    let raw_handler = file(
        "/ws/b/get.ts",
        1,
        vec![node("get_user", NodeKind::GrpcMethod, "").signature("fn get_user()")],
    );
    run_steps(
        &[
            proto.clone(),
            consts("$METHODS.ADD=SignUp"),
            handler("/ws/b/one.ts"),
            raw_handler,
        ],
        &[
            // The handler moves: removed from one file, added in another.
            ("/ws/b/one.ts", None, DELTA),
            ("/ws/b/two.ts", Some(handler("/ws/b/two.ts")), DELTA),
            // Its decorator's constant now names another RPC, then an undeclared one.
            (
                "/ws/a/consts.ts",
                Some(consts("$METHODS.ADD=GetUser")),
                DELTA,
            ),
            (
                "/ws/a/consts.ts",
                Some(consts("$METHODS.ADD=Nothing")),
                DELTA,
            ),
            (
                "/ws/a/consts.ts",
                Some(consts("$METHODS.ADD=SignUp")),
                DELTA,
            ),
            // A proto method change rebuilds the gRPC families whole.
            (
                "/ws/p/user.proto",
                Some(file(
                    "/ws/p/user.proto",
                    0,
                    vec![
                        node("UserService", NodeKind::GrpcService, "demo.user"),
                        node("UserService.SignUp", NodeKind::GrpcMethod, "demo.user"),
                    ],
                )),
                DELTA_GRPC_FULL,
            ),
            ("/ws/p/user.proto", Some(proto), DELTA_GRPC_FULL),
        ],
    );
}

#[test]
fn delta_keeps_record_order_when_callers_calls_interleave() {
    let services = file(
        "/ws/a/server.go",
        0,
        vec![
            node("UserService", NodeKind::GrpcService, "demo.user"),
            node("AdService", NodeKind::GrpcService, "demo.ad"),
        ],
    );
    let mut callers = file(
        "/ws/b/client.go",
        1,
        vec![
            node("First", NodeKind::ServiceClass, "demo.gw").rpc(&["UserService", "AdService"]),
            node("Second", NodeKind::ServiceClass, "demo.gw").rpc(&["AdService", "UserService"]),
        ],
    );
    callers.interleave_rpc = true;
    let other = file(
        "/ws/c/other.go",
        2,
        vec![node("Third", NodeKind::ServiceClass, "").rpc(&["UserService"])],
    );
    run_steps(
        &[services.clone(), callers.clone()],
        &[
            // Interleaved records: `CallsRpc` is rebuilt in record order.
            ("/ws/c/other.go", Some(other), DELTA_CALLS_FULL),
            ("/ws/a/server.go", None, DELTA_CALLS_FULL),
            ("/ws/a/server.go", Some(services), DELTA_CALLS_FULL),
            // Once the interleaved file is gone, the delta applies again.
            ("/ws/b/client.go", None, DELTA),
        ],
    );
}

#[test]
fn delta_links_a_moved_handler_to_its_rpc() {
    let mut g = ContractGraph::new();
    fold(
        &mut g,
        &file(
            "/ws/p/user.proto",
            0,
            vec![node(
                "UserService.SignUp",
                NodeKind::GrpcMethod,
                "demo.user",
            )],
        ),
    );
    let handler = |path: &'static str| {
        file(
            path,
            1,
            vec![node("signUp", NodeKind::GrpcMethod, "").rpc(&["@'UserService'|'SignUp'"])],
        )
    };
    fold(&mut g, &handler("/ws/a/old.ts"));
    g.reconcile_edges();
    assert!(has_edge(
        &g,
        EdgeKind::Implements,
        "signUp",
        "UserService.SignUp"
    ));
    reload(&mut g, "/ws/a/old.ts", None);
    fold(&mut g, &handler("/ws/a/new.ts"));
    let (g, mode) = delta_vs_full(&g);
    assert_eq!(mode, DELTA);
    let moved = g.all_edges().iter().any(|e| {
        e.kind == EdgeKind::Implements
            && g.get_node(e.from)
                .is_some_and(|n| n.file_path.as_ref() == Path::new("/ws/a/new.ts"))
    });
    assert!(moved, "the moved handler implements its RPC");
}

#[test]
fn delta_falls_back_to_full_without_a_journal_or_after_a_hand_added_edge() {
    let mut g = ContractGraph::new();
    let a = g.add_node(ContractNode {
        id: 0,
        name: CompactStr::new("A"),
        kind: NodeKind::ServiceClass,
        file_path: Path::new("/ws/a.go").into(),
        line_start: 1,
        line_end: 1,
        package: CompactStr::new(""),
        repo_id: 0,
        signature: None,
        docstring: None,
    });
    // Never reconciled: nothing was journaled.
    assert_eq!(
        g.reconcile_delta(),
        ReconcileMode::Full("no change journal")
    );
    g.add_edge(ContractEdge {
        from: a,
        to: a,
        kind: EdgeKind::DispatchesTo,
        metadata: None,
        confidence: EdgeConfidence::Exact,
    });
    assert_eq!(
        g.reconcile_delta(),
        ReconcileMode::Full("no change journal")
    );
    assert_eq!(g.reconcile_delta(), DELTA);
}

#[test]
fn delta_falls_back_to_full_for_a_large_change() {
    let files: Vec<FileSpec> = (0..100)
        .map(|i| {
            file(
                &format!("/ws/f{i}.go"),
                0,
                vec![node("Svc", NodeKind::ServiceClass, "")],
            )
        })
        .collect();
    let mut g = ContractGraph::new();
    for f in &files {
        fold(&mut g, f);
    }
    g.reconcile_edges();
    for f in &files[..40] {
        reload(&mut g, &f.path, Some(f));
    }
    assert_eq!(
        g.reconcile_delta(),
        ReconcileMode::Full("changed set too large")
    );
}

/// Deterministic xorshift: no dependency, the same sequence on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
    fn some<T: Copy>(&mut self, items: &[T], max: usize) -> Vec<T> {
        (0..self.below(max + 1)).map(|_| self.pick(items)).collect()
    }
}

const NAMES: &[&str] = &[
    "Shared",
    "User",
    "UserService",
    "SignUp",
    "sign_up",
    "signUp",
    "GetUser",
    "index",
    "orders.created",
    "Orders",
    "AdService",
    "Ad",
    "Health",
    "BaseCtrl",
    "Ctrl",
    "METHODS",
];
const KINDS: &[NodeKind] = &[
    NodeKind::ServiceClass,
    NodeKind::ServiceClass,
    NodeKind::GrpcMethod,
    NodeKind::GrpcService,
    NodeKind::EventStream,
    NodeKind::KafkaTopic,
    NodeKind::Module,
];
const PACKAGES: &[&str] = &["", "", "com.acme", "demo.user", "event-bus"];
const DEPS: &[&str] = &[
    "com.acme.Shared",
    "Shared",
    "./index",
    "../lib/index",
    "./lib",
    "com.acme",
    "demo.user/UserService",
    "fs",
    "orders.created",
    "crate::acme::Shared",
    "UserService",
];
const TOPICS: &[&str] = &[
    "orders.created",
    "orders.*",
    "orders.>",
    "billing.done",
    "Streams.ORDERS_CREATED",
    "sensors/+/temp",
    "sensors/a/temp",
];
const RPC: &[&str] = &[
    "=UserService.SignUp",
    "~UserService.GetUser",
    "^BaseCtrl#userService.signUp",
    "^BaseCtrl#adClient.get",
    "#userService=UserService",
    "#userService=",
    "#adClient=AdService",
    "$SVC=UserService",
    "$METHODS.SIGN_UP=SignUp",
    "$METHODS.SIGN_UP=GetUser",
    "@SVC|METHODS.SIGN_UP",
    "@'UserService'|'GetUser'",
    "@'UserService'|~",
    "UserService",
    "Ad",
    "grpc.health.v1.Health",
    "Health",
    "demo.user.UserService",
];
const SIGNATURES: &[Option<&str>] = &[
    None,
    None,
    Some("@GrpcMethod('UserService', 'SignUp')"),
    Some("fn sign_up()"),
    Some("def get_user(self)"),
];
const PROTO_METHODS: &[&str] = &[
    "UserService.SignUp",
    "UserService.GetUser",
    "AdService.Get",
    "Health.Check",
];
const SLOTS: &[(&str, &str)] = &[
    ("svc0/index", "ts"),
    ("svc0/lib/index", "ts"),
    ("svc0/app", "ts"),
    ("svc1/user", "go"),
    ("svc1/base", "ts"),
    ("svc1/Shared", "java"),
    ("svc2/orders", "py"),
    ("svc2/ctrl", "ts"),
    ("svc2/bus", "ts"),
    ("proto/user", "proto"),
    ("proto/ad", "proto"),
    ("svc3/lib", "ts"),
];

fn random_file(rng: &mut Rng, slot: usize) -> FileSpec {
    let (stem, ext) = SLOTS[slot];
    let path = format!("/ws/{stem}.{ext}");
    let repo = (slot % 4) as RepoId;
    let mut nodes = Vec::new();
    if ext == "proto" {
        for _ in 0..=rng.below(2) {
            let package = rng.pick(&["demo.user", "demo.ad", "grpc.health.v1"]);
            nodes.push(node(rng.pick(PROTO_METHODS), NodeKind::GrpcMethod, package));
        }
        if rng.below(2) == 0 {
            nodes.push(node(
                rng.pick(&["UserService", "AdService"]),
                NodeKind::GrpcService,
                "demo.user",
            ));
        }
        return file(&path, repo, nodes);
    }
    for _ in 0..=rng.below(3) {
        let mut n = node(rng.pick(NAMES), rng.pick(KINDS), rng.pick(PACKAGES))
            .deps(&rng.some(DEPS, 2))
            .produces(&rng.some(TOPICS, 1))
            .consumes(&rng.some(TOPICS, 1))
            .rpc(&rng.some(RPC, 3));
        n.signature = rng.pick(SIGNATURES);
        n.heuristic = rng.below(4) == 0;
        nodes.push(n);
    }
    let mut f = file(&path, repo, nodes);
    f.interleave_rpc = rng.below(8) == 0;
    f
}

#[test]
fn delta_equals_full_over_seeded_random_edit_sequences() {
    let mut deltas = 0usize;
    let mut grpc_full = 0usize;
    let mut calls_full = 0usize;
    for seed in [0x9E37_79B9_7F4A_7C15u64, 42, 7, 1_000_003, 0xDEAD_BEEF] {
        let mut rng = Rng(seed);
        let mut g = ContractGraph::new();
        for slot in 0..SLOTS.len() {
            let f = random_file(&mut rng, slot);
            fold(&mut g, &f);
        }
        g.reconcile_edges();
        for step in 0..150 {
            // One to three files edited or deleted per reload.
            for _ in 0..=rng.below(3) {
                let slot = rng.below(SLOTS.len());
                let path = format!("/ws/{}.{}", SLOTS[slot].0, SLOTS[slot].1);
                if rng.below(6) == 0 {
                    reload(&mut g, &path, None);
                } else {
                    let f = random_file(&mut rng, slot);
                    reload(&mut g, &path, Some(&f));
                }
            }
            let (next, mode) = delta_vs_full(&g);
            assert!(
                !matches!(mode, ReconcileMode::Full(_)),
                "seed {seed} step {step}: {mode:?}"
            );
            match mode {
                ReconcileMode::Delta {
                    grpc_full: true, ..
                } => grpc_full += 1,
                ReconcileMode::Delta {
                    calls_full: true, ..
                } => calls_full += 1,
                ReconcileMode::Delta { .. } => deltas += 1,
                ReconcileMode::Full(_) => {}
            }
            g = next;
        }
    }
    // Every delta path ran, not only one of them.
    assert!(
        deltas > 100 && grpc_full > 10 && calls_full > 10,
        "{deltas} deltas, {grpc_full} gRPC-full, {calls_full} calls-full"
    );
}
