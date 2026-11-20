//! Calls layer: resolver rules, pruning, determinism, cycles and deep chains.

use super::*;
use std::sync::Arc;

fn path(p: &str) -> FilePath {
    Arc::from(Path::new(p))
}

fn node(name: &str, kind: NodeKind, file: &str, line: usize) -> ContractNode {
    ContractNode {
        id: 0,
        name: CompactStr::new(name),
        kind,
        file_path: path(file),
        line_start: line,
        line_end: line,
        package: CompactStr::new("app"),
        repo_id: 0,
        signature: None,
        docstring: None,
    }
}

fn callable(owner: Option<&str>, name: &str, start: u32, end: u32) -> CallableFact {
    CallableFact {
        name: CompactStr::new(name),
        owner: owner.map(CompactStr::new),
        line_start: start,
        line_end: end,
    }
}

fn site(caller: u32, callee: &str, receiver: CallReceiver, line: u32) -> CallFact {
    CallFact {
        caller,
        callee: CompactStr::new(callee),
        receiver,
        line,
    }
}

fn field(f: &str) -> CallReceiver {
    CallReceiver::InjectedField(CompactStr::new(f))
}

fn import(local: &str, module: &str) -> ImportFact {
    ImportFact {
        local: CompactStr::new(local),
        exported: CompactStr::new(local),
        module: CompactStr::new(module),
    }
}

fn class(name: &str, kind: TypeFactKind, implements: &[&str]) -> TypeFact {
    TypeFact {
        name: CompactStr::new(name),
        kind,
        extends: Vec::new(),
        implements: implements.iter().map(|s| CompactStr::new(*s)).collect(),
    }
}

fn inject(class: &str, f: &str, t: &str) -> InjectionFact {
    InjectionFact {
        class: CompactStr::new(class),
        field: CompactStr::new(f),
        declared_type: CompactStr::new(t),
    }
}

/// One file: a declaration node per callable (so the file has nodes), its facts,
/// plus extra nodes (consumers / producers / routes) registered as asked.
struct FileSpec {
    path: &'static str,
    facts: CallFacts,
    consumers: Vec<(usize, &'static str)>,
    producers: Vec<(usize, &'static str)>,
}

fn build(files: &[FileSpec]) -> ContractGraph {
    let mut g = ContractGraph::new();
    for f in files {
        g.add_call_facts(path(f.path), 0, CompactStr::new("app"), f.facts.clone());
        g.add_node(node("decl", NodeKind::ServiceClass, f.path, 1));
        for (line, topic) in &f.consumers {
            let id = g.add_node(node(topic, NodeKind::KafkaTopic, f.path, *line));
            g.add_consumer(id, topic);
        }
        for (line, topic) in &f.producers {
            let id = g.add_node(node(topic, NodeKind::KafkaTopic, f.path, *line));
            g.add_producer(id, topic);
        }
    }
    g.reconcile_edges();
    g
}

fn calls(g: &ContractGraph) -> Vec<(String, String, EdgeConfidence)> {
    g.call_edges()
        .iter()
        .map(|e| {
            let name = |id| g.get_node(id).map_or(String::new(), |n| n.name.to_string());
            (name(e.from), name(e.to), e.confidence)
        })
        .collect()
}

/// consumer -> service (injected, imported) -> notifier interface with two impls.
fn layered() -> Vec<FileSpec> {
    vec![
        FileSpec {
            path: "/w/src/consumer.ts",
            facts: CallFacts {
                callables: vec![callable(Some("Consumer"), "handle", 10, 12)],
                call_sites: vec![site(0, "run", field("svc"), 11)],
                injections: vec![inject("Consumer", "svc", "Service")],
                imports: vec![import("Service", "./service")],
                types: vec![class("Consumer", TypeFactKind::Class, &[])],
                capped: false,
            },
            consumers: vec![(10, "in.topic")],
            producers: vec![],
        },
        FileSpec {
            path: "/w/src/service.ts",
            facts: CallFacts {
                callables: vec![
                    callable(Some("Service"), "run", 5, 8),
                    callable(Some("Service"), "helper", 9, 11),
                ],
                call_sites: vec![
                    site(0, "helper", CallReceiver::This, 6),
                    site(1, "send", field("notifier"), 10),
                ],
                injections: vec![inject("Service", "notifier", "Notifier")],
                imports: vec![import("Notifier", "./notifiers")],
                types: vec![class("Service", TypeFactKind::Class, &[])],
                capped: false,
            },
            consumers: vec![],
            producers: vec![],
        },
        FileSpec {
            path: "/w/src/notifiers.ts",
            facts: CallFacts {
                callables: vec![
                    callable(Some("Email"), "send", 5, 7),
                    callable(Some("Sms"), "send", 10, 12),
                ],
                call_sites: vec![],
                injections: vec![],
                imports: vec![],
                types: vec![
                    class("Notifier", TypeFactKind::Interface, &[]),
                    class("Email", TypeFactKind::Class, &["Notifier"]),
                    class("Sms", TypeFactKind::Class, &["Notifier"]),
                ],
                capped: false,
            },
            consumers: vec![],
            producers: vec![(6, "email.queued"), (11, "sms.queued")],
        },
    ]
}

#[test]
fn injected_field_this_and_interface_dispatch() {
    let g = build(&layered());
    assert_eq!(
        calls(&g),
        vec![
            ("Consumer.handle".into(), "Service.run".into(), EdgeConfidence::Exact),
            ("Service.run".into(), "Service.helper".into(), EdgeConfidence::Exact),
            ("Service.helper".into(), "Email.send".into(), EdgeConfidence::Ambiguous),
            ("Service.helper".into(), "Sms.send".into(), EdgeConfidence::Ambiguous),
        ]
    );
    // Anchoring: the producer node at line 6 lies in Email.send.
    let producer = g
        .all_nodes()
        .find(|n| n.name == "email.queued" && n.file_path.as_ref() != Path::new("event-bus"))
        .cloned();
    let anchor = producer.as_ref().and_then(|p| g.enclosing_callable(p));
    assert_eq!(anchor.map(|c| c.name.as_str()), Some("Email.send"));
    // Callables are not in the reconciled node set nor its indices.
    assert!(g.all_nodes().all(|n| n.kind != NodeKind::Callable));
    assert!(g.search_symbols("Service.run", None).is_empty());
    assert!(g.search_symbols("run", None).iter().all(|n| n.kind != NodeKind::Callable));
}

#[test]
fn nothing_is_materialized_without_a_producer_on_the_path() {
    let mut files = layered();
    files[2].producers.clear();
    let g = build(&files);
    assert!(g.call_edges().is_empty());
    assert!(g.callable_nodes().is_empty());
    assert_eq!(g.call_stats().candidate_files, 0);
}

#[test]
fn single_implementation_is_exact_and_unknown_receivers_give_no_edge() {
    let mut files = layered();
    // Drop Sms: one implementation left.
    files[2].facts.types.pop();
    files[2].facts.callables.pop();
    files[2].producers.pop();
    // A call through an undeclared field and an unresolvable import resolve to nothing.
    files[1].facts.call_sites.push(site(0, "x", field("unknown"), 7));
    files[1].facts.call_sites.push(site(0, "f", CallReceiver::ImportedSymbol, 7));
    let g = build(&files);
    let c = calls(&g);
    assert!(c.contains(&("Service.helper".into(), "Email.send".into(), EdgeConfidence::Exact)));
    assert_eq!(c.len(), 3, "{c:?}");
}

#[test]
fn cyclic_injection_terminates_and_keeps_both_directions() {
    // A.post -> B.confirm -> A.post, A.post publishes.
    let files = vec![
        FileSpec {
            path: "/w/a.ts",
            facts: CallFacts {
                callables: vec![callable(Some("A"), "post", 3, 6), callable(Some("A"), "entry", 7, 9)],
                call_sites: vec![
                    site(0, "confirm", field("b"), 5),
                    site(1, "post", CallReceiver::This, 8),
                ],
                injections: vec![inject("A", "b", "B")],
                imports: vec![import("B", "./b")],
                types: vec![class("A", TypeFactKind::Class, &[])],
                capped: false,
            },
            consumers: vec![(7, "go")],
            producers: vec![(4, "posted")],
        },
        FileSpec {
            path: "/w/b.ts",
            facts: CallFacts {
                callables: vec![callable(Some("B"), "confirm", 3, 5)],
                call_sites: vec![site(0, "post", field("a"), 4)],
                injections: vec![inject("B", "a", "A")],
                imports: vec![import("A", "./a")],
                types: vec![class("B", TypeFactKind::Class, &[])],
                capped: false,
            },
            consumers: vec![],
            producers: vec![],
        },
    ];
    let g = build(&files);
    let c = calls(&g);
    assert!(c.contains(&("A.post".into(), "B.confirm".into(), EdgeConfidence::Exact)));
    assert!(c.contains(&("B.confirm".into(), "A.post".into(), EdgeConfidence::Exact)));
    assert!(c.contains(&("A.entry".into(), "A.post".into(), EdgeConfidence::Exact)));
}

#[test]
fn imported_function_through_a_barrel_and_same_module() {
    let files = vec![
        FileSpec {
            path: "/w/src/main.ts",
            facts: CallFacts {
                callables: vec![callable(None, "main", 3, 6), callable(None, "local", 7, 9)],
                call_sites: vec![
                    site(0, "local", CallReceiver::SameModule, 4),
                    site(1, "publish", CallReceiver::ImportedSymbol, 8),
                ],
                imports: vec![import("publish", "./lib")],
                ..CallFacts::default()
            },
            consumers: vec![(3, "start")],
            producers: vec![],
        },
        FileSpec {
            // `lib/index.ts`: `export * from './pub'`.
            path: "/w/src/lib/index.ts",
            facts: CallFacts {
                imports: vec![ImportFact {
                    local: CompactStr::new("*"),
                    exported: CompactStr::new("*"),
                    module: CompactStr::new("./pub"),
                }],
                ..CallFacts::default()
            },
            consumers: vec![],
            producers: vec![],
        },
        FileSpec {
            path: "/w/src/lib/pub.ts",
            facts: CallFacts {
                callables: vec![callable(None, "publish", 2, 4)],
                ..CallFacts::default()
            },
            consumers: vec![],
            producers: vec![(3, "out")],
        },
    ];
    let g = build(&files);
    assert_eq!(
        calls(&g),
        vec![
            ("main".into(), "local".into(), EdgeConfidence::Exact),
            ("local".into(), "publish".into(), EdgeConfidence::Exact),
        ]
    );
}

#[test]
fn workspace_package_imports_are_heuristic() {
    let mut files = layered();
    // The service is imported by package name, from a package `app` file.
    files[0].facts.imports = vec![import("Service", "@scope/app")];
    let g = build(&files);
    let c = calls(&g);
    assert!(c.contains(&("Consumer.handle".into(), "Service.run".into(), EdgeConfidence::Heuristic)));
}

#[test]
fn output_is_independent_of_insertion_order_and_stable_across_reloads() {
    let files = layered();
    let a = build(&files);
    let mut rev: Vec<FileSpec> = layered();
    rev.reverse();
    let b = build(&rev);
    let ca = a.canonical_lines().into_iter().filter(|l| l.contains("Calls") || l.contains("callable")).collect::<Vec<_>>();
    let cb = b.canonical_lines().into_iter().filter(|l| l.contains("Calls") || l.contains("callable")).collect::<Vec<_>>();
    assert!(!ca.is_empty());
    assert_eq!(ca, cb);

    // Re-folding one file through patch_files + delta reconcile ends where a full
    // rebuild does.
    let mut g = build(&files);
    let f = &files[1];
    g.patch_files([Path::new(f.path)]);
    g.add_call_facts(path(f.path), 0, CompactStr::new("app"), f.facts.clone());
    g.add_node(node("decl", NodeKind::ServiceClass, f.path, 1));
    g.reconcile_delta();
    assert_eq!(calls(&g), calls(&a));
    // Removing the interface file cuts every path.
    g.patch_files([Path::new("/w/src/notifiers.ts")]);
    g.reconcile_delta();
    assert!(g.call_edges().is_empty());
}

#[test]
fn deep_call_chain_is_walked_without_recursion_on_a_2mb_stack() {
    const DEPTH: u32 = 50_000;
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let callables = (0..DEPTH)
                .map(|k| callable(None, &format!("f{k}"), 2 * k + 1, 2 * k + 2))
                .collect();
            let call_sites = (0..DEPTH - 1)
                .map(|k| site(k, &format!("f{}", k + 1), CallReceiver::SameModule, 2 * k + 1))
                .collect();
            let files = vec![FileSpec {
                path: "/w/deep.ts",
                facts: CallFacts {
                    callables,
                    call_sites,
                    ..CallFacts::default()
                },
                consumers: vec![(1, "in")],
                producers: vec![(2 * DEPTH as usize, "out")],
            }];
            let g = build(&files);
            (g.callable_nodes().len(), g.call_edges().len())
        })
        .expect("spawn");
    let (nodes, edges) = handle.join().expect("no stack overflow");
    assert_eq!(nodes, DEPTH as usize);
    assert_eq!(edges, DEPTH as usize - 1);
}
