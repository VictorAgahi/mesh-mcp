//! Calls layer on Java, Go and Python: package scope, import resolution by path,
//! structural Go interfaces, constructor anchoring, caps, cycles and reloads.

use super::calls_tests::{
    build, callable, calls, class, field, import, inject, node, path, site, FileSpec,
};
use super::*;

const X: EdgeConfidence = EdgeConfidence::Exact;
const A: EdgeConfidence = EdgeConfidence::Ambiguous;

fn edge(from: &str, to: &str, c: EdgeConfidence) -> (String, String, EdgeConfidence) {
    (from.into(), to.into(), c)
}

fn spec(
    path: &'static str,
    facts: CallFacts,
    consumers: Vec<(usize, &'static str)>,
    producers: Vec<(usize, &'static str)>,
) -> FileSpec {
    FileSpec {
        path,
        facts,
        consumers,
        producers,
    }
}

fn iface(name: &str, methods: &[&str]) -> TypeFact {
    TypeFact {
        methods: methods.iter().map(|m| CompactStr::new(*m)).collect(),
        ..class(name, TypeFactKind::Interface, &[])
    }
}

fn sorted(mut v: Vec<(String, String, EdgeConfidence)>) -> Vec<(String, String, EdgeConfidence)> {
    v.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    v
}

// ---------------------------------------------------------------- Java

/// listener (package shop) -> BillingService (package shop.billing, imported by its
/// qualified name) -> Publisher (same package as the service, no import) -> impls.
fn spring(impls: usize) -> Vec<FileSpec> {
    let mut files = vec![
        spec(
            "/w/src/main/java/com/shop/Listener.java",
            CallFacts {
                callables: vec![callable(Some("Listener"), "onOrder", 10, 14)],
                call_sites: vec![site(0, "charge", field("billing"), 12)],
                injections: vec![inject("Listener", "billing", "BillingService")],
                imports: vec![import("BillingService", "com.shop.billing")],
                types: vec![class("Listener", TypeFactKind::Class, &[])],
                capped: false,
            },
            vec![(10, "orders")],
            vec![],
        ),
        spec(
            "/w/src/main/java/com/shop/billing/BillingService.java",
            CallFacts {
                callables: vec![callable(Some("BillingService"), "charge", 6, 9)],
                call_sites: vec![site(0, "send", field("publisher"), 8)],
                injections: vec![inject("BillingService", "publisher", "Publisher")],
                types: vec![class("BillingService", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![],
            vec![],
        ),
        spec(
            "/w/src/main/java/com/shop/billing/Publisher.java",
            CallFacts {
                types: vec![class("Publisher", TypeFactKind::Interface, &[])],
                ..CallFacts::default()
            },
            vec![],
            vec![],
        ),
        spec(
            "/w/src/main/java/com/shop/billing/KafkaPublisher.java",
            CallFacts {
                callables: vec![callable(Some("KafkaPublisher"), "send", 5, 8)],
                types: vec![class("KafkaPublisher", TypeFactKind::Class, &["Publisher"])],
                ..CallFacts::default()
            },
            vec![],
            vec![(6, "billed")],
        ),
    ];
    if impls > 1 {
        files.push(spec(
            "/w/src/main/java/com/shop/billing/AuditPublisher.java",
            CallFacts {
                callables: vec![callable(Some("AuditPublisher"), "send", 5, 8)],
                types: vec![class("AuditPublisher", TypeFactKind::Class, &["Publisher"])],
                ..CallFacts::default()
            },
            vec![],
            vec![(6, "audited")],
        ));
    }
    files
}

#[test]
fn java_qualified_import_same_package_and_interface_implementations() {
    let g = build(&spring(1));
    assert_eq!(
        sorted(calls(&g)),
        vec![
            edge("BillingService.charge", "KafkaPublisher.send", X),
            edge("Listener.onOrder", "BillingService.charge", X),
        ]
    );
    // Two implementations: each candidate ambiguous; the injected class stays exact.
    let g = build(&spring(2));
    assert_eq!(
        sorted(calls(&g)),
        vec![
            edge("BillingService.charge", "AuditPublisher.send", A),
            edge("BillingService.charge", "KafkaPublisher.send", A),
            edge("Listener.onOrder", "BillingService.charge", X),
        ]
    );
}

#[test]
fn java_wildcard_import_and_unimported_packages() {
    // `import com.shop.billing.*;` brings the service in; without any import, a type of
    // another package resolves to nothing.
    let mut files = spring(1);
    files[0].facts.imports = vec![import("*", "com.shop.billing")];
    let g = build(&files);
    assert!(calls(&g).contains(&edge("Listener.onOrder", "BillingService.charge", X)));
    files[0].facts.imports.clear();
    let g = build(&files);
    assert!(calls(&g).is_empty(), "{:?}", calls(&g));
}

#[test]
fn java_same_name_in_two_modules_resolves_to_the_closest() {
    // `com/shop/billing/BillingService` exists under two modules; the importer's own
    // module wins, a tie gives no edge (never a guess).
    let mut files = spring(1);
    let mut other = spring(1);
    for f in &mut other {
        f.path = match f.path {
            "/w/src/main/java/com/shop/Listener.java" => {
                "/w/other/src/main/java/com/shop/Listener.java"
            }
            "/w/src/main/java/com/shop/billing/BillingService.java" => {
                "/w/other/src/main/java/com/shop/billing/BillingService.java"
            }
            "/w/src/main/java/com/shop/billing/Publisher.java" => {
                "/w/other/src/main/java/com/shop/billing/Publisher.java"
            }
            _ => "/w/other/src/main/java/com/shop/billing/KafkaPublisher.java",
        };
    }
    files.extend(other);
    let g = build(&files);
    let c = sorted(calls(&g));
    // Two listeners, each bound to its own module's service: four edges, none crossed.
    assert_eq!(c.len(), 4, "{c:?}");
    let ids: Vec<(&str, &str)> = g
        .call_edges()
        .iter()
        .filter_map(|e| {
            let from = g.callable_nodes().iter().find(|n| n.id == e.from)?;
            let to = g.callable_nodes().iter().find(|n| n.id == e.to)?;
            let root = |p: &Path| {
                if p.starts_with("/w/other") {
                    "other"
                } else {
                    "main"
                }
            };
            Some((root(&from.file_path), root(&to.file_path)))
        })
        .collect();
    assert!(ids.iter().all(|(a, b)| a == b), "{ids:?}");
}

// ---------------------------------------------------------------- Go

/// consumer.go: `NewOrderConsumer` holds the topic (line 15), `Run` handles;
/// service.go: `Handle` publishes through the `Publisher` interface;
/// extra.go: another method of `OrderService`, declared in another file;
/// publisher.go: the implementations, `NewX` holding the topic.
fn go_files(impls: usize) -> Vec<FileSpec> {
    let mut publishers = CallFacts {
        callables: vec![
            callable(None, "NewKafkaPublisher", 18, 20),
            callable(Some("KafkaPublisher"), "Publish", 22, 24),
        ],
        call_sites: vec![site(1, "WriteMessages", field("writer"), 23)],
        injections: vec![inject("KafkaPublisher", "writer", "kafka.Writer")],
        types: vec![class("KafkaPublisher", TypeFactKind::Class, &[])],
        ..CallFacts::default()
    };
    let mut producers = vec![(19, "orders.priced")];
    if impls > 1 {
        publishers
            .callables
            .push(callable(None, "NewAuditPublisher", 30, 32));
        publishers
            .callables
            .push(callable(Some("AuditPublisher"), "Publish", 34, 36));
        publishers
            .call_sites
            .push(site(3, "WriteMessages", field("writer"), 35));
        publishers
            .injections
            .push(inject("AuditPublisher", "writer", "kafka.Writer"));
        publishers
            .types
            .push(class("AuditPublisher", TypeFactKind::Class, &[]));
        producers.push((31, "orders.audit"));
    }
    vec![
        spec(
            "/w/app/consumer.go",
            CallFacts {
                callables: vec![
                    callable(None, "NewOrderConsumer", 14, 17),
                    callable(Some("OrderConsumer"), "Run", 19, 29),
                ],
                call_sites: vec![
                    site(1, "ReadMessage", field("reader"), 21),
                    site(1, "Handle", field("svc"), 25),
                ],
                injections: vec![
                    inject("OrderConsumer", "reader", "kafka.Reader"),
                    inject("OrderConsumer", "svc", "OrderService"),
                ],
                types: vec![class("OrderConsumer", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![(15, "orders.placed")],
            vec![],
        ),
        spec(
            "/w/app/service.go",
            CallFacts {
                callables: vec![callable(Some("OrderService"), "Handle", 14, 18)],
                call_sites: vec![
                    site(0, "Audit", CallReceiver::This, 15),
                    site(0, "Publish", field("pub"), 16),
                    site(0, "format", CallReceiver::SameModule, 17),
                ],
                injections: vec![inject("OrderService", "pub", "Publisher")],
                types: vec![
                    class("OrderService", TypeFactKind::Class, &[]),
                    iface("Publisher", &["Publish"]),
                ],
                ..CallFacts::default()
            },
            vec![],
            vec![],
        ),
        // A method of OrderService in a file without the type.
        spec(
            "/w/app/extra.go",
            CallFacts {
                callables: vec![
                    callable(Some("OrderService"), "Audit", 5, 8),
                    callable(None, "format", 10, 12),
                ],
                ..CallFacts::default()
            },
            vec![],
            vec![],
        ),
        spec("/w/app/publisher.go", publishers, vec![], producers),
    ]
}

#[test]
fn go_constructor_anchoring_structural_interfaces_and_package_scope() {
    let g = build(&go_files(1));
    // The consumer sits in `NewOrderConsumer`; `Run` (it calls through fields) is the
    // entry. `Publisher` has one implementation: exact. `Audit` lives in another file
    // of the package, `format` is a package function: neither reaches a producer, so
    // callable pruning drops them.
    assert_eq!(
        sorted(calls(&g)),
        vec![
            edge("OrderConsumer.Run", "OrderService.Handle", X),
            edge("OrderService.Handle", "KafkaPublisher.Publish", X),
        ]
    );
    // Two structural implementations: both ambiguous.
    let g = build(&go_files(2));
    assert_eq!(
        sorted(calls(&g)),
        vec![
            edge("OrderConsumer.Run", "OrderService.Handle", X),
            edge("OrderService.Handle", "AuditPublisher.Publish", A),
            edge("OrderService.Handle", "KafkaPublisher.Publish", A),
        ]
    );
}

#[test]
fn go_method_in_another_file_and_package_function_are_resolved() {
    // `Audit` (extra.go) and `format` publish: they now lie on a path and are kept.
    let mut files = go_files(1);
    files[2].producers = vec![(6, "audited"), (11, "formatted")];
    let g = build(&files);
    let c = sorted(calls(&g));
    assert!(
        c.contains(&edge("OrderService.Handle", "OrderService.Audit", X)),
        "{c:?}"
    );
    assert!(
        c.contains(&edge("OrderService.Handle", "format", X)),
        "{c:?}"
    );
}

#[test]
fn go_interface_needs_every_method_and_is_dropped_past_the_cap() {
    // `Publisher` now wants Publish and Flush: KafkaPublisher has no Flush.
    let mut files = go_files(1);
    files[1].facts.types[1] = iface("Publisher", &["Publish", "Flush"]);
    let g = build(&files);
    assert!(calls(&g).iter().all(|c| c.1 != "KafkaPublisher.Publish"));
    // More than MAX_STRUCTURAL_IMPLS structs satisfy it: no edge, counted.
    let mut files = go_files(1);
    for k in 0..=MAX_STRUCTURAL_IMPLS {
        let name = format!("Impl{k}");
        files[3].facts.callables.push(callable(
            Some(&name),
            "Publish",
            100 + 3 * k as u32,
            101 + 3 * k as u32,
        ));
        files[3]
            .facts
            .types
            .push(class(&name, TypeFactKind::Class, &[]));
    }
    let g = build(&files);
    assert_eq!(g.call_stats().dispatch_overflow, 1);
    assert!(calls(&g).iter().all(|c| c.1 != "KafkaPublisher.Publish"));
    let mut health = crate::health::IndexHealth::default();
    health.set_call_stats(&g.call_stats());
    assert!(health.calls_truncated());
    assert!(health
        .render_summary(10)
        .contains("1 Go interface(s) with more than 64 implementations"));
}

#[test]
fn go_package_import_resolves_by_path_tail() {
    let resolved = |module: &str| {
        let main = spec(
            "/w/cmd/main.go",
            CallFacts {
                callables: vec![callable(None, "main", 5, 9)],
                call_sites: vec![site(0, "Save", CallReceiver::Namespace("store".into()), 7)],
                imports: vec![ImportFact {
                    local: "store".into(),
                    exported: "*".into(),
                    module: module.into(),
                }],
                ..CallFacts::default()
            },
            vec![(6, "start")],
            vec![],
        );
        let store = spec(
            "/w/internal/store/store.go",
            CallFacts {
                callables: vec![callable(None, "Save", 3, 6)],
                ..CallFacts::default()
            },
            vec![],
            vec![(4, "saved")],
        );
        calls(&build(&[main, store]))
    };
    assert_eq!(
        resolved("example.com/app/internal/store"),
        vec![edge("main", "Save", X)]
    );
    // A one-segment path is the standard library, never a workspace directory; a
    // path whose tail names no directory resolves to nothing.
    assert!(resolved("store").is_empty());
    assert!(resolved("example.com/app/other").is_empty());
}

// ---------------------------------------------------------------- Python

fn py_files(absolute: bool) -> Vec<FileSpec> {
    let (handler_mod, tasks_mod) = if absolute {
        ("pkg.handler", "pkg")
    } else {
        (".handler", ".")
    };
    vec![
        spec(
            "/w/src/pkg/consumer.py",
            CallFacts {
                callables: vec![callable(Some("Consumer"), "run", 5, 9)],
                call_sites: vec![site(0, "handle", field("_handler"), 8)],
                injections: vec![inject("Consumer", "_handler", "Handler")],
                imports: vec![import("Handler", handler_mod)],
                types: vec![class("Consumer", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![(6, "orders")],
            vec![],
        ),
        spec(
            "/w/src/pkg/handler.py",
            CallFacts {
                callables: vec![callable(Some("Handler"), "handle", 4, 8)],
                call_sites: vec![
                    site(0, "run", CallReceiver::Namespace("tasks".into()), 5),
                    site(0, "build", CallReceiver::ImportedSymbol, 6),
                ],
                imports: vec![
                    import("tasks", tasks_mod),
                    ImportFact {
                        local: "build".into(),
                        exported: "build_report".into(),
                        module: if absolute {
                            "pkg.report".into()
                        } else {
                            ".report".into()
                        },
                    },
                ],
                types: vec![class("Handler", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![],
            vec![],
        ),
        spec(
            "/w/src/pkg/tasks.py",
            CallFacts {
                callables: vec![callable(None, "run", 2, 4)],
                ..CallFacts::default()
            },
            vec![],
            vec![(3, "task.done")],
        ),
        spec(
            "/w/src/pkg/report.py",
            CallFacts {
                callables: vec![callable(None, "build_report", 2, 4)],
                ..CallFacts::default()
            },
            vec![],
            vec![(3, "report.ready")],
        ),
    ]
}

#[test]
fn python_relative_and_absolute_imports_modules_and_functions() {
    for absolute in [false, true] {
        let g = build(&py_files(absolute));
        assert_eq!(
            sorted(calls(&g)),
            vec![
                edge("Consumer.run", "Handler.handle", X),
                edge("Handler.handle", "build_report", X),
                edge("Handler.handle", "run", X),
            ],
            "absolute: {absolute}"
        );
    }
}

#[test]
fn python_package_init_reexport_and_unresolved_imports() {
    // `from pkg import Handler` where `pkg/__init__.py` re-exports it from `.handler`.
    let mut files = py_files(true);
    files[0].facts.imports = vec![import("Handler", "pkg")];
    files.push(spec(
        "/w/src/pkg/__init__.py",
        CallFacts {
            imports: vec![import("Handler", ".handler")],
            ..CallFacts::default()
        },
        vec![],
        vec![],
    ));
    let g = build(&files);
    assert!(calls(&g).contains(&edge("Consumer.run", "Handler.handle", X)));
    // A module nobody provides resolves to nothing.
    let mut files = py_files(true);
    files[0].facts.imports = vec![import("Handler", "elsewhere.handler")];
    assert!(!calls(&build(&files)).iter().any(|c| c.0 == "Consumer.run"));
}

// ---------------------------------------------------------------- shared guarantees

#[test]
fn cyclic_injection_and_embedding_terminate_on_a_2mb_stack() {
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            // Java: A <-> B inject each other in one package; Go: structs embed each
            // other and an interface embeds itself.
            let java = vec![
                spec(
                    "/w/j/A.java",
                    CallFacts {
                        callables: vec![
                            callable(Some("A"), "entry", 3, 5),
                            callable(Some("A"), "post", 7, 10),
                        ],
                        call_sites: vec![
                            site(0, "post", CallReceiver::This, 4),
                            site(1, "confirm", field("b"), 8),
                        ],
                        injections: vec![inject("A", "b", "B")],
                        types: vec![class("A", TypeFactKind::Class, &[])],
                        ..CallFacts::default()
                    },
                    vec![(3, "go")],
                    vec![(9, "posted")],
                ),
                spec(
                    "/w/j/B.java",
                    CallFacts {
                        callables: vec![callable(Some("B"), "confirm", 3, 6)],
                        call_sites: vec![site(0, "post", field("a"), 4)],
                        injections: vec![inject("B", "a", "A")],
                        types: vec![class("B", TypeFactKind::Class, &[])],
                        ..CallFacts::default()
                    },
                    vec![],
                    vec![],
                ),
            ];
            let go = vec![spec(
                "/w/g/a.go",
                CallFacts {
                    callables: vec![
                        callable(Some("A"), "Run", 5, 8),
                        callable(Some("B"), "Stop", 10, 12),
                    ],
                    call_sites: vec![
                        site(0, "Stop", field("b"), 6),
                        site(0, "Run", field("b"), 7),
                    ],
                    injections: vec![inject("A", "b", "I")],
                    types: vec![
                        TypeFact {
                            extends: vec!["B".into()],
                            ..class("A", TypeFactKind::Class, &[])
                        },
                        TypeFact {
                            extends: vec!["A".into()],
                            ..class("B", TypeFactKind::Class, &[])
                        },
                        TypeFact {
                            extends: vec!["I".into()],
                            ..iface("I", &["Stop", "Run"])
                        },
                    ],
                    ..CallFacts::default()
                },
                vec![(6, "in")],
                vec![(11, "out")],
            )];
            (calls(&build(&java)).len(), calls(&build(&go)).len())
        })
        .expect("spawn");
    let (java, go) = handle.join().expect("no stack overflow");
    assert_eq!(java, 3);
    assert!(go >= 1);
}

#[test]
fn output_is_independent_of_insertion_order_and_repeats() {
    let lines = |files: &[FileSpec]| {
        build(files)
            .canonical_lines()
            .into_iter()
            .filter(|l| l.contains("Calls") || l.contains("callable"))
            .collect::<Vec<_>>()
    };
    let sets: [fn() -> Vec<FileSpec>; 3] = [|| spring(2), || go_files(2), || py_files(true)];
    for make in sets {
        let first = lines(&make());
        assert!(!first.is_empty());
        let mut rev = make();
        rev.reverse();
        assert_eq!(first, lines(&rev));
        for _ in 0..5 {
            assert_eq!(first, lines(&make()));
        }
    }
}

#[test]
fn live_reloads_of_an_implementation_file_match_a_fresh_build() {
    fn refold(g: &mut ContractGraph, f: &FileSpec) {
        g.add_call_facts(path(f.path), 0, CompactStr::new("app"), f.facts.clone());
        g.add_node(node("decl", NodeKind::ServiceClass, f.path, 1));
        for (line, topic) in &f.producers {
            let id = g.add_node(node(topic, NodeKind::KafkaTopic, f.path, *line));
            g.add_producer(id, topic);
        }
    }
    // Java: the audit implementation (index 4) is deleted, then renamed.
    let initial = build(&spring(2));
    assert!(calls(&initial).iter().any(|c| c.1 == "AuditPublisher.send"));
    let mut live = build(&spring(2));
    live.patch_files([Path::new(
        "/w/src/main/java/com/shop/billing/AuditPublisher.java",
    )]);
    live.reconcile_delta();
    assert_eq!(calls(&live), calls(&build(&spring(1))));
    assert!(calls(&live).iter().all(|c| c.1 != "AuditPublisher.send"));

    let mut live = build(&spring(2));
    let mut renamed = spring(2);
    renamed[4].facts.callables[0].name = CompactStr::new("deliver");
    live.patch_files([Path::new(renamed[4].path)]);
    refold(&mut live, &renamed[4]);
    live.reconcile_delta();
    assert_eq!(calls(&live), calls(&build(&renamed)));

    // Go: the second Publisher implementation (publisher.go holds both) changes: the
    // interface keeps one implementation and its edge turns exact.
    let mut live = build(&go_files(2));
    let one = go_files(1);
    live.patch_files([Path::new(one[3].path)]);
    refold(&mut live, &one[3]);
    live.reconcile_delta();
    assert_eq!(calls(&live), calls(&build(&one)));
    assert!(calls(&live).contains(&edge("OrderService.Handle", "KafkaPublisher.Publish", X)));
}

#[test]
fn call_facts_round_trip_through_compact_json() {
    let facts = CallFacts {
        callables: vec![callable(Some("A"), "m", 1, 2), callable(None, "f", 3, 4)],
        call_sites: vec![
            site(0, "x", CallReceiver::This, 1),
            site(0, "y", field("dep"), 2),
            site(1, "z", CallReceiver::ImportedSymbol, 3),
            site(1, "w", CallReceiver::SameModule, 4),
            site(1, "q", CallReceiver::Namespace("ns".into()), 5),
        ],
        injections: vec![inject("A", "dep", "Dep")],
        imports: vec![import("Dep", "pkg.dep")],
        types: vec![iface("I", &["M"]), class("A", TypeFactKind::Class, &["I"])],
        capped: true,
    };
    let json = serde_json::to_string(&facts).expect("serialize");
    let back: CallFacts = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(facts, back);
    // Arrays, not field-name maps: the cache holds every file's record at once.
    assert!(json.contains(r#"[0,"y",1,"dep",2]"#), "{json}");
    assert!(json.contains(r#"["m","A",1,2]"#), "{json}");
    // A record of an unknown kind is an error, never a silent receiver.
    assert!(serde_json::from_str::<CallFacts>(r#"{"call_sites":[[0,"x",9,"",1]]}"#).is_err());
}
