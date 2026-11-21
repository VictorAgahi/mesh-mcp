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

// ---------------------------------------------------------------- hop and pair caps

/// `depth + 1` classes `C0 .. C<depth>` in one file, each extending the next: `C0.run`
/// is the entry (it calls `this.post`), `C<depth>.post` publishes. In Java the types
/// are classes of one package, in Go structs embedding each other in one package.
fn chain_files(depth: usize, path: &'static str) -> Vec<FileSpec> {
    let mut callables = vec![
        callable(Some("C0"), "run", 3, 5),
        callable(Some(&format!("C{depth}")), "post", 8, 10),
    ];
    callables[0].line_start = 3;
    let types = (0..=depth)
        .map(|k| TypeFact {
            extends: if k < depth {
                vec![CompactStr::new(format!("C{}", k + 1))]
            } else {
                Vec::new()
            },
            ..class(&format!("C{k}"), TypeFactKind::Class, &[])
        })
        .collect();
    vec![spec(
        path,
        CallFacts {
            callables,
            call_sites: vec![site(0, "post", CallReceiver::This, 4)],
            types,
            ..CallFacts::default()
        },
        vec![(3, "in")],
        vec![(9, "out")],
    )]
}

#[test]
fn a_base_class_or_embedding_chain_deeper_than_the_hop_cap_is_counted() {
    for path in ["/w/j/Chain.java", "/w/g/chain.go"] {
        // Within the cap the method is found through every base.
        let g = build(&chain_files(MAX_TYPE_HOPS - 2, path));
        assert_eq!(g.call_stats().hops_capped, 0, "{path}");
        assert_eq!(
            calls(&g),
            vec![edge("C0.run", &format!("C{}.post", MAX_TYPE_HOPS - 2), X)]
        );
        // Past it the call is cut, counted, and reaches IndexHealth and the doctor summary.
        let g = build(&chain_files(MAX_TYPE_HOPS + 4, path));
        assert!(g.call_stats().hops_capped > 0, "{path}");
        assert!(calls(&g).is_empty(), "{path}");
        let mut health = crate::health::IndexHealth::default();
        health.set_call_stats(&g.call_stats());
        assert!(health.calls_truncated());
        assert!(
            health.render_summary(10).contains("cut at 16 hops"),
            "{path}"
        );
    }
}

#[test]
fn go_interface_matching_past_the_pair_cap_is_counted() {
    let mut g = ContractGraph::new();
    g.structural_pair_budget = 1;
    for f in go_files(2) {
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
    // `Publisher` has two candidate structs: over the cap of one pair, left unmatched.
    assert_eq!(g.call_stats().pairs_capped, 1);
    assert!(calls(&g).iter().all(|c| !c.1.ends_with(".Publish")));
    let mut health = crate::health::IndexHealth::default();
    health.set_call_stats(&g.call_stats());
    assert!(health.calls_truncated());
    assert!(health
        .render_summary(10)
        .contains("1 Go interface(s) not matched against struct types"));
    // The default cap does not bind this fixture.
    assert_eq!(build(&go_files(2)).call_stats().pairs_capped, 0);
}

// ---------------------------------------------------------------- Go: quadratic probe

/// One Go package: `Consumer.Run` (the entry) calls `T.m0`, which calls `m1`, ... `m2999`
/// through `this`; the even methods also call through a field; `NewT` holds 50 producer
/// sites (a client built in the constructor). 8,000 call sites in all.
fn big_go_package() -> Vec<FileSpec> {
    const METHODS: u32 = 3_000;
    let mut callables = vec![callable(None, "NewT", 1, 3)];
    let mut call_sites = Vec::new();
    for k in 0..METHODS {
        let line = 10 + 4 * k;
        callables.push(callable(Some("T"), &format!("m{k}"), line, line + 3));
        let me = k + 1;
        if k + 1 < METHODS {
            call_sites.push(site(
                me,
                &format!("m{}", k + 1),
                CallReceiver::This,
                line + 1,
            ));
        }
        if k % 2 == 0 {
            call_sites.push(site(me, "WriteMessages", field("w"), line + 2));
        }
        // Filler: package functions that do not exist.
        call_sites.push(site(me, "helperA", CallReceiver::SameModule, line + 2));
        if k % 2 == 1 {
            call_sites.push(site(me, "helperB", CallReceiver::SameModule, line + 2));
        }
    }
    while call_sites.len() < 8_000 {
        call_sites.push(site(
            1,
            &format!("filler{}", call_sites.len()),
            CallReceiver::SameModule,
            12,
        ));
    }
    let producers: Vec<(usize, &'static str)> = (0..50)
        .map(|k| (2, &*Box::leak(format!("t{k}").into_boxed_str())))
        .collect();
    vec![
        spec(
            "/w/g/t.go",
            CallFacts {
                callables,
                call_sites,
                injections: vec![inject("T", "w", "kafka.Writer")],
                types: vec![class("T", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![],
            producers,
        ),
        spec(
            "/w/g/consumer.go",
            CallFacts {
                callables: vec![callable(Some("Consumer"), "Run", 5, 8)],
                call_sites: vec![site(0, "m0", field("t"), 6)],
                injections: vec![inject("Consumer", "t", "T")],
                types: vec![class("Consumer", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![(6, "in")],
            vec![],
        ),
    ]
}

#[test]
fn a_constructor_with_thousands_of_methods_and_many_producer_sites_stays_linear() {
    let files = big_go_package();
    assert!(files[0].facts.call_sites.len() >= 8_000);
    let started = std::time::Instant::now();
    let g = build(&files);
    let took = started.elapsed();
    // Run -> m0 -> m1 -> ... -> m2998 (the last even method publishes): m2999 is a dead end.
    let edges = calls(&g);
    assert_eq!(edges.len(), 2_999, "{}", edges.len());
    assert!(edges.contains(&edge("Consumer.Run", "T.m0", X)));
    assert!(edges.contains(&edge("T.m2997", "T.m2998", X)));
    assert!(edges.iter().all(|e| e.1 != "T.m2999"));
    // The pre-fix scan (50 constructor sites x 3,000 methods x 8,000 call sites) took 830 ms
    // in release here; the fixed one 10 ms. The bounds leave a loaded CI runner room.
    let bound = if cfg!(debug_assertions) { 3_000 } else { 250 };
    assert!(took.as_millis() < bound, "{took:?}");
}

// ---------------------------------------------------------------- call-site pruning

fn refold_spec(g: &mut ContractGraph, f: &FileSpec) {
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

fn sites_in(g: &ContractGraph, file: &str) -> usize {
    g.call_facts
        .get(Path::new(file))
        .map_or(usize::MAX, |f| f.facts.call_sites.len())
}

#[test]
fn files_off_every_path_keep_no_call_sites_and_come_back_when_a_path_reaches_them() {
    // The consumer imports a module nobody provides: `handler.py` lies on no path.
    let mut off = py_files(true);
    off[0].facts.imports = vec![import("Handler", "elsewhere.handler")];
    let mut g = build(&off);
    assert!(g.call_edges().is_empty());
    let handler = "/w/src/pkg/handler.py";
    assert_eq!(sites_in(&g, handler), 0);
    assert_eq!(sites_in(&g, "/w/src/pkg/consumer.py"), 0);
    assert!(g.call_stats().files_pruned >= 2);
    assert!(g.call_files_missing_sites().is_empty());
    // Its callables, imports and types stay: they decide which files lie on a path.
    let kept = &g.call_facts[Path::new(handler)].facts;
    assert_eq!(kept.callables.len(), 1);
    assert_eq!(kept.imports.len(), 2);

    // The consumer now imports it: the file is on a path, its sites are asked for.
    let on = py_files(true);
    g.patch_files([Path::new(on[0].path)]);
    refold_spec(&mut g, &on[0]);
    g.reconcile_delta();
    assert_eq!(
        g.call_files_missing_sites(),
        &[path(handler)],
        "{:?}",
        calls(&g)
    );
    assert_eq!(g.call_stats().missing_sites, 1);
    let mut health = crate::health::IndexHealth::default();
    health.set_call_stats(&g.call_stats());
    assert!(health.calls_truncated());
    // The caller re-extracts the file and stores its full facts.
    g.patch_files([Path::new(handler)]);
    refold_spec(&mut g, &on[1]);
    g.reconcile_delta();
    assert!(g.call_files_missing_sites().is_empty());
    assert_eq!(calls(&g), calls(&build(&on)));
    assert_eq!(sites_in(&g, handler), 2);
    // The candidate files keep their sites.
    assert_eq!(sites_in(&g, "/w/src/pkg/consumer.py"), 1);
}

#[test]
fn a_graph_without_an_entry_or_a_producer_prunes_every_java_go_and_python_file() {
    let mut files = py_files(true);
    for f in &mut files {
        f.consumers.clear();
    }
    let g = build(&files);
    assert!(files.iter().all(|f| sites_in(&g, f.path) == 0));
    // TypeScript facts are kept whole.
    let ts = build(&[spec(
        "/w/src/a.ts",
        CallFacts {
            callables: vec![callable(None, "f", 1, 3)],
            call_sites: vec![site(0, "g", CallReceiver::SameModule, 2)],
            ..CallFacts::default()
        },
        vec![],
        vec![],
    )]);
    assert_eq!(sites_in(&ts, "/w/src/a.ts"), 1);
}

#[test]
fn an_inherited_field_guess_is_never_exact() {
    // `Sub` extends `Base`, which declares `bus`; `Sub.run` calls `bus.send()` bare.
    let files = vec![
        spec(
            "/w/j/Sub.java",
            CallFacts {
                callables: vec![callable(Some("Sub"), "run", 3, 6)],
                call_sites: vec![site(
                    0,
                    "send",
                    CallReceiver::InheritedField("bus".into()),
                    4,
                )],
                types: vec![TypeFact {
                    extends: vec!["Base".into()],
                    ..class("Sub", TypeFactKind::Class, &[])
                }],
                ..CallFacts::default()
            },
            vec![(3, "in")],
            vec![],
        ),
        spec(
            "/w/j/Base.java",
            CallFacts {
                injections: vec![inject("Base", "bus", "Bus")],
                types: vec![class("Base", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![],
            vec![],
        ),
        spec(
            "/w/j/Bus.java",
            CallFacts {
                callables: vec![callable(Some("Bus"), "send", 2, 5)],
                types: vec![class("Bus", TypeFactKind::Class, &[])],
                ..CallFacts::default()
            },
            vec![],
            vec![(3, "out")],
        ),
    ];
    let g = build(&files);
    assert_eq!(
        calls(&g),
        vec![edge("Sub.run", "Bus.send", EdgeConfidence::Heuristic)]
    );
    assert!(g.call_edges()[0]
        .metadata
        .as_deref()
        .is_some_and(|m| m.starts_with("inherited_field")));
}
