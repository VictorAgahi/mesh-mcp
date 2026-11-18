use super::*;

#[test]
fn declared_as_shows_only_what_was_written() {
    assert_eq!(ContractGraph::declared_as("S|M").as_deref(), Some("S, M"));
    assert_eq!(
        ContractGraph::declared_as("S|'Derived'|~").as_deref(),
        Some("S")
    );
    assert_eq!(ContractGraph::declared_as("|M").as_deref(), Some("M"));
    // `@GrpcMethod()`: nothing was written, so nothing is shown.
    assert_eq!(ContractGraph::declared_as("|'Derived'|~"), None);
}

#[test]
fn served_methods_of_unresolved_handlers_ignore_the_marker() {
    let mut graph = ContractGraph::new();
    graph.rpc_calls.push((
        1,
        format!("{HANDLER_REF_PREFIX}'UserService'|'AddBadge'|~").into(),
    ));
    graph.rpc_calls.push((
        2,
        format!("{HANDLER_REF_PREFIX}UNKNOWN_SERVICE|'AddBadge'|~").into(),
    ));
    graph
        .rpc_calls
        .push((3, format!("{HANDLER_REF_PREFIX}'S'|'Other'").into()));
    let served = graph.handler_served_methods();
    let method = fold_rpc_name("AddBadge");
    assert_eq!(
        served.get(&1),
        Some(&(Some("UserService".to_string()), method.clone()))
    );
    assert_eq!(served.get(&2), Some(&(None, method)));
    assert_eq!(
        served.get(&3),
        Some(&(Some("S".to_string()), fold_rpc_name("Other")))
    );
}

#[test]
fn a_package_qualifier_picks_one_of_two_same_named_rpcs() {
    let node = |name: &str, pkg: &str, kind: NodeKind| ContractNode {
        id: 0,
        name: CompactStr::new(name),
        kind,
        file_path: FilePath::from(Path::new("f")),
        line_start: 1,
        line_end: 1,
        package: CompactStr::new(pkg),
        repo_id: 0,
        signature: None,
        docstring: None,
    };
    let mut graph = ContractGraph::new();
    let proto_a = graph.add_node(node("UserService.Get", "a.v1", NodeKind::GrpcMethod));
    let proto_b = graph.add_node(node("UserService.Get", "b.v1", NodeKind::GrpcMethod));
    let qualified = graph.add_node(node("h1", "", NodeKind::ServiceClass));
    let bare = graph.add_node(node("h2", "", NodeKind::ServiceClass));
    graph.rpc_calls.push((
        qualified,
        format!("{HANDLER_REF_PREFIX}'a.v1.UserService'|'Get'").into(),
    ));
    graph.rpc_calls.push((
        bare,
        format!("{HANDLER_REF_PREFIX}'UserService'|'Get'").into(),
    ));
    let methods = vec![
        (proto_a, CompactStr::new("UserService.Get")),
        (proto_b, CompactStr::new("UserService.Get")),
    ];
    let resolved = graph.resolve_handler_rpc_names(&methods);
    assert_eq!(
        resolved.get(&qualified).map(CompactStr::as_str),
        Some("UserService.Get")
    );
    // Without a qualifier the name is declared twice: ambiguous, left out.
    assert!(!resolved.contains_key(&bare));
}

#[test]
fn the_marker_is_never_part_of_the_method() {
    let parsed = ContractGraph::parse_handler_ref("UNKNOWN|'AddBadge'|~").expect("parse");
    assert_eq!(parsed.service, "UNKNOWN");
    assert_eq!(parsed.method, "'AddBadge'");
    assert!(!parsed.method_written);
    let written = ContractGraph::parse_handler_ref("'S'|'M'").expect("parse");
    assert!(written.method_written);
}
