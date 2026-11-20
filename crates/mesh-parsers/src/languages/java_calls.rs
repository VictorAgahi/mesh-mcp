//! Java call facts for the 9.0 calls layer (Spring and plain Java): classes,
//! interfaces, enums and records with their heritage, methods, the fields a class
//! holds dependencies in (constructor injection, `@Autowired` / `@Inject` fields and
//! Lombok-style final fields all end up as a typed field, which is what is read),
//! imports, and the calls the resolver can follow. Resolution happens in
//! `mesh_core::contracts::calls`, where a type name resolves through an import or the
//! package (directory) of the file.
//!
//! Recorded on purpose, and nothing else: `this.m()` and `m()` on the enclosing class,
//! `field.m()` / `this.field.m()` on a field of the class (or of a class it extends)
//! that no parameter or local shadows. Static calls, calls on locals, parameters and
//! call chains are dropped.
//!
//! Every walk is iterative, every list is capped per file and a hit cap sets
//! `CallFacts::capped` (counted in `IndexHealth`).

use super::call_support::{collect_sites, prune_unused};
use super::ts_calls::{text, walk, Builder, MAX_DECLS_PER_FILE};
use mesh_core::{CallFacts, CallReceiver, CompactStr, TypeFact, TypeFactKind};
use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

fn is_type_decl(kind: &str) -> bool {
    matches!(
        kind,
        "class_declaration" | "interface_declaration" | "enum_declaration" | "record_declaration"
    )
}

/// `Foo`, `Foo<Bar>`, `a.b.Foo` -> `Foo`; primitives, arrays and anything else -> `None`.
fn type_name(node: Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        "type_identifier" => text(node, source).map(str::to_string),
        "generic_type" => type_name(node.named_child(0)?, source),
        "scoped_type_identifier" => {
            let t = text(node, source)?;
            let last = t.rsplit('.').next()?.trim();
            let last = last.split('<').next()?.trim();
            (!last.is_empty()).then(|| last.to_string())
        }
        _ => None,
    }
}

/// Names of the types of a `type_list` / `superclass` node.
fn type_list(node: Node, source: &[u8], out: &mut Vec<CompactStr>) {
    let mut cursor = node.walk();
    for t in node.named_children(&mut cursor) {
        if t.kind() == "type_list" {
            type_list(t, source, out);
        } else if let Some(n) = type_name(t, source) {
            out.push(CompactStr::new(n));
        }
    }
}

/// The class, interface, enum or record whose body directly holds `member`; `None`
/// for a member of an anonymous or local class (it has no name a call could use).
fn owner_name<'s>(member: Node, source: &'s [u8]) -> Option<&'s str> {
    let mut current = member.parent();
    while let Some(n) = current {
        let kind = n.kind();
        if is_type_decl(kind) {
            return text(n.child_by_field_name("name")?, source);
        }
        match kind {
            "class_body" | "interface_body" | "enum_body" | "enum_body_declarations" => {
                current = n.parent();
            }
            _ => return None,
        }
    }
    None
}

fn heritage(node: Node, source: &[u8]) -> (Vec<CompactStr>, Vec<CompactStr>) {
    let mut extends = Vec::new();
    let mut implements = Vec::new();
    if let Some(sc) = node.child_by_field_name("superclass") {
        type_list(sc, source, &mut extends);
    }
    if let Some(ifaces) = node.child_by_field_name("interfaces") {
        type_list(ifaces, source, &mut implements);
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "extends_interfaces" {
            type_list(child, source, &mut extends);
        }
    }
    (extends, implements)
}

fn has_modifier(node: Node, source: &[u8], word: &str) -> bool {
    let mut cursor = node.walk();
    let found = node.children(&mut cursor).any(|c| {
        c.kind() == "modifiers"
            && text(c, source).is_some_and(|t| t.split_whitespace().any(|w| w == word))
    });
    found
}

/// The call facts of one parsed Java file.
pub fn extract(tree: &Tree, source: &[u8]) -> CallFacts {
    let root = tree.root_node();
    let mut b = Builder::new();

    walk(root, &mut |node| match node.kind() {
        "import_declaration" => {
            let Some(t) = text(node, source) else { return };
            let t = t
                .trim()
                .trim_start_matches("import")
                .trim()
                .trim_end_matches(';')
                .trim();
            if t.starts_with("static ") || t.is_empty() {
                return;
            }
            if let Some(pkg) = t.strip_suffix(".*") {
                b.import("*", "*", pkg);
            } else if let Some((pkg, name)) = t.rsplit_once('.') {
                b.import(name, name, pkg);
            }
        }
        kind if is_type_decl(kind) => {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
            else {
                return;
            };
            let (extends, implements) = heritage(node, source);
            let kind = match kind {
                "interface_declaration" => TypeFactKind::Interface,
                "class_declaration" if has_modifier(node, source, "abstract") => {
                    TypeFactKind::Abstract
                }
                _ => TypeFactKind::Class,
            };
            let fact = TypeFact {
                name: CompactStr::new(name),
                kind,
                extends,
                implements,
                methods: Vec::new(),
            };
            Builder::push_capped(
                &mut b.facts.types,
                fact,
                MAX_DECLS_PER_FILE,
                &mut b.facts.capped,
            );
            // `record Pair(Service svc)`: the components are fields.
            if node.kind() == "record_declaration" {
                if let Some(params) = node.child_by_field_name("parameters") {
                    let mut cursor = params.walk();
                    for p in params.named_children(&mut cursor) {
                        let ty = p
                            .child_by_field_name("type")
                            .and_then(|t| type_name(t, source));
                        let field = p.child_by_field_name("name").and_then(|n| text(n, source));
                        if let (Some(field), Some(ty)) = (field, ty) {
                            b.injection(name, field, &ty);
                        }
                    }
                }
            }
        }
        "field_declaration" => {
            let Some(owner) = owner_name(node, source) else {
                return;
            };
            let Some(ty) = node
                .child_by_field_name("type")
                .and_then(|t| type_name(t, source))
            else {
                return;
            };
            let mut cursor = node.walk();
            for d in node.children_by_field_name("declarator", &mut cursor) {
                if let Some(field) = d.child_by_field_name("name").and_then(|n| text(n, source)) {
                    b.injection(owner, field, &ty);
                }
            }
        }
        "method_declaration" => {
            // An abstract or interface method has no body to call from.
            if node.child_by_field_name("body").is_none() {
                return;
            }
            let (Some(owner), Some(name)) = (
                owner_name(node, source),
                node.child_by_field_name("name")
                    .and_then(|n| text(n, source)),
            ) else {
                return;
            };
            b.callable(node, name, Some(owner));
        }
        _ => {}
    });

    let fields: HashSet<(CompactStr, CompactStr)> = b
        .facts
        .injections
        .iter()
        .map(|i| (i.class.clone(), i.field.clone()))
        .collect();
    // A class extending another may call through a field its base declares.
    let derived: HashSet<CompactStr> = b
        .facts
        .types
        .iter()
        .filter(|t| !t.extends.is_empty())
        .map(|t| t.name.clone())
        .collect();
    // Names a parameter or a local declares inside each callable, in document order:
    // a call before the declaration still reaches the field.
    let mut shadows: HashMap<u32, HashSet<CompactStr>> = HashMap::new();

    collect_sites(&mut b, root, &mut |node, caller, fact| {
        let mut shadow = |name: Option<Node>| {
            if let Some(n) = name.and_then(|n| text(n, source)) {
                shadows
                    .entry(caller)
                    .or_default()
                    .insert(CompactStr::new(n));
            }
        };
        match node.kind() {
            "formal_parameter" | "spread_parameter" | "variable_declarator" => {
                shadow(node.child_by_field_name("name"));
                return None;
            }
            "catch_formal_parameter" => {
                shadow(node.child_by_field_name("name"));
                return None;
            }
            "enhanced_for_statement" => {
                shadow(node.child_by_field_name("name"));
                return None;
            }
            "inferred_parameters" => {
                let mut cursor = node.walk();
                for n in node.named_children(&mut cursor) {
                    shadow(Some(n));
                }
                return None;
            }
            "lambda_expression" => {
                if let Some(p) = node.child_by_field_name("parameters") {
                    if p.kind() == "identifier" {
                        shadow(Some(p));
                    }
                }
                return None;
            }
            "method_invocation" => {}
            _ => return None,
        }
        let callee = CompactStr::new(text(node.child_by_field_name("name")?, source)?);
        let owner = fact.owner.as_deref()?;
        // An inherited field is guessed from the base class only for a lower-case name:
        // `Util.stat()` is a static call.
        let field_of = |name: &str| {
            fields.contains(&(CompactStr::new(owner), CompactStr::new(name)))
                || (derived.contains(owner) && name.starts_with(char::is_lowercase))
        };
        let receiver = match node.child_by_field_name("object") {
            None => CallReceiver::This,
            Some(o) => match o.kind() {
                "this" => CallReceiver::This,
                "identifier" => {
                    let f = text(o, source)?;
                    let shadowed = shadows
                        .get(&caller)
                        .is_some_and(|s| s.contains(&CompactStr::new(f)));
                    if shadowed || !field_of(f) {
                        return None;
                    }
                    CallReceiver::InjectedField(CompactStr::new(f))
                }
                "field_access" => {
                    let base = o.child_by_field_name("object")?;
                    let f = text(o.child_by_field_name("field")?, source)?;
                    if base.kind() != "this" || !field_of(f) {
                        return None;
                    }
                    CallReceiver::InjectedField(CompactStr::new(f))
                }
                _ => return None,
            },
        };
        Some((callee, receiver))
    });

    prune_unused(&mut b);
    b.facts.shrink_to_fit();
    b.facts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decapitate::LanguageKind;
    use crate::guard::{AstGuard, ParseOutcome};

    fn facts(code: &str) -> CallFacts {
        match AstGuard::parse_with(
            LanguageKind::Java,
            code,
            AstGuard::INDEX_PARSE_TIMEOUT_MICROS,
            |tree| extract(tree, code.as_bytes()),
        ) {
            ParseOutcome::Parsed(f) => f,
            _ => CallFacts::default(),
        }
    }

    fn sites(f: &CallFacts) -> Vec<(String, String, CallReceiver, u32)> {
        f.call_sites
            .iter()
            .map(|s| {
                let c = &f.callables[s.caller as usize];
                let caller = format!("{}.{}", c.owner.as_deref().unwrap_or(""), c.name);
                (caller, s.callee.to_string(), s.receiver.clone(), s.line)
            })
            .collect()
    }

    #[test]
    fn spring_injection_styles_and_receivers() {
        let f = facts(
            r#"package com.shop;

import com.shop.billing.BillingService;
import com.shop.audit.*;
import java.util.List;
import static java.lang.Math.max;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class Controller extends Base implements Handler, Auditable<Event> {
    private final BillingService billing;
    @Autowired
    private PaymentPublisher payments;
    private final KafkaTemplate<String, String> kafka;
    private List<String> names;

    public Controller(BillingService billing) {
        this.billing = billing;
    }

    public void create(String id, PaymentPublisher payments) {
        billing.charge(id);
        payments.publish(id);
        this.kafka.send("t", id);
        this.audit();
        helper(id);
        inherited.go();
        names.forEach(n -> kafka.send(n, id));
        BillingService local = null;
        local.charge(id);
        Util.stat();
        billing.charge(id).then();
    }

    void audit() {}
    abstract void todo();
}

interface Handler {
    void handle();
}

record Pair(Handler first) {
    void go() { first.handle(); }
}
"#,
        );
        let kinds: Vec<(&str, TypeFactKind)> =
            f.types.iter().map(|t| (t.name.as_str(), t.kind)).collect();
        assert_eq!(
            kinds,
            vec![
                ("Controller", TypeFactKind::Abstract),
                ("Handler", TypeFactKind::Interface),
                ("Pair", TypeFactKind::Class)
            ]
        );
        assert_eq!(f.types[0].extends, vec![CompactStr::new("Base")]);
        assert_eq!(
            f.types[0].implements,
            vec![CompactStr::new("Handler"), CompactStr::new("Auditable")]
        );
        let s = sites(&f);
        let c = "Controller.create".to_string();
        let field = |n: &str| CallReceiver::InjectedField(n.into());
        assert!(s.contains(&(c.clone(), "charge".into(), field("billing"), 21)));
        // `payments` is a parameter of `create`: it shadows the field.
        assert!(!s.iter().any(|x| x.1 == "publish"));
        assert!(s.contains(&(c.clone(), "send".into(), field("kafka"), 23)));
        assert!(s.contains(&(c.clone(), "audit".into(), CallReceiver::This, 24)));
        assert!(s.contains(&(c.clone(), "helper".into(), CallReceiver::This, 25)));
        // The class extends `Base`: a field the base may declare is kept for the resolver.
        assert!(s.contains(&(c.clone(), "go".into(), field("inherited"), 26)));
        // `local`, `Util`, a call chain and an abstract method give nothing; the repeated
        // `billing.charge` (line 30, a chain's inner call is not a site) and `send` are de-duplicated.
        assert_eq!(s.len(), 6 + 1, "{s:?}");
        assert!(s.contains(&("Pair.go".into(), "handle".into(), field("first"), 43)));
        let imports: Vec<(&str, &str)> = f
            .imports
            .iter()
            .map(|i| (i.local.as_str(), i.module.as_str()))
            .collect();
        assert_eq!(
            imports,
            vec![
                ("BillingService", "com.shop.billing"),
                ("*", "com.shop.audit"),
                // Called through `names.forEach`: kept, the resolver finds nothing.
                ("List", "java.util")
            ]
        );
    }

    #[test]
    fn anonymous_and_local_classes_have_no_callables() {
        let f = facts(
            "class A { Svc svc; void run() { new Runnable() { public void run() { svc.go(); } }; } }",
        );
        let names: Vec<&str> = f.callables.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["run"]);
        // The call inside the anonymous class is attributed to the enclosing method.
        assert_eq!(sites(&f).len(), 1);
    }

    #[test]
    fn caps_are_flagged_not_silent() {
        let mut code = String::from("class Big { Svc svc;\n");
        for k in 0..(super::super::ts_calls::MAX_CALLABLES_PER_FILE + 10) {
            code.push_str(&format!("  void m{k}() {{ svc.run(); }}\n"));
        }
        code.push_str("}\n");
        let f = facts(&code);
        assert!(f.capped);
        assert_eq!(
            f.callables.len(),
            super::super::ts_calls::MAX_CALLABLES_PER_FILE
        );
    }
}
