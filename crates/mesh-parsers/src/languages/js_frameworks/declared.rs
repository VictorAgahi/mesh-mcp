//! Calls of a helper the project declares as a producer (9.0 T14b,
//! `[[engines.contracts.producer_calls]]`): `JobsOutboxEntity.createJob({ type, target })`,
//! `publisher.publish(Streams.X, event)`.
//!
//! The call is found in the syntax tree (never in a string or a comment), its receiver is the
//! declared one and not shadowed by a local, and the name is read as any other topic name is
//! (a literal, a constant of the file, an imported `Enum.MEMBER`). Whatever cannot be read as
//! a name (a parameter, a lookup table, a refused secret-shaped value) yields no producer and
//! is counted, never dropped silently.

use super::scope::{is_shadowed, Names};
use mesh_core::ProducerCallConfig;
use tree_sitter::Node;

/// Sites kept per file; the rest are counted in [`Declared::capped`].
pub(crate) const MAX_DECLARED_SITES_PER_FILE: usize = 2_000;

/// A topic or queue name a declared call states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeclaredSite {
    /// 1-based line of the call.
    pub line: usize,
    /// As written: a literal, a placeholder, or `Enum.MEMBER`.
    pub topic: String,
    /// Read from a queue key: an enum member resolves to its value.
    pub queue: bool,
    /// The callee's method name, for the node's signature.
    pub callee: String,
}

#[derive(Debug, Default)]
pub(crate) struct Declared {
    pub sites: Vec<DeclaredSite>,
    /// Names a declared call stated that could not be read (not a name, or refused).
    pub unread: u32,
    /// The per-file cap cut sites.
    pub capped: bool,
}

/// Whether `name` may become a topic name (commandment 5): a name that is no token, URL or
/// password, or a placeholder whose every default is a name and whose key is not a credential's
/// (`${DB_PASSWORD:hunter2}` and `${TOPIC:hunter2-prod}` are refused, `${TOPIC:orders}` is kept).
/// The shared fold-time masking only drops a default that is not topic-shaped, which a short
/// password is; this site must not rely on it.
fn name_may_be_recorded(name: &str) -> bool {
    if name.contains("${") {
        return placeholders_are_safe(name);
    }
    if mesh_core::is_member_ref(name) {
        // The member's own name is what is judged; the value is not known here.
        return mesh_core::member_consts::constant_is_a_name(name, "name");
    }
    mesh_core::member_consts::constant_is_a_name("", name)
}

fn placeholders_are_safe(name: &str) -> bool {
    let mut rest = name;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return false;
        };
        let inner = &after[..end];
        if inner.contains("${") {
            return false;
        }
        if let Some((key, default)) = inner.split_once(':') {
            let key = key.replace(['.', '-'], "_");
            if mesh_core::member_consts::member_is_credential_named(&key)
                || !mesh_core::member_consts::constant_is_a_name("", default)
            {
                return false;
            }
        }
        rest = &after[end + 1..];
    }
    true
}

/// The dotted name of a receiver, read through the wrappers that do not change it
/// (`(X as any)`, `this.jobs!`): `None` for anything else (a call, an index).
fn receiver_path(node: Node, source: &[u8]) -> Option<String> {
    let mut segments: Vec<String> = Vec::new();
    let mut current = node;
    for _ in 0..=crate::guard::AstGuard::MAX_NESTING_DEPTH {
        current = super::super::typescript::TypeScriptExtractor::peel_wrappers(current);
        match current.kind() {
            "identifier" | "this" => {
                segments.push(current.utf8_text(source).ok()?.to_string());
                segments.reverse();
                return Some(segments.join("."));
            }
            "member_expression" => {
                let property = current.child_by_field_name("property")?;
                if property.kind() != "property_identifier" {
                    return None;
                }
                segments.push(property.utf8_text(source).ok()?.to_string());
                current = current.child_by_field_name("object")?;
            }
            _ => return None,
        }
    }
    None
}

/// `receiver` ends with the declared qualifier on a segment boundary.
fn receiver_matches(receiver: &str, qualifier: &str) -> bool {
    receiver == qualifier || receiver.ends_with(&format!(".{qualifier}"))
}

pub(crate) fn collect(
    root: Node,
    source: &[u8],
    calls: &[ProducerCallConfig],
    names: &Names,
) -> Declared {
    let mut out = Declared::default();
    let rules: Vec<_> = calls
        .iter()
        .filter_map(|c| c.callee_parts().map(|(q, m)| (c, q, m)))
        .collect();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        let mut cursor = node.walk();
        // Reverse so the pops visit children in source order: deterministic site order.
        let children: Vec<Node> = node.named_children(&mut cursor).collect();
        stack.extend(children.into_iter().rev());
        if node.kind() != "call_expression" {
            continue;
        }
        let Some(function) = node
            .child_by_field_name("function")
            .map(super::super::typescript::TypeScriptExtractor::peel_wrappers)
        else {
            continue;
        };
        let (method, receiver) = match function.kind() {
            "identifier" => (function.utf8_text(source).ok(), None),
            "member_expression" => (
                function
                    .child_by_field_name("property")
                    .and_then(|p| p.utf8_text(source).ok()),
                function
                    .child_by_field_name("object")
                    .and_then(|o| receiver_path(o, source)),
            ),
            _ => continue,
        };
        let Some(method) = method else { continue };
        let bare = function.kind() == "identifier";
        for (call, qualifier, _) in rules.iter().filter(|(_, _, m)| *m == method) {
            // The identifier a local could hide: the helper itself (a bare call) or the
            // first segment of the declared receiver.
            let hideable = match (qualifier, receiver.as_deref()) {
                (Some(q), Some(r)) if receiver_matches(r, q) => q.split('.').next(),
                // A receiver that is a call, an index or another expression cannot be compared
                // with the declared one: counted, not guessed.
                (Some(_), None) => {
                    out.unread += 1;
                    continue;
                }
                (Some(_), _) => continue,
                (None, _) if bare => Some(method),
                (None, _) => None,
            };
            // A local of the same name (a parameter called like the helper or its class) is
            // not the declared helper.
            if hideable
                .is_some_and(|n| n != "this" && is_shadowed(function, n, source, names.scopes))
            {
                continue;
            }
            read_call(node, call, method, names, &mut out);
        }
    }
    out
}

fn read_call(
    call_node: Node,
    call: &ProducerCallConfig,
    method: &str,
    names: &Names,
    out: &mut Declared,
) {
    let Some(args) = call_node.child_by_field_name("arguments") else {
        return;
    };
    let Some(arg) = args
        .named_children(&mut args.walk())
        .filter(|n| n.kind() != "comment")
        .nth(call.arg)
    else {
        return;
    };
    // Each name is reported on the line that carries it (the `type:` line of a multi-line call,
    // the element of an array), where a reader looks for it.
    let push = |value: Node, queue: bool, out: &mut Declared| {
        let value = super::super::typescript::TypeScriptExtractor::peel_wrappers(value);
        let items: Vec<Node> = if value.kind() == "array" {
            value.named_children(&mut value.walk()).collect()
        } else {
            vec![value]
        };
        for item in items {
            let Some(topic) = names.resolve(item) else {
                out.unread += 1;
                continue;
            };
            if !name_may_be_recorded(&topic) {
                out.unread += 1;
            } else if out.sites.len() >= MAX_DECLARED_SITES_PER_FILE {
                out.capped = true;
            } else {
                out.sites.push(DeclaredSite {
                    line: item.start_position().row + 1,
                    topic,
                    queue,
                    callee: method.to_string(),
                });
            }
        }
    };
    if call.topic_keys.is_empty() && call.queue_keys.is_empty() {
        push(arg, false, out);
        return;
    }
    let peeled = super::super::typescript::TypeScriptExtractor::peel_wrappers(arg);
    if peeled.kind() != "object" {
        out.unread += 1;
        return;
    }
    for (keys, queue) in [(&call.topic_keys, false), (&call.queue_keys, true)] {
        for key in keys {
            if let Some(value) = names.object_value(peeled, key) {
                push(value, queue, out);
            }
        }
    }
}
