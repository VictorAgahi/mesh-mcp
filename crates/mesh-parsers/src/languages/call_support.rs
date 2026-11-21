//! What the Java, Go and Python call extractors share with the TypeScript one
//! (`ts_calls`): the walk that tracks the enclosing callable and records the call
//! sites a language's `classify` names, de-duplicated and capped per file.

use super::ts_calls::{walk, Builder, MAX_CALL_SITES_PER_FILE};
use mesh_core::{CallFact, CallReceiver, CallableFact, CompactStr};
use std::collections::{HashMap, HashSet};
use tree_sitter::Node;

/// A call site: the callee name and how it is reached.
pub(super) type Site = (CompactStr, CallReceiver);

/// Walks `root` in document order. For every node inside a recorded callable,
/// `classify(node, caller, callable)` returns the call site the node is (the callee
/// name and how it is reached) or `None`; it may also keep its own per-callable state
/// (a Java local that shadows a field). Sites are de-duplicated per (caller, callee,
/// receiver), the first line kept, and capped at [`MAX_CALL_SITES_PER_FILE`]
/// (`CallFacts::capped`).
pub(super) fn collect_sites<'t>(
    b: &mut Builder,
    root: Node<'t>,
    classify: &mut dyn FnMut(Node<'t>, u32, &CallableFact) -> Option<Site>,
) {
    let mut seen: HashSet<(u32, CompactStr, CallReceiver)> = HashSet::new();
    let mut sites: Vec<CallFact> = Vec::new();
    let mut capped = false;
    // Enclosing callable tracked on the way down (end byte, index): asking each call
    // for its ancestors is quadratic in tree-sitter (`Node::parent` walks from the root).
    let mut open: Vec<(usize, u32)> = Vec::new();
    let callable_at = &b.callable_at;
    let callables = &b.facts.callables;
    walk(root, &mut |node| {
        while open
            .last()
            .is_some_and(|&(end, _)| end <= node.start_byte())
        {
            open.pop();
        }
        if let Some(&idx) = callable_at.get(&node.start_byte()) {
            let same = callables.get(idx as usize).is_some_and(|c| {
                c.line_start == node.start_position().row as u32 + 1
                    && c.line_end == node.end_position().row as u32 + 1
            });
            if same && open.last().is_none_or(|&(_, top)| top != idx) {
                open.push((node.end_byte(), idx));
            }
        }
        let Some(&(_, caller)) = open.last() else {
            return;
        };
        let Some(fact) = callables.get(caller as usize) else {
            return;
        };
        let Some((callee, receiver)) = classify(node, caller, fact) else {
            return;
        };
        if !seen.insert((caller, callee.clone(), receiver.clone())) {
            return;
        }
        if sites.len() >= MAX_CALL_SITES_PER_FILE {
            capped = true;
            return;
        }
        sites.push(CallFact {
            caller,
            callee,
            receiver,
            line: node.start_position().row as u32 + 1,
        });
    });
    b.facts.call_sites = sites;
    b.facts.capped |= capped;
}

/// Names a parameter or a local binds inside each recorded callable, in document order
/// (a call before the binding still reaches the module or package of that name). The
/// Python and Go extractors use it so that a parameter `orders` does not turn
/// `orders.save()` into a call on the imported module `orders`.
///
/// shortcut: a name bound in a nested function or closure counts for the whole enclosing
/// callable, and a name bound after the call in document order is not seen; a precise
/// per-scope analysis would need a full binder, upgrade if a corpus shows false edges.
#[derive(Default)]
pub(super) struct Shadows(HashMap<u32, HashSet<CompactStr>>);

impl Shadows {
    pub(super) fn bind(&mut self, caller: u32, name: &str) {
        self.0
            .entry(caller)
            .or_default()
            .insert(CompactStr::new(name));
    }

    pub(super) fn has(&self, caller: u32, name: &str) -> bool {
        self.0.get(&caller).is_some_and(|s| s.contains(name))
    }
}

/// Whether `name` is a plain identifier (letters, digits, `_`, `$`; not starting with a digit).
pub(super) fn is_ident(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_' || c == '$')
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// Keeps the injections whose field some recorded call site goes through (a field
/// nothing calls through cannot give an edge), then the imports a kept fact names: an
/// injected or inherited type, a namespace or imported function called, written as
/// `local` or `local.Name`. Wildcard imports stay.
pub(super) fn prune_unused(b: &mut Builder) {
    let called: HashSet<&str> = b
        .facts
        .call_sites
        .iter()
        .filter_map(|s| match &s.receiver {
            CallReceiver::InjectedField(f) | CallReceiver::InheritedField(f) => Some(f.as_str()),
            _ => None,
        })
        .collect();
    let kept: Vec<_> = b
        .facts
        .injections
        .iter()
        .filter(|i| called.contains(i.field.as_str()))
        .cloned()
        .collect();
    b.facts.injections = kept;
    let used: HashSet<&str> = b
        .facts
        .injections
        .iter()
        .map(|i| i.declared_type.as_str())
        .chain(
            b.facts
                .types
                .iter()
                .flat_map(|t| t.extends.iter().chain(&t.implements))
                .map(CompactStr::as_str),
        )
        .chain(b.facts.call_sites.iter().filter_map(|s| match &s.receiver {
            CallReceiver::Namespace(ns) => Some(ns.as_str()),
            CallReceiver::ImportedSymbol => Some(s.callee.as_str()),
            _ => None,
        }))
        .collect();
    let imports: Vec<_> = b
        .facts
        .imports
        .iter()
        .filter(|i| i.local == "*" || used.iter().any(|u| names_import(u, i.local.as_str())))
        .cloned()
        .collect();
    b.facts.imports = imports;
}

/// Whether a (possibly qualified) type name `declared` starts with the namespace `local`.
pub(super) fn names_import(declared: &str, local: &str) -> bool {
    declared == local
        || declared
            .strip_prefix(local)
            .is_some_and(|rest| rest.starts_with('.'))
}
