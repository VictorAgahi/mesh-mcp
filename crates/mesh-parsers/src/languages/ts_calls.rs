//! TypeScript / JavaScript call facts for the 9.0 calls layer (NestJS, Angular and
//! plain modules): callables, the call sites the resolver can follow, constructor
//! parameter properties and typed / `inject(T)` fields, relative imports and type
//! heritage. Resolution happens in `mesh_core::contracts::calls`.
//!
//! Recorded on purpose, and nothing else (the main size cut): `this.m()`,
//! `this.field.m()` through a field whose declared type is imported from a relative
//! module, a workspace package or the file itself, `f()` of an imported binding,
//! `f()` of a free function of the file. Calls on locals, parameters and builtins
//! are dropped here, and a package import is kept only when a recorded fact names
//! it (the resolver matches `@scope/name` against the workspace package `name`).
//!
//! Every walk is iterative (`TreeCursor`), every list is capped per file and a hit
//! cap sets `CallFacts::capped` (counted in `IndexHealth`).

use mesh_core::{
    CallFact, CallFacts, CallReceiver, CallableFact, CompactStr, ImportFact, InjectionFact,
    TypeFact, TypeFactKind,
};
use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

/// Callables recorded per file at most.
pub const MAX_CALLABLES_PER_FILE: usize = 4000;
/// Call sites recorded per file at most (after de-duplication).
pub const MAX_CALL_SITES_PER_FILE: usize = 8000;
/// Types, imports and injections recorded per file at most, each.
pub const MAX_DECLS_PER_FILE: usize = 4000;

/// Pre-order iterative walk.
fn walk<'t>(root: Node<'t>, visit: &mut impl FnMut(Node<'t>)) {
    let mut cursor = root.walk();
    loop {
        visit(cursor.node());
        if cursor.goto_first_child() || cursor.goto_next_sibling() {
            continue;
        }
        loop {
            if !cursor.goto_parent() {
                return;
            }
            if cursor.goto_next_sibling() {
                break;
            }
        }
    }
}

fn text<'s>(node: Node, source: &'s [u8]) -> Option<&'s str> {
    node.utf8_text(source).ok()
}

/// `Foo<Bar>` / `ns.Foo` / `Foo[]`-free bare type name, or `None` for anything
/// that is not a plain (possibly qualified, possibly generic) type reference.
fn bare_type_name(raw: &str) -> Option<&str> {
    let head = raw.trim().trim_start_matches(':').trim();
    let head = head.split('<').next()?.trim();
    let name = head.rsplit('.').next()?.trim();
    let ident = !name.is_empty()
        && name.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_' || c == '$')
        && name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '$');
    ident.then_some(name)
}

fn is_class_node(node: Node) -> bool {
    matches!(node.kind(), "class_declaration" | "abstract_class_declaration" | "class")
}

/// Whether a declaration sits at module level (under the program, an `export`).
fn is_module_level(node: Node) -> bool {
    let mut current = node.parent();
    while let Some(n) = current {
        match n.kind() {
            "program" => return true,
            "export_statement" | "lexical_declaration" | "variable_declaration"
            | "variable_declarator" => current = n.parent(),
            _ => return false,
        }
    }
    false
}

/// Name of the class whose body directly holds `member`.
fn owner_class<'s>(member: Node, source: &'s [u8]) -> Option<&'s str> {
    let body = member.parent()?;
    if body.kind() != "class_body" {
        return None;
    }
    let class = body.parent()?;
    if !is_class_node(class) {
        return None;
    }
    text(class.child_by_field_name("name")?, source)
}

/// The string of a module specifier node (`'./x'`), quotes removed.
fn module_text(node: Node, source: &[u8]) -> Option<String> {
    let raw = text(node, source)?;
    let inner = raw.trim().trim_matches(|c| c == '\'' || c == '"' || c == '`');
    (!inner.is_empty()).then(|| inner.to_string())
}

/// Heritage names of a class / interface (`extends`, `implements`).
fn heritage(node: Node, source: &[u8]) -> (Vec<CompactStr>, Vec<CompactStr>) {
    let mut extends = Vec::new();
    let mut implements = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "class_heritage" => {
                let mut c2 = child.walk();
                for clause in child.children(&mut c2) {
                    match clause.kind() {
                        "extends_clause" => {
                            if let Some(v) = clause
                                .child_by_field_name("value")
                                .and_then(|v| text(v, source))
                                .and_then(bare_type_name)
                            {
                                extends.push(CompactStr::new(v));
                            }
                        }
                        "implements_clause" => {
                            let mut c3 = clause.walk();
                            for t in clause.named_children(&mut c3) {
                                if let Some(n) = text(t, source).and_then(bare_type_name) {
                                    implements.push(CompactStr::new(n));
                                }
                            }
                        }
                        // JavaScript: the base expression sits in the heritage itself.
                        _ if clause.is_named() => {
                            if let Some(n) = text(clause, source).and_then(bare_type_name) {
                                extends.push(CompactStr::new(n));
                            }
                        }
                        _ => {}
                    }
                }
            }
            "extends_type_clause" => {
                let mut c2 = child.walk();
                for t in child.named_children(&mut c2) {
                    if let Some(n) = text(t, source).and_then(bare_type_name) {
                        extends.push(CompactStr::new(n));
                    }
                }
            }
            _ => {}
        }
    }
    (extends, implements)
}

/// `inject(OrderService)` (Angular) -> `OrderService`.
fn inject_call_type<'s>(value: Node, source: &'s [u8]) -> Option<&'s str> {
    if value.kind() != "call_expression" {
        return None;
    }
    let function = text(value.child_by_field_name("function")?, source)?;
    if function != "inject" {
        return None;
    }
    let args = value.child_by_field_name("arguments")?;
    let first = args.named_child(0)?;
    text(first, source).and_then(bare_type_name)
}

struct Builder {
    facts: CallFacts,
    /// Start byte of each recorded callable's node -> its index.
    callable_at: HashMap<usize, u32>,
}

impl Builder {
    fn push_capped<T>(list: &mut Vec<T>, item: T, cap: usize, capped: &mut bool) -> bool {
        if list.len() >= cap {
            *capped = true;
            return false;
        }
        list.push(item);
        true
    }

    fn callable(&mut self, node: Node, name: &str, owner: Option<&str>) {
        if name.is_empty() || self.callable_at.contains_key(&node.start_byte()) {
            return;
        }
        let idx = self.facts.callables.len() as u32;
        let fact = CallableFact {
            name: CompactStr::new(name),
            owner: owner.map(CompactStr::new),
            line_start: node.start_position().row as u32 + 1,
            line_end: node.end_position().row as u32 + 1,
        };
        if Self::push_capped(
            &mut self.facts.callables,
            fact,
            MAX_CALLABLES_PER_FILE,
            &mut self.facts.capped,
        ) {
            self.callable_at.insert(node.start_byte(), idx);
        }
    }
}

/// The call facts of one parsed file. `rewrite` maps an aliased module specifier to
/// the relative path the file would have written (`TsAliasIndex::rewrite`).
pub fn extract(tree: &Tree, source: &[u8], rewrite: &dyn Fn(&str) -> Option<String>) -> CallFacts {
    let root = tree.root_node();
    let mut b = Builder {
        facts: CallFacts::default(),
        callable_at: HashMap::new(),
    };

    // Imports and re-exports of relative modules (top-level statements only).
    let mut cursor = root.walk();
    for stmt in root.children(&mut cursor) {
        let module = |stmt: Node| {
            stmt.child_by_field_name("source")
                .and_then(|s| module_text(s, source))
                .map(|m| rewrite(&m).unwrap_or(m))
        };
        match stmt.kind() {
            "import_statement" => {
                let Some(module) = module(stmt) else { continue };
                let mut c2 = stmt.walk();
                for clause in stmt.children(&mut c2) {
                    if clause.kind() != "import_clause" {
                        continue;
                    }
                    let mut c3 = clause.walk();
                    for part in clause.children(&mut c3) {
                        match part.kind() {
                            "identifier" => {
                                if let Some(name) = text(part, source) {
                                    b.import(name, name, &module);
                                }
                            }
                            "named_imports" => {
                                let mut c4 = part.walk();
                                for spec in part.children(&mut c4) {
                                    if spec.kind() != "import_specifier" {
                                        continue;
                                    }
                                    let Some(exported) =
                                        spec.child_by_field_name("name").and_then(|n| text(n, source))
                                    else {
                                        continue;
                                    };
                                    let local = spec
                                        .child_by_field_name("alias")
                                        .and_then(|n| text(n, source))
                                        .unwrap_or(exported);
                                    b.import(local, exported, &module);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            // `export { A, B as C } from './x'` and `export * from './x'`.
            "export_statement" => {
                let Some(module) = module(stmt) else { continue };
                let mut c2 = stmt.walk();
                let mut named = false;
                for part in stmt.children(&mut c2) {
                    match part.kind() {
                        "export_clause" => {
                            named = true;
                            let mut c3 = part.walk();
                            for spec in part.children(&mut c3) {
                                if spec.kind() != "export_specifier" {
                                    continue;
                                }
                                let Some(exported) =
                                    spec.child_by_field_name("name").and_then(|n| text(n, source))
                                else {
                                    continue;
                                };
                                let local = spec
                                    .child_by_field_name("alias")
                                    .and_then(|n| text(n, source))
                                    .unwrap_or(exported);
                                b.import(local, exported, &module);
                            }
                        }
                        "namespace_export" => named = true,
                        _ => {}
                    }
                }
                if !named {
                    b.import("*", "*", &module);
                }
            }
            _ => {}
        }
    }

    // Types, callables and injections.
    walk(root, &mut |node| match node.kind() {
        "class_declaration" | "abstract_class_declaration" | "class" | "interface_declaration" => {
            let Some(name) = node.child_by_field_name("name").and_then(|n| text(n, source)) else {
                return;
            };
            let kind = match node.kind() {
                "interface_declaration" => TypeFactKind::Interface,
                "abstract_class_declaration" => TypeFactKind::Abstract,
                _ => TypeFactKind::Class,
            };
            let (extends, implements) = heritage(node, source);
            let fact = TypeFact {
                name: CompactStr::new(name),
                kind,
                extends,
                implements,
            };
            Builder::push_capped(&mut b.facts.types, fact, MAX_DECLS_PER_FILE, &mut b.facts.capped);
        }
        "method_definition" => {
            let Some(owner) = owner_class(node, source) else {
                return;
            };
            let Some(name_node) = node.child_by_field_name("name") else {
                return;
            };
            if name_node.kind() == "computed_property_name" {
                return;
            }
            let Some(name) = text(name_node, source) else {
                return;
            };
            b.callable(node, name, Some(owner));
            if name == "constructor" {
                b.parameter_properties(node, owner, source);
            }
        }
        "public_field_definition" | "field_definition" => {
            let Some(owner) = owner_class(node, source) else {
                return;
            };
            let name = node
                .child_by_field_name("name")
                .or_else(|| node.child_by_field_name("property"))
                .and_then(|n| text(n, source));
            let Some(name) = name else { return };
            let value = node.child_by_field_name("value");
            // `handle = async (x) => { ... }`: a method in all but syntax.
            if let Some(v) = value.filter(|v| matches!(v.kind(), "arrow_function" | "function_expression" | "function")) {
                b.callable(v, name, Some(owner));
                return;
            }
            let declared = node
                .child_by_field_name("type")
                .and_then(|t| text(t, source))
                .and_then(bare_type_name)
                .or_else(|| value.and_then(|v| inject_call_type(v, source)));
            if let Some(t) = declared {
                b.injection(owner, name, t);
            }
        }
        "function_declaration" | "generator_function_declaration" => {
            if !is_module_level(node) {
                return;
            }
            if let Some(name) = node.child_by_field_name("name").and_then(|n| text(n, source)) {
                b.callable(node, name, None);
            }
        }
        "variable_declarator" => {
            let Some(value) = node.child_by_field_name("value") else {
                return;
            };
            if !matches!(value.kind(), "arrow_function" | "function_expression" | "function")
                || !is_module_level(node)
            {
                return;
            }
            if let Some(name) = node.child_by_field_name("name").and_then(|n| text(n, source)) {
                b.callable(value, name, None);
            }
        }
        _ => {}
    });

    b.prune_injections();
    b.call_sites(root, source);
    b.prune_imports();
    b.facts
}

impl Builder {
    fn import(&mut self, local: &str, exported: &str, module: &str) {
        let fact = ImportFact {
            local: CompactStr::new(local),
            exported: CompactStr::new(exported),
            module: CompactStr::new(module),
        };
        Self::push_capped(&mut self.facts.imports, fact, MAX_DECLS_PER_FILE, &mut self.facts.capped);
    }

    fn injection(&mut self, class: &str, field: &str, declared_type: &str) {
        let fact = InjectionFact {
            class: CompactStr::new(class),
            field: CompactStr::new(field),
            declared_type: CompactStr::new(declared_type),
        };
        Self::push_capped(
            &mut self.facts.injections,
            fact,
            MAX_DECLS_PER_FILE,
            &mut self.facts.capped,
        );
    }

    /// `constructor(private readonly orders: OrderService)`: parameter properties.
    fn parameter_properties(&mut self, ctor: Node, owner: &str, source: &[u8]) {
        let Some(params) = ctor.child_by_field_name("parameters") else {
            return;
        };
        let mut cursor = params.walk();
        for p in params.named_children(&mut cursor) {
            if !matches!(p.kind(), "required_parameter" | "optional_parameter") {
                continue;
            }
            let mut c2 = p.walk();
            let is_property = p
                .children(&mut c2)
                .any(|c| matches!(c.kind(), "accessibility_modifier" | "readonly"));
            if !is_property {
                continue;
            }
            let field = p.child_by_field_name("pattern").and_then(|n| text(n, source));
            let declared = p
                .child_by_field_name("type")
                .and_then(|t| text(t, source))
                .and_then(bare_type_name);
            if let (Some(field), Some(t)) = (field, declared) {
                self.injection(owner, field, t);
            }
        }
    }

    /// Keeps only the injections whose type can resolve to a workspace file: a
    /// relative import or a type of this file.
    fn prune_injections(&mut self) {
        let resolvable: HashSet<&str> = self
            .facts
            .imports
            .iter()
            .map(|i| i.local.as_str())
            .chain(self.facts.types.iter().map(|t| t.name.as_str()))
            .collect();
        let kept: Vec<InjectionFact> = self
            .facts
            .injections
            .iter()
            .filter(|i| resolvable.contains(i.declared_type.as_str()))
            .cloned()
            .collect();
        self.facts.injections = kept;
    }

    /// Keeps every relative import (the file graph the resolver prunes on) and a
    /// package import only when a recorded fact names it: an injected type, a type
    /// the file extends or implements, an imported function it calls.
    fn prune_imports(&mut self) {
        let used: HashSet<&str> = self
            .facts
            .injections
            .iter()
            .map(|i| i.declared_type.as_str())
            .chain(
                self.facts
                    .types
                    .iter()
                    .flat_map(|t| t.extends.iter().chain(&t.implements))
                    .map(CompactStr::as_str),
            )
            .chain(
                self.facts
                    .call_sites
                    .iter()
                    .filter(|c| c.receiver == CallReceiver::ImportedSymbol)
                    .map(|c| c.callee.as_str()),
            )
            .collect();
        let kept: Vec<ImportFact> = self
            .facts
            .imports
            .iter()
            .filter(|i| i.module.starts_with('.') || used.contains(i.local.as_str()))
            .cloned()
            .collect();
        self.facts.imports = kept;
    }

    fn call_sites(&mut self, root: Node, source: &[u8]) {
        let fields: HashSet<(&str, &str)> = self
            .facts
            .injections
            .iter()
            .map(|i| (i.class.as_str(), i.field.as_str()))
            .collect();
        // A class extending another may call through a field its base declares.
        let derived: HashSet<&str> = self
            .facts
            .types
            .iter()
            .filter(|t| !t.extends.is_empty())
            .map(|t| t.name.as_str())
            .collect();
        let imported: HashSet<&str> = self
            .facts
            .imports
            .iter()
            .map(|i| i.local.as_str())
            .filter(|l| *l != "*")
            .collect();
        let free: HashSet<&str> = self
            .facts
            .callables
            .iter()
            .filter(|c| c.owner.is_none())
            .map(|c| c.name.as_str())
            .collect();
        let mut seen: HashSet<(u32, String, CallReceiver)> = HashSet::new();
        let mut sites: Vec<CallFact> = Vec::new();
        let mut capped = false;
        // Enclosing callable tracked on the way down (end byte, index): asking each
        // call for its ancestors is quadratic in tree-sitter (`Node::parent` walks
        // from the root) and a 3,000-link `.p().p()` chain made that cubic.
        let mut open: Vec<(usize, u32)> = Vec::new();
        let callable_at = &self.callable_at;
        let callables = &self.facts.callables;
        walk(root, &mut |node| {
            while open.last().is_some_and(|&(end, _)| end <= node.start_byte()) {
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
            if node.kind() != "call_expression" {
                return;
            }
            let Some(function) = node.child_by_field_name("function") else {
                return;
            };
            let Some(&(_, caller)) = open.last() else {
                return;
            };
            let Some(owner_fact) = callables.get(caller as usize) else {
                return;
            };
            let owner = owner_fact.owner.as_deref();
            let site = match function.kind() {
                "member_expression" => {
                    let (Some(object), Some(property)) = (
                        function.child_by_field_name("object"),
                        function.child_by_field_name("property").and_then(|p| text(p, source)),
                    ) else {
                        return;
                    };
                    match object.kind() {
                        "this" if owner.is_some() => Some((property, CallReceiver::This)),
                        "member_expression" => {
                            let inner_this = object
                                .child_by_field_name("object")
                                .is_some_and(|o| o.kind() == "this");
                            let field = object
                                .child_by_field_name("property")
                                .and_then(|p| text(p, source));
                            match (inner_this, field, owner) {
                                (true, Some(field), Some(owner))
                                    if fields.contains(&(owner, field))
                                        || derived.contains(owner) =>
                                {
                                    Some((
                                        property,
                                        CallReceiver::InjectedField(CompactStr::new(field)),
                                    ))
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    }
                }
                "identifier" => text(function, source).and_then(|name| {
                    if imported.contains(name) {
                        Some((name, CallReceiver::ImportedSymbol))
                    } else if free.contains(name) {
                        Some((name, CallReceiver::SameModule))
                    } else {
                        None
                    }
                }),
                _ => None,
            };
            let Some((callee, receiver)) = site else {
                return;
            };
            if !seen.insert((caller, callee.to_string(), receiver.clone())) {
                return;
            }
            if sites.len() >= MAX_CALL_SITES_PER_FILE {
                capped = true;
                return;
            }
            sites.push(CallFact {
                caller,
                callee: CompactStr::new(callee),
                receiver,
                line: node.start_position().row as u32 + 1,
            });
        });
        self.facts.call_sites = sites;
        self.facts.capped |= capped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decapitate::LanguageKind;
    use crate::guard::{AstGuard, ParseOutcome};

    fn facts(code: &str) -> CallFacts {
        match AstGuard::parse_with(
            LanguageKind::TypeScript,
            code,
            AstGuard::INDEX_PARSE_TIMEOUT_MICROS,
            |tree| extract(tree, code.as_bytes(), &|m| (m == "@app/svc").then(|| "./svc".to_string())),
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
    fn nest_and_angular_injections_and_receivers() {
        let f = facts(
            r#"import { Injectable, inject } from '@nestjs/common';
import { OrderService } from './order.service';
import { Repo } from '@app/svc';
import { helper } from './helpers';
import { Logger } from 'some-logger';
export class Consumer extends Base implements Handler {
  private readonly repo = inject(Repo);
  private local: Logger;
  constructor(private readonly orders: OrderService, plain: OrderService) {}
  async handle(dto: unknown) {
    await this.orders.place(dto);
    this.repo.save(dto);
    this.inherited.go();
    this.audit();
    helper(dto);
    local(dto);
    plain.place(dto);
    this.local.log('x');
    items.forEach((i) => this.orders.place(i));
  }
  audit() {}
}
function local(x: unknown) {}
"#,
        );
        let inj: Vec<(&str, &str, &str)> = f
            .injections
            .iter()
            .map(|i| (i.class.as_str(), i.field.as_str(), i.declared_type.as_str()))
            .collect();
        assert!(inj.contains(&("Consumer", "orders", "OrderService")));
        assert!(inj.contains(&("Consumer", "repo", "Repo")));
        // A package type is kept (it may be a workspace package); a plain parameter is not.
        assert!(inj.contains(&("Consumer", "local", "Logger")));
        assert!(inj.iter().all(|i| i.1 != "plain"));
        let s = sites(&f);
        let h = "Consumer.handle".to_string();
        assert!(s.contains(&(h.clone(), "place".into(), CallReceiver::InjectedField("orders".into()), 11)));
        assert!(s.contains(&(h.clone(), "save".into(), CallReceiver::InjectedField("repo".into()), 12)));
        // Inherited field: the class extends a base, kept for the resolver.
        assert!(s.contains(&(h.clone(), "go".into(), CallReceiver::InjectedField("inherited".into()), 13)));
        assert!(s.contains(&(h.clone(), "audit".into(), CallReceiver::This, 14)));
        assert!(s.contains(&(h.clone(), "helper".into(), CallReceiver::ImportedSymbol, 15)));
        assert!(s.contains(&(h.clone(), "local".into(), CallReceiver::SameModule, 16)));
        assert!(s.contains(&(h.clone(), "log".into(), CallReceiver::InjectedField("local".into()), 18)));
        // A parameter (`plain`) gives nothing; the repeated `this.orders.place` in the
        // callback is de-duplicated.
        assert_eq!(s.len(), 7, "{s:?}");
        // Imports: relative kept, the alias rewritten, a package import only when named.
        let modules: Vec<&str> = f.imports.iter().map(|i| i.module.as_str()).collect();
        assert_eq!(modules, vec!["./order.service", "./svc", "./helpers", "some-logger"]);
        let t = &f.types[0];
        assert_eq!((t.name.as_str(), t.kind), ("Consumer", TypeFactKind::Class));
        assert_eq!(t.extends, vec![CompactStr::new("Base")]);
        assert_eq!(t.implements, vec![CompactStr::new("Handler")]);
    }

    #[test]
    fn unicode_names_reexports_and_interfaces() {
        let f = facts(
            "export { Émetteur as Sender } from './émetteur';\nexport * from './all';\n\
             export interface Notifié extends Base<T> { envoyer(): void }\n\
             export abstract class Abstrait { abstract run(): void }\n\
             export const flèche = () => { autre(); };\nfunction autre() {}\n",
        );
        let imports: Vec<(&str, &str)> =
            f.imports.iter().map(|i| (i.local.as_str(), i.exported.as_str())).collect();
        assert_eq!(imports, vec![("Sender", "Émetteur"), ("*", "*")]);
        let kinds: Vec<(&str, TypeFactKind)> =
            f.types.iter().map(|t| (t.name.as_str(), t.kind)).collect();
        assert!(kinds.contains(&("Notifié", TypeFactKind::Interface)));
        assert!(kinds.contains(&("Abstrait", TypeFactKind::Abstract)));
        let s = sites(&f);
        assert_eq!(s, vec![(".flèche".into(), "autre".into(), CallReceiver::SameModule, 5)]);
    }

    #[test]
    fn caps_are_flagged_not_silent() {
        let mut code = String::from("export class Big {\n");
        for k in 0..(MAX_CALLABLES_PER_FILE + 10) {
            code.push_str(&format!("  m{k}() {{ this.m0(); }}\n"));
        }
        code.push_str("}\n");
        let f = facts(&code);
        assert!(f.capped);
        assert_eq!(f.callables.len(), MAX_CALLABLES_PER_FILE);
        assert!(f.call_sites.len() <= MAX_CALL_SITES_PER_FILE);
    }

    #[test]
    fn a_broken_file_still_gives_bounded_facts() {
        let f = facts("export class A {\n  run() { this.b.c( \n");
        assert!(f.call_sites.len() <= 1);
    }
}
