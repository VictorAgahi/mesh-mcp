//! Python call facts for the 9.0 calls layer: module functions and class methods,
//! the dependencies a class receives through annotated `__init__` parameters
//! (`def __init__(self, dep: Dep): self._dep = dep`), annotated class attributes
//! (dataclass style) and `self.x: T = ...`, imports and class heritage, and the
//! calls the resolver can follow. Resolution happens in
//! `mesh_core::contracts::calls`.
//!
//! A parameter without an annotation gives no type, hence no edge: no name-based
//! guess. Recorded on purpose, and nothing else: `self.m()`, `self.dep.m()` on an
//! annotated dependency, `mod.f()` on an imported module, `f()` of an imported
//! function and `f()` of a module-level function of the file. Calls on locals and
//! parameters are dropped.
//!
//! Every walk is iterative, every list is capped per file and a hit cap sets
//! `CallFacts::capped` (counted in `IndexHealth`).

use super::call_support::{collect_sites, is_ident, prune_unused};
use super::ts_calls::{text, walk, Builder, MAX_DECLS_PER_FILE};
use mesh_core::{CallFacts, CallReceiver, CompactStr, TypeFact, TypeFactKind};
use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

/// Annotations that name no workspace class.
const BUILTIN_TYPES: &[&str] = &[
    "Any", "bool", "bytes", "Callable", "dict", "Dict", "float", "int", "List", "list", "None",
    "object", "Set", "set", "str", "Tuple", "tuple",
];

/// `Dep`, `"Dep"`, `Optional[Dep]`, `Dep | None`, `mod.Dep`, `Dep[T]` -> `Dep` / `mod.Dep`;
/// containers, builtins and anything not a plain name -> `None`.
fn annotation_type(raw: &str) -> Option<String> {
    let mut t = raw.trim().trim_matches(|c| c == '"' || c == '\'').trim();
    if let Some(inner) = t
        .strip_prefix("Optional[")
        .and_then(|r| r.strip_suffix(']'))
    {
        t = inner.trim();
    }
    if t.contains('|') {
        t = t.split('|').map(str::trim).find(|p| *p != "None")?;
    }
    let t = t.split('[').next()?.trim();
    let plain = !t.is_empty()
        && t.split('.').all(is_ident)
        && !BUILTIN_TYPES.contains(&t.rsplit('.').next().unwrap_or(t));
    plain.then(|| t.to_string())
}

/// The class whose body directly holds `def_node` (a function or its decorated wrapper).
fn enclosing_class<'s>(def_node: Node, source: &'s [u8]) -> Option<&'s str> {
    let block = def_node.parent()?;
    if block.kind() != "block" {
        return None;
    }
    let class = block.parent()?;
    if class.kind() != "class_definition" {
        return None;
    }
    text(class.child_by_field_name("name")?, source)
}

/// `__init__(self, dep: Dep)` followed by `self._dep = dep` (or `self._dep: Dep = ...`).
fn init_injections(b: &mut Builder, func: Node, class: &str, source: &[u8]) {
    let mut typed: HashMap<&str, String> = HashMap::new();
    if let Some(params) = func.child_by_field_name("parameters") {
        let mut cursor = params.walk();
        for p in params.named_children(&mut cursor) {
            let (name, ty) = match p.kind() {
                "typed_parameter" => {
                    let mut c2 = p.walk();
                    let name = p
                        .named_children(&mut c2)
                        .find(|c| c.kind() == "identifier")
                        .and_then(|n| text(n, source));
                    (name, p.child_by_field_name("type"))
                }
                "typed_default_parameter" => (
                    p.child_by_field_name("name").and_then(|n| text(n, source)),
                    p.child_by_field_name("type"),
                ),
                _ => continue,
            };
            if let (Some(name), Some(ty)) = (name, ty.and_then(|t| text(t, source))) {
                if let Some(ty) = annotation_type(ty) {
                    typed.insert(name, ty);
                }
            }
        }
    }
    let Some(body) = func.child_by_field_name("body") else {
        return;
    };
    walk(body, &mut |node| {
        if node.kind() != "assignment" {
            return;
        }
        let Some(left) = node.child_by_field_name("left") else {
            return;
        };
        if left.kind() != "attribute" {
            return;
        }
        let is_self = left
            .child_by_field_name("object")
            .and_then(|o| text(o, source))
            == Some("self");
        let Some(field) = left
            .child_by_field_name("attribute")
            .and_then(|a| text(a, source))
        else {
            return;
        };
        if !is_self {
            return;
        }
        let annotated = node
            .child_by_field_name("type")
            .and_then(|t| text(t, source))
            .and_then(annotation_type);
        let from_param = node
            .child_by_field_name("right")
            .and_then(|r| text(r, source))
            .and_then(|r| typed.get(r).cloned());
        if let Some(ty) = annotated.or(from_param) {
            b.injection(class, field, &ty);
        }
    });
}

/// The call facts of one parsed Python file.
pub fn extract(tree: &Tree, source: &[u8]) -> CallFacts {
    let root = tree.root_node();
    let mut b = Builder::new();

    walk(root, &mut |node| match node.kind() {
        "import_statement" => {
            let mut cursor = node.walk();
            for n in node.children_by_field_name("name", &mut cursor) {
                match n.kind() {
                    "aliased_import" => {
                        if let (Some(module), Some(alias)) = (
                            n.child_by_field_name("name").and_then(|m| text(m, source)),
                            n.child_by_field_name("alias").and_then(|a| text(a, source)),
                        ) {
                            b.import(alias, "*", module);
                        }
                    }
                    // `import a.b` binds `a`, whose attributes are not followed.
                    "dotted_name" => {
                        if let Some(m) = text(n, source).filter(|m| !m.contains('.')) {
                            b.import(m, "*", m);
                        }
                    }
                    _ => {}
                }
            }
        }
        "import_from_statement" => {
            let Some(module) = node
                .child_by_field_name("module_name")
                .and_then(|m| text(m, source))
            else {
                return;
            };
            let mut cursor = node.walk();
            for n in node.children_by_field_name("name", &mut cursor) {
                match n.kind() {
                    "dotted_name" => {
                        if let Some(name) = text(n, source) {
                            b.import(name, name, module);
                        }
                    }
                    "aliased_import" => {
                        if let (Some(name), Some(alias)) = (
                            n.child_by_field_name("name").and_then(|m| text(m, source)),
                            n.child_by_field_name("alias").and_then(|a| text(a, source)),
                        ) {
                            b.import(alias, name, module);
                        }
                    }
                    _ => {}
                }
            }
            let mut c2 = node.walk();
            if node
                .children(&mut c2)
                .any(|c| c.kind() == "wildcard_import")
            {
                b.import("*", "*", module);
            }
        }
        "class_definition" => {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
            else {
                return;
            };
            let mut extends = Vec::new();
            let mut abstract_base = false;
            if let Some(args) = node.child_by_field_name("superclasses") {
                let mut cursor = args.walk();
                for a in args.named_children(&mut cursor) {
                    let Some(t) = text(a, source) else { continue };
                    match a.kind() {
                        "identifier" | "attribute" => {
                            let base = t.rsplit('.').next().unwrap_or(t);
                            abstract_base |= matches!(base, "ABC" | "Protocol");
                            extends.push(CompactStr::new(t));
                        }
                        "keyword_argument" => abstract_base |= t.contains("ABCMeta"),
                        _ => {}
                    }
                }
            }
            let fact = TypeFact {
                name: CompactStr::new(name),
                kind: if abstract_base {
                    TypeFactKind::Abstract
                } else {
                    TypeFactKind::Class
                },
                extends,
                implements: Vec::new(),
                methods: Vec::new(),
            };
            Builder::push_capped(
                &mut b.facts.types,
                fact,
                MAX_DECLS_PER_FILE,
                &mut b.facts.capped,
            );
            // Dataclass-style annotated attributes: `repo: Repo`.
            if let Some(body) = node.child_by_field_name("body") {
                let mut cursor = body.walk();
                for st in body.named_children(&mut cursor) {
                    if st.kind() != "expression_statement" {
                        continue;
                    }
                    let Some(assign) = st.named_child(0).filter(|a| a.kind() == "assignment")
                    else {
                        continue;
                    };
                    let field = assign
                        .child_by_field_name("left")
                        .filter(|l| l.kind() == "identifier")
                        .and_then(|l| text(l, source));
                    let ty = assign
                        .child_by_field_name("type")
                        .and_then(|t| text(t, source))
                        .and_then(annotation_type);
                    if let (Some(field), Some(ty)) = (field, ty) {
                        b.injection(name, field, &ty);
                    }
                }
            }
        }
        "function_definition" => {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
            else {
                return;
            };
            // The decorators belong to the callable's lines (a route or a task is
            // registered on the decorator line).
            let outer = node
                .parent()
                .filter(|p| p.kind() == "decorated_definition")
                .unwrap_or(node);
            let in_module = outer.parent().is_some_and(|p| p.kind() == "module");
            let owner = enclosing_class(outer, source);
            if owner.is_none() && !in_module {
                return;
            }
            b.callable(outer, name, owner);
            if let (Some(owner), "__init__") = (owner, name) {
                init_injections(&mut b, node, owner, source);
            }
        }
        _ => {}
    });

    let fields: HashSet<(CompactStr, CompactStr)> = b
        .facts
        .injections
        .iter()
        .map(|i| (i.class.clone(), i.field.clone()))
        .collect();
    let derived: HashSet<CompactStr> = b
        .facts
        .types
        .iter()
        .filter(|t| !t.extends.is_empty())
        .map(|t| t.name.clone())
        .collect();
    let modules: HashSet<CompactStr> = b
        .facts
        .imports
        .iter()
        .filter(|i| i.local != "*" && !i.local.starts_with(char::is_uppercase))
        .map(|i| i.local.clone())
        .collect();
    let functions: HashSet<CompactStr> = b
        .facts
        .imports
        .iter()
        .filter(|i| i.exported != "*" && !i.local.starts_with(char::is_uppercase))
        .map(|i| i.local.clone())
        .collect();
    let free: HashSet<CompactStr> = b
        .facts
        .callables
        .iter()
        .filter(|c| c.owner.is_none())
        .map(|c| c.name.clone())
        .collect();

    collect_sites(&mut b, root, &mut |node, _, fact| {
        if node.kind() != "call" {
            return None;
        }
        let function = node.child_by_field_name("function")?;
        let owner = fact.owner.as_deref();
        match function.kind() {
            "identifier" => {
                let name = text(function, source)?;
                if functions.contains(name) {
                    Some((CompactStr::new(name), CallReceiver::ImportedSymbol))
                } else if free.contains(name) {
                    Some((CompactStr::new(name), CallReceiver::SameModule))
                } else {
                    None
                }
            }
            "attribute" => {
                let callee =
                    CompactStr::new(text(function.child_by_field_name("attribute")?, source)?);
                let object = function.child_by_field_name("object")?;
                match object.kind() {
                    "identifier" => {
                        let name = text(object, source)?;
                        if owner.is_some() && matches!(name, "self" | "cls") {
                            Some((callee, CallReceiver::This))
                        } else if modules.contains(name) {
                            Some((callee, CallReceiver::Namespace(CompactStr::new(name))))
                        } else {
                            None
                        }
                    }
                    "attribute" => {
                        let base = object.child_by_field_name("object")?;
                        let field = text(object.child_by_field_name("attribute")?, source)?;
                        let owner = owner?;
                        if base.kind() == "identifier"
                            && text(base, source) == Some("self")
                            && (derived.contains(owner)
                                || fields
                                    .contains(&(CompactStr::new(owner), CompactStr::new(field))))
                        {
                            Some((callee, CallReceiver::InjectedField(CompactStr::new(field))))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
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
            LanguageKind::Python,
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
    fn annotated_dependencies_and_receivers() {
        let f = facts(
            r#"from abc import ABC
from pkg.svc import Service, helper as util
from . import tasks
import json
import pkg.report as rep

class Handler(Base, ABC):
    repo: Repo
    def __init__(self, service: Service, plain, cache: "Optional[Cache]" = None):
        self._service = service
        self._plain = plain
        self._cache: Cache = cache or Cache()
        self.other = Other()

    @staticmethod
    def handle(self, msg):
        self._service.issue(msg)
        self._plain.go(msg)
        self.repo.save(msg)
        self.audit()
        util(msg)
        tasks.run(msg)
        rep.build(msg)
        json.dumps(msg)
        inherited = 1
        self.parent_dep.go()

    def audit(self):
        local()

def local():
    util(1)
"#,
        );
        let kinds: Vec<(&str, TypeFactKind)> =
            f.types.iter().map(|t| (t.name.as_str(), t.kind)).collect();
        assert_eq!(kinds, vec![("Handler", TypeFactKind::Abstract)]);
        assert_eq!(
            f.types[0].extends,
            vec![CompactStr::new("Base"), CompactStr::new("ABC")]
        );
        let inj: Vec<(&str, &str)> = f
            .injections
            .iter()
            .map(|i| (i.field.as_str(), i.declared_type.as_str()))
            .collect();
        // `_plain` has no annotation and `other` no declared type: no injection.
        assert_eq!(
            inj,
            vec![("repo", "Repo"), ("_service", "Service")],
            "{inj:?}"
        );
        let s = sites(&f);
        let h = "Handler.handle".to_string();
        let field = |n: &str| CallReceiver::InjectedField(n.into());
        assert!(s.contains(&(h.clone(), "issue".into(), field("_service"), 17)));
        assert!(s.contains(&(h.clone(), "save".into(), field("repo"), 19)));
        assert!(s.contains(&(h.clone(), "audit".into(), CallReceiver::This, 20)));
        assert!(s.contains(&(h.clone(), "util".into(), CallReceiver::ImportedSymbol, 21)));
        assert!(s.contains(&(
            h.clone(),
            "run".into(),
            CallReceiver::Namespace("tasks".into()),
            22
        )));
        assert!(s.contains(&(
            h.clone(),
            "build".into(),
            CallReceiver::Namespace("rep".into()),
            23
        )));
        // The class extends `Base`: a field the base may declare is kept for the resolver.
        assert!(s.contains(&(h.clone(), "go".into(), field("parent_dep"), 26)));
        assert!(s.contains(&(
            "Handler.audit".into(),
            "local".into(),
            CallReceiver::SameModule,
            29
        )));
        // The decorated method spans its decorator line.
        let handle = f
            .callables
            .iter()
            .find(|c| c.name == "handle")
            .map(|c| c.line_start);
        assert_eq!(handle, Some(15));
    }

    #[test]
    fn annotation_forms() {
        assert_eq!(annotation_type("Dep").as_deref(), Some("Dep"));
        assert_eq!(annotation_type("'Dep'").as_deref(), Some("Dep"));
        assert_eq!(
            annotation_type("Optional[mod.Dep]").as_deref(),
            Some("mod.Dep")
        );
        assert_eq!(annotation_type("None | Dep").as_deref(), Some("Dep"));
        assert_eq!(annotation_type("Repo[T]").as_deref(), Some("Repo"));
        assert_eq!(annotation_type("list[Dep]"), None);
        assert_eq!(annotation_type("str"), None);
        assert_eq!(annotation_type("Callable[[int], str]"), None);
    }

    #[test]
    fn nested_functions_are_not_callables() {
        let f = facts("def outer():\n    def inner():\n        pass\n    inner()\n");
        let names: Vec<&str> = f.callables.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["outer"]);
    }

    #[test]
    fn caps_are_flagged_not_silent() {
        let mut code =
            String::from("class Big:\n    def __init__(self, svc: Svc):\n        self.svc = svc\n");
        for k in 0..(super::super::ts_calls::MAX_CALLABLES_PER_FILE + 10) {
            code.push_str(&format!("    def m{k}(self):\n        self.svc.run()\n"));
        }
        let f = facts(&code);
        assert!(f.capped);
        assert_eq!(
            f.callables.len(),
            super::super::ts_calls::MAX_CALLABLES_PER_FILE
        );
    }
}
