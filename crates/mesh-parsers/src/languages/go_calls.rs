//! Go call facts for the 9.0 calls layer: functions and methods (the owner is the
//! receiver type), struct fields (the declared type of a field is the dependency a
//! constructor `NewX(dep Dep)` stores in it), embedded types, interface method sets
//! (matched structurally against struct methods by `mesh_core::contracts::calls`),
//! package imports and the calls the resolver can follow. Resolution happens there.
//!
//! Recorded on purpose, and nothing else: `r.m()` on the method receiver `r`,
//! `r.field.m()` through a struct field, `pkg.F()` on an imported package, and `F()`
//! of a name that is not a builtin or predeclared type (a function of the same
//! package, in this file or a sibling). Calls on locals and parameters are dropped: a
//! parameter, `:=` / `var` name or `range` variable that carries the name of an imported
//! package or of a package function keeps its call out of the facts (limit: a name bound
//! after the call in document order, or in a closure, is judged by the enclosing callable
//! as a whole, see `Shadows`).
//!
//! Every walk is iterative, every list is capped per file and a hit cap sets
//! `CallFacts::capped` (counted in `IndexHealth`).

use super::call_support::{collect_sites, prune_unused, Shadows};
use super::ts_calls::{text, walk, Builder, MAX_DECLS_PER_FILE};
use mesh_core::{CallFacts, CallReceiver, CompactStr, TypeFact, TypeFactKind};
use std::collections::HashSet;
use tree_sitter::{Node, Tree};

/// Builtin functions and predeclared types: never a package function.
const PREDECLARED: &[&str] = &[
    "append",
    "any",
    "bool",
    "byte",
    "cap",
    "clear",
    "close",
    "comparable",
    "complex",
    "complex64",
    "complex128",
    "copy",
    "delete",
    "error",
    "float32",
    "float64",
    "imag",
    "int",
    "int8",
    "int16",
    "int32",
    "int64",
    "len",
    "make",
    "max",
    "min",
    "new",
    "panic",
    "print",
    "println",
    "real",
    "recover",
    "rune",
    "string",
    "uint",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "uintptr",
];

/// First path segments of the Go standard library: an import under one of them names
/// no workspace directory, and its calls (`fmt.Errorf`, `strings.Join`, ...) are the
/// bulk of all package-qualified calls, so they are not recorded at all.
const STDLIB_ROOTS: &[&str] = &[
    "archive",
    "bufio",
    "bytes",
    "cmp",
    "compress",
    "container",
    "context",
    "crypto",
    "database",
    "debug",
    "embed",
    "encoding",
    "errors",
    "expvar",
    "flag",
    "fmt",
    "go",
    "hash",
    "html",
    "image",
    "index",
    "io",
    "iter",
    "log",
    "maps",
    "math",
    "mime",
    "net",
    "os",
    "path",
    "plugin",
    "reflect",
    "regexp",
    "runtime",
    "slices",
    "sort",
    "strconv",
    "strings",
    "structs",
    "sync",
    "syscall",
    "testing",
    "text",
    "time",
    "unicode",
    "unique",
    "unsafe",
    "weak",
];

fn is_stdlib(path: &str) -> bool {
    STDLIB_ROOTS.contains(&path.split('/').next().unwrap_or(path))
}

/// `T`, `*T`, `T[X]`, `pkg.T`, `*pkg.T` -> `T` / `pkg.T`; slices, maps, channels,
/// functions and predeclared types -> `None`.
fn type_name(node: Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        "type_identifier" => {
            let t = text(node, source)?;
            (!PREDECLARED.contains(&t)).then(|| t.to_string())
        }
        "qualified_type" => {
            let pkg = text(node.child_by_field_name("package")?, source)?;
            let name = text(node.child_by_field_name("name")?, source)?;
            Some(format!("{pkg}.{name}"))
        }
        "pointer_type" | "parenthesized_type" => type_name(node.named_child(0)?, source),
        "generic_type" => type_name(node.child_by_field_name("type")?, source),
        _ => None,
    }
}

/// The name a package is used by when an import does not rename it: the last path
/// segment without a major-version suffix (`/v2`, `.v3`) or a `-go` marker.
fn import_name(path: &str) -> Option<String> {
    let mut segments = path.rsplit('/');
    let mut last = segments.next()?;
    let is_version = |s: &str| {
        s.strip_prefix('v')
            .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
    };
    if is_version(last) {
        last = segments.next().unwrap_or(last);
    }
    if let Some((stem, version)) = last.rsplit_once('.') {
        if is_version(version) {
            last = stem;
        }
    }
    let name = last
        .strip_suffix("-go")
        .or_else(|| last.strip_prefix("go-"))
        .unwrap_or(last)
        .replace('-', "_");
    (!name.is_empty()).then_some(name)
}

/// The fields and embedded types of a struct (`struct_type` node) of type `owner`.
fn struct_members(
    b: &mut Builder,
    owner: &str,
    body: Node,
    source: &[u8],
    extends: &mut Vec<CompactStr>,
) {
    let mut cursor = body.walk();
    for list in body.named_children(&mut cursor) {
        if list.kind() != "field_declaration_list" {
            continue;
        }
        let mut c2 = list.walk();
        for field in list.named_children(&mut c2) {
            if field.kind() != "field_declaration" {
                continue;
            }
            let Some(ty) = field
                .child_by_field_name("type")
                .and_then(|t| type_name(t, source))
            else {
                continue;
            };
            let mut c3 = field.walk();
            let names: Vec<&str> = field
                .children_by_field_name("name", &mut c3)
                .filter_map(|n| text(n, source))
                .collect();
            if names.is_empty() {
                // `Base` / `*Base` / `pkg.Base`: an embedded type, its methods are promoted.
                extends.push(CompactStr::new(ty));
            } else {
                for name in names {
                    b.injection(owner, name, &ty);
                }
            }
        }
    }
}

/// The method names and embedded interfaces of an `interface_type` node.
fn interface_members(
    body: Node,
    source: &[u8],
    methods: &mut Vec<CompactStr>,
    extends: &mut Vec<CompactStr>,
) {
    let mut cursor = body.walk();
    for el in body.named_children(&mut cursor) {
        match el.kind() {
            "method_elem" => {
                if let Some(name) = el.child_by_field_name("name").and_then(|n| text(n, source)) {
                    methods.push(CompactStr::new(name));
                }
            }
            "type_elem" => {
                let mut c2 = el.walk();
                for t in el.named_children(&mut c2) {
                    if let Some(n) = type_name(t, source) {
                        extends.push(CompactStr::new(n));
                    }
                }
            }
            _ => {}
        }
    }
}

/// The call facts of one parsed Go file.
pub fn extract(tree: &Tree, source: &[u8]) -> CallFacts {
    let root = tree.root_node();
    let mut b = Builder::new();
    // Receiver variable name of each recorded callable (index-aligned), `None` for a
    // function or an unnamed receiver.
    let mut receivers: Vec<Option<CompactStr>> = Vec::new();

    walk(root, &mut |node| match node.kind() {
        "import_spec" => {
            let Some(path) = node
                .child_by_field_name("path")
                .and_then(|p| text(p, source))
                .map(|p| p.trim_matches(|c| c == '"' || c == '`'))
            else {
                return;
            };
            if is_stdlib(path) {
                return;
            }
            let local = match node
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
            {
                Some("_" | ".") => return,
                Some(alias) => alias.to_string(),
                None => match import_name(path) {
                    Some(n) => n,
                    None => return,
                },
            };
            b.import(&local, "*", path);
        }
        "type_spec" => {
            let (Some(name), Some(ty)) = (
                node.child_by_field_name("name")
                    .and_then(|n| text(n, source)),
                node.child_by_field_name("type"),
            ) else {
                return;
            };
            let mut extends = Vec::new();
            let mut methods = Vec::new();
            let kind = match ty.kind() {
                "struct_type" => {
                    struct_members(&mut b, name, ty, source, &mut extends);
                    TypeFactKind::Class
                }
                "interface_type" => {
                    interface_members(ty, source, &mut methods, &mut extends);
                    TypeFactKind::Interface
                }
                _ => return,
            };
            let fact = TypeFact {
                name: CompactStr::new(name),
                kind,
                extends,
                implements: Vec::new(),
                methods,
            };
            Builder::push_capped(
                &mut b.facts.types,
                fact,
                MAX_DECLS_PER_FILE,
                &mut b.facts.capped,
            );
        }
        "function_declaration" => {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
            else {
                return;
            };
            if let Some(idx) = b.callable(node, name, None) {
                receivers.resize(idx as usize + 1, None);
            }
        }
        "method_declaration" => {
            let Some(name) = node
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
            else {
                return;
            };
            // `(s *OrderService)`: the receiver variable and its type.
            let Some(param) = node
                .child_by_field_name("receiver")
                .and_then(|r| r.named_child(0))
            else {
                return;
            };
            let Some(owner) = param
                .child_by_field_name("type")
                .and_then(|t| type_name(t, source))
            else {
                return;
            };
            let owner = owner.rsplit('.').next().unwrap_or(&owner).to_string();
            let var = param
                .child_by_field_name("name")
                .and_then(|n| text(n, source))
                .filter(|v| *v != "_")
                .map(CompactStr::new);
            if let Some(idx) = b.callable(node, name, Some(&owner)) {
                receivers.resize(idx as usize + 1, None);
                receivers[idx as usize] = var;
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
    // A struct embedding another may call through a field the embedded one declares.
    let derived: HashSet<CompactStr> = b
        .facts
        .types
        .iter()
        .filter(|t| !t.extends.is_empty())
        .map(|t| t.name.clone())
        .collect();
    let imported: HashSet<CompactStr> = b.facts.imports.iter().map(|i| i.local.clone()).collect();

    let mut shadows = Shadows::default();
    collect_sites(&mut b, root, &mut |node, caller, fact| {
        if node.kind() != "call_expression" {
            bind_locals(node, source, &mut |name| shadows.bind(caller, name));
            return None;
        }
        let function = node.child_by_field_name("function")?;
        match function.kind() {
            "identifier" => {
                let name = text(function, source)?;
                (!PREDECLARED.contains(&name) && !shadows.has(caller, name))
                    .then(|| (CompactStr::new(name), CallReceiver::SameModule))
            }
            "selector_expression" => {
                let operand = function.child_by_field_name("operand")?;
                let callee = CompactStr::new(text(function.child_by_field_name("field")?, source)?);
                let var = receivers.get(caller as usize)?.as_deref();
                let owner = fact.owner.as_deref();
                match operand.kind() {
                    "identifier" => {
                        let name = text(operand, source)?;
                        if owner.is_some() && Some(name) == var {
                            Some((callee, CallReceiver::This))
                        } else if imported.contains(name) && !shadows.has(caller, name) {
                            Some((callee, CallReceiver::Namespace(CompactStr::new(name))))
                        } else {
                            None
                        }
                    }
                    "selector_expression" => {
                        let base = operand.child_by_field_name("operand")?;
                        let field = text(operand.child_by_field_name("field")?, source)?;
                        let owner = owner?;
                        if base.kind() == "identifier"
                            && var.is_some()
                            && text(base, source) == var
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

/// Calls `bind` with each name `node` declares locally: a parameter (receiver included),
/// a `:=` or `var` name, a `range` or type-switch variable.
fn bind_locals(node: Node, source: &[u8], bind: &mut dyn FnMut(&str)) {
    let mut names: Vec<Node> = Vec::new();
    let mut cursor = node.walk();
    match node.kind() {
        "parameter_declaration" | "variadic_parameter_declaration" | "var_spec" => {
            names.extend(node.children_by_field_name("name", &mut cursor));
        }
        "short_var_declaration" | "range_clause" | "receive_statement" => {
            names.extend(node.child_by_field_name("left"));
        }
        "type_switch_statement" => names.extend(node.child_by_field_name("alias")),
        _ => {}
    }
    while let Some(n) = names.pop() {
        match n.kind() {
            "identifier" => {
                if let Some(t) = text(n, source) {
                    bind(t);
                }
            }
            "expression_list" => {
                let mut c = n.walk();
                names.extend(n.named_children(&mut c));
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decapitate::LanguageKind;
    use crate::guard::{AstGuard, ParseOutcome};

    fn facts(code: &str) -> CallFacts {
        match AstGuard::parse_with(
            LanguageKind::Go,
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
    fn receivers_fields_namespaces_and_package_functions() {
        let f = facts(
            r#"package main

import (
	"context"
	"fmt"
	kf "github.com/segmentio/kafka-go"
	"example.com/app/store/v2"
)

type Pricer interface {
	Price(sku string) float64
	Reset()
}

type Base struct{}

type OrderService struct {
	Base
	pricer Pricer
	pub, alt Publisher
	count  int
	w      *kf.Writer
}

func (s *OrderService) Handle(ctx context.Context) error {
	total := s.pricer.Price("x")
	s.helper()
	s.inherited.Go()
	fmt.Println(total)
	store.Save(total)
	audit(total)
	len(ctx)
	other.thing.Run()
	s.w.WriteMessages(ctx)
	return s.pub.Publish(ctx)
}

func (s *OrderService) helper() {}

func audit(x float64) {}
"#,
        );
        let kinds: Vec<(&str, TypeFactKind)> =
            f.types.iter().map(|t| (t.name.as_str(), t.kind)).collect();
        assert_eq!(
            kinds,
            vec![
                ("Pricer", TypeFactKind::Interface),
                ("Base", TypeFactKind::Class),
                ("OrderService", TypeFactKind::Class)
            ]
        );
        assert_eq!(
            f.types[0]
                .methods
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>(),
            vec!["Price", "Reset"]
        );
        assert_eq!(f.types[2].extends, vec![CompactStr::new("Base")]);
        let inj: Vec<(&str, &str)> = f
            .injections
            .iter()
            .map(|i| (i.field.as_str(), i.declared_type.as_str()))
            .collect();
        // Only fields a call goes through; `count` (predeclared) and `alt` (unused) are gone.
        assert_eq!(
            inj,
            vec![
                ("pricer", "Pricer"),
                ("pub", "Publisher"),
                ("w", "kf.Writer")
            ]
        );
        let s = sites(&f);
        let h = "OrderService.Handle".to_string();
        let field = |n: &str| CallReceiver::InjectedField(n.into());
        assert!(s.contains(&(h.clone(), "Price".into(), field("pricer"), 26)));
        assert!(s.contains(&(h.clone(), "helper".into(), CallReceiver::This, 27)));
        // `Base` is embedded: a field the base may declare is kept for the resolver.
        assert!(s.contains(&(h.clone(), "Go".into(), field("inherited"), 28)));
        assert!(s.contains(&(
            h.clone(),
            "Save".into(),
            CallReceiver::Namespace("store".into()),
            30
        )));
        assert!(s.contains(&(h.clone(), "audit".into(), CallReceiver::SameModule, 31)));
        assert!(s.contains(&(h.clone(), "WriteMessages".into(), field("w"), 34)));
        assert!(s.contains(&(h.clone(), "Publish".into(), field("pub"), 35)));
        // `fmt.Println` (standard library), `len` (builtin) and `other.thing.Run()` (not
        // the receiver) give nothing more.
        assert_eq!(s.len(), 7, "{s:?}");
        // Imports: only the packages a kept fact names, the alias and the version suffix applied.
        let imports: Vec<(&str, &str)> = f
            .imports
            .iter()
            .map(|i| (i.local.as_str(), i.module.as_str()))
            .collect();
        assert_eq!(
            imports,
            vec![
                ("kf", "github.com/segmentio/kafka-go"),
                ("store", "example.com/app/store/v2")
            ]
        );
    }

    #[test]
    fn parameters_and_locals_shadow_packages_and_functions() {
        let f = facts(
            r#"package p

import "example.com/app/store"

func byParam(store Saver) { store.Save() }

func byLocal() {
	store := open()
	store.Save()
}

func byRange(items []Saver) {
	for _, store := range items {
		store.Save()
	}
}

func byCallback(audit func()) { audit() }

func plain() {
	store.Save()
	audit()
}

func audit() {}
"#,
        );
        let s = sites(&f);
        let ns = CallReceiver::Namespace("store".into());
        let ns_callers: Vec<&str> = s
            .iter()
            .filter(|c| c.2 == ns)
            .map(|c| c.0.as_str())
            .collect();
        assert_eq!(ns_callers, vec![".plain"], "{s:?}");
        let audit_callers: Vec<&str> = s
            .iter()
            .filter(|c| c.1 == "audit")
            .map(|c| c.0.as_str())
            .collect();
        assert_eq!(audit_callers, vec![".plain"], "{s:?}");
    }

    #[test]
    fn a_method_keeps_its_receiver_type_across_files_and_generics() {
        let f = facts(
            "package p\ntype Box[T any] struct{ inner Inner }\n\
             func (b *Box[T]) Get() { b.inner.Load() }\n\
             func (Box[T]) Anon() { inner.Load() }\n",
        );
        let owners: Vec<(&str, &str)> = f
            .callables
            .iter()
            .map(|c| (c.owner.as_deref().unwrap_or(""), c.name.as_str()))
            .collect();
        assert_eq!(owners, vec![("Box", "Get"), ("Box", "Anon")]);
        // `inner.Load()` in a method with no receiver variable is not a field call.
        assert_eq!(sites(&f).len(), 1);
    }

    #[test]
    fn import_names() {
        assert_eq!(import_name("github.com/x/y/v3").as_deref(), Some("y"));
        assert_eq!(import_name("gopkg.in/yaml.v3").as_deref(), Some("yaml"));
        assert_eq!(
            import_name("github.com/segmentio/kafka-go").as_deref(),
            Some("kafka")
        );
        assert_eq!(import_name("net/http").as_deref(), Some("http"));
    }

    #[test]
    fn caps_are_flagged_not_silent() {
        let mut code = String::from("package p\ntype T struct{ d D }\n");
        for k in 0..(super::super::ts_calls::MAX_CALLABLES_PER_FILE + 10) {
            code.push_str(&format!("func (t *T) m{k}() {{ t.d.Run() }}\n"));
        }
        let f = facts(&code);
        assert!(f.capped);
        assert_eq!(
            f.callables.len(),
            super::super::ts_calls::MAX_CALLABLES_PER_FILE
        );
    }
}
