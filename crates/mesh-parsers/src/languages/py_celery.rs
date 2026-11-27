//! The Celery gate of the Python extractor (9.0 ticket T15).
//!
//! A decorator or a `.delay(...)` is a Celery site only when the file's parsed `import`
//! statements say so (never a comment or a string). Before this gate any decorator containing
//! `@task`, `@celery` or `.task` was a consumer, so a Locust `@task` was a "Celery consumer".
//!
//! Consumer (a decorator on a `def`, called or not):
//! - `@name` / `@name(...)`: `name` is imported from `celery` (or a module whose path holds
//!   `celery`) as `task` or `shared_task`, under any alias, and is not also defined or assigned in
//!   the file;
//! - `@x.task` / `@x.shared_task` (called or not, `x` may be a chain): the file imports `celery`
//!   (so `app = Celery(...)`, `@celery.task`, `@app.task` after `from celery import Celery` all
//!   count), or `x` is imported from a module whose path holds `celery` or `tasks`, or the
//!   imported name holds `celery` (`from myproj.celery import app`, `from proj import celery_app`).
//!
//! Producer (`x.delay(...)` / `x.apply_async(...)`): the file imports `celery`, or `x` is a `def`
//! of the file that is a Celery consumer by the rule above, or `x` is imported from a module whose
//! path holds `celery` or `tasks` (`from myapp.tasks import process_order`).
//!
//! shortcut: an `app` imported from a module whose path holds neither word (`from proj import app`,
//! Flask `make_celery`), in a file that does not import `celery`, is not read; a `.delay` on a
//! task imported from a module such as `myapp.jobs`, same. Widen with a workspace-level pass when
//! a corpus shows it.

use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

#[derive(Default)]
pub(crate) struct CeleryGate {
    imports_celery: bool,
    /// local name -> `module.name` (a `from` import) or `module` (an `import`).
    names: HashMap<String, String>,
    /// Names the file defines (`def`, `class`) or assigns at module level.
    shadowed: HashSet<String>,
    /// Functions of the file that are Celery consumers.
    local_tasks: HashSet<String>,
}

fn holds_celery(path: &str) -> bool {
    path.to_ascii_lowercase().contains("celery")
}

fn holds_celery_or_tasks(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.contains("celery") || p.contains("tasks")
}

fn compact(n: Node, src: &[u8]) -> String {
    n.utf8_text(src)
        .unwrap_or("")
        .split_whitespace()
        .collect::<String>()
}

impl CeleryGate {
    pub(crate) fn scan(tree: &Tree, src: &[u8]) -> Self {
        let mut g = Self::default();
        // Iterative on purpose: the tree can be as deep as the AST guard allows.
        let mut stack = vec![tree.root_node()];
        let mut decorated: Vec<Node> = Vec::new();
        while let Some(node) = stack.pop() {
            match node.kind() {
                "import_statement" => g.bind_import(node, src),
                "import_from_statement" => g.bind_from(node, src),
                "function_definition" | "class_definition" => {
                    if let Some(n) = node.child_by_field_name("name") {
                        g.shadowed.insert(compact(n, src));
                    }
                }
                "assignment" => {
                    if let Some(l) = node.child_by_field_name("left") {
                        if l.kind() == "identifier" {
                            g.shadowed.insert(compact(l, src));
                        }
                    }
                }
                "decorated_definition" => decorated.push(node),
                _ => {}
            }
            let mut c = node.walk();
            stack.extend(node.children(&mut c));
        }
        for d in decorated {
            let Some(def) = d.child_by_field_name("definition") else {
                continue;
            };
            if def.kind() != "function_definition" {
                continue;
            }
            let mut c = d.walk();
            let is_task = d
                .children(&mut c)
                .any(|dec| dec.kind() == "decorator" && g.is_task_decorator(dec, src));
            if is_task {
                if let Some(n) = def.child_by_field_name("name") {
                    g.local_tasks.insert(compact(n, src));
                }
            }
        }
        g
    }

    fn bind_import(&mut self, stmt: Node, src: &[u8]) {
        let mut c = stmt.walk();
        for n in stmt.children_by_field_name("name", &mut c) {
            let (path, local) = match n.kind() {
                "dotted_name" => {
                    let full = compact(n, src);
                    let first = full.split('.').next().unwrap_or("").to_string();
                    (full, first)
                }
                "aliased_import" => match (
                    n.child_by_field_name("name"),
                    n.child_by_field_name("alias"),
                ) {
                    (Some(a), Some(b)) => (compact(a, src), compact(b, src)),
                    _ => continue,
                },
                _ => continue,
            };
            if path == "celery" || path.starts_with("celery.") {
                self.imports_celery = true;
            }
            self.names.insert(local, path);
        }
    }

    fn bind_from(&mut self, stmt: Node, src: &[u8]) {
        let Some(module) = stmt.child_by_field_name("module_name") else {
            return;
        };
        let module = compact(module, src);
        if module == "celery" || module.starts_with("celery.") {
            self.imports_celery = true;
        }
        let mut c = stmt.walk();
        for n in stmt.children_by_field_name("name", &mut c) {
            let (name, local) = match n.kind() {
                "dotted_name" => (compact(n, src), compact(n, src)),
                "aliased_import" => match (
                    n.child_by_field_name("name"),
                    n.child_by_field_name("alias"),
                ) {
                    (Some(a), Some(b)) => (compact(a, src), compact(b, src)),
                    _ => continue,
                },
                _ => continue,
            };
            self.names.insert(local, format!("{module}.{name}"));
        }
    }

    /// `a`, `a.b.c` as segments; anything else (a call, a subscript) is not a decorator path.
    fn path(expr: Node, src: &[u8]) -> Option<Vec<String>> {
        let mut segs = Vec::new();
        let mut cur = expr;
        loop {
            match cur.kind() {
                "identifier" => {
                    segs.push(compact(cur, src));
                    segs.reverse();
                    return Some(segs);
                }
                "attribute" => {
                    segs.push(compact(cur.child_by_field_name("attribute")?, src));
                    cur = cur.child_by_field_name("object")?;
                }
                _ => return None,
            }
        }
    }

    /// Is this `decorator` node a Celery task decorator by the rule in the module doc?
    pub(crate) fn is_task_decorator(&self, dec: Node, src: &[u8]) -> bool {
        let Some(mut expr) = dec.named_child(0) else {
            return false;
        };
        if expr.kind() == "call" {
            let Some(f) = expr.child_by_field_name("function") else {
                return false;
            };
            expr = f;
        }
        let Some(segs) = Self::path(expr, src) else {
            return false;
        };
        let last = segs[segs.len() - 1].as_str();
        if !matches!(last, "task" | "shared_task") && segs.len() > 1 {
            return false;
        }
        if segs.len() == 1 {
            let name = segs[0].as_str();
            if self.shadowed.contains(name) {
                return false;
            }
            return self.names.get(name).is_some_and(|p| {
                let (module, orig) = p.rsplit_once('.').unwrap_or(("", p));
                matches!(orig, "task" | "shared_task") && holds_celery(module)
            });
        }
        self.imports_celery || self.base_is_celery_like(&segs[0])
    }

    /// `x` imported from a module whose path holds `celery` / `tasks`, or named like Celery.
    fn base_is_celery_like(&self, base: &str) -> bool {
        self.names
            .get(base)
            .is_some_and(|p| holds_celery_or_tasks(p))
    }

    /// May `<object>.delay(...)` / `.apply_async(...)` be a Celery producer? `object` is the text
    /// of the receiver.
    pub(crate) fn is_producer_receiver(&self, object: Node, src: &[u8]) -> bool {
        if self.imports_celery {
            return true;
        }
        let Some(segs) = Self::path(object, src) else {
            return false;
        };
        if segs.len() == 1 && self.local_tasks.contains(&segs[0]) {
            return true;
        }
        self.base_is_celery_like(&segs[0])
    }
}
