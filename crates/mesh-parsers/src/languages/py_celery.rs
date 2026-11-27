//! The Celery gate of the Python extractor (9.0 ticket T15).
//!
//! A decorator or a `.delay(...)` is a Celery site only when the file's parsed `import`
//! statements and its module-level names say so (never a comment or a string). Before this gate
//! any decorator containing `@task`, `@celery` or `.task` was a consumer, so a Locust `@task` was
//! a "Celery consumer".
//!
//! Words are matched on tokens, never substrings: a module path holds `celery` when a segment or a
//! `_`-token of it is `celery` (`celery`, `proj.celery`, `my_celery`, `celery_app`; not
//! `mycelery_utils`, `celeryman`), and `tasks` only as a whole dotted segment (`myapp.tasks`; not
//! `subtasks`, `shop_tasks`). Imports from [`FOREIGN`] packages are never Celery by the `tasks`
//! rule nor by the producer rule.
//!
//! Consumer (a decorator on a `def`, called or not):
//! - `@name`: `name` is imported from a celery-like module as `task` or `shared_task` (any alias)
//!   and is not defined or assigned at module level;
//! - `@x.task` / `@x.shared_task` (`x` may be a chain): `x` itself holds the token `celery`, or is
//!   assigned at module level only from calls to `Celery` / `make_celery` / `create_celery` (any
//!   callee whose name holds the token, or that is imported from a celery-like module), or is
//!   imported from a celery-like module or under a celery-like name, or is imported from a module
//!   with a `tasks` segment (`from myapp.tasks import app`).
//!   Importing `celery` somewhere in the file is NOT enough by itself.
//!
//! Producer (`x.delay(...)`, `x.apply_async(...)`, and `x.s(...)` / `.si(...)` / `.signature(...)`
//! before them): `x` is a gated consumer `def` of the file, or a name bound by a `from` import of
//! any module that is not [`FOREIGN`] (these method names are Celery-specific), or an attribute
//! chain rooted at an imported name. A local variable, a parameter, `self.x` or a name assigned in
//! the file is not read.
//!
//! shortcut: an `app` imported from a module with neither word (`from proj import app`) is not
//! read as a consumer, and `from celery import *` binds no names; widen with a workspace-level pass
//! when a corpus shows it.

use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

/// Packages whose `task` / `delay` / `apply_async` are not Celery's.
const FOREIGN: [&str; 11] = [
    "invoke",
    "fabric",
    "locust",
    "prefect",
    "huey",
    "dramatiq",
    "arq",
    "rq",
    "luigi",
    "multiprocessing",
    "gevent",
];

struct Import {
    module: String,
    /// The imported name of `from module import name`; `None` for `import module`.
    name: Option<String>,
}

fn has_celery_token(s: &str) -> bool {
    s.split(['.', '_'])
        .any(|t| t.eq_ignore_ascii_case("celery"))
}

impl Import {
    fn celery_like(&self) -> bool {
        has_celery_token(&self.module) || self.name.as_deref().is_some_and(has_celery_token)
    }

    fn foreign(&self) -> bool {
        self.module
            .split('.')
            .find(|s| !s.is_empty())
            .is_some_and(|root| FOREIGN.contains(&root))
    }

    fn tasks_module(&self) -> bool {
        !self.foreign() && self.module.split('.').any(|s| s == "tasks")
    }
}

#[derive(Default)]
pub(crate) struct CeleryGate {
    names: HashMap<String, Import>,
    /// A local name bound to two different imports.
    ambiguous: HashSet<String>,
    /// Names the file defines (`def`, `class`) at module level.
    defined: HashSet<String>,
    /// Module-level `name = <call or other>`: the callee path of each binding, `None` when the
    /// value is not a call to a plain name or attribute chain.
    assigned: HashMap<String, Vec<Option<Vec<String>>>>,
    /// Functions of the file that are Celery consumers.
    local_tasks: HashSet<String>,
}

fn compact(n: Node, src: &[u8]) -> String {
    n.utf8_text(src)
        .unwrap_or("")
        .split_whitespace()
        .collect::<String>()
}

/// Statements through which a module-level definition is still module level.
fn passes_module_level(kind: &str) -> bool {
    matches!(
        kind,
        "module"
            | "block"
            | "if_statement"
            | "elif_clause"
            | "else_clause"
            | "try_statement"
            | "except_clause"
            | "finally_clause"
            | "with_statement"
            | "for_statement"
            | "while_statement"
            | "expression_statement"
            | "decorated_definition"
    )
}

impl CeleryGate {
    pub(crate) fn scan(tree: &Tree, src: &[u8]) -> Self {
        let mut g = Self::default();
        // Iterative on purpose: the tree can be as deep as the AST guard allows.
        let mut stack = vec![(tree.root_node(), true)];
        let mut decorated: Vec<Node> = Vec::new();
        while let Some((node, top)) = stack.pop() {
            let mut child_top = top && passes_module_level(node.kind());
            match node.kind() {
                "import_statement" => g.bind_import(node, src),
                "import_from_statement" => g.bind_from(node, src),
                "function_definition" | "class_definition" => {
                    if top {
                        if let Some(n) = node.child_by_field_name("name") {
                            g.defined.insert(compact(n, src));
                        }
                    }
                    child_top = false;
                }
                "assignment" => {
                    if top {
                        g.bind_assignment(node, src);
                    }
                    child_top = false;
                }
                "decorated_definition" => decorated.push(node),
                _ => {}
            }
            let mut c = node.walk();
            stack.extend(node.children(&mut c).map(|ch| (ch, child_top)));
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

    fn add(&mut self, local: String, imp: Import) {
        match self.names.get(&local) {
            Some(prev) if prev.module != imp.module || prev.name != imp.name => {
                self.ambiguous.insert(local);
            }
            Some(_) => {}
            None => {
                self.names.insert(local, imp);
            }
        }
    }

    fn bind_assignment(&mut self, node: Node, src: &[u8]) {
        let Some(left) = node.child_by_field_name("left") else {
            return;
        };
        if left.kind() != "identifier" {
            return;
        }
        let callee = node
            .child_by_field_name("right")
            .filter(|r| r.kind() == "call")
            .and_then(|r| r.child_by_field_name("function"))
            .and_then(|f| Self::path(f, src));
        self.assigned
            .entry(compact(left, src))
            .or_default()
            .push(callee);
    }

    fn bind_import(&mut self, stmt: Node, src: &[u8]) {
        let mut c = stmt.walk();
        for n in stmt.children_by_field_name("name", &mut c) {
            let (module, local) = match n.kind() {
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
            self.add(local, Import { module, name: None });
        }
    }

    fn bind_from(&mut self, stmt: Node, src: &[u8]) {
        let Some(module) = stmt.child_by_field_name("module_name") else {
            return;
        };
        let module = compact(module, src);
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
            self.add(
                local,
                Import {
                    module: module.clone(),
                    name: Some(name),
                },
            );
        }
    }

    /// `a`, `a.b.c` as segments; anything else (a call, a subscript) is not a path.
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

    /// Does this callee build a Celery app (`Celery`, `make_celery`, `create_celery`, an alias of
    /// `celery.Celery`)?
    fn celery_callee(&self, segs: &[String]) -> bool {
        segs.last().is_some_and(|l| has_celery_token(l))
            || self.names.get(&segs[0]).is_some_and(Import::celery_like)
    }

    /// May `<base>.task` be a Celery task decorator?
    fn base_is_celery(&self, base: &str) -> bool {
        if self.defined.contains(base) || self.ambiguous.contains(base) {
            return false;
        }
        if has_celery_token(base) {
            return true;
        }
        if let Some(values) = self.assigned.get(base) {
            return values
                .iter()
                .all(|c| c.as_ref().is_some_and(|s| self.celery_callee(s)));
        }
        self.names
            .get(base)
            .is_some_and(|i| i.celery_like() || i.tasks_module())
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
        if segs.len() == 1 {
            let name = segs[0].as_str();
            if self.defined.contains(name)
                || self.assigned.contains_key(name)
                || self.ambiguous.contains(name)
            {
                return false;
            }
            return self.names.get(name).is_some_and(|i| {
                matches!(i.name.as_deref(), Some("task" | "shared_task")) && i.celery_like()
            });
        }
        matches!(last, "task" | "shared_task") && self.base_is_celery(&segs[0])
    }

    /// The task a `<object>.delay(...)` / `.apply_async(...)` publishes to, when `object` is a
    /// Celery task by the producer rule; `x.s(...)` / `.si(...)` / `.signature(...)` wrappers are
    /// looked through.
    pub(crate) fn producer_task(&self, object: Node, src: &[u8]) -> Option<String> {
        let mut obj = object;
        while obj.kind() == "call" {
            let f = obj.child_by_field_name("function")?;
            if f.kind() != "attribute"
                || !matches!(
                    compact(f.child_by_field_name("attribute")?, src).as_str(),
                    "s" | "si" | "signature" | "subtask"
                )
            {
                return None;
            }
            obj = f.child_by_field_name("object")?;
        }
        let segs = Self::path(obj, src)?;
        let root = segs[0].as_str();
        let task = segs[segs.len() - 1].clone();
        if segs.len() == 1 && self.local_tasks.contains(root) {
            return Some(task);
        }
        if self.defined.contains(root)
            || self.assigned.contains_key(root)
            || self.ambiguous.contains(root)
        {
            return None;
        }
        let i = self.names.get(root)?;
        (i.celery_like() || (!i.foreign() && (i.name.is_some() || segs.len() > 1))).then_some(task)
    }
}
