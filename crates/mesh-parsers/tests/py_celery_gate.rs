//! Celery import gate (9.0 T15) through the real dispatch (`PolyglotIndexer`). A decorator or a
//! `.delay()` is a Celery site only when the file's parsed imports say so; a Locust `@task`, a
//! local decorator named `task` and a `task` of another library are no consumers.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::NodeKind;
use mesh_parsers::languages::PolyglotIndexer;
use mesh_parsers::FileIndex;
use std::path::Path;

fn extract(code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new("src/svc/tasks.py"), code, 0);
    assert!(!fi.parse_failed, "must parse");
    fi
}

fn consumers(code: &str) -> Vec<String> {
    let mut v: Vec<String> = extract(code)
        .consumers
        .iter()
        .map(|(_, t)| t.to_string())
        .collect();
    v.sort();
    v
}

fn producers(code: &str) -> Vec<String> {
    let mut v: Vec<String> = extract(code)
        .producers
        .iter()
        .map(|(_, t)| t.to_string())
        .collect();
    v.sort();
    v
}

fn none() -> Vec<String> {
    Vec::new()
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

// ---- positives: consumers ---------------------------------------------------------------

#[test]
fn app_created_from_celery_in_the_same_file() {
    let code = r#"
from celery import Celery

app = Celery("proj", broker="redis://localhost:6379/0")

@app.task
def send_email(to):
    pass

@app.task(bind=True)
def resize(self, img):
    pass
"#;
    assert_eq!(consumers(code), names(&["resize", "send_email"]));
}

#[test]
fn import_celery_and_celery_dot_task() {
    let code = r#"
import celery

@celery.task
def a():
    pass
"#;
    assert_eq!(consumers(code), names(&["a"]));
}

#[test]
fn shared_task_bare_called_and_aliased() {
    let code = r#"
from celery import shared_task, shared_task as st

@shared_task
def one():
    pass

@shared_task(name="two.v2")
def two():
    pass

@st(bind=True)
def three(self):
    pass
"#;
    assert_eq!(consumers(code), names(&["one", "three", "two.v2"]));
}

#[test]
fn old_style_celery_task_import() {
    let code = r#"
from celery.decorators import task

@task(name="legacy")
def old():
    pass
"#;
    assert_eq!(consumers(code), names(&["legacy"]));
}

#[test]
fn cross_file_app_import_keeps_working() {
    // `app` is created in `proj/celery.py`; this file never imports celery itself.
    assert_eq!(
        consumers("from proj.celery import app\n\n@app.task\ndef a():\n    pass\n"),
        names(&["a"])
    );
    assert_eq!(
        consumers("from .celery import app\n\n@app.task(name=\"x\")\ndef a():\n    pass\n"),
        names(&["x"])
    );
    assert_eq!(
        consumers("from proj import celery_app\n\n@celery_app.task\ndef a():\n    pass\n"),
        names(&["a"])
    );
    assert_eq!(
        consumers("from myapp.tasks import app\n\n@app.task\ndef a():\n    pass\n"),
        names(&["a"])
    );
}

#[test]
fn task_name_keyword_is_the_consumer_key() {
    let code = r#"
from celery import Celery
app = Celery()

@app.task(name="x")
def f():
    pass
"#;
    assert_eq!(consumers(code), names(&["x"]));
}

#[test]
fn a_gated_task_is_a_queue_node() {
    let fi =
        extract("from celery import Celery\napp = Celery()\n\n@app.task\ndef f():\n    pass\n");
    assert!(fi
        .nodes
        .iter()
        .any(|n| n.name == "f" && n.kind == NodeKind::Queue));
}

// ---- negatives: consumers ---------------------------------------------------------------

#[test]
fn locust_task_is_no_consumer() {
    let code = r#"
from locust import HttpUser, task, between

class WebsiteUser(HttpUser):
    wait_time = between(1, 10)

    @task
    def browse_product(self):
        self.client.get("/api/products/1")

    @task(3)
    def index(self):
        self.client.get("/")
"#;
    assert_eq!(consumers(code), none());
    let fi = extract(code);
    assert!(
        fi.nodes.iter().all(|n| n.kind != NodeKind::Queue),
        "a Locust task is no queue node"
    );
}

#[test]
fn locust_module_task_is_no_consumer() {
    let code = r#"
import locust

class U(locust.HttpUser):
    @locust.task
    def a(self):
        pass
"#;
    assert_eq!(consumers(code), none());
}

#[test]
fn a_local_decorator_named_task_is_no_consumer() {
    let code = r#"
def task(fn):
    return fn

@task
def a():
    pass
"#;
    assert_eq!(consumers(code), none());
}

#[test]
fn a_task_defined_by_assignment_in_the_file_is_no_consumer() {
    let code = r#"
from celery import shared_task
shared_task = my_registry.register

@shared_task
def a():
    pass
"#;
    assert_eq!(consumers(code), none());
}

#[test]
fn task_imported_from_another_library_is_no_consumer() {
    for code in [
        "from other import task\n\n@task\ndef a():\n    pass\n",
        "from other import task as t\n\n@t\ndef a():\n    pass\n",
        "from other.jobs import shared_task\n\n@shared_task\ndef a():\n    pass\n",
        "@task\ndef a():\n    pass\n",
        "@app.task\ndef a():\n    pass\n",
    ] {
        assert_eq!(consumers(code), none(), "{code}");
    }
}

#[test]
fn a_comment_or_string_naming_celery_does_not_open_the_gate() {
    let code = r#"
# from celery import Celery
"""
from celery import shared_task
"""
from other import app

@app.task
def a():
    pass
"#;
    assert_eq!(consumers(code), none());
}

#[test]
fn an_ungated_app_task_is_not_turned_into_a_route() {
    let fi = extract("from other import app\n\n@app.task\ndef a():\n    pass\n");
    assert!(fi
        .nodes
        .iter()
        .all(|n| n.kind != NodeKind::Queue && n.kind != NodeKind::HttpEndpoint));
}

#[test]
fn flask_routes_are_unchanged() {
    let code = "from flask import Flask\napp = Flask(__name__)\n\n@app.route(\"/x\")\ndef x():\n    pass\n";
    let fi = extract(code);
    assert!(fi
        .nodes
        .iter()
        .any(|n| n.name == "x" && n.kind == NodeKind::HttpEndpoint));
}

// ---- producers ---------------------------------------------------------------------------

#[test]
fn delay_and_apply_async_on_an_imported_task() {
    let code = r#"
from myapp.tasks import process_order, notify

def handle(order_id):
    process_order.delay(order_id)
    notify.apply_async(args=[1])
"#;
    assert_eq!(producers(code), names(&["notify", "process_order"]));
}

#[test]
fn delay_on_a_task_of_the_same_file() {
    let code = r#"
from celery import Celery
app = Celery()

@app.task
def a():
    pass

def go():
    a.delay()
"#;
    assert_eq!(producers(code), names(&["a"]));
}

#[test]
fn delay_in_a_file_that_imports_celery() {
    let code = "import celery\n\ndef go(t):\n    t.delay(1)\n";
    assert_eq!(producers(code), names(&["t"]));
}

#[test]
fn delay_without_celery_origin_is_no_producer() {
    for code in [
        // a timer object of another library
        "from rx import timer\n\ndef go():\n    timer.delay(1)\n",
        "def go(obj):\n    obj.apply_async(1)\n",
        // a name of this file that is not a task
        "def job():\n    pass\n\ndef go():\n    job.delay()\n",
        // a locust file
        "from locust import task\n\ndef go(self):\n    self.wait.delay(1)\n",
    ] {
        assert_eq!(producers(code), none(), "{code}");
    }
}

#[test]
fn a_non_celery_local_task_is_no_producer_target() {
    let code = r#"
from locust import task

@task
def browse():
    pass

def go():
    browse.delay()
"#;
    assert_eq!(producers(code), none());
    assert_eq!(consumers(code), none());
}

#[test]
fn delay_on_a_task_of_the_same_file_without_a_celery_import() {
    // the app comes from a celery module (cross-file); the task is called in the same file
    let code = r#"
from proj.celery import app

@app.task
def a():
    pass

def go():
    a.delay()
"#;
    assert_eq!(producers(code), names(&["a"]));
}

#[test]
fn only_task_decorators_count_in_a_celery_file() {
    let code = r#"
import celery
from flask import Flask
app = Flask(__name__)

@app.route("/x")
def x():
    pass

@app.errorhandler(404)
def nf(e):
    pass
"#;
    assert_eq!(consumers(code), none());
}

#[test]
fn a_bare_decorator_of_celery_that_is_not_a_task_decorator() {
    let code = "from celery import signature\n\n@signature\ndef a():\n    pass\n";
    assert_eq!(consumers(code), none());
}

#[test]
fn a_celery_import_opens_the_attribute_form_for_any_base() {
    let code = "import celery.app\nfrom elsewhere import app\n\n@app.task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["a"]));
}

#[test]
fn a_submodule_import_of_celery_opens_the_gate() {
    let code = "from celery.canvas import group\nfrom elsewhere import app\n\n@app.task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["a"]));
    assert_eq!(
        consumers(
            "import celery.app.base\nfrom elsewhere import app\n\n@app.task\ndef b():\n    pass\n"
        ),
        names(&["b"])
    );
}

// ---- known false negatives (documented, pinned) ------------------------------------------

#[test]
fn documented_false_negatives() {
    // app imported from a module whose path holds neither `celery` nor `tasks`, no celery import
    assert_eq!(
        consumers("from proj import app\n\n@app.task\ndef a():\n    pass\n"),
        none()
    );
    // task imported from a module such as `myapp.jobs`
    assert_eq!(
        producers("from myapp.jobs import run\n\ndef go():\n    run.delay()\n"),
        none()
    );
}
