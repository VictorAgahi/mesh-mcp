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

// ---- consumers: shadowing, Flask factory, token rules, shared_task ------------------------

#[test]
fn only_module_level_names_shadow_a_celery_task_import() {
    // a nested assignment and a method named `task` do not hide the module-level import
    let nested = r#"
from celery import task

def build():
    task = load()
    return task

class Runner:
    def task(self):
        pass

@task
def notify_user():
    pass
"#;
    assert_eq!(consumers(nested), names(&["notify_user"]));
    // a module-level def, assignment or guarded def does
    for code in [
        "from celery import task\n\ndef task(fn):\n    return fn\n\n@task\ndef a():\n    pass\n",
        "from celery import task\ntask = wrap(task)\n\n@task\ndef a():\n    pass\n",
        "from celery import task\ntry:\n    pass\nexcept ImportError:\n    def task(fn):\n        return fn\n\n@task\ndef a():\n    pass\n",
    ] {
        assert_eq!(consumers(code), none(), "{code}");
    }
}

#[test]
fn a_name_bound_to_two_imports_is_ambiguous() {
    let code = "from celery import shared_task\nfrom locust import shared_task\n\n@shared_task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), none());
}

#[test]
fn flask_make_celery_in_the_same_file() {
    let code = "celery = make_celery(app)\n\n@celery.task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["a"]));
    let code = "queue = create_celery(app)\n\n@queue.task(name=\"q\")\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["q"]));
    let code = "from celery import Celery as C\nqueue = C()\n\n@queue.task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["a"]));
}

#[test]
fn an_app_assigned_from_another_call_is_no_celery_app() {
    for code in [
        "app = Flask(__name__)\n\n@app.task\ndef a():\n    pass\n",
        "app = make_thing()\n\n@app.task\ndef a():\n    pass\n",
        "from celery import Celery\napp = Celery()\napp = Flask(__name__)\n\n@app.task\ndef a():\n    pass\n",
    ] {
        assert_eq!(consumers(code), none(), "{code}");
    }
}

#[test]
fn module_words_match_tokens_not_substrings() {
    for code in [
        "from mycelery_utils import app\n\n@app.task\ndef a():\n    pass\n",
        "from celeryman import app\n\n@app.task\ndef a():\n    pass\n",
        "from proj.subtasks import app\n\n@app.task\ndef a():\n    pass\n",
        "from proj.taskstore import app\n\n@app.task\ndef a():\n    pass\n",
        "from proj.tasksmith import app\n\n@app.task\ndef a():\n    pass\n",
        "from proj.shop_tasks import app\n\n@app.task\ndef a():\n    pass\n",
        "from invoke import tasks\n\n@tasks.task\ndef a():\n    pass\n",
        "from fabric import tasks\n\n@tasks.task\ndef a():\n    pass\n",
        "from locust.tasks import app\n\n@app.task\ndef a():\n    pass\n",
    ] {
        assert_eq!(consumers(code), none(), "{code}");
    }
    for code in [
        "from proj.my_celery import app\n\n@app.task\ndef a():\n    pass\n",
        "from proj import celery_app\n\n@celery_app.task\ndef a():\n    pass\n",
        "from proj.celery_app import app\n\n@app.task\ndef a():\n    pass\n",
    ] {
        assert_eq!(consumers(code), names(&["a"]), "{code}");
    }
}

#[test]
fn importing_celery_elsewhere_does_not_open_the_attribute_form() {
    for code in [
        "import celery\nimport locust\n\nclass U(locust.HttpUser):\n    @locust.task\n    def a(self):\n        pass\n",
        "import celery.app\nfrom elsewhere import app\n\n@app.task\ndef a():\n    pass\n",
        "from celery import Celery\nfrom locust import HttpUser, task\n\nclass U(HttpUser):\n    @task\n    def a(self):\n        pass\n",
    ] {
        assert_eq!(consumers(code), none(), "{code}");
    }
}

#[test]
fn app_shared_task_and_x_shared_task_are_consumers() {
    let code = "from celery import Celery\napp = Celery()\n\n@app.shared_task\ndef a():\n    pass\n\n@app.shared_task(name=\"b.v2\")\ndef b():\n    pass\n";
    assert_eq!(consumers(code), names(&["a", "b.v2"]));
    let code =
        "from proj.celery import app\nimport celery\n\n@celery.shared_task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["a"]));
}

#[test]
fn a_decorated_class_is_no_consumer() {
    let code = "from celery import Celery\napp = Celery()\n\n@app.task\nclass Job:\n    pass\n";
    assert_eq!(consumers(code), none());
}

#[test]
fn only_task_decorators_count_in_a_celery_file() {
    let code = r#"
from celery import Celery
from flask import Flask
app = Flask(__name__)
queue = Celery()

@app.route("/x")
def x():
    pass

@queue.on_after_configure
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
fn import_order_does_not_decide_an_ambiguous_name() {
    let code = "from locust import shared_task\nfrom celery import shared_task\n\n@shared_task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), none());
}

#[test]
fn an_app_name_defined_in_the_file_is_not_the_imported_app() {
    let code =
        "from proj.celery import app\n\ndef app():\n    pass\n\n@app.task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), none());
}

#[test]
fn a_celery_named_base_is_read_even_when_bound_by_a_star_import() {
    let code = "from ext import *\n\n@celery.task\ndef a():\n    pass\n";
    assert_eq!(consumers(code), names(&["a"]));
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
fn delay_on_a_from_imported_name_of_any_module() {
    let code = r#"
from notify.mailers import deliver_push, deliver_sms
from billing.jobs import invoice_monthly as monthly

def go():
    deliver_push.delay()
    deliver_sms.apply_async(args=[1])
    monthly.delay()
"#;
    // the key is the receiver as written (the local alias)
    assert_eq!(
        producers(code),
        names(&["deliver_push", "deliver_sms", "monthly"])
    );
}

#[test]
fn delay_through_signature_helpers_and_module_chains() {
    let code = r#"
import shop_tasks
from shop import jobs

def go():
    shop_tasks.charge_card.s(2).delay()
    jobs.refund.si(1).apply_async()
    jobs.ship.signature((1,)).delay()
    shop_tasks.email.delay()
"#;
    assert_eq!(
        producers(code),
        names(&["charge_card", "email", "refund", "ship"])
    );
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
fn delay_without_a_known_origin_is_no_producer() {
    for code in [
        // a parameter, even in a file that imports celery
        "import celery\n\ndef go(t):\n    t.delay(1)\n",
        "def go(obj):\n    obj.apply_async(1)\n",
        // a local instance
        "from rx import make\n\ndef go():\n    timer = make()\n    timer.delay(1)\n",
        "def go(self):\n    self.timer.delay(1)\n",
        // a def of this file that is not a task, a module-level instance
        "def job():\n    pass\n\ndef go():\n    job.delay()\n",
        "from rx import make\ntimer = make()\n\ndef go():\n    timer.delay(1)\n",
        // foreign packages and a plain module import
        "from locust import events\n\ndef go():\n    events.delay(1)\n",
        "from multiprocessing import pool\n\ndef go():\n    pool.apply_async(1)\n",
        "import rx\n\ndef go():\n    rx.delay(1)\n",
        // a call that is not a signature helper
        "from shop import jobs\n\ndef go():\n    jobs.make().delay()\n",
    ] {
        assert_eq!(producers(code), none(), "{code}");
    }
}

#[test]
fn a_rebound_import_is_no_producer() {
    let code = "from rx import timer\ntimer = wrap(timer)\n\ndef go():\n    timer.delay(1)\n";
    assert_eq!(producers(code), none());
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

// ---- known limits (documented, pinned) ---------------------------------------------------

#[test]
fn documented_limits() {
    // app imported from a module whose path holds neither `celery` nor a `tasks` segment
    assert_eq!(
        consumers("from proj import app\n\n@app.task\ndef a():\n    pass\n"),
        none()
    );
    // `from celery import *` binds no name
    assert_eq!(
        consumers("from celery import *\n\n@shared_task\ndef a():\n    pass\n"),
        none()
    );
    // a from-imported object of another library is read as a task (precision cost of the rule)
    assert_eq!(
        producers("from rx import timer\n\ndef go():\n    timer.delay(1)\n"),
        names(&["timer"])
    );
}
