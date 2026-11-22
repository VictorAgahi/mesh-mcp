#!/usr/bin/env python3
"""Validate tests/golden/events/*.expected.yaml (stdlib only).

Checks per file: structure and types (never a traceback: malformed input is reported and the
exit code is 1), enum values, unique ids across all categories, `optional` is a bool that is
allowed only on producers, consumers and hops and goes with the tag `declared_pattern` both ways, every `file` exists under `root`,
every `line` is inside its file, and that the line carries the name it claims:

  * producer / consumer / consume / produce step: the topic, matched on identifier and segment
    boundaries (`stock` does not satisfy `stock.reserved`), case-insensitive; waived for
    `config_topic` items and for lines that hold a `${...}` placeholder;
  * hop `from` and `to`: the callee name (last segment of `to.symbol`);
  * `call` step: it must be the declaration line of a hop's `to` (same file and line); the entry
    handler of a route / cron / handler chain, as first step, must be a non-blank line (and carry
    the handler name for a cron or handler entry);
  * every consume / produce step must be the same file, topic and line as a consumer / producer
    item (so a chain never invents a site), and must not be an `optional` item;
  * `min_hops` equals the number of `call` steps.

Usage: check_golden.py [--table] [--no-pyyaml] [golden.yaml ...]   (default: every file here)
PyYAML is used when importable (and --no-pyyaml is absent); otherwise a built-in reader for the
subset the golden files use (block maps and lists, one-line flow maps and lists, scalars).
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]

LANGS = {"ts", "js", "java", "go", "python", "csharp", "rust", "sql"}
TAGS = {
    "kafka_basic", "layered_calls", "inproc_bus", "cqrs", "cron", "sql", "config_topic",
    "broker_key", "jvm_broker", "go_broker", "py_broker", "cs_broker", "declared_pattern",
}
VIA = {"this", "injected_field", "imported_function", "same_module", "interface_impl"}
ENTRY_KINDS = {"topic", "route", "handler", "cron"}
STEP_KINDS = {"consume", "call", "produce"}
NEG_KINDS = {"producer", "consumer"}
# Broker qualifiers of a topic key (mesh_core::Broker::slug; a Rust test keeps the two in sync).
BROKER_SLUGS = (
    "kafka", "rabbitmq", "redis", "nats", "mqtt", "sqs", "sns", "kinesis", "eventbridge",
    "pubsub", "pulsar", "servicebus", "eventhubs", "stomp", "bull", "beequeue", "inproc", "jms",
)


# ---------------------------------------------------------------- minimal YAML reader
def _strip_comment(s):
    quote = None
    for i, ch in enumerate(s):
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch == "#" and (i == 0 or s[i - 1] in " \t"):
            return s[:i].rstrip()
    return s.rstrip()


def _scalar(tok):
    tok = tok.strip()
    if len(tok) >= 2 and tok[0] == tok[-1] and tok[0] in "'\"":
        return tok[1:-1]
    if tok in ("null", "~", ""):
        return None
    if tok in ("true", "false"):
        return tok == "true"
    if re.fullmatch(r"-?\d+", tok):
        return int(tok)
    return tok


def _flow(s, i=0):
    while s[i] == " ":
        i += 1
    if s[i] == "{":
        i += 1
        obj = {}
        while True:
            while s[i] in " ,":
                i += 1
            if s[i] == "}":
                return obj, i + 1
            m = re.compile(r"""\s*(["']?)([\w.\-]+)\1\s*:\s*""").match(s, i)
            if not m:
                raise ValueError(f"bad flow mapping near {s[i:i + 40]!r}")
            obj[m.group(2)], i = _flow(s, m.end())
    if s[i] == "[":
        i += 1
        arr = []
        while True:
            while s[i] in " ,":
                i += 1
            if s[i] == "]":
                return arr, i + 1
            val, i = _flow(s, i)
            arr.append(val)
    if s[i] in "'\"":
        j = s.index(s[i], i + 1)
        return s[i + 1:j], j + 1
    j = i
    while j < len(s) and s[j] not in ",}]":
        j += 1
    return _scalar(s[i:j]), j


def _value(s):
    s = s.strip()
    if s[:1] in ("{", "["):
        val, end = _flow(s)
        if s[end:].strip():
            raise ValueError(f"trailing text after flow value: {s!r}")
        return val
    return _scalar(s)


_KEY = re.compile(r"^([A-Za-z_][\w.\-]*):(?:\s+(.*))?$")


def mini_yaml(text):
    lines = []
    for raw in text.splitlines():
        s = _strip_comment(raw)
        if s.strip():
            lines.append([len(s) - len(s.lstrip(" ")), s.strip()])
    if not lines:
        raise ValueError("empty file")

    def is_item(t):
        return t.startswith("- ") or t == "-"

    def block(i, indent):
        return seq(i, indent) if is_item(lines[i][1]) else mapping(i, indent)

    def seq(i, indent):
        out = []
        while i < len(lines) and lines[i][0] == indent and is_item(lines[i][1]):
            rest = lines[i][1][1:].lstrip(" ")
            if not rest:
                val, i = block(i + 1, lines[i + 1][0])
            elif _KEY.match(rest):
                lines[i] = [indent + len(lines[i][1]) - len(rest), rest]
                val, i = mapping(i, lines[i][0])
            else:
                val, i = _value(rest), i + 1
            out.append(val)
        return out, i

    def mapping(i, indent):
        out = {}
        while i < len(lines) and lines[i][0] == indent and not is_item(lines[i][1]):
            m = _KEY.match(lines[i][1])
            if not m:
                raise ValueError(f"unsupported YAML line: {lines[i][1]!r}")
            key, rest = m.group(1), m.group(2)
            if rest is None or not rest.strip():
                nxt = lines[i + 1] if i + 1 < len(lines) else None
                if nxt and (nxt[0] > indent or (nxt[0] == indent and is_item(nxt[1]))):
                    out[key], i = block(i + 1, nxt[0])
                else:
                    out[key], i = None, i + 1
            else:
                out[key], i = _value(rest), i + 1
        return out, i

    val, end = block(0, lines[0][0])
    if end != len(lines):
        raise ValueError(f"unparsed YAML near {lines[end][1]!r}")
    return val


def load(path, use_pyyaml=True):
    text = Path(path).read_text()
    if use_pyyaml:
        try:
            import yaml  # noqa: PLC0415
        except ImportError:
            yaml = None
        if yaml is not None:
            return yaml.safe_load(text)
    return mini_yaml(text)


def parser_name(use_pyyaml=True):
    if use_pyyaml:
        try:
            import yaml  # noqa: F401, PLC0415
            return "PyYAML"
        except ImportError:
            pass
    return "built-in reader"


# ---------------------------------------------------------------------------- matching
_TOPIC_BEFORE = r"(?<![A-Za-z0-9_\-.:])"
# After a topic: not an identifier char, and not `.x` / `:x` (a longer topic); `.delay(` and
# `.apply_async(` are allowed because a Celery producer names its topic that way.
_TOPIC_AFTER = r"(?![A-Za-z0-9_\-]|[.:](?!delay\b|apply_async\b)[A-Za-z0-9_])"


def source_topic(topic, tags):
    """The name a line of source carries for `topic`. Under the tag `broker_key` a topic is
    written `<broker>:<name>` when the name is used on two or more brokers of the corpus
    (SCHEMA.md "Broker-qualified keys"); the qualifier is not in the source."""
    if "broker_key" in (tags or []) and isinstance(topic, str):
        slug, sep, rest = topic.partition(":")
        if sep and rest and slug.lower() in BROKER_SLUGS:
            return rest
    return topic


# An ARN (`arn:aws:sns:eu-west-1:123456789012:orders`) names its topic after a colon.
_TOPIC_BEFORE_ARN = r"(?<![A-Za-z0-9_\-.])"


def has_topic(line, topic, arn=False):
    before = _TOPIC_BEFORE_ARN if arn else _TOPIC_BEFORE
    return re.search(before + re.escape(str(topic)) + _TOPIC_AFTER, line, re.I) is not None


def has_symbol(line, name):
    return re.search(r"(?<![A-Za-z0-9_])" + re.escape(str(name)) + r"(?![A-Za-z0-9_])", line) is not None


class Checker:
    def __init__(self, path, repo=REPO, use_pyyaml=True):
        self.path = Path(path)
        self.repo = Path(repo)
        self.use_pyyaml = use_pyyaml
        self.errors = []
        self.cache = {}

    def err(self, where, msg):
        self.errors.append(f"{self.path.name}: {where}: {msg}")

    def lines_of(self, root, rel):
        key = (root, rel)
        if key not in self.cache:
            p = root / rel
            self.cache[key] = p.read_text(errors="replace").splitlines() if p.is_file() else None
        return self.cache[key]

    def line_text(self, where, root, file, line):
        """The text of file:line, or None after recording why it is unusable."""
        if not isinstance(file, str) or not file or isinstance(line, bool) or not isinstance(line, int):
            self.err(where, f"file/line missing or mistyped ({file!r}, {line!r})")
            return None
        src = self.lines_of(root, file)
        if src is None:
            self.err(where, f"file does not exist under root: {file}")
            return None
        if not 1 <= line <= len(src):
            self.err(where, f"line {line} outside {file} (1..{len(src)})")
            return None
        return src[line - 1]

    def need(self, where, text, test, what, exempt=False):
        if text is None or exempt:
            return
        if not text.strip():
            self.err(where, f"line is blank (expected {what})")
        elif not test(text):
            self.err(where, f"line lacks {what}: {text.strip()!r}")

    def listing(self, g, key):
        """g[key] as a list of dicts; anything else is an error and yields []."""
        val = g.get(key)
        if val is None:
            return []
        if not isinstance(val, list):
            self.err(key, f"must be a list, got {type(val).__name__}")
            return []
        out = []
        for n, it in enumerate(val, 1):
            if isinstance(it, dict):
                out.append(it)
            else:
                self.err(f"{key}[{n}]", f"must be a mapping, got {it!r}")
        return out

    def check(self):
        g = load(self.path, self.use_pyyaml)
        if not isinstance(g, dict):
            self.err("file", "top level is not a mapping")
            return None
        for k in ("corpus", "root"):
            if not g.get(k) or not isinstance(g.get(k), str):
                self.err(k, "required string")
        if g.get("corpus") != self.path.name.removesuffix(".expected.yaml"):
            self.err("corpus", f"{g.get('corpus')!r} does not match the file name")
        root = self.repo / str(g.get("root") or "")
        if not g.get("root") or not root.is_dir():
            self.err("root", f"directory {g.get('root')!r} does not exist")
            return g
        if g.get("config") and not (root / str(g["config"])).is_file():
            self.err("config", f"{g['config']} not found under root")
        ids = set()

        def common(where, item):
            i = item.get("id")
            if not i or not isinstance(i, str):
                self.err(where, "missing id")
            elif i in ids:
                self.err(where, f"duplicate id {i}")
            ids.add(i)
            tags = item.get("tags") or []
            if not isinstance(tags, list):
                self.err(where, "tags must be a list")
                tags = []
            for t in tags:
                if t not in TAGS:
                    self.err(where, f"unknown tag {t}")
            if "optional" in item and not isinstance(item["optional"], bool):
                self.err(where, f"optional must be a bool, got {item['optional']!r}")
            if "declared_pattern" in tags and item.get("optional") is not True:
                self.err(where, "tag declared_pattern requires optional: true")
            if item.get("optional") is True and "declared_pattern" not in tags:
                self.err(where, "optional: true requires the tag declared_pattern")
            return tags

        sites = {"producers": {}, "consumers": {}}  # (file, topic.lower()) -> [(line, optional)]
        for kind in ("producers", "consumers"):
            for n, it in enumerate(self.listing(g, kind), 1):
                w = f"{kind}/{it.get('id', n)}"
                tags = common(w, it)
                topic = it.get("topic")
                if not topic or not isinstance(topic, (str, int)):
                    self.err(w, "missing topic")
                    continue
                if it.get("language") not in LANGS:
                    self.err(w, f"bad language {it.get('language')!r}")
                text = self.line_text(w, root, it.get("file"), it.get("line"))
                # A declared_pattern site is emitted by a custom pattern (often at the enclosing
                # function), so its line need not carry the topic.
                self.need(w, text, lambda t, tp=source_topic(topic, tags): has_topic(t, tp, "broker_key" in tags),
                          f"topic {topic!r}",
                          "config_topic" in tags or "declared_pattern" in tags
                          or (text is not None and "${" in text))
                sites[kind].setdefault((it.get("file"), str(topic).lower()), []).append(
                    (it.get("line"), it.get("optional") is True))
        hop_targets = set()
        for n, it in enumerate(self.listing(g, "hops"), 1):
            w = f"hops/{it.get('id', n)}"
            common(w, it)
            if it.get("via") not in VIA:
                self.err(w, f"bad via {it.get('via')!r}")
            frm, to = it.get("from"), it.get("to")
            if not isinstance(frm, dict) or not isinstance(to, dict):
                self.err(w, "from and to must be mappings")
                continue
            if not frm.get("symbol") or not to.get("symbol"):
                self.err(w, "from/to need a symbol")
                continue
            callee = str(to["symbol"]).split(".")[-1]
            for side, d in (("from", frm), ("to", to)):
                text = self.line_text(f"{w}/{side}", root, d.get("file"), d.get("line"))
                self.need(f"{w}/{side}", text, lambda t: has_symbol(t, callee), f"callee {callee!r}")
            hop_targets.add((to.get("file"), to.get("line")))
        for n, it in enumerate(self.listing(g, "chains"), 1):
            w = f"chains/{it.get('id', n)}"
            common(w, it)
            if "optional" in it:
                self.err(w, "optional is not allowed on chains (only on producers, consumers and hops)")
            e = it.get("entry")
            if not isinstance(e, dict) or e.get("kind") not in ENTRY_KINDS or not e.get("value"):
                self.err(w, f"bad entry {e!r}")
                e = {}
            if it.get("direction") not in ("forward", "reverse"):
                self.err(w, f"bad direction {it.get('direction')!r}")
            steps = it.get("steps")
            if not isinstance(steps, list) or not steps:
                self.err(w, "steps must be a non-empty list")
                continue
            calls = 0
            for k, st in enumerate(steps):
                sw = f"{w}/step{k + 1}"
                if not isinstance(st, dict) or st.get("kind") not in STEP_KINDS:
                    self.err(sw, f"bad step {st!r}")
                    continue
                kind = st["kind"]
                if "optional" in st:
                    self.err(sw, "optional is not allowed on a chain step")
                text = self.line_text(sw, root, st.get("file"), st.get("line"))
                if kind == "call":
                    calls += 1
                    entry_step = k == 0 and e.get("kind") in ("route", "cron", "handler")
                    if entry_step:
                        name = str(e.get("value", "")).split(".")[-1]
                        if e.get("kind") == "route":
                            self.need(sw, text, lambda t: "(" in t, "a handler declaration")
                        else:
                            self.need(sw, text, lambda t: has_symbol(t, name), f"handler {name!r}")
                    elif text is not None and not text.strip():
                        self.err(sw, f"call step {st.get('file')}:{st.get('line')} is a blank line")
                    elif text is not None and (st.get("file"), st.get("line")) not in hop_targets:
                        self.err(sw, f"call step {st.get('file')}:{st.get('line')} is not the "
                                     "declaration line (hop `to`) of any hop")
                else:
                    topic = st.get("topic")
                    if not topic:
                        self.err(sw, "consume/produce step needs a topic")
                        continue
                    tags = it.get("tags") or []
                    self.need(sw, text, lambda t: has_topic(t, source_topic(topic, tags), "broker_key" in tags),
                              f"topic {topic!r}",
                              "config_topic" in tags or (text is not None and "${" in text))
                    pool = sites["producers" if kind == "produce" else "consumers"]
                    hits = [o for ln, o in pool.get((st.get("file"), str(topic).lower()), [])
                            if ln == st.get("line")]
                    if not hits:
                        self.err(sw, f"no {'producer' if kind == 'produce' else 'consumer'} item at "
                                     f"{st.get('file')}:{st.get('line')} for topic {topic!r}")
                    elif all(hits):
                        self.err(sw, "step depends on an optional item")
            if it.get("min_hops") != calls:
                self.err(w, f"min_hops {it.get('min_hops')} != number of call steps {calls}")
        for n, it in enumerate(self.listing(g, "negatives"), 1):
            w = f"negatives/{n}"
            if "optional" in it:
                self.err(w, "optional is not allowed on negatives")
            if it.get("kind") not in NEG_KINDS:
                self.err(w, f"bad kind {it.get('kind')!r}")
            self.line_text(w, root, it.get("file"), it.get("line"))
        return g


def table(goldens):
    rows = ["| Corpus | Languages | Producers | Consumers | Hops | Chains | Chains >= 3 call steps | Optional items | Tags (items) |",
            "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |"]
    for g in goldens:
        tag_count, optional = {}, 0
        for kind in ("producers", "consumers", "hops", "chains"):
            for it in g.get(kind) or []:
                optional += it.get("optional") is True
                for t in it.get("tags") or []:
                    tag_count[t] = tag_count.get(t, 0) + 1
        langs = sorted({it.get("language") for k in ("producers", "consumers") for it in g.get(k) or []})
        deep = sum(1 for c in g.get("chains") or [] if (c.get("min_hops") or 0) >= 3)
        rows.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            g["corpus"], ", ".join(langs), len(g.get("producers") or []), len(g.get("consumers") or []),
            len(g.get("hops") or []), len(g.get("chains") or []), deep, optional,
            ", ".join(f"{t} {n}" for t, n in sorted(tag_count.items()))))
    return "\n".join(rows)


def run_one(path, repo=REPO, use_pyyaml=True):
    """Check one file; returns (errors, golden). Never raises on malformed input."""
    c = Checker(path, repo, use_pyyaml)
    g = None
    try:
        g = c.check()
    except Exception as exc:  # noqa: BLE001 - any malformed input must be a message, not a traceback
        c.err("input", f"{type(exc).__name__}: {exc}")
    return c.errors, g


def main(argv):
    show_table = "--table" in argv
    use_pyyaml = "--no-pyyaml" not in argv
    paths = [Path(a) for a in argv if not a.startswith("--")] or sorted(HERE.glob("*.expected.yaml"))
    print(f"parser: {parser_name(use_pyyaml)}")
    failed, goldens = 0, []
    for p in paths:
        errors, g = run_one(p, REPO, use_pyyaml)
        if isinstance(g, dict) and isinstance(g.get("corpus"), str) and not errors:
            goldens.append(g)
        for e in errors:
            print("ERROR", e)
        failed += bool(errors)
        if not errors:
            print(f"ok   {p.name}")
    if show_table and goldens:
        print()
        print(table(goldens))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
