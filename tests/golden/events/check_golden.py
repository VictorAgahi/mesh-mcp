#!/usr/bin/env python3
"""Validate tests/golden/events/*.expected.yaml (stdlib only).

Checks per file: schema keys and enum values, unique ids, every `file` exists under `root`,
every `line` is inside the file, and, where a topic or symbol name is given, that the line
carries it (case-insensitive, punctuation ignored). A topic that is configuration-driven
(tag `config_topic`) is exempt from the token check, because its name is not in the line.

Usage: check_golden.py [--table] [golden.yaml ...]   (default: every file in this directory)
Exit code 0 when every file passes, 1 otherwise.

PyYAML is used when importable; otherwise a small reader for the subset the golden files
use (block maps and lists, one-line flow maps and lists, plain or quoted scalars).
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]

LANGS = {"ts", "js", "java", "go", "python", "csharp", "rust", "sql"}
TAGS = {
    "kafka_basic", "layered_calls", "inproc_bus", "cqrs", "cron", "sql", "config_topic",
    "broker_key", "jvm_broker", "go_broker", "py_broker", "cs_broker",
}
VIA = {"this", "injected_field", "imported_function", "same_module", "interface_impl"}
ENTRY_KINDS = {"topic", "route", "handler", "cron"}
STEP_KINDS = {"consume", "call", "produce"}
NEG_KINDS = {"producer", "consumer"}


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


def load(path):
    text = path.read_text()
    try:
        import yaml  # noqa: PLC0415
        return yaml.safe_load(text)
    except ImportError:
        return mini_yaml(text)


# ---------------------------------------------------------------------------- checks
def norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


class Checker:
    def __init__(self, path):
        self.path = path
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

    def loc(self, where, root, file, line, tokens=(), exempt=False):
        if not isinstance(file, str) or not isinstance(line, int):
            self.err(where, f"file/line missing or mistyped ({file!r}, {line!r})")
            return
        src = self.lines_of(root, file)
        if src is None:
            self.err(where, f"file does not exist under root: {file}")
            return
        if not 1 <= line <= len(src):
            self.err(where, f"line {line} outside {file} (1..{len(src)})")
            return
        tokens = [t for t in tokens if t]
        exempt = exempt or "${" in src[line - 1]  # a placeholder topic: the name is in the config
        if tokens and not exempt and not any(norm(t) in norm(src[line - 1]) for t in tokens):
            self.err(where, f"{file}:{line} does not contain {tokens}: {src[line - 1].strip()!r}")

    def check(self):
        g = load(self.path)
        if not isinstance(g, dict):
            self.err("file", "top level is not a mapping")
            return None
        for k in ("corpus", "root"):
            if not g.get(k):
                self.err(k, "required")
        if g.get("corpus") != self.path.name.removesuffix(".expected.yaml"):
            self.err("corpus", f"{g.get('corpus')!r} does not match the file name")
        root = REPO / str(g.get("root", ""))
        if not root.is_dir():
            self.err("root", f"directory {g.get('root')} does not exist")
            return g
        if g.get("config") and not (root / g["config"]).is_file():
            self.err("config", f"{g['config']} not found under root")
        ids = set()

        def uid(where, item):
            i = item.get("id")
            if not i:
                self.err(where, "missing id")
            elif i in ids:
                self.err(where, f"duplicate id {i}")
            ids.add(i)

        def tags(where, item):
            for t in item.get("tags") or []:
                if t not in TAGS:
                    self.err(where, f"unknown tag {t}")

        for kind in ("producers", "consumers"):
            for it in g.get(kind) or []:
                w = f"{kind}/{it.get('id')}"
                uid(w, it)
                tags(w, it)
                if not it.get("topic"):
                    self.err(w, "missing topic")
                if it.get("language") not in LANGS:
                    self.err(w, f"bad language {it.get('language')!r}")
                self.loc(w, root, it.get("file"), it.get("line"), [it.get("topic")],
                         "config_topic" in (it.get("tags") or []))
        for it in g.get("hops") or []:
            w = f"hops/{it.get('id')}"
            uid(w, it)
            tags(w, it)
            if it.get("via") not in VIA:
                self.err(w, f"bad via {it.get('via')!r}")
            frm, to = it.get("from") or {}, it.get("to") or {}
            callee = str(to.get("symbol", "")).split(".")[-1]
            self.loc(w + "/from", root, frm.get("file"), frm.get("line"), [callee])
            self.loc(w + "/to", root, to.get("file"), to.get("line"), [callee])
            if not frm.get("symbol") or not to.get("symbol"):
                self.err(w, "from/to need a symbol")
        for it in g.get("chains") or []:
            w = f"chains/{it.get('id')}"
            uid(w, it)
            tags(w, it)
            e = it.get("entry") or {}
            if e.get("kind") not in ENTRY_KINDS or not e.get("value"):
                self.err(w, f"bad entry {e!r}")
            if it.get("direction") not in ("forward", "reverse"):
                self.err(w, f"bad direction {it.get('direction')!r}")
            steps = it.get("steps") or []
            if len(steps) < 2 and not (steps and e.get("kind") != "topic"):
                self.err(w, "a chain needs at least two steps")
            calls = 0
            for n, st in enumerate(steps):
                sw = f"{w}/step{n + 1}"
                if st.get("kind") not in STEP_KINDS:
                    self.err(sw, f"bad kind {st.get('kind')!r}")
                    continue
                calls += st["kind"] == "call"
                toks = [st.get("topic")] if st["kind"] != "call" else []
                self.loc(sw, root, st.get("file"), st.get("line"), toks, "config_topic" in (it.get("tags") or []))
            if it.get("min_hops") != calls:
                self.err(w, f"min_hops {it.get('min_hops')} != number of call steps {calls}")
        for n, it in enumerate(g.get("negatives") or []):
            w = f"negatives/{n + 1}"
            if it.get("kind") not in NEG_KINDS:
                self.err(w, f"bad kind {it.get('kind')!r}")
            self.loc(w, root, it.get("file"), it.get("line"))
        return g


def table(goldens):
    rows = ["| Corpus | Languages | Producers | Consumers | Hops | Chains | Chains >= 3 call steps | Tags (items) |",
            "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- |"]
    for g in goldens:
        tag_count = {}
        for kind in ("producers", "consumers", "hops", "chains"):
            for it in g.get(kind) or []:
                for t in it.get("tags") or []:
                    tag_count[t] = tag_count.get(t, 0) + 1
        langs = sorted({it.get("language") for k in ("producers", "consumers") for it in g.get(k) or []})
        deep = sum(1 for c in g.get("chains") or [] if (c.get("min_hops") or 0) >= 3)
        rows.append("| {} | {} | {} | {} | {} | {} | {} | {} |".format(
            g["corpus"], ", ".join(langs), len(g.get("producers") or []), len(g.get("consumers") or []),
            len(g.get("hops") or []), len(g.get("chains") or []), deep,
            ", ".join(f"{t} {n}" for t, n in sorted(tag_count.items()))))
    return "\n".join(rows)


def main(argv):
    show_table = "--table" in argv
    paths = [Path(a) for a in argv if not a.startswith("--")] or sorted(HERE.glob("*.expected.yaml"))
    failed, goldens = 0, []
    for p in paths:
        c = Checker(p)
        try:
            g = c.check()
        except (ValueError, IndexError, KeyError) as exc:
            c.err("parse", str(exc))
            g = None
        if g:
            goldens.append(g)
        for e in c.errors:
            print("ERROR", e)
        failed += bool(c.errors)
        if not c.errors:
            print(f"ok   {p.name}")
    if show_table:
        print()
        print(table(goldens))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
