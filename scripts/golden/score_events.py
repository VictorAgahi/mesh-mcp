#!/usr/bin/env python3
"""scripts/golden/score_events.py -- precision/recall of mesh-mcp's event-flow
extraction (producers, consumers, call hops, end-to-end chains) against the
hand-written golden files of tests/golden/events/<corpus>.expected.yaml.

The golden schema and the matching rules are specified in
tests/golden/events/SCHEMA.md; this script implements them exactly.

usage:
    score_events.py GOLDEN [GOLDEN ...] --binary PATH/mesh-mcp
                    [--chain-tool analyze_impact|trace_flow] [--json]
                    [--include-tests] [--no-chains]
                    [--fail-under-precision P] [--fail-under-recall R]

Where each number comes from
- producers / consumers / hops: `mesh-mcp graph --format json` run on the
  corpus root (Produces / Consumes edges and their topic nodes; `Calls` edges
  for hops, present from 9.x, absent on 8.x, in which case hop recall is 0 by
  construction and no error is raised).
- chains: one `run --standalone` MCP session per corpus; for each chain the
  chain tool is called on the entry (analyze_impact(target=entry.value,
  depth=5, limit=200), following the answer's "offset" footer for further
  pages) and the ordered `path:line` occurrences of the Markdown answer are
  compared with the chain steps. `--chain-tool trace_flow` goes through the
  `TOOL_ADAPTERS` table; a tool that `tools/list` does not expose is a clear
  error, not a silent zero.

Behaviour that is easy to get wrong, stated once
- Test files: analyze_impact hides test-file rows unless include_tests is
  set. The scorer is symmetric: by default, graph sites located in a test file
  (spec/test/__tests__/tests dirs) are ignored when they match nothing in the
  golden (they are not false positives), and the tool is called WITHOUT
  include_tests. `--include-tests` counts such sites as normal findings and
  passes include_tests=true. A golden item that sits in a test file is always
  scored.
- Ambiguous edges count as emitted: a match on an `ambiguous` edge is a tp.
  Two precision columns are reported: "ambiguous counted" (all emitted edges)
  and "ambiguous excluded" (ambiguous edges dropped from both numerator and
  denominator). `heuristic` always counts as emitted. Recall is computed
  with ambiguous counted.
- Line accuracy: a matched producer / consumer whose line is within 2 of the
  golden line. Never a miss by itself. Reported as a separate stat.
- A false positive cannot be attributed to a feature tag by itself: it inherits the tags and
  language of the nearest NON-optional golden item in the same file within --line-tolerance,
  else of the non-optional golden items on the same topic, else "untagged" / "unknown".
- Hops: precision is not defined (the golden lists only the hops that lie on
  a chain, not every call edge), so a hop has tp / fn only.
- Topic keys are compared by strict equality after normalize_topic_key (no separator
  folding: `order:created` is not `order_created`).
- Chain order: analyze_impact's table is not in chain order, so every run reports BOTH
  "chains in order" (strict, the in-order rule of SCHEMA.md) and "chains presence" (every
  step location present, any order). `--chain-order strict|any` picks which one is the
  tp/fn of the chain category and therefore feeds the --fail-under-* thresholds.
- Line columns: matching never depends on the line. "tp with line <= N" (N =
  --line-tolerance, default 2) counts matched sites whose line is that close; the number
  of found sites dropped by the default test-file exclusion is reported per corpus.
- Cost: the matcher is O(golden x found) per category; golden files are expected to hold
  at most a few hundred items per category. same_file is a path-suffix test (a golden file
  `a.ts` also matches `x/a.ts`): golden paths should be root-relative and specific.
- Golden validation: a malformed golden file (empty chain steps, duplicate ids, wrong
  types, unterminated flow value or quote, root or config outside the repo) is an error,
  exit code 2.
- Optional items (`optional: true`, tag `declared_pattern`) are ignored both ways: a match
  is neither tp nor fp, a miss is not an fn; their count is reported as "optional ignored".
  `optional` is allowed only on a producer, consumer or hop that also carries the tag
  `declared_pattern` (and `declared_pattern` requires `optional`); a chain, a chain step or
  a chain that references an optional item is an error. Dropped items are listed by id.
- Chain table: chain recall for all chains, for the `min_hops >= 3` slice (min_hops is the
  explicit value, else the number of `call` steps) and per tag, in order and as presence.
- Chains: tp = traced, fn = not traced; precision is not defined. Precision columns
  are computed over producers and consumers only; recall over every category.
- Negatives: a found site within 2 lines of a negative (same file, same kind)
  is a false positive and is also listed under "negatives_hit".

YAML: no third-party library is used; a small parser for the subset the
golden files use (block maps / lists, flow maps / lists, scalars, comments)
is built in. A `.json` golden file is also accepted.
"""
import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LINE_TOL = 2
CHAIN_DEPTH = 5
CALL_TIMEOUT_S = float(os.environ.get("MESH_SCORE_TIMEOUT", "60"))
GRAPH_TIMEOUT_S = float(os.environ.get("MESH_SCORE_GRAPH_TIMEOUT", "300"))

EXT_LANG = {
    ".ts": "ts", ".tsx": "ts", ".js": "js", ".jsx": "js", ".mjs": "js", ".cjs": "js",
    ".java": "java", ".kt": "java", ".go": "go", ".py": "python", ".cs": "csharp",
    ".rs": "rust", ".sql": "sql",
}
TEST_PATH_RE = re.compile(
    r"(^|/)(__tests__|tests?|spec|specs|e2e)/|\.(spec|test)\.[A-Za-z]+$|_test\.(go|py|rs)$|(^|/)test_[^/]*\.py$|Tests?\.(java|cs)$"
)


class ScorerError(Exception):
    pass


# --------------------------------------------------------------------------
# Mini YAML (the subset of the golden files)
# --------------------------------------------------------------------------

def _strip_comment(s):
    out, quote = [], None
    for i, ch in enumerate(s):
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch == "#" and (i == 0 or s[i - 1] in " \t"):
            break
        out.append(ch)
    return "".join(out).rstrip()


def _scalar(tok):
    tok = tok.strip()
    if tok[:1] in ("'", '"') and not (len(tok) >= 2 and tok[-1] == tok[0]):
        raise ScorerError(f"unterminated quote: {tok[:40]!r}")
    if len(tok) >= 2 and tok[0] == tok[-1] and tok[0] in "'\"":
        return tok[1:-1]
    if tok in ("null", "~", ""):
        return None
    if tok == "true":
        return True
    if tok == "false":
        return False
    if re.fullmatch(r"-?\d+", tok):
        return int(tok)
    return tok


def _flow(s, i=0):
    """Parse one flow value starting at s[i]; returns (value, next index)."""
    while i < len(s) and s[i] == " ":
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
                raise ScorerError(f"bad flow mapping near: {s[i:i + 40]!r}")
            val, i = _flow(s, m.end())
            obj[m.group(2)] = val
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
        q = s[i]
        j = s.find(q, i + 1)
        if j < 0:
            raise ScorerError(f"unterminated quote: {s[i:i + 40]!r}")
        return s[i + 1:j], j + 1
    j = i
    while j < len(s) and s[j] not in ",}]":
        j += 1
    return _scalar(s[i:j]), j


def _value(s):
    s = s.strip()
    if s[:1] in ("{", "["):
        try:
            val, end = _flow(s)
        except IndexError:
            raise ScorerError(f"unterminated flow value: {s[:60]!r}") from None
        if s[end:].strip():
            raise ScorerError(f"trailing text after flow value: {s!r}")
        return val
    return _scalar(s)


_KEY_RE = re.compile(r"^([A-Za-z_][\w.\-]*):(?:\s+(.*))?$")


def parse_yaml(text):
    lines = []
    for raw in text.splitlines():
        s = _strip_comment(raw)
        if s.strip() and not s.strip() == "---":
            lines.append([len(s) - len(s.lstrip(" ")), s.strip()])

    def block(i, indent):
        if lines[i][1].startswith("- ") or lines[i][1] == "-":
            return seq(i, indent)
        return mapping(i, indent)

    def seq(i, indent):
        out = []
        while i < len(lines) and lines[i][0] == indent and (lines[i][1].startswith("- ") or lines[i][1] == "-"):
            rest = lines[i][1][1:].lstrip(" ")
            if not rest:
                if i + 1 < len(lines) and lines[i + 1][0] > indent:
                    val, i = block(i + 1, lines[i + 1][0])
                else:
                    val, i = None, i + 1
            elif _KEY_RE.match(rest):
                # "- key: v" opens a mapping whose first line sits after the dash.
                lines[i] = [indent + len(lines[i][1]) - len(rest), rest]
                val, i = mapping(i, lines[i][0])
            else:
                val, i = _value(rest), i + 1
            out.append(val)
        return out, i

    def mapping(i, indent):
        out = {}
        while i < len(lines) and lines[i][0] == indent and not lines[i][1].startswith("- "):
            m = _KEY_RE.match(lines[i][1])
            if not m:
                raise ScorerError(f"unsupported YAML line: {lines[i][1]!r}")
            key, rest = m.group(1), m.group(2)
            if rest is None or not rest.strip():
                nxt = lines[i + 1] if i + 1 < len(lines) else None
                if nxt and (nxt[0] > indent or (nxt[0] == indent and nxt[1].startswith("- "))):
                    out[key], i = block(i + 1, nxt[0])
                else:
                    out[key], i = None, i + 1
            else:
                out[key], i = _value(rest), i + 1
        return out, i

    if not lines:
        return {}
    try:
        val, end = block(0, lines[0][0])
    except IndexError:
        raise ScorerError("malformed YAML: unterminated flow value ('{' or '[' never closed)") from None
    if end != len(lines):
        raise ScorerError(f"unparsed YAML near: {lines[end][1]!r}")
    return val


def _need(cond, msg):
    if not cond:
        raise ScorerError(msg)


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def validate_golden(g, path="golden"):
    """Reject a malformed golden file with a clear message (never a free score)."""
    _need(isinstance(g, dict), f"{path}: top level must be a mapping, got {type(g).__name__}")
    for key in ("corpus", "root"):
        _need(isinstance(g.get(key), str) and g[key], f"{path}: '{key}' is required (non-empty string)")
    for key in ("producers", "consumers", "hops", "chains", "negatives"):
        g[key] = g.get(key) or []
        _need(isinstance(g[key], list) and all(isinstance(x, dict) for x in g[key]),
              f"{path}: '{key}' must be a list of mappings")
        ids = [x.get("id") for x in g[key]] if key != "negatives" else []
        _need(all(i is not None for i in ids), f"{path}: every item of '{key}' needs an 'id'")
        dup = sorted({str(i) for i in ids if ids.count(i) > 1})
        _need(not dup, f"{path}: duplicate ids in '{key}': {', '.join(dup)}")
    all_ids = [str(x["id"]) for key in ("producers", "consumers", "hops", "chains") for x in g[key]]
    cross = sorted({i for i in all_ids if all_ids.count(i) > 1})
    _need(not cross, f"{path}: ids must be unique across the file, duplicated: {', '.join(cross)}")
    for key in ("producers", "consumers", "hops", "chains"):
        for x in g[key]:
            _need(isinstance(x.get("tags") or [], list), f"{path}: {key} {x.get('id')}: 'tags' must be a list")
            _need(isinstance(x.get("optional", False), bool), f"{path}: {key} {x.get('id')}: 'optional' must be true or false")
            declared = "declared_pattern" in (x.get("tags") or [])
            if key in ("producers", "consumers", "hops"):
                _need(bool(x.get("optional")) == declared,
                      f"{path}: {key} {x.get('id')}: 'optional: true' and the tag 'declared_pattern' must go together")
            else:
                _need(not x.get("optional") and not declared,
                      f"{path}: {key} {x.get('id')}: 'optional' / 'declared_pattern' is not allowed on {key}")
            _need(not key == "chains" or x.get("min_hops") is None or _is_int(x["min_hops"]),
                  f"{path}: chain {x.get('id')}: 'min_hops' must be an integer")
    for cat in ("producers", "consumers"):
        for x in g[cat]:
            _need(isinstance(x.get("topic"), str) and x["topic"] and isinstance(x.get("file"), str) and _is_int(x.get("line")),
                  f"{path}: {cat} {x.get('id')}: needs string 'topic', string 'file' and integer 'line'")
    for x in g["hops"]:
        for side in ("from", "to"):
            e = x.get(side)
            _need(isinstance(e, dict) and isinstance(e.get("file"), str) and isinstance(e.get("symbol"), str),
                  f"{path}: hop {x.get('id')}: '{side}' needs string 'file' and 'symbol'")
    for x in g["chains"]:
        _need(isinstance(x.get("entry"), dict) and isinstance(x["entry"].get("value"), str) and x["entry"]["value"],
              f"{path}: chain {x.get('id')}: 'entry.value' is required")
        steps = x.get("steps")
        _need(isinstance(steps, list) and steps, f"{path}: chain {x.get('id')}: 'steps' must be a non-empty list")
        for st in steps:
            _need(isinstance(st, dict) and isinstance(st.get("file"), str) and _is_int(st.get("line")),
                  f"{path}: chain {x.get('id')}: every step needs string 'file' and integer 'line'")
            _need(not st.get("optional"), f"{path}: chain {x.get('id')}: 'optional' is not allowed on a chain step")
    optional_locs = [(i["file"], i["line"]) for k in ("producers", "consumers") for i in g[k] if i.get("optional")]
    for h in g["hops"]:
        if h.get("optional"):
            optional_locs += [(h[side]["file"], h[side]["line"]) for side in ("from", "to") if _is_int(h[side].get("line"))]
    for x in g["chains"]:
        for st in x["steps"]:
            _need(not any(same_file(st["file"], f) and abs(st["line"] - ln) <= LINE_TOL for f, ln in optional_locs),
                  f"{path}: chain {x.get('id')} step {st['file']}:{st['line']} references an optional item")
    for x in g["negatives"]:
        _need(x.get("kind") in ("producer", "consumer") and isinstance(x.get("file"), str) and _is_int(x.get("line")),
              f"{path}: negatives need kind producer|consumer, string 'file' and integer 'line'")
    return g


def load_golden(path):
    try:
        text = Path(path).read_text()
        g = json.loads(text) if str(path).endswith(".json") else parse_yaml(text)
    except (OSError, ValueError, RecursionError) as e:  # ValueError covers JSON and Unicode errors
        raise ScorerError(f"{path}: cannot read or parse: {e}") from None
    return validate_golden(g, str(path))


def resolve_corpus_paths(golden, repo_root=None):
    """(root dir, config path or None); both must lie inside the repository."""
    repo = Path(repo_root or REPO_ROOT).resolve()
    root = (repo / golden["root"]).resolve()
    _need(root.is_relative_to(repo), f"{golden['corpus']}: root {golden['root']!r} escapes the repository")
    cfg = None
    if golden.get("config"):
        _need(isinstance(golden["config"], str), f"{golden['corpus']}: 'config' must be a string")
        cfg = (root / golden["config"]).resolve()
        _need(cfg.is_relative_to(repo), f"{golden['corpus']}: config {golden['config']!r} escapes the repository")
    return root, cfg


# --------------------------------------------------------------------------
# Normalisation and sites
# --------------------------------------------------------------------------

_CONST_PATH_RE = re.compile(r"^[A-Z][A-Za-z0-9_]*(\.[A-Za-z0-9_]+)*\.[A-Z][A-Z0-9_]*$")


def normalize_topic_key(topic):
    """Lower-case topic key as an agent would name it. A constant member path
    (`Streams.EVENT_CREATED`) keys on the member alone, a broker literal
    (`orders.created`) is kept whole, as in docs/mcp-tools.md "Topic matching"."""
    t = str(topic).strip().strip("\"'`")
    t = re.sub(r"^typeof\s+", "", t)
    if _CONST_PATH_RE.match(t):
        t = t.rsplit(".", 1)[1]
    return t.lower()


def same_topic(a, b):
    """Strict equality after normalize_topic_key (lower-case + constant-member
    reduction). No separator folding: `order:created` and `order_created` are
    different topics."""
    return normalize_topic_key(a) == normalize_topic_key(b)


def norm_path(p, root):
    p = str(p).replace("\\", "/")
    for r in {str(root), os.path.realpath(str(root))}:
        r = r.replace("\\", "/").rstrip("/")
        if p.startswith(r + "/"):
            p = p[len(r) + 1:]
            break
    while p.startswith("./"):
        p = p[2:]
    return p


def same_file(found, golden):
    found, golden = found.lstrip("/"), golden.lstrip("/")
    return found == golden or found.endswith("/" + golden) or golden.endswith("/" + found)


def lang_of(path):
    return EXT_LANG.get(os.path.splitext(path)[1].lower())


def is_test_path(path):
    return bool(TEST_PATH_RE.search(path))


def extract_sites(graph, root):
    """Producer / consumer sites and Calls edges of a `graph --format json` dump.

    Shape (verified on 8.0.1): nodes {id, name, kind, file_path (absolute),
    line_start, ...}; edges {from, to, kind, confidence}. A Produces edge goes
    from the code node (file_path, line_start) to a topic node; a Consumes edge
    goes from the topic node to the code node. The topic end is the hub node whose
    file_path is "event-bus" (`name` = topic key, e.g. `cache:purged`); its kind
    (EventStream / KafkaTopic / Queue / Saga) is NOT reliable, since a code node can
    carry a topic kind too.
    """
    nodes = {n["id"]: n for n in graph.get("nodes", [])}
    sites = {"producer": [], "consumer": [], "call": []}
    for e in graph.get("edges", []):
        kind = str(e.get("kind", ""))
        a, b = nodes.get(e.get("from")), nodes.get(e.get("to"))
        if a is None or b is None:
            continue
        conf = str(e.get("confidence", "exact")).lower()
        if kind in ("Produces", "Consumes"):
            # The topic end is the hub node (file_path == "event-bus"), NOT a node of a topic
            # kind: on 8.0.1 a Consumes edge's code node can itself be of kind KafkaTopic /
            # EventStream. Without a hub end, fall back on the edge direction
            # (Produces: code -> topic; Consumes: topic -> code).
            if _is_hub(a) != _is_hub(b):
                topic, code = (a, b) if _is_hub(a) else (b, a)
            else:
                code, topic = (a, b) if kind == "Produces" else (b, a)
            sites["producer" if kind == "Produces" else "consumer"].append({
                "file": norm_path(code.get("file_path", ""), root),
                "line": int(code.get("line_start") or 0),
                "topic": str(topic.get("name", "")),
                "confidence": conf,
                "symbol": str(code.get("name", "")),
            })
        elif kind.lower() == "calls":
            sites["call"].append({
                "from_file": norm_path(a.get("file_path", ""), root), "from_name": str(a.get("name", "")),
                "to_file": norm_path(b.get("file_path", ""), root), "to_name": str(b.get("name", "")),
                "confidence": conf,
            })
    return sites


_TOPIC_KINDS = {"kafkatopic", "eventstream", "queue", "saga"}


def _is_topic(node):
    return str(node.get("kind", "")).replace("_", "").lower() in _TOPIC_KINDS


HUB_FILE = "event-bus"


def _is_hub(node):
    """The topic hub node of the graph: file_path is the literal "event-bus"."""
    return str(node.get("file_path", "")) == HUB_FILE


# --------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------

def match_sites(golden_items, found_items):
    """One-to-one match on (file, topic), nearest line first. Returns
    (pairs [(g, f)], unmatched_golden, unmatched_found)."""
    cand = []
    for gi, g in enumerate(golden_items):
        for fi, f in enumerate(found_items):
            if same_file(f["file"], g["file"]) and same_topic(f["topic"], g["topic"]):
                cand.append((abs(f["line"] - int(g["line"])), gi, fi))
    cand.sort()
    used_g, used_f, pairs = set(), set(), []
    for _d, gi, fi in cand:
        if gi in used_g or fi in used_f:
            continue
        used_g.add(gi)
        used_f.add(fi)
        pairs.append((golden_items[gi], found_items[fi]))
    return (
        pairs,
        [g for i, g in enumerate(golden_items) if i not in used_g],
        [f for i, f in enumerate(found_items) if i not in used_f],
    )


def _sym_match(want, node_name):
    want, got = want.lower(), node_name.lower()
    return want == got or want.rsplit(".", 1)[-1] == got.rsplit(".", 1)[-1]


def match_hops(golden_hops, calls):
    """Hop matched on (from.file, to.file) plus the symbol names; lines are informational."""
    pairs, missed = [], []
    for h in golden_hops:
        hit = None
        for c in calls:
            if (same_file(c["from_file"], h["from"]["file"]) and same_file(c["to_file"], h["to"]["file"])
                    and _sym_match(h["from"]["symbol"], c["from_name"]) and _sym_match(h["to"]["symbol"], c["to_name"])):
                hit = c
                break
        (pairs if hit else missed).append((h, hit) if hit else h)
    return pairs, missed


PATH_LINE_RE = re.compile(r"([A-Za-z0-9_@.\-/\\]+\.[A-Za-z0-9]+):(\d+)(?:[-:]\d+)?")


def extract_occurrences(answer):
    """Ordered (path, line) occurrences of a Markdown answer."""
    return [(m.group(1).replace("\\", "/"), int(m.group(2))) for m in PATH_LINE_RE.finditer(answer)]


def chain_traced(steps, occurrences):
    """True when every step (file, line within LINE_TOL) appears IN ORDER (greedy
    earliest match; optimal for a subsequence test). An empty step list is never traced."""
    if not steps:
        return False
    pos = 0
    for st in steps:
        for j in range(pos, len(occurrences)):
            path, line = occurrences[j]
            if same_file(path, st["file"]) and abs(line - int(st["line"])) <= LINE_TOL:
                pos = j + 1
                break
        else:
            return False
    return True


def chain_min_hops(ch):
    """min_hops of a chain: the explicit value, else its number of `call` steps."""
    if ch.get("min_hops") is not None:
        return ch["min_hops"]
    return sum(st.get("kind") == "call" for st in ch["steps"])


def chain_present(steps, occurrences):
    """True when every step location appears somewhere in the answer, in any order."""
    return all(any(same_file(p, st["file"]) and abs(ln - int(st["line"])) <= LINE_TOL for p, ln in occurrences)
               for st in steps)


# --------------------------------------------------------------------------
# Scoring
# --------------------------------------------------------------------------

def score_corpus(golden, sites, answers, include_tests=False, chain_order="strict", line_tol=LINE_TOL):
    """Per-item outcome records for one corpus.

    answers: {chain id: Markdown answer} or None (chains skipped).
    chain_order: "strict" (steps in order) or "any" (every step present): decides the
    chain tp/fn outcome; both verdicts are kept on the record (`strict`, `presence`).
    record = {corpus, category, language, tags, outcome: tp|fp|fn, ambiguous, dline}
    Returns (records, negatives_hit, meta); meta["ignored_test_sites"] counts the
    found producer/consumer sites dropped by the default test-file exclusion.
    """
    corpus = golden["corpus"]
    recs, negatives_hit, ignored = [], [], 0
    optional_ids = [f"{k[:-1]} {x['id']}" for k in ("producers", "consumers", "hops") for x in golden[k] if x.get("optional")]
    real = [(g, k) for k in ("producers", "consumers") for g in golden[k] if not g.get("optional")]

    def inherit(f):
        """Tags and language of a false positive: nearest non-optional golden item in the
        same file within line_tol, else the non-optional items on the same topic."""
        near = [(abs(f["line"] - g["line"]), i) for i, (g, _k) in enumerate(real)
                if same_file(f["file"], g["file"]) and abs(f["line"] - g["line"]) <= line_tol]
        if near:
            g = real[min(near)[1]][0]
            return set(g.get("tags") or []), g.get("language")
        same = [g for g, _k in real if same_topic(g["topic"], f["topic"])]
        if same:
            return {t for g in same for t in (g.get("tags") or [])}, same[0].get("language")
        return set(), None

    def rec(category, outcome, language, tags, ambiguous=False, dline=None, **extra):
        recs.append(dict({"corpus": corpus, "category": category, "language": language or "unknown",
                          "tags": sorted(tags) or ["untagged"], "outcome": outcome,
                          "ambiguous": ambiguous, "dline": dline}, **extra))

    for cat, kind in (("producers", "producer"), ("consumers", "consumer")):
        found = sites[kind]
        gold_files = {g["file"] for g in golden[cat]}
        if not include_tests:
            kept = [f for f in found if not is_test_path(f["file"]) or any(same_file(f["file"], gf) for gf in gold_files)]
            ignored += len(found) - len(kept)
            found = kept
        pairs, missed, extra = match_sites(golden[cat], found)
        # Optional golden items take part in the matching (they consume the site they
        # match) but are ignored both ways: never tp / fp / fn.
        pairs = [(g, f) for g, f in pairs if not g.get("optional")]
        missed = [g for g in missed if not g.get("optional")]
        for g, f in pairs:
            rec(kind, "tp", g.get("language"), g.get("tags") or [], f["confidence"] == "ambiguous", abs(f["line"] - int(g["line"])))
        for g in missed:
            rec(kind, "fn", g.get("language"), g.get("tags") or [])
        for f in extra:
            tags, lang = inherit(f)
            rec(kind, "fp", lang, tags, f["confidence"] == "ambiguous")
            for n in golden["negatives"]:
                if n.get("kind") == kind and same_file(f["file"], n["file"]) and abs(f["line"] - int(n["line"])) <= LINE_TOL:
                    negatives_hit.append({"kind": kind, "file": n["file"], "line": n["line"], "note": n.get("note")})

    pairs, missed = match_hops([h for h in golden["hops"] if not h.get("optional")], sites["call"])
    for h, c in pairs:
        rec("hop", "tp", lang_of(h["from"]["file"]), h.get("tags") or [], c["confidence"] == "ambiguous")
    for h in missed:
        rec("hop", "fn", lang_of(h["from"]["file"]), h.get("tags") or [])

    if answers is not None:
        for ch in golden["chains"]:
            occ = extract_occurrences(answers.get(ch["id"], ""))
            strict, presence = chain_traced(ch["steps"], occ), chain_present(ch["steps"], occ)
            ok = presence if chain_order == "any" else strict
            rec("chain", "tp" if ok else "fn", lang_of(ch["steps"][0]["file"]), ch.get("tags") or [],
                strict=strict, presence=presence, min_hops=chain_min_hops(ch),
                direction=ch.get("direction") or "forward", n_steps=len(ch["steps"]))
    return recs, negatives_hit, {"ignored_test_sites": ignored, "optional_ignored": len(optional_ids),
                                 "optional_ids": optional_ids}


def _ratio(num, den):
    return None if den == 0 else num / den


def summarize(recs, line_tol=LINE_TOL):
    tp = sum(r["outcome"] == "tp" for r in recs)
    fp = sum(r["outcome"] == "fp" for r in recs)
    fn = sum(r["outcome"] == "fn" for r in recs)
    # Precision is defined for producers / consumers only (hops and chains have no fp).
    pr = [r for r in recs if r["category"] in ("producer", "consumer")]
    ptp = sum(r["outcome"] == "tp" for r in pr)
    pfp = sum(r["outcome"] == "fp" for r in pr)
    ptp_x = sum(r["outcome"] == "tp" and not r["ambiguous"] for r in pr)
    pfp_x = sum(r["outcome"] == "fp" and not r["ambiguous"] for r in pr)
    dl = [r["dline"] for r in recs if r["dline"] is not None]
    ch = [r for r in recs if r["category"] == "chain" and "strict" in r]
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision_ambiguous_counted": _ratio(ptp, ptp + pfp),
        "precision_ambiguous_excluded": _ratio(ptp_x, ptp_x + pfp_x),
        "recall": _ratio(tp, tp + fn),
        "line_accuracy": _ratio(sum(d <= line_tol for d in dl), len(dl)),
        "line_samples": len(dl),
        "tp_line_ok": sum(d <= line_tol for d in dl),
        "chains_total": len(ch),
        "chains_in_order": sum(r["strict"] for r in ch),
        "chains_presence": sum(r["presence"] for r in ch),
    }


def _chain_row(rs):
    n = len(rs)
    a, b = sum(r["strict"] for r in rs), sum(r["presence"] for r in rs)
    return {"chains": n, "in_order": a, "presence": b, "recall_in_order": _ratio(a, n), "recall_presence": _ratio(b, n)}


def chain_summary(recs):
    """Chain recall, all chains next to the `min_hops >= 3` slice and per tag; in order
    (strict) and presence (any order) side by side."""
    ch = [r for r in recs if r["category"] == "chain" and "strict" in r]
    per_tag = defaultdict(list)
    for r in ch:
        for t in r["tags"]:
            per_tag[t].append(r)
    per_dir = defaultdict(list)
    for r in ch:
        per_dir[r.get("direction", "forward")].append(r)
    return {"all": _chain_row(ch), "min_hops_ge_3": _chain_row([r for r in ch if (r.get("min_hops") or 0) >= 3]),
            "per_direction": {d: _chain_row(v) for d, v in sorted(per_dir.items())},
            "longer_than_cap": _chain_row([r for r in ch if r.get("n_steps", 0) - 1 > CHAIN_DEPTH]),
            "depth_cap": CHAIN_DEPTH,
            "per_tag": {t: _chain_row(v) for t, v in sorted(per_tag.items())}}


def slices(recs, line_tol=LINE_TOL):
    out = {"total": summarize(recs, line_tol), "category": {}, "language": {}, "tag": {}, "chains": chain_summary(recs)}
    for key, getter in (("category", lambda r: [r["category"]]), ("language", lambda r: [r["language"]]), ("tag", lambda r: r["tags"])):
        groups = defaultdict(list)
        for r in recs:
            for k in getter(r):
                groups[k].append(r)
        out[key] = {k: summarize(v, line_tol) for k, v in sorted(groups.items())}
    return out


# --------------------------------------------------------------------------
# Running mesh-mcp
# --------------------------------------------------------------------------

def isolated_env(home):
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["MESH_AUDIT_DB"] = str(Path(home) / "audit.db")
    env["MESH_SOCKET_PATH"] = str(Path(home) / "m.sock")
    for k in ("MESH_CONFIG", "XDG_CACHE_HOME", "XDG_CONFIG_HOME"):
        env.pop(k, None)
    return env


def corpus_config(golden, root, home, config=None):
    if config:
        return config
    cfg = Path(home) / "mesh-mcp.toml"
    cfg.write_text(
        f'[workspace]\nname = "{golden["corpus"]}"\nversion = "1.0.0"\nroots = ["{root}"]\n'
        'exclude_patterns = ["**/node_modules/**"]\n\n[engines.contracts]\nenabled = true\n'
    )
    return cfg


def run_graph(binary, cfg, root, env, home):
    out = Path(home) / "graph.json"
    subprocess.run([str(binary), "--config", str(cfg), "graph", "--format", "json", "-o", str(out)],
                   cwd=root, env=env, check=True, capture_output=True, text=True, timeout=GRAPH_TIMEOUT_S)
    return json.loads(out.read_text())


class McpSession:
    """Minimal `run --standalone` stdio client (ndjson, as scripts/bench/mcp_client.py).

    stdout is drained by a reader thread into a queue, so several lines arriving
    in one write (a notification plus a response) are all seen. `argv` replaces
    the mesh-mcp command line (used by the self-tests with a fake server)."""

    def __init__(self, binary, cfg, root, env, argv=None):
        self.proc = subprocess.Popen(argv or [str(binary), "--config", str(cfg), "run", "--standalone"], cwd=root, env=env,
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     text=True, bufsize=1)
        self._lines = queue.Queue()
        self._reader = threading.Thread(target=self._drain, daemon=True)
        self._reader.start()
        self._id = 0
        self.rpc("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                                "clientInfo": {"name": "score-events", "version": "0"}})
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        self.proc.stdin.flush()
        self.tools = {t["name"]: t for t in self.rpc("tools/list", {}).get("tools", [])}

    def _drain(self):
        for line in self.proc.stdout:
            self._lines.put(line)
        self._lines.put(None)

    def rpc(self, method, params):
        self._id += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        deadline = time.monotonic() + CALL_TIMEOUT_S
        while True:
            try:
                line = self._lines.get(timeout=max(deadline - time.monotonic(), 0.001))
            except queue.Empty:
                raise ScorerError(f"{method}: no response within {CALL_TIMEOUT_S:.0f}s") from None
            if line is None:
                raise ScorerError(f"{method}: server closed stdout")
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                raise ScorerError(f"{method}: non-JSON line on stdout: {line[:80]!r}") from None
            if isinstance(msg, dict) and msg.get("id") == self._id:
                if "error" in msg:
                    raise ScorerError(f"{method}: {msg['error']}")
                return msg.get("result", {})

    def call(self, name, args):
        res = self.rpc("tools/call", {"name": name, "arguments": args})
        text = "\n".join(c.get("text", "") for c in res.get("content", []))
        if res.get("isError"):
            raise ScorerError(f"{name} returned isError: {text[:200]}")
        return text

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()
        self._reader.join(timeout=2)
        self.proc.stdout.close()


def _impact_args(chain, include_tests):
    return {"target": chain["entry"]["value"], "depth": CHAIN_DEPTH, "limit": 200, "include_tests": include_tests}


def _trace_flow_args(chain, include_tests):
    # Extension point: the trace_flow argument shape arrives with the 9.x tool.
    # Arguments the tool's inputSchema does not declare are dropped by
    # _filter_args, so this stays valid when the schema grows or shrinks.
    return {"target": chain["entry"]["value"], "depth": CHAIN_DEPTH, "direction": chain.get("direction", "forward"),
            "include_tests": include_tests}


TOOL_ADAPTERS = {"analyze_impact": _impact_args, "trace_flow": _trace_flow_args}


def _filter_args(args, tool):
    props = (tool.get("inputSchema") or {}).get("properties")
    return {k: v for k, v in args.items() if props is None or k in props}


def trace_chain(session, tool_name, chain, include_tests):
    if tool_name not in session.tools:
        raise ScorerError(f"chain tool {tool_name!r} is not exposed by tools/list (have: {', '.join(sorted(session.tools))})")
    args = _filter_args(TOOL_ADAPTERS[tool_name](chain, include_tests), session.tools[tool_name])
    paged = "offset" in (session.tools[tool_name].get("inputSchema") or {}).get("properties", {})
    pages, offset = [], 0
    for _ in range(10):
        pages.append(session.call(tool_name, dict(args, offset=offset) if offset and paged else args))
        # Footer: "More rows: repeat the same call with `offset: N` for the next page."
        m = re.search(r"More rows:.*?offset:\s*(\d+)", pages[-1])
        if not m or int(m.group(1)) <= offset:
            break
        offset = int(m.group(1))
    return "\n".join(pages)


def score_file(golden_path, binary, chain_tool, include_tests, run_chains, chain_order="strict", line_tol=LINE_TOL):
    golden = load_golden(golden_path)
    root, config = resolve_corpus_paths(golden)
    if not root.is_dir():
        raise ScorerError(f"{golden_path}: root {root} does not exist")
    if config is not None and not config.is_file():
        raise ScorerError(f"{golden_path}: config {config} does not exist")
    home = Path(tempfile.mkdtemp(prefix="se-"))
    try:
        env = isolated_env(home)
        cfg = corpus_config(golden, root, home, config)
        sites = extract_sites(run_graph(binary, cfg, root, env, home), root)
        answers = None
        if run_chains and golden["chains"]:
            session = McpSession(binary, cfg, root, env)
            try:
                answers = {ch["id"]: trace_chain(session, chain_tool, ch, include_tests) for ch in golden["chains"]}
            finally:
                session.close()
        return score_corpus(golden, sites, answers, include_tests, chain_order, line_tol)
    finally:
        shutil.rmtree(home, ignore_errors=True)


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def _pct(x):
    return "n/a" if x is None else f"{x:.1%}"


def _table(title, rows, tol=LINE_TOL):
    out = [f"### {title}", "",
           f"| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= {tol} | chains in order | chains presence (any order) |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for name, s in rows:
        chains = (f"{s['chains_in_order']}/{s['chains_total']}", f"{s['chains_presence']}/{s['chains_total']}") if s["chains_total"] else ("n/a", "n/a")
        out.append(f"| {name} | {s['tp']} | {s['fp']} | {s['fn']} | {_pct(s['precision_ambiguous_counted'])} | "
                   f"{_pct(s['precision_ambiguous_excluded'])} | {_pct(s['recall'])} | {s['tp_line_ok']} | {chains[0]} | {chains[1]} |")
    return "\n".join(out) + "\n"


def _chain_table(cs):
    cap = cs["depth_cap"]
    rows = [("all chains", cs["all"]), ("chains with min_hops >= 3", cs["min_hops_ge_3"])]
    rows += [(f"direction: {d}", v) for d, v in cs["per_direction"].items()]
    rows.append((f"longer than the cap (> {cap} hops after the entry step)", cs["longer_than_cap"]))
    rows += [(f"tag: {t}", v) for t, v in cs["per_tag"].items()]
    out = [f"### chains (tool depth cap: {cap}; reverse chains and chains longer than the cap miss by construction, "
           "and still count as misses)", "", "| slice | chains | traced in order | recall in order | presence (any order) | recall presence |",
           "|---|---|---|---|---|---|"]
    for name, r in rows:
        out.append(f"| {name} | {r['chains']} | {r['in_order']} | {_pct(r['recall_in_order'])} | {r['presence']} | {_pct(r['recall_presence'])} |")
    return "\n".join(out) + "\n"


def markdown(report):
    tol = report["line_tolerance"]
    out = []
    for name, rep in report["corpora"].items():
        out.append(f"## {name}\n")
        out.append(_table("total", [("total", rep["total"])], tol))
        for key in ("category", "language", "tag"):
            out.append(_table(f"per {key}", list(rep[key].items()), tol))
        out.append(_chain_table(rep["chains"]))
        la = rep["total"]
        out.append(f"Line accuracy (within {tol}): {_pct(la['line_accuracy'])} of {la['line_samples']} matched sites. "
                   f"Sites ignored as test files: {rep['ignored_test_sites']}. Optional ignored: {rep['optional_ignored']}"
                   + (f" ({', '.join(rep['optional_ids'])})" if rep["optional_ids"] else "") + ".\n")
        if rep["negatives_hit"]:
            out.append("Negatives hit: " + ", ".join(f"{n['file']}:{n['line']}" for n in rep["negatives_hit"]) + "\n")
    t = report["all"]
    out.append("## all corpora\n")
    out.append(_table("total", [("total", t["total"])], tol))
    for key in ("category", "language", "tag"):
        out.append(_table(f"per {key}", list(t[key].items()), tol))
    out.append(_chain_table(t["chains"]))
    out.append(f"Line accuracy (within {tol}): {_pct(t['total']['line_accuracy'])} of {t['total']['line_samples']} matched sites. "
               f"Sites ignored as test files: {report['ignored_test_sites']}. Optional ignored: {report['optional_ignored']}.\n")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("golden", nargs="+", help="golden YAML (or JSON) files")
    ap.add_argument("--binary", required=True, help="mesh-mcp binary to test")
    ap.add_argument("--chain-tool", choices=sorted(TOOL_ADAPTERS), default="analyze_impact")
    ap.add_argument("--include-tests", action="store_true", help="count test-file sites and pass include_tests=true")
    ap.add_argument("--no-chains", action="store_true", help="skip the MCP session (graph-only categories)")
    ap.add_argument("--line-tolerance", type=int, default=LINE_TOL,
                    help="tolerance of the 'tp with line' column and line accuracy only (default 2); matching ignores it")
    ap.add_argument("--chain-order", choices=("strict", "any"), default="strict",
                    help="strict: steps must appear in order; any: presence only. Decides chain tp/fn and the thresholds; "
                         "both numbers are always reported")
    ap.add_argument("--json", action="store_true", help="JSON report instead of Markdown")
    ap.add_argument("--fail-under-precision", type=float, default=None, help="on the total (ambiguous counted)")
    ap.add_argument("--fail-under-recall", type=float, default=None, help="on the total")
    args = ap.parse_args()

    all_recs, corpora, ignored, optional = [], {}, 0, 0
    try:
        for path in args.golden:
            recs, neg, meta = score_file(path, args.binary, args.chain_tool, args.include_tests, not args.no_chains,
                                         args.chain_order, args.line_tolerance)
            name = recs[0]["corpus"] if recs else Path(path).stem
            corpora[name] = dict(slices(recs, args.line_tolerance), negatives_hit=neg, **meta)
            all_recs += recs
            ignored += meta["ignored_test_sites"]
            optional += meta["optional_ignored"]
    except (ScorerError, subprocess.SubprocessError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except (KeyError, TypeError, AttributeError, IndexError, ValueError) as e:  # malformed input that slipped validation
        print(f"error: malformed input ({type(e).__name__}: {e})", file=sys.stderr)
        return 2
    report = {"binary": str(args.binary), "chain_tool": args.chain_tool, "chain_order": args.chain_order,
              "line_tolerance": args.line_tolerance, "ignored_test_sites": ignored, "optional_ignored": optional, "corpora": corpora,
              "all": slices(all_recs, args.line_tolerance)}
    print(json.dumps(report, indent=2) if args.json else markdown(report))

    total = report["all"]["total"]
    ok = True
    if args.fail_under_precision is not None and (total["precision_ambiguous_counted"] or 0.0) < args.fail_under_precision:
        print(f"FAIL: precision {_pct(total['precision_ambiguous_counted'])} < {args.fail_under_precision:.1%}", file=sys.stderr)
        ok = False
    if args.fail_under_recall is not None and (total["recall"] or 0.0) < args.fail_under_recall:
        print(f"FAIL: recall {_pct(total['recall'])} < {args.fail_under_recall:.1%}", file=sys.stderr)
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
