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
- A false positive cannot be attributed to a feature tag by itself: it
  inherits the tags (and language) of the golden items in the same file or on
  the same topic, else "untagged".
- Hops: precision is not defined (the golden lists only the hops that lie on
  a chain, not every call edge), so a hop has tp / fn only.
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
import re
import shutil
import subprocess
import sys
import tempfile
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
        j = s.index(q, i + 1)
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
    val, end = block(0, lines[0][0])
    if end != len(lines):
        raise ScorerError(f"unparsed YAML near: {lines[end][1]!r}")
    return val


def load_golden(path):
    text = Path(path).read_text()
    g = json.loads(text) if str(path).endswith(".json") else parse_yaml(text)
    for key in ("producers", "consumers", "hops", "chains", "negatives"):
        g[key] = g.get(key) or []
    if not g.get("corpus") or not g.get("root"):
        raise ScorerError(f"{path}: 'corpus' and 'root' are required")
    return g


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


def _loose(key):
    return re.sub(r"[.:\-/]", "_", key)


def same_topic(a, b):
    a, b = normalize_topic_key(a), normalize_topic_key(b)
    return a == b or _loose(a) == _loose(b)


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
    goes from the topic node to the code node. Topic nodes are `EventStream` /
    `KafkaTopic` / `Queue` / `Saga` with `name` = topic key (e.g. `cache:purged`)
    and file_path "event-bus" for the cross-repo hub.
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
            # Produces: code -> topic; Consumes: topic -> code (verified on 8.0.1).
            code, topic = (b, a) if _is_topic(a) and not _is_topic(b) else (a, b)
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
    earliest match; optimal for a subsequence test)."""
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


# --------------------------------------------------------------------------
# Scoring
# --------------------------------------------------------------------------

def score_corpus(golden, sites, answers, include_tests=False):
    """Per-item outcome records for one corpus.

    answers: {chain id: Markdown answer} or None (chains skipped).
    record = {corpus, category, language, tags, outcome: tp|fp|fn, ambiguous, dline}
    """
    corpus = golden["corpus"]
    recs, negatives_hit = [], []
    file_tags, topic_tags, file_lang = defaultdict(set), defaultdict(set), {}
    for cat in ("producers", "consumers"):
        for g in golden[cat]:
            file_tags[g["file"]].update(g.get("tags") or [])
            topic_tags[normalize_topic_key(g["topic"])].update(g.get("tags") or [])
            file_lang[g["file"]] = g.get("language")

    def rec(category, outcome, language, tags, ambiguous=False, dline=None):
        recs.append({"corpus": corpus, "category": category, "language": language or "unknown",
                     "tags": sorted(tags) or ["untagged"], "outcome": outcome,
                     "ambiguous": ambiguous, "dline": dline})

    for cat, kind in (("producers", "producer"), ("consumers", "consumer")):
        found = sites[kind]
        gold_files = {g["file"] for g in golden[cat]}
        if not include_tests:
            found = [f for f in found if not is_test_path(f["file"]) or any(same_file(f["file"], gf) for gf in gold_files)]
        pairs, missed, extra = match_sites(golden[cat], found)
        for g, f in pairs:
            rec(kind, "tp", g.get("language"), g.get("tags") or [], f["confidence"] == "ambiguous", abs(f["line"] - int(g["line"])))
        for g in missed:
            rec(kind, "fn", g.get("language"), g.get("tags") or [])
        for f in extra:
            tags = set(topic_tags.get(normalize_topic_key(f["topic"]), set()))
            for gf, ts in file_tags.items():
                if same_file(f["file"], gf):
                    tags |= ts
            lang = next((l for gf, l in file_lang.items() if same_file(f["file"], gf)), None) or lang_of(f["file"])
            rec(kind, "fp", lang, tags, f["confidence"] == "ambiguous")
            for n in golden["negatives"]:
                if n.get("kind") == kind and same_file(f["file"], n["file"]) and abs(f["line"] - int(n["line"])) <= LINE_TOL:
                    negatives_hit.append({"kind": kind, "file": n["file"], "line": n["line"], "note": n.get("note")})

    pairs, missed = match_hops(golden["hops"], sites["call"])
    for h, c in pairs:
        rec("hop", "tp", lang_of(h["from"]["file"]), h.get("tags") or [], c["confidence"] == "ambiguous")
    for h in missed:
        rec("hop", "fn", lang_of(h["from"]["file"]), h.get("tags") or [])

    if answers is not None:
        for ch in golden["chains"]:
            ok = chain_traced(ch["steps"], extract_occurrences(answers.get(ch["id"], "")))
            rec("chain", "tp" if ok else "fn", lang_of(ch["steps"][0]["file"]) if ch["steps"] else None, ch.get("tags") or [])
    return recs, negatives_hit


def _ratio(num, den):
    return None if den == 0 else num / den


def summarize(recs):
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
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision_ambiguous_counted": _ratio(ptp, ptp + pfp),
        "precision_ambiguous_excluded": _ratio(ptp_x, ptp_x + pfp_x),
        "recall": _ratio(tp, tp + fn),
        "line_accuracy": _ratio(sum(d <= LINE_TOL for d in dl), len(dl)),
        "line_samples": len(dl),
    }


def slices(recs):
    out = {"total": summarize(recs), "category": {}, "language": {}, "tag": {}}
    for key, getter in (("category", lambda r: [r["category"]]), ("language", lambda r: [r["language"]]), ("tag", lambda r: r["tags"])):
        groups = defaultdict(list)
        for r in recs:
            for k in getter(r):
                groups[k].append(r)
        out[key] = {k: summarize(v) for k, v in sorted(groups.items())}
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


def corpus_config(golden, root, home):
    if golden.get("config"):
        return Path(root) / golden["config"]
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
    """Minimal `run --standalone` stdio client (ndjson, as scripts/bench/mcp_client.py)."""

    def __init__(self, binary, cfg, root, env):
        self.proc = subprocess.Popen([str(binary), "--config", str(cfg), "run", "--standalone"], cwd=root, env=env,
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     text=True, bufsize=1)
        self._id = 0
        self.rpc("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                                "clientInfo": {"name": "score-events", "version": "0"}})
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        self.proc.stdin.flush()
        self.tools = {t["name"]: t for t in self.rpc("tools/list", {}).get("tools", [])}

    def rpc(self, method, params):
        self._id += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        deadline = time.monotonic() + CALL_TIMEOUT_S
        import select
        while True:
            left = deadline - time.monotonic()
            if left <= 0 or not select.select([self.proc.stdout], [], [], left)[0]:
                raise ScorerError(f"{method}: no response within {CALL_TIMEOUT_S:.0f}s")
            line = self.proc.stdout.readline()
            if line == "":
                raise ScorerError(f"{method}: server closed stdout")
            if line.strip():
                msg = json.loads(line)
                if msg.get("id") == self._id:
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


def score_file(golden_path, binary, chain_tool, include_tests, run_chains):
    golden = load_golden(golden_path)
    root = (REPO_ROOT / golden["root"]).resolve()
    if not root.is_dir():
        raise ScorerError(f"{golden_path}: root {root} does not exist")
    home = Path(tempfile.mkdtemp(prefix="se-"))
    try:
        env = isolated_env(home)
        cfg = corpus_config(golden, root, home)
        sites = extract_sites(run_graph(binary, cfg, root, env, home), root)
        answers = None
        if run_chains and golden["chains"]:
            session = McpSession(binary, cfg, root, env)
            try:
                answers = {ch["id"]: trace_chain(session, chain_tool, ch, include_tests) for ch in golden["chains"]}
            finally:
                session.close()
        return score_corpus(golden, sites, answers, include_tests)
    finally:
        shutil.rmtree(home, ignore_errors=True)


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def _pct(x):
    return "n/a" if x is None else f"{x:.1%}"


def _table(title, rows):
    out = [f"### {title}", "", "| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall |",
           "|---|---|---|---|---|---|---|"]
    for name, s in rows:
        out.append(f"| {name} | {s['tp']} | {s['fp']} | {s['fn']} | {_pct(s['precision_ambiguous_counted'])} | "
                   f"{_pct(s['precision_ambiguous_excluded'])} | {_pct(s['recall'])} |")
    return "\n".join(out) + "\n"


def markdown(report):
    out = []
    for name, rep in report["corpora"].items():
        out.append(f"## {name}\n")
        out.append(_table("total", [("total", rep["total"])]))
        for key in ("category", "language", "tag"):
            out.append(_table(f"per {key}", list(rep[key].items())))
        la = rep["total"]
        out.append(f"Line accuracy (within {LINE_TOL}): {_pct(la['line_accuracy'])} of {la['line_samples']} matched sites.\n")
        if rep["negatives_hit"]:
            out.append("Negatives hit: " + ", ".join(f"{n['file']}:{n['line']}" for n in rep["negatives_hit"]) + "\n")
    t = report["all"]
    out.append("## all corpora\n")
    out.append(_table("total", [("total", t["total"])]))
    for key in ("category", "language", "tag"):
        out.append(_table(f"per {key}", list(t[key].items())))
    out.append(f"Line accuracy (within {LINE_TOL}): {_pct(t['total']['line_accuracy'])} of {t['total']['line_samples']} matched sites.\n")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("golden", nargs="+", help="golden YAML (or JSON) files")
    ap.add_argument("--binary", required=True, help="mesh-mcp binary to test")
    ap.add_argument("--chain-tool", choices=sorted(TOOL_ADAPTERS), default="analyze_impact")
    ap.add_argument("--include-tests", action="store_true", help="count test-file sites and pass include_tests=true")
    ap.add_argument("--no-chains", action="store_true", help="skip the MCP session (graph-only categories)")
    ap.add_argument("--json", action="store_true", help="JSON report instead of Markdown")
    ap.add_argument("--fail-under-precision", type=float, default=None, help="on the total (ambiguous counted)")
    ap.add_argument("--fail-under-recall", type=float, default=None, help="on the total")
    args = ap.parse_args()

    all_recs, corpora = [], {}
    try:
        for path in args.golden:
            recs, neg = score_file(path, args.binary, args.chain_tool, args.include_tests, not args.no_chains)
            name = recs[0]["corpus"] if recs else Path(path).stem
            corpora[name] = dict(slices(recs), negatives_hit=neg)
            all_recs += recs
    except (ScorerError, subprocess.SubprocessError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    report = {"binary": str(args.binary), "chain_tool": args.chain_tool, "corpora": corpora, "all": slices(all_recs)}
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
