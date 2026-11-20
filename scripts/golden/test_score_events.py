#!/usr/bin/env python3
"""Self-tests of score_events.py (stdlib only; no mesh-mcp binary needed).

    python3 -I -m unittest scripts/golden/test_score_events.py
"""
import contextlib
import io
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import score_events as se  # noqa: E402

ROOT = "/ws/corpus"
SCHEMA = Path(__file__).resolve().parents[2] / "tests" / "golden" / "events" / "SCHEMA.md"


def graph(*edges):
    """edges: (kind, file, line, symbol, topic, confidence). Produces: code -> topic; Consumes: topic -> code."""
    nodes, out, ids = [], [], {}

    def node(key, name, kind, file, line):
        if key not in ids:
            ids[key] = len(nodes)
            nodes.append({"id": ids[key], "name": name, "kind": kind, "file_path": file, "line_start": line})
        return ids[key]

    for kind, file, line, sym, topic, conf in edges:
        code = node(("c", file, line, sym), sym, "ServiceClass", f"{ROOT}/{file}", line)
        top = node(("t", topic), topic, "EventStream", "event-bus", 1)
        a, b = (code, top) if kind == "Produces" else (top, code)
        out.append({"from": a, "to": b, "kind": kind, "confidence": conf})
    return {"nodes": nodes, "edges": out}


def golden(**kw):
    g = {"corpus": "syn", "root": "x", "producers": [], "consumers": [], "hops": [], "chains": [], "negatives": []}
    g.update(kw)
    return g


def item(i, topic, file, line, tags=("kafka_basic",), lang="ts"):
    return {"id": i, "topic": topic, "file": file, "line": line, "language": lang, "tags": list(tags)}


def run(g, gr, answers=None, include_tests=False, chain_order="strict"):
    recs, neg, _meta = se.score_corpus(g, se.extract_sites(gr, ROOT), answers, include_tests, chain_order)
    return recs, neg


def count(recs, category, outcome):
    return sum(r["category"] == category and r["outcome"] == outcome for r in recs)


class ProducerConsumer(unittest.TestCase):
    def test_tp_fp_fn(self):
        g = golden(
            producers=[item("p1", "order.created", "src/a.ts", 10)],
            consumers=[item("c1", "order.created", "src/missing.ts", 3)],
        )
        gr = graph(
            ("Produces", "src/a.ts", 11, "publish", "order.created", "exact"),
            ("Produces", "src/b.ts", 4, "extra", "other.topic", "exact"),
        )
        recs, _ = run(g, gr)
        self.assertEqual(count(recs, "producer", "tp"), 1)
        self.assertEqual(count(recs, "producer", "fp"), 1)
        self.assertEqual(count(recs, "consumer", "fn"), 1)
        s = se.summarize(recs)
        self.assertEqual((s["tp"], s["fp"], s["fn"]), (1, 1, 1))
        self.assertAlmostEqual(s["precision_ambiguous_counted"], 0.5)
        self.assertAlmostEqual(s["recall"], 0.5)
        self.assertEqual(s["line_accuracy"], 1.0)

    def test_line_is_accuracy_not_a_miss(self):
        g = golden(producers=[item("p1", "t", "src/a.ts", 10)])
        recs, _ = run(g, graph(("Produces", "src/a.ts", 40, "f", "t", "exact")))
        self.assertEqual(count(recs, "producer", "tp"), 1)
        self.assertEqual(se.summarize(recs)["line_accuracy"], 0.0)

    def test_one_to_one_nearest_line(self):
        g = golden(producers=[item("p1", "t", "src/a.ts", 10), item("p2", "t", "src/a.ts", 30)])
        gr = graph(("Produces", "src/a.ts", 30, "f", "t", "exact"), ("Produces", "src/a.ts", 10, "g", "t", "exact"))
        recs, _ = run(g, gr)
        self.assertEqual([r["dline"] for r in recs], [0, 0])

    def test_ambiguous_counts_as_emitted_two_precisions(self):
        g = golden(producers=[item("p1", "t1", "src/a.ts", 5)])
        gr = graph(
            ("Produces", "src/a.ts", 5, "f", "t1", "ambiguous"),
            ("Produces", "src/z.ts", 5, "g", "t2", "ambiguous"),
            ("Produces", "src/y.ts", 5, "h", "t3", "exact"),
        )
        s = se.summarize(run(g, gr)[0])
        self.assertEqual((s["tp"], s["fp"], s["fn"]), (1, 2, 0))
        self.assertAlmostEqual(s["precision_ambiguous_counted"], 1 / 3)
        self.assertEqual(s["precision_ambiguous_excluded"], 0.0)  # tp is ambiguous, one exact fp left
        self.assertEqual(s["recall"], 1.0)

    def test_topic_key_normalisation(self):
        self.assertEqual(se.normalize_topic_key("Streams.EVENT_CREATED"), "event_created")
        self.assertEqual(se.normalize_topic_key("Orders.Created"), "orders.created")
        self.assertFalse(se.same_topic("event.created", "event_created"))
        self.assertFalse(se.same_topic("order:created", "order_created"))
        self.assertTrue(se.same_topic("Order.Created", "order.created"))

    def test_test_files_hidden_unless_included(self):
        g = golden(producers=[item("p1", "t", "src/a.ts", 5)])
        gr = graph(("Produces", "src/a.ts", 5, "f", "t", "exact"), ("Produces", "src/a.spec.ts", 5, "g", "t", "exact"))
        self.assertEqual(count(run(g, gr)[0], "producer", "fp"), 0)
        self.assertEqual(count(run(g, gr, include_tests=True)[0], "producer", "fp"), 1)

    def test_negative_hit_is_a_false_positive(self):
        g = golden(negatives=[{"kind": "producer", "file": "src/noise.ts", "line": 5, "note": "logger"}])
        recs, neg = run(g, graph(("Produces", "src/noise.ts", 6, "log", "noise", "exact")))
        self.assertEqual(count(recs, "producer", "fp"), 1)
        self.assertEqual(len(neg), 1)
        self.assertEqual(run(g, graph())[1], [])


class Hops(unittest.TestCase):
    HOP = {"id": "h1", "from": {"symbol": "A.handle", "file": "src/a.ts", "line": 1},
           "to": {"symbol": "B.process", "file": "src/b.ts", "line": 2}, "via": "this", "tags": ["layered_calls"]}

    def test_absent_on_8x_is_zero_recall(self):
        recs, _ = run(golden(hops=[self.HOP]), graph())
        self.assertEqual((count(recs, "hop", "tp"), count(recs, "hop", "fn")), (0, 1))

    def test_calls_edge_matches(self):
        gr = {"nodes": [{"id": 0, "name": "handle", "kind": "ServiceClass", "file_path": f"{ROOT}/src/a.ts", "line_start": 1},
                        {"id": 1, "name": "process", "kind": "ServiceClass", "file_path": f"{ROOT}/src/b.ts", "line_start": 2}],
              "edges": [{"from": 0, "to": 1, "kind": "Calls", "confidence": "exact"}]}
        recs, _ = run(golden(hops=[self.HOP]), gr)
        self.assertEqual(count(recs, "hop", "tp"), 1)


class Chains(unittest.TestCase):
    STEPS = [{"kind": "consume", "file": "src/a.ts", "line": 9}, {"kind": "call", "file": "src/b.ts", "line": 20},
             {"kind": "produce", "file": "src/c.ts", "line": 12}]

    def traced(self, answer):
        g = golden(chains=[{"id": "ch1", "entry": {"kind": "topic", "value": "t"}, "steps": self.STEPS, "tags": []}])
        recs, _ = run(g, graph(), {"ch1": answer})
        return count(recs, "chain", "tp") == 1

    def test_in_order_traced_with_line_tolerance(self):
        self.assertTrue(self.traced("| 1 | `src/a.ts:10` |\nthen src/b.ts:20 and `x/src/c.ts:11`"))

    def test_out_of_order_not_traced(self):
        self.assertFalse(self.traced("src/b.ts:20\nsrc/a.ts:9\nsrc/c.ts:12"))

    def test_missing_step_or_far_line_not_traced(self):
        self.assertFalse(self.traced("src/a.ts:9 src/b.ts:20"))
        self.assertFalse(self.traced("src/a.ts:9 src/b.ts:30 src/c.ts:12"))

    def test_occurrence_extraction(self):
        self.assertEqual(se.extract_occurrences("`src/a.ts:10` event-bus:1 (src/b.go:7-9)"), [("src/a.ts", 10), ("src/b.go", 7)])


class Yaml(unittest.TestCase):
    def test_schema_example_parses(self):
        text = re.search(r"```yaml\n(.*?)```", SCHEMA.read_text(), re.S).group(1)
        g = se.parse_yaml(text)
        self.assertEqual(g["corpus"], "layered-ts")
        self.assertEqual(g["languages"], ["ts"])
        self.assertEqual(g["producers"][0]["tags"], ["kafka_basic"])
        self.assertEqual(g["hops"][0]["from"]["symbol"], "OrderConsumer.handle")
        self.assertEqual(len(g["chains"][0]["steps"]), 4)
        self.assertEqual(g["chains"][0]["entry"], {"kind": "topic", "value": "order.created"})
        self.assertEqual(g["negatives"][0]["note"], "logger.emit is not a bus")

class StrictTopics(unittest.TestCase):
    def test_separator_variants_are_different_topics(self):
        g = golden(producers=[item("p1", "order_created", "src/a.ts", 5)])
        recs, _ = run(g, graph(("Produces", "src/a.ts", 5, "f", "order:created", "exact")))
        self.assertEqual((count(recs, "producer", "tp"), count(recs, "producer", "fp"), count(recs, "producer", "fn")), (0, 1, 1))


class LineAndTests(unittest.TestCase):
    def test_tp_with_line_within_tolerance_column(self):
        g = golden(producers=[item("p1", "t", "src/a.ts", 10), item("p2", "u", "src/a.ts", 50)])
        gr = graph(("Produces", "src/a.ts", 15, "f", "t", "exact"), ("Produces", "src/a.ts", 50, "g", "u", "exact"))
        recs, _ = run(g, gr)
        self.assertEqual(se.summarize(recs)["tp"], 2)  # matching ignores the line
        self.assertEqual(se.summarize(recs, 2)["tp_line_ok"], 1)
        self.assertEqual(se.summarize(recs, 5)["tp_line_ok"], 2)

    def test_ignored_test_sites_are_counted(self):
        g = golden(producers=[item("p1", "t", "src/a.ts", 5)])
        gr = graph(("Produces", "src/a.ts", 5, "f", "t", "exact"), ("Produces", "src/a.spec.ts", 5, "g", "t", "exact"))
        _recs, _neg, meta = se.score_corpus(g, se.extract_sites(gr, ROOT), None, False)
        self.assertEqual(meta["ignored_test_sites"], 1)
        self.assertEqual(se.score_corpus(g, se.extract_sites(gr, ROOT), None, True)[2]["ignored_test_sites"], 0)


class ChainOrder(unittest.TestCase):
    STEPS = Chains.STEPS

    def verdict(self, answer, order="strict"):
        g = golden(chains=[{"id": "ch1", "entry": {"kind": "topic", "value": "t"}, "steps": self.STEPS, "tags": []}])
        recs, _ = run(g, graph(), {"ch1": answer}, chain_order=order)
        s = se.summarize(recs)
        return recs[0]["outcome"], s["chains_in_order"], s["chains_presence"]

    def test_in_order(self):
        self.assertEqual(self.verdict("src/a.ts:9 src/b.ts:20 src/c.ts:12"), ("tp", 1, 1))

    def test_out_of_order_strict_not_traced_any_traced(self):
        ans = "src/c.ts:12 src/a.ts:9 src/b.ts:20"
        self.assertEqual(self.verdict(ans, "strict"), ("fn", 0, 1))
        self.assertEqual(self.verdict(ans, "any"), ("tp", 0, 1))

    def test_missing_step_neither(self):
        ans = "src/a.ts:9 src/c.ts:12"
        self.assertEqual(self.verdict(ans, "strict"), ("fn", 0, 0))
        self.assertEqual(self.verdict(ans, "any"), ("fn", 0, 0))

    def test_empty_steps_never_traced(self):
        self.assertFalse(se.chain_traced([], [("a.ts", 1)]))


GOOD = """corpus: c
root: examples/x
producers:
  - {id: p1, topic: t, file: a.ts, line: 1}
chains:
  - id: ch1
    entry: {kind: topic, value: t}
    steps:
      - {kind: consume, file: a.ts, line: 1}
"""

CHAIN2 = """  - id: ch1
    entry: {kind: topic, value: t}
    steps:
      - {file: a.ts, line: 1}
"""


class Validation(unittest.TestCase):
    def load(self, text):
        return se.validate_golden(se.parse_yaml(text), "g.yaml")

    def test_good_golden_passes(self):
        self.assertEqual(self.load(GOOD)["corpus"], "c")

    def test_rejections(self):
        bad = {
            "empty steps": GOOD.replace("    steps:\n      - {kind: consume, file: a.ts, line: 1}\n", "    steps: []\n"),
            "duplicate chain ids": GOOD + CHAIN2,
            "duplicate producer ids": GOOD.replace("producers:\n", "producers:\n  - {id: p1, topic: t, file: b.ts, line: 2}\n"),
            "top-level list": "- a\n- b\n",
            "unterminated flow": GOOD.replace("line: 1}\nchains", "line: 1\nchains"),
            "unterminated quote": GOOD.replace("topic: t,", 'topic: "t,'),
            "unterminated quote scalar": GOOD.replace("root: examples/x", 'root: "examples/x'),
            "bad line type": GOOD.replace("line: 1}\nchains", "line: one}\nchains"),
        }
        for name, text in bad.items():
            with self.subTest(name), self.assertRaises(se.ScorerError):
                self.load(text)

    def test_main_exit_code_2_on_malformed(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "bad.expected.yaml"
            f.write_text("- just\n- a list\n")
            argv = sys.argv
            sys.argv = ["score_events.py", str(f), "--binary", "/nonexistent"]
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(se.main(), 2)
            finally:
                sys.argv = argv

    def test_root_and_config_must_stay_in_repo(self):
        with tempfile.TemporaryDirectory() as d:
            repo = Path(d) / "repo"
            (repo / "ex").mkdir(parents=True)
            root, cfg = se.resolve_corpus_paths({"corpus": "c", "root": "ex", "config": "m.toml"}, repo)
            self.assertTrue(root.is_dir() and cfg.name == "m.toml")
            for g in ({"corpus": "c", "root": "../outside"}, {"corpus": "c", "root": "/etc"},
                      {"corpus": "c", "root": "ex", "config": "../../etc/passwd"}):
                with self.subTest(g), self.assertRaises(se.ScorerError):
                    se.resolve_corpus_paths(g, repo)


FAKE_SERVER = r"""
import json, sys
for line in sys.stdin:
    m = json.loads(line)
    if "id" not in m:
        continue
    res = {"initialize": {}, "tools/list": {"tools": [{"name": "analyze_impact", "inputSchema": {"properties": {}}}]},
           "tools/call": {"content": [{"type": "text", "text": "src/a.ts:9"}]}}[m["method"]]
    note = json.dumps({"jsonrpc": "2.0", "method": "notifications/message", "params": {}})
    resp = json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": res})
    sys.stdout.write(note + "\n" + resp + "\n")  # one write: notification and response together
    sys.stdout.flush()
"""


class Session(unittest.TestCase):
    def test_two_lines_in_one_write(self):
        se.CALL_TIMEOUT_S = 10
        s = se.McpSession(None, None, ".", None, argv=[sys.executable, "-I", "-c", FAKE_SERVER])
        try:
            self.assertIn("analyze_impact", s.tools)
            self.assertEqual(s.call("analyze_impact", {"target": "t"}), "src/a.ts:9")
        finally:
            s.close()


class Optional(unittest.TestCase):
    def test_optional_items_ignored_both_ways(self):
        opt = dict(item("p1", "t", "src/a.ts", 5), optional=True)
        # found: neither tp nor fp
        recs, _ = run(golden(producers=[opt]), graph(("Produces", "src/a.ts", 5, "f", "t", "exact")))
        self.assertEqual(recs, [])
        # not found: not an fn
        recs, _ = run(golden(producers=[opt]), graph())
        self.assertEqual(recs, [])
        meta = se.score_corpus(golden(producers=[opt]), se.extract_sites(graph(), ROOT), None)[2]
        self.assertEqual(meta["optional_ignored"], 1)

    def test_optional_hop_and_chain_step(self):
        hop = dict(Hops.HOP, optional=True)
        self.assertEqual(run(golden(hops=[hop]), graph())[0], [])
        steps = [{"kind": "consume", "file": "src/a.ts", "line": 9}, {"kind": "call", "file": "src/b.ts", "line": 2, "optional": True}]
        g = golden(chains=[{"id": "ch1", "entry": {"kind": "topic", "value": "t"}, "steps": steps}])
        recs, _ = run(g, graph(), {"ch1": ""})
        self.assertEqual(recs, [])
        self.assertEqual(se.score_corpus(g, se.extract_sites(graph(), ROOT), {"ch1": ""})[2]["optional_ignored"], 1)


class ChainSummary(unittest.TestCase):
    def call(self, i):
        return {"kind": "call", "file": f"src/s{i}.ts", "line": 3}

    def test_min_hops_slice_and_tags(self):
        long_steps = [{"kind": "consume", "file": "src/a.ts", "line": 1}] + [self.call(i) for i in range(3)]
        short_steps = [{"kind": "consume", "file": "src/a.ts", "line": 1}, self.call(9)]
        chains = [{"id": "long", "entry": {"kind": "topic", "value": "t"}, "steps": long_steps, "tags": ["layered_calls"]},
                  {"id": "short", "entry": {"kind": "topic", "value": "t"}, "steps": short_steps, "tags": ["cqrs"]},
                  {"id": "explicit", "entry": {"kind": "topic", "value": "t"}, "steps": short_steps, "min_hops": 3, "tags": ["cqrs"]}]
        ans = {"long": "src/s2.ts:3 src/s1.ts:3 src/s0.ts:3 src/a.ts:1",  # presence only
               "short": "src/a.ts:1 src/s9.ts:3", "explicit": "src/a.ts:1 src/s9.ts:3"}
        recs, _ = run(golden(chains=chains), graph(), ans)
        cs = se.chain_summary(recs)
        self.assertEqual((cs["all"]["chains"], cs["all"]["in_order"], cs["all"]["presence"]), (3, 2, 3))
        self.assertEqual((cs["min_hops_ge_3"]["chains"], cs["min_hops_ge_3"]["in_order"], cs["min_hops_ge_3"]["presence"]), (2, 1, 2))
        self.assertEqual(cs["per_tag"]["cqrs"]["recall_in_order"], 1.0)
        self.assertEqual(cs["per_tag"]["layered_calls"]["recall_in_order"], 0.0)
        self.assertEqual(cs["per_tag"]["layered_calls"]["recall_presence"], 1.0)


class Tools(unittest.TestCase):
    def test_missing_chain_tool_is_a_clear_error(self):
        class S:
            tools = {"analyze_impact": {}}
        with self.assertRaises(se.ScorerError):
            se.trace_chain(S(), "trace_flow", {"entry": {"value": "t"}}, False)


if __name__ == "__main__":
    unittest.main()
