#!/usr/bin/env python3
"""Self-tests of score_events.py (stdlib only; no mesh-mcp binary needed).

    python3 -I -m unittest scripts/golden/test_score_events.py
"""
import os
import re
import sys
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


def run(g, gr, answers=None, include_tests=False):
    recs, neg = se.score_corpus(g, se.extract_sites(gr, ROOT), answers, include_tests)
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
        self.assertTrue(se.same_topic("event.created", "event_created"))

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


class Tools(unittest.TestCase):
    def test_missing_chain_tool_is_a_clear_error(self):
        class S:
            tools = {"analyze_impact": {}}
        with self.assertRaises(se.ScorerError):
            se.trace_chain(S(), "trace_flow", {"entry": {"value": "t"}}, False)


if __name__ == "__main__":
    unittest.main()
