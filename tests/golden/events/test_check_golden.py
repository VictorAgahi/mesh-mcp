#!/usr/bin/env python3
"""Mutation tests for check_golden.py (stdlib unittest): a valid golden passes, and each
mutation of it is rejected with a message (never a traceback)."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_golden as cg  # noqa: E402

SOURCE = """\
// header
class Consumer {
  @Sub('order.created')
  handle(e) {
    this.svc.process(e);
  }
}

class Svc {
  process(e) {
    this.pub.announce(e);
  }
}
class Pub {
  announce(e) {
    this.bus.emit('stock.reserved', e);
  }
}
"""

GOLDEN = """\
corpus: fx
root: examples/fx
languages: [ts]
producers:
  - {id: p1, topic: stock.reserved, file: src/a.ts, line: 16, language: ts, tags: [kafka_basic]}
consumers:
  - {id: c1, topic: order.created, file: src/a.ts, line: 3, language: ts, tags: [kafka_basic]}
hops:
  - id: h1
    from: {symbol: Consumer.handle, file: src/a.ts, line: 5}
    to: {symbol: Svc.process, file: src/a.ts, line: 10}
    via: injected_field
    tags: [layered_calls]
  - id: h2
    from: {symbol: Svc.process, file: src/a.ts, line: 11}
    to: {symbol: Pub.announce, file: src/a.ts, line: 15}
    via: injected_field
    tags: [layered_calls]
chains:
  - id: ch1
    entry: {kind: topic, value: order.created}
    direction: forward
    steps:
      - {kind: consume, file: src/a.ts, line: 3, topic: order.created}
      - {kind: call, file: src/a.ts, line: 10}
      - {kind: call, file: src/a.ts, line: 15}
      - {kind: produce, file: src/a.ts, line: 16, topic: stock.reserved}
    tags: [layered_calls]
    min_hops: 2
"""

# (name, old, new): applied once with str.replace(old, new, 1) to GOLDEN.
MUTATIONS = [
    ("producer on a non-topic line", "file: src/a.ts, line: 16, language", "file: src/a.ts, line: 15, language"),
    ("missing file", "{id: p1, topic: stock.reserved, file: src/a.ts", "{id: p1, topic: stock.reserved, file: src/none.ts"),
    ("duplicate id", "{id: c1,", "{id: p1,"),
    ("min_hops mismatch", "min_hops: 2", "min_hops: 3"),
    ("line past EOF", "file: src/a.ts, line: 16, language", "file: src/a.ts, line: 99, language"),
    ("blank call-step line", "{kind: call, file: src/a.ts, line: 10}", "{kind: call, file: src/a.ts, line: 8}"),
    ("call step on a call site", "{kind: call, file: src/a.ts, line: 10}", "{kind: call, file: src/a.ts, line: 5}"),
    ("truncated topic", "{id: p1, topic: stock.reserved", "{id: p1, topic: stock"),
    ("bare string under producers", "producers:\n  - {id: p1,", "producers:\n  - just-a-string\n  - {id: p1,"),
    ("optional is not a bool", "language: ts, tags: [kafka_basic]}\nconsumers", "language: ts, tags: [kafka_basic], optional: yes please}\nconsumers"),
    ("chain step with no matching item", "{kind: produce, file: src/a.ts, line: 16, topic: stock.reserved}", "{kind: produce, file: src/a.ts, line: 17, topic: stock.reserved}"),
    ("unknown tag", "tags: [layered_calls]\n    min_hops", "tags: [made_up]\n    min_hops"),
    ("not a mapping at all", GOLDEN, "- 1\n- 2\n"),
    ("garbage", GOLDEN, "corpus: [unclosed\n"),
]


class CheckGolden(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        src = self.repo / "examples" / "fx" / "src"
        src.mkdir(parents=True)
        (src / "a.ts").write_text(SOURCE)
        self.path = self.repo / "fx.expected.yaml"

    def tearDown(self):
        self.tmp.cleanup()

    def run_text(self, text, use_pyyaml):
        self.path.write_text(text)
        return cg.run_one(self.path, self.repo, use_pyyaml)[0]

    def test_baseline_passes(self):
        for use in (False, True):
            with self.subTest(pyyaml=use):
                self.assertEqual(self.run_text(GOLDEN, use), [])

    def test_mutations_are_rejected(self):
        for name, old, new in MUTATIONS:
            assert old in GOLDEN, name
            for use in (False, True):
                with self.subTest(mutation=name, pyyaml=use):
                    errors = self.run_text(GOLDEN.replace(old, new, 1), use)
                    self.assertTrue(errors, f"mutation not detected: {name}")

    def test_sections_of_the_wrong_type_do_not_crash(self):
        for section in ("producers", "consumers", "hops", "chains", "negatives"):
            with self.subTest(section=section):
                errors = self.run_text(GOLDEN + f"{section}: nope\n" if section not in GOLDEN
                                       else GOLDEN.replace(f"{section}:", f"{section}: nope\nx{section}:", 1), False)
                self.assertTrue(errors)

    def test_topic_boundary(self):
        self.assertTrue(cg.has_topic("emit('stock.reserved', e)", "stock.reserved"))
        self.assertFalse(cg.has_topic("emit('stock.reserved', e)", "stock"))
        self.assertFalse(cg.has_topic("emit('restock.reserved', e)", "stock.reserved"))
        self.assertTrue(cg.has_topic("generate_report.delay(order_id)", "generate_report"))


if __name__ == "__main__":
    unittest.main()
