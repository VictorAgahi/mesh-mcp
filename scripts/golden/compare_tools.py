#!/usr/bin/env python3
"""scripts/golden/compare_tools.py -- are two mesh-mcp binaries byte-identical on the golden corpora?

For every golden file (except those named by --skip) it starts one `run --standalone` session per
binary on the corpus and calls, for every producer / consumer topic of the golden file,
`analyze_impact` (depth 1 and 3), `trace_flow`, then `visualize_mesh` once. It prints the number of
answers compared and the ones that differ; exit code 1 if any differs.

usage: compare_tools.py --old BIN --new BIN [--skip CORPUS ...] [--max-topics N]

Used to check that a change leaves single-broker workspaces byte-identical (9.0 T7).
"""
import argparse
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import score_events as se  # noqa: E402

REPO = HERE.parents[1]


def answers(binary, golden, topics):
    out = {}
    with tempfile.TemporaryDirectory() as home:
        root = REPO / golden["root"]
        env = se.isolated_env(home)
        cfg = root / golden["config"] if golden.get("config") else se.corpus_config(golden, root, home)
        session = se.McpSession(binary, cfg, root, env)
        try:
            for t in topics:
                out[f"analyze_impact {t} depth 1"] = session.call("analyze_impact", {"target": t, "limit": 200})
                out[f"analyze_impact {t} depth 3"] = session.call(
                    "analyze_impact", {"target": t, "depth": 3, "limit": 200})
                if "trace_flow" in session.tools:
                    out[f"trace_flow {t}"] = session.call("trace_flow", {"target": t})
            out["visualize_mesh"] = session.call("visualize_mesh", {})
        finally:
            session.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--skip", nargs="*", default=[])
    ap.add_argument("--max-topics", type=int, default=1000)
    args = ap.parse_args()
    total = same = 0
    for path in sorted((REPO / "tests/golden/events").glob("*.expected.yaml")):
        golden = se.load_golden(str(path))
        if golden["corpus"] in args.skip:
            continue
        topics = sorted({str(x["topic"]) for x in golden["producers"] + golden["consumers"]})[: args.max_topics]
        a, b = answers(args.old, golden, topics), answers(args.new, golden, topics)
        diff = [k for k in a if a[k] != b.get(k)]
        total += len(a)
        same += len(a) - len(diff)
        print(f"{golden['corpus']}: {len(a)} answers, {len(diff)} differ {diff[:3]}")
    print(f"TOTAL {total} answers compared, {same} byte-identical")
    return 0 if total == same else 1


if __name__ == "__main__":
    sys.exit(main())
