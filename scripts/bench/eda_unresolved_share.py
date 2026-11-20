#!/usr/bin/env python3
"""Share of topics whose analyze_impact answer says "No producer / consumer
statically resolved" -- the 9.0 "unresolved share" metric on a real workspace.

usage:
    eda_unresolved_share.py --config PATH/mesh-mcp.toml --binary PATH/mesh-mcp
                            [--per-call-timeout 60] [--max-seconds 900] [--json]

Runs `mesh-mcp graph --format json` for the topic list (every hub node, file_path
"event-bus", unique by name), then one `run --standalone`
session (isolated HOME, audit DB and socket: it never touches a running meshd)
and calls analyze_impact(target=topic, depth=1) once per topic. Test-file rows
are hidden by the tool (include_tests is not set), as an agent would see them.
Reports total topics, the count and share of answers containing each note, and
wall time. Topics not reached before --max-seconds are reported as `skipped`
and left out of the shares. Output: Markdown table, or JSON with --json.
"""
import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "golden"))
import score_events as se  # noqa: E402  (McpSession, isolated_env, run_graph)

NO_PRODUCER = "No producer statically resolved"
NO_CONSUMER = "No consumer statically resolved"


def topics_of(graph):
    return sorted({n["name"] for n in graph.get("nodes", []) if se._is_hub(n)})


def share(n, total):
    return None if total == 0 else n / total


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", required=True, help="mesh config (its directory is the working directory)")
    ap.add_argument("--binary", required=True, help="mesh-mcp binary")
    ap.add_argument("--per-call-timeout", type=float, default=60.0)
    ap.add_argument("--max-seconds", type=float, default=900.0, help="overall budget for the topic loop")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    se.CALL_TIMEOUT_S = args.per_call_timeout
    cfg = Path(args.config).resolve()
    home = Path(tempfile.mkdtemp(prefix="eda-"))
    session = None
    t0 = time.monotonic()
    try:
        env = se.isolated_env(home)
        topics = topics_of(se.run_graph(args.binary, cfg, cfg.parent, env, home))
        session = se.McpSession(args.binary, cfg, cfg.parent, env)
        if "analyze_impact" not in session.tools:
            raise se.ScorerError("analyze_impact is not exposed by tools/list")
        no_prod = no_cons = answered = errors = 0
        for t in topics:
            if time.monotonic() - t0 > args.max_seconds:
                break
            try:
                text = session.call("analyze_impact", {"target": t, "depth": 1, "limit": 20})
            except se.ScorerError:
                errors += 1
                continue
            answered += 1
            no_prod += NO_PRODUCER in text
            no_cons += NO_CONSUMER in text
        res = {
            "binary": args.binary, "config": str(cfg), "total_topics": len(topics), "answered": answered,
            "errors": errors, "skipped": len(topics) - answered - errors,
            "no_producer": no_prod, "no_producer_share": share(no_prod, answered),
            "no_consumer": no_cons, "no_consumer_share": share(no_cons, answered),
            "wall_seconds": round(time.monotonic() - t0, 2),
        }
    except (se.ScorerError, subprocess.SubprocessError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    finally:
        if session:
            session.close()
        shutil.rmtree(home, ignore_errors=True)

    pct = lambda x: "n/a" if x is None else f"{x:.1%}"  # noqa: E731
    if args.json:
        print(json.dumps(res, indent=2))
    else:
        print("| topics | answered | errors | skipped | no producer | share | no consumer | share | wall (s) |")
        print("|---|---|---|---|---|---|---|---|---|")
        print(f"| {res['total_topics']} | {res['answered']} | {res['errors']} | {res['skipped']} | {res['no_producer']} | "
              f"{pct(res['no_producer_share'])} | {res['no_consumer']} | {pct(res['no_consumer_share'])} | {res['wall_seconds']} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
