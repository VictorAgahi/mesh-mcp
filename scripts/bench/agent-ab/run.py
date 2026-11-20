#!/usr/bin/env python3
"""Headless agent A/B runner (protocol: docs/pilot-scorecard.md).

Runs a frozen task list through `claude -p` in two arms and appends one JSON
line per run to a results file:
  A = without MeshMCP (empty MCP config, --strict-mcp-config)
  B = with MeshMCP    (`mesh-mcp run --standalone`, isolated audit DB)

SAFETY: this spends real money. The default is a dry run that only prints the
plan. A real run needs --execute AND the environment variable
MESH_AB_APPROVED_RUNS=<N> with N >= the number of runs still to execute.
Nothing in this script calls `claude` before both are satisfied.

usage:
    run.py --tasks tasks.json --results out.jsonl [--arms A B] [--reps 3]
           [--mesh-binary PATH --mesh-config PATH] [--model ID]
           [--max-budget-usd X] [--timeout S] [--reset-worktree]
           [--retry-errors] [--claude-arg ARG ...] [--execute]
    run.py --selftest

Tasks file (JSON, or the YAML subset of score_events.py; single-line prompts):
    {"tasks": [{"id": "T1", "prompt": "...", "repo": "/abs/path",
                "commit": "<sha, optional>", "success_cmd": "<shell, optional>",
                "mesh_config": "<optional, overrides --mesh-config>"}]}
`success_cmd` runs in `repo` after the run; exit 0 = success. Without it
`success` is null (judge by blind review, see the protocol).

Order: ABBA across repetitions per task (rep 0: A,B; rep 1: B,A; ...).
Resumable: a (task, arm, rep) already in the results file is skipped; rows
with status != ok are re-run only with --retry-errors.
Tool calls come from the stream-json transcript (--output-format stream-json,
the default here); with --output-format json only `num_turns` is available and
tool call counts are null. A run needs a clean working tree at the recorded
commit; --reset-worktree runs `git reset --hard <commit> && git clean -fd` in
the task repo before each run (destructive: use on a throwaway clone).
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "golden"))
import score_events as se  # noqa: E402  (parse_yaml only)

MESH_TOOL_PREFIX = "mcp__mesh-mcp__"
SUCCESS_TIMEOUT_S = 600


def load_tasks(path):
    text = Path(path).read_text()
    data = json.loads(text) if str(path).endswith(".json") else se.parse_yaml(text)
    tasks = data["tasks"] if isinstance(data, dict) else data
    for t in tasks:
        for k in ("id", "prompt", "repo"):
            if not t.get(k):
                raise SystemExit(f"error: task {t.get('id', '?')} lacks '{k}'")
    if len({t["id"] for t in tasks}) != len(tasks):
        raise SystemExit("error: duplicate task ids")
    return tasks


def plan_runs(tasks, arms, reps):
    runs = []
    for t in tasks:
        for r in range(reps):
            order = arms if r % 2 == 0 else arms[::-1]
            for pos, arm in enumerate(order):
                runs.append({"task": t["id"], "arm": arm, "rep": r, "order": pos})
    return runs


def done_keys(results_path, retry_errors):
    keys = set()
    if Path(results_path).exists():
        for line in Path(results_path).read_text().splitlines():
            if line.strip():
                row = json.loads(line)
                if row.get("status") == "ok" or not retry_errors:
                    keys.add((row["task"], row["arm"], row["rep"]))
    return keys


def mcp_config(arm, args, task, workdir):
    if arm == "A":
        cfg = {"mcpServers": {}}
    else:
        # A per-run HOME keeps the run away from the user's ~/.cache/mesh-mcp index cache and meshd.
        home = Path(workdir) / "home"
        home.mkdir(exist_ok=True)
        env = {"HOME": str(home), "MESH_AUDIT_DB": str(Path(args.results).resolve().with_suffix(".audit.db")),
               "MESH_SOCKET_PATH": str(Path(workdir) / "mesh.sock")}
        cfg = {"mcpServers": {"mesh-mcp": {
            "command": str(Path(args.mesh_binary).resolve()),
            "args": ["--config", str(Path(task.get("mesh_config") or args.mesh_config).resolve()), "run", "--standalone"],
            "env": env}}}
    path = Path(workdir) / f"mcp-{arm}.json"
    path.write_text(json.dumps(cfg))
    return path


def claude_cmd(arm, args, task, workdir):
    cmd = [args.claude, "-p", task["prompt"], "--output-format", args.output_format,
           "--mcp-config", str(mcp_config(arm, args, task, workdir)), "--strict-mcp-config",
           "--permission-mode", args.permission_mode]
    if args.output_format == "stream-json":
        cmd.append("--verbose")
    if args.model:
        cmd += ["--model", args.model]
    if args.max_budget_usd is not None:
        cmd += ["--max-budget-usd", str(args.max_budget_usd)]
    return cmd + list(args.claude_arg or [])


def parse_output(stdout):
    """(result event dict, tool_use names). Accepts one JSON object or ndjson events."""
    stdout = stdout.strip()
    events = []
    try:
        obj = json.loads(stdout)
        events = obj if isinstance(obj, list) else [obj]
    except json.JSONDecodeError:
        events = [json.loads(l) for l in stdout.splitlines() if l.strip().startswith("{")]
    result = next((e for e in reversed(events) if e.get("type") == "result"), {})
    tools = [b.get("name", "") for e in events if e.get("type") == "assistant"
             for b in (e.get("message", {}).get("content") or []) if isinstance(b, dict) and b.get("type") == "tool_use"]
    return result, tools, any(e.get("type") == "assistant" for e in events)


def row_from(run, result, tools, have_transcript):
    u = result.get("usage") or {}
    return {
        **run, "status": "ok" if result and not result.get("is_error") else "error",
        "input_tokens": u.get("input_tokens"), "output_tokens": u.get("output_tokens"),
        "cache_read_tokens": u.get("cache_read_input_tokens"), "cache_write_tokens": u.get("cache_creation_input_tokens"),
        "tool_calls": len(tools) if have_transcript else None,
        "mesh_tool_calls": sum(t.startswith(MESH_TOOL_PREFIX) for t in tools) if have_transcript else None,
        "num_turns": result.get("num_turns"), "duration_api_ms": result.get("duration_api_ms"),
        "cost_usd": result.get("total_cost_usd"), "session_id": result.get("session_id"),
        "result_excerpt": str(result.get("result", ""))[:500],
    }


def git(repo, *a):
    return subprocess.run(["git", "-C", str(repo), *a], capture_output=True, text=True, timeout=60, check=True)


def prepare_repo(task, reset):
    repo = task["repo"]
    if task.get("commit") and reset:
        git(repo, "reset", "--hard", task["commit"])
        git(repo, "clean", "-fdq")
    if git(repo, "status", "--porcelain").stdout.strip():
        raise RuntimeError(f"{repo} has a dirty working tree (clean it or pass --reset-worktree)")
    head = git(repo, "rev-parse", "HEAD").stdout.strip()
    if task.get("commit") and not head.startswith(task["commit"]):
        raise RuntimeError(f"{repo} is at {head[:12]}, task wants {task['commit']}")
    return head


def _text(x):
    return x.decode("utf-8", "replace") if isinstance(x, bytes) else (x or "")


def execute(run, task, args):
    """One run; always returns a row. A paid run's row is never lost: failures after
    the `claude` call (success_cmd, parsing) are recorded in `error`, and a row that
    carries usage keeps status ok so a rerun does not pay twice."""
    t0 = time.monotonic()
    row = dict(run, status="error", success=None, model=args.model, ts=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    try:
        row["commit"] = prepare_repo(task, args.reset_worktree)
    except (subprocess.SubprocessError, OSError, RuntimeError) as e:
        row["error"] = f"prepare_repo: {type(e).__name__}: {e}"[:300]  # nothing was spent
        row["wall_s"] = round(time.monotonic() - t0, 2)
        return row
    with tempfile.TemporaryDirectory(prefix="ab-") as workdir:
        try:
            p = subprocess.run(claude_cmd(run["arm"], args, task, workdir), cwd=task["repo"], capture_output=True,
                               text=True, timeout=args.timeout, stdin=subprocess.DEVNULL)
            result, tools, have = parse_output(p.stdout)
            row.update(row_from(run, result, tools, have))
            if p.returncode != 0 and row["status"] == "ok":
                row["status"] = "error"
        except subprocess.TimeoutExpired as e:
            row["status"] = "timeout"
            try:  # keep whatever usage the partial transcript carries
                row.update(row_from(run, *parse_output(_text(e.stdout))), status="timeout")
            except (ValueError, OSError):
                pass
        except (ValueError, OSError, subprocess.SubprocessError) as e:
            row["error"] = f"claude run: {type(e).__name__}: {e}"[:300]
    if task.get("success_cmd") and row["status"] == "ok":
        try:
            row["success"] = subprocess.run(task["success_cmd"], shell=True, cwd=task["repo"], capture_output=True,
                                            timeout=SUCCESS_TIMEOUT_S, stdin=subprocess.DEVNULL).returncode == 0
        except (subprocess.SubprocessError, OSError) as e:
            row["error"] = f"success_cmd: {type(e).__name__}"  # usage kept, status stays ok
    row["wall_s"] = round(time.monotonic() - t0, 2)
    return row


UNSAFE_ENV = "MESH_AB_ALLOW_UNSAFE_CLAUDE_ARGS"
SAFE_MODES = ("default", "plan")


def unsafe_claude_args(argv):
    """Permission-bypassing flags in a passthrough argument list (empty when safe)."""
    bad, a = [], list(argv or [])
    for i, x in enumerate(a):
        flag, _, val = x.partition("=")
        if flag in ("--dangerously-skip-permissions", "--allow-dangerously-skip-permissions"):
            bad.append(x)
        elif flag == "--permission-mode":
            if (val or (a[i + 1] if i + 1 < len(a) else "")) not in SAFE_MODES:
                bad.append(x)
        elif flag in ("--allowedTools", "--allowed-tools"):
            vals = [val] if val else []
            vals += [y for y in a[i + 1:] if not y.startswith("--")] if not val else []
            if any("Bash" in v for v in vals):
                bad.append(x)
    return bad


def selftest():
    ndjson = "\n".join(json.dumps(e) for e in [
        {"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "Read"}, {"type": "tool_use", "name": MESH_TOOL_PREFIX + "analyze_impact"}]}},
        {"type": "result", "is_error": False, "num_turns": 3, "total_cost_usd": 0.5, "duration_api_ms": 10,
         "usage": {"input_tokens": 1, "output_tokens": 2, "cache_read_input_tokens": 3, "cache_creation_input_tokens": 4}, "result": "ok"}])
    r = row_from({"task": "T1", "arm": "B", "rep": 0}, *parse_output(ndjson))
    assert (r["status"], r["tool_calls"], r["mesh_tool_calls"], r["cache_write_tokens"], r["cost_usd"]) == ("ok", 2, 1, 4, 0.5), r
    runs = plan_runs([{"id": "T1"}, {"id": "T2"}], ["A", "B"], 3)
    assert len(runs) == 12 and [x["arm"] for x in runs[:4]] == ["A", "B", "B", "A"]
    assert unsafe_claude_args(["--dangerously-skip-permissions"]) and unsafe_claude_args(["--permission-mode", "bypassPermissions"])
    assert unsafe_claude_args(["--allowedTools", "Read", "Bash(git:*)"]) and unsafe_claude_args(["--permission-mode=acceptEdits"])
    assert not unsafe_claude_args(["--permission-mode", "plan", "--allowedTools", "Read", "--max-turns", "5"])
    print("selftest ok")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tasks")
    ap.add_argument("--results", default="ab-results.jsonl")
    ap.add_argument("--arms", nargs="+", default=["A", "B"], choices=["A", "B"])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--mesh-binary")
    ap.add_argument("--mesh-config")
    ap.add_argument("--model")
    ap.add_argument("--claude", default="claude")
    ap.add_argument("--output-format", choices=["stream-json", "json"], default="stream-json")
    ap.add_argument("--permission-mode", default="acceptEdits", choices=["default", "acceptEdits", "plan"])
    ap.add_argument("--max-budget-usd", type=float)
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--reset-worktree", action="store_true")
    ap.add_argument("--retry-errors", action="store_true")
    ap.add_argument("--claude-arg", action="append")
    ap.add_argument("--execute", action="store_true", help="really run (default: dry run)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if not args.tasks:
        ap.error("--tasks is required")
    if "B" in args.arms and not (args.mesh_binary and args.mesh_config):
        ap.error("arm B needs --mesh-binary and --mesh-config")

    if args.max_budget_usd is not None and args.max_budget_usd <= 0:
        ap.error("--max-budget-usd must be > 0")
    unsafe = unsafe_claude_args(args.claude_arg)
    if unsafe and os.environ.get(UNSAFE_ENV) != "1":
        print(f"refused: permission-bypassing --claude-arg {unsafe} (set {UNSAFE_ENV}=1 to allow)", file=sys.stderr)
        return 3
    tasks = {t["id"]: t for t in load_tasks(args.tasks)}
    planned = plan_runs(list(tasks.values()), args.arms, args.reps)
    done = done_keys(args.results, args.retry_errors)
    todo = [r for r in planned if (r["task"], r["arm"], r["rep"]) not in done]

    print(f"plan: {len(tasks)} tasks x {len(args.arms)} arms x {args.reps} reps = {len(planned)} runs; "
          f"{len(planned) - len(todo)} already in {args.results}; {len(todo)} to execute")
    if args.max_budget_usd is not None:
        print(f"upper bound: {len(todo)} x ${args.max_budget_usd} = ${len(todo) * args.max_budget_usd:.2f}")
    for r in todo[:12]:
        print(f"  {r['task']} arm {r['arm']} rep {r['rep']}")
    if len(todo) > 12:
        print(f"  ... {len(todo) - 12} more")
    if todo:
        with tempfile.TemporaryDirectory(prefix="ab-") as w:
            print("example command (arm of the first run): " + " ".join(
                repr(c) if " " in c else c for c in claude_cmd(todo[0]["arm"], args, tasks[todo[0]["task"]], w)))
    if not args.execute:
        print("dry run: nothing executed (pass --execute and set MESH_AB_APPROVED_RUNS to run)")
        return 0
    if args.max_budget_usd is None:
        print("refused: --execute requires --max-budget-usd > 0", file=sys.stderr)
        return 3
    approved = os.environ.get("MESH_AB_APPROVED_RUNS", "")
    if not approved.isdigit() or int(approved) < len(todo):
        print(f"refused: set MESH_AB_APPROVED_RUNS=<N> with N >= {len(todo)} (got {approved or 'unset'})", file=sys.stderr)
        return 3
    with open(args.results, "a") as out:
        for i, run in enumerate(todo, 1):
            row = execute(run, tasks[run["task"]], args)
            out.write(json.dumps(row) + "\n")
            out.flush()
            print(f"[{i}/{len(todo)}] {run['task']} {run['arm']} rep {run['rep']}: {row['status']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
