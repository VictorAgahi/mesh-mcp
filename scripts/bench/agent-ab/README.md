# Agent A/B runner

Headless `claude -p` A/B of an agent without (arm A) and with (arm B) MeshMCP, following the
protocol of [`docs/pilot-scorecard.md`](../../../docs/pilot-scorecard.md). One JSON line per run
is appended to the results file, with tokens (input, output, cache read, cache write), tool calls
(all, MeshMCP), turns, API and wall time, cost and success.

## Safety

A run spends real money. The default is a dry run (prints the plan, the example command and the
number of runs, calls nothing). A real run needs both `--execute` and
`MESH_AB_APPROVED_RUNS=<N>` with N at least the number of runs still to execute; otherwise the
script refuses (exit 3). Agree the run count and budget before setting the variable.

## Use

```bash
# 1. Freeze the tasks (prompts and success commands) before any run.
cat > tasks.json <<'JSON'
{"tasks": [{"id": "T1", "prompt": "Which services consume order.created?",
            "repo": "/abs/path/to/repo", "commit": "<sha>", "success_cmd": "grep -q ..."}]}
JSON

# 2. Dry run (default): plan and estimated run count.
python3 scripts/bench/agent-ab/run.py --tasks tasks.json --results out.jsonl \
  --mesh-binary /path/mesh-mcp --mesh-config /path/mesh-mcp.toml --max-budget-usd 2

# 3. Real run, only once approved.
MESH_AB_APPROVED_RUNS=36 python3 scripts/bench/agent-ab/run.py --execute --tasks tasks.json \
  --results out.jsonl --mesh-binary /path/mesh-mcp --mesh-config /path/mesh-mcp.toml --model <id>
```

Re-running the same command resumes: finished `(task, arm, rep)` rows are skipped, failed rows
are re-run with `--retry-errors`. `--selftest` checks the output parser and the ABBA order.

## Behaviour

- Both arms use `--strict-mcp-config`, so the user's own MCP servers never leak into a run. Arm B
  starts `mesh-mcp run --standalone` with its own audit DB (`<results>.audit.db`) and socket and
  never touches a running `meshd`.
- Order alternates per repetition (rep 0: A,B; rep 1: B,A; ...). At least 3 reps per cell.
- Each run needs a clean working tree at the task's `commit`; `--reset-worktree` resets the task
  repo (`git reset --hard` + `git clean -fd`), so use it on a throwaway clone only.
- Tool calls come from the stream-json transcript (default `--output-format stream-json`); with
  `--output-format json` they are null and only `num_turns` is available.
- `success` is the exit status of the task's `success_cmd`; without one it is null and must be
  judged blind, as the protocol says. Extra `claude` flags go through `--claude-arg`.
