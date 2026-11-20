#!/usr/bin/env python3
"""Self-tests of run.py with a fake `claude` (stdlib only; nothing is spent).

    python3 -I scripts/bench/agent-ab/test_run.py
"""
import argparse
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run  # noqa: E402

FAKE_CLAUDE = """#!{py}
import json
print(json.dumps({{"type": "assistant", "message": {{"content": [{{"type": "tool_use", "name": "Read"}}]}}}}))
print(json.dumps({{"type": "result", "is_error": False, "num_turns": 2, "total_cost_usd": 0.1,
                  "usage": {{"input_tokens": 5, "output_tokens": 6}}, "result": "done"}}))
"""


def args(claude, **kw):
    d = dict(claude=claude, output_format="stream-json", permission_mode="plan", model=None, max_budget_usd=1.0,
             timeout=30, reset_worktree=False, claude_arg=None, results="/tmp/unused.jsonl", mesh_binary="/x/mesh-mcp",
             mesh_config="/x/c.toml")
    d.update(kw)
    return argparse.Namespace(**d)


class Execute(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = Path(self.tmp.name)
        self.claude = d / "claude"
        self.claude.write_text(FAKE_CLAUDE.format(py=sys.executable))
        self.claude.chmod(0o755)
        self.repo = d / "repo"
        self.repo.mkdir()
        for c in (["init", "-q"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x"]):
            subprocess.run(["git", "-C", str(self.repo), *c], check=True, capture_output=True)

    def tearDown(self):
        self.tmp.cleanup()

    def task(self, **kw):
        return dict({"id": "T1", "prompt": "p", "repo": str(self.repo)}, **kw)

    RUN = {"task": "T1", "arm": "A", "rep": 0, "order": 0}

    def test_paid_row_survives_success_cmd_timeout(self):
        run.SUCCESS_TIMEOUT_S = 1
        try:
            row = run.execute(self.RUN, self.task(success_cmd="sleep 5"), args(str(self.claude)))
        finally:
            run.SUCCESS_TIMEOUT_S = 600
        self.assertEqual(row["status"], "ok")  # a rerun must not pay twice
        self.assertEqual((row["input_tokens"], row["tool_calls"]), (5, 1))
        self.assertIn("success_cmd", row["error"])
        self.assertIsNone(row["success"])

    def test_success_cmd_result(self):
        self.assertTrue(run.execute(self.RUN, self.task(success_cmd="true"), args(str(self.claude)))["success"])
        self.assertFalse(run.execute(self.RUN, self.task(success_cmd="false"), args(str(self.claude)))["success"])

    def test_prepare_failure_is_a_row_not_a_crash(self):
        row = run.execute(self.RUN, self.task(repo=self.tmp.name + "/missing"), args(str(self.claude)))
        self.assertEqual(row["status"], "error")
        self.assertIn("prepare_repo", row["error"])
        self.assertNotIn("input_tokens", row)

    def test_dirty_tree_is_a_row(self):
        (self.repo / "f").write_text("x")
        row = run.execute(self.RUN, self.task(), args(str(self.claude)))
        self.assertIn("dirty", row["error"])

    def test_timeout_keeps_a_row(self):
        slow = Path(self.tmp.name) / "slow"
        slow.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(5)\n")
        slow.chmod(0o755)
        row = run.execute(self.RUN, self.task(), args(str(slow), timeout=1))
        self.assertEqual(row["status"], "timeout")


class Config(unittest.TestCase):
    def test_arm_b_gets_an_isolated_home(self):
        with tempfile.TemporaryDirectory() as w:
            path = run.mcp_config("B", args("claude"), {}, w)
            env = json.loads(path.read_text())["mcpServers"]["mesh-mcp"]["env"]
        self.assertTrue(env["HOME"].startswith(w))
        self.assertIn("MESH_AUDIT_DB", env)
        self.assertIn("MESH_SOCKET_PATH", env)

    def test_unsafe_args(self):
        self.assertTrue(run.unsafe_claude_args(["--dangerously-skip-permissions"]))
        self.assertTrue(run.unsafe_claude_args(["--permission-mode", "bypassPermissions"]))
        self.assertTrue(run.unsafe_claude_args(["--allowedTools", "Read", "Bash(git:*)"]))
        self.assertFalse(run.unsafe_claude_args(["--permission-mode=plan", "--allowedTools", "Read"]))


class Main(unittest.TestCase):
    def main(self, extra, env=None):
        with tempfile.TemporaryDirectory() as d:
            tasks = Path(d) / "t.json"
            tasks.write_text(json.dumps({"tasks": [{"id": "T1", "prompt": "p", "repo": d}]}))
            argv = ["run.py", "--tasks", str(tasks), "--results", str(Path(d) / "r.jsonl"), "--arms", "A", *extra]
            old, olde = sys.argv, dict(os.environ)
            sys.argv = argv
            os.environ.update(env or {})
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    return run.main()
            except SystemExit as e:
                return e.code
            finally:
                sys.argv = old
                os.environ.clear()
                os.environ.update(olde)

    def test_dry_run_is_default(self):
        self.assertEqual(self.main([]), 0)

    def test_execute_needs_a_positive_budget(self):
        env = {"MESH_AB_APPROVED_RUNS": "99"}
        self.assertEqual(self.main(["--execute"], env), 3)
        self.assertEqual(self.main(["--execute", "--max-budget-usd", "0"], env), 2)  # argparse error

    def test_execute_needs_approval(self):
        os.environ.pop("MESH_AB_APPROVED_RUNS", None)
        self.assertEqual(self.main(["--execute", "--max-budget-usd", "1"]), 3)

    def test_unsafe_claude_arg_refused(self):
        self.assertEqual(self.main(["--claude-arg=--dangerously-skip-permissions"]), 3)


if __name__ == "__main__":
    unittest.main()
