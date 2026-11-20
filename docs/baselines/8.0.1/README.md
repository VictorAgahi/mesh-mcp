# Baselines of MeshMCP 8.0.1 (reference for the 9.0 work)

Measured 2026-11-20 on an Apple M2, 8 cores, 16 GB RAM, macOS 26.6.2 (`Darwin 25.6.0 arm64`).
Binary: `mesh-mcp 8.0.1`, sha256 `ada84e37e3e7ef65d09fc79469d9d20a357000b33267dd93ea7e94b24c6017d9`
(built from the 8.0.1 tree, copied to `/Users/victoragahi/cargo-targets/baseline-8.0.1/mesh-mcp`). Every
run used an isolated `HOME`, audit DB and socket; no `meshd` was touched. Nothing was estimated:
what could not be measured is stated as such.

## 1. Event scores (`events.json`, `events.md`)

```bash
python3 scripts/golden/score_events.py tests/golden/events/*.expected.yaml --binary <8.0.1> --json > events.json
python3 scripts/golden/score_events.py tests/golden/events/*.expected.yaml --binary <8.0.1>        > events.md
```

Defaults: chain tool `analyze_impact` (depth 5), `--chain-order strict`, line tolerance 2, test-file
sites ignored (4 ignored), optional items ignored (8, all in polyglot-shop). Run twice: the two JSON
outputs are **byte-identical** (`cmp`), so the baseline is deterministic.

| category | tp | fp | fn | precision | recall |
|---|---|---|---|---|---|
| producer | 30 | 2 | 10 | 93.8% | 75.0% |
| consumer | 32 | 1 | 11 | 97.0% | 74.4% |
| hop | 0 | 0 | 42 | n/a | 0.0% |
| chain (in order) | 2 | 0 | 28 | n/a | 6.7% |
| total | 64 | 3 | 91 | 95.4% | 41.3% |

Chains: 30; traced in order 2, presence in any order 3. With `min_hops >= 3`: 11 chains, 0 in both. Reverse: 1 chain, 0.
Longer than the depth cap of 5: 8 chains, 0. Line accuracy: 61 of 62 matched sites within 2 lines.

Plain reading:
1. 8.0.1 finds most literal producers and consumers (75% / 74% recall) and is almost always right when it does (94-97% precision, 3 false positives in all).
2. Where the code uses a plain Kafka/Redis literal (`kafka_basic`, `unresolved-flow`) it does well: 27 tp, recall 82%; `unresolved-flow` scores 35 of 37.
3. Hops are 0 of 42 by construction: 8.0.1 has no call edges (`Calls` does not exist), so every hop and every `layered_calls` chain misses.
4. Chains are 2 of 30. They fail by construction for three reasons: no call edges, analyze_impact lists rows by scope, role and path (not in chain order), and the depth cap is 5.
5. The two chains traced in order are one in unresolved-flow and one in layered-ts; any-order presence adds one more chain, so for that one the answer held every step but not in chain order.
6. Tags for features that do not exist yet score 0: `inproc_bus` (28 fn), `cqrs` (9), `cron` (7), `layered_calls` (66), `config_topic` (4 fn, 1 fp). These are the headroom of 9.0, not regressions.
7. `nestjs-monolith-bus` is 0 of 27: its golden items are tagged inproc_bus, cqrs, cron and layered_calls, none of which 8.0.1 reads.
8. Java is the weak spot (6 tp, 2 fp, 19 fn). The 3 false positives are 2 producers in polyglot-shop and 1 consumer in spring-monolith.
9. Line numbers are not a problem: 61 of 62 matched sites are within 2 lines.
10. Numbers that are failures by construction (do not read them as bugs): hop recall, chains in order, reverse chains, chains over the depth cap, the four unimplemented tags above.

## 2. Unresolved share (`unresolved_volontariapp.json`, `.md`)

```bash
python3 scripts/bench/eda_unresolved_share.py --config /Users/victoragahi/Developer/meta/.agents/mesh-mcp.toml --binary <8.0.1> --max-seconds 420 --json
```

Read-only on the workspace (graph + `run --standalone` with an isolated HOME). Indexing and all 136 queries took 2.2 s, well inside the 600 s limit.

| workspace | roots | files indexed | topics | no producer statically resolved | no consumer statically resolved |
|---|---|---|---|---|---|
| Volontariapp (meta workspace) | 16 | 5,260 | 136 | 82 (60.3%) | 31 (22.8%) |
| examples/unresolved-flow | 1 | 18 | 18 | 5 (27.8%) | 3 (16.7%) |
| examples/eda/layered-go | 1 | 8 | 4 | 1 (25.0%) | 2 (50.0%) |
| examples/eda/layered-py | 1 | 9 | 4 | 1 (25.0%) | 2 (50.0%) |
| examples/eda/layered-ts | 1 | 19 | 9 | 3 (33.3%) | 4 (44.4%) |
| examples/eda/nestjs-monolith-bus | 1 | 17 | 0 | n/a (no topic indexed) | n/a |
| examples/eda/spring-monolith | 1 | 15 | 4 | 1 (25.0%) | 2 (50.0%) |

File counts are the `Scanned N files` line of `mesh-mcp graph`. Topics are the hub nodes of
the graph; the shares are of analyze_impact(target=topic, depth=1) answers, test-file rows hidden.

## 3. kubernetes (`kubernetes.json`, `kubernetes.mesh-mcp.toml`)

```bash
MESH_MCP_BIN=<8.0.1> python3 scripts/bench/tier_bench.py --repo ~/bench-repos/kubernetes --runs 3 \
  --wait-calm 600 --calm-load 4 --workdir <scratch> --out kubernetes.json
```

`tier_bench.py` runs `scale_bench.py --no-budget --no-reload` three times, each with a fresh `HOME` (cold parse cache),
and takes the median. The config is the one it writes (one root, the whole repo; the absolute paths are the author's, edit them to reuse it).
Repo `~/bench-repos/kubernetes` at `16b319ff`; `git status` unchanged before and after (it already had an untracked `.agents/`).

| metric | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| boot (ms) | 8,061 | 6,395 | 6,124 | **6,395** |
| peak memory footprint (MB, `/usr/bin/time -l`) | 341.1 | 319.1 | 315.2 | **319.1** |
| smart_search p50 (ms) | 0.2 | 0.2 | 0.2 | **0.2** (result cache, see docs/quality.md) |
| smart_search p95 (ms) | 56.2 | 52.8 | 54.7 | **54.7** |
| files indexed | 30,527 | 30,527 | 30,527 | 30,527 |
| load1 at start | 3.18 | 3.73 | 3.98 | |

Reload latency: not measured (`--no-reload`, it would write into the repo).

Against the 2026-09-27 figures of docs/quality.md (step 4.3: 319.0 MB median, boot 12.6 s, p95 68.3 ms, same repo commit, same file count):
memory is identical (319.1 vs 319.0 MB). Boot is about half (6.4 s vs 12.6 s) and p95 about 20% lower. The machine model is the same
(M2, 16 GB); the binary is different (8.0.1 against the 4c9589e release of the time) and the machine state (macOS 26.6.2 now, load 3-4 in both)
may differ, so the table cannot say which of the two explains the speed-up: only a run of both binaries back to back could.
