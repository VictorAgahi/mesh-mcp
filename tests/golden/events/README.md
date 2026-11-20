# Event golden corpus (9.0 ground truth)

Hand-annotated ground truth for "what does handling event X trigger": producers, consumers,
intra-repo call hops and end-to-end chains. The format and matching rules are in
[SCHEMA.md](SCHEMA.md); `scripts/golden/score_events.py` scores a tool answer against it.

## How it was built

- Fixtures live in `examples/eda/<corpus>/` (small repos, one `mesh-mcp.toml` and one manifest
  each). `unresolved-flow` and `polyglot-shop` are the existing examples, annotated without edits.
- Every file below was annotated BY HAND from the fixture source. It was never generated from, or
  copied out of, MeshMCP output, and a fixture was never edited to make the tool pass or fail.
  Things 8.0.1 cannot see (in-process buses, layered calls, config topics, interface calls) are
  annotated anyway and tagged: they are expected to score 0 on 8.0.1, which is the baseline.
- Conventions: a producer or consumer sits on the line that carries the topic (the call, the
  decorator argument, or the Go `kafka.Writer` / `ReaderConfig` literal); an in-process bus topic
  is the event name string, or the CLASS name for NestJS CQRS and Spring events; a hop `from` line is
  the call site and `to` line the callee declaration; a `call` step points at the callee declaration;
  `min_hops` is the number of `call` steps (a route, cron or handler entry counts as one);
  a Celery task is a consumer keyed by the task function name and `.delay()` is its producer.

## Add a case

1. Write or extend a fixture under `examples/eda/<corpus>/` (keep files short, add a `mesh-mcp.toml`).
2. Find the lines with `rg -n`, then write `<corpus>.expected.yaml` by reading the source.
3. Run `python3 tests/golden/events/check_golden.py` until it passes, then refresh the table below
   with `--table`.

`check_golden.py` is stdlib only (it uses PyYAML when importable, else a built-in reader for the
subset the files use). It checks keys and enums, unique ids, that every file exists, that every line
is inside its file and carries the topic or symbol name (waived for `config_topic` items and
`${...}` placeholders), and that `min_hops` equals the number of `call` steps.

## Corpora

| Corpus | Languages | Producers | Consumers | Hops | Chains | Chains >= 3 call steps | Tags (items) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- |
| layered-go | go | 3 | 2 | 6 | 4 | 1 | kafka_basic 5, layered_calls 10 |
| layered-py | python | 3 | 2 | 4 | 2 | 2 | kafka_basic 3, layered_calls 6, py_broker 3 |
| layered-ts | ts | 6 | 5 | 13 | 10 | 4 | kafka_basic 12, layered_calls 22 |
| nestjs-monolith-bus | ts | 7 | 8 | 7 | 5 | 2 | cqrs 9, cron 4, inproc_bus 20, layered_calls 11 |
| polyglot-shop | go, java, python, rust, ts | 4 | 6 | 4 | 4 | 1 | go_broker 4, kafka_basic 4, layered_calls 7, py_broker 4 |
| spring-monolith | java | 5 | 4 | 8 | 5 | 2 | config_topic 3, cron 3, inproc_bus 8, kafka_basic 8, layered_calls 13 |
| unresolved-flow | ts | 14 | 21 | 0 | 3 | 0 | config_topic 2 |

"Chains >= 3 call steps" counts chains whose `min_hops` is at least 3. Tag counts are items
(producers, consumers, hops and chains) carrying the tag.
