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

## What these corpora are (and are not)

- The `examples/eda/*` corpora are SYNTHETIC and STATIC-ONLY. They are not bootable applications
  (no `@SpringBootApplication` or `@EnableScheduling`, a partial NestJS module registration, no
  build outputs): they exist to exercise static extraction, nothing runs them.
- Line placement within the scorer tolerance of 2: in `spring-monolith`, consumers c1 and c2 point at
  the method line and c3 and c4 at the annotation line; in `layered-py`, consumer c2 (Celery) points
  at the `def` line (the decorator is one line above).
- `polyglot-shop` mixes real sites (the TypeScript producer, the Java listener and producer) with
  sites declared only by the fixture's own custom patterns, where the source makes no broker call.
  Those are `optional: true` with the tag `declared_pattern`: the scorer ignores them both ways.
- Decision on `polyglot-shop` p2 and its chain: `BillingSaga.java` calls `kafkaTemplate.send(topic,
  payload)` on a nested class named `KafkaTemplate` whose `send` only prints. The producer and the
  chain's produce step stay REAL, because the call has the shape of the Spring API, unlike the Go,
  Rust and Python sites, which use no messaging idiom at all.
- Wrong sites reported by the tool (for example a producer reported on the `@KafkaListener` method
  line instead of the publish line) are baseline defects counted against the tool. Reverse chains and
  chains longer than the tool's depth cap (5) are failures by construction on 8.0.1.
- `unresolved-flow` chains are limited to what one function links; its deletion pipeline is only
  annotated at topic level. The `ghost.spec.ts` negatives only count when the answer is produced with
  `--include-tests` (test files are hidden by default).
- The link to [`scripts/golden/score_events.py`](../../../scripts/golden/score_events.py) resolves once
  PR #110 is merged.

## Add a case

1. Write or extend a fixture under `examples/eda/<corpus>/` (keep files short, add a `mesh-mcp.toml`).
2. Find the lines with `rg -n`, then write `<corpus>.expected.yaml` by reading the source.
3. Run `python3 tests/golden/events/check_golden.py` until it passes, then refresh the table below
   with `--table`.

`check_golden.py` is stdlib only (it uses PyYAML when importable, `--no-pyyaml` forces the built-in
reader for the subset the files use; it prints which one ran). It checks keys, types and enums,
unique ids, `optional` being a bool, that every file exists, that every line is inside its file and
carries the topic (matched on segment boundaries) or callee name (waived for `config_topic` items and
`${...}` placeholders), that every non-entry `call` step is the declaration line of a hop target, that
every consume/produce step is an existing non-optional producer/consumer item, and that `min_hops`
equals the number of `call` steps. Malformed input exits 1 with a message, never a traceback.
`python3 tests/golden/events/test_check_golden.py` runs the mutation tests (each mutation of a valid
golden must be rejected).

## Corpora

| Corpus | Languages | Producers | Consumers | Hops | Chains | Chains >= 3 call steps | Optional items | Tags (items) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| bullmq-enum-queues | ts | 4 | 4 | 0 | 0 | 0 | 0 | enum_topic 8 |
| cs-brokers | csharp | 17 | 14 | 0 | 2 | 0 | 0 | broker_key 4, config_topic 3, cs_broker 32, kafka_basic 1 |
| declared-producers | ts | 12 | 2 | 0 | 0 | 0 | 0 | declared_call 12, enum_topic 12 |
| layered-go | go | 3 | 2 | 6 | 4 | 1 | 0 | kafka_basic 5, layered_calls 10 |
| layered-py | python | 3 | 2 | 4 | 2 | 2 | 0 | kafka_basic 3, layered_calls 6, py_broker 3 |
| layered-ts | ts | 6 | 5 | 13 | 10 | 4 | 0 | kafka_basic 12, layered_calls 22 |
| multi-broker | go, ts | 9 | 14 | 2 | 2 | 0 | 0 | broker_key 27, kafka_basic 7, layered_calls 4 |
| nestjs-monolith-bus | ts | 7 | 8 | 7 | 5 | 2 | 0 | cqrs 9, cron 4, inproc_bus 20, layered_calls 11 |
| polyglot-shop | go, java, python, rust, ts | 4 | 7 | 4 | 2 | 0 | 8 | declared_pattern 8, go_broker 2, kafka_basic 5, layered_calls 4, py_broker 3 |
| spring-monolith | java | 5 | 4 | 8 | 5 | 2 | 0 | config_topic 3, cron 3, inproc_bus 8, kafka_basic 8, layered_calls 13 |
| sql-outbox | go, java, python, sql, ts | 12 | 4 | 2 | 1 | 0 | 0 | kafka_basic 2, layered_calls 3, sql 15 |
| unresolved-flow | ts | 14 | 21 | 0 | 2 | 0 | 0 | config_topic 1 |

"Chains >= 3 call steps" counts chains whose `min_hops` is at least 3. Tag counts are items
(producers, consumers, hops and chains) carrying the tag. "Optional items" are ignored by the
scorer both ways (see below).
