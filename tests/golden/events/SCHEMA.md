# Event golden files: schema (v9 plan, ticket T1)

One file per corpus: `tests/golden/events/<corpus>.expected.yaml`, scored by
`scripts/golden/score_events.py`. The truth is annotated BY HAND from the source of the
fixture. It is never generated from, or copied out of, MeshMCP output.

```yaml
corpus: layered-ts                 # same as the file name stem
root: examples/eda/layered-ts      # repo-relative directory indexed as the only root
config: mesh-mcp.toml              # optional, relative to root; default: a generated minimal config
languages: [ts]                    # informational; every item also carries its own language

producers:
  - id: p1                         # unique within the file
    topic: order.created           # topic key as an agent would name it (lower-case, see normalize_topic_key)
    file: src/order.publisher.ts   # relative to root
    line: 14                       # 1-based line of the publish call expression
    language: ts                   # ts | js | java | go | python | csharp | rust | sql
    tags: [kafka_basic]            # feature tags, see below; used for per-ticket score slices
    optional: false                # optional, default false; true: ignored by the scorer both ways
consumers:
  - id: c1
    topic: order.created
    file: src/order.consumer.ts
    line: 9                        # line of the decorator / subscribe / listener registration
    language: ts
    tags: [kafka_basic]

hops:                              # intra-repo call edges that lie on a chain (caller -> callee)
  - id: h1
    from: {symbol: OrderConsumer.handle, file: src/order.consumer.ts, line: 11}
    to:   {symbol: OrderService.process, file: src/order.service.ts, line: 20}
    via: injected_field            # this | injected_field | imported_function | same_module | interface_impl
    tags: [layered_calls]

chains:                            # end-to-end flows; "full chain" score
  - id: ch1
    entry: {kind: topic, value: order.created}   # topic | route | handler | cron
    direction: forward             # forward | reverse
    steps:                         # ordered; each is a source location an answer must show
      - {kind: consume, file: src/order.consumer.ts,  line: 9,  topic: order.created}
      - {kind: call,    file: src/order.service.ts,   line: 20}
      - {kind: call,    file: src/invoice.service.ts, line: 8}
      - {kind: produce, file: src/invoice.publisher.ts, line: 12, topic: invoice.issued}
    tags: [layered_calls]
    min_hops: 2                    # number of `call` steps in the chain (for the ">= 3 hops" metric)

# Things that look like a flow but are NOT one (precision traps). Optional.
negatives:
  - {kind: producer, file: src/noise.ts, line: 5, note: "logger.emit is not a bus"}
```

## Matching rules (what the scorer implements)

- A producer / consumer is matched on `(file, normalized topic key)`; `line` is checked
  within a tolerance of 2 and reported as "line accuracy", never as a miss by itself.
- Several sites with the same `(file, topic)` are matched one to one, nearest line first.
- A hop is matched on `(from.file, to.file)` plus the symbol names; lines are informational.
- A chain is "traced" when every step location (`file`, line within 2) appears, IN ORDER,
  in the tool answer for the entry. The scorer extracts `path:line` occurrences from the
  Markdown answer (analyze_impact in 8.x, trace_flow in 9.x), so it is tool-agnostic.
- Edge confidence: an `ambiguous` edge counts as emitted. The report always shows two precision
  columns: "ambiguous counted" and "ambiguous excluded". `heuristic` counts as emitted.
- `negatives` found in the output are false positives.

## Annotation conventions

- `min_hops` is the number of `call` steps in the chain. A route, cron or handler entry that is the
  first step counts as one call step (it is a `call` step on the entry handler).
- A `call` step points at the callee declaration line. A hop's `from.line` is the call site and its
  `to.line` is the callee declaration, so every non-entry `call` step equals some hop's `to`.
- A producer or consumer whose topic is bound where the client is built (a Go `kafka.Writer` or
  `kafka.ReaderConfig` literal) sits on that construction line, not on the later `WriteMessages` or
  `ReadMessage` call.
- CQRS and Spring in-process keys are the command or event class name as written in the source;
  the scorer lower-cases it.
- `optional: true` marks a site the scorer ignores both ways: not a false negative when missing and
  not a false positive when found. It is used with the tag `declared_pattern` for sites that exist
  only because a fixture's own custom pattern declares them, while the source makes no broker call.
  An optional item must not be a step of any chain.

## Tags (feature slices, one per later ticket)

`kafka_basic`, `layered_calls` (T2/T3), `inproc_bus` (T4), `cqrs` (T4), `cron` (T4), `sql` (T5),
`config_topic` (T6), `broker_key` (T7), `jvm_broker`, `go_broker`, `py_broker`, `cs_broker` (T8),
`declared_pattern` (site declared by a custom pattern only; always `optional: true`).
A tag whose feature is not implemented yet is EXPECTED to score 0 on 8.0.1: that is the baseline.

## Report shape

Per corpus and total, per category (producer, consumer, hop, chain) and per language and per tag:
`tp / fp / fn`, precision, recall, in JSON (`--json`) and a Markdown table. Exit code 0 unless a
`--fail-under-*` threshold is given.
