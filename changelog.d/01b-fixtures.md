- Add the annotated event golden corpus for 9.0: five new fixtures under `examples/eda/` (layered-ts,
  nestjs-monolith-bus, spring-monolith, layered-go, layered-py) and hand-written golden files for them
  plus `unresolved-flow` and `polyglot-shop` in `tests/golden/events/`.
- Add `tests/golden/events/check_golden.py` (stdlib validator) and the corpus README. No behaviour change.
