## layered-go

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 5 | 0 | 10 | 100.0% | 100.0% | 33.3% | 5 | 0/4 | 0/4 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 0 | 0 | 4 | n/a | n/a | 0.0% | 0 | 0/4 | 0/4 |
| consumer | 2 | 0 | 0 | 100.0% | 100.0% | 100.0% | 2 | n/a | n/a |
| hop | 0 | 0 | 6 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 3 | 0 | 0 | 100.0% | 100.0% | 100.0% | 3 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| go | 5 | 0 | 10 | 100.0% | 100.0% | 33.3% | 5 | 0/4 | 0/4 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| kafka_basic | 5 | 0 | 0 | 100.0% | 100.0% | 100.0% | 5 | n/a | n/a |
| layered_calls | 0 | 0 | 10 | n/a | n/a | 0.0% | 0 | 0/4 | 0/4 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 4 | 0 | 0.0% | 0 | 0.0% |
| chains with min_hops >= 3 | 1 | 0 | 0.0% | 0 | 0.0% |
| direction: forward | 4 | 0 | 0.0% | 0 | 0.0% |
| longer than the cap (> 5 hops after the entry step) | 1 | 0 | 0.0% | 0 | 0.0% |
| tag: layered_calls | 4 | 0 | 0.0% | 0 | 0.0% |

Line accuracy (within 2): 100.0% of 5 matched sites. Sites ignored as test files: 0. Optional ignored: 0.

## layered-py

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 5 | 0 | 6 | 100.0% | 100.0% | 45.5% | 5 | 0/2 | 0/2 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 0 | 0 | 2 | n/a | n/a | 0.0% | 0 | 0/2 | 0/2 |
| consumer | 2 | 0 | 0 | 100.0% | 100.0% | 100.0% | 2 | n/a | n/a |
| hop | 0 | 0 | 4 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 3 | 0 | 0 | 100.0% | 100.0% | 100.0% | 3 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| python | 5 | 0 | 6 | 100.0% | 100.0% | 45.5% | 5 | 0/2 | 0/2 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| kafka_basic | 3 | 0 | 0 | 100.0% | 100.0% | 100.0% | 3 | n/a | n/a |
| layered_calls | 0 | 0 | 6 | n/a | n/a | 0.0% | 0 | 0/2 | 0/2 |
| py_broker | 2 | 0 | 1 | 100.0% | 100.0% | 66.7% | 2 | 0/1 | 0/1 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 2 | 0 | 0.0% | 0 | 0.0% |
| chains with min_hops >= 3 | 2 | 0 | 0.0% | 0 | 0.0% |
| direction: forward | 2 | 0 | 0.0% | 0 | 0.0% |
| longer than the cap (> 5 hops after the entry step) | 1 | 0 | 0.0% | 0 | 0.0% |
| tag: layered_calls | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: py_broker | 1 | 0 | 0.0% | 0 | 0.0% |

Line accuracy (within 2): 100.0% of 5 matched sites. Sites ignored as test files: 0. Optional ignored: 0.

## layered-ts

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 12 | 0 | 22 | 100.0% | 100.0% | 35.3% | 10 | 1/10 | 1/10 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 1 | 0 | 9 | n/a | n/a | 10.0% | 0 | 1/10 | 1/10 |
| consumer | 5 | 0 | 0 | 100.0% | 100.0% | 100.0% | 5 | n/a | n/a |
| hop | 0 | 0 | 13 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 6 | 0 | 0 | 100.0% | 100.0% | 100.0% | 5 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| ts | 12 | 0 | 22 | 100.0% | 100.0% | 35.3% | 10 | 1/10 | 1/10 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| kafka_basic | 12 | 0 | 0 | 100.0% | 100.0% | 100.0% | 10 | 1/1 | 1/1 |
| layered_calls | 0 | 0 | 22 | n/a | n/a | 0.0% | 0 | 0/9 | 0/9 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 10 | 1 | 10.0% | 1 | 10.0% |
| chains with min_hops >= 3 | 4 | 0 | 0.0% | 0 | 0.0% |
| direction: forward | 9 | 1 | 11.1% | 1 | 11.1% |
| direction: reverse | 1 | 0 | 0.0% | 0 | 0.0% |
| longer than the cap (> 5 hops after the entry step) | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: kafka_basic | 1 | 1 | 100.0% | 1 | 100.0% |
| tag: layered_calls | 9 | 0 | 0.0% | 0 | 0.0% |

Line accuracy (within 2): 90.9% of 11 matched sites. Sites ignored as test files: 0. Optional ignored: 0.

## nestjs-monolith-bus

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 0 | 0 | 27 | n/a | n/a | 0.0% | 0 | 0/5 | 0/5 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 0 | 0 | 5 | n/a | n/a | 0.0% | 0 | 0/5 | 0/5 |
| consumer | 0 | 0 | 8 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| hop | 0 | 0 | 7 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 0 | 0 | 7 | n/a | n/a | 0.0% | 0 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| ts | 0 | 0 | 27 | n/a | n/a | 0.0% | 0 | 0/5 | 0/5 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| cqrs | 0 | 0 | 9 | n/a | n/a | 0.0% | 0 | 0/2 | 0/2 |
| cron | 0 | 0 | 4 | n/a | n/a | 0.0% | 0 | 0/2 | 0/2 |
| inproc_bus | 0 | 0 | 20 | n/a | n/a | 0.0% | 0 | 0/5 | 0/5 |
| layered_calls | 0 | 0 | 11 | n/a | n/a | 0.0% | 0 | 0/4 | 0/4 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 5 | 0 | 0.0% | 0 | 0.0% |
| chains with min_hops >= 3 | 2 | 0 | 0.0% | 0 | 0.0% |
| direction: forward | 5 | 0 | 0.0% | 0 | 0.0% |
| longer than the cap (> 5 hops after the entry step) | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: cqrs | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: cron | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: inproc_bus | 5 | 0 | 0.0% | 0 | 0.0% |
| tag: layered_calls | 4 | 0 | 0.0% | 0 | 0.0% |

Line accuracy (within 2): n/a of 0 matched sites. Sites ignored as test files: 0. Optional ignored: 0.

## polyglot-shop

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 3 | 2 | 6 | 60.0% | 60.0% | 33.3% | 3 | 0/2 | 1/2 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 0 | 0 | 2 | n/a | n/a | 0.0% | 0 | 0/2 | 1/2 |
| consumer | 1 | 0 | 0 | 100.0% | 100.0% | 100.0% | 1 | n/a | n/a |
| hop | 0 | 0 | 4 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 2 | 2 | 0 | 50.0% | 50.0% | 100.0% | 2 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| go | 0 | 0 | 1 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| java | 2 | 1 | 1 | 66.7% | 66.7% | 66.7% | 2 | 0/1 | 1/1 |
| rust | 0 | 0 | 3 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| ts | 1 | 1 | 1 | 50.0% | 50.0% | 50.0% | 1 | 0/1 | 0/1 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| kafka_basic | 3 | 2 | 2 | 60.0% | 60.0% | 60.0% | 3 | 0/2 | 1/2 |
| layered_calls | 0 | 0 | 4 | n/a | n/a | 0.0% | 0 | n/a | n/a |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 2 | 0 | 0.0% | 1 | 50.0% |
| chains with min_hops >= 3 | 0 | 0 | n/a | 0 | n/a |
| direction: forward | 2 | 0 | 0.0% | 1 | 50.0% |
| longer than the cap (> 5 hops after the entry step) | 0 | 0 | n/a | 0 | n/a |
| tag: kafka_basic | 2 | 0 | 0.0% | 1 | 50.0% |

Line accuracy (within 2): 100.0% of 3 matched sites. Sites ignored as test files: 0. Optional ignored: 8 (producer p3, producer p4, consumer c2, consumer c3, consumer c4, consumer c5, consumer c6, consumer c7).

## spring-monolith

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 4 | 1 | 18 | 80.0% | 80.0% | 18.2% | 4 | 0/5 | 0/5 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 0 | 0 | 5 | n/a | n/a | 0.0% | 0 | 0/5 | 0/5 |
| consumer | 1 | 1 | 3 | 50.0% | 50.0% | 25.0% | 1 | n/a | n/a |
| hop | 0 | 0 | 8 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 3 | 0 | 2 | 100.0% | 100.0% | 60.0% | 3 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| java | 4 | 1 | 18 | 80.0% | 80.0% | 18.2% | 4 | 0/5 | 0/5 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| config_topic | 0 | 1 | 3 | 0.0% | 0.0% | 0.0% | 0 | 0/2 | 0/2 |
| cron | 0 | 0 | 3 | n/a | n/a | 0.0% | 0 | 0/1 | 0/1 |
| inproc_bus | 0 | 0 | 8 | n/a | n/a | 0.0% | 0 | 0/4 | 0/4 |
| kafka_basic | 4 | 1 | 4 | 80.0% | 80.0% | 50.0% | 4 | 0/3 | 0/3 |
| layered_calls | 0 | 0 | 13 | n/a | n/a | 0.0% | 0 | 0/5 | 0/5 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 5 | 0 | 0.0% | 0 | 0.0% |
| chains with min_hops >= 3 | 2 | 0 | 0.0% | 0 | 0.0% |
| direction: forward | 5 | 0 | 0.0% | 0 | 0.0% |
| longer than the cap (> 5 hops after the entry step) | 1 | 0 | 0.0% | 0 | 0.0% |
| tag: config_topic | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: cron | 1 | 0 | 0.0% | 0 | 0.0% |
| tag: inproc_bus | 4 | 0 | 0.0% | 0 | 0.0% |
| tag: kafka_basic | 3 | 0 | 0.0% | 0 | 0.0% |
| tag: layered_calls | 5 | 0 | 0.0% | 0 | 0.0% |

Line accuracy (within 2): 100.0% of 4 matched sites. Sites ignored as test files: 0. Optional ignored: 0.

## unresolved-flow

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 35 | 0 | 2 | 100.0% | 100.0% | 94.6% | 34 | 1/2 | 1/2 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 1 | 0 | 1 | n/a | n/a | 50.0% | 0 | 1/2 | 1/2 |
| consumer | 21 | 0 | 0 | 100.0% | 100.0% | 100.0% | 21 | n/a | n/a |
| producer | 13 | 0 | 1 | 100.0% | 100.0% | 92.9% | 13 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| ts | 35 | 0 | 2 | 100.0% | 100.0% | 94.6% | 34 | 1/2 | 1/2 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| config_topic | 0 | 0 | 1 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| untagged | 35 | 0 | 1 | 100.0% | 100.0% | 97.2% | 34 | 1/2 | 1/2 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 2 | 1 | 50.0% | 1 | 50.0% |
| chains with min_hops >= 3 | 0 | 0 | n/a | 0 | n/a |
| direction: forward | 2 | 1 | 50.0% | 1 | 50.0% |
| longer than the cap (> 5 hops after the entry step) | 1 | 0 | 0.0% | 0 | 0.0% |
| tag: untagged | 2 | 1 | 50.0% | 1 | 50.0% |

Line accuracy (within 2): 100.0% of 34 matched sites. Sites ignored as test files: 4. Optional ignored: 0.

## all corpora

### total

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| total | 64 | 3 | 91 | 95.4% | 95.4% | 41.3% | 61 | 2/30 | 3/30 |

### per category

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| chain | 2 | 0 | 28 | n/a | n/a | 6.7% | 0 | 2/30 | 3/30 |
| consumer | 32 | 1 | 11 | 97.0% | 97.0% | 74.4% | 32 | n/a | n/a |
| hop | 0 | 0 | 42 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| producer | 30 | 2 | 10 | 93.8% | 93.8% | 75.0% | 29 | n/a | n/a |

### per language

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| go | 5 | 0 | 11 | 100.0% | 100.0% | 31.2% | 5 | 0/4 | 0/4 |
| java | 6 | 2 | 19 | 75.0% | 75.0% | 24.0% | 6 | 0/6 | 1/6 |
| python | 5 | 0 | 6 | 100.0% | 100.0% | 45.5% | 5 | 0/2 | 0/2 |
| rust | 0 | 0 | 3 | n/a | n/a | 0.0% | 0 | n/a | n/a |
| ts | 48 | 1 | 52 | 97.9% | 97.9% | 48.0% | 45 | 2/18 | 2/18 |

### per tag

| slice | tp | fp | fn | precision (ambiguous counted) | precision (ambiguous excluded) | recall | tp with line <= 2 | chains in order | chains presence (any order) |
|---|---|---|---|---|---|---|---|---|---|
| config_topic | 0 | 1 | 4 | 0.0% | 0.0% | 0.0% | 0 | 0/2 | 0/2 |
| cqrs | 0 | 0 | 9 | n/a | n/a | 0.0% | 0 | 0/2 | 0/2 |
| cron | 0 | 0 | 7 | n/a | n/a | 0.0% | 0 | 0/3 | 0/3 |
| inproc_bus | 0 | 0 | 28 | n/a | n/a | 0.0% | 0 | 0/9 | 0/9 |
| kafka_basic | 27 | 3 | 6 | 89.7% | 89.7% | 81.8% | 25 | 1/6 | 2/6 |
| layered_calls | 0 | 0 | 66 | n/a | n/a | 0.0% | 0 | 0/24 | 0/24 |
| py_broker | 2 | 0 | 1 | 100.0% | 100.0% | 66.7% | 2 | 0/1 | 0/1 |
| untagged | 35 | 0 | 1 | 100.0% | 100.0% | 97.2% | 34 | 1/2 | 1/2 |

### chains (tool depth cap: 5; reverse chains and chains longer than the cap miss by construction, and still count as misses)

| slice | chains | traced in order | recall in order | presence (any order) | recall presence |
|---|---|---|---|---|---|
| all chains | 30 | 2 | 6.7% | 3 | 10.0% |
| chains with min_hops >= 3 | 11 | 0 | 0.0% | 0 | 0.0% |
| direction: forward | 29 | 2 | 6.9% | 3 | 10.3% |
| direction: reverse | 1 | 0 | 0.0% | 0 | 0.0% |
| longer than the cap (> 5 hops after the entry step) | 8 | 0 | 0.0% | 0 | 0.0% |
| tag: config_topic | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: cqrs | 2 | 0 | 0.0% | 0 | 0.0% |
| tag: cron | 3 | 0 | 0.0% | 0 | 0.0% |
| tag: inproc_bus | 9 | 0 | 0.0% | 0 | 0.0% |
| tag: kafka_basic | 6 | 1 | 16.7% | 2 | 33.3% |
| tag: layered_calls | 24 | 0 | 0.0% | 0 | 0.0% |
| tag: py_broker | 1 | 0 | 0.0% | 0 | 0.0% |
| tag: untagged | 2 | 1 | 50.0% | 1 | 50.0% |

Line accuracy (within 2): 98.4% of 62 matched sites. Sites ignored as test files: 4. Optional ignored: 8.

