# Parser support: how deep each language goes

MeshMCP does not understand every language equally. TypeScript and JavaScript are read at a much
higher level than C#, and C# than Ruby. This page says, per language and file format, what ends up
in the graph and where it stops, so you can judge how far an answer can be trusted on your code.

The statements here are checked against the extractors in `crates/mesh-parsers/src/languages/`
and against `CHANGELOG.md`. They describe what the code recognises, **not** how accurate it is on
your code: the only extraction accuracy measured against hand-written ground truth is gRPC edges
on three open-source demo applications ([`quality.md`](quality.md), and the README). Everything
else below is "recognised and unit-tested", not "validated".

---

## 1. The four levels

| Level | Name | What the language contributes |
| :---: | :--- | :--- |
| 1 | Declarations | Classes, interfaces and similar symbols, found by `smart_search` and returned with bodies stripped. At most one route idiom (Rails, Laravel / Symfony, Vapor, Akka HTTP). No imports, no gRPC, no events. |
| 2 | Contracts, no imports | Level 1 plus several contract idioms read from annotations and calls (HTTP routes, Kafka). No import graph, so `find_dependents` finds nothing for these files. |
| 3 | Imports and contracts | Level 2 plus the import graph (`find_dependents`), recognised by per-language idioms. Which of gRPC, HTTP and brokers are covered varies: each section says. Written "3-" below when a whole family is missing. |
| 4 | Semantic resolution | Level 3 plus scope-aware resolution inside a file and across files: constants and enums behind a topic name, path aliases, the client an object was built from, composed route prefixes, file-system routing. Only the JavaScript / TypeScript extractor does this; the level is defined by those features, so no other language can reach it as things stand. |

A level is a description of depth, not a quality score. The levels are not strictly cumulative (C#
has Kafka and ASP.NET routes but no imports; Rust has imports but no gRPC clients), so read the
capability table of the language rather than the number. Every level is pattern-based: there is no
type checker behind any of them.

| Language / format | Level | Section |
| :--- | :---: | :--- |
| TypeScript, JavaScript (and `.vue` / `.svelte` / `.astro` scripts) | 4 | [2.1](#21-typescript-and-javascript-level-4) |
| Java | 3 | [2.2](#22-java-level-3) |
| Go | 3- (no gRPC beyond `Register*`/`New*Client`; brokers in [3.9](#39-go-brokers)) | [2.3](#23-go-level-3) |
| Python | 3 | [2.4](#24-python-level-3) |
| Rust | 3- (no gRPC clients) | [2.5](#25-rust-level-3) |
| C++ | 3- (includes and gRPC servers only) | [2.6](#26-c-level-3-narrow) |
| Kotlin | 2 | [2.7](#27-kotlin-and-c-level-2) |
| C# | 2 | [2.7](#27-kotlin-and-c-level-2) |
| Ruby, PHP, Swift, Scala | 1 (plus one route idiom each) | [2.8](#28-ruby-php-swift-scala-level-1) |
| Protobuf | contract source | [3.1](#31-protobuf) |
| OpenAPI, AsyncAPI, Spring properties | contract source | [3.2](#32-openapi-asyncapi-spring-properties) |
| Markdown | docs | [3.3](#33-markdown) |
| SQL (`.sql`, and the SQL strings of TypeScript / JavaScript, Go, Java, Python) | events only: outbox inserts, `NOTIFY` / `pg_notify`, `LISTEN` | [3.4](#34-sql) |
| Declared producer helpers (`[[engines.contracts.producer_calls]]`, TypeScript / JavaScript) | producers: calls of an outbox or publish helper the config names | [3.11](#311-declared-producer-calls) |
| In-process buses and schedules (NestJS EventEmitter2 / CQRS / `@nestjs/schedule`, Spring application events / `@Scheduled`) | events and chain entries | [3.5](#35-in-process-buses-and-schedules) |
| Go brokers (RabbitMQ, NATS, Redis, SQS / SNS, Pub/Sub) | events and chain entries | [3.9](#39-go-brokers) |

---

## 2. Source languages

Extensions read: Java `.java`; Go `.go`; Python `.py`; Rust `.rs`; Kotlin `.kt .kts`; C# `.cs`; Ruby `.rb .rake`; PHP `.php .phtml`; Swift `.swift`; Scala `.scala .sc`; C++ `.cpp .cc .cxx .hpp .hh .hxx .h` (there is no C grammar; `.h` is read as C++); Protobuf `.proto`; TypeScript / JavaScript as in 2.1.

All source languages go through the same guards before tree-sitter runs (size, binary, line
length, nesting; see [architecture](architecture.md)) and are decapitated by `smart_search` the
same way. What differs is the extractor.

### 2.1 TypeScript and JavaScript (level 4)

Grammars: TypeScript, TSX and JavaScript (own grammar; a file with Flow or Babel annotations that
does not parse as JavaScript is re-parsed as TSX). Extensions: `.ts .mts .cts .tsx .js .jsx .mjs
.cjs`. Single-file components `.vue`, `.svelte` and `.astro` are read through their `<script>`
blocks, with the original line numbers.

| Capability | What is recognised |
| :--- | :--- |
| Declarations | classes, interfaces, functions, enums, type aliases, `const`/`let`, `declare` forms, namespaces, anonymous `export default`, CommonJS `module.exports` / `exports.x` |
| Imports | `import`, `require()`, dynamic `import()`, `import x = require()`, destructured requires; `tsconfig` / `jsconfig` `paths`, `baseUrl`, `extends` chains and `package.json` `imports` aliases; a relative import links to the file it names |
| gRPC | NestJS `@GrpcMethod` (arguments resolved through enums and imported constants), `getService<...>()` clients and their method calls, imported `new XClient(...)` |
| Message brokers | a rule table gated on the libraries a file loads and on the client it built: Kafka family, Redis (pub/sub, streams, list queues), NATS, MQTT, RabbitMQ, BullMQ / Bull, SQS / SNS, Kinesis, EventBridge, Google Pub/Sub, Pulsar, Azure Service Bus and Event Hubs, STOMP, NestJS microservices |
| HTTP routes | Express, Fastify, Koa, Hono, Elysia, h3, restify, polka, itty-router, hapi (with composed prefixes and in-file mounts); decorated controllers (NestJS, routing-controllers, tsoa, inversify); file-system routers (Next.js, Remix / React Router, SvelteKit, Nuxt / Nitro, Astro) |
| Typed APIs | tRPC procedures, GraphQL operations (`@nestjs/graphql`, `type-graphql`, resolver maps) |
| Name resolution | string literals, templates, `+` concatenation, module `const`s, `as const` / `Object.freeze` objects, string enums, class members, function-local constants, scope-aware (a parameter or `let` hides a module constant) |

> **If you run NestJS, read this.** Not resolved: the application's global prefix
> (`setGlobalPrefix`) and `@Version` (set `http_prefix` for a constant prefix), inherited controller
> bases, and a topic or queue name that comes from a config *object* or a registry. A name read
> with `process.env.X`, `configService.get('X')` or `config.getOrThrow('X')` is resolved against the
> workspace's configuration files (section 3.6); a name no file defines stays a visible `${X}` topic.
> Expect missing routes and "not statically resolved" on topics wherever production code is
> configuration-driven; the label "level 4" describes the extractor, not the recall on your code.

Not recognised at all (non-exhaustive; absence from the table above means invisible): Socket.IO /
NestJS WebSocket gateways, Temporal, server-sent events, ORMs (Prisma, TypeORM). EventEmitter2 /
`@OnEvent`, NestJS CQRS buses and sagas and `@Cron` schedulers are in-process buses since 9.0
([3.5](#35-in-process-buses-and-schedules)). The BullMQ decorators `@Processor` /
`@InjectQueue`, `@nestjs/bull(mq)`, `@RabbitSubscribe` / `@RabbitRPC` and `@SqsMessageHandler` are
recognised.

Where it stops (all listed in `CHANGELOG.md` 7.1.0 to 7.1.10):
- A name taken from a parameter, a config object or a runtime registry is not a name and records
  nothing. Declare the registry with a custom pattern. (`process.env.X` and `configService.get('X')`
  record a `${X}` placeholder, section 3.6.)
- NestJS global prefixes and `@Version`, Express/Koa mounts across files, a Fastify plugin passed
  as a named function, inherited controller bases, and HTTP client calls are not linked.
- Template-only component usage (auto-imported Vue/Nuxt components with no `import`) is not seen.
- A Kafka topic, a Redis stream, a RabbitMQ queue, an SNS topic and so on that share a name are
  separate channels since 9.0 (broker-qualified keys, [architecture 3.4](architecture.md#34-broker-qualified-topic-keys)):
  each site carries the broker its client library speaks to. A NestJS `@EventPattern`,
  `@MessagePattern` or `client.emit` has no statically known broker (the transport is configured
  elsewhere): it joins every channel of its name, `heuristic`, while two or more brokers use the name.
  Redis pub/sub, streams and lists are one broker (`redis`), as are SNS topics and SQS queues two
  (`sns`, `sqs`): they are not linked, although AWS can subscribe a queue to a topic.
- `export =`, UMD/webpack wrappers, namespace-qualified constants (`ns.Enum.MEMBER`) are not
  resolved.
- TanStack Router, Expo Router, Qwik City and SolidStart conventions are not covered.

Why this is the deepest: JavaScript and TypeScript are the most frequent languages in the
workspaces that drove the work, and they have no single dominant framework, so the extractor is a
rule table (`languages/js_frameworks/`) plus per-file scope facts rather than a handful of
hard-coded cases. That also makes it the most heuristic-heavy part of the codebase: every rule is
a convention, and a convention the table lacks is invisible.

### 2.2 Java (level 3)

| Capability | What is recognised |
| :--- | :--- |
| Declarations | classes, interfaces, methods |
| Imports | `import` statements, for `find_dependents` |
| gRPC | `*ImplBase` servers, `@GrpcService`, `newBlockingStub` / `newStub` / `newFutureStub` clients |
| Events | `@KafkaListener` (consumers), `KafkaTemplate` (producers); a topic written `"${a.b}"` / `"${a.b:default}"`, or a field or parameter injected with `@Value("${a.b}")` and passed to `send`, is resolved against `application*.yml` / `.properties` and the other configuration files of section 3.6 |
| Other brokers | RabbitMQ, JMS, SQS / SNS, Redis, NATS and Google Pub/Sub, each under its own broker key: [3.7](#37-jvm-brokers-in-java) |
| In-process events | `@EventListener`, `@TransactionalEventListener`, `ApplicationListener<E>`, `publishEvent(new E(...))`, `@Scheduled`: [3.5](#35-in-process-buses-and-schedules) |
| HTTP | Spring MVC mappings, JAX-RS annotations |

**You will not get:** Spring Cloud Stream, Kafka Streams, ActiveMQ / Artemis native clients, Kinesis,
EventBridge, Pulsar or Service Bus; Feign / RestTemplate / WebClient links to routes; WebFlux
functional routes; a Kafka topic given as a constant or enum (the other brokers of 3.7 do read a
constant of the same file).

Stops: the Kafka extractor has no constant propagation (a topic built in code is not resolved; use
Spring properties), and it reads only the Kafka, HTTP, in-process and 3.7 idioms named above. `@Value` is read by
field or parameter name inside the declaring class (nested type declarations are not part of it;
a parameter or local of the same name hides the field, `this.name` is the field); `@ConfigurationProperties` classes, SpEL
(`#{...}`) and Spring's relaxed binding (`APP_TOPICS_ORDERS` for `app.topics.orders`) are not.

### 2.3 Go (level 3)

| Capability | What is recognised |
| :--- | :--- |
| Declarations | structs/types as classes, interfaces, funcs and methods |
| Imports | import paths |
| gRPC | `Register*Server` servers, `New*Client` clients (receivers `redis`, `http`, `mongo`, `s3`, `sqs`, `vault`, `elastic`, `sql`, `db`, `kafka`, `sarama` are excluded) |
| Events | Kafka via `sarama`, `kafka-go`, `confluent-kafka-go`; string constants in the file are resolved. RabbitMQ, NATS, Redis, SQS / SNS and Pub/Sub, plus sarama consumer groups and franz-go, by receiver type: [3.9](#39-go-brokers) |
| HTTP | any `.Get/.Post/...("/literal", handler)` call with two or more arguments, whatever the receiver (chi, gin, echo shapes); a non-router call of that shape also matches |

Stops: the brokers of 3.9 are the only ones read besides Kafka; only the router-style route calls
above are read; constants are resolved within the file, not across packages.

### 2.4 Python (level 3)

| Capability | What is recognised |
| :--- | :--- |
| Declarations | classes, functions (docstrings kept when bodies are stripped) |
| Imports | `import` / `from ... import` |
| gRPC | `*Servicer` servers, `*_pb2_grpc.*Stub(...)` clients |
| Events | `confluent_kafka`, `kafka-python`, `aiokafka` producers and consumers; Celery tasks as queues, only on a Celery origin (below); module constants resolved; RabbitMQ (pika, aio-pika, kombu), Redis, NATS, SQS / SNS, Google Pub/Sub ([3.10](#310-python-brokers)); an environment variable read is a `${KEY}` placeholder (3.6) |
| HTTP | routes on decorators of `app`, `router` or `api_router` (Flask, FastAPI); a Blueprint (`@bp.route`) is not read |

**Celery is read through the file's imports and names (9.0 T15).** A decorator is a Celery task
consumer, and a `.delay(...)` / `.apply_async(...)` a producer, only when the parsed `import`
statements and the module-level names (never a comment or a string) say so. Before 9.0 any decorator
that contained `@task`, `@celery` or `.task` was a consumer, so every Locust `@task` was a "Celery
consumer" (21 of 21 event rows on otel-demo and bank-of-anthos were such rows); the same reader never
saw `@shared_task` or an alias such as `@st`, which it now does. Words are matched on tokens, never
substrings: a module path or name "holds `celery`" when a dotted segment or a `_`-token of it is
`celery` (`proj.celery`, `my_celery`, `celery_app`; not `mycelery_utils`, `celeryman`), and "has a
`tasks` segment" only for a whole dotted segment (`myapp.tasks`; not `subtasks`, `taskstore`,
`shop_tasks`). Packages `invoke`, `fabric`, `locust`, `prefect`, `huey`, `dramatiq`, `arq`, `rq`,
`luigi`, `multiprocessing` and `gevent` never count as Celery by the `tasks` or the producer rule.

- Consumer, bare form (`@shared_task`, `@task(...)`, any alias): the name is imported from a module
  holding `celery` as `task` or `shared_task`, and the file does not define or assign it at module
  level (a nested assignment or a method named `task` does not hide it). A name bound to two different
  imports is ambiguous and reads nothing. `from locust import task` is not Celery.
- Consumer, attribute form (`@x.task`, `@x.shared_task`, called or not, `x` may be a chain): `x` holds
  the token `celery` (`@celery.task`), or is assigned at module level only from calls to `Celery`,
  `make_celery`, `create_celery` or any callee holding the token or imported from a celery module (the
  Flask factory `celery = make_celery(app)` works), or is imported from a module holding `celery`, under
  a name holding `celery`, or from a module with a `tasks` segment (`from proj.celery import app`,
  `from proj import celery_app`, `from myapp.tasks import app`). Importing `celery` somewhere else in
  the file is not enough: `import celery` next to `@locust.task` reads nothing.
- Producer (`x.delay(...)`, `x.apply_async(...)`, also behind `x.s(...)`, `.si(...)`, `.signature(...)`,
  keyed by `x`): `x` is a consumer `def` of the file, or a name bound by a `from ... import` of any
  module that is not in the list above, or an attribute chain rooted at an imported name
  (`shop_tasks.charge_card.s(2).delay()` is read, keyed `charge_card`; base read it as a producer named
  `s(2)`). A parameter, a local variable, `self.x`, a name assigned in the file and a `def` that is not a
  task are not read.
- A decorator the gate refuses is no longer a queue node either, and an `@app.task` it refuses is not
  turned into an HTTP route.

False negatives this introduces (a real site that is no longer read): `from proj import app` then
`@app.task` where the module path holds neither word (this includes the Flask `make_celery(app)` object
imported from `app`); a task assigned from a call in the file under a name the rule does not know;
`from celery import *` (binds no name: `@shared_task` is not read); `@(app.task)`; a `.delay()` on a
task held in a variable, a parameter or `self`. Not read before either: `send_task("name")`.
False positives that remain: a `.delay()` / `.apply_async()` on a name imported from another library
(`from rx import timer; timer.delay(1)`), because these method names are taken as Celery's.
Tests: `crates/mesh-parsers/tests/py_celery_gate.rs`; golden `layered-py` is the positive corpus.

Stops: only the frameworks and libraries named above are read; a topic name that comes from a
settings object or an environment variable is not recorded.

### 2.5 Rust (level 3)

| Capability | What is recognised |
| :--- | :--- |
| Declarations | structs/enums/traits, functions |
| Imports | `use`, `extern crate` |
| gRPC | tonic **servers** (`#[tonic::async_trait]` `impl`). Client stubs are not recognised |
| Events | `rdkafka` (`.subscribe(&["topic"])`, `FutureRecord::to("topic")`) |
| HTTP | `axum` `Router::route("/path", get(handler))`, actix-web attribute macros |

Stops: only the idioms named above are read (no gRPC clients, no other brokers or web
frameworks).

### 2.6 C++ (level 3, narrow)

Declarations (classes, structs, free functions; a class whose methods are all pure virtual is an
interface), quoted `#include "x.h"` as dependencies (angle-bracket system headers are ignored,
`[engines.contracts.cpp] include_paths` resolves search paths), and gRPC servers (classes deriving
`::grpc::Service`, including out-of-line `Foo::Method` definitions). No client stubs, no events,
no import resolution through build systems (CMake, Bazel).

### 2.7 Kotlin and C# (level 2)

| | Kotlin | C# |
| :--- | :--- | :--- |
| Declarations | classes, objects, interfaces, functions | classes, interfaces, methods |
| Imports | not extracted | not extracted |
| Events | Spring `@KafkaListener`; `kafka-clients` `.subscribe` / `.send` / `.produce`; string defaults resolved | Confluent.Kafka producers and consumers, **string literal topics only** (a variable argument records nothing rather than the variable's name) |
| HTTP | Spring annotations | ASP.NET attributes (`[ApiController]`, `[HttpGet("/x")]`) |
| gRPC | `@GrpcService` classes are tagged; no clients | none |

**You will not get** (Kotlin): imports, gRPC clients. **(C#)**: a `using` graph, gRPC, minimal APIs
(`MapGet`), MassTransit, Azure Service Bus, RabbitMQ, DI-registered handlers, and the `.csproj` /
NuGet / solution graph.

Consequence: `find_dependents` cannot answer "who imports this" for Kotlin or C# files, and
gRPC tracing (`analyze_grpc`) will not list a C# service or client. The `.proto` is still the
source of truth for the contract, and a Java or Go caller of a C# service is still found. (In the
`online-boutique` golden file, `CartService` is a C# server, but its scored edges are
`caller -> CartService`, written from client construction sites in the callers; no C# gRPC extraction
is involved in them.)

### 2.8 Ruby, PHP, Swift, Scala (level 1)

Declarations only, plus one routing idiom each:

| | Declarations | Route idiom |
| :--- | :--- | :--- |
| Ruby | classes, modules (as interfaces) | Rails controller actions and `config/routes.rb` DSL calls |
| PHP | classes, interfaces | Symfony `#[Route(...)]`, Laravel `Route::get(...)` |
| Swift | classes/structs/extensions, protocols | Vapor `app.get("/path") { ... }` |
| Scala | classes/objects, traits | Akka HTTP `path("segment") { get { ... } }` |

No imports, no gRPC, no events. These languages are useful to `smart_search` and, for the route
idioms, to route lookup; they are not part of the cross-service graph beyond that.

---

## 3. Contract and document formats

### 3.1 Protobuf

Grammar: `tree-sitter-proto`. Services, methods, messages and `import` dependencies. It is the
source of truth for `analyze_grpc`, which also compares a `.proto` against a Git base for
wire-format breaking changes: field number reuse, incompatible type changes, deleted fields
without `reserved`, enum number reuse and deletion, with a documented compatibility table
([mcp-tools](mcp-tools.md#wire-format-check)). Files under 1.5 MB are accepted (the schema size
limit is higher than for source files). `[engines.contracts.grpc] proto_dirs` scopes which
`.proto` files count.

### 3.2 OpenAPI, AsyncAPI, Spring properties

Read without tree-sitter: YAML is streamed event by event with budgets (an alias bomb ingests
nothing). OpenAPI contributes paths, AsyncAPI channels (topics), Spring `application*.yml` and
`.properties` contribute properties used to resolve topic placeholders; keys that look like secrets
are masked. `[engines.contracts.openapi|asyncapi|spring]` scope which files count.

### 3.3 Markdown

Split into sections for `search_docs`, with a per-file memory budget, YAML/TOML frontmatter kept out
of the body, and prompt-injection phrases filtered. See [mcp-tools](mcp-tools.md#tool-5-search_docs).

### 3.4 SQL

No tree-sitter grammar: a bounded lexer (one pass, no regex, no recursion) over `.sql` files, and
over the string literals of TypeScript / JavaScript, Go, Java and Python files. Declarations
(tables, functions) are not indexed; only events that start or end in SQL are:

| Statement | Registers | Key |
| :--- | :--- | :--- |
| `INSERT INTO <outbox> (<cols>) VALUES (...)` / `... SELECT ...` | producer | the single-quoted literal in the first column named `topic`, `topic_name`, `channel`, `event_type`, `event_name` or `routing_key` |
| `NOTIFY ch` and `pg_notify('ch', ...)` | producer | `ch` |
| `LISTEN ch` | consumer | `ch` |

- An outbox table is one whose last name part (schema prefix and quoted identifiers are read,
  case-insensitive) contains `outbox`, or is `event_queue` / `event_queues`. Every row of a
  multi-row `VALUES` registers its own key.
- A trigger needs no special rule: its function body is scanned, so the `INSERT` or `pg_notify`
  inside it is the producer, on the line of its literal. A dollar-quoted body (`$$` or `$tag$`) is
  read as code only after `AS` (a function body) or in a `DO` block; any other `$$..$$` text
  (`COMMENT ON ... IS $$NOTIFY x$$`, `SELECT $$..$$`) is a string literal and registers nothing. The
  trigger's own table is not read.
- A key that is not a static literal (a bind parameter such as `$1` or `?`, `TG_TABLE_NAME || '.x'`,
  a column, a missing column list, `pg_notify` on a variable) is not resolved: no producer is
  registered and no key is made up. Each such site is COUNTED: `IndexHealth::sql_sites_unresolved`,
  listed by `mesh-mcp doctor`. This is the usual shape of application code
  (`INSERT INTO outbox (topic, payload) VALUES ($1, $2)` with the topic as an argument), which
  therefore shows as unresolved, not as a producer. An unquoted channel is folded to lower case, as
  Postgres does; a quoted one is kept.
- Comments are skipped, and a host-language string literal is read as SQL only when it BEGINS with
  `INSERT`, `NOTIFY`, `LISTEN`, `SELECT`, `PERFORM` or `BEGIN`: prose that mentions `INSERT INTO
  outbox` is not a producer. In such a literal `NOTIFY` and `LISTEN` must be written in upper case
  and the channel must start with a letter or `_` (`log.info("Notify admins")` and
  `log.info("Listen 3000")` are log lines); a lower-case `listen orders` in code is therefore not
  read, while `INSERT` and `pg_notify` stay case-insensitive. A `"NOTIFY stock"` string that is only
  documentation cannot be told from a statement and is read. Host escapes (`\'`, `\n`) are decoded
  before the scan. Adjacent literals (`"INSERT ..." + "VALUES ..."`) are not joined.
- Sites go through the ordinary producer / consumer path (an `EventStream` node on the line of the
  key), so `analyze_impact`, `trace_flow` and the topic matching see them like any broker call; a
  SQL producer inside a service method is reached by the calls layer of its language.
- Not covered: stored procedures called by name, `COPY`, CDC / Debezium, MySQL or SQL Server
  queues, other host languages (Kotlin, C#, Ruby, PHP, Rust), and a topic taken from a bind
  argument at the call site.
- Known limits: a site is not deduplicated across files, so 300 migrations that each re-create the
  same trigger function give 600 site nodes (nothing in `doctor` signals it: no cap is hit and each
  site is a resolved literal); a file under `test/` or `tests/` gives nodes like any other file; a
  regex literal holding a quote can desynchronise the host lexer for the rest of its line.
- Bounds: a `.sql` file has its own guard, the 384 KB budget and the null-byte sniff (a lexer is
  linear and capped, so the line-length and nesting rules of the tree-sitter guard do not apply: a
  long comment line or a `-- (` banner does not drop the file). Caps: 200,000 tokens, 4,096 sites per
  file, 512 rows per `VALUES`, 4,096 scanned literals per host file. Every cap is counted:
  `IndexHealth::files_sql_capped`, listed by `mesh-mcp doctor` (a warning); the sites found before
  the cut are kept.

Fixture and ground truth: `examples/eda/sql-outbox` and `tests/golden/events/sql-outbox.expected.yaml`.

### 3.5 In-process buses and schedules

9.0 (ticket T4) makes the application's own buses and schedulers visible to the event graph, so
`trace_flow` chains through them like it does through a broker. They are recorded under the broker
`inproc`: an in-process event named like a Kafka topic is a different channel, never linked to it
(`inproc:orders` and `kafka:orders` when both exist, [architecture 3.4](architecture.md#34-broker-qualified-topic-keys)).

| Idiom | Role | Key |
| :--- | :--- | :--- |
| NestJS `@OnEvent('a.b')`, `@OnEvent(['a', 'b'])` | consumer | the event name |
| EventEmitter2 `emitter.emit('a.b')`, `emitAsync`, `.on` / `.once` | producer / consumer | the event name (`Event.name` reads as `Event`) |
| NestJS CQRS `@CommandHandler(C)`, `@EventsHandler(A, B)`, `@QueryHandler(Q)` | consumer | the class name as written (`C`, `A`, `B`, `Q`) |
| `commandBus.execute(new C())`, `queryBus.execute(new Q())`, `eventBus.publish(new E())`, `publishAll([new E()])` | producer | the class name |
| `@Saga()` member: `ofType(E)` and each `new C()` it builds | consumer / producer | `E` / `C`, on the line of each |
| Spring `@EventListener`, `@TransactionalEventListener`, `onApplicationEvent` of an `ApplicationListener<E>` | consumer | the class of the first parameter, or of `classes = {A.class}`; `PayloadApplicationEvent<T>` reads as `T` |
| Spring `publishEvent(new E(...))` in a file naming `ApplicationEventPublisher` or `ApplicationContext` | producer | `E`, on the line of the call |
| NestJS `@Cron`, `@Interval`, `@Timeout` (`@nestjs/schedule`), Spring `@Scheduled` | chain entry (kind `cron`) | a `Schedule` node named `Class.method`, signature the decorator text |

- Gating. A NestJS call needs the receiver typed with a class of the library
  (`private readonly emitter: EventEmitter2`, `commandBus: CommandBus`), or built from one; a
  socket's `emit`, a Node `events.EventEmitter` and an untyped parameter are not the bus. A name
  that is not statically knowable (a parameter, a call result, a variable holding the message) is
  not recorded and nothing is guessed. A class-level CQRS handler is anchored on its `execute` /
  `handle` method, so the chain continues into the calls it makes.
- Schedules are entries, like an HTTP route: `trace_flow` with `entry_kind = cron` (or `auto`, which
  reads a scheduled `Class.method` as a cron entry) starts at the `Schedule` node named
  `Class.method` (a name no schedule carries still resolves as a handler). A reverse trace from a
  publish ends at the schedule when the scheduled method calls a method that publishes; a publish
  made inside the scheduled method itself is not reached (the same limit as a Kafka handler that
  publishes in its own body, a residual of the chain engine).
- In-process and broker sites never meet. Besides the separate `inproc:` channels, the legacy
  same-file fallback of `trace_flow` (offered when no call path reaches a publish) does not link a
  site known to be in-process with a site known to talk to another broker; a site whose broker is
  unknown (a NestJS `client.emit`, a custom pattern) keeps the old behaviour.
- A class-name key is the name as written, with an import alias resolved to what the module
  exports (`import { OrderCreated as Created }` is `OrderCreated`). It is not qualified by module:
  two unrelated classes of the same name in two packages are one key and are linked.
- Not covered: Node `events.EventEmitter`, wildcard event names (`order.*`), `AggregateRoot.apply`,
  Spring `@Async` ordering and `ApplicationEvent` subclasses published through a variable,
  Spring Modulith `@DomainEvents`, `SchedulerRegistry` / `cron.schedule(...)` calls, Quartz, a
  schedule read from configuration (`@Scheduled(cron = "${...}")` is kept as written in the
  signature, the method is still an entry), Kotlin and Scala Spring code. Also: a class whose name
  is all capitals (`new OC()`) is not read as a class; a `@Cron` on an abstract member and a
  `@Scheduled` on an interface method are entries of a body-less method (nothing follows them);
  `diff_mesh` does not report a `Schedule` entry (a added or removed schedule is not a contract
  change it lists); homonymous classes across packages are linked (see above).
- Bounds: at most 2,000 in-process sites and schedules per file (`MAX_INPROC_SITES_PER_FILE`);
  the sites past it are not registered, and the file is counted in
  `IndexHealth::files_inproc_capped`, listed by `mesh-mcp doctor` (a warning).

Fixtures and ground truth: `examples/eda/nestjs-monolith-bus` and `examples/eda/spring-monolith`
with `tests/golden/events/nestjs-monolith-bus.expected.yaml` and `spring-monolith.expected.yaml`.

---

### 3.6 Config-driven topics

A producer or consumer whose topic is read from configuration is recorded with a Spring-style
placeholder, `${KEY}` or `${KEY:default}`, and resolved when the file is folded into the graph, so a
config edit re-resolves it without the source file changing:

| Source read | Placeholder |
| :--- | :--- |
| TypeScript / JavaScript `process.env.X`, `process.env['X']`, `<config>.get('X')`, `<config>.getOrThrow('X')` (receiver named `*config*`), with `?? 'd'` / `\|\| 'd'`, through local and module `const`s | `${X}` / `${X:d}` |
| Go `os.Getenv("X")`, `viper.GetString("a.b")` / `viper.Get(...)`, directly or through `v := os.Getenv("X")` | `${X}` |
| Python `os.environ["X"]`, `os.environ.get("X"[, "d"])`, `os.getenv("X"[, "d"])`, directly or through a module-level assignment | `${X}` / `${X:d}` |
| Java `"${a.b}"` / `"${a.b:d}"` in `@KafkaListener`, or a field / parameter annotated `@Value("${a.b}")` passed to `kafkaTemplate.send` | as written |

Values come from: Spring `application*.yml` / `*.properties` (dotted keys; honours
`[engines.contracts.spring] property_files`), the four dotenv templates `.env.example`,
`.env.sample`, `.env.template` and `.env.dist` (exact names; no other dotenv file is read), a compose
`environment:` block (map or list form) and a Kubernetes `env:` list (`name` / `value`). Resolution:

- one value for a key: the link is `exact`; a value that only a template (`.env.example`), a
  multi-document yaml (only its first document is read) or the source's own `:default` supplies, or
  two files that disagree (profiles, `.env` against an overlay), makes the link `heuristic` and
  registers every candidate. A deployed value always wins over a template value.
- a placeholder nothing resolves (no file defines it, a cycle, nesting beyond 8 hops, more than 8
  candidates or more than 256 expansion steps, checked while expanding, so a wide chain of profiles
  is abandoned early) stays a visible `${KEY}` topic, `heuristic`, counted in
  `IndexHealth::config_topics_unresolved`. Producers and consumers of the same unresolved key meet on
  it. No name is guessed; the env key is never recorded as a Go or Python topic.
- secrets (commandment 5): a real dotenv file (`.env`, `.env.local`, `.env.production`,
  `production.env`, `.env.distributor`, `.env.example.local`) is never read, whatever
  `exclude_patterns` says; the default `**/.env*` pattern lets exactly the four template names
  through, and only when `exclude_patterns` is absent from the config: an operator who lists
  `**/.env*` (or any `exclude_patterns`) themselves keeps every `.env*` file excluded, templates
  included (the loader rewrites the exact string to `.env*`, the same files). A template's values are weak. An entry is kept only if its value is
  topic-shaped (ASCII letters, digits, `. _ - : / $ { } # * +`, at most 249 bytes, no `://`, no `@`,
  no token-looking run such as `sk_live_`, `ghp_`, `AKIA`, a 32-char hex run or a long base64-ish
  run) and its key is not a credential name (ending `_KEY`, `_DSN`, `_URL`, `_URI`, `_PASSWORD`,
  `_PASSWD`, `_PWD`, `_CREDENTIALS`, `_AUTH`, or containing `dsn`, `stripe`, `secret`, `token`,
  `password`, `credential`, `apikey`, ...). A placeholder DEFAULT is held to the same rule (`${k:postgres://u:pw@h/x}`,
  `${k:sk_live_...}`, `${a:${b:x}}` lose their default at extraction, in the graph, in the parse
  cache and in `smart_search` snippets). A topic site whose key is dropped stays a visible
  `${KEY}`. By design, a topic-shaped value under a NON-secret key name is kept (a name is the
  only signal available), and a legitimate topic of 24 or more alphanumeric characters with a digit
  and no separator (`orders2024eventsV2Stream`) is taken for a token and dropped, known limit.
- bounds, all counted (`IndexHealth::files_config_capped`, `config_entries_dropped`, `doctor`): 2,048
  entries per file, 50,000 per workspace, values up to 249 bytes; config files go through the same
  384 KB / binary guards as every file; symlinks are not followed outside the root. Also counted
  (`files_config_parse_failed`, `files_config_lossy`): a yaml that does not parse (a profile file next
  to it is read as weak, since the base may hold the active value) and a non-UTF-8 config file (read
  with the invalid bytes replaced, only the lines that held them are lost). `doctor` warns on any of
  them, and says when an unresolved placeholder may be due to a skipped (oversized) config file.

Not resolved: Spring relaxed binding, `@ConfigurationProperties`, Helm / Kustomize templating,
`env_file:` indirection, `docker-compose` `${VAR}` interpolation, a config object
(`settings.KAFKA_TOPIC`, `config.kafka.topic`), NestJS `registerAs` factories, and any config file
format other than the above (JSON, TOML, `.ini`).

### 3.7 JVM brokers in Java

Java reads the brokers below besides Kafka (9.0 T8a, tag `jvm_broker`). Each site is recorded under its
own broker, so a queue named like a Kafka topic is another channel (`rabbitmq:orders`,
`sqs:orders`, `jms:orders`; [architecture 3.4](architecture.md#34-broker-qualified-topic-keys)).
`jms` is a broker key added by this ticket: the JMS API hides the provider (ActiveMQ, Artemis,
IBM MQ), which no call site shows.

| Broker | Consumers | Producers | Key of a site |
| :--- | :--- | :--- | :--- |
| `rabbitmq` | `@RabbitListener(queues = ...)`, `queuesToDeclare = @Queue(...)`, `bindings = @QueueBinding(...)` (on a method; at class level, with `@RabbitHandler` methods, the class is the consumer) | `RabbitTemplate` / `AmqpTemplate` / `RabbitMessagingTemplate` `convertAndSend`, `send`, `convertSendAndReceive` | the queue; for a binding its routing `key` (else its queue); for a producer the routing key of `(exchange, key, msg)`, or the queue of `(queue, msg)` |
| `jms` | `@JmsListener(destination = ...)`, `JmsTemplate.receive` / `receiveAndConvert` | `JmsTemplate` / `JmsMessagingTemplate` `convertAndSend`, `send`, `sendAndReceive` | the destination name, or the literal of `new ActiveMQQueue("x")` / `new ActiveMQTopic("x")` |
| `sqs` | `@SqsListener("q")` / `value` / `queueNames`, `SqsClient.receiveMessage` | `SqsTemplate.send` / `sendAsync`, `QueueMessagingTemplate`, `SqsClient.sendMessage` | the queue name, or the last segment of a queue URL |
| `sns` | none (SNS delivers to a queue: not followed) | `SnsTemplate` / `NotificationMessagingTemplate`, `SnsClient.publish` | the topic name, or the last segment of a topic ARN |
| `redis` | `RedisMessageListenerContainer.addMessageListener(l, topic)` with a `ChannelTopic` / `PatternTopic` | `RedisTemplate` / `StringRedisTemplate.convertAndSend` | the channel or pattern |
| `nats` | `Connection` / `Dispatcher` / `JetStream` `.subscribe`, `createDispatcher(h).subscribe` | `.publish`, `.request`, `publish(NatsMessage.builder().subject("x")...)` | the subject (a `*` / `>` wildcard as written) |
| `pubsub` | none | `PubSubTemplate.publish("topic", ...)` | the topic |

What a name may be (nothing else is read, no name is guessed from an identifier):

- a string literal, a `final` string field of the same file (`static final String Q = "q"`,
  `Q`, `this.Q`, `Queues.Q` of a class declared in the file), a destination object of the file
  (`new ChannelTopic("x")`, `ChannelTopic.of("x")`, a field or a no-argument method returning one,
  then `.getTopic()`; the class must be imported from the library), or an `@Value("${a.b}")` field
  or parameter. A placeholder is resolved against the configuration like a `@KafkaListener` topic
  (section 3.6), with one difference: **its default is never taken as a name**. `${a.b:d}` is read as
  `${a.b}`: the configuration resolves the key, or the site stays a visible `${a.b}` topic
  (`heuristic`, counted in `config_topics_unresolved`). A default that merely looks like a topic
  (`hunter2-prod`) may be a secret. The one-line signature of the node that holds the site has its
  defaults removed too. (The `@KafkaListener` path of 3.6 keeps taking a plain default as a name; the
  change would alter documented T6 behaviour and is a follow-up.)
- a call is a site only when its receiver is a variable declared, in scope, with the client class
  (`T x`, `var x = new T()`, `this.x` for the field), in a file that really imports it. The
  imports are the `import` declarations of the parse tree (a commented-out or quoted import counts for
  nothing): the class is imported by name from one of the library's packages, or by a wildcard of
  one (`org.springframework.amqp.*`, `io.nats.client.*`), or written fully qualified
  (`springframework.amqp`, `springframework.jms` / `jakarta.jms` / `javax.jms` /
  `org.apache.activemq`, `awspring.cloud` / `software.amazon.awssdk` / `com.amazonaws`,
  `springframework.data.redis`, `io.nats.client`, `com.google.cloud.spring.pubsub`; a wildcard is
  trusted for any type name of the table). The nearest declaration wins: a parameter, a lambda
  parameter, a local declared earlier in an enclosing block, a for / for-each / `try` / `catch`
  variable, then the fields of the enclosing classes. A class of the file with the client's simple name
  (a nested `RabbitTemplate`), or an import of that simple name from another package, makes it not the
  library's. Listener annotations (`@RabbitListener`, `@JmsListener`, `@SqsListener`) follow the same
  rule. A `convertAndSend` of any other class is not a site;
- an AWS SDK request names its channel when the `queueUrl` / `topicArn` literal is inside the call or
  inside the initializer of a local variable the call passes; a request built by another method is not
  read;
- a producer never publishes to a pattern: a key with `*`, `#` or `>` registers no producer (a
  consumer may subscribe to one);
- RabbitMQ sends: `(exchange, routingKey, message[, ...])` is keyed on the routing key,
  `(routingKey, message[, postProcessor])` on its first argument. A three-argument call is told apart
  by its last argument: a lambda, a method reference, a `new ...PostProcessor(...)` or a variable
  declared with a type named `...PostProcessor` / `...CorrelationData` is the post-processor; a
  variable declared with another type, a literal, a `new` or a call is the message; a variable the
  file does not declare (an inherited field) leaves the call without a name.

Producers and call-site consumers (NATS, Redis, `receive`, `receiveMessage`) are nodes on the call line;
listener annotations sit on their method.

Bounds: at most 2,000 sites per file (`MAX_JVM_BROKER_SITES_PER_FILE`) and 16,384 declared names; a
file over a bound is counted in `IndexHealth::files_jvm_brokers_capped` and listed by `mesh-mcp doctor`
(a warning). The walk is iterative, a search of one argument or initializer visits at most 256 nodes
and a `getTopic()` chain is followed 4 links, so a hostile file costs a bounded pass whatever it nests.

**You will not get:** Kotlin (its extractor is a separate, text-level grammar: not extended);
exchange-to-queue wiring (a `convertAndSend(exchange, key, msg)` is keyed on the routing key, so a
queue bound under another key, a fanout exchange, a topic-exchange `#` pattern or a queue declared as a
`@Bean` never links); `@RabbitListener(queues = "#{...}")` SpEL; a constant, enum or `application.yml` value
reached through another file's class; string concatenation; Redis streams (`opsForStream`) and
`opsForList` queues; `redisTemplate.convertAndSend` with a `ChannelTopic` held only in another file;
Kinesis, EventBridge and `SnsClient.publish` with a `PublishRequest` built by a helper method; Google
Pub/Sub subscriptions (a subscription is not a topic name) and `Publisher.newBuilder(TopicName.of(...))`;
SQS FIFO deduplication or message groups; JMS `Destination` beans and `JndiTemplate` lookups.
A three-argument `convertAndSend(queue, "literal payload", processor)` whose `processor` is
declared with a type not named `...PostProcessor` reads its payload as the routing key.

Chains: the same-file fallback of `analyze_impact` / `trace_flow` (a handler that re-emits, with no call
path to a publish) never links two sites that are each known to talk to a broker when they share none:
a RabbitMQ listener does not "re-emit" to a Kafka or JMS producer of its file. A site whose broker is
unknown links as before, `heuristic`. `diff_mesh` names the broker of a removed site that is not Kafka
(`produces topic \`orders\` removed (on jms)`) and looks up its other side on that broker.

Golden: `tests/golden/events/jvm-brokers.expected.yaml` (31 sites, 3 hops, 2 chains, shadowing and
pattern decoys).

### 3.8 Queue names given as enum members

A BullMQ / Bull / bee-queue site that names its queue by an imported enum member
(`@Processor(JobQueue.EMAIL)`, `@Processor({ name: JobQueue.EMAIL })`, `@InjectQueue(JobQueue.EMAIL)`,
`new Queue(JobQueue.EMAIL)`, `new Worker(JobQueue.EMAIL, ...)`, the `bull` constructor) is resolved, when
the file is folded into the graph, against the string constants of the workspace: `JobQueue.EMAIL =
'email-jobs'` makes the topic `email-jobs`, so the consumer and the producer meet on the value. A
constant edit re-folds the unchanged files naming it (one extra pass after the reload, never a loop),
and an incremental reload equals a cold rebuild.

Which declaration a site means is decided by its own imports, never by the bare enum name:

- the import binds the enum's local name to a module. A relative path (`./queues`, `../a/queues.js`,
  `./queues/index`) selects the declaring file; a tsconfig / package.json alias is rewritten to the
  relative path it stands for first; an alias (`import { JobQueue as Q }`) and a namespace import
  (`Qs.JobQueue.REPORTS`) bind the same way. A relative path that matches no declaring file (a barrel
  that re-exports it) selects the declarations of the importer's own repository.
- a package specifier (`@org/queues`, `@org/queues/src/q`) binds ONLY to the declarations of the
  package it names, by the `package.json` name the graph already gives every node (npm scope stripped,
  subpath dropped). It never binds to a homonymous enum of the importer's own repository: with no such
  package in the workspace (`import { JobQueue } from 'legacy-queue-lib'`) the site is unresolved, not
  guessed.
- a site with no import for the enum (declared in the same file) binds to that file's own declaration.
- the declared string literals of the candidates are the answer when they all agree (`exact`). Two
  candidates that give the member different values (two files of one package) resolve to NEITHER: the
  topic is the qualified member key (`jobqueue.email`), `heuristic`, and
  `IndexHealth::const_members_conflicting` counts the members a queue site could not choose between
  (`doctor` warns; twenty files each declaring and importing their own enum are no conflict).
- a member no candidate declares (an enum of a package outside the roots, a non-literal value, a
  member named like a credential, a value that is no name) keeps the QUALIFIED member key
  (`externalqueue.billing`), `heuristic`. Never the bare member (`billing`): two enums with a
  `BILLING` member are two queues. Sites of that qualified key still meet each other.
- the constants of test files (`*.spec.ts`, `__tests__/`, `test/`, ...) and of build output
  (`dist/`, `out/`, `build/`, `.next/`, `coverage/`, `target/`) are never recorded.
- what may be a name (commandment 5), applied where the constants are extracted, so a refused value
  never reaches the graph or the parse cache: the member is not named like a credential (a whole word
  `KEY`, `SECRET`, `TOKEN`, `PASSWORD`, `DSN`, ... of its name: `API_KEY`, not `SECRETARY`), and the
  value is a plain name, not a URL or a base64 text, not `Bearer_...`, `Basic_...`, `tok_...`, `sk_...`
  and the other token prefixes, not a known weak password (`hunter2`), not a lone `password` / `secret` /
  `token`, and has no random-looking run (six or more letters and digits with a digit anywhere but a
  trailing run of at most four). `password-reset-jobs`, `token-refresh-jobs`, `private-events`,
  `secretary-jobs`, `orders2024` and `invoice-batch-v2024` are names. Not caught, by design: a
  passphrase made of words (`correct-horse-battery-staple`) and a five-character code.

Only queue sites are resolved by value. An event, a stream, a Kafka topic or any other site naming a
member (`@EventPattern(Streams.USER_CREATED)`) keeps the member key it always had, so the same member of
two enums stays one flow in a codebase that names a stream and its event type alike (Volontariapp:
`Streams.USER_CREATED = 'user:created'`, `UserEventMessagingType.USER_CREATED = 'user.created'`).
A custom pattern's capture of a member (`target:\s*(\w+Queue\.\w+)`) joins a queue by value only when
it binds, through the importing file's own imports, to a declaration some queue site names a queue by.

What is read: string-literal `enum` members and `as const` / plain object keys whose last segment is
ALL-CAPS (`Topics.ORDERS`, not `Topics.orders`). Not read, silently (no site, no counter): camelCase
members, an alias of the enum (`const q = JobQueue; q.EMAIL`), `await import(...)` and CommonJS
`require(...)` bindings, `static` class fields (`Names.EMAIL` keeps its qualified key), template
literals, enum aliases, `FlowProducer` / `@InjectFlowProducer`, `Enum[kind]` and queues built at
runtime, an aliased `bee-queue` constructor. Resolved to the qualified key (the declaration is not
found): a default import, a re-export under another name (`export { JobQueue as JQ }`), an enum
nested in a namespace, a declaration in a `.d.ts`. `BullModule.registerQueue({ name })` only makes a
queue injectable (every worker app registers the queues it consumes): it is not a producer. The
producer of an injected queue is its `@InjectQueue(...)` injection point, that of a `new Queue(...)`
its construction. A chain (`trace_flow`) does not continue through such a site: it is outside any
method, so the calls layer sees no callable that publishes.

`analyze_impact` and `trace_flow` accept the member as written (`JobQueue.EMAIL`), its value
(`email-jobs`) or the qualified key.

Bounds, all counted (`IndexHealth::files_const_capped`, `doctor` warns; the member past a cap keeps its
qualified key): at most 4,000 string constants and 512 enum-like imports per file, 249-byte values, no
recursive walk.

### 3.9 Go brokers

Go reads the brokers below besides Kafka (9.0 T8b, tag `go_broker`). Each site is recorded under its
own broker, so a queue named like a Kafka topic is another channel (`rabbitmq:orders`, `sqs:orders`;
[architecture 3.4](architecture.md#34-broker-qualified-topic-keys)). The broker keys are the existing
ones; the ticket adds none.

| Broker | Library | Consumers | Producers | Key of a site |
| :--- | :--- | :--- | :--- | :--- |
| `rabbitmq` | `rabbitmq/amqp091-go`, `streadway/amqp` | `Channel.Consume`, `ConsumeWithContext`, `Get` | `Channel.Publish`, `PublishWithContext`, `PublishWithDeferredConfirm[WithContext]` | the queue; for a queue the file binds (`QueueBind(q.Name, key, exchange, ...)`) its routing key (the exchange when the key is empty); for a producer the routing key (the exchange when it is empty: a fanout publish) |
| `nats` | `nats-io/nats.go`, `.../jetstream` | `Subscribe`, `QueueSubscribe`, `ChanSubscribe`, `ChanQueueSubscribe`, `SubscribeSync`, `QueueSubscribeSync`, `BindRecvChan` (connection); the same and `PullSubscribe` (`nats.JetStreamContext`) | `Publish`, `PublishMsg`, `Request`, `RequestMsg`, `PublishRequest`, `BindSendChan` (connection); `Publish`, `PublishAsync`, `PublishMsg[Async]` (both JetStream APIs) | the subject (a `*` / `>` wildcard as written, for a consumer) |
| `redis` | `redis/go-redis` v9, `go-redis/redis` v6 to v9 | `Subscribe`, `PSubscribe`, `SSubscribe` on a client or a `*redis.PubSub` | `Publish`, `SPublish` | the channel or pattern |
| `sqs` | `aws-sdk-go-v2/service/sqs`, `aws-sdk-go/service/sqs` | `ReceiveMessage[WithContext]` | `SendMessage[Batch][WithContext]` | the last segment of the `QueueUrl` of the request literal |
| `sns` | `aws-sdk-go-v2/service/sns`, `aws-sdk-go/service/sns` | none | `Publish[Batch][WithContext]` | the last segment of the `TopicArn` of the request literal |
| `pubsub` | `cloud.google.com/go/pubsub` (and `/v2`) | `Receive` of a subscription whose topic the file shows | `Topic("t").Publish`, a `*pubsub.Topic` holding it | the topic |
| `kafka` | `sarama` (`IBM`, `Shopify`), `franz-go/pkg/kgo` | `ConsumerGroup.Consume(ctx, []string{"t"}, h)`, `kgo.ConsumeTopics("t")` | `kgo.Record{Topic: "t"}`, `kgo.DefaultProduceTopic("t")` | the topic |

The Kafka row adds the two variants the Go extractor did not read; `kafka-go`, `sarama` producers and
`confluent-kafka-go` are unchanged.

What makes a call a site (nothing else is read):

- the receiver is a variable, parameter, struct field (`p.ch` of `type P struct { ch *amqp.Channel }`,
  through the receiver) or call result whose type the file shows to be the library's client: a declared
  type (`ch *amqp.Channel`, `var nc *nats.Conn`), a constructor (`nats.Connect`, `redis.NewClient`,
  `sqs.NewFromConfig`, `pubsub.NewClient`, `sarama.NewConsumerGroup`, `jetstream.New`), an accessor
  (`conn.Channel()`, `nc.JetStream()`, `client.Topic("t")`, `client.Subscription("s")`), or a function or
  method of the file with a declared result (`func connect() (*nats.Conn, error)`). The file must hold a
  parsed `import` of the library (a commented-out or quoted import counts for nothing), under the package
  name it gives (`amqp "github.com/rabbitmq/amqp091-go"`, the default `amqp091`, `redis` for any `/vN`);
  `_` and `.` imports are not read. A local type of the same name, a package name hidden by a
  parameter, a local, a range, `if` / `switch` / `select` or closure variable, and an import of another
  library never make a client; the nearest declaration wins, a name declared after the call is not
  seen. `type X = amqp.Channel` aliases are followed;
- a name is a string literal, a `const` or `var` of the same file that nothing assigns again, a `+`
  concatenation of those, `aws.String(x)` of one, the `Name` of a queue the file declared
  (`q, _ := ch.QueueDeclare("hello", ...)` then `q.Name`), the `QueueUrl` / `TopicArn` of a
  `GetQueueUrl` / `CreateQueue` / `CreateTopic` result the file asked for by name, the field of a
  request literal (`&sqs.SendMessageInput{QueueUrl: ...}`, directly or through a local initialized with
  one, `&nats.Msg{Subject: ...}`, `nats.NewMsg("s")`), or a configuration read, `os.Getenv("K")` /
  `viper.GetString("k")`, which is the `${K}` placeholder of 3.6 resolved against the configuration.
  **A default is never a name**: a literal that anything in the file assigns over (the
  `if v := os.Getenv(...); v != "" { topic = v }` idiom) is not read, and a function that returns a
  fallback is not followed; the site is simply not registered. A parameter, a call result, a
  `fmt.Sprintf` and a name of another package register nothing;
- a producer never publishes to a pattern: a key with `*`, `#` or `>` registers no producer; a
  consumer may subscribe to one (a RabbitMQ `#` key is no name in the dot dialect the graph matches and
  is dropped);
- a Pub/Sub subscription is not a topic: `Receive` is a consumer of the topic only when the file creates
  the subscription on it (`client.CreateSubscription(ctx, "id", pubsub.SubscriptionConfig{Topic: t})`
  with `t` a topic of the file); a subscription opened by `client.Subscription("id")` alone registers
  nothing;
- a call this pass owns is not also read by the Kafka heuristics of the Go extractor, which read any
  `Subscribe("x")` / `Produce(...)` of a file: a `Subscribe("orders")` of a NATS or Redis client is not a
  Kafka consumer of `orders`, nor is the `Subscribe` of a method of a type the file declares and that
  holds no broker client (`type Bus struct{}`). A call the 2,000-site cap would cut is not claimed
  either, so the older reading keeps it. A struct of the file that holds a Kafka client
  (`type Consumer struct{ r *kafka.Reader }`) keeps the older reading of its own `Subscribe("x")`: a
  Kafka consumer. In a file that imports one of the new libraries, a Kafka-shaped call (`Subscribe`,
  `Produce`, `WriteMessages`, ...) on a receiver that is not shown to be a Kafka client (an interface
  parameter, a type of another library, a wrapper of a NATS connection) is still read, but with no
  known broker, not as Kafka: it joins the channel of its name on every broker (`heuristic`), like a
  SQL site. Outside such files nothing changed;

Every site is a node on its call line, like the Java ones; its broker travels with the producer or
consumer, so the same-file fallback of `trace_flow` / `analyze_impact` never links two sites that know
different brokers (3.7). `diff_mesh` names the broker of a removed site that is not Kafka.

Bounds: at most 2,000 sites per file (`MAX_GO_BROKER_SITES_PER_FILE`) and 16,384 declared names; a file
over a bound is counted in `IndexHealth::files_go_brokers_capped` and listed by `mesh-mcp doctor` (a
warning). Every walk is iterative, a type or name resolution follows at most 8 links (a constant chain: a plain alias `A1 = A0` costs one link per hop and resolves through 7 hops; a concatenation `X1 = X0 + "x"` costs two per hop and resolves through 3; a longer chain registers no site), and the scope
tables are built once per scope, so a hostile file costs a bounded pass whatever it nests.

**You will not get:** `redigo` (`conn.Do("PUBLISH", ...)`), `rueidis`, `nsq`, `asynq`, `machinery`, MQTT,
Kinesis, EventBridge, Azure Service Bus; the new `jetstream` consumers (`stream.Consumer(...)`,
`Consume`, which name a stream and a durable, not a subject); embedded fields (`type P struct{
*amqp.Channel }`, `p.Publish(...)`); a receiver typed only by an interface of the file or passed through
`any`; a client built by a function of another file or package; a name from another file or package
(a `const` of a sibling file, `config.Orders`), a `switch` or a map lookup, a name built with
`fmt.Sprintf`; a request struct built by a helper function or by field assignment
(`in.QueueUrl = ...`); `PublishMsg` with a message built by a helper; RabbitMQ exchange-to-queue wiring
other than a `QueueBind` literal of the same file (a topic-exchange `#` binding, a binding made by an
admin script); SNS-to-SQS subscriptions; a Pub/Sub subscription whose topic is only named by an
administrative tool or `pubsubpb`. A literal holding a *plain* default (`topic := "orders"` never
reassigned) is read, since the file shows it.

Golden: `tests/golden/events/go-brokers.expected.yaml` (27 sites, 4 hops, 3 chains; unrelated-method,
log-line, parameter, `fmt.Sprintf`, pattern, overridden-default, shadowing, commented-import and
test-file decoys).

### 3.10 Python brokers

Python reads the brokers below besides Kafka and Celery (9.0 T8c, tag `py_broker`). Each site is
recorded under its own broker, so a queue named like a Kafka topic is another channel (`rabbitmq:orders`,
`sqs:orders`; [architecture 3.4](architecture.md#34-broker-qualified-topic-keys)). No new broker key: the
slugs are `rabbitmq`, `redis`, `nats`, `sqs`, `sns`, `pubsub` and `kafka`.

| Broker | Consumers | Producers | Key of a site |
| :--- | :--- | :--- | :--- |
| `rabbitmq` | pika `basic_consume` / `basic_get` / `consume`; aio-pika `queue.consume()` / `queue.iterator()`; kombu `Consumer(queues=[...])` and `connection.Consumer(...)` | pika `basic_publish`; aio-pika `exchange.publish` (also `channel.default_exchange`); kombu `Producer.publish` | the queue (aio-pika: the name given to `declare_queue` / `get_queue`; kombu: the name of the `Queue(...)`); for a producer the routing key |
| `redis` | `pubsub.subscribe` / `psubscribe` (positional channels, and the keyword of `subscribe(chan=handler)`) | `Redis.publish` | the channel or pattern |
| `nats` | nats-py `subscribe`; JetStream `subscribe` / `pull_subscribe` | `publish`, `request`, JetStream `publish` | the subject (a `*` / `>` wildcard as written) |
| `sqs` | boto3 `receive_message(QueueUrl=...)` | `send_message`, `send_message_batch` | the last segment of the queue URL, or the queue named by a `get_queue_url(QueueName=...)["QueueUrl"]` / `create_queue` |
| `sns` | none (SNS delivers to a queue: not followed) | `publish`, `publish_batch` (`TopicArn=...`) | the last segment of the ARN |
| `pubsub` | `SubscriberClient.subscribe(subscription)`, only for a subscription the same file creates on a topic (`create_subscription(name=sub, topic=topic)`); any other subscription registers no consumer (as in Java, and as in Go without a `CreateSubscription`) | `PublisherClient.publish(topic)` | the topic of `publisher.topic_path(project, topic)` or the last segment of a `projects/p/topics/t` literal; a consumer is keyed by the topic its subscription was created on, never by the subscription id |
| `kafka` | gaps of the shape-based reader: `KafkaConsumer("a", "b")`, `subscribe(topics=[...])` | none new | the topic |

What a name may be (nothing else is read, no name is guessed from an identifier): a plain string
literal (no f-string, no prefix but `r` / `u`, no escape, no concatenation), a string constant of the
same scope chain (module, class body, or a `self.x` / `cls.x` assigned in the class) that every binding
of the name agrees on, a keyword argument literal, or an `os.environ[...]` / `os.getenv(...)` read as a
`${KEY}` placeholder resolved against the configuration (section 3.6, a compose `environment:` block;
`.env` itself is never read). **A default is never a name**: `os.getenv("K", "d")` is read as `${K}`,
resolved by the configuration or left a visible, counted `${K}` topic. A literal that looks like a
credential (a token, a URL, `Bearer_...`, a password-like token, a random-looking run) is not a name either, and neither is a constant named like one (`API_KEY = "x"`): both use the TypeScript queue-constant predicate (`member_consts::constant_is_a_name`, section 3.8), and a refused literal registers no node.

A call is a site **of this pass** only when its receiver is a value built from the library, in a file whose `import`
statements (parsed, never a comment or a docstring) bring it in, through any of `import x`,
`import x as y`, `from x import y [as z]`:

- a name assigned from a call on an imported module or class (`pika.BlockingConnection(...)`,
  `redis.Redis(...)`, `boto3.client("sqs")`, `session = boto3.Session(); session.client("sqs")`,
  `pubsub_v1.PublisherClient()`, `nats.connect(...)`), a method chain from one
  (`connection.channel()`, `redis.pubsub()`, `nc.jetstream()`), a `with` / `async with` target, a
  `self.x` assigned in its class, or a parameter annotated with the library's type;
- scopes follow Python: a parameter, a loop / `except` / `with` variable, a comprehension variable, a
  lambda parameter, a `def` or a `class` of the same name hides an outer name, and a class body is not
  visible from its methods. A name bound twice to different things (a client and `None`, a client and
  a local class) is ambiguous and registers nothing. A plain parameter has no known type: its
  `publish` is not a site of this pass (the generic reader may still read it, see below);
- a call on an instance of a class the file declares is claimed too (it is nobody's producer), so a
  local `Bus.publish("x")` no longer reaches the shape-based Kafka reader, **unless the class holds a
  client of a library itself** (a wrapper: `class KafkaBus: self.producer = Producer(...)`): its
  callers' `bus.publish("topic")` then stays the generic reader's edge, as before this pass.

A call this pass recognises is read once, under its own broker: the shape-based Kafka reader skips it
unless the broker is Kafka, where it keeps reading it and the site now carries the `kafka` broker. A
site of unknown broker (an injected producer `self._producer.produce(...)`, a parameter) is read as
before, with no broker.

A producer never publishes to a pattern: a key with `*`, `#` or `>` registers no producer (a consumer
may subscribe to one). Producers and call-site consumers are `EventStream` nodes on the call line.

Bounds: at most 2,000 sites per file (`MAX_PY_BROKER_SITES_PER_FILE`; a call past it is not claimed, so
the generic reader still reads it), 32,768 scope bindings, and an evaluation budget of 2,048 steps per
call site and 400,000 per file (name resolution is memoised per scope and name, a cycle reads as
nothing); a file over a bound is counted in `IndexHealth::files_py_brokers_capped` and listed by
`mesh-mcp doctor` (a warning). Every walk is iterative, a receiver or a dotted path is followed 8 / 12 links, and each scope's
bindings are built once, so a hostile file costs a bounded pass whatever it nests.

**You will not get:** `queue_declare` / `exchange_declare` / `queue_bind` (declarations, not flow);
exchange-to-queue wiring (a `basic_publish(exchange, key)` is keyed on the routing key, so a queue bound
under another key or a fanout exchange does not link); `boto3.resource("sqs")` objects, `get_queue_url`
results held in a function result, SQS `TargetArn`; Redis streams (`xadd` / `xread`); a Celery /
kombu pool (`producers[conn].acquire()`); a client created in another module and imported, a client in
a function parameter without an annotation, a name rebound with `global`; Pub/Sub `pull` /
`streaming_pull`, and a Pub/Sub subscription the file does not create on a topic.

**Pre-existing limits of the generic (shape-based) reader, unchanged and not guaranteed by this pass.**
For a receiver this pass does not recognise, the older reader still registers an untagged event-bus
producer or consumer for `x.publish("literal")`, `.send("literal")`, `.produce("literal")` and
`.subscribe(["literal"])`. Known false positives of that reader: a parameter (`def f(obj):
obj.publish("fp.param")`), a loop, comprehension, lambda, `with ... as` or `except ... as` variable or
a `global` rebinding that hides a client, and an object from another package
(`from other_pkg import Redis; Redis().publish("x")`). The matcher was deliberately not widened; this
pass only guarantees that a call on a client it recognises is read once, under its broker.

### 3.11 Declared producer calls

A project that writes its events and jobs through its own helper (an outbox entity, a stream publisher)
leaves no broker API call for the generic rules to read. The helper is declared in the config, and its
calls become producers (9.0 T14b, tag `declared_call`, TypeScript and JavaScript):

```toml
[[engines.contracts.producer_calls]]
name = "outbox-job"
callee = "JobsOutbox.createJob"     # a method name, or `Receiver.method`
topic_keys = ["type"]               # object keys read as topics (a member keeps its member key)
queue_keys = ["target"]             # object keys read as queues (a member resolves to its value)

[[engines.contracts.producer_calls]]
name = "stream-publish"
callee = "streamBus.publish"        # no keys: argument `arg` (default 0) is the topic itself
```

- The call is found in the syntax tree, so a call in a comment or a string is not one. A
  `Receiver.method` callee needs the call's receiver to end with `Receiver` on a segment boundary
  (`this.deps.JobsOutbox.createJob` matches, `MyJobsOutbox.createJob` does not); a bare `method`
  callee matches any receiver and a bare call. A parameter or local of the same name as the helper (or
  its class) hides it. An import alias of the class (`import { JobsOutbox as J }`) is not followed.
- A key's value is read as any topic name is: a literal, a constant of the file, an imported
  `Enum.MEMBER`, a `process.env` / `ConfigService` placeholder, or an array of those
  (`targetServices: [Streams.A, Streams.B]`, one producer per element, each on its own line). Each
  name is reported on the line that carries it, not on the call line.
- The topics follow the rules of their kind. A `topic_keys` member keeps its MEMBER key (`scan_file`),
  so the handler that names the same member meets it (section 3.8: the same member of two enums is
  one flow). A `queue_keys` member resolves to the member's value through its own imports, and the
  `Queue.STORAGE` of one enum never meets the `LegacyQueue.STORAGE` of another.
- Every producer is `heuristic`: the helper is a name the operator vouches for, not a library API.
  There is no `exact` setting. Brokers never join: a declared producer has no broker, so the
  graph links it to a consumer by name only.
- What is not read is counted, never dropped: a value that is not a name (a parameter, `TABLE[kind]`,
  an argument that is no object literal), a receiver that is a call or an index and so cannot be
  compared with the declared one (`getOutbox().createJob(...)`, an angle-bracket cast), and a value
  refused as secret-shaped add to `IndexHealth::declared_call_sites_unread`, which `doctor` prints.
  `event_anomalies` (its Coverage line) and `analyze_impact` (under a "No producer statically
  resolved" answer) say so too, only when the count or a file cap is non-zero: "N declared producer
  call site(s) could not be read statically ...: a missing producer may be one of them". It is not a
  claim that no producer exists. A key an object does not carry is nothing. Refused as secret-shaped:
  a literal or constant that is not a name (section 3.8 rules), a member named like a credential
  (`API_KEY`), and a placeholder whose KEY is credential-named (`${DB_PASSWORD:x}`, whatever the
  default) or whose DEFAULT is not a name (`${TOPIC:hunter2-prod}`); `${TOPIC:orders}` and `${TOPIC}`
  are kept. This judgement is made at the declared site itself: the shared fold-time masking only
  drops a default that is not topic-shaped, and a short password is.
- A receiver is read through `as`, `!`, `satisfies` and parentheses (`(JobsOutbox as any).createJob`,
  `this.jobs!.enqueue`), and so is the callee itself (`(JobsOutbox.createJob as any)(...)`).
- A bare enum member as a topic key (`type: K.ONE`) becomes the generic topic `one`, so it can join
  any other `X.ONE` of the workspace (the section 3.8 rule: a member keeps its member key). Declare a
  queue key, or write the value, where two enums share a member name and must stay apart.
- Bounds: 64 declared entries (`doctor` warns on more), 2,000 sites per file
  (`IndexHealth::files_declared_capped`, `doctor` warns). A file that never writes the method name is
  not analysed. Test files are read like any other (their producers are hidden from tool answers
  unless `include_tests` is set).
- The declaration is part of the parse-cache key: editing it re-extracts the TypeScript and JavaScript
  files, and a change to it is a full reload.
- Not read: lookup tables (`type: COMPUTE_JOB_TYPE[kind]`), generic type arguments
  (`createEvent<Events.X>(...)`, use a `[[engines.contracts.patterns]]` regex), `new` expressions, a
  call through an alias of the receiver, other languages.

Fixture and ground truth: `examples/eda/declared-producers` and
`tests/golden/events/declared-producers.expected.yaml`.

## 4. What every level shares

- **Pattern-based.** A framework is recognised by a convention in the syntax tree, not by
  resolving types. A project that wraps a library behind its own helper (an outbox client, an event
  bus, a base controller) is invisible until you declare it with `[[engines.contracts.patterns]]`.
- **Confidence is explicit.** Edges are `exact`, `heuristic` or `ambiguous`. A declarative
  pattern produces `heuristic` edges unless it sets `confidence = "exact"`.
- **Zero means "not statically resolved in the indexed roots".** See the README for the
  wording and [mcp-tools](mcp-tools.md#unresolved-flow-notes).
- **Bounded input.** A file over 384 KB (1.5 MB for `.proto`, protobuf-generated stubs and files named exactly `openapi`/`asyncapi` (`.yaml`/`.json`)), with a line
  over 1,024 bytes, a null byte in the first 4 KB or nesting deeper than 64 is not parsed; it is
  listed by `mesh-mcp doctor` and named in tool answers whose scope contains it.
- **Generated code counts as code.** A generated stub inside a root is indexed unless excluded.
  Stubs outside the workspace are not seen.

## 5. Adding depth

To raise a language: [`development.md`](development.md#4-adding-a-tree-sitter-language) for a
new grammar, and the JavaScript/TypeScript subsection for adding a library or framework to the
rule tables. A new idiom needs a unit test through `PolyglotIndexer::index_file` so the dispatch
wiring is covered, and a note of its known limits in `CHANGELOG.md`.
