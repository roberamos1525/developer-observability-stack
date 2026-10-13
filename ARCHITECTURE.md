# Architecture

## Overview

`obsd` is one process composed of loosely coupled packages. HTTP handlers
parse and validate payloads, hand them to bounded ingestion queues, and
return; worker pools persist batches into per-tenant storage engines.
Queries read the storage engines directly. Background loops evaluate alert
rules, scrape Prometheus targets and run retention/compaction.

```
                 ┌─────────────────────────────── obsd ─────────────────────────────────┐
 clients ──HTTP──►  httpapi: auth (API key → tenant+scopes), body limits, gzip, envelope │
                 │      │ parse/validate (logs, syslog, promtext, otlp, JSON samples)    │
                 │      ▼                                                               │
                 │  queue.Pipeline[T]  ×3 (logs, metrics, spans)                        │
                 │   bounded channel → N workers → per-tenant batches → handler         │
                 │      │                       ▲ backpressure: 429 + Retry-After       │
                 │      ▼                                                               │
                 │  tenant.Registry ──► Tenant{ logs/store, tsdb, traces }              │
                 │      ▲        ▲          ▲                                           │
                 │      │        │          └── queries: logs/query, promql, traces     │
                 │  scrape.Manager   alerting.Manager ──► Dispatcher ──► webhook / log  │
                 │  (promtext)       (promql + log count)                               │
                 │  retention.Runner (seal, compact, cut blocks, downsample, expire)    │
                 │  selfmon (client_golang registry → /metrics)                         │
                 └──────────────────────────────────────────────────────────────────────┘
```

## Package map

| Package | Responsibility |
|---|---|
| `cmd/obsd` | Flags, config loading, logger, signal handling |
| `cmd/loadgen` | Load generator and synthetic `/metrics` endpoint |
| `internal/app` | Wires everything; lifecycle (`Start`, `Flush`, `Shutdown`, `Serve`); alert querier adapter |
| `internal/config` | Defaults, JSON file (unknown fields rejected), `OBS_*` env overrides, validation |
| `internal/auth` | API keys stored as SHA-256 digests, scopes `read`/`write`/`admin`, tenant header when auth is disabled |
| `internal/errs` | Typed errors (`Kind`) mapped to HTTP status codes and error codes |
| `internal/queue` | Generic batching pipeline with worker pool, backpressure, flush/drain, panic containment |
| `internal/selfmon` | Prometheus self-metrics |
| `internal/logs` | Entry model, JSON/NDJSON/syslog parsers, tokenizer shared by index and query |
| `internal/logs/postings` | Sorted posting-list algebra and delta-varint codec |
| `internal/logs/query` | Lexer, recursive-descent parser, AST matcher, index planner |
| `internal/logs/store` | Partitions, segments, index persistence, search/count execution, compaction, retention |
| `internal/tsdb/labels` | Label sets, validation, matchers |
| `internal/tsdb/chunk` | Gorilla XOR chunk encoding (bit stream, delta-of-delta, XOR values) |
| `internal/tsdb` | Head, WAL, blocks, downsampling, retention, selection with block/head merge |
| `internal/promtext` | Prometheus text exposition parser |
| `internal/scrape` | Scrape loops, target labels (`job`, `instance`, `exported_*`), `up` series |
| `internal/promql` | PromQL lexer/parser (Pratt), evaluator with preloading, functions, aggregation, vector matching |
| `internal/otlp` | OTLP/JSON structs and conversion to spans, log entries and samples |
| `internal/traces` | Span model, NDJSON-backed store, search, tree assembly, dependency graph |
| `internal/alerting` | Rules, state machine, silences, notification channels, dispatcher, manager + persistence |
| `internal/tenant` | Lazily opened per-tenant storage under `<data_dir>/tenants/<id>` |
| `internal/retention` | Periodic maintenance across tenants |
| `internal/httpapi` | Routes, middleware, handlers, response envelopes |
| `internal/loadgen` | Payload builders and load runner |

## Data flow

### Ingestion

1. `authorize` resolves the API key to a principal (tenant + scopes) and
   checks the route's scope.
2. `readBody` enforces `max_body_bytes` on the wire and
   `max_decompressed_bytes` after gzip decoding.
3. The payload is parsed and validated into internal items; invalid records
   are counted and reported per line (or as OTLP `partialSuccess`).
4. `queue.Pipeline.Submit` enqueues one job. If the channel is full for
   longer than `submit_timeout`, the request fails with **429** and
   `Retry-After: 1`. With `?sync=true` the handler waits for the batch to be
   persisted and reports storage errors.
5. Workers coalesce jobs into batches (up to `batch_size` items or whatever
   arrived within `flush_interval`), group them by tenant, and call the
   storage engine once per tenant batch.

### Queries

- **Logs**: the query string is parsed into an AST; for every segment
  overlapping the time range, `query.Plan` turns the AST into a candidate
  posting list and an *exact* flag using the segment's inverted index. Exact
  candidates need no verification; others are read and checked with
  `Node.Match`. A top-K heap keeps the newest (or oldest) `limit` entries and
  segments that cannot improve the heap are pruned using their min/max
  timestamps. `count by` aggregates without reading entries when the plan is
  exact and no grouping is needed.
- **Metrics**: the engine walks the AST once and preloads every selector's
  series for the whole evaluation window from `tsdb.DB.Select` (raw blocks
  merged with the head; ranges older than the raw data are served from
  downsampled blocks). Each evaluation step then works on in-memory slices
  with binary search. Limits: max samples loaded, max range steps, query
  timeout.
- **Traces**: search scans the in-memory trace index; a trace matches when
  any span satisfies every criterion. Trace-by-ID assembles a tree.

### Alerting

Every `evaluation_interval` the manager evaluates all rules of all tenants
(queries run outside the manager lock). Results become alert instances keyed
by a label fingerprint; `alerting.Step` advances each instance's state.
Firing alerts and fresh resolutions are handed to the dispatcher, which
groups them by `group_by` labels (default `alertname`) and decides per group
whether a notification is due (`group_wait`, `group_interval`,
`repeat_interval`, content hash de-duplication, silences). Deliveries happen
outside the lock.

## Storage engines

Details of the byte formats are in [docs/data-model.md](docs/data-model.md).

- **Logs** — one directory per time partition (default 1 h). Each partition
  has at most one active segment receiving appends (rolled at
  `segment_max_bytes` / `segment_max_entries`); sealing fsyncs the log and
  writes the `.idx` file atomically. Segments are reference counted so
  retention and compaction can delete files while queries still read them.
- **Metrics** — the head keeps one series object per label set with a list of
  Gorilla chunks; chunks are cut at block-aligned boundaries so cutting a
  block is exact. Accepted samples are appended to the WAL. When the head
  spans more than 1.5 block durations the oldest window is written as a block
  and the WAL is checkpointed (rewritten to contain only the live head).
- **Traces** — spans are appended as JSON lines to hourly partition files and
  indexed in memory (trace → spans, service → operations); on open the files
  are replayed.

## Concurrency model

- Storage engines are safe for concurrent use: `logs/store` serialises
  writers with `appendMu` and protects partition lists with an RWMutex;
  segments have their own RWMutex and an atomic reference count. `tsdb`
  serialises appends and block cuts, while each series has a mutex so reads
  run concurrently with writes. The trace store uses one RWMutex.
- Background loops (`alerting`, `scrape`, `retention`) run in goroutines
  owned by `app.App` and stop via context cancellation on shutdown.
- Shutdown order: readiness off → HTTP server shutdown → stop loops → drain
  queues (`Close` processes everything already queued) → close tenants (seal
  active log segments, fsync WAL, flush span files).

## Multi-tenancy

Every API key belongs to exactly one tenant, and every request operates only
on its principal's tenant. Tenants are isolated on disk
(`<data_dir>/tenants/<id>/{logs,metrics,traces}`) and in memory (separate
store instances). Alert rules and silences are namespaced by tenant inside the
alerting state file. Tenant IDs are restricted to `[a-z0-9][a-z0-9_-]{0,63}`
because they become directory names.

## Error handling

All layers return `*errs.Error` values carrying a `Kind`. The HTTP layer maps
kinds to status codes and responds with a uniform envelope
`{"status":"error","error":{"code":…,"message":…},"requestId":…}`; internal
errors are logged with the request ID and reported to clients as a generic
message. OTLP endpoints return `google.rpc.Status`-shaped errors as the
OTLP/HTTP specification requires.
