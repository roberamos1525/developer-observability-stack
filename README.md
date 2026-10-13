# developer-observability-stack

A single-binary observability backend written in Go. It ingests **logs**,
**metrics** and **traces**, stores them in purpose-built embedded storage
engines, answers queries in three query languages, evaluates **alert rules**
and sends grouped, signed webhook notifications — all multi-tenant and behind
per-tenant API keys.

It is a portfolio project: it implements the core mechanisms of systems like
Loki, Prometheus, Jaeger and Alertmanager from scratch (inverted indexes,
Gorilla compression, a PromQL engine, an OTLP receiver, an alert state
machine) at a scale suitable for one machine.

## Highlights

| Area | What is implemented |
|---|---|
| Log ingestion | JSON array / object / NDJSON with flattening of nested fields, syslog RFC 5424 and RFC 3164 parsing, OTLP/HTTP JSON logs, per-line error reporting |
| Log storage | Time-partitioned, append-only segments with CRC-framed records; per-segment inverted index (message terms + `field=value` postings, delta-varint encoded); crash recovery of unsealed segments; background compaction; retention by partition |
| Log query language | Field filters (`=`, `!=`, `<`, `<=`, `>`, `>=`, `=~`, `!~`, prefix `*`, exists), full-text terms and phrases, `AND`/`OR`/`NOT`/parentheses, pipeline stages `limit`, `sort`, `since`, `between`, `count by` — parsed to an AST, planned against the index, verified, top-K merged |
| Metrics ingestion | Own JSON format, Prometheus text exposition (push endpoint **and** a scrape manager), OTLP/HTTP JSON metrics (gauge, sum, histogram, summary) |
| TSDB | In-memory head of **Gorilla-compressed chunks** (delta-of-delta timestamps, XOR floats), write-ahead log with checkpointing, immutable on-disk blocks, **downsampling** (count/sum/min/max/last per 5 m bucket), retention per resolution |
| PromQL subset | Selectors with all matcher types and `offset`, `rate`, `irate`, `increase`, `delta`, `*_over_time`, `histogram_quantile`, `sum/avg/min/max/count/stddev/topk/bottomk/quantile` with `by`/`without`, arithmetic/comparison (`bool`)/set operators with `on`/`ignoring`, instant and range queries, Prometheus-compatible response format |
| Tracing | OTLP/HTTP JSON ingestion following the OpenTelemetry data model (hex IDs, resource/scope nesting, typed AnyValue attributes, events, links, status), search by service/operation/duration/attributes/errors/time, span-tree assembly with self time, service dependency graph |
| Alerting | Metric (PromQL) and log-count rules, `for` durations, state machine *inactive → pending → firing → resolved*, templated annotations, silences with (regex) matchers, grouping, de-duplication, repeat interval, webhook (HMAC-SHA256 signed, retries with backoff) and log channels |
| Operations | Per-tenant API keys with read/write/admin scopes, bounded ingestion queues with worker pools and backpressure (HTTP 429), gzip request bodies with decompression limits, self-metrics via `client_golang` on `/metrics`, `/healthz` and `/readyz`, background retention/compaction/downsampling, JSON config + env overrides |
| Tooling | `cmd/loadgen` load generator (logs, metrics, OTLP traces, synthetic `/metrics` target), example webhook receiver, scripted smoke test |

## Quick start

```bash
go build -o bin/ ./cmd/obsd ./cmd/loadgen
OBS_API_KEYS="demo:demo-admin-key-123:read+write+admin" OBS_DATA_DIR=./var/data ./bin/obsd

# in another shell
./bin/loadgen -api-key demo-admin-key-123 -duration 10s
curl -s -G -H "Authorization: Bearer demo-admin-key-123" localhost:8428/api/v1/logs/query \
     --data-urlencode 'q=level:error service:payments | limit 5'
curl -s -G -H "Authorization: Bearer demo-admin-key-123" localhost:8428/api/v1/query \
     --data-urlencode 'query=sum by (service) (rate(http_requests_total[1m]))'
```

`examples/smoke.sh` runs the whole flow (server, load, queries, an alert
delivered to a local webhook receiver) in one command. See
[GETTING_STARTED.md](GETTING_STARTED.md) for a walkthrough.

## Documentation

- [GETTING_STARTED.md](GETTING_STARTED.md) — install, configure, run, first queries
- [API.md](API.md) — every endpoint with parameters and examples
- [ARCHITECTURE.md](ARCHITECTURE.md) — components, data flow, concurrency
- [docs/data-model.md](docs/data-model.md) — on-disk formats and encodings
- [docs/query-languages.md](docs/query-languages.md) — log query language and PromQL subset
- [docs/operations.md](docs/operations.md) — configuration reference, runbook, retention, alerting
- [docs/testing.md](docs/testing.md) — test strategy and how to run tests and fuzzers
- [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [CHANGELOG.md](CHANGELOG.md)

## Technology stack

- Go 1.26, standard library HTTP server (`net/http` method/path patterns), `log/slog`
- `github.com/prometheus/client_golang` for self-monitoring (the only direct dependency)
- Pure Go, no CGO: builds without a C toolchain; all storage is embedded and file based

## Testing

```bash
go test ./... -count=1
```

Unit tests cover parsers, the query planner (randomised cross-check against
brute-force matching), Gorilla compression (property tests with random series
including NaN/±Inf/negative/zero deltas, plus a fuzz target), WAL/segment
crash recovery, the PromQL engine, the alert state machine, dispatcher
grouping and webhook signing. Integration tests in `tests/integration` start
the full server in-process and drive it over HTTP (ingest → query for every
signal, alerts to a webhook, auth/tenant isolation, limits, retention,
restart). See [docs/testing.md](docs/testing.md).

## Limitations

This is a single-node system designed for learning and local use. Notable
limitations (full list in [docs/operations.md](docs/operations.md#limitations)):

- No clustering, replication or high availability; one process owns the data directory.
- Sealed log indexes, TSDB blocks and the trace store are held in memory (no mmap), so data volume is bounded by RAM.
- PromQL is a subset: no subqueries, `group_left`/`group_right`, `label_replace`, `@` modifier or native histograms; staleness is handled by the lookback window only.
- OTLP is supported over HTTP with JSON encoding only (no protobuf, no gRPC); delta temporality and exponential histograms are rejected.
- Alert state (pending/firing) is in memory: after a restart rules re-evaluate from scratch. Rules and silences are persisted.
- API keys are static (config/env); there is no TLS termination, key rotation API or request-rate limiting beyond queue backpressure and body limits.
- The Dockerfile and `compose.yaml` were written but **not built locally** (Docker is not installed on the development machine); the CI workflow is a local file that has never run; the race detector (`-race`, needs CGO) was **not** run locally.

## Roadmap

- Memory-mapped index and block files; streaming compaction
- Protobuf OTLP and gRPC receivers
- More PromQL (`group_left`, subqueries, `label_replace`) and recording rules
- Persisted alert state and Alertmanager-compatible silences API
- Log query regex index (n-grams) and per-tenant quotas / rate limits

## AI-assisted development

This project was developed with AI assistance (Claude by Anthropic). The
design, code, tests and documentation were produced in an AI-assisted
workflow and reviewed and verified by building the code, running
`gofmt`/`go vet`, running the full test suite (`go test ./... -count=1`,
twice), running short fuzzing sessions, and executing the end-to-end smoke
test described above. Performance figures quoted anywhere in the docs come
from a single local run on a development machine and are indicative only.

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Robert Ramos.
