# Testing

## Running

```bash
go test ./... -count=1                 # full suite (unit + integration)
go test ./tests/integration -v         # in-process end-to-end tests
go test ./internal/tsdb/chunk -run '^$' -fuzz FuzzRoundTrip -fuzztime 30s
go test ./internal/logs/query -run '^$' -fuzz FuzzParse -fuzztime 30s
go test ./internal/promql -run '^$' -fuzz FuzzParseAndEval -fuzztime 30s
go test ./internal/promtext -run '^$' -fuzz FuzzParse -fuzztime 30s
go test ./internal/logs -run '^$' -fuzz FuzzParsers -fuzztime 30s
examples/smoke.sh                      # real binaries, real sockets
```

The race detector (`go test -race ./...`) needs CGO and was not run on the
development machine (no C compiler); the CI workflow runs it on Linux.

## Strategy

### Unit tests (next to the code)

| Package | What is covered |
|---|---|
| `tsdb/chunk` | **Property test**: 500 random series mixing regular intervals, jitter, zero deltas, negative deltas, huge jumps, every delta-of-delta bucket, integers, sign flips and special values (NaN payloads, ±Inf, −0, max/min float, subnormals) must decode bit-for-bit; extreme `int64` timestamps; exact bucket boundaries; compression ratio on a regular counter; resuming an appender; truncated chunks; capacity; a **fuzz target** |
| `tsdb` | Matchers (including empty-matching), out-of-order/duplicate/invalid rejection, WAL replay with a torn tail, block cut + reopen + checkpoint (no resurrected samples), downsampled aggregates (count/sum/min/max/last/avg) and fallback, retention of raw/downsampled blocks and head data, corrupt blocks skipped, concurrent appends with concurrent reads, chunk boundaries, postings intersection |
| `logs` | JSON array/object/NDJSON parsing, flattening, timestamp units, validation and truncation, RFC 5424 (structured data escapes) and RFC 3164 (year rollover), tokenizer, a **fuzz target** checking storage invariants of accepted entries |
| `logs/query` | Parser structure and stages, syntax errors (typed with positions), matcher semantics, a **randomised property test** comparing the index planner with brute-force matching on 400 random queries, index-key truncation, a **fuzz target** |
| `logs/store` | Ordering/limit/truncation with top-K pruning, time ranges and partition pruning, `count by` (including the no-read fast path), recovery of unsealed segments with torn tails, corrupt index rebuild, segment rolling, compaction re-sorting, retention, active-segment limits, reference-counted deletion under concurrent readers, concurrent append/search/compact |
| `promql` | rate/irate/increase with counter resets and extrapolation, all aggregations, `*_over_time`, `histogram_quantile` edge cases, operator precedence, vector matching, set operators, functions, lookback, range queries, limits, parse errors, error kinds, a **fuzz target** over parse + evaluate |
| `promtext` | Full exposition example (escapes, timestamps, NaN/Inf, `# EOF`), 17 malformed inputs, a **fuzz target** (it found that oversize names/values were accepted; the parser now enforces label limits) |
| `alerting` | **State machine** table tests for every transition, rule validation, template expansion, silences (regex, negative matchers), dispatcher group wait / interval / repeat / de-duplication / silencing, webhook HMAC signing and retry policy, manager end-to-end with persistence |
| `otlp` | Fixtures shaped like real OTLP/JSON exports (`testdata/`), typed attributes, partial rejections, metric mapping, invalid spans one by one, scalar codecs |
| `traces` | Append/dedup/limits/persistence, corrupt-line replay, search filters, tree assembly with self time, orphans and parent cycles, dependency graph, retention, concurrency |
| others | config precedence and validation, auth scopes and digests, queue batching/backpressure/panics/drain, scrape labels and limits, retention runner, tenant registry, HTTP helpers, self-metrics, load generator payload validity |

### Integration tests (`tests/integration`)

Each test starts the complete application (`app.New`) on a temporary data
directory behind `httptest.Server` and talks HTTP:

- logs over NDJSON, gzip JSON, syslog and OTLP → queries with phrases,
  numeric filters, negation, time ranges, `since`, `count by`, field listing;
- metrics over JSON, Prometheus text and OTLP → `sum(rate)`,
  `histogram_quantile`, `avg_over_time`, range queries, series/labels APIs,
  form-encoded queries, error envelopes;
- OTLP traces fixture → partial success, search by service/error/duration/
  attributes/time, trace tree, services, dependencies, protobuf rejection;
- alert rules (metric and log-count) firing to a webhook receiver with
  signature verification, silences suppressing resolution notices, rule CRUD
  and tenant isolation;
- authentication, scopes, tenant isolation, admin endpoints;
- body limits, gzip bombs, unsupported encodings/content types, 404/405;
- health, readiness and `/metrics` content;
- retention + compaction + downsampling through the admin endpoint, then a
  restart on the same directory proving persistence of logs, metrics,
  traces and rules;
- scraping a live target through the app, concurrent clients plus the load
  generator with exact-count verification, OTLP gzip with auth disabled.

### Smoke test

`examples/smoke.sh` builds the binaries and exercises them over real sockets
(see [GETTING_STARTED.md](../GETTING_STARTED.md#end-to-end-smoke-test)).

## Conventions

- Tests must be deterministic: fixed random seeds, injected clocks where the
  logic is time based (`alerting.Options.Now`, `retention.Runner.Now`,
  explicit timestamps in storage tests).
- Short sleeps appear only where wall-clock ordering is itself under test
  (queue batching, least-recently-written segment sealing, loop shutdown).
- Every bug found by fuzzing gets a regular regression test case.

## Last verification (2026-10-12, Windows 11, Go 1.26.2)

- `gofmt -l .` printed nothing; `go vet ./...` reported nothing.
- `go test ./... -count=1`, run twice: 172 passing tests each time (132
  top-level tests plus 40 subtests/fuzz seed cases) across 23 packages, 0
  failures.
- Short fuzzing sessions (10–20 s each) of every fuzz target; the
  `promtext` session found the label-length gap described in the changelog.
- `examples/smoke.sh` passed. In that single local run the load generator sent
  300 requests in 10 s (20 000 log entries, 5 200 samples, 4 000 spans) with
  0 errors and p99 request latencies of roughly 10-12 ms; these numbers are
  indicative only.
- A manual crash test (hard-killing `obsd` after ingestion and restarting)
  recovered all 20 000 log entries from the unsealed segment, replayed the
  metrics WAL and kept rules and traces.
