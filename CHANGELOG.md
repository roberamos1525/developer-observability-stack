# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
semantic versioning.

## [0.1.0] - 2026-10-12

### Added

- `obsd` server with per-tenant API keys (read/write/admin scopes), JSON config
  file plus `OBS_*` environment overrides, structured logging, `/healthz`,
  `/readyz` and `client_golang` self-metrics on `/metrics`.
- Bounded ingestion pipelines (logs, metrics, spans) with worker pools,
  per-tenant batching, backpressure (HTTP 429), `?sync=true` and drain on
  shutdown.
- Logs: JSON/NDJSON and syslog (RFC 5424/3164) ingestion; time-partitioned
  segment storage with CRC-framed records and per-segment inverted indexes
  (format `OBSLIDX1`); crash recovery; compaction; retention; log query
  language with field filters, full text, boolean operators, time ranges,
  limit, sort and `count by`.
- Metrics: JSON and Prometheus text ingestion, scrape manager, Gorilla
  compressed TSDB head with WAL, immutable blocks (format `OBSBLK01`),
  downsampling (count/sum/min/max/last), retention; PromQL subset with
  instant and range queries in Prometheus response format.
- Tracing: OTLP/HTTP JSON receiver for traces, logs and metrics; trace store,
  search, span-tree assembly and service dependency graph.
- Alerting: metric and log-count rules, state machine, templated annotations,
  silences, grouping and de-duplication, webhook (HMAC-signed, retries) and
  log channels, persisted rules and silences.
- Background maintenance for retention, compaction and downsampling, with an
  admin endpoint to trigger it.
- `loadgen` load generator and synthetic scrape target; example webhook
  receiver, configs, payloads, rules and `examples/smoke.sh`.
- Unit, property, fuzz and in-process integration tests; Dockerfile,
  compose file and CI workflow (not executed locally).

### Fixed during development

- The Prometheus text parser accepted metric names and label values larger
  than the storage limits (found by fuzzing); it now rejects them.
