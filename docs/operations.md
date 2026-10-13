# Operations guide

## Configuration reference

Precedence: defaults < JSON file (`-config` / `OBS_CONFIG`) < environment.
Durations accept Go syntax plus `d` and `w`. `obsd -check-config` validates
and exits.

| Key | Default | Env | Notes |
|---|---|---|---|
| `listen_addr` | `127.0.0.1:8428` | `OBS_LISTEN_ADDR` | Loopback by default; put a TLS proxy in front for remote access |
| `data_dir` | `./var/data` | `OBS_DATA_DIR` | One process per directory |
| `log_level` / `log_format` | `info` / `json` | `OBS_LOG_LEVEL` / `OBS_LOG_FORMAT` | `text` for humans |
| `max_body_bytes` | 8 MiB | | Wire size limit per request |
| `max_decompressed_bytes` | 32 MiB | | Limit after gzip decoding (zip-bomb guard) |
| `shutdown_timeout` | 15s | | Graceful shutdown budget, also the `?sync=true` wait limit |
| `auth.enabled` | `true` | `OBS_AUTH_ENABLED` | When false, `X-Tenant-ID` selects the tenant and every request has admin rights |
| `auth.default_tenant` | `default` | `OBS_DEFAULT_TENANT` | Used when auth is disabled |
| `auth.keys[]` | | `OBS_API_KEYS` | `{tenant, key | key_sha256, scopes}`; env format `tenant:key[:read+write+admin];…` |
| `ingest.workers` | 4 | `OBS_INGEST_WORKERS` | Workers per signal pipeline |
| `ingest.queue_capacity` | 1024 | `OBS_INGEST_QUEUE_CAPACITY` | Jobs (requests) per pipeline |
| `ingest.batch_size` | 2000 | | Items coalesced per batch |
| `ingest.flush_interval` | 200ms | | Max time a worker waits to fill a batch while the queue is busy |
| `ingest.submit_timeout` | 250ms | | Wait for queue space before answering 429 |
| `logs.partition_duration` | 1h | | Time partition size |
| `logs.segment_max_bytes` / `segment_max_entries` | 32 MiB / 200 000 | | Segment roll thresholds |
| `logs.retention` | 7d | `OBS_LOGS_RETENTION` | Whole partitions are deleted |
| `logs.compact_min_segments` | 4 | | Minimum small segments to merge |
| `logs.default_query_limit` / `max_query_limit` | 100 / 5000 | | |
| `metrics.block_duration` | 2h | | Block window and chunk boundary |
| `metrics.chunk_max_samples` | 120 | | Samples per Gorilla chunk |
| `metrics.raw_retention` | 15d | `OBS_METRICS_RAW_RETENTION` | Raw blocks (and stale head data) |
| `metrics.downsample_resolution` | 5m | | Bucket width |
| `metrics.downsample_after` | 24h | | Age before a raw block is downsampled |
| `metrics.downsampled_retention` | 90d | `OBS_METRICS_DOWNSAMPLED_RETENTION` | Must be ≥ raw retention |
| `metrics.lookback_delta` | 5m | | Instant-vector staleness window |
| `metrics.query_timeout` | 30s | | Per query |
| `metrics.max_samples_per_query` | 5 000 000 | | Samples loaded per query |
| `metrics.max_range_steps` | 11 000 | | Steps per range query |
| `metrics.scrape.interval` / `timeout` | 15s / 10s | `OBS_SCRAPE_INTERVAL` | |
| `metrics.scrape.max_body_bytes` | 16 MiB | | Per scrape response |
| `metrics.scrape.targets[]` | | | `{tenant, job, url, labels}` |
| `traces.partition_duration` | 1h | | Span file partition |
| `traces.retention` | 3d | `OBS_TRACES_RETENTION` | |
| `traces.max_spans_per_trace` | 10 000 | | Extra spans are dropped |
| `traces.max_search_results` | 1000 | | Upper bound for `limit` |
| `alerting.evaluation_interval` | 15s | `OBS_ALERT_EVAL_INTERVAL` | |
| `alerting.group_wait` / `group_interval` / `repeat_interval` | 10s / 1m / 4h | | Notification timing |
| `alerting.resolved_retention` | 15m | | How long resolved alerts stay visible |
| `alerting.channels[]` | `[{name: log, type: log}]` | | `{name, type: log|webhook, url, secret_env, timeout, max_retries}` |
| `retention.interval` | 1m | `OBS_RETENTION_INTERVAL` | Maintenance loop period |

## Runbook

### Starting and stopping

- Start: `obsd -config /etc/obsd.json` with secrets in the environment.
- Stop: send SIGINT/SIGTERM. The server stops accepting connections, drains
  ingestion queues, seals active log segments, fsyncs the WAL and closes span
  files within `shutdown_timeout`.
- Crash safety: log records and WAL records are CRC-framed. After a crash,
  unsealed log segments are scanned and their torn tails truncated, and the
  metrics WAL is replayed (verified by killing the process during the smoke
  test). Items still waiting in an ingestion queue when the process dies are
  lost; a `?sync=true` response means the batch was handed to the OS. Data
  is fsynced when segments are sealed, during every maintenance pass and at
  shutdown, so a power loss can lose writes since the last fsync.

### Health signals

| Signal | Meaning / action |
|---|---|
| `/readyz` 503 | Starting up or shutting down |
| HTTP 429 on ingestion | Queue full. Check `obs_queue_depth`, `obs_queue_batch_duration_seconds`; raise `ingest.workers` / `queue_capacity` or slow down producers (clients should honour `Retry-After`) |
| `obs_queue_items_total{outcome="failed"}` | Storage errors; see logs for `request failed` / batch errors |
| `obs_ingest_rejected_total{reason="storage"}` | Out-of-order, duplicate or too-old metric samples, duplicate spans |
| `obs_maintenance_runs_total{result="error"}` | Retention/compaction failures (disk full, permissions) |
| `obs_alert_notifications_total{result="failure"}` | Webhook unreachable or rejecting; check the channel URL and secret |
| `obs_scrape_duration_seconds`, `up` series, `/api/v1/scrape/targets` | Scrape health |

### Retention, compaction and downsampling

The maintenance loop (`retention.interval`) runs per tenant:

1. seal active log segments of partitions that have ended;
2. delete log partitions older than `logs.retention`;
3. compact small sealed log segments;
4. cut completed metric blocks from the head and checkpoint the WAL;
5. downsample raw blocks older than `downsample_after`;
6. delete expired raw and downsampled blocks (and stale head chunks);
7. fsync the WAL;
8. drop expired traces and span files.

`POST /api/v1/admin/maintenance` runs one pass immediately and returns a
report per tenant.

### Disk usage

`GET /api/v1/admin/stats` reports, per tenant, log partitions/segments/bytes,
TSDB head series, WAL bytes and block metadata, and trace counts. Removing a
tenant means stopping the server and deleting `<data_dir>/tenants/<id>`.

### Alerting

- Rules and silences survive restarts (`<data_dir>/alerting/state.json`);
  alert instance state does not, so after a restart a rule with `for: 5m`
  needs another five minutes before it fires again.
- A failing rule query keeps the previous alert states and records
  `lastError` (visible in `GET /api/v1/alerts/rules`).
- Webhook payloads are signed with HMAC-SHA256 over the raw body when the
  channel has `secret_env`; receivers should verify `X-Obs-Signature`
  (`examples/webhook-receiver` shows how).

### Backup

Stop the server (or accept a crash-consistent copy) and copy `data_dir`.
Sealed segments, blocks and span files are immutable once written; the WAL
and active segments are append-only.

## Limitations

- Single node: no clustering, replication, HA or sharding.
- Memory: sealed log indexes, all TSDB blocks and the entire trace store
  within retention are held in memory; there is no memory-mapping or
  spilling. Log compaction loads the merged segments into memory.
- Logs: full-text search covers the message only; field equality is exact
  and case-sensitive (except `level`); regex and numeric comparisons are
  verified by scanning candidates; `count by` is capped at 10 000 groups;
  data older than the retention period is accepted and removed by the next
  maintenance pass.
- Metrics: samples must arrive in order per series; samples older than the
  newest block are rejected; no native histograms or exemplars; the
  downsampled fallback for old ranges exposes the `last` aggregate, so
  `rate()` over downsampled data works on bucket-final values only.
- PromQL: see [query-languages.md](query-languages.md#not-supported).
- Traces: search scans traces in memory; no sampling; dependency edges come
  from parent/child relationships and `peer.service` only.
- OTLP: HTTP + JSON only; delta temporality and exponential histograms are
  rejected.
- Alerting: no HA de-duplication; alert state is not persisted; inhibition
  rules are not implemented.
- Security: static API keys (no rotation API, no OAuth/OIDC), no built-in TLS,
  no per-tenant quotas or request rate limits beyond queue backpressure and
  body limits. Read requests for a valid tenant ID create its (empty) storage
  directories.
- Verification: Docker image and compose file not built locally; the CI
  workflow has never run; `-race` was not run locally (requires CGO).
