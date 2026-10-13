# API reference

Base URL: `http://<listen_addr>` (default `127.0.0.1:8428`).

## Conventions

**Authentication.** Send an API key as `Authorization: Bearer <key>` or
`X-API-Key: <key>`. Each key belongs to one tenant and has scopes:
`read` (queries), `write` (ingestion, alert rules, silences), `admin`
(everything, plus `/api/v1/admin/*`). With authentication disabled the
`X-Tenant-ID` header selects the tenant (default `auth.default_tenant`).

**Envelopes.** JSON API responses use

```json
{"status": "success", "data": ...}
{"status": "error", "error": {"code": "invalid_argument", "message": "..."}, "requestId": "9f1c..."}
```

Error codes and statuses: `invalid_argument` 400, `unauthorized` 401,
`forbidden` 403, `not_found` 404, `conflict` 409, `payload_too_large` 413,
`unsupported_media_type` 415, `unavailable` 429 (with `Retry-After`; returned
when an ingestion queue is full), `timeout` 503, `internal` 500 (details are
only logged). Every response carries `X-Request-ID` (echoed if supplied).

**Bodies.** Requests may be gzip-compressed (`Content-Encoding: gzip`).
Limits: `max_body_bytes` on the wire (default 8 MiB) and
`max_decompressed_bytes` after decompression (default 32 MiB).

**Ingestion is asynchronous.** Ingestion endpoints return **202** once data is
queued. Add `?sync=true` to receive **200** only after the batch is persisted
(storage errors are then reported).

**Time parameters** accept RFC 3339 (`2026-10-12T10:00:00Z`) or Unix seconds
(`1791799200`, fractions allowed). **Durations** accept Go syntax plus `d`/`w`
(`90s`, `1h30m`, `7d`) or float seconds.

---

## Health and self-monitoring (no auth)

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness: `{"status":"ok"}` |
| GET | `/readyz` | Readiness: 200 `ready`, 503 while starting or shutting down |
| GET | `/metrics` | Prometheus exposition of `obs_*` self-metrics plus Go/process collectors |

Self-metrics include `obs_http_requests_total{route,method,code}`,
`obs_http_request_duration_seconds`, `obs_ingest_accepted_total{signal,tenant}`,
`obs_ingest_rejected_total{signal,reason}`, `obs_queue_items_total{queue,outcome}`,
`obs_queue_depth{queue}`, `obs_queue_batch_duration_seconds`,
`obs_query_duration_seconds{signal}`, `obs_alert_rule_evaluations_total`,
`obs_alert_state_transitions_total{to}`, `obs_alert_notifications_total{channel,result}`,
`obs_scrape_duration_seconds{job}`, `obs_maintenance_runs_total{job,result}`,
`obs_tenants_open`.

---

## Logs

### `POST /api/v1/logs` — scope `write`

Content types: `application/json` (array of objects or a single object),
`application/x-ndjson` / `application/jsonl` (one object per line).

Recognised keys: timestamp from `timestamp|ts|time|@timestamp|date` (RFC 3339,
`YYYY-MM-DD hh:mm:ss`, or epoch s/ms/µs/ns inferred from magnitude; default:
receive time), level from `level|severity|lvl|log.level|severity_text`
(normalised to trace/debug/info/warn/error/fatal), message from
`message|msg|log|body|text`. Every other key becomes a field; nested objects
are flattened with dots, arrays are stored as JSON text.

```json
{"timestamp":"2026-10-12T10:00:02Z","level":"ERROR","message":"connection refused","service":"checkout","db":{"system":"postgres"}}
```

Response: `{"accepted": 3, "rejected": 1, "errors": [{"line": 4, "error": "invalid JSON object: ..."}]}`
(at most 20 errors listed).

### `POST /api/v1/logs/syslog` — scope `write`

Body: one syslog message per line. RFC 5424 (`<PRI>1 TIMESTAMP HOST APP PROCID MSGID [SD] MSG`)
and RFC 3164 (`<PRI>Mmm dd hh:mm:ss HOST TAG[PID]: MSG`) are recognised; lines
without `<PRI>` are stored as plain messages. Severity maps to the level;
`facility`, `severity`, `host`, `app` (also `service`), `procid`, `msgid` and
structured data (`sd.<id>.<param>`) become fields.

### `GET|POST /api/v1/logs/query` — scope `read`

| Parameter | Description |
|---|---|
| `q` | Query in the [log query language](docs/query-languages.md#log-query-language); empty matches everything |
| `start`, `end` | Optional inclusive time bounds |
| `limit` | Max entries (default `logs.default_query_limit`, max `logs.max_query_limit`); `| limit N` in the query takes precedence |

Search response:

```json
{"entries": [{"timestamp": "2026-10-12T04:23:35.4443577Z", "level": "error", "message": "...", "fields": {"service": "payments"}}],
 "truncated": true,
 "stats": {"partitions_scanned": 1, "segments_scanned": 1, "segments_pruned": 0, "candidate_entries": 27, "entries_read": 27, "matched": 27}}
```

With a `| count [by f1, f2]` stage the response contains
`"groups": [{"labels": {"level": "error"}, "count": 1000}, ...]` sorted by
count descending.

### `GET /api/v1/logs/fields` — scope `read`

Indexed field names with the highest per-segment distinct-value count.

---

## Metrics

### `POST /api/v1/metrics` — scope `write`

```json
{"samples": [
  {"name": "http_requests_total", "labels": {"service": "api", "code": "200"}, "value": 1024, "timestamp": 1791799200000},
  {"labels": {"__name__": "temperature", "room": "lab"}, "value": "NaN"}
]}
```

`timestamp` is Unix milliseconds (default: receive time); `value` accepts
numbers and `"NaN"`, `"+Inf"`, `"-Inf"`. Up to 100 000 samples per request.
Label names must match `[a-zA-Z_][a-zA-Z0-9_]*`; metric names
`[a-zA-Z_:][a-zA-Z0-9_:]*`. The storage layer additionally rejects
out-of-order samples, conflicting duplicates and samples older than the newest
persisted block (counted in `obs_ingest_rejected_total{reason="storage"}`).

### `POST /api/v1/metrics/prometheus` — scope `write`

Body: Prometheus text exposition format 0.0.4. Repeated `label=name=value`
query parameters add labels to every sample.

### `GET|POST /api/v1/query` — scope `read`

Prometheus-compatible instant query. Parameters: `query`, `time` (default
now). POST accepts `application/x-www-form-urlencoded`.

```json
{"resultType": "vector", "result": [{"metric": {"service": "checkout"}, "value": [1791779023.051, "88.41"]}]}
```

### `GET|POST /api/v1/query_range` — scope `read`

Parameters: `query`, `start` (default end − 1h), `end` (default now), `step`
(default 1m). Returns `resultType: "matrix"` with `values: [[ts, "v"], ...]`.
Limited by `metrics.max_range_steps`, `metrics.max_samples_per_query` and
`metrics.query_timeout`.

### `GET /api/v1/series` — scope `read`

Parameters: one or more `match[]` vector selectors, `start`, `end`. Returns
label sets.

### `GET /api/v1/labels`, `GET /api/v1/label/{name}/values` — scope `read`

Label names / values present in the head and blocks.

### `GET /api/v1/metrics/downsampled` — scope `read`

Reads downsampled blocks directly. Parameters: `match[]`, `start`, `end`,
`aggr` = `avg` (default), `count`, `sum`, `min`, `max`, `last`.

### `GET /api/v1/scrape/targets` — scope `read`

Configured scrape targets of the caller's tenant with `health`,
`last_scrape`, `last_duration_ms`, `last_error`, `samples_scraped`.

---

## OTLP/HTTP (JSON encoding) — scope `write`

| Method | Path | Body |
|---|---|---|
| POST | `/v1/traces` | `ExportTraceServiceRequest` |
| POST | `/v1/logs` | `ExportLogsServiceRequest` |
| POST | `/v1/metrics` | `ExportMetricsServiceRequest` |

`Content-Type` must be `application/json` (protobuf returns 415). Payloads
follow the OTLP JSON mapping: lowerCamelCase fields, hex `traceId`/`spanId`
(case-insensitive on input, stored lowercase), 64-bit integers as strings or
numbers, enum values as integers, attributes as `AnyValue`. Fixtures:
`internal/otlp/testdata/*.json`.

On success the response is the matching `Export*ServiceResponse`; when some
items were invalid it includes `partialSuccess`:

```json
{"partialSuccess": {"rejectedSpans": "1", "errorMessage": "span \"x\": traceId must be 32 lowercase hex characters and non-zero"}}
```

Errors use `google.rpc.Status` JSON: `{"code": 3, "message": "..."}`.

Mapping notes: `service.name` → span `service`, log field `service`, metric
label `job`; `service.instance.id` → metric label `instance`; metric names and
attribute keys have `.` replaced by `_`; monotonic cumulative sums get a
`_total` suffix; histograms become `_bucket{le}`, `_sum`, `_count`; summaries
become `{quantile}`, `_sum`, `_count`. Delta temporality and exponential
histograms are rejected as partial failures.

---

## Traces — scope `read`

### `GET /api/v1/traces`

| Parameter | Description |
|---|---|
| `service`, `operation` | Exact match on a span's service / name |
| `minDuration`, `maxDuration` | Span duration bounds |
| `attr` | Repeated `key=value`, matched against span then resource attributes |
| `error=true` | Only spans with error status |
| `start`, `end` | Bounds on trace start time |
| `limit` | Default 20, max `traces.max_search_results` |

A trace matches when one span satisfies all criteria. Results are newest
first: `traceId`, `rootService`, `rootOperation`, `startTimeUnixNano`,
`durationMs`, `spanCount`, `errorCount`, `services`.

### `GET /api/v1/traces/{traceID}`

The span tree: `roots[]` of `{span, depth, selfTimeMs, children[]}` plus
`spanCount`, `durationMs`, `services` and `warnings` (missing parents, broken
parent cycles). 404 if unknown.

### `GET /api/v1/services`

`{service: {operation: spanCount}}`.

### `GET /api/v1/dependencies`

Optional `start`, `end`. Edges `{parent, child, callCount, errorCount}`
derived from cross-service parent/child spans and from client/producer spans
carrying `peer.service` without an instrumented child.

---

## Alerting

### Rules

| Method | Path | Scope |
|---|---|---|
| GET | `/api/v1/alerts/rules` | read — rules with last evaluation, duration, error, active alert count, plus available channels |
| POST | `/api/v1/alerts/rules` | write — create or replace (by name); 201 |
| DELETE | `/api/v1/alerts/rules/{name}` | write — 204, 404 if unknown |

Metric rule:

```json
{"name": "HighErrorRate", "type": "metric",
 "expr": "sum by (service) (rate(http_requests_total{code=~\"5..\"}[5m])) > 1",
 "for": "2m", "labels": {"severity": "page"},
 "annotations": {"summary": "{{ $labels.service }} errors at {{ $value }}/s"},
 "channels": ["local-webhook"], "group_by": ["alertname", "service"]}
```

Every series returned by `expr` is an active alert instance. Log-count rule:

```json
{"name": "ErrorLogBurst", "type": "log_count", "query": "level:error | count by service",
 "window": "10m", "op": ">=", "threshold": 10, "channels": ["log"]}
```

`op` is one of `> >= < <= == !=`; each `count by` group meeting the threshold
is an instance. Omitting `channels` sends to all configured channels.
Annotations are Go templates with `$value` and `$labels`.

### Alerts

`GET /api/v1/alerts` (read): instances with `rule`, `labels`, `annotations`,
`fingerprint`, `state` (`pending|firing|resolved`), `value`, `activeAt`,
`firedAt`, `resolvedAt`, `silenced`.

### Silences

| Method | Path | Scope |
|---|---|---|
| GET | `/api/v1/silences` | read — with computed `status` (`pending|active|expired`) |
| POST | `/api/v1/silences` | write — 201 with assigned `id` |
| DELETE | `/api/v1/silences/{id}` | write — expires the silence; 204 |

```json
{"matchers": [{"name": "alertname", "value": "ErrorLogBurst"}, {"name": "service", "value": "pay.*", "isRegex": true}],
 "startsAt": "2026-10-12T10:00:00Z", "endsAt": "2026-10-12T12:00:00Z", "createdBy": "oncall", "comment": "maintenance"}
```

`isEqual: false` negates a matcher. At least one matcher must not match the
empty label set.

### Notifications

Webhook channels POST:

```json
{"version": "1", "tenant": "demo", "groupKey": "demo|alertname=ErrorLogBurst", "status": "firing",
 "receiver": "local-webhook", "groupLabels": {"alertname": "ErrorLogBurst"},
 "alerts": [{"status": "firing", "labels": {...}, "annotations": {...}, "startsAt": "...", "value": 248, "fingerprint": "30c1..."}]}
```

with `X-Obs-Signature: sha256=<hex HMAC-SHA256 of the body>` when the channel
has a secret. Network errors, 429 and 5xx are retried with exponential backoff
(`max_retries`); other 4xx responses are not retried.

---

## Administration

| Method | Path | Scope | Description |
|---|---|---|---|
| GET | `/api/v1/whoami` | read | Tenant, key ID (first 8 hex chars of the key digest), scopes |
| GET | `/api/v1/admin/stats` | admin | Per-tenant storage statistics and queue depths |
| POST | `/api/v1/admin/flush` | admin | Wait until all queued ingestion is persisted |
| POST | `/api/v1/admin/maintenance` | admin | Flush, then run one retention/compaction/downsampling pass; returns per-tenant reports |
