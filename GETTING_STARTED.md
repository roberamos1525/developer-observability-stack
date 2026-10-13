# Getting started

## Prerequisites

- Go 1.26 or newer (no C compiler needed; everything is pure Go)
- `curl` for the examples
- Optional: Docker (the `Dockerfile` and `compose.yaml` are provided but were
  not built on the development machine)

## Build

```bash
git clone <this repository> && cd developer-observability-stack
go build -o bin/ ./cmd/obsd ./cmd/loadgen ./examples/webhook-receiver
./bin/obsd -version
```

## Configure

Configuration comes from built-in defaults, an optional JSON file
(`-config path` or `OBS_CONFIG`) and `OBS_*` environment variables, in that
order of precedence. Unknown JSON fields are rejected and the whole
configuration is validated at startup (`obsd -check-config` validates without
starting).

Authentication is enabled by default and requires at least one API key:

```bash
export OBS_API_KEYS="demo:demo-admin-key-123:read+write+admin;ci:ci-writer-key-1:write"
```

To keep plaintext keys out of config files, store digests instead:

```bash
./bin/obsd -hash-key demo-admin-key-123   # prints the SHA-256 for "key_sha256"
```

See `.env.example` for all environment variables and
[docs/operations.md](docs/operations.md) for every configuration field.

## Run

```bash
OBS_API_KEYS="demo:demo-admin-key-123:read+write+admin" \
OBS_WEBHOOK_SECRET=s3cret \
OBS_DATA_DIR=./var/data \
./bin/obsd -config examples/config/obsd.local.json
```

The example config defines a webhook channel with
`"secret_env": "OBS_WEBHOOK_SECRET"`; startup fails if that variable is not
set. Without `-config` only a `log` channel exists and no secret is needed.

`examples/config/obsd.local.json` additionally scrapes a synthetic target on
`127.0.0.1:9101` and sends alerts to a webhook on `127.0.0.1:9095`. Start
those helpers in other terminals:

```bash
./bin/loadgen -serve-metrics 127.0.0.1:9101
WEBHOOK_SECRET=s3cret ./bin/webhook-receiver -addr 127.0.0.1:9095
```

The receiver verifies the `X-Obs-Signature` HMAC with the same secret.

Check health:

```bash
curl -s localhost:8428/healthz   # {"status":"ok"}
curl -s localhost:8428/readyz    # {"status":"ready"}
```

## Send data

```bash
K="Authorization: Bearer demo-admin-key-123"

# Logs: NDJSON, JSON arrays, syslog lines, OTLP
curl -s -H "$K" -H 'Content-Type: application/x-ndjson' --data-binary @examples/payloads/logs.ndjson \
     'localhost:8428/api/v1/logs?sync=true'
curl -s -H "$K" -H 'Content-Type: text/plain' --data-binary @examples/payloads/syslog.txt \
     localhost:8428/api/v1/logs/syslog
curl -s -H "$K" -H 'Content-Type: application/json' --data-binary @internal/otlp/testdata/logs.json \
     localhost:8428/v1/logs

# Metrics: own JSON, Prometheus text, OTLP
curl -s -H "$K" -H 'Content-Type: application/json' --data-binary @examples/payloads/metrics.json \
     localhost:8428/api/v1/metrics
curl -s -H "$K" --data-binary @examples/payloads/exposition.txt \
     'localhost:8428/api/v1/metrics/prometheus?label=source=push'

# Traces: OTLP/HTTP JSON
curl -s -H "$K" -H 'Content-Type: application/json' --data-binary @internal/otlp/testdata/traces.json \
     localhost:8428/v1/traces
```

Or generate a realistic mix of all three:

```bash
./bin/loadgen -api-key demo-admin-key-123 -duration 30s -rate 10 -batch 200
```

Ingestion is asynchronous (HTTP 202). Add `?sync=true` to wait until data is
persisted, or call `POST /api/v1/admin/flush` (admin scope) before querying in
scripts.

## Query

```bash
# Logs
curl -s -G -H "$K" localhost:8428/api/v1/logs/query \
     --data-urlencode 'q=level:error service:checkout "connection refused" | limit 20'
curl -s -G -H "$K" localhost:8428/api/v1/logs/query --data-urlencode 'q=status>=500 | since 1h | count by service'

# Metrics (Prometheus API shapes)
curl -s -G -H "$K" localhost:8428/api/v1/query \
     --data-urlencode 'query=histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))'
curl -s -G -H "$K" localhost:8428/api/v1/query_range \
     --data-urlencode 'query=sum by (service) (rate(http_requests_total[1m]))' \
     --data-urlencode "start=$(($(date +%s)-600))" --data-urlencode step=30s

# Traces
curl -s -G -H "$K" localhost:8428/api/v1/traces --data-urlencode service=payments --data-urlencode minDuration=100ms
curl -s -H "$K" localhost:8428/api/v1/traces/5b8efff798038103d269b633813fc60c
curl -s -H "$K" localhost:8428/api/v1/dependencies
```

## Alerting

```bash
curl -s -H "$K" -H 'Content-Type: application/json' --data-binary @examples/rules/error-log-burst.json \
     localhost:8428/api/v1/alerts/rules
curl -s -H "$K" localhost:8428/api/v1/alerts
curl -s -H "$K" -H 'Content-Type: application/json' --data-binary @examples/rules/silence.json \
     localhost:8428/api/v1/silences
```

## End-to-end smoke test

```bash
examples/smoke.sh
```

builds the binaries, starts the server, a webhook receiver and a synthetic
scrape target, generates 10 s of load, runs log/PromQL/trace queries, creates
an alert rule, waits for the signed webhook notification and stops
everything.

## Run the tests

```bash
go test ./... -count=1
```

See [docs/testing.md](docs/testing.md) for fuzzing and the integration suite.

## Docker (not verified locally)

```bash
docker compose up --build             # obsd + synthetic target + webhook receiver
docker compose --profile load up      # also runs the load generator
```

These files were written carefully but have not been built or run on the
development machine because Docker is not installed there.
