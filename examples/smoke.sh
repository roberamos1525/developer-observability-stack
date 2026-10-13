#!/usr/bin/env bash
# End-to-end smoke test: builds the binaries, starts obsd with a webhook
# receiver and a synthetic scrape target, generates load, queries every
# signal, fires an alert to the webhook and stops everything.
#
# Usage: examples/smoke.sh            (from the repository root)
# Requires: go, curl.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${SMOKE_DIR:-$(mktemp -d)}"
KEY="smoke-admin-key-0001"
SECRET="smoke-webhook-secret"
BASE="http://127.0.0.1:8428"
EXE=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) EXE=".exe" ;; esac

cd "$ROOT"
echo "== building into $WORK/bin"
go build -o "$WORK/bin/" ./cmd/obsd ./cmd/loadgen ./examples/webhook-receiver

PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

WEBHOOK_SECRET="$SECRET" "$WORK/bin/webhook-receiver$EXE" -addr 127.0.0.1:9095 >"$WORK/webhook.log" 2>&1 &
PIDS+=($!)
"$WORK/bin/loadgen$EXE" -serve-metrics 127.0.0.1:9101 >/dev/null 2>&1 &
PIDS+=($!)
OBS_DATA_DIR="$WORK/data" OBS_WEBHOOK_SECRET="$SECRET" OBS_API_KEYS="demo:$KEY:read+write+admin" \
  "$WORK/bin/obsd$EXE" -config examples/config/obsd.local.json >"$WORK/obsd.log" 2>&1 &
PIDS+=($!)

for _ in $(seq 1 50); do
  curl -sf "$BASE/readyz" >/dev/null && break
  sleep 0.2
done
curl -sf "$BASE/readyz" >/dev/null || { echo "obsd did not become ready"; cat "$WORK/obsd.log"; exit 1; }

auth=(-H "Authorization: Bearer $KEY")
echo "== generating load for 10s"
"$WORK/bin/loadgen$EXE" -url "$BASE" -api-key "$KEY" -duration 10s -rate 10 -batch 200 -seed 42
curl -sf -X POST "${auth[@]}" "$BASE/api/v1/admin/flush" >/dev/null

echo "== log query"
curl -sf -G "${auth[@]}" "$BASE/api/v1/logs/query" --data-urlencode 'q=level:error "connection refused" | count by service'
echo
echo "== PromQL: request rate by service"
curl -sf -G "${auth[@]}" "$BASE/api/v1/query" --data-urlencode 'query=sum by (service) (rate(http_requests_total[30s]))'
echo
echo "== PromQL: p95 latency"
curl -sf -G "${auth[@]}" "$BASE/api/v1/query" --data-urlencode 'query=histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[30s])))'
echo
echo "== trace search"
SEARCH=$(curl -sf -G "${auth[@]}" "$BASE/api/v1/traces" --data-urlencode service=payments --data-urlencode error=true --data-urlencode limit=1)
echo "$SEARCH"
TRACE_ID=$(echo "$SEARCH" | grep -o '"traceId":"[0-9a-f]*"' | head -1 | cut -d'"' -f4)
echo "== trace $TRACE_ID"
curl -sf "${auth[@]}" "$BASE/api/v1/traces/$TRACE_ID" | head -c 400
echo
echo "== service dependencies"
curl -sf "${auth[@]}" "$BASE/api/v1/dependencies"
echo

echo "== creating alert rule and waiting for the webhook"
curl -sf "${auth[@]}" -H 'Content-Type: application/json' -X POST "$BASE/api/v1/alerts/rules" \
  --data-binary @examples/rules/error-log-burst.json >/dev/null
for _ in $(seq 1 30); do
  grep -q '"status":"firing"' "$WORK/webhook.log" 2>/dev/null && break
  sleep 0.5
done
if grep -q '"signature":"valid"' "$WORK/webhook.log"; then
  echo "webhook received $(grep -c notification "$WORK/webhook.log") signed notification(s)"
else
  echo "no alert notification received"; cat "$WORK/obsd.log"; exit 1
fi
echo "== smoke test passed (data in $WORK)"
