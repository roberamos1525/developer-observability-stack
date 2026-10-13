# Examples

| Path | Purpose |
|---|---|
| `smoke.sh` | End-to-end smoke test: builds binaries, starts `obsd`, a webhook receiver and a synthetic scrape target, sends load, queries logs/metrics/traces, fires an alert and stops everything |
| `config/obsd.local.json` | Local configuration (scrapes `127.0.0.1:9101`, webhook to `127.0.0.1:9095`, fast alert evaluation) |
| `config/obsd.compose.json` | Same configuration with container host names, used by `compose.yaml` |
| `webhook-receiver/` | Minimal alert webhook endpoint that verifies `X-Obs-Signature` and prints notifications |
| `payloads/logs.ndjson` | NDJSON log lines (`POST /api/v1/logs`) |
| `payloads/syslog.txt` | RFC 5424 / RFC 3164 / plain lines (`POST /api/v1/logs/syslog`) |
| `payloads/metrics.json` | Samples in the JSON format (`POST /api/v1/metrics`) |
| `payloads/exposition.txt` | Prometheus text format (`POST /api/v1/metrics/prometheus`) |
| `rules/high-error-rate.json` | Metric alert rule (error ratio per service) |
| `rules/error-log-burst.json` | Log-count alert rule |
| `rules/silence.json` | Silence with a regex matcher |

OTLP/JSON request examples live in `internal/otlp/testdata/` (they double as
test fixtures): `traces.json`, `logs.json`, `metrics.json`.

```bash
K="Authorization: Bearer demo-admin-key-123"
curl -s -H "$K" -H 'Content-Type: application/x-ndjson' --data-binary @examples/payloads/logs.ndjson 'localhost:8428/api/v1/logs?sync=true'
curl -s -H "$K" -H 'Content-Type: application/json' --data-binary @examples/rules/high-error-rate.json localhost:8428/api/v1/alerts/rules
```
