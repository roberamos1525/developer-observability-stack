# Security

## Reporting a vulnerability

Please report suspected vulnerabilities privately to the maintainer (Robert
Ramos) rather than opening a public issue. Include a description, affected
version/commit, reproduction steps and impact. You will receive an
acknowledgement and a plan for a fix.

## Security model

- **Authentication.** Every API route except `/healthz`, `/readyz` and
  `/metrics` requires an API key (`Authorization: Bearer` or `X-API-Key`).
  Keys are kept in memory only as SHA-256 digests; config files can contain
  `key_sha256` instead of plaintext (`obsd -hash-key`). Plaintext keys should
  be supplied through `OBS_API_KEYS`, never committed.
- **Authorization.** Keys carry `read`, `write` and/or `admin` scopes; admin
  implies the others and is required for `/api/v1/admin/*`.
- **Tenant isolation.** A key is bound to exactly one tenant; handlers only
  ever open that tenant's stores. Tenant IDs are restricted to
  `[a-z0-9][a-z0-9_-]{0,63}` because they become directory names (no path
  traversal). At most 256 tenants can be open.
- **Disabling auth** (`OBS_AUTH_ENABLED=false`) gives every caller admin
  rights on any tenant named in `X-Tenant-ID`. Only do this on a trusted
  loopback interface.
- **Input limits.** Request bodies are limited before and after gzip
  decompression; log messages, fields, labels, spans, query lengths, regex
  sizes, query nesting depth, samples per query, range steps, `count by`
  groups and scrape response sizes are bounded. Queries have timeouts.
- **Error hygiene.** Internal errors are logged with a request ID and
  reported to clients as a generic message.
- **Outbound requests.** Alert webhooks and scrape targets are configured by
  the operator (config file), not through the API, which avoids
  server-side request forgery via user input. Webhook payloads can be signed
  with HMAC-SHA256 (`X-Obs-Signature`) using a secret read from an
  environment variable.
- **Transport.** `obsd` serves plain HTTP and listens on loopback by default.
  Terminate TLS in a reverse proxy for any remote access.
- **Container.** The Dockerfile runs as an unprivileged user with a dedicated
  data volume (not built locally — see README).

## Known gaps

No built-in TLS, no key rotation API, no per-tenant rate limiting or quotas,
no audit log, and no encryption at rest. These are listed under Limitations
in the README and docs.
