# Contributing

## Development setup

- Go 1.26+ (pure Go; no CGO required)
- `go build ./...` builds everything; binaries: `go build -o bin/ ./cmd/obsd ./cmd/loadgen`

## Before sending a change

```bash
gofmt -l .            # must print nothing
go vet ./...
go test ./... -count=1
go mod tidy           # go.mod/go.sum must not change
```

If you touch a parser, an encoder or the query planner, also run the
relevant fuzz target for a minute (see [docs/testing.md](docs/testing.md)).
Run `examples/smoke.sh` for changes that affect the HTTP API or startup.

## Code guidelines

- Keep package boundaries: storage engines (`logs/store`, `tsdb`, `traces`)
  must not import the HTTP layer; query languages (`logs/query`, `promql`)
  must not import storage implementations (they use small interfaces).
- Return `*errs.Error` with the right `Kind` from anything reachable by an API
  call; never leak internal error details to clients.
- Validate every external input at the boundary (size, syntax, ranges) and
  keep hard limits configurable where reasonable.
- On-disk formats are versioned by magic strings (`OBSLIDX1`, `OBSBLK01`);
  changing a format requires a new magic and a migration note in
  `CHANGELOG.md`.
- Tests: deterministic seeds and injected clocks; integration tests go to
  `tests/integration`; a bug found by fuzzing gets a regular regression test.
- Comments explain *why*; exported identifiers have doc comments.

## Commit messages

Use the imperative mood (`Add downsampled query endpoint`) and reference the
affected package in the body when the subject is generic.
