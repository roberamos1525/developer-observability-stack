# Data model and on-disk formats

All files live under `data_dir`:

```
<data_dir>/
  alerting/state.json                     rules + silences of all tenants (JSON, atomic rename)
  tenants/<tenant>/
    logs/p-<partitionStartUnixSec>/<segmentID:016x>.log   framed log records
    logs/p-<partitionStartUnixSec>/<segmentID:016x>.idx   sealed segment index
    metrics/wal/<seq:08d>.wal             write-ahead log segments
    metrics/blocks/raw-<minT>-<maxT>/     raw block (meta.json + chunks.bin)
    metrics/blocks/ds<res>-<minT>-<maxT>/ downsampled block
    traces/spans-<partitionStartUnixSec>.ndjson
```

Integers are little-endian unless noted; `uvarint`/`varint` are Go
`encoding/binary` varints; `str` is `uvarint(len) bytes`. CRCs are CRC-32C
(Castagnoli).

## Logs

### Entry

```
Entry { Timestamp int64 (Unix ns), Level string, Message string, Fields map[string]string }
```

Limits: message 64 KiB (truncated at a UTF-8 boundary), 128 fields, field
names 1–128 bytes of `[A-Za-z0-9_.\-/]` (`level`, `message`, `msg`,
`timestamp` are reserved), values 8 KiB (truncated).

### Segment log (`.log`)

A sequence of records:

```
record  = uvarint(len(payload)) payload crc32c(payload):u32
payload = varint(timestamp) str(level) str(message) uvarint(nfields) { str(key) str(value) }   (keys sorted)
```

On open, a segment without a valid `.idx` is scanned record by record; the
first truncated/corrupt record ends the scan, the file is truncated to the
last good record and the index is rebuilt and written.

### Segment index (`.idx`)

```
"OBSLIDX1"
uvarint(n)  varint(minTs)  varint(maxTs)
n × uvarint(offset delta)            record start offsets
n × varint(timestamp - minTs)        per-ordinal timestamps (lets queries skip reads)
uvarint(nterms)  { str(term) postings }
uvarint(nfields) { str(field) uvarint(nvalues) { str(indexKey(value)) postings } }
crc32c(all preceding bytes):u32
postings = uvarint(count) uvarint(first) { uvarint(delta) }
```

- Terms are lowercase alphanumeric tokens of the message (letters/digits of
  any script plus `_`), at most 64 bytes; the same tokenizer is used by the
  query engine.
- `level` is indexed as a field. Values longer than 240 bytes are indexed as
  a truncated prefix followed by a NUL byte, so equality on them falls back to
  verification while prefix queries still use the index.
- Postings are entry ordinals within the segment, strictly increasing.

### Partitions, compaction and retention

Entries are routed to the partition containing their timestamp (default 1 h);
late data simply lands in an older partition. Each partition has at most one
active segment; at most 8 active segments exist per tenant (the least
recently written are sealed first). Partitions that have ended are sealed by
the maintenance loop. Compaction merges sealed segments smaller than half of
`segment_max_bytes` (at least `compact_min_segments` of them, total at most
`segment_max_bytes`) into one segment sorted by timestamp. Retention deletes
whole partitions whose end is older than `logs.retention`.

## Metrics

### Series and samples

A series is a sorted label set including `__name__`; samples are
`(T int64 Unix ms, V float64)`. Within a series samples must be strictly
increasing in time; an identical re-delivery of the last sample is ignored.

### Gorilla chunks

Chunks hold up to `chunk_max_samples` (≤ 65 535) samples:

```
u16 big-endian sample count, then a bit stream:
sample 0: varint(t) as bytes, 64 raw bits of v
sample 1: varint(t1 - t0) as bytes, XOR(v)
sample n: delta-of-delta of t in a bucket, XOR(v)
  '0'                  dod = 0
  '10'   + 14 bits     dod in [-8191, 8192]
  '110'  + 17 bits     dod in [-65535, 65536]
  '1110' + 20 bits     dod in [-524287, 524288]
  '1111' + 64 bits     any other value
XOR(v): '0'                                   v equals previous value
        '10' + bits within previous window    leading/trailing zeros fit
        '11' + 5b leading + 6b length + bits  new window (length 64 encoded as 0)
```

Timestamp arithmetic wraps like Go's `int64`, so decreasing or extreme
timestamps round-trip exactly; values round-trip bit for bit (NaN payloads,
±Inf, −0). A regular counter scraped every 15 s needs well under one byte per
sample (the test suite asserts < 120 bytes for 120 samples).

In the head, a new chunk is started when the current one is full or when a
sample crosses a `block_duration` boundary, so every chunk belongs to exactly
one block window.

### Write-ahead log

```
record = uvarint(len(rec)) rec crc32c(rec):u32
rec    = 0x01 uvarint(n) { uvarint(ref) uvarint(nlabels) { str(name) str(value) } }   series
       | 0x02 uvarint(n) { uvarint(ref) varint(t) u64(float bits) }                    samples
```

Only accepted samples are logged, after a series record for any new series.
On startup all segments are replayed in order; a torn tail in the newest
segment is truncated, corruption in an older segment is a fatal error.
Samples older than the newest block are skipped during replay. After every
block cut the WAL is checkpointed: a new segment containing the live head
(series + samples) is written and fsynced, then older segments are deleted.

### Blocks

```
meta.json:  {id, min_t, max_t (exclusive), resolution_ms, aggregates[], source, num_series, num_samples, num_chunks, size_bytes, created_at}
chunks.bin: "OBSBLK01" uvarint(nseries)
            { uvarint(nlabels) {str str}  for each aggregate: uvarint(nchunks) { varint(minT) varint(maxT) str(chunk bytes) } }
            crc32c:u32
```

Raw blocks have `aggregates = ["raw"]`. Blocks are written to `<id>.tmp`
and renamed. A window `[t, t+block_duration)` is cut once the head extends at
least half a block beyond its end; afterwards samples older than the newest
block's end are rejected as too old.

### Downsampling and retention

Raw blocks older than `downsample_after` are aggregated into
`downsample_resolution` buckets with five aggregates per bucket: `count`,
`sum`, `min`, `max`, `last` (each stored as its own Gorilla chunk series;
the sample timestamp is the last raw timestamp in the bucket). Raw blocks
expire after `raw_retention`, downsampled ones after `downsampled_retention`.
Selections fall back to the `last` aggregate for time ranges older than the
oldest raw data; all aggregates (and the derived `avg`) are available from
`/api/v1/metrics/downsampled`.

## Traces

Spans follow the OpenTelemetry model:

```
Span { traceId (32 hex), spanId (16 hex), parentSpanId, traceState, name, kind (0-5), service,
       startTimeUnixNano, endTimeUnixNano, attributes map, resource map, scope,
       status {code 0 unset|1 ok|2 error, message}, events[], links[] }
```

Validation: non-zero lowercase hex IDs of the right length, a span is not its
own parent, a 1–1024 byte name, `0 < start <= end`, valid kind, at most 256
attributes / 1024 events / 128 links. Duplicate `(traceId, spanId)` pairs are
ignored and traces are capped at `max_spans_per_trace` spans.

Spans are appended as JSON lines to the partition file of their start time
and indexed in memory. Retention drops traces whose last span ended before
the cutoff and deletes partition files that ended before it.

## Alerting state

`alerting/state.json` holds, per tenant, the rule definitions and silences.
It is rewritten atomically (temp file + rename) on every change. Alert
instance state (pending/firing/resolved) and notification groups live only
in memory.
