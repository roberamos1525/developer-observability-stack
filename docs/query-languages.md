# Query languages

## Log query language

### Grammar

```
query    = [ or ] { "|" stage }
or       = and { ("OR" | "||") and }
and      = unary { [ "AND" | "&&" ] unary }        juxtaposition means AND
unary    = ("NOT" | "-") unary | primary
primary  = "(" or ")" | field op value | word | "quoted phrase"
op       = ":" | "=" | "!=" | ">" | ">=" | "<" | "<=" | "=~" | "!~"
stage    = "limit" N | "sort" ("asc" | "desc") | "since" DURATION
         | "between" TIME TIME | "count" [ "by" field { "," field } ]
```

Keywords are upper case (`AND`, `OR`, `NOT`); lower-case words are search
terms. Strings may use `"…"` or `'…'` with `\"`, `\\`, `\n`, `\t` escapes.

### Semantics

| Expression | Meaning |
|---|---|
| `refused` | message contains the token `refused` (case-insensitive) |
| `retr*` | message contains a token starting with `retr` |
| `"connection refused"` | the token sequence `connection refused` appears consecutively |
| `connection-refused` | a word with punctuation is treated as a phrase |
| `service:api` / `service=api` | field equals `api` (case-sensitive) |
| `service:pay*` | field starts with `pay` |
| `trace_id:*` | field exists |
| `user!=bob` | field differs **or is missing** |
| `status>=500`, `latency_ms<250` | numeric comparison when both sides are numbers, otherwise lexical |
| `host=~"web-[0-9]+"` / `!~` | RE2 regular expression (unanchored) |
| `level:warning` | `level` values are normalised (`warning`→`warn`, `err`→`error`, `crit`→`fatal`, …) |
| `message:"disk full"` | same as a bare phrase |
| `NOT x`, `-x` | negation |

Stages: `limit N` overrides the API `limit`; `sort asc|desc` (default newest
first); `since 15m` restricts to the last 15 minutes; `between T1 T2` sets an
absolute range (RFC 3339 or epoch); `count` returns the number of matches and
`count by f1, f2` returns grouped counts (missing fields group as `""`, at
most 10 000 groups).

Examples:

```
level:error service:checkout "connection refused" | limit 50
(status>=500 OR level:fatal) AND NOT path:/healthz | since 1h
service:api* latency_ms>1000 | sort asc
| count by service, level
app:nginx sd.req_32473.status:499 | between "2026-10-12T00:00:00Z" "2026-10-12T06:00:00Z"
```

### Execution

1. **Planning.** For each segment the AST is mapped onto the inverted index:
   terms and field equality/prefix produce exact posting lists; phrases
   intersect their tokens' postings (inexact: verified later); `AND`
   intersects, `OR` unions, `NOT` complements exact children; comparisons and
   regexes mean "all candidates, verify". A property test cross-checks the
   planner against brute-force matching on random data and queries.
2. **Pruning.** Segments outside the time range are skipped using their
   min/max timestamps, as are segments that cannot improve the current top-K
   result. Per-ordinal timestamps let the executor skip out-of-range or
   too-old candidates without reading them.
3. **Verification and merge.** Candidates are read (one `ReadAt` per entry,
   CRC checked) and, if the plan was not exact, checked against the AST. A
   bounded heap keeps the best `limit` entries across segments.

Response `stats` report partitions/segments scanned and pruned, candidates,
entries read and matches; `truncated` signals that more matches may exist.

## PromQL subset

### Supported

- **Selectors:** `name{label="v", l2!="v", l3=~"re", l4!~"re"}`, `{__name__=~"…"}`,
  range selectors `x[5m]`, `offset 5m` (also negative). Regexes are fully
  anchored. A selector needs at least one matcher that does not match the
  empty string.
- **Literals:** numbers (including `Inf`, `NaN`, exponent notation), strings
  (`"…"`, `'…'`, `` `…` ``), durations `ms s m h d w` and combinations (`1h30m`).
- **Operators** (precedence low → high): `or`; `and`, `unless`;
  `== != > < >= <=` (filtering, or `bool` to return 0/1); `+ -`; `* / %`;
  `^` (right associative). Unary minus binds looser than `^` (`-2^2 = -4`).
  Vector/vector operations match one-to-one on all labels except `__name__`,
  or `on(…)` / `ignoring(…)`. Arithmetic drops the metric name; comparison
  filters keep it.
- **Aggregations:** `sum avg min max count stddev topk bottomk quantile` with
  `by (…)` or `without (…)` before or after the arguments.
- **Functions:** `rate irate increase delta` (Prometheus extrapolation and
  counter-reset handling), `avg_over_time sum_over_time min_over_time
  max_over_time count_over_time last_over_time`, `histogram_quantile`
  (classic `le` buckets, monotonicity fix, highest finite bound for the +Inf
  bucket), `abs ceil floor sqrt exp ln clamp_min clamp_max time vector scalar
  absent`.
- **Instant and range queries** with Prometheus response shapes.

### Not supported

Subqueries (`x[5m:1m]`), `group_left`/`group_right` (many-to-one matching
returns an error), the `@` modifier, `label_replace`/`label_join`,
`predict_linear`, `holt_winters`, `sort*`, `count_values`, native histograms,
exemplars, recording rules and staleness markers (a series disappears from
instant vectors only after `lookback_delta`).

### Execution

Queries are parsed with a Pratt parser and type checked (scalar, instant
vector, range vector, string). Before evaluation the engine collects every
selector in the AST and loads the series once for the whole window
`[start − range − offset − lookback, end − offset]`, enforcing
`max_samples_per_query`. Each step then evaluates the AST against these
in-memory series. Errors carry kinds: syntax/type/limit/matching errors are
`invalid_argument`, cancellations are `timeout`, storage failures `internal`.
