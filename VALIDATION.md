# Go SDK validation

Final verification passed: `go test -race -count=1 ./...`, `go vet ./...`, the
complete integration suite (370.733 seconds), and all three collector/API
pipeline modes: raw, approved policy, and live sampling/quotas.

## Scope and environment

All tests use this local SDK and disposable local services. No production data,
credentials, policies, deployments, or YOM worktrees are changed. The collector
and API are the existing sibling implementations, not Go-specific replacements.

Executed toolchain: Go 1.27.3, macOS arm64; instrumented Linux arm64 binary also
executed in Alpine 3.20. Compiler engine: OpenTelemetry otelc v1.0.0. The module
requires Go 1.26+, but the executed version is not a full Go-version matrix.

| Dependency | Tested version |
| --- | --- |
| PostgreSQL | 16 |
| pgx / pgxpool | v5.7.6 |
| Bun / pgdialect | v1.2.15 |
| Redis | 7.4, standalone, three-node Cluster, primary/replica/Sentinel |
| go-redis UniversalClient | v9.12.1 |
| rueidis | v1.0.68 |
| Kafka | Confluent Kafka 7.6.1, real broker |
| kafka-go | v0.4.49 |
| franz-go | v1.19.5 |
| MongoDB | 7.0 replica set |
| MongoDB Go drivers | v1.17.4 and v2.3.1 |
| ClickHouse pipeline fixture | 25.6 |

## Executable coverage

| Test | Assertions |
| --- | --- |
| `TestCompiledSemanticsRaceAndBaseline` | Instrumented results match the ordinary Go binary; concurrent scopes, closures, generics, defer/named returns, panic/recover, return identity, build tags, original source locations, exclusions, and unchanged application files |
| `TestTestCommandCapturesTestOnlyCode` | `repro-go test` instruments functions defined only in test files |
| `TestLinuxCrossCompiledRuntime` | Cross-compiled instrumented runtime executes in Linux and matches eight concurrent baseline results |
| `TestAutomaticHTTP` | Correlation and parent links, redirects, streaming, partial reads, body limits, preserved request/body identity and response interfaces, protocol-upgrade writes, panic behavior, late body reads and background work |
| `TestShutdownDeadlineAndOverflow` | Already-admitted work survives shutdown, drain deadlines and scope exhaustion produce explicit incomplete evidence, and application responses remain successful |
| `TestAutomaticDatabaseCapture` | SQL/Bun queries, prepared statements, transactions, rollback and errors; pgx rows, batches and CopyFrom; Redis exact/cache results; both MongoDB drivers, cursor consumption and transactions; evidence counts match manifests |
| `TestRedisClusterAndSentinel` | Both Redis clients route through actual topology discovery and capture all twelve cross-client business values; both original clients survive a forced Sentinel promotion and capture the post-failover value; transaction, missing-key and cancellation behavior preserved |
| `TestAutomaticKafka` | Real asynchronous broker acknowledgements, kafka-go and franz-go propagation, callback and typed-range consumers, exact business results and record identity, matched completion manifests; sixteen handled asynchronous publish rejections retain their errors; live admission for messages with no upstream session |
| `TestCaptureOverheadAndCollectorFailureIsolation` | Ordinary, unsampled, captured and collector-outage modes preserve business checksums; unsampled mode uploads nothing; terminal delivery failures are reported |
| `internal/capture` tests | Immutable targeting config, admission timeout and denial, mutation-aware value references, exact large integers and math/big values, returned errors distinct from panics, cycles, infrastructure projections, structural limits, shared trace/database memory budgets |
| `internal/delivery` tests | Stable bytes/event IDs on retry, quota refusal, queue pressure, shutdown, no credential forwarding on redirect, and complete delivery receipts required |
| `internal/privacy` tests | Approved policy behavior, authenticated/scoped fetch, fail-closed malformed policy handling, bounded evaluation and recovery after timeout |

Database, Kafka, HTTP, topology and semantic fixture applications are built with
`-race`. Performance measurements deliberately use non-race binaries. Linux
cross-compilation is a separate non-race test. Tests exit unsuccessfully on a
fixture race, assertion failure or nonzero application exit.

## Bugs caught during validation

- Goroutine propagation initially performed capture allocation on the runtime
  scheduler stack. Propagation now prepares the context before that stack switch,
  and completion runs before the goroutine's race context is destroyed.
- Asynchronous Kafka producer return and acknowledgement raced over completion.
  Acknowledgements now own nonempty async completion; rejected publishes cannot
  become successful merely because `Produce` returned normally.
- Shutdown previously disabled hooks for work already admitted. Existing scopes
  now continue capturing during drain, while new roots are refused.
- Enabled-but-missing policies and incomplete ingest receipts now fail explicitly
  instead of becoming fallback/raw success or confirmed delivery.
- Go error returns now populate error evidence without claiming a panic.
  Standard `math/big` values no longer serialize as empty private-field structs.

## Unchanged collector/API pipeline

`test/pipeline.cjs` starts isolated MongoDB, ClickHouse, PostgreSQL and Redis.
It uses the real collector ingest handler, real API authentication/admission,
real metering, reconstruction, timeline and trace-value reader endpoints.

The collector test injects Kafka's oversized-message error and exercises the
existing durable ClickHouse fallback. It does not pretend to exercise the
collector's Kafka-consumer path. SDK Kafka transport has separate broker tests.

Passed pipeline assertions:

- Raw captures: full response values, including a roughly 14 KB business
  document and database results, remain exact after storage and value-reference
  resolution. Requests, traces, database evidence and timeline entries exist.
- Approved policy: business values remain exact while protected email and
  infrastructure secrets do not appear in stored values. The SDK fetches and
  applies the approved policy; this is not merely a completion-state check.
- Live: zero sampling, expired targeting and nonmatching groups upload nothing;
  actor/group targets and full sampling capture; concurrent requests share the
  admitted session; six background results after HTTP completion survive.
- Live continuation retains the admitted decision after the sample rate changes.
  Three admitted actors produce three billable segments, not one per request.
- Exhausting the isolated test tenant's allowance prevents additional captures
  while business responses still succeed. Collector byte usage is metered.

Example session identifiers from successful disposable runs:

| Run | Session |
| --- | --- |
| Raw MongoDB | `S_1793390729259_go_raw_mongo` |
| Raw pgx | `S_1793390729259_go_raw_pgx` |
| Raw Redis | `S_1793390729259_go_raw_redis` |
| Raw SQL | `S_1793390729259_go_raw_sql` |
| Approved policy | `S_1793390816502_go_policy_redis` |
| Live actor target | `S_1793390882675_8716ba78` |
| Live label target | `S_1793390882812_3155be4f` |
| Live concurrent/continued | `S_1793390882885_ee5b7213` |

These IDs are audit references to test output, not production sessions available
in the hosted UI. Containers and their data are removed after each run.

## Measured overhead

One local non-race run, 250 sequential HTTP requests with five business-function
calls per request. All four modes produced the same checksum, 1,125,000.

| Mode | p50 | p95 | Observed heap | Upload attempts |
| --- | ---: | ---: | ---: | ---: |
| Ordinary Go binary | 70 us | 112 us | 2,086,592 bytes | 0 |
| Instrumented, unsampled | 78 us | 158 us | 2,131,816 bytes | 0 |
| Captured | 249 us | 743 us | 2,159,928 bytes | 500 |
| Collector returns 503 | 195 us | 518 us | 3,041,656 bytes | 11; delivery failure reported |

This is a small loopback regression measurement, not a production throughput,
peak-memory or latency guarantee. Privacy-policy workload and live admission
network latency are not included in this microbenchmark.

## Reproduce

Put the desired Go toolchain on `PATH`. Set `REPRO_OTELC` only when using an
already-installed pinned engine; otherwise the wrapper installs it on first use.

The development-machine run used these temporary tool locations:

```sh
export PATH="/private/tmp/go/bin:$PATH"
export GOPATH=/private/tmp/repro-go-path
export GOCACHE=/private/tmp/repro-go-build-cache
export REPRO_OTELC=/private/tmp/repro-go-tools/otelc
```

```sh
# go-sdk
go build -o bin/repro-go ./cmd/repro-go
go test -race ./...
go vet ./...

# go-sdk/test/integration; requires Docker
go test -v -count=1 -timeout=25m ./...

# go-sdk; sibling API and collector dependencies must be installed
node test/pipeline.cjs
REPRO_PIPELINE_POLICY=1 node test/pipeline.cjs
REPRO_PIPELINE_LIVE=1 node test/pipeline.cjs
```

Run the container suites sequentially on an 8 GB Docker VM, especially when
other local Repro stacks are running. An overlapping pipeline run caused Docker
contention and a Kafka connection timeout; that run was not accepted as passing.
Do not disable the race detector or weaken evidence assertions to hide a timeout.

## Boundaries

### Meridian follow-up (2026-10-31)

The sibling `repro-live-demo/go-backend` now exercises this SDK against Meridian's
unchanged browser UI, real local databases/brokers, and the remote recording
pipeline. Its [validation report](../repro-live-demo/go-backend/VALIDATION.md)
documents 12 passing contract/browser tests and production session
`S_1793455805734_ywj1a2py`, verified complete with exact replay evidence.
This follow-up adds automatic amqp091-go publishing/confirmation and typed
delivery-handler capture. RabbitMQ channel/connection internals are represented
as runtime-infrastructure markers. The previously recorded SDK-wide matrix above
has not been rerun in its entirety for this follow-up; race tests, vet, and the
Meridian integration suite passed.

### General limits

- Validation covers the listed operations and versions, not every API of each
  driver, every Go release, or every OS/architecture.
- Application-function instrumentation applies to the main Go module.
  Third-party internals are not recursively traced. Supported standard-library
  and driver boundaries have dedicated hooks.
- Automatic session roots are HTTP requests, Kafka consumers, and supported
  typed RabbitMQ delivery handlers. Correlated
  goroutines inherit evidence context; an unrelated arbitrary timer/poll loop
  does not acquire a session merely because its process called `Init`.
- Database results are observed when the application consumes them. No extra
  reads are issued to invent before/after documents or unconsumed rows.
- Viewer API contracts were exercised in the isolated suite. The Meridian
  follow-up additionally drives Chromium and replays a production-downloaded
  recording; it does not drive an external MCP client.
- No publishing, deployment, production-scale validation or production billing
  measurement was performed for this SDK.
