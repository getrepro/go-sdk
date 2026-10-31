# Go SDK implementation record

## Contract

- Initialize once; compile using `repro-go build`. No manually wrapped handlers,
  clients, functions, or jobs.
- Reuse the existing Repro collector envelope, request/trace/DB payloads, capture
  manifests, privacy policy, sampling, and quota contracts. No API changes.
- Keep application return identities, panic/recover semantics, cancellation,
  transaction behavior, and lazy result consumption unchanged.
- Never mark missing, denied, truncated, or undelivered evidence complete.
- Keep existing repositories and YOM worktrees unchanged.

## Build architecture

Pinned OpenTelemetry compile-time engine: `go.opentelemetry.io/otelc` v1.0.0,
upstream commit `9068e2cff8029922058dcc7d2d18708fc8c697d2` (Apache-2.0).
Repro supplies its own runtime, serialization, protocol, and instrumentation.
Build-time generated sources stay outside customer sources. Runtime library
hooks and application function instrumentation are independently tested.

## Verified behavior

- Compiler: functions/methods, closures, generics, defer/named returns,
  panic/recover, correlated goroutines, build tags, original source locations,
  unchanged customer files, and instrumented test-only functions.
- Runtime: concurrent scopes, inherited context, bounded capture/delivery,
  stable retry bytes and ingestion IDs, flush/shutdown and explicit failures.
- HTTP: server/client propagation, streaming, redirects, errors, partial reads,
  body limits, protocol upgrades, and late background work.
- SQL/Bun and pgx/pgxpool: queries/results, statements, transactions,
  lazy rows, batch parameters, CopyFrom, rollback and cancellation.
- go-redis and rueidis: commands/results, pipelines, transactions, cache hits,
  standalone/Sentinel/Cluster discovery, existing clients surviving Sentinel
  failover, errors and cancellation.
- kafka-go and franz-go: broker-acknowledged asynchronous publishing, rejected
  delivery evidence, callback/range consumers, exact values and correlation,
  and live admission without upstream session headers.
- MongoDB v1/v2: operations, decoded results, cursor batches, transactions,
  errors and preserved application monitoring.
- Policies: embedded Node-compatible evaluator, approved policy fetch and
  application, fail-closed malformed policy handling, exact retained values.
- Live: existing API admission, sampling, actor/label targeting, continuation,
  quota refusal without business-response failure, and metered ingestion.
- Pipeline: unchanged collector -> ClickHouse -> API reconstruction -> MongoDB
  -> viewer timeline and full-value reader APIs.

See [VALIDATION.md](VALIDATION.md) for commands, versions, assertions and measured
overhead. The final complete integration suite passed in 370.733 seconds after
the acknowledgement-race fix, with race-enabled fixture applications. Root unit
tests passed with `-race -count=1`; `go vet ./...` also passed.
The final raw, approved-policy and live/quota collector/API runs all passed on
the same implementation. Their disposable session IDs are in the report.

## Explicit boundaries

The tests do not establish universal driver/version coverage, production-scale
capacity, browser replay behavior or an external MCP-client run. No API changes,
publishing or deployment are part of this implementation. Automatic roots are
HTTP and Kafka; correlated application goroutines inherit those roots. Unrelated
polling loops do not automatically become jobs with invented business boundaries.
