# Repro Go SDK

Initialize once and build with `repro-go`. The build instruments application
functions and supported library calls. Do not replace your HTTP handlers,
database clients, transactions, or Kafka processing code with Repro wrappers.
The SDK uses the existing Repro collector and API contracts.

## Install from the private repository

Requires Go 1.26 or newer and read access to `getrepro/go-sdk`. Authenticate Git
with GitHub first (for example, `gh auth login` then `gh auth setup-git`). Keep any
existing `GOPRIVATE` entries when adding this module:

```sh
go env -w GOPRIVATE=github.com/getrepro/go-sdk
go get github.com/getrepro/go-sdk@v0.1.0
go install github.com/getrepro/go-sdk/cmd/repro-go@v0.1.0
```

Add `$(go env GOPATH)/bin` to `PATH`. No local module replacement is needed.

Add initialization before starting servers or consumers:

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    repro "github.com/getrepro/go-sdk"
)

func main() {
    sdk, err := repro.Init(repro.Config{
        TenantID:    os.Getenv("REPRO_TENANT_ID"),
        AppID:       os.Getenv("REPRO_APP_ID"),
        AppSecret:   os.Getenv("REPRO_APP_SECRET"),
        AppName:     "orders-api",
        ServiceName: "Orders API",
        IngestBase:  "https://YOUR-COLLECTOR",
        Mode:        "staging",
        Privacy: repro.PrivacyConfig{
            APIBase:     "https://YOUR-API/api",
            Environment: "dev",
        },
    })
    if err != nil {
        // Decide your startup policy. The SDK returns the error, not os.Exit.
        log.Printf("Repro capture unavailable: %v", err)
    } else {
        defer func() {
            ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
            defer cancel()
            if err := sdk.Shutdown(ctx); err != nil {
                log.Printf("Repro delivery incomplete: %v", err)
            }
        }()
    }
    // Start the application's existing server/consumer and graceful shutdown.
}
```

```sh
go mod tidy
repro-go build -o app ./cmd/server
./app
```

Use `repro-go test` for instrumented test binaries. Ordinary `go build` does not
instrument; `Init` explicitly reports that mismatch. The wrapper downloads the
pinned OpenTelemetry compiler engine on first use. `REPRO_OTELC` can point to an
already-installed `otelc v1.0.0` executable for offline builds. No OpenTelemetry
collector or hosted LLM is involved.

## Modes

### Staging

`Mode: "staging"` captures requests correlated with a browser recording and
messages carrying that correlation. Use the existing browser SDK for recording,
rrweb, actions, and the session lifecycle. The Go SDK does not invent unrelated
sessions for every uncorrelated HTTP request.

### Live

```go
rate := 0.1
config.Mode = "live"
config.ActorLabels = map[string]string{"team": "operations", "region": "eu"}
config.Live = repro.LiveConfig{
    APIBase: "https://YOUR-API/api",
    Policy: repro.LivePolicy{SampleRate: &rate},
    DefineActorID: func(r *http.Request) string {
        // Use a trusted identity available before the application handler runs.
        // Do not trust a public client-supplied identity header without validation.
        return trustedIdentity(r)
    },
}
```

`trustedIdentity` is the application's identity lookup, not a Repro API. If you
omit the callback, `ActorID` identifies the service (default: `AppID`). This is
service-level sampling, not per-user sampling. A blank resolved actor skips
capture. Correlated browser requests retain their existing session.

The existing API decides sampling, three-minute segment rollover, and quota
admission. The SDK does not maintain a separate billing counter. Backend work
already admitted stays with its original request/session after the response.

Update targeting at runtime without reinitializing:

```go
zero := 0.0
err := sdk.SetLiveCapture(repro.LivePolicy{
    SampleRate: &zero,
    Targets: []repro.Target{
        {ActorIDs: []string{"customer-42"}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()},
        {Labels: map[string]string{"team": "support"}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()},
    },
})
```

Existing admitted chains retain the server's sampling decision. Quota refusal,
admission errors, and ingest outages do not alter the handler's result. Live
admission can add network latency, bounded by `AdmissionTimeout`; it is not
zero-overhead or offline admission.

Kafka consumers also admit messages without upstream Repro headers in live mode,
using the configured service `ActorID`. In staging, those messages remain
uncaptured. Already-correlated Kafka messages keep their original session.

## Privacy and exclusions

Policies are fetched at initialization using app credentials, service name, and
environment. The embedded evaluator is bundled from the Node SDK's existing
policy implementation. It runs on the bounded delivery worker, before value
deduplication and upload. No Node.js runtime is required by the application.
An enabled-but-missing/malformed policy is an initialization error, not a silent
raw fallback. Evaluation failures withhold the affected values and report failed
privacy application. An explicitly disabled policy uses the evaluator's fallback.

For deliberate raw testing, `CaptureRaw: true` bypasses policies. It does **not**
remove infrastructure projections, structural limits, or memory limits. Never
enable raw capture assuming secrets will still be policy-redacted.

```go
config.Tracing = repro.TracingConfig{
    Exclude: []*regexp.Regexp{
        regexp.MustCompile(`/internal/tenant/database\.go$`),
    },
    ExcludeFunctions: []*regexp.Regexp{
        regexp.MustCompile(`\.loadInfrastructureConfig$`),
    },
}
```

Exclusions skip function evidence, not the execution of the function. Library
instrumentation remains active inside the request. Files use normalized `/`
paths. Use a privacy policy, not function exclusion, to redact a value everywhere.

## Captured evidence

| Surface | Evidence |
| --- | --- |
| Application Go functions | Entry/exit, arguments, return values, returned errors (distinct from panics), panic evidence, parent span, original file/line; methods, closures, variadics, generics, and correlated goroutines |
| `net/http` | Incoming request/response and outgoing client calls, propagation, status, headers, bodies read/written by the application, timing |
| `database/sql`, Bun | SQL and parameters, scanned rows, command results, statements, transactions, database errors |
| pgx v5 / pgxpool | Queries, decoded rows, batches and parameters, CopyFrom input, results and errors |
| go-redis v9 | Commands and results, pipelines and transactions; UniversalClient standalone, Sentinel failover, Cluster |
| rueidis | Commands, results, multi-command execution, cache hits; standalone, Sentinel, Cluster |
| kafka-go / franz-go | Produce/consume payloads and correlation, broker acknowledgements, callbacks, iterators, typed record-slice loops, business traces |
| amqp091-go | RabbitMQ publishing, deferred broker confirmations, correlation and application handlers receiving a typed `amqp.Delivery` argument |
| MongoDB drivers v1/v2 | Collection operations and arguments, decoded documents, cursor batches, transaction boundaries and errors |

Driver/pool/context infrastructure is represented by a typed semantic marker,
not recursively copied. Results are observed as the application consumes them;
Repro does not issue extra database reads or drain unread HTTP/database streams.
Incomplete reads and configured limits carry explicit omission metadata.
RabbitMQ consumer capture starts at a compiled application function receiving
`amqp.Delivery`; a bare inline channel-receive loop without such a handler is not
claimed as covered. The SDK observes deferred confirmations without replacing
or delaying the application's publish call. Non-confirming publishes are marked
`confirmed: false`, not represented as acknowledged delivery.
Deduplication uses protected value content, not pointer identity, so a mutated
argument produces a new captured value. Integers outside JavaScript's exact
range use tagged decimal values. `math/big` integers, rationals and floats have
explicit value representations; float precision is retained for round-tripping.

## Configuration reference

| Option | Default / meaning |
| --- | --- |
| `TenantID`, `AppID`, `AppSecret` | Required Repro app identity; keep the secret on the backend |
| `AppName`, `ServiceName` | Service attribution; `ServiceName` defaults to `AppName` |
| `IngestBase` | Required collector origin; SDK appends `/v1/ingest/events` |
| `Mode` | `staging` or `live`; default `staging` |
| `ActorID`, `ActorLabels` | Service identity and labels; default actor is `AppID` |
| `CaptureRaw` | Default false; explicit privacy bypass |
| `Privacy.APIBase`, `.Environment` | API prefix and policy environment; environment defaults to `dev` |
| `Privacy.Policy` | Optional policy JSON supplied locally instead of fetching |
| `Privacy.FetchTimeout` | 5 seconds |
| `Live.APIBase` | Defaults to `Privacy.APIBase`; required in live mode |
| `Live.Policy.SampleRate` | Pointer to 0..1; nil delegates the default to the API |
| `Live.Policy.Targets` | Expiring actor-ID or label matches |
| `Live.DefineActorID` | Optional HTTP identity callback |
| `Live.AdmissionTimeout` | 1 second |
| `Live.MaxConcurrentAdmissions` | 32; exhaustion skips capture rather than waiting in an unbounded queue |
| `Tracing.Exclude`, `.ExcludeFunctions` | File/function regular expressions |
| `Limits.Depth` | 128 staging, 10 live |
| `Limits.Collection`, `.Keys` | 10,000 staging, 100 live |
| `Limits.StringBytes` | 1 MiB per string/byte value |
| HTTP body capture | 1 MiB observed prefix; larger or unread bodies carry explicit omission metadata |
| `Limits.Nodes`, `.Bytes` | 10,000 nodes, 2 MiB aggregate per snapshot |
| `MaxScopeBytes` | 8 MiB retained evidence per request |
| `MaxCaptureBytes` | 64 MiB shared retained-evidence budget; not an exact heap cap |
| `MaxConcurrentScopes` | 256 |
| `QueueBytes` | 16 MiB encoded delivery queue |
| `DrainTimeout` | 30 seconds after the handler ends for unfinished correlated work |

## Verification

Go 1.26+ is required by the module. The executed validation environment uses
Go 1.27.3. See [VALIDATION.md](VALIDATION.md) for the tested dependency versions
and acceptance evidence rather than assuming every Go/library release works.

```sh
go build -o bin/repro-go ./cmd/repro-go
go test -race ./...
cd test/integration
go test -v -count=1 -timeout=25m ./...
```

The integration suite requires Docker and uses disposable Testcontainers.
API interoperability uses the sibling API/collector dependencies already installed:

```sh
# From go-sdk, with the desired Go toolchain on PATH:
node test/pipeline.cjs
REPRO_PIPELINE_POLICY=1 node test/pipeline.cjs
REPRO_PIPELINE_LIVE=1 node test/pipeline.cjs
```

These commands do not use production credentials or mutate production data.
The API/collector harness exercises their real oversized-message durable
ClickHouse fallback; Kafka has separate real-broker integration coverage.

Application values still obey Go's concurrency rules: writing a map while another
goroutine reads it is unsafe. Instrumentation cannot make unsynchronized shared
state safe. Initialize before accepting work, stop accepting work before shutdown,
and inspect `Flush`/`Shutdown` errors. Process termination cannot guarantee delivery.
