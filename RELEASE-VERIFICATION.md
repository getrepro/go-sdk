# Private release verification

Verified on 2026-10-31, macOS arm64, Go 1.27.3.

- Repository: `https://github.com/getrepro/go-sdk` (PRIVATE).
- Release: `v0.1.0`, commit `185addb2af4417d8247a49001dce42d9ff5993e8`.
- Module checksum: `h1:+1CC3XsKw8pf44rtOPIJUwKwkQSoHd4fvmd8I/S2MHQ=`.

## Installation

Installed `github.com/getrepro/go-sdk/cmd/repro-go@v0.1.0` through authenticated
GitHub access with isolated HOME, GOPATH, GOMODCACHE and GOCACHE; GOWORK was off.
The application module resolved v0.1.0 from the downloaded module cache with
no `replace` directive. `go version -m` confirmed the installed CLI version.
REPRO_OTELC was unset: the CLI downloaded and built its pinned engine itself.
GitHub authentication was passed in memory, not saved in the test directory.

## Passed checks

- SDK unit tests with race detection and `go vet ./...`.
- Released SDK: instrumented and baseline application results matched, including
  pointer/panic identity, named returns, deferred calls, generics, build tags and
  concurrent request scopes. Captured 32 events across eight concurrent scopes.
- Instrumented test-only functions produced the expected captured return value.
- Automatic HTTP capture and correlation, redirects and streaming semantics.
- Shutdown, deadline and queue-overflow scenarios preserved business execution.
- A separate Meridian source copy was built with the downloaded v0.1.0 SDK and
  CLI, without the development replacement. Binary module metadata confirmed
  v0.1.0 and the checksum above.
- Meridian regression suite: 12 tests passed, zero failed/skipped, including
  unchanged Chromium UI scenarios, Node account/JWT/data compatibility, concurrent
  payments, fee discrepancy, stale cache, rollback, projection failure and outbox
  recovery, PostgreSQL/MongoDB/Redis evidence, Kafka and RabbitMQ correlation,
  poison-message handling, tenant isolation and manifest completeness.
- Meridian test capture: 2,133 events, session
  `S_1793460313143_meridian_go_contract` (local test sink, not a production session).

Meridian tests used isolated temporary namespaces on existing local containers.
No existing application databases, topics or queues were reset. This release
verification did not record a new DO session or redeploy production services.

To exercise a release-built Meridian binary with the existing regression suite:

```sh
GO_TEST_LOCAL_OFFSET=100 GO_BROWSER_TEST=1 \
GO_TEST_BINARY=/absolute/path/to/release-built-meridian \
node --test scripts/go-backend.testcontainers.cjs
```

Run that command from `repro-live-demo`; omit GO_TEST_LOCAL_OFFSET to provision
disposable test containers instead of using the existing local infrastructure.
