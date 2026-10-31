# Third-party components

The build tool uses OpenTelemetry otelc v1.0.0, licensed under Apache-2.0.
`instrumentation/gls/runtime_gls.go` is adapted from that project's runtime
goroutine context bridge. Copyright The OpenTelemetry Authors.
https://github.com/open-telemetry/opentelemetry-go-compile-instrumentation
License copy: `licenses/Apache-2.0.txt`. Repro's adaptation adds application-only
goroutine leases and runs propagation before the scheduler stack switch.

HTTP response interface preservation uses felixge/httpsnoop (MIT).
https://github.com/felixge/httpsnoop

YAML build rules use go-yaml/yaml v3 (MIT and Apache-2.0).
https://github.com/go-yaml/yaml

Go package/type analysis uses golang.org/x/tools (BSD-3-Clause).
https://go.googlesource.com/tools

The embedded privacy runtime uses dop251/goja (MIT); evaluator logic is bundled
from Repro's Node SDK. `internal/privacy/sources.json` records source checksums.
https://github.com/dop251/goja
