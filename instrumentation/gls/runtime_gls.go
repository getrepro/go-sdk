//go:build ignore

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
// Adapted from otelc v1.0.0 instrumentation/runtime/runtime_gls.go.

package runtime

import _ "unsafe"

//go:linkname GetTraceContextFromGLS
func GetTraceContextFromGLS() interface{} { return getg().m.curg.otel_trace_context }

//go:linkname SetTraceContextToGLS
func SetTraceContextToGLS(value interface{}) { getg().m.curg.otel_trace_context = value }

func propagateReproContext(value interface{}, caller uintptr) interface{} {
	if cloner, ok := value.(interface{ CloneForGoroutine(string) interface{} }); ok {
		return cloner.CloneForGoroutine(funcname(findfunc(caller)))
	}
	return value
}

func finishReproGoroutine() {
	value := getg().otel_trace_context
	getg().otel_trace_context = nil
	if finished, ok := value.(interface{ Done() }); ok {
		finished.Done()
	}
}
