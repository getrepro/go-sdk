//go:build ignore

package instrumentation

import (
	"github.com/getrepro/go-sdk/internal/bridge"
	_ "unsafe"
)

//go:linkname getContext runtime.GetTraceContextFromGLS
func getContext() any

//go:linkname setContext runtime.SetTraceContextToGLS
func setContext(any)

func init() {
	bridge.Get = getContext
	bridge.Set = setContext
	bridge.Instrumented = true
}
