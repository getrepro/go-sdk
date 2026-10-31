// Package bridge is the dependency-free boundary between injected hooks and capture.
package bridge

// Context values must be immutable. Child goroutines inherit the current value.
var Get = func() any { return nil }
var Set = func(any) {}
var Instrumented bool

// HookContext is structurally compatible with otelc v1's generated context.
type HookContext interface {
	SetSkipCall(bool)
	IsSkipCall() bool
	SetData(any)
	GetData() any
	GetKeyData(string) any
	SetKeyData(string, any)
	HasKeyData(string) bool
	GetParamCount() int
	GetParam(int) any
	SetParam(int, any)
	GetReturnValCount() int
	GetReturnVal(int) any
	SetReturnVal(int, any)
	GetFuncName() string
	GetPackageName() string
}
