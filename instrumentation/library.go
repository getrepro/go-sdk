package instrumentation

import (
	"reflect"
	_ "unsafe"

	"github.com/getrepro/go-sdk/internal/capture"
)

type libraryContext struct {
	pkg, fn         string
	params, results []any
	data            any
	keyData         map[string]any
	previous        *capture.Frame
}

// The link boundary deliberately uses only strings, slices, and empty
// interfaces; no nonempty-interface method tables cross package boundaries.
//
//go:linkname LibraryEnter
func LibraryEnter(pkg, fn string, params []any) (state any) {
	if !capture.Capturing() {
		return nil
	}
	previous := capture.Current()
	defer func() {
		if recover() != nil {
			capture.Restore(previous)
			state = nil
		}
	}()
	c := &libraryContext{pkg: pkg, fn: fn, params: params, previous: previous}
	args := make([]any, len(params))
	for i := range args {
		args[i] = c.GetParam(i)
	}
	before(c, args)
	return c
}

//go:linkname LibraryExit
func LibraryExit(state any, results []any, panicValue any) {
	c, ok := state.(*libraryContext)
	if !ok {
		return
	}
	defer func() { capture.Restore(c.previous); _ = recover() }()
	c.results = results
	if panicValue != nil {
		c.SetKeyData("panic", panicValue)
	}
	values := make([]any, len(results))
	for i := range values {
		values[i] = c.GetReturnVal(i)
	}
	after(c, values)
}

func (c *libraryContext) SetSkipCall(bool)          {}
func (c *libraryContext) IsSkipCall() bool          { return false }
func (c *libraryContext) SetData(value any)         { c.data = value }
func (c *libraryContext) GetData() any              { return c.data }
func (c *libraryContext) GetKeyData(key string) any { return c.keyData[key] }
func (c *libraryContext) SetKeyData(key string, value any) {
	if c.keyData == nil {
		c.keyData = map[string]any{}
	}
	c.keyData[key] = value
}
func (c *libraryContext) HasKeyData(key string) bool { _, ok := c.keyData[key]; return ok }
func (c *libraryContext) GetParamCount() int         { return len(c.params) }
func (c *libraryContext) GetParam(i int) any         { return deref(c.params[i]) }
func (c *libraryContext) SetParam(i int, value any) {
	reflect.ValueOf(c.params[i]).Elem().Set(reflect.ValueOf(value))
}
func (c *libraryContext) GetReturnValCount() int { return len(c.results) }
func (c *libraryContext) GetReturnVal(i int) any { return deref(c.results[i]) }
func (c *libraryContext) SetReturnVal(i int, value any) {
	reflect.ValueOf(c.results[i]).Elem().Set(reflect.ValueOf(value))
}
func (c *libraryContext) GetFuncName() string    { return c.fn }
func (c *libraryContext) GetPackageName() string { return c.pkg }
func deref(ptr any) any {
	if ptr == nil {
		return nil
	}
	return reflect.ValueOf(ptr).Elem().Interface()
}
