//go:build ignore

package library

import _ "unsafe"

//go:linkname _reproLibraryEnterV1 github.com/getrepro/go-sdk/instrumentation.LibraryEnter
func _reproLibraryEnterV1(pkg, fn string, params []interface{}) interface{}

//go:linkname _reproLibraryExitV1 github.com/getrepro/go-sdk/instrumentation.LibraryExit
func _reproLibraryExitV1(state interface{}, results []interface{}, panicValue interface{})
