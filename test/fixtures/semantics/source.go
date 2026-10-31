package main

import "runtime"

// The instrumented binary must report the same caller location as plain Go.
func sourceLocation() int {
	_, _, line, _ := runtime.Caller(0)
	return line
}
