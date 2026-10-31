module example.com/repro-http

go 1.26.0

require github.com/getrepro/go-sdk v0.0.0

require (
	github.com/dlclark/regexp2/v2 v2.5.2 // indirect
	github.com/dop251/goja v0.0.0-20260930195847-0f92c903ca4a // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	golang.org/x/text v0.3.8 // indirect
)

replace github.com/getrepro/go-sdk => ../../..
