package capture

import (
	"errors"
	"testing"
	"time"
)

func TestReturnedErrorIsEvidenceWithoutInventingPanic(t *testing.T) {
	r := &Runtime{config: Config{MaxScopeBytes: 8192, MaxCaptureBytes: 16384, Limits: Limits{Depth: 10, Collection: 100, Keys: 100, Nodes: 1000, StringBytes: 4096}}}
	s := &Scope{runtime: r}
	err := errors.New("settlement rejected")
	call := &Call{frame: &Frame{Scope: s, Span: 1}, meta: Meta{Function: "settle"}, start: time.Now()}
	call.End([]any{0, err}, nil)
	if len(s.trace) != 1 {
		t.Fatal("missing function exit")
	}
	exit := s.trace[0]
	got, ok := exit["error"].(map[string]any)
	if !ok || got["message"] != "settlement rejected" || exit["threw"] != false {
		t.Fatalf("Go error result missing or falsely labeled a panic: %v", exit)
	}
	values := exit["returnValue"].([]any)
	if values[0] != int64(0) || values[1].(map[string]any)["message"] != err.Error() {
		t.Fatal("error metadata changed captured return values")
	}
	call = &Call{frame: &Frame{Scope: s, Span: 2}, meta: Meta{Function: "settle"}, start: time.Now()}
	call.End([]any{1, nil}, nil)
	if s.trace[1]["error"] != nil || s.trace[1]["threw"] != false {
		t.Fatal("nil error falsely marked as failure")
	}
}
