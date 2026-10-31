package capture

import (
	"strings"
	"testing"
	"time"
)

func TestDatabaseAndTraceShareBoundedCaptureMemory(t *testing.T) {
	r := &Runtime{config: Config{MaxScopeBytes: 2048, MaxCaptureBytes: 3072, Limits: Limits{Collection: 100}}}
	a := &Scope{runtime: r}
	b := &Scope{runtime: r}
	record := a.Database("postgresql", "orders", "select", map[string]any{"sql": "SELECT payload"}, 1)
	if record == nil {
		t.Fatal("small query denied")
	}
	for i := 0; i < 100; i++ {
		record.Row(strings.Repeat("x", 400))
	}
	record.EndRows(true, nil)
	meta := record.payload["resultMeta"].(map[string]any)
	if meta["captureComplete"] != false || len(a.failures) == 0 || a.bytes > 2048 {
		t.Fatal("large database results silently escaped scope budget")
	}
	if meta["reason"] != "capture_byte_limit" {
		t.Fatal("incorrect omission reason")
	}
	b.traceEvent(map[string]any{"args": strings.Repeat("y", 2000)})
	if len(b.trace) != 0 || len(b.failures) == 0 || r.bufferBytes.Load() > 3072 {
		t.Fatal("concurrent scopes escaped global budget")
	}
	before := r.bufferBytes.Load()
	record.Result(strings.Repeat("z", 8000), nil, time.Millisecond)
	if r.bufferBytes.Load() != before {
		t.Fatal("refused value retained")
	}
	if record.payload["resultMeta"].(map[string]any)["complete"] != false {
		t.Fatal("budget-limited result reported complete")
	}
}
