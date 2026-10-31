package capture

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/getrepro/go-sdk/internal/wire"
)

func TestContentReferencesPreserveMutationsAndReduceRepeatedValues(t *testing.T) {
	r := &Runtime{config: Config{TenantID: "TENANT", AppID: "APP", AppSecret: "test-only"}}
	original := map[string]any{"value": strings.Repeat("business-data-", 1000), "revision": 1}
	traces := []any{}
	limits := Limits{Depth: 20, Keys: 100, Collection: 100, Nodes: 10000, StringBytes: 1 << 20}
	for i := 0; i < 20; i++ {
		if i == 10 {
			original["revision"] = 2
		}
		traces = append(traces, map[string]any{"args": Snapshot([]any{original}, limits, true)})
	}
	batch := wire.Batch{Events: []wire.Event{{Payload: map[string]any{"trace": traces}}}}
	before, _ := json.Marshal(batch)
	r.compactValues(&batch)
	after, _ := json.Marshal(batch)
	if len(after) >= len(before)/4 {
		t.Fatalf("insufficient deduplication: %d -> %d", len(before), len(after))
	}
	entries := batch.Events[0].Payload["traceValues"].([]any)
	if len(entries) != 2 {
		t.Fatalf("mutated snapshot versions conflated: %d", len(entries))
	}
	values := map[string]any{}
	for _, raw := range entries {
		entry := raw.(map[string]any)
		values[entry["id"].(string)] = entry["value"]
	}
	firstID := ""
	for i, raw := range traces {
		trace := raw.(map[string]any)
		ref := trace["argsValueCapture"].(map[string]any)
		id := ref["valueId"].(string)
		value := values[id].([]any)[0].(map[string]any)
		revision := int64(1)
		if i >= 10 {
			revision = 2
		}
		if value["revision"] != revision || value["value"] != original["value"] {
			t.Fatal("full captured content changed")
		}
		if i == 0 {
			firstID = id
		}
		if i == 10 && id == firstID {
			t.Fatal("pointer mutation reused stale value")
		}
	}
	t.Logf("repeated observations: %d -> %d bytes, %.2f%% reduction with both versions exact", len(before), len(after), 100*(1-float64(len(after))/float64(len(before))))
}

func TestReferenceNamespaceAndOmissionMetadata(t *testing.T) {
	makeBatch := func() wire.Batch {
		return wire.Batch{Events: []wire.Event{{Payload: map[string]any{"request": map[string]any{"body": map[string]any{"value": strings.Repeat("x", 3000), "tail": omission("max_depth", nil)}}}}}}
	}
	first, second := makeBatch(), makeBatch()
	a := &Runtime{config: Config{TenantID: "A", AppID: "app", AppSecret: "key"}}
	b := &Runtime{config: Config{TenantID: "B", AppID: "app", AppSecret: "key"}}
	a.compactValues(&first)
	b.compactValues(&second)
	refA := first.Events[0].Payload["request"].(map[string]any)["bodyValueCapture"].(map[string]any)
	refB := second.Events[0].Payload["request"].(map[string]any)["bodyValueCapture"].(map[string]any)
	if refA["complete"] != false || reflect.DeepEqual(refA["valueId"], refB["valueId"]) {
		t.Fatal("namespace or incomplete-value metadata lost")
	}
}
