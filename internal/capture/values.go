package capture

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/getrepro/go-sdk/internal/wire"
)

// Values are keyed by protected content, never Go pointer identity. Mutating a
// reused argument produces a different value ID while equal observations share
// storage within this request, using the API's existing full-value contract.
func (r *Runtime) compactValues(batch *wire.Batch) {
	seen := map[string]bool{}
	captureValue := func(payload map[string]any, entries string, object map[string]any, key, target string) {
		value, exists := object[key]
		if !exists || value == nil {
			return
		}
		data, err := json.Marshal(value)
		if err != nil || len(data) < 2048 {
			return
		}
		hash := hmac.New(sha256.New, []byte(r.config.AppSecret))
		hash.Write([]byte("repro-captured-value-v1\x00" + r.config.TenantID + ":" + r.config.AppID + "\x00" + target + "\x00"))
		hash.Write(data)
		id := "rv1_" + hex.EncodeToString(hash.Sum(nil))[:40]
		ref := map[string]any{"status": "stored", "valueId": id}
		omitted := hasOmission(value)
		if omitted {
			ref["complete"] = false
			ref["omissions"] = []any{map[string]any{"reason": "capture_limit", "path": target}}
		}
		object[key+"ValueCapture"] = ref
		object[key] = Snapshot(value, Limits{Depth: 4, Collection: 8, Keys: 12, StringBytes: 256, Nodes: 100}, true)
		if !seen[id] {
			seen[id] = true
			values, _ := payload[entries].([]any)
			payload[entries] = append(values, map[string]any{"id": id, "target": target, "value": value, "truncatedForStorage": false})
		}
	}
	for _, event := range batch.Events {
		if request, ok := event.Payload["request"].(map[string]any); ok {
			for _, key := range []string{"headers", "body", "params", "query"} {
				captureValue(event.Payload, "requestValues", request, key, "request."+key)
			}
			captureValue(event.Payload, "requestValues", request, "respBody", "response.body")
		}
		if traces, ok := event.Payload["trace"].([]any); ok {
			for _, entry := range traces {
				if trace, ok := entry.(map[string]any); ok {
					for _, key := range []string{"args", "returnValue", "error"} {
						captureValue(event.Payload, "traceValues", trace, key, "trace."+key)
					}
				}
			}
		}
		if changes, ok := event.Payload["db"].([]any); ok {
			for _, entry := range changes {
				if db, ok := entry.(map[string]any); ok {
					for _, key := range []string{"pk", "before", "after", "query", "resultMeta", "error"} {
						captureValue(event.Payload, "dbValues", db, key, "db."+key)
					}
				}
			}
		}
	}
}

func hasOmission(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		if v["__kind"] == "repro-omitted" || v["__kind"] == "truncated" || (v["__kind"] == "bytes" && v["truncated"] == true) {
			return true
		}
		for _, child := range v {
			if hasOmission(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if hasOmission(child) {
				return true
			}
		}
	}
	return false
}
