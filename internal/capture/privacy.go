package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/getrepro/go-sdk/internal/wire"
)

// prepareBatch runs exclusively on the bounded delivery worker, never on a
// customer handler. Retries reuse the already protected bytes.
func (r *Runtime) prepareBatch(data []byte) ([]byte, error) {
	var batch wire.Batch
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&batch); err != nil {
		return nil, err
	}
	if r.policy == nil {
		r.compactValues(&batch)
		return json.Marshal(batch)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failed := false
	apply := func(object map[string]any, key, surface string, details map[string]any) {
		value, exists := object[key]
		if !exists || value == nil {
			return
		}
		protected, err := r.policy.Apply(ctx, surface, value, details)
		if err != nil {
			failed = true
			protected = omission("privacy_evaluation_failed", nil)
			r.recordError(errors.New("repro: evidence withheld after privacy evaluation failure"))
		}
		object[key] = protected
	}
	route := ""
	for _, event := range batch.Events {
		if request, ok := event.Payload["request"].(map[string]any); ok {
			route, _ = request["key"].(string)
		}
	}
	for _, event := range batch.Events {
		details := map[string]any{"appId": r.config.AppID, "appSecret": r.config.AppSecret, "routeKey": route}
		if request, ok := event.Payload["request"].(map[string]any); ok {
			for _, key := range []string{"headers", "body", "params", "query"} {
				apply(request, key, "request."+key, details)
			}
			apply(request, "responseHeaders", "request.headers", details)
			apply(request, "respBody", "response.body", details)
		}
		if traces, ok := event.Payload["trace"].([]any); ok {
			for _, entry := range traces {
				trace, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				details["trace"] = map[string]any{"fn": trace["fn"], "file": trace["file"], "functionType": trace["functionType"]}
				for _, key := range []string{"args", "returnValue", "error"} {
					apply(trace, key, "trace."+key, details)
				}
			}
		}
		if changes, ok := event.Payload["db"].([]any); ok {
			for _, entry := range changes {
				db, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				details["db"] = map[string]any{"type": db["type"], "collection": db["collection"], "op": db["op"]}
				for _, key := range []string{"pk", "before", "after", "query", "resultMeta", "error"} {
					apply(db, key, "db."+key, details)
				}
			}
		}
	}
	if failed {
		for _, event := range batch.Events {
			manifest, ok := event.Payload["requestCaptureManifest"].(map[string]any)
			if !ok {
				continue
			}
			if privacy, ok := manifest["privacy"].(map[string]any); ok {
				privacy["status"] = "failed"
			}
			failures, _ := manifest["captureFailures"].([]any)
			manifest["captureFailures"] = append(failures, map[string]any{"stage": "privacy.apply", "target": "payload", "reason": "evaluation_failed"})
		}
	}
	r.compactValues(&batch)
	return json.Marshal(batch)
}
