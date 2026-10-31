package capture

import (
	"github.com/getrepro/go-sdk/internal/bridge"
	"time"
)

func MarkIntegration(name string) {
	if current := Current(); current != nil {
		next := *current
		next.Integration = name
		bridge.Set(&next)
	}
}

type DatabaseRecord struct {
	scope          *Scope
	payload        map[string]any
	rows           []any
	rowCount       int
	omissionReason string
}

func (s *Scope) Database(kind, collection, operation string, query any, span uint64) *DatabaseRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	payload := map[string]any{"type": kind, "collection": collection, "op": operation, "query": query, "spanContext": map[string]any{"rid": s.RID, "traceId": s.RID, "spanId": span}}
	if !s.retainValueLocked(payload, "db.query") {
		return nil
	}
	r := &DatabaseRecord{scope: s, payload: payload}
	s.counts.DBChange++
	s.add("db_change", map[string]any{"requestRid": s.RID, "db": []any{payload}}, time.Now())
	return r
}

func (r *DatabaseRecord) Result(result, err any, duration time.Duration) {
	if r == nil {
		return
	}
	s := r.scope
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	r.payload["durMs"] = duration.Milliseconds()
	if !s.retainValueLocked(result, "db.resultMeta") {
		result = omission("capture_byte_limit", nil)
	}
	r.payload["resultMeta"] = result
	if err != nil {
		r.payload["error"] = err
	}
}

func (r *DatabaseRecord) Row(row any) {
	if r == nil {
		return
	}
	s := r.scope
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	r.rowCount++
	if len(r.rows) >= s.runtime.config.Limits.Collection {
		if r.omissionReason == "" {
			r.omissionReason = "max_collection_length"
		}
	} else if s.retainValueLocked(row, "db.resultMeta") {
		r.rows = append(r.rows, row)
	} else {
		r.omissionReason = "capture_byte_limit"
	}
	r.payload["resultMeta"] = map[string]any{"rows": r.rows, "observedRowCount": r.rowCount, "captureComplete": false, "reason": "result_not_fully_consumed"}
}

func (r *DatabaseRecord) EndRows(exhausted bool, err any) {
	if r == nil {
		return
	}
	s := r.scope
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	result := map[string]any{"rows": r.rows, "observedRowCount": r.rowCount, "captureComplete": exhausted && err == nil && r.rowCount <= len(r.rows)}
	if r.rowCount > len(r.rows) {
		result["reason"] = r.omissionReason
	} else if !exhausted {
		result["reason"] = "result_not_fully_consumed"
	}
	r.payload["resultMeta"] = result
	if err != nil {
		r.payload["error"] = err
	}
}

func (r *DatabaseRecord) CopyResult(result map[string]any, err any, duration time.Duration) {
	if r == nil {
		return
	}
	s := r.scope
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	result["rows"] = r.rows
	result["observedRowCount"] = r.rowCount
	result["captureComplete"] = err == nil && r.rowCount <= len(r.rows)
	if r.omissionReason != "" {
		result["reason"] = r.omissionReason
	}
	result["rowsAre"] = "attempted-copy-input"
	r.payload["resultMeta"] = result
	r.payload["durMs"] = duration.Milliseconds()
	if err != nil {
		r.payload["error"] = err
	}
}

func (s *Scope) Store(key, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.objects == nil {
		s.objects = map[any]any{}
	}
	s.objects[key] = value
}
func (s *Scope) Load(key any) any { s.mu.Lock(); defer s.mu.Unlock(); return s.objects[key] }
