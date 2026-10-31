// Package wire implements the existing Repro ingestion contract.
package wire

type Event struct {
	IngestID      string            `json:"ingest_id"`
	SchemaVersion int               `json:"schema_version"`
	TenantID      string            `json:"tenant_id"`
	AppID         string            `json:"app_id"`
	SessionID     string            `json:"session_id"`
	EventType     string            `json:"event_type"`
	EventTS       string            `json:"event_ts"`
	ActorID       string            `json:"actor_id"`
	ActorType     string            `json:"actor_type"`
	ActorLabels   map[string]string `json:"actor_labels,omitempty"`
	RequestID     string            `json:"request_id,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	Payload       map[string]any    `json:"payload"`
}

type Batch struct {
	Events []Event `json:"events"`
}

type Counts struct {
	BackendRequest int `json:"backendRequest"`
	TraceBatch     int `json:"traceBatch"`
	DBChange       int `json:"dbChange"`
	EmailEvent     int `json:"emailEvent"`
}

type Failure struct {
	Stage  string `json:"stage"`
	Target string `json:"target"`
	Reason string `json:"reason"`
}
