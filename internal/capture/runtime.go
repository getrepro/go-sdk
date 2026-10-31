package capture

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getrepro/go-sdk/internal/bridge"
	"github.com/getrepro/go-sdk/internal/delivery"
	"github.com/getrepro/go-sdk/internal/privacy"
	"github.com/getrepro/go-sdk/internal/wire"
)

type Config struct {
	TenantID, AppID, AppSecret, AppName, ServiceName string
	IngestBase                                       string
	Mode                                             string
	ActorID                                          string
	ActorLabels                                      map[string]string
	CaptureRaw                                       bool
	Limits                                           Limits
	QueueBytes                                       int
	MaxScopeBytes                                    int
	MaxCaptureBytes                                  int
	MaxConcurrentScopes                              int
	DrainTimeout                                     time.Duration
	Privacy                                          privacy.Config
	Live                                             LiveConfig
	Tracing                                          TracingConfig
}

type TracingConfig struct {
	Exclude          []*regexp.Regexp
	ExcludeFunctions []*regexp.Regexp
}

func clonePatterns(patterns []*regexp.Regexp) []*regexp.Regexp {
	out := make([]*regexp.Regexp, len(patterns))
	for i, pattern := range patterns {
		if pattern != nil {
			out[i] = pattern.Copy()
		}
	}
	return out
}

func (r *Runtime) excluded(meta Meta) bool {
	for _, pattern := range r.config.Tracing.Exclude {
		if pattern != nil && pattern.MatchString(strings.ReplaceAll(meta.File, "\\", "/")) {
			return true
		}
	}
	for _, pattern := range r.config.Tracing.ExcludeFunctions {
		if pattern != nil && pattern.MatchString(meta.Function) {
			return true
		}
	}
	return false
}

type Runtime struct {
	config      Config
	queue       *delivery.Queue
	seq         atomic.Uint64
	bufferBytes atomic.Int64
	mu          sync.Mutex
	scopes      map[*Scope]struct{}
	changed     chan struct{}
	err         error
	policy      *privacy.Evaluator
	live        *liveControl
}

var active atomic.Pointer[Runtime]

type Scope struct {
	runtime                             *Runtime
	SessionID, ActionID, RID, Transport string
	ActorID                             string
	Start                               time.Time
	mu                                  sync.Mutex
	events                              []wire.Event
	trace                               []map[string]any
	counts                              wire.Counts
	failures                            []wire.Failure
	bytes                               int
	closed                              bool
	objects                             map[any]any
	pending                             int
	request                             map[string]any
	timer                               *time.Timer
	children                            atomic.Int64
	traceExhausted                      atomic.Bool
}

type Frame struct {
	Scope        *Scope
	Span, Parent uint64
	Depth        int
	Integration  string
	lease        *goroutineLease
}
type Call struct {
	frame, previous *Frame
	meta            Meta
	start           time.Time
	OnEnd           func([]any, any)
}
type Meta struct {
	Function, File string
	Line           int
}

func Init(c Config) (*Runtime, error) {
	if !bridge.Instrumented {
		return nil, errors.New("repro: compile this application with repro-go build")
	}
	if c.TenantID == "" || c.AppID == "" || c.AppSecret == "" {
		return nil, errors.New("repro: TenantID, AppID and AppSecret are required")
	}
	if c.Mode == "" {
		c.Mode = "staging"
	}
	if c.Mode != "staging" && c.Mode != "live" {
		return nil, errors.New("repro: Mode must be staging or live")
	}
	if c.IngestBase == "" {
		c.IngestBase = "https://log-collector-5rj86.ondigitalocean.app"
	}
	if c.Privacy.APIBase == "" {
		c.Privacy.APIBase = "https://repro-api-d7288.ondigitalocean.app/api"
	}
	u, err := url.Parse(c.IngestBase)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, errors.New("repro: IngestBase must be an HTTP(S) URL without credentials")
	}
	if c.ServiceName == "" {
		c.ServiceName = c.AppName
	}
	if c.ActorID == "" {
		c.ActorID = c.AppID
	}
	labels := make(map[string]string, len(c.ActorLabels))
	for k, v := range c.ActorLabels {
		labels[k] = v
	}
	c.ActorLabels = labels
	c.Tracing.Exclude = clonePatterns(c.Tracing.Exclude)
	c.Tracing.ExcludeFunctions = clonePatterns(c.Tracing.ExcludeFunctions)
	if c.Limits.Depth <= 0 {
		c.Limits.Depth = 128
		if c.Mode == "live" {
			c.Limits.Depth = 10
		}
	}
	if c.Limits.Keys <= 0 {
		c.Limits.Keys = 10000
		if c.Mode == "live" {
			c.Limits.Keys = 100
		}
	}
	if c.Limits.Collection <= 0 {
		c.Limits.Collection = 10000
		if c.Mode == "live" {
			c.Limits.Collection = 100
		}
	}
	if c.Limits.StringBytes <= 0 {
		c.Limits.StringBytes = 1 << 20
	}
	if c.Limits.Nodes <= 0 {
		c.Limits.Nodes = 10000
	}
	if c.MaxScopeBytes <= 0 {
		c.MaxScopeBytes = 8 << 20
	}
	if c.MaxCaptureBytes <= 0 {
		c.MaxCaptureBytes = 64 << 20
	}
	if c.MaxConcurrentScopes <= 0 {
		c.MaxConcurrentScopes = 256
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	c.CaptureRaw = c.CaptureRaw || c.Privacy.CaptureRaw
	r := &Runtime{config: c, scopes: make(map[*Scope]struct{}), changed: make(chan struct{})}
	if err := r.initLive(); err != nil {
		return nil, err
	}
	if !c.CaptureRaw {
		raw, err := privacy.Fetch(context.Background(), c.Privacy, c.AppID, c.AppSecret, c.AppName, c.ServiceName)
		if err != nil {
			return nil, err
		}
		r.policy, err = privacy.New(raw)
		if err != nil {
			return nil, err
		}
	}
	r.queue = delivery.New(delivery.Config{URL: strings.TrimRight(c.IngestBase, "/") + "/v1/ingest/events", AppID: c.AppID, AppSecret: c.AppSecret, AppName: c.AppName, MaxBytes: c.QueueBytes, Transform: r.prepareBatch})
	if !active.CompareAndSwap(nil, r) {
		_ = r.queue.Close(context.Background())
		return nil, errors.New("repro: already initialized")
	}
	return r, nil
}

func (r *Runtime) Flush(ctx context.Context) error {
	for {
		r.mu.Lock()
		n, changed := len(r.scopes), r.changed
		r.mu.Unlock()
		if n == 0 {
			deliveryErr := r.queue.Flush(ctx)
			r.mu.Lock()
			err := r.err
			r.mu.Unlock()
			return errors.Join(err, deliveryErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (r *Runtime) Shutdown(ctx context.Context) error {
	active.CompareAndSwap(r, nil)
	err := r.Flush(ctx)
	if err != nil {
		r.mu.Lock()
		scopes := make([]*Scope, 0, len(r.scopes))
		for s := range r.scopes {
			scopes = append(scopes, s)
		}
		r.mu.Unlock()
		for _, s := range scopes {
			s.expire("shutdown_deadline")
		}
	}
	return errors.Join(err, r.queue.Close(ctx))
}
func Current() *Frame         { frame, _ := bridge.Get().(*Frame); return frame }
func (s *Scope) Closed() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.closed }
func Enabled() bool           { return active.Load() != nil }
func Capturing() bool {
	if Enabled() {
		return true
	}
	frame := Current()
	return frame != nil && frame.Scope != nil && !frame.Scope.Closed()
}
func NewID() string {
	var b [12]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func Begin(session, action, rid, transport string, actors ...string) (*Scope, *Frame) {
	previous := Current()
	r := active.Load()
	if r == nil && previous != nil && previous.Scope != nil && previous.Scope.Retain() {
		r = previous.Scope.runtime
		defer previous.Scope.Release()
	}
	if r == nil || session == "" {
		return nil, nil
	}
	if rid == "" {
		rid = fmt.Sprintf("%d-%s", time.Now().UnixMilli(), NewID()[:8])
	}
	s := &Scope{runtime: r, SessionID: session, ActionID: action, RID: rid, Transport: transport, Start: time.Now()}
	s.ActorID = r.config.ActorID
	if previous := Current(); previous != nil && previous.Scope != nil {
		s.ActorID = previous.Scope.ActorID
	}
	if len(actors) > 0 && actors[0] != "" {
		s.ActorID = actors[0]
	}
	r.mu.Lock()
	if active.Load() != r && (previous == nil || previous.Scope == nil || previous.Scope.runtime != r) {
		r.mu.Unlock()
		return nil, nil
	}
	if len(r.scopes) >= r.config.MaxConcurrentScopes {
		if r.err == nil {
			r.err = errors.New("repro: maximum concurrent captures reached")
		}
		r.mu.Unlock()
		// Announce missing evidence instead of leaving the API a falsely complete
		// set of only the requests that fit the concurrency budget.
		s.fail("capture", "request", "maximum_concurrent_captures")
		start := s.event("request_capture_start", map[string]any{"requestRid": rid, "requestCaptureStart": map[string]any{"scopeId": "req:" + rid, "requestRid": rid, "transport": transport, "openedAt": s.Start.UnixMilli()}}, s.Start)
		manifest := s.event("request_capture_manifest", map[string]any{"requestRid": rid, "requestCaptureManifest": map[string]any{"scopeId": "req:" + rid, "requestRid": rid, "transport": transport, "expected": wire.Counts{}, "closedAt": time.Now().UnixMilli(), "privacy": map[string]any{"status": "failed"}, "captureFailures": s.failures}}, time.Now())
		data, _ := json.Marshal(wire.Batch{Events: []wire.Event{start, manifest}})
		if err := r.queue.Enqueue(data); err != nil {
			r.recordError(err)
		}
		return nil, nil
	}
	r.scopes[s] = struct{}{}
	r.mu.Unlock()
	start := s.event("request_capture_start", map[string]any{"requestRid": rid, "requestCaptureStart": map[string]any{"scopeId": "req:" + rid, "requestRid": rid, "transport": transport, "openedAt": s.Start.UnixMilli()}}, s.Start)
	data, _ := json.Marshal(wire.Batch{Events: []wire.Event{start}})
	if err := r.queue.Enqueue(data); err != nil {
		s.fail("delivery", "request_capture_start", err.Error())
		r.recordError(err)
	}
	frame := &Frame{Scope: s}
	if previous != nil {
		frame.lease = previous.lease
	}
	bridge.Set(frame)
	return s, previous
}

func Restore(frame *Frame) {
	if frame == nil {
		bridge.Set(nil)
	} else {
		bridge.Set(frame)
	}
}

func Enter(meta Meta, args []any) (call *Call) {
	defer func() { _ = recover() }()
	prev := Current()
	if prev == nil || prev.Scope == nil || prev.Scope.traceExhausted.Load() {
		return nil
	}
	r := prev.Scope.runtime
	if r.excluded(meta) {
		return nil
	}
	if !prev.Scope.Retain() {
		return nil
	}
	f := &Frame{Scope: prev.Scope, Span: r.seq.Add(1), Parent: prev.Span, Depth: prev.Depth + 1, Integration: prev.Integration, lease: prev.lease}
	call = &Call{frame: f, previous: prev, meta: meta, start: time.Now()}
	f.Scope.traceEvent(call.event("enter", map[string]any{"args": Snapshot(args, r.config.Limits, true)}))
	bridge.Set(f)
	return call
}

func (c *Call) End(values []any, panicValue any) {
	if c == nil {
		return
	}
	defer func() { Restore(c.previous); _ = recover() }()
	if c.OnEnd != nil {
		defer c.OnEnd(values, panicValue)
	}
	defer c.frame.Scope.Release()
	r := c.frame.Scope.runtime
	details := map[string]any{"returnValue": Snapshot(values, r.config.Limits, true), "threw": panicValue != nil, "durationMs": float64(time.Since(c.start)) / float64(time.Millisecond)}
	if panicValue != nil {
		details["error"] = Snapshot(panicValue, r.config.Limits, true)
	} else {
		for i, value := range values {
			if _, ok := value.(error); !ok || value == nil {
				continue
			}
			// Reuse the protected snapshot instead of calling Error a second time.
			// An error return is evidence, but it is not a thrown panic.
			if captured, ok := details["returnValue"].([]any); ok && i < len(captured) {
				details["error"] = captured[i]
				if captured[i] == nil {
					details["error"] = map[string]any{"__kind": "typed-nil-error", "type": fmt.Sprintf("%T", value)}
				}
			} else {
				details["error"] = omission("return_value_not_fully_captured", nil)
			}
			break
		}
	}
	c.frame.Scope.traceEvent(c.event("exit", details))
}

func (c *Call) event(kind string, detail map[string]any) map[string]any {
	detail["type"] = kind
	detail["t"] = time.Now().UnixMilli()
	detail["fn"] = c.meta.Function
	detail["file"] = c.meta.File
	detail["line"] = c.meta.Line
	detail["spanId"] = c.frame.Span
	detail["depth"] = c.frame.Depth
	detail["traceId"] = c.frame.Scope.RID
	if c.frame.Parent != 0 {
		detail["parentSpanId"] = c.frame.Parent
	}
	return detail
}

func (s *Scope) traceEvent(event map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	data, err := json.Marshal(event)
	if err != nil {
		s.fail("trace.capture", "trace", "serialization_failed")
		return
	}
	if !s.reserveLocked(len(data), "trace") {
		return
	}
	s.trace = append(s.trace, event)
}

// Every retained evidence surface shares the same budget. The delivery queue
// has its own byte limit after a scope transfers ownership of encoded evidence.
func (s *Scope) reserveLocked(size int, target string) bool {
	r := s.runtime
	if s.bytes+size > r.config.MaxScopeBytes {
		s.fail("capture", target, "scope_byte_limit")
		s.traceExhausted.Store(true)
		return false
	}
	for {
		current := r.bufferBytes.Load()
		if current+int64(size) > int64(r.config.MaxCaptureBytes) {
			s.fail("capture", target, "runtime_byte_limit")
			s.traceExhausted.Store(true)
			return false
		}
		if r.bufferBytes.CompareAndSwap(current, current+int64(size)) {
			s.bytes += size
			return true
		}
	}
}

func (s *Scope) retainValueLocked(value any, target string) bool {
	data, err := json.Marshal(value)
	if err != nil {
		s.fail("capture", target, "serialization_failed")
		return false
	}
	return s.reserveLocked(len(data), target)
}

func (s *Scope) fail(stage, target, reason string) {
	for _, f := range s.failures {
		if f.Stage == stage && f.Target == target && f.Reason == reason {
			return
		}
	}
	s.failures = append(s.failures, wire.Failure{Stage: stage, Target: target, Reason: reason})
}

func (s *Scope) event(kind string, payload map[string]any, at time.Time) wire.Event {
	c := s.runtime.config
	payload["action_id"] = s.ActionID
	payload["serviceName"] = c.ServiceName
	return wire.Event{IngestID: NewID(), SchemaVersion: 1, TenantID: c.TenantID, AppID: c.AppID, SessionID: s.SessionID, EventType: kind, EventTS: at.UTC().Format(time.RFC3339Nano), ActorID: s.ActorID, ActorType: "service", ActorLabels: c.ActorLabels, RequestID: s.RID, TraceID: s.RID, Payload: payload}
}

func (s *Scope) add(kind string, payload map[string]any, at time.Time) {
	s.events = append(s.events, s.event(kind, payload, at))
}

func (s *Scope) Finish(request map[string]any) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.request = request
	if s.pending > 0 || s.children.Load() > 0 {
		if s.timer == nil {
			s.timer = time.AfterFunc(s.runtime.config.DrainTimeout, func() { s.expire("unfinished_activity_deadline") })
		}
		return nil
	}
	return s.finishLocked()
}

func (s *Scope) finishLocked() error {
	s.closed = true
	if s.timer != nil {
		s.timer.Stop()
	}
	request := s.request
	if request == nil {
		request = map[string]any{"method": s.Transport, "url": "unfinished activity"}
	}
	request["rid"] = s.RID
	if !s.retainValueLocked(request, "request") {
		// Preserve request identity/status while explicitly withholding large fields.
		for _, key := range []string{"body", "respBody", "headers", "responseHeaders", "params", "query"} {
			if _, ok := request[key]; ok {
				request[key] = omission("capture_byte_limit", nil)
			}
		}
	}
	s.counts.BackendRequest = 1
	s.add("backend_request", map[string]any{"request": request}, s.Start)
	if len(s.trace) > 0 {
		s.counts.TraceBatch = (len(s.trace) + 99) / 100
		for i, start := 0, 0; start < len(s.trace); i, start = i+1, start+100 {
			s.add("trace_batch", map[string]any{"trace": s.trace[start:min(start+100, len(s.trace))], "traceBatch": map[string]any{"rid": s.RID, "index": i, "total": s.counts.TraceBatch}}, s.Start)
		}
	}
	mode, status := "fallback", "applied"
	statementCount := 0
	fingerprint := ""
	if s.runtime.policy != nil && s.runtime.policy.StatementCount > 0 {
		mode = "policy"
		statementCount = s.runtime.policy.StatementCount
		fingerprint = s.runtime.policy.Fingerprint
	}
	if s.runtime.config.CaptureRaw {
		mode, status = "raw", "bypassed"
	}
	if len(s.failures) > 0 {
		status = "failed"
	}
	manifest := map[string]any{"scopeId": "req:" + s.RID, "requestRid": s.RID, "transport": s.Transport, "expected": s.counts, "closedAt": time.Now().UnixMilli(), "privacy": map[string]any{"mode": mode, "status": status, "statementCount": statementCount, "policyFingerprint": fingerprint}}
	if s.counts.TraceBatch > 0 {
		manifest["traceBatch"] = map[string]int{"from": 0, "to": s.counts.TraceBatch - 1}
	}
	if len(s.failures) > 0 {
		manifest["captureFailures"] = s.failures
	}
	s.add("request_capture_manifest", map[string]any{"requestRid": s.RID, "requestCaptureManifest": manifest}, time.Now())
	data, err := json.Marshal(wire.Batch{Events: s.events})
	s.trace = nil
	s.events = nil
	s.objects = nil
	s.runtime.bufferBytes.Add(-int64(s.bytes))
	if err != nil {
		s.runtime.complete(s, err)
		return err
	}
	err = s.runtime.queue.Enqueue(data)
	if err == nil && len(s.failures) > 0 {
		err = fmt.Errorf("repro: incomplete capture (%s: %s)", s.failures[0].Stage, s.failures[0].Reason)
	}
	s.runtime.complete(s, err)
	return err
}

func (r *Runtime) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}
func (r *Runtime) complete(s *Scope, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.scopes, s)
	if err != nil && r.err == nil {
		r.err = err
	}
	close(r.changed)
	r.changed = make(chan struct{})
}

// Retain keeps already-started work in its original request scope after an HTTP
// handler returns. It never delays the application's own response.
func (s *Scope) Retain() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.pending++
	return true
}
func (s *Scope) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending > 0 {
		s.pending--
	}
	if !s.closed && s.request != nil && s.pending == 0 && s.children.Load() == 0 {
		_ = s.finishLocked()
	}
}
func (s *Scope) expire(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.fail("lifecycle", "request", reason)
		_ = s.finishLocked()
	}
}
func (s *Scope) Failure(stage, target, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.fail(stage, target, reason)
	}
}

func (c *Call) Frame() *Frame {
	if c == nil {
		return nil
	}
	return c.frame
}
func InternalURL(raw string) bool {
	r := active.Load()
	if r == nil {
		return false
	}
	u, e := url.Parse(raw)
	base, _ := url.Parse(r.config.IngestBase)
	if e != nil {
		return false
	}
	if u.Host == base.Host {
		return true
	}
	privacyBase, _ := url.Parse(r.config.Privacy.APIBase)
	if privacyBase != nil && u.Host == privacyBase.Host {
		return true
	}
	liveBase, _ := url.Parse(r.config.Live.APIBase)
	return liveBase != nil && u.Host == liveBase.Host
}
func (s *Scope) Mode() string { return s.runtime.config.Mode }

func (s *Scope) Value(v any) any {
	return Snapshot(v, s.runtime.config.Limits, true)
}
