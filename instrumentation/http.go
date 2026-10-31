package instrumentation

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/getrepro/go-sdk/internal/capture"
)

type bodyBuffer struct {
	mu       sync.Mutex
	data     bytes.Buffer
	total    int
	limit    int
	eof      bool
	expected int64
}

func (b *bodyBuffer) append(p []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total += len(p)
	if n := min(len(p), b.limit-b.data.Len()); n > 0 {
		b.data.Write(p[:n])
	}
}
func (b *bodyBuffer) value(requireEOF bool) any {
	b.mu.Lock()
	defer b.mu.Unlock()
	var value any
	decoder := json.NewDecoder(bytes.NewReader(b.data.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		value = b.data.String()
	}
	if b.total > b.limit {
		return map[string]any{"__kind": "repro-omitted", "complete": false, "reason": "body_byte_limit", "observedBytes": b.total, "value": value}
	}
	if requireEOF && !b.eof && (b.expected < 0 || int64(b.total) != b.expected) {
		return map[string]any{"__kind": "repro-omitted", "complete": false, "reason": "body_not_fully_consumed", "observedBytes": b.total, "value": value}
	}
	return value
}

type recordingReader struct {
	io.ReadCloser
	buffer *bodyBuffer
	done   func()
	once   sync.Once
}

// Switching-protocol responses expose io.Writer in addition to ReadCloser.
// Keep that interface only when the original response exposes it.
type recordingDuplex struct {
	*recordingReader
	writer io.Writer
}

func (r *recordingDuplex) Write(p []byte) (int, error) { return r.writer.Write(p) }

func (r *recordingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.buffer.append(p[:n])
	if err == io.EOF {
		r.buffer.mu.Lock()
		r.buffer.eof = true
		r.buffer.mu.Unlock()
		if r.done != nil {
			r.once.Do(r.done)
		}
	}
	return n, err
}

func (r *recordingReader) Close() error {
	err := r.ReadCloser.Close()
	if r.done != nil {
		r.once.Do(r.done)
	}
	return err
}

type httpServerCapture struct {
	scope    *capture.Scope
	previous *capture.Frame
	request  *http.Request
	status   int
	written  bool
	response http.ResponseWriter
	in, out  bodyBuffer
}

func beforeHTTPServer(ctx HookContext, args []any) {
	if len(args) != 3 || !capture.Enabled() {
		return
	}
	w, ok := args[1].(http.ResponseWriter)
	if !ok {
		return
	}
	r, ok := args[2].(*http.Request)
	if !ok {
		return
	}
	sid := r.Header.Get("x-bug-session-id")
	if sid == "" {
		sid = r.URL.Query().Get("__repro_sid")
	}
	aid := r.Header.Get("x-bug-action-id")
	actor := ""
	if aid == "" {
		aid = r.URL.Query().Get("__repro_aid")
	}
	if sid == "" {
		sid, aid, actor = capture.AdmitHTTP(r)
	}
	scope, previous := capture.Begin(sid, aid, "", "http", actor)
	if scope == nil {
		return
	}
	state := &httpServerCapture{scope: scope, previous: previous, request: r, response: w, status: 200, in: bodyBuffer{limit: 1 << 20, expected: r.ContentLength}, out: bodyBuffer{limit: 1 << 20}}
	if r.Body != nil {
		r.Body = &recordingReader{ReadCloser: r.Body, buffer: &state.in}
	}
	wrapped := httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(status int) {
				if status >= 200 && !state.written {
					state.status = status
					state.written = true
				}
				next(status)
			}
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(p []byte) (int, error) {
				state.written = true
				n, err := next(p)
				state.out.append(p[:n])
				return n, err
			}
		},
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(reader io.Reader) (int64, error) {
				state.written = true
				buffer := bodyBuffer{limit: state.out.limit}
				n, err := next(io.TeeReader(reader, writerFunc(func(p []byte) (int, error) { buffer.append(p); return len(p), nil })))
				state.out.append(buffer.data.Bytes()[:min(int(n), buffer.data.Len())])
				state.out.mu.Lock()
				state.out.total += max(0, int(n)-buffer.data.Len())
				state.out.mu.Unlock()
				return n, err
			}
		},
		Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc { return func() { state.written = true; next() } },
	})
	ctx.SetData(state)
	ctx.SetParam(1, wrapped)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func (s *httpServerCapture) finish(panicValue any) {
	defer capture.Restore(s.previous)
	r := s.request
	request := map[string]any{"method": r.Method, "url": r.URL.String(), "path": r.URL.Path, "key": r.Method + " " + r.URL.Path, "status": s.status, "durMs": time.Since(s.scope.Start).Milliseconds(), "headers": s.scope.Value(r.Header), "responseHeaders": s.scope.Value(s.response.Header()), "query": s.scope.Value(r.URL.Query()), "parentRequestRid": r.Header.Get("x-repro-request-rid"), "body": s.scope.Value(s.in.value(true)), "respBody": s.scope.Value(s.out.value(false))}
	if panicValue != nil {
		request["error"] = s.scope.Value(panicValue)
		request["responseComplete"] = false
		if !s.written {
			request["status"] = 0
		}
	}
	_ = s.scope.Finish(request)
}

type httpClientCapture struct {
	call     *capture.Call
	previous *capture.Frame
	request  *http.Request
	in, out  bodyBuffer
	start    time.Time
	response *http.Response
	once     sync.Once
}

func beforeHTTPClient(ctx HookContext, args []any) {
	if len(args) != 2 {
		return
	}
	r, ok := args[1].(*http.Request)
	if !ok || r == nil || r.URL == nil || capture.InternalURL(r.URL.String()) {
		return
	}
	frame := capture.Current()
	if frame == nil {
		return
	}
	copy := r.Clone(r.Context())
	correlation := map[string]string{"x-bug-session-id": frame.Scope.SessionID, "x-bug-action-id": frame.Scope.ActionID, "x-bug-request-start": strconv.FormatInt(time.Now().UnixMilli(), 10), "x-repro-request-rid": frame.Scope.RID, "x-repro-trace-id": frame.Scope.RID, "x-repro-parent-span-id": strconv.FormatUint(frame.Span, 10), "x-repro-capture-mode": frame.Scope.Mode()}
	for key, value := range correlation {
		if copy.Header.Get(key) == "" && value != "" {
			copy.Header.Set(key, value)
		}
	}
	call := capture.Enter(capture.Meta{Function: "net/http.Client.Do"}, []any{map[string]any{"method": r.Method, "url": r.URL.String(), "headers": copy.Header}})
	if call == nil {
		return
	}
	state := &httpClientCapture{call: call, previous: frame, request: copy, start: time.Now(), in: bodyBuffer{limit: 1 << 20, expected: r.ContentLength}, out: bodyBuffer{limit: 1 << 20, expected: -1}}
	if copy.Body != nil && copy.Body != http.NoBody {
		copy.Body = &recordingReader{ReadCloser: copy.Body, buffer: &state.in}
	}
	ctx.SetParam(1, copy)
	ctx.SetData(state)
}

func (s *httpClientCapture) finish(values []any) {
	defer capture.Restore(s.previous)
	if len(values) > 1 && values[1] != nil {
		s.complete(values[1])
		return
	}
	if len(values) == 0 {
		s.complete(nil)
		return
	}
	response, _ := values[0].(*http.Response)
	s.response = response
	if response == nil || response.Body == nil || response.Body == http.NoBody {
		s.out.eof = true
		s.complete(nil)
		return
	}
	s.out.expected = response.ContentLength
	original := response.Body
	reader := &recordingReader{ReadCloser: original, buffer: &s.out, done: func() { s.complete(nil) }}
	response.Body = reader
	if writer, ok := original.(io.Writer); ok {
		response.Body = &recordingDuplex{recordingReader: reader, writer: writer}
	}
}

func (s *httpClientCapture) complete(err any) {
	s.once.Do(func() {
		previous := capture.Current()
		defer capture.Restore(previous)
		result := map[string]any{"method": s.request.Method, "url": s.request.URL.String(), "durationMs": time.Since(s.start).Milliseconds(), "requestBody": s.in.value(true), "body": s.out.value(true)}
		if s.response != nil {
			result["status"] = s.response.StatusCode
			result["headers"] = s.response.Header
		}
		if err != nil {
			result["error"] = err
		}
		s.call.End([]any{result}, nil)
	})
}
