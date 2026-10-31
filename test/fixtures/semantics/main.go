package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sync"
	"time"

	repro "github.com/getrepro/go-sdk"
)

type order struct {
	ID     int
	Amount int
	Items  []int
}

var buildTag = "default"

func identity[T any](value T) T    { return value }
func deferred(value int) (out int) { defer func() { out += 3 }(); return value * 2 }
func namedPanic(value error) (out error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			out = recovered.(error)
		}
	}()
	panic(value)
}
func sum(values ...int) int {
	total := 0
	for _, v := range values {
		total += v
	}
	return total
}
func (o *order) total() int         { return o.Amount + sum(o.Items...) }
func excludedBusinessValue() string { return "business-still-executes" }

func calculate(id int) map[string]any {
	if excludedBusinessValue() != "business-still-executes" {
		panic("excluded function changed")
	}
	o := &order{ID: id, Amount: 100, Items: []int{2, 3}}
	if identity(o) != o {
		panic("pointer identity changed")
	}
	err := errors.New("expected panic")
	if namedPanic(err) != err {
		panic("panic identity changed")
	}
	result := make(chan int, 1)
	go func(value int) { result <- deferred(value) }(id)
	return map[string]any{"id": id, "total": o.total(), "deferred": <-result, "sourceLine": sourceLocation()}
}

func main() {
	if os.Getenv("REPRO_PERFORMANCE") != "" {
		performance()
		return
	}
	if os.Getenv("REPRO_TAG_ASSERT") == "1" && buildTag != "reprotesttag" {
		panic("application build tags were lost")
	}
	var mu sync.Mutex
	var events []map[string]any
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			panic(err)
		}
		mu.Lock()
		events = append(events, batch.Events...)
		mu.Unlock()
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(batch.Events), "dropped": 0})
	}))
	defer collector.Close()
	var runtime *repro.Runtime
	if os.Getenv("UNINSTRUMENTED") != "1" {
		var err error
		runtime, err = repro.Init(repro.Config{TenantID: "TENANT_go_test", AppID: "APP_go_test", AppSecret: "test-only-secret", AppName: "go-semantics", IngestBase: collector.URL, CaptureRaw: true, Tracing: repro.TracingConfig{Exclude: []*regexp.Regexp{regexp.MustCompile(`/source\.go$`)}, ExcludeFunctions: []*regexp.Regexp{regexp.MustCompile(`\.excludedBusinessValue$`)}}})
		if err != nil {
			panic(err)
		}
	}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id int
		_, _ = fmt.Sscan(r.URL.Query().Get("id"), &id)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(calculate(id))
	}))
	defer app.Close()
	var wg sync.WaitGroup
	responses := make([]string, 8)
	for i := range responses {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", fmt.Sprintf("%s/order?id=%d", app.URL, index), nil)
			req.Header.Set("x-bug-session-id", fmt.Sprintf("S_go_%d", index))
			req.Header.Set("x-bug-action-id", fmt.Sprintf("A_%d", index))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				panic(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			responses[index] = string(body)
		}(i)
	}
	wg.Wait()
	if runtime != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runtime.Shutdown(ctx); err != nil {
			panic(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(events) == 0 {
			panic("no automatic capture")
		}
		seen := map[string]map[string]bool{}
		for _, event := range events {
			sid := event["session_id"].(string)
			if seen[sid] == nil {
				seen[sid] = map[string]bool{}
			}
			seen[sid][event["event_type"].(string)] = true
			if event["event_type"] == "trace_batch" {
				payload := event["payload"].(map[string]any)
				for _, raw := range payload["trace"].([]any) {
					trace := raw.(map[string]any)
					if trace["fn"] == "example.com/repro-semantics.excludedBusinessValue" || trace["fn"] == "example.com/repro-semantics.sourceLocation" {
						panic("tracing exclusion ignored")
					}
					if trace["fn"] == "example.com/repro-semantics.deferred" && trace["type"] == "exit" {
						seen[sid]["goroutine-trace"] = true
					}
					if trace["fn"] == "example.com/repro-semantics.namedPanic" && trace["type"] == "exit" {
						err, _ := trace["error"].(map[string]any)
						seen[sid]["returned-error"] = trace["threw"] == false && err["message"] == "expected panic"
					}
				}
			}
		}
		for i := range responses {
			sid := fmt.Sprintf("S_go_%d", i)
			for _, kind := range []string{"backend_request", "trace_batch", "request_capture_manifest", "goroutine-trace", "returned-error"} {
				if !seen[sid][kind] {
					panic(fmt.Sprintf("%s missing %s", sid, kind))
				}
			}
		}
		fmt.Fprintf(os.Stderr, "validated %d events across %d concurrent scopes\n", len(events), len(seen))
	}
	_ = json.NewEncoder(os.Stdout).Encode(responses)
}
