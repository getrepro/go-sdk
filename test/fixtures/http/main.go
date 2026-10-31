package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	repro "github.com/getrepro/go-sdk"
)

func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func lateBusinessValue(amount int) int { return amount * 4 / 100 }

func main() {
	if os.Getenv("REPRO_LIFECYCLE") != "" {
		lifecycle()
		return
	}
	var mu sync.Mutex
	events := []map[string]any{}
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
	runtime, err := repro.Init(repro.Config{TenantID: "TENANT_test", AppID: "APP_test", AppSecret: "test-only", AppName: "http-test", IngestBase: collector.URL, CaptureRaw: true})
	if err != nil {
		panic(err)
	}
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check(r.Header.Get("x-bug-session-id") == "S_http", "session propagation")
		check(r.Header.Get("x-bug-action-id") == "A_http", "action propagation")
		check(r.Header.Get("x-repro-request-rid") != "", "parent request propagation")
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/result", http.StatusTemporaryRedirect)
		case "/result":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			check(string(body) == `{"amount":1099}`, "request body changed")
			_, _ = io.WriteString(w, `{"amount":1099,"receipt":"paid"}`)
		case "/large":
			_, _ = io.WriteString(w, strings.Repeat("x", (1<<20)+1))
		case "/stream":
			_, _ = io.WriteString(w, "first-")
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
			_, _ = io.WriteString(w, "second")
		}
	}))
	defer downstream.Close()
	done := make(chan struct{})
	readLate := make(chan struct{})
	background := make(chan int, 1)
	releaseBackground := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/panic":
			panic("business-panic-identity")
		case "/background":
			go func() { <-releaseBackground; background <- lateBusinessValue(2500) }()
			w.WriteHeader(202)
		case "/launch":
			duplexWorkflow()
			req, _ := http.NewRequest("POST", downstream.URL+"/redirect", strings.NewReader(`{"amount":1099}`))
			req.Header.Set("User-Header", "preserved")
			originalBody := req.Body
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				panic(err)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}
			_ = resp.Body.Close()
			check(string(body) == `{"amount":1099,"receipt":"paid"}`, "response body changed")
			check(req.Body == originalBody, "caller body pointer changed")
			check(len(req.Header) == 1 && req.Header.Get("User-Header") == "preserved", "caller headers mutated")
			for _, path := range []string{"/large", "/stream"} {
				resp, err = http.Get(downstream.URL + path)
				if err != nil {
					panic(err)
				}
				body, err = io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil {
					panic(err)
				}
				if path == "/large" {
					check(len(body) == (1<<20)+1, "large body truncated for caller")
				} else {
					check(string(body) == "first-second", "stream bytes changed")
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/late":
			resp, err := http.Get(downstream.URL + "/stream")
			if err != nil {
				panic(err)
			}
			go func() {
				defer close(done)
				<-readLate
				body, err := io.ReadAll(resp.Body)
				check(err == nil && string(body) == "first-second", "late response lost")
				_ = resp.Body.Close()
			}()
			w.WriteHeader(202)
		case "/partial":
			buffer := make([]byte, 3)
			_, _ = r.Body.Read(buffer)
			w.WriteHeader(204)
		case "/flush":
			w.(http.Flusher).Flush()
			w.WriteHeader(418)
		}
	}))
	defer upstream.Close()
	for _, path := range []string{"/launch", "/late", "/partial", "/flush", "/background"} {
		req, _ := http.NewRequest("POST", upstream.URL+path, bytes.NewBufferString("abcdef"))
		req.Header.Set("x-bug-session-id", "S_http")
		req.Header.Set("x-bug-action-id", "A_http")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if path == "/late" {
			close(readLate)
			<-done
		}
		if path == "/flush" {
			check(resp.StatusCode == 200, "flush status changed")
		}
	}
	close(releaseBackground)
	check(<-background == 100, "background result changed")
	req, _ := http.NewRequest("GET", upstream.URL+"/panic", nil)
	req.Header.Set("x-bug-session-id", "S_http")
	resp, panicErr := http.DefaultClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	check(panicErr != nil, "server panic swallowed by instrumentation")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		panic(err)
	}
	mu.Lock()
	defer mu.Unlock()
	check(len(events) > 0, "capture empty")
	requests := map[string]map[string]any{}
	lateExit, largeTruncated, backgroundTrace := false, false, false
	for _, event := range events {
		payload := event["payload"].(map[string]any)
		if event["event_type"] == "backend_request" {
			r := payload["request"].(map[string]any)
			requests[r["path"].(string)] = r
		}
		if event["event_type"] == "trace_batch" {
			for _, raw := range payload["trace"].([]any) {
				trace := raw.(map[string]any)
				if trace["fn"] == "example.com/repro-http.lateBusinessValue" && trace["type"] == "exit" {
					backgroundTrace = trace["returnValue"].([]any)[0] == float64(100)
				}
				if trace["fn"] != "net/http.Client.Do" || trace["type"] != "exit" {
					continue
				}
				ret := trace["returnValue"].([]any)[0].(map[string]any)
				if ret["body"] == "first-second" {
					lateExit = true
				}
				if body, ok := ret["body"].(map[string]any); ok && body["reason"] == "body_byte_limit" {
					largeTruncated = true
				}
			}
		}
	}
	check(backgroundTrace, "business goroutine outlived response and was lost")
	check(requests["/panic"]["status"] == float64(0) && requests["/panic"]["responseComplete"] == false, "panic falsely captured as a successful HTTP response")
	check(lateExit, "late streamed body not captured")
	check(largeTruncated, "missing explicit body limit evidence")
	check(requests["/flush"]["status"] == float64(200), "captured flush status wrong")
	partial, ok := requests["/partial"]["body"].(map[string]any)
	check(ok && partial["reason"] == "body_not_fully_consumed", "partial request presented as complete")
	check(requests["/result"]["parentRequestRid"] == requests["/launch"]["rid"], "downstream not linked to upstream")
	fmt.Fprintln(os.Stderr, "validated HTTP propagation, redirects, streaming, partial reads, response interfaces, identity and late body completion")
	_ = json.NewEncoder(os.Stdout).Encode(events)
}
