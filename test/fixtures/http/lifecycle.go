package main

import (
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

func lifecycle() {
	mode := os.Getenv("REPRO_LIFECYCLE")
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
	config := repro.Config{TenantID: "test", AppID: "test", AppSecret: "test-only", IngestBase: collector.URL, CaptureRaw: true}
	if mode == "deadline" {
		config.DrainTimeout = 75 * time.Millisecond
	}
	if mode == "overflow" {
		config.MaxConcurrentScopes = 1
	}
	sdk, err := repro.Init(config)
	if err != nil {
		panic(err)
	}
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "late-payment-verified") }))
	defer downstream.Close()
	release, entered, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			if mode == "overflow" {
				close(entered)
				<-release
			} else {
				go func() {
					defer close(done)
					close(entered)
					<-release
					if mode == "shutdown" {
						response, err := http.Get(downstream.URL + "/payment")
						if err != nil {
							panic(err)
						}
						body, err := io.ReadAll(response.Body)
						_ = response.Body.Close()
						check(err == nil && string(body) == "late-payment-verified", "shutdown changed late HTTP work")
					}
				}()
			}
		}
		w.WriteHeader(200)
	}))
	defer app.Close()
	send := func(path string) {
		req, _ := http.NewRequest("GET", app.URL+path, nil)
		req.Header.Set("x-bug-session-id", "S_lifecycle")
		response, err := http.DefaultClient.Do(req)
		check(err == nil, "application request failed")
		check(response.StatusCode == 200, "application status changed")
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	if mode == "overflow" {
		go func() { defer close(done); send("/hold") }()
		<-entered
		send("/overflow")
		close(release)
		<-done
	} else {
		send("/hold")
		<-entered
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if mode == "shutdown" {
		time.AfterFunc(75*time.Millisecond, func() { close(release) })
	}
	err = sdk.Shutdown(ctx)
	if mode == "deadline" {
		close(release)
		<-done
	}
	check((err != nil) == (mode != "shutdown"), fmt.Sprintf("incorrect shutdown outcome: %v", err))
	mu.Lock()
	defer mu.Unlock()
	encoded, _ := json.Marshal(events)
	if mode == "shutdown" {
		check(strings.Contains(string(encoded), "net/http.Client.Do") && strings.Contains(string(encoded), "late-payment-verified"), "shutdown discarded admitted library evidence")
	}
	if mode == "deadline" {
		check(strings.Contains(string(encoded), "unfinished_activity_deadline"), "incomplete work silently completed")
	}
	if mode == "overflow" {
		check(strings.Contains(string(encoded), "maximum_concurrent_captures"), "overflow missing from manifest")
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": mode, "events": len(events), "businessOK": true})
}
