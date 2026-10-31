package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	repro "github.com/getrepro/go-sdk"
)

func testOnlyBusiness(value int) int { return value * 3 }

func TestInstrumentedTestBinary(t *testing.T) {
	var mu sync.Mutex
	seen := false
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
		}
		mu.Lock()
		for _, event := range raw.Events {
			payload, _ := event["payload"].(map[string]any)
			traces, _ := payload["trace"].([]any)
			for _, entry := range traces {
				trace := entry.(map[string]any)
				if trace["fn"] == "example.com/repro-semantics.testOnlyBusiness" && trace["type"] == "exit" {
					values := trace["returnValue"].([]any)
					if values[0] == float64(42) {
						seen = true
					}
				}
			}
		}
		mu.Unlock()
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(raw.Events), "dropped": 0})
	}))
	defer collector.Close()
	sdk, err := repro.Init(repro.Config{TenantID: "test", AppID: "test", AppSecret: "test-only", IngestBase: collector.URL, CaptureRaw: true})
	if err != nil {
		t.Fatal(err)
	}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(testOnlyBusiness(14)) }))
	defer app.Close()
	req, _ := http.NewRequest("GET", app.URL, nil)
	req.Header.Set("x-bug-session-id", "S_test_binary")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = sdk.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !seen {
		t.Fatal("test-only business function was not automatically instrumented")
	}
}
