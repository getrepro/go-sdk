package capture

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getrepro/go-sdk/internal/bridge"
	"github.com/getrepro/go-sdk/internal/privacy"
)

type configTransport func(*http.Request) (*http.Response, error)

func (f configTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOptionalEndpointsThroughInitAndDelivery(t *testing.T) {
	const collector = "https://log-collector-5rj86.ondigitalocean.app"
	const api = "https://repro-api-d7288.ondigitalocean.app/api"
	const policy = `{"environment":"dev","groups":[{"statements":[{"sid":"drop","when":{"scope":"FIELD","selector":"secretField"},"apply":{"action":"DROP"}}]}]}`
	for _, tc := range []struct {
		name, mode, ingest, privacyAPI, liveAPI string
		raw, inline                             bool
		status                                  int
	}{
		{name: "staging defaults"}, {name: "live defaults", mode: "live"},
		{name: "collector override", ingest: "http://collector.local"},
		{name: "private API inherited by live", mode: "live", privacyAPI: "http://api.local/api"},
		{name: "independent live override", mode: "live", liveAPI: "http://admission.local/api"},
		{name: "explicit endpoints", ingest: "http://collector.local/", privacyAPI: "http://api.local/api/"},
		{name: "raw bypass", raw: true}, {name: "inline policy", inline: true},
		{name: "policy failure stays closed", status: 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var urls []string
			var captured strings.Builder
			policyCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-App-Secret") != "test-only" {
					t.Error("authentication missing")
				}
				switch r.URL.Path {
				case "/api/v1/sdk/privacy-policy":
					mu.Lock()
					policyCalls++
					mu.Unlock()
					if r.URL.Query().Get("environment") != "dev" || r.URL.Query().Get("serviceName") != "Orders" {
						t.Error("policy scope changed")
					}
					if tc.status != 0 {
						w.WriteHeader(tc.status)
						return
					}
					_, _ = w.Write([]byte(`{"enabled":true,"policy":` + policy + `}`))
				case "/api/v1/live-sessions/claim":
					_, _ = w.Write([]byte(`{"captured":true,"sessionId":"S_parent","childSessionId":"S_child","actionId":"A_test"}`))
				case "/v1/ingest/events":
					var batch struct {
						Events []json.RawMessage `json:"events"`
					}
					if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
						t.Error(err)
					}
					mu.Lock()
					for _, e := range batch.Events {
						captured.Write(e)
					}
					mu.Unlock()
					w.WriteHeader(202)
					_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(batch.Events), "dropped": 0})
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			oldTransport, oldInstrumented := http.DefaultTransport, bridge.Instrumented
			http.DefaultTransport = configTransport(func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				urls = append(urls, r.URL.Scheme+"://"+r.URL.Host+r.URL.Path)
				mu.Unlock()
				copy := r.Clone(r.Context())
				copy.URL.Scheme = target.Scheme
				copy.URL.Host = target.Host
				return transport.RoundTrip(copy)
			})
			bridge.Instrumented = true
			defer func() { http.DefaultTransport = oldTransport; bridge.Instrumented = oldInstrumented }()
			config := Config{TenantID: "test", AppID: "test", AppSecret: "test-only", ServiceName: "Orders", Mode: tc.mode, IngestBase: tc.ingest, CaptureRaw: tc.raw, Privacy: privacy.Config{APIBase: tc.privacyAPI}, Live: LiveConfig{APIBase: tc.liveAPI}}
			if tc.inline {
				config.Privacy.Policy = json.RawMessage(policy)
			}
			runtime, err := Init(config)
			if tc.status != 0 {
				if err == nil || runtime != nil || active.Load() != nil {
					t.Fatal("policy failure admitted capture")
				}
				if !strings.Contains(err.Error(), "HTTP 503") {
					t.Fatalf("unexpected initialization failure: %v", err)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(urls) != 1 || urls[0] != api+"/v1/sdk/privacy-policy" {
					t.Fatalf("capture uploaded despite policy fetch failure: %v", urls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := runtime.Shutdown(ctx); err != nil {
					t.Error(err)
				}
			}()
			ingestBase := strings.TrimRight(tc.ingest, "/")
			if ingestBase == "" {
				ingestBase = collector
			}
			apiBase := strings.TrimRight(tc.privacyAPI, "/")
			if apiBase == "" {
				apiBase = api
			}
			liveBase := tc.liveAPI
			if liveBase == "" {
				liveBase = apiBase
			}
			if runtime.config.IngestBase != tc.ingest && tc.ingest != "" {
				t.Fatal("collector override changed")
			}
			if !tc.raw && runtime.policy.StatementCount != 1 {
				t.Fatal("approved policy not loaded")
			}
			expected := map[string]bool{ingestBase + "/v1/ingest/events": false}
			if !tc.raw && !tc.inline {
				expected[apiBase+"/v1/sdk/privacy-policy"] = false
			}
			if tc.mode == "live" {
				sid, _ := runtime.admit(context.Background(), "actor")
				if sid != "S_child" {
					t.Fatal("live admission failed")
				}
				expected[liveBase+"/v1/live-sessions/claim"] = false
			}
			scope, previous := Begin("S_test", "A_test", "request", "http")
			defer Restore(previous)
			if scope == nil {
				t.Fatal("capture not started")
			}
			if err := scope.Finish(map[string]any{"key": "POST /orders", "body": map[string]any{"secretField": "private-value"}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := runtime.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, u := range urls {
				if _, ok := expected[u]; !ok {
					t.Errorf("unexpected destination: %s", u)
				}
				expected[u] = true
			}
			for u, seen := range expected {
				if !seen {
					t.Errorf("endpoint never called: %s", u)
				}
			}
			if (tc.raw || tc.inline) && policyCalls != 0 {
				t.Error("unnecessary policy fetch")
			}
			if !tc.raw && strings.Contains(captured.String(), "private-value") {
				t.Fatal("policy was not applied before upload")
			}
			if tc.raw && !strings.Contains(captured.String(), "private-value") {
				t.Fatal("raw capture changed")
			}
		})
	}
}

func TestInvalidExplicitEndpointDoesNotUseHostedFallback(t *testing.T) {
	old := bridge.Instrumented
	bridge.Instrumented = true
	defer func() { bridge.Instrumented = old }()
	for _, endpoint := range []string{"not-a-url", "ftp://example.test", "https://user:password@example.test"} {
		for _, field := range []string{"collector", "privacy", "live"} {
			t.Run(field+"/"+endpoint, func(t *testing.T) {
				config := Config{TenantID: "test", AppID: "test", AppSecret: "test-only"}
				switch field {
				case "collector":
					config.IngestBase = endpoint
				case "privacy":
					config.Privacy.APIBase = endpoint
				case "live":
					config.Mode = "live"
					config.Live.APIBase = endpoint
				}
				runtime, err := Init(config)
				if err == nil || runtime != nil || active.Load() != nil {
					t.Fatal("invalid explicit endpoint accepted")
				}
			})
		}
	}
}
