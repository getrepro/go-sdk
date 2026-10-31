package capture

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestLiveAdmissionUsesExistingAPIContract(t *testing.T) {
	var mu sync.Mutex
	var claims []map[string]any
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/live-sessions/claim" || r.Header.Get("X-App-Secret") != "test-secret" {
			t.Error("wrong admission endpoint/auth")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		mu.Lock()
		claims = append(claims, body)
		mu.Unlock()
		policy := body["liveCapture"].(map[string]any)
		if policy["sampleRate"] == float64(0) {
			_, _ = w.Write([]byte(`{"captured":false}`))
			return
		}
		_, _ = w.Write([]byte(`{"captured":true,"sessionId":"parent","childSessionId":"child","actionId":"action"}`))
	}))
	defer api.Close()
	rate := 1.0
	r := &Runtime{config: Config{Mode: "live", AppID: "APP_test", AppSecret: "test-secret", Live: LiveConfig{APIBase: api.URL + "/api", Policy: LivePolicy{SampleRate: &rate}}}}
	if err := r.initLive(); err != nil {
		t.Fatal(err)
	}
	sid, aid := r.admit(context.Background(), "customer-42")
	if sid != "child" || aid != "action" {
		t.Fatalf("wrong correlation: %q %q", sid, aid)
	}
	rate = 0
	if err := r.SetLiveCapture(LivePolicy{SampleRate: &rate, Targets: []Target{{ActorIDs: []string{"customer-99"}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}}}); err != nil {
		t.Fatal(err)
	}
	sid, _ = r.admit(context.Background(), "customer-42")
	if sid != "" {
		t.Fatal("ignored server sampling decision")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(claims) != 2 || claims[0]["captureMode"] != "backend-only" || claims[0]["actorId"] != "customer-42" {
		t.Fatal("wrong actor or mode")
	}
	second := claims[1]["liveCapture"].(map[string]any)
	if len(second["targets"].([]any)) != 1 {
		t.Fatal("runtime targets not forwarded")
	}
}

func TestLiveAdmissionBoundsOutageAndRespectsDenial(t *testing.T) {
	for _, code := range []int{402, 403, 429, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
			defer api.Close()
			r := &Runtime{config: Config{Mode: "live", Live: LiveConfig{APIBase: api.URL}}}
			if err := r.initLive(); err != nil {
				t.Fatal(err)
			}
			sid, _ := r.admit(context.Background(), "actor")
			if sid != "" || r.err == nil {
				t.Fatal("denied session admitted or denial invisible")
			}
		})
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(150 * time.Millisecond):
		}
	}))
	defer api.Close()
	r := &Runtime{config: Config{Mode: "live", Live: LiveConfig{APIBase: api.URL, AdmissionTimeout: 25 * time.Millisecond}}}
	if err := r.initLive(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	sid, _ := r.admit(context.Background(), "actor")
	if sid != "" || time.Since(start) > 500*time.Millisecond {
		t.Fatal("admission outage blocked caller")
	}
}

func TestLivePolicyIsImmutableAndValidated(t *testing.T) {
	rate := 0.5
	input := LivePolicy{SampleRate: &rate, Targets: []Target{{ActorIDs: []string{"actor"}, Labels: map[string]string{"plan": "enterprise"}, ExpiresAt: 1}}}
	copy, err := copyLivePolicy(input)
	if err != nil {
		t.Fatal(err)
	}
	rate = 2
	input.Targets[0].ActorIDs[0] = "changed"
	input.Targets[0].Labels["plan"] = "changed"
	if *copy.SampleRate != 0.5 || copy.Targets[0].ActorIDs[0] != "actor" || copy.Targets[0].Labels["plan"] != "enterprise" {
		t.Fatal("runtime policy aliases caller input")
	}
	if _, err = copyLivePolicy(input); err == nil {
		t.Fatal("out-of-range rate accepted")
	}
}
