package privacy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestApprovedPolicyAndFallback(t *testing.T) {
	raw := json.RawMessage(`{"environment":"dev","strength":"medium","groups":[{"statements":[{"sid":"exact","when":{"scope":"FUNCTION","selector":"checkout","target":"trace.args"},"apply":{"action":"KEEP_EXACT"}},{"sid":"secret","when":{"scope":"FIELD","selector":"apiConfigService"},"apply":{"action":"DROP"}},{"sid":"email","when":{"scope":"FIELD","selector":"email"},"apply":{"action":"TOKENIZE"}}]}]}`)
	evaluator, err := New(raw)
	if err != nil {
		t.Fatal(err)
	}
	if evaluator.StatementCount != 3 || evaluator.Fingerprint == "" {
		t.Fatal("policy not loaded")
	}
	input := []any{map[string]any{"amount": 1234, "email": "customer@example.com", "apiConfigService": map[string]any{"secret": "do-not-capture"}, "nested": map[string]any{"apiConfigService": map[string]any{"password": "nested-secret"}}}}
	before, _ := json.Marshal(input)
	details := map[string]any{"appId": "APP_test", "appSecret": "test-only", "trace": map[string]any{"fn": "checkout"}}
	output, err := evaluator.Apply(context.Background(), "trace.args", input, details)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(output)
	for _, secret := range []string{"customer@example.com", "do-not-capture", "nested-secret"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("unprotected value: %s", secret)
		}
	}
	if !strings.Contains(string(data), "1234") {
		t.Fatalf("business value lost: %s", data)
	}
	after, _ := json.Marshal(input)
	if string(after) != string(before) {
		t.Fatal("input mutated")
	}
	again, err := evaluator.Apply(context.Background(), "trace.args", input, details)
	if err != nil || !reflect.DeepEqual(output, again) {
		t.Fatal("non-deterministic policy output")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, e := evaluator.Apply(context.Background(), "trace.args", input, details)
			if e != nil || !reflect.DeepEqual(output, got) {
				t.Error("concurrent policy isolation failed")
			}
		}()
	}
	wg.Wait()
	fallback, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := fallback.Apply(context.Background(), "request.headers", map[string]any{"authorization": "Bearer secret-token"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(protected)
	if strings.Contains(string(data), "secret-token") {
		t.Fatal("fallback exposed credential")
	}
}

func TestPolicyFetchAuthenticationScopeAndFailure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/v1/sdk/privacy-policy" || r.URL.Query().Get("serviceName") != "Billing Worker" || r.URL.Query().Get("environment") != "dev" || r.Header.Get("X-App-Id") != "APP_test" || r.Header.Get("X-App-Secret") != "secret" {
			t.Error("wrong policy scope/authentication")
		}
		w.WriteHeader(403)
	}))
	defer server.Close()
	c := Config{APIBase: server.URL + "/api", Environment: "dev"}
	if _, err := Fetch(context.Background(), c, "APP_test", "secret", "billing", "Billing Worker"); err == nil {
		t.Fatal("unauthorized policy fetch silently fell back")
	}
	c.CaptureRaw = true
	if _, err := Fetch(context.Background(), c, "APP_test", "secret", "billing", "Billing Worker"); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("raw capture fetched policy")
	}
}

func TestPolicyDeadlineDoesNotPoisonEvaluator(t *testing.T) {
	evaluator, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err = evaluator.Apply(ctx, "trace.args", []any{1}, nil); err == nil {
		t.Fatal("expired deadline ignored")
	}
	if _, err = evaluator.Apply(context.Background(), "trace.args", []any{1}, nil); err != nil {
		t.Fatal("evaluator remained interrupted", err)
	}
}

func TestMalformedEnabledPolicyNeverBecomesFallback(t *testing.T) {
	for _, body := range []string{`{}`, `{"enabled":true}`, `{"enabled":true,"policy":null}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		_, err := Fetch(context.Background(), Config{APIBase: server.URL}, "app", "secret", "name", "service")
		server.Close()
		if err == nil {
			t.Fatalf("invalid enabled policy accepted: %s", body)
		}
	}
	for _, raw := range []string{`{}`, `[]`, `{"groups":[]}`, `{"groups":[{"statements":[{"apply":{"action":"not-real"}}]}]}`} {
		if _, err := New(json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid policy silently fell back: %s", raw)
		}
	}
}
