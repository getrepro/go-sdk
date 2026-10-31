package delivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestRetryPreservesBytesAndScopeOrder(t *testing.T) {
	var mu sync.Mutex
	var got []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, string(data))
		attempt := len(got)
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(202)
			_, _ = io.WriteString(w, `{"accepted":1,"dropped":0}`)
		}
	}))
	defer server.Close()
	q := New(Config{URL: server.URL, MaxAttempts: 3})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer q.Close(ctx)
	for _, batch := range []string{`{"events":[{"ingest_id":"stable"}]}`, `{"events":[{"ingest_id":"next"}]}`} {
		if err := q.Enqueue([]byte(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 || got[0] != got[1] || got[1] == got[2] {
		t.Fatalf("retry/order violated: %v", got)
	}
}

func TestQuotaDeniedDoesNotRetry(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(402) }))
	defer server.Close()
	q := New(Config{URL: server.URL})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer q.Close(ctx)
	_ = q.Enqueue([]byte(`{"events":[{}]}`))
	if q.Flush(ctx) == nil {
		t.Fatal("must report refused evidence")
	}
	if calls != 1 {
		t.Fatalf("retried quota refusal %d times", calls)
	}
}

func TestBackpressureAndShutdown(t *testing.T) {
	q := New(Config{URL: "http://127.0.0.1:1", MaxBytes: 2})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Enqueue([]byte("too large")); err != ErrFull {
		t.Fatalf("got %v", err)
	}
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue([]byte("{}")); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
}

func TestRedirectNeverForwardsCredentialsOrEvidence(t *testing.T) {
	contacted := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { contacted = true; w.WriteHeader(202) }))
	defer target.Close()
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer collector.Close()
	q := New(Config{URL: collector.URL, AppSecret: "must-not-leak"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer q.Close(ctx)
	if err := q.Enqueue([]byte(`{"events":[{"business":"private"}]}`)); err != nil {
		t.Fatal(err)
	}
	if q.Flush(ctx) == nil || contacted {
		t.Fatal("ingest redirect leaked credentials/evidence or was reported as delivered")
	}
}

func TestReceiptMustAcknowledgeEveryEvent(t *testing.T) {
	for _, receipt := range []string{`{"accepted":2,"dropped":0}`, `{"accepted":1,"dropped":1}`, `{"accepted":0,"dropped":0}`, `{"accepted":3,"dropped":0}`, `{"accepted":2}`, ``, `{`} {
		t.Run(receipt, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(202)
				_, _ = io.WriteString(w, receipt)
			}))
			defer server.Close()
			q := New(Config{URL: server.URL})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			defer q.Close(ctx)
			data, _ := json.Marshal(map[string]any{"events": []any{map[string]any{"ingest_id": "a"}, map[string]any{"ingest_id": "b"}}})
			if err := q.Enqueue(data); err != nil {
				t.Fatal(err)
			}
			err := q.Flush(ctx)
			if (err == nil) != (receipt == `{"accepted":2,"dropped":0}`) {
				t.Fatalf("incorrect receipt result: %v", err)
			}
			if calls != 1 {
				t.Fatal("permanent receipt rejection retried")
			}
		})
	}
}
