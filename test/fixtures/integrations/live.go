package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	repro "github.com/getrepro/go-sdk"
)

func liveBusiness(ctx context.Context) (any, error) {
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	defer client.Close()
	if err := client.Set(ctx, "go-live-evidence", 1099, time.Minute).Err(); err != nil {
		return nil, err
	}
	value, err := client.Get(ctx, "go-live-evidence").Int()
	if err != nil {
		return nil, err
	}
	if value != 1099 {
		return nil, fmt.Errorf("business result changed: %d", value)
	}
	return map[string]any{"amount": value, "status": "paid"}, nil
}

func liveWorkflow(sdk *repro.Runtime) {
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result, err := liveBusiness(r.Context())
		if err != nil {
			w.WriteHeader(500)
			result = err.Error()
		}
		if r.URL.Path == "/full" {
			go func() { time.Sleep(45 * time.Millisecond); _ = largeResponse("late business work") }()
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer app.Close()
	send := func(path, actor string) error {
		req, _ := http.NewRequest("GET", app.URL+"/"+path, nil)
		req.Header.Set("X-Test-Actor", actor)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != 200 {
			return fmt.Errorf("capture changed business response: %d", resp.StatusCode)
		}
		return nil
	}
	rate0, rate1 := 0.0, 1.0
	scenarios := []struct {
		path, actor string
		policy      repro.LivePolicy
	}{
		{"zero", "zero", repro.LivePolicy{SampleRate: &rate0}},
		{"target", "target", repro.LivePolicy{SampleRate: &rate0, Targets: []repro.Target{{ActorIDs: []string{"target"}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}}}},
		{"expired", "expired", repro.LivePolicy{SampleRate: &rate0, Targets: []repro.Target{{ActorIDs: []string{"expired"}, ExpiresAt: time.Now().Add(-time.Hour).UnixMilli()}}}},
		{"group", "group", repro.LivePolicy{SampleRate: &rate0, Targets: []repro.Target{{Labels: map[string]string{"team": "operations"}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}}}},
		{"wrong-group", "wrong-group", repro.LivePolicy{SampleRate: &rate0, Targets: []repro.Target{{Labels: map[string]string{"team": "different"}, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}}}},
	}
	denied := os.Getenv("REPRO_TEST_QUOTA_DENIED") == "1"
	if denied {
		scenarios = scenarios[:0]
	}
	for _, scenario := range scenarios {
		if err := sdk.SetLiveCapture(scenario.policy); err != nil {
			panic(err)
		}
		if err := send(scenario.path, scenario.actor); err != nil {
			panic(err)
		}
	}
	if err := sdk.SetLiveCapture(repro.LivePolicy{SampleRate: &rate1}); err != nil {
		panic(err)
	}
	actor := "full"
	if denied {
		actor = "quota-denied"
	}
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- send("full", actor) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			panic(err)
		}
	}
	if !denied {
		if err := sdk.SetLiveCapture(repro.LivePolicy{SampleRate: &rate0}); err != nil {
			panic(err)
		}
		if err := send("continued", "full"); err != nil {
			panic(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := sdk.Shutdown(ctx)
	if (err != nil) != denied {
		panic(fmt.Sprintf("unexpected delivery/admission result: %v", err))
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"live": true, "quotaDenied": denied, "businessResponsesOK": true})
}
