package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	repro "github.com/getrepro/go-sdk"
)

func priceLine(price, quantity int) int { return price * quantity }
func orderTotal(prices []int) int {
	sum := 0
	for _, price := range prices {
		sum += priceLine(price, 3)
	}
	return sum
}

func performance() {
	mode := os.Getenv("REPRO_PERFORMANCE")
	var uploads atomic.Int64
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch struct {
			Events []json.RawMessage `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			panic(err)
		}
		uploads.Add(1)
		if mode == "unavailable" {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(202)
			_ = json.NewEncoder(w).Encode(map[string]int{"accepted": len(batch.Events), "dropped": 0})
		}
	}))
	defer collector.Close()
	var sdk *repro.Runtime
	if mode != "baseline" {
		var err error
		sdk, err = repro.Init(repro.Config{TenantID: "test", AppID: "test", AppSecret: "test-only", AppName: "perf", IngestBase: collector.URL, CaptureRaw: true, QueueBytes: 1 << 20})
		if err != nil {
			panic(err)
		}
	}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(orderTotal([]int{100, 200, 300, 400, 500}))
	}))
	defer app.Close()
	const requests = 250
	durations := make([]int64, 0, requests)
	var high uint64
	var total int
	for i := 0; i < requests; i++ {
		req, _ := http.NewRequest("GET", app.URL+"/order", nil)
		if mode != "unsampled" {
			req.Header.Set("x-bug-session-id", "S_performance")
			req.Header.Set("x-bug-action-id", "A_price")
		}
		start := time.Now()
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		var value int
		err = json.NewDecoder(response.Body).Decode(&value)
		_ = response.Body.Close()
		if err != nil || value != 4500 {
			panic("application result changed")
		}
		total += value
		durations = append(durations, time.Since(start).Microseconds())
		if i%25 == 0 {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > high {
				high = stats.HeapAlloc
			}
		}
	}
	failed := false
	if sdk != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		failed = sdk.Shutdown(ctx) != nil
	}
	if mode == "unavailable" && !failed {
		panic("undelivered evidence silently marked successful")
	}
	if mode != "unavailable" && failed {
		panic("capture failed")
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	if mode == "unsampled" && uploads.Load() != 0 {
		panic("unsampled HTTP created upload traffic")
	}
	if mode == "captured" && uploads.Load() == 0 {
		panic("captured scenario produced no evidence")
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": mode, "requests": requests, "checksum": total, "p50us": durations[requests/2], "p95us": durations[requests*95/100], "peakObservedHeap": high, "uploads": uploads.Load(), "deliveryFailureReported": failed})
	fmt.Fprint(os.Stderr, "")
}
