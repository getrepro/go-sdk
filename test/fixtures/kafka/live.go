package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"time"

	repro "github.com/getrepro/go-sdk"
	kafka "github.com/segmentio/kafka-go"
)

func liveKafka() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	broker := os.Getenv("KAFKA_ADDR")
	admin, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		panic(err)
	}
	if err = admin.CreateTopics(kafka.TopicConfig{Topic: "uncorrelated-orders", NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		panic(err)
	}
	_ = admin.Close()
	// Simulate a producer with no Repro SDK or correlation headers.
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "uncorrelated-orders", RequiredAcks: kafka.RequireAll, BatchTimeout: time.Millisecond}
	if err = writer.WriteMessages(ctx, kafka.Message{Value: []byte("1000")}); err != nil {
		panic(err)
	}
	_ = writer.Close()
	var claims atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims.Add(1)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			panic(err)
		}
		check(body["actorId"] == "settlement-consumer", "live consumer actor lost")
		_ = json.NewEncoder(w).Encode(map[string]any{"captured": true, "sessionId": "S_live_kafka", "actionId": "A_live"})
	}))
	defer api.Close()
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
	sdk, err := repro.Init(repro.Config{TenantID: "test", AppID: "test", AppSecret: "test-only", ActorID: "settlement-consumer", IngestBase: collector.URL, Mode: "live", Live: repro.LiveConfig{APIBase: api.URL}, CaptureRaw: true})
	if err != nil {
		panic(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: "uncorrelated-orders", Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		panic(err)
	}
	check(len(message.Headers) == 0, "consumer headers mutated")
	check(calculateFee(1000) == 40, "business result changed")
	if err = reader.Close(); err != nil {
		panic(err)
	}
	if err = sdk.Shutdown(ctx); err != nil {
		panic(err)
	}
	check(claims.Load() == 1, "consumer claimed more than once")
	mu.Lock()
	defer mu.Unlock()
	request, traced := false, false
	for _, event := range events {
		check(event["session_id"] == "S_live_kafka" && event["actor_id"] == "settlement-consumer", "live correlation changed")
		payload := event["payload"].(map[string]any)
		if event["event_type"] == "backend_request" {
			request = payload["request"].(map[string]any)["method"] == "KAFKA_CONSUME"
		}
		if event["event_type"] == "trace_batch" {
			for _, raw := range payload["trace"].([]any) {
				trace := raw.(map[string]any)
				if trace["fn"] == "example.com/repro-kafka.calculateFee" && trace["type"] == "exit" {
					traced = trace["returnValue"].([]any)[0] == float64(40)
				}
			}
		}
	}
	check(request && traced, "uncorrelated live Kafka message missing request/function evidence")
	_ = json.NewEncoder(os.Stdout).Encode(events)
}
