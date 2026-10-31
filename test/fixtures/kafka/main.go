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
	kafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func calculateFee(amount int) int { return amount * 4 / 100 }
func handleOrder(message kafka.Message, producer *kgo.Client) error {
	var order map[string]int
	if err := json.Unmarshal(message.Value, &order); err != nil {
		return err
	}
	order["fee"] = calculateFee(order["amount"])
	value, _ := json.Marshal(order)
	record := &kgo.Record{Topic: "go-receipts", Key: message.Key, Value: value}
	done := make(chan error, 1)
	producer.Produce(context.Background(), record, func(returned *kgo.Record, err error) {
		check(returned == record, "franz record identity changed")
		done <- err
	})
	return <-done
}
func inspectReceipt(record *kgo.Record) int {
	var receipt map[string]int
	if err := json.Unmarshal(record.Value, &receipt); err != nil {
		panic(err)
	}
	return receipt["fee"]
}

func main() {
	if os.Getenv("KAFKA_LIVE_MODE") == "1" {
		liveKafka()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	broker := os.Getenv("KAFKA_ADDR")
	admin, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		panic(err)
	}
	if err = admin.CreateTopics(kafka.TopicConfig{Topic: "go-orders", NumPartitions: 1, ReplicationFactor: 1}, kafka.TopicConfig{Topic: "go-receipts", NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		panic(err)
	}
	_ = admin.Close()
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
	runtime, err := repro.Init(repro.Config{TenantID: "TENANT_test", AppID: "APP_test", AppSecret: "test-only", AppName: "kafka-test", IngestBase: collector.URL, CaptureRaw: true})
	if err != nil {
		panic(err)
	}
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "go-orders", Async: true, BatchTimeout: 10 * time.Millisecond, RequiredAcks: kafka.RequireAll}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: "go-orders", Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 100 * time.Millisecond})
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		panic(err)
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("go-receipts"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		panic(err)
	}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rejected" {
			record := &kgo.Record{Value: []byte(`{"amount":1000}`)}
			done := make(chan error, 1)
			producer.TryProduce(r.Context(), record, func(returned *kgo.Record, err error) {
				check(returned == record, "rejected record identity changed")
				done <- err
			})
			err := <-done
			check(err != nil && strings.Contains(err.Error(), "topic"), "missing-topic error lost")
			w.WriteHeader(202)
			_, _ = io.WriteString(w, "handled")
			return
		}
		msgs := []kafka.Message{{Key: []byte("first"), Value: []byte(`{"amount":1000}`)}, {Key: []byte("second"), Value: []byte(`{"amount":2500}`)}}
		if err := writer.WriteMessages(r.Context(), msgs...); err != nil {
			panic(err)
		}
		check(len(msgs[0].Headers) == 0 && len(msgs[1].Headers) == 0, "caller kafka-go messages mutated")
		w.WriteHeader(202)
	}))
	defer app.Close()
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", app.URL+"/order", nil)
		req.Header.Set("x-bug-session-id", fmt.Sprintf("S_kafka_%d", i))
		req.Header.Set("x-bug-action-id", fmt.Sprintf("A_%d", i))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	// Immediate async rejection must not be overwritten by Produce returning
	// normally. The application deliberately handles the error and stays healthy.
	for i := 0; i < 16; i++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", app.URL+"/rejected", nil)
		req.Header.Set("x-bug-session-id", "S_kafka_rejected")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		check(err == nil && resp.StatusCode == 202 && string(body) == "handled", "handled produce failure changed HTTP outcome")
	}
	if err = writer.Close(); err != nil {
		panic(err)
	}
	for i := 0; i < 4; i++ {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			panic(err)
		}
		if err = handleOrder(message, producer); err != nil {
			panic(err)
		}
	}
	if err = reader.Close(); err != nil {
		panic(err)
	}
	producer.Close()
	count, total := 0, 0
	for count < 4 {
		fetches := consumer.PollRecords(ctx, 4-count)
		if err = fetches.Err(); err != nil {
			panic(err)
		}
		if os.Getenv("KAFKA_RANGE_MODE") == "1" {
			for _, record := range fetches.Records() {
				count++
				total += inspectReceipt(record)
				if count == 4 {
					break
				}
				continue
			}
		} else {
			fetches.EachRecord(func(record *kgo.Record) { count++; total += inspectReceipt(record) })
		}
	}
	consumer.Close()
	check(total == 280, "business results changed")
	if err = runtime.Shutdown(ctx); err != nil {
		panic(err)
	}
	mu.Lock()
	defer mu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(events)
}
