package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestAutomaticKafka(t *testing.T) {
	for _, mode := range []string{"callback", "range", "live"} {
		t.Run(mode, func(t *testing.T) { testKafka(t, mode) })
	}
}

func testKafka(t *testing.T, mode string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "kafka-app")
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "build", "-race", "-o", binary, ".")
	cmd.Dir = filepath.Join(root, "test/fixtures/kafka")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	broker, err := kafka.Run(ctx, "confluentinc/cp-kafka:7.6.1", kafka.WithClusterID("MkU3OEVBNTcwNTJENDM2Qk"))
	if broker != nil {
		t.Cleanup(func() {
			if err := broker.Terminate(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	brokers, err := broker.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cmd = exec.CommandContext(ctx, binary)
	cmd.Env = append(os.Environ(), "KAFKA_ADDR="+brokers[0])
	if mode == "range" {
		cmd.Env = append(cmd.Env, "KAFKA_RANGE_MODE=1")
	}
	if mode == "live" {
		cmd.Env = append(cmd.Env, "KAFKA_LIVE_MODE=1")
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		t.Fatalf("Kafka workflow: %v\n%s", err, stderr.String())
	}
	var events []map[string]any
	if err = json.Unmarshal(data, &events); err != nil {
		t.Fatal(err)
	}
	if mode == "live" {
		if len(events) < 3 {
			t.Fatal("live consumer empty")
		}
		return
	}
	rejected, handled, closed := 0, 0, 0
	for _, event := range events {
		if event["session_id"] != "S_kafka_rejected" {
			continue
		}
		payload := event["payload"].(map[string]any)
		if event["event_type"] == "request_capture_manifest" {
			closed++
		}
		if event["event_type"] != "backend_request" {
			continue
		}
		request := payload["request"].(map[string]any)
		switch request["method"] {
		case "KAFKA_PUBLISH":
			rejected++
			body, _ := json.Marshal(request["respBody"])
			if request["status"] != float64(500) || !strings.Contains(string(body), "topic") {
				t.Fatalf("async rejection falsely succeeded or lost its error: %v", request)
			}
		case "POST":
			handled++
			if request["status"] != float64(202) {
				t.Fatal("handled failure changed application status")
			}
		}
	}
	if rejected != 16 || handled != 16 || closed != 32 {
		t.Fatalf("async rejection lost/duplicated evidence: rejected=%d handled=%d completed=%d", rejected, handled, closed)
	}
	for _, sid := range []string{"S_kafka_0", "S_kafka_1"} {
		methods := map[string]int{}
		fees := map[float64]bool{}
		inspections := 0
		counts := map[string]map[string]int{}
		manifests := map[string]map[string]any{}
		for _, event := range events {
			if event["session_id"] != sid {
				continue
			}
			rid := event["request_id"].(string)
			if counts[rid] == nil {
				counts[rid] = map[string]int{}
			}
			kind := event["event_type"].(string)
			counts[rid][kind]++
			payload := event["payload"].(map[string]any)
			switch kind {
			case "backend_request":
				request := payload["request"].(map[string]any)
				methods[request["method"].(string)]++
				if request["status"] != float64(200) && request["status"] != float64(202) {
					t.Fatalf("unexpected status: %v", request)
				}
			case "trace_batch":
				for _, raw := range payload["trace"].([]any) {
					trace := raw.(map[string]any)
					if trace["fn"] == "example.com/repro-kafka.calculateFee" && trace["type"] == "exit" {
						values := trace["returnValue"].([]any)
						fees[values[0].(float64)] = true
					}
					if trace["fn"] == "example.com/repro-kafka.inspectReceipt" && trace["type"] == "exit" {
						inspections++
					}
				}
			case "request_capture_manifest":
				manifests[rid] = payload["requestCaptureManifest"].(map[string]any)
			}
		}
		if methods["POST"] != 1 || methods["KAFKA_PUBLISH"] != 3 || methods["KAFKA_CONSUME"] != 4 {
			t.Errorf("%s missing/duplicated correlated requests: %v", sid, methods)
		}
		if !fees[40] || !fees[100] {
			t.Errorf("%s missing exact business function results: %v", sid, fees)
		}
		if inspections != 2 {
			t.Errorf("%s consumer-loop business traces missing/duplicated: %d", sid, inspections)
		}
		for rid, count := range counts {
			manifest := manifests[rid]
			if manifest == nil {
				t.Errorf("unfinished request %s", rid)
				continue
			}
			expected := manifest["expected"].(map[string]any)
			if expected["traceBatch"] != float64(count["trace_batch"]) || expected["backendRequest"] != float64(count["backend_request"]) {
				t.Errorf("manifest mismatch %s", rid)
			}
		}
	}
}
