package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func container(t *testing.T, image, port string, env map[string]string) (string, string) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{Image: image, ExposedPorts: []string{port}, Env: env, WaitingFor: wait.ForListeningPort(nat.Port(port)).WithStartupTimeout(2 * time.Minute)}
	if strings.HasPrefix(image, "mongo:") {
		req.Cmd = []string{"--replSet", "rs0", "--bind_ip_all"}
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		t.Cleanup(func() {
			if err := c.Terminate(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(image, "mongo:") {
		code, _, err := c.Exec(ctx, []string{"mongosh", "--quiet", "--eval", `rs.initiate({_id:'rs0',members:[{_id:0,host:'127.0.0.1:27017'}]}); for(let i=0;i<200;i++){if(db.hello().isWritablePrimary)quit(0);sleep(100)} quit(1)`})
		if err != nil || code != 0 {
			t.Fatalf("replica set init: exit %d: %v", code, err)
		}
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := c.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatal(err)
	}
	return host, mapped.Port()
}

func TestAutomaticDatabaseCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "go-integration-app")
	build := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "build", "-race", "-o", binary, ".")
	build.Dir = filepath.Join(root, "test/fixtures/integrations")
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("instrumented build: %v\n%s", err, out)
	}
	pgHost, pgPort := container(t, "postgres:16-alpine", "5432/tcp", map[string]string{"POSTGRES_PASSWORD": "test-only", "POSTGRES_DB": "repro_test"})
	redisHost, redisPort := container(t, "redis:7.4-alpine", "6379/tcp", nil)
	mongoHost, mongoPort := container(t, "mongo:7.0", "27017/tcp", nil)
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(os.Environ(), "POSTGRES_URL="+fmt.Sprintf("postgres://postgres:test-only@%s:%s/repro_test?sslmode=disable", pgHost, pgPort), "REDIS_ADDR="+redisHost+":"+redisPort, "MONGO_URL=mongodb://"+mongoHost+":"+mongoPort+"/?directConnection=true&replicaSet=rs0")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		t.Fatalf("business workflow: %v\n%s", err, stderr.String())
	}
	var result struct {
		Responses map[string]any   `json:"responses"`
		Events    []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sql", "pgx", "redis", "mongo", "mongo-v1"} {
		t.Run(name, func(t *testing.T) {
			var changes []map[string]any
			var manifest map[string]any
			var traceCount int
			for _, event := range result.Events {
				if event["session_id"] != "S_go_"+name {
					continue
				}
				payload := event["payload"].(map[string]any)
				switch event["event_type"] {
				case "db_change":
					for _, db := range payload["db"].([]any) {
						changes = append(changes, db.(map[string]any))
					}
				case "request_capture_manifest":
					manifest = payload["requestCaptureManifest"].(map[string]any)
				case "trace_batch":
					traceCount++
				}
			}
			if len(changes) == 0 || traceCount == 0 || manifest == nil {
				t.Fatalf("missing evidence: changes=%d traces=%d manifest=%v", len(changes), traceCount, manifest)
			}
			expected := manifest["expected"].(map[string]any)
			if int(expected["dbChange"].(float64)) != len(changes) || int(expected["traceBatch"].(float64)) != traceCount {
				t.Fatal("manifest/evidence mismatch")
			}
			switch name {
			case "sql":
				if !hasResultRow(changes, "amount", float64(1099)) {
					t.Fatal("SQL scanned business result missing")
				}
				if !hasDBError(changes, "duplicate key") {
					t.Fatal("SQL error evidence missing")
				}
			case "pgx":
				for _, amount := range []float64{10, 20, 30, 12, 24} {
					if !hasResultArrayValue(changes, amount) {
						t.Fatalf("PG decoded/copy row value %v missing", amount)
					}
				}
				for _, value := range []float64{41, 42} {
					found := false
					for _, change := range changes {
						query, _ := change["query"].(map[string]any)
						params, _ := query["parameters"].([]any)
						if query["sql"] == "SELECT $1::int" && len(params) == 1 && params[0] == value && hasResultRow([]map[string]any{change}, "int4", value) {
							found = true
						}
					}
					if !found {
						t.Fatalf("batch SQL, parameter and result %v not correlated: %v", value, changes)
					}
				}
			case "redis":
				found := false
				for _, change := range changes {
					meta, _ := change["resultMeta"].(map[string]any)
					if meta["cacheHit"] == true && meta["value"] == "14" {
						found = true
					}
				}
				if !found {
					t.Fatal("Redis exact cached value missing")
				}
			case "mongo":
				for _, amount := range []float64{1299, 1500, 2500} {
					if !hasResultRow(changes, "amount", amount) {
						t.Fatalf("Mongo decoded document amount %v missing", amount)
					}
				}
				if !hasDBError(changes, "duplicate key") {
					t.Fatal("Mongo error evidence missing")
				}
				if !hasDBError(changes, "no documents") {
					t.Fatal("Mongo missing document error missing")
				}
				committed, aborted, partial := false, false, false
				for _, change := range changes {
					committed = committed || change["op"] == "commitTransaction"
					aborted = aborted || change["op"] == "abortTransaction"
					meta, _ := change["resultMeta"].(map[string]any)
					partial = partial || meta["reason"] == "result_not_fully_consumed"
				}
				if !committed || !aborted || !partial {
					t.Fatalf("Mongo transaction/partial evidence missing: commit=%v abort=%v partial=%v", committed, aborted, partial)
				}
			case "mongo-v1":
				if !hasResultRow(changes, "amount", float64(1599)) || !hasDBError(changes, "no documents") {
					t.Fatal("MongoDB v1 result/error evidence missing")
				}
			}
			t.Logf("%d DB events, %d trace batches; manifest matches", len(changes), traceCount)
		})
	}
}

func hasResultRow(changes []map[string]any, key string, want any) bool {
	for _, change := range changes {
		meta, _ := change["resultMeta"].(map[string]any)
		rows, _ := meta["rows"].([]any)
		for _, row := range rows {
			value, _ := row.(map[string]any)
			if value[key] == want {
				return true
			}
		}
	}
	return false
}
func hasResultArrayValue(changes []map[string]any, want float64) bool {
	for _, change := range changes {
		meta, _ := change["resultMeta"].(map[string]any)
		rows, _ := meta["rows"].([]any)
		for _, row := range rows {
			values, _ := row.([]any)
			if len(values) > 1 && values[1] == want {
				return true
			}
		}
	}
	return false
}
func hasDBError(changes []map[string]any, message string) bool {
	for _, change := range changes {
		err, _ := change["error"].(map[string]any)
		text, _ := err["message"].(string)
		if strings.Contains(text, message) {
			return true
		}
	}
	return false
}
