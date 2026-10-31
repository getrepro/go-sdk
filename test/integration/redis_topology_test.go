package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRedisClusterAndSentinel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "redis-topology")
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "build", "-race", "-o", binary, ".")
	cmd.Dir = filepath.Join(root, "test/fixtures/integrations")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, mode := range []string{"cluster", "sentinel"} {
		t.Run(mode, func(t *testing.T) {
			netw, err := network.New(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := netw.Remove(context.Background()); err != nil {
					t.Error(err)
				}
			})
			dialMap := map[string]string{}
			start := func(req testcontainers.ContainerRequest) (testcontainers.Container, string, string) {
				t.Helper()
				req.Image = "redis:7.4-alpine"
				req.Networks = []string{netw.Name}
				req.ExposedPorts = []string{"6379/tcp"}
				req.WaitingFor = wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute)
				c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
				if c != nil {
					t.Cleanup(func() {
						if e := c.Terminate(context.Background()); e != nil {
							t.Error(e)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
				ip, err := c.ContainerIP(ctx)
				if err != nil {
					t.Fatal(err)
				}
				host, err := c.Host(ctx)
				if err != nil {
					t.Fatal(err)
				}
				port, err := c.MappedPort(ctx, "6379/tcp")
				if err != nil {
					t.Fatal(err)
				}
				internal, external := ip+":6379", host+":"+port.Port()
				dialMap[internal] = external
				return c, internal, external
			}
			var seed string
			if mode == "cluster" {
				nodes := []string{}
				var first testcontainers.Container
				for i := 0; i < 3; i++ {
					c, addr, external := start(testcontainers.ContainerRequest{Cmd: []string{"redis-server", "--cluster-enabled", "yes", "--cluster-node-timeout", "2000", "--appendonly", "no"}})
					nodes = append(nodes, addr)
					if i == 0 {
						first = c
						seed = external
					}
				}
				args := append([]string{"redis-cli", "--cluster", "create"}, nodes...)
				args = append(args, "--cluster-replicas", "0", "--cluster-yes")
				code, out, err := first.Exec(ctx, args)
				if err != nil || code != 0 {
					var b bytes.Buffer
					if out != nil {
						_, _ = b.ReadFrom(out)
					}
					t.Fatalf("create cluster: %v code %d %s", err, code, b.String())
				}
				if err := wait.ForExec([]string{"sh", "-c", "redis-cli cluster info | grep -q cluster_state:ok"}).WithStartupTimeout(time.Minute).WaitUntilReady(ctx, first); err != nil {
					t.Fatal(err)
				}
			} else {
				_, master, _ := start(testcontainers.ContainerRequest{})
				masterHost, _, _ := strings.Cut(master, ":")
				replica, _, _ := start(testcontainers.ContainerRequest{Cmd: []string{"redis-server", "--replicaof", masterHost, "6379"}})
				if err := wait.ForExec([]string{"sh", "-c", "redis-cli info replication | grep -q master_link_status:up"}).WithStartupTimeout(time.Minute).WaitUntilReady(ctx, replica); err != nil {
					t.Fatal(err)
				}
				config := fmt.Sprintf("port 6379\nsentinel monitor primary %s 6379 1\nsentinel down-after-milliseconds primary 1000\nsentinel failover-timeout primary 5000\n", masterHost)
				_, _, seed = start(testcontainers.ContainerRequest{Cmd: []string{"redis-server", "/tmp/sentinel.conf", "--sentinel"}, Files: []testcontainers.ContainerFile{{Reader: strings.NewReader(config), ContainerFilePath: "/tmp/sentinel.conf", FileMode: 0666}}})
			}
			mapping, _ := json.Marshal(dialMap)
			cmd := exec.CommandContext(ctx, binary)
			cmd.Env = append(os.Environ(), "REPRO_TEST_WORKFLOWS=redis-topology", "REDIS_TOPOLOGY="+mode, "REDIS_SEEDS="+seed, "REDIS_DIAL_MAP="+string(mapping))
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("topology app: %v\n%s", err, stderr.String())
			}
			var result struct {
				Events []map[string]any `json:"events"`
			}
			if err := json.Unmarshal(out, &result); err != nil {
				t.Fatal(err)
			}
			observed := map[string]bool{}
			for _, event := range result.Events {
				if event["event_type"] != "db_change" {
					continue
				}
				payload := event["payload"].(map[string]any)
				for _, raw := range payload["db"].([]any) {
					db := raw.(map[string]any)
					meta, _ := db["resultMeta"].(map[string]any)
					if value, ok := meta["value"].(string); ok {
						observed[value] = true
					}
				}
			}
			for i := 0; i < 12; i++ {
				if !observed[fmt.Sprintf("stock:%d", i*17)] {
					t.Fatalf("%s: captured routed Redis value missing for key %d", mode, i)
				}
			}
			if mode == "sentinel" && !observed["stock:promoted"] {
				t.Fatal("existing Redis clients lost captured business value after Sentinel failover")
			}
			t.Logf("%s: all 12 cross-client values captured through real topology discovery", mode)
		})
	}
}
