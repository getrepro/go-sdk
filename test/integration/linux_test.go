package integration

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestLinuxCrossCompiledRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	var baseline string
	for _, mode := range []string{"plain", "instrumented"} {
		t.Run(mode, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "app")
			command := "go"
			if mode == "instrumented" {
				command = filepath.Join(root, "bin/repro-go")
			}
			cmd := exec.CommandContext(ctx, command, "build", "-o", binary, ".")
			cmd.Dir = filepath.Join(root, "test/fixtures/semantics")
			cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("cross compile: %v\n%s", err, out)
			}
			env := map[string]string{}
			if mode == "plain" {
				env["UNINSTRUMENTED"] = "1"
			}
			c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{Started: true, ContainerRequest: testcontainers.ContainerRequest{Image: "alpine:3.20", ImagePlatform: "linux/arm64", Env: env, Cmd: []string{"/app"}, Files: []testcontainers.ContainerFile{{HostFilePath: binary, ContainerFilePath: "/app", FileMode: 0755}}, WaitingFor: wait.ForExit().WithExitTimeout(time.Minute)}})
			if c != nil {
				defer func() {
					if err := c.Terminate(context.Background()); err != nil {
						t.Error(err)
					}
				}()
			}
			if err != nil {
				t.Fatal(err)
			}
			logs, err := c.Logs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(logs)
			_ = logs.Close()
			if err != nil {
				t.Fatal(err)
			}
			state, err := c.State(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if state.ExitCode != 0 {
				t.Fatalf("Linux runtime exit %d: %s", state.ExitCode, data)
			}
			var result string
			for _, line := range strings.Split(string(data), "\n") {
				var values []string
				if json.Unmarshal([]byte(line), &values) == nil && len(values) == 8 {
					result = line
				}
			}
			if result == "" {
				t.Fatalf("missing Linux business results: %s", data)
			}
			if mode == "plain" {
				baseline = result
			} else if result != baseline {
				t.Fatalf("Linux business results changed\nplain=%s\nrepro=%s", baseline, result)
			}
			t.Logf("%s Linux executable returned all 8 concurrent business results", mode)
		})
	}
}
