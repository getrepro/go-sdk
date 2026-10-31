package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCaptureOverheadAndCollectorFailureIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(root, "test/fixtures/semantics")
	plain, instrumented := filepath.Join(t.TempDir(), "plain"), filepath.Join(t.TempDir(), "instrumented")
	for _, build := range []struct{ command, binary string }{{"go", plain}, {filepath.Join(root, "bin/repro-go"), instrumented}} {
		cmd := exec.CommandContext(ctx, build.command, "build", "-o", build.binary, ".")
		cmd.Dir = fixture
		cmd.Env = os.Environ()
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build: %v\n%s", err, out)
		}
	}
	for _, mode := range []string{"baseline", "unsampled", "captured", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			binary := instrumented
			if mode == "baseline" {
				binary = plain
			}
			cmd := exec.CommandContext(ctx, binary)
			cmd.Env = append(os.Environ(), "REPRO_PERFORMANCE="+mode)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("%s: %v\n%s", mode, err, stderr.String())
			}
			var result map[string]any
			if err = json.Unmarshal(out, &result); err != nil {
				t.Fatal(err)
			}
			if result["checksum"] != float64(250*4500) {
				t.Fatal("capturing changed business results")
			}
			// A generous safety gate catches capture backpressure leaking into handlers;
			// reported measurements, not this threshold, describe observed overhead.
			if result["p95us"].(float64) > 100000 {
				t.Fatalf("handler latency exceeds 100 ms: %s", out)
			}
			if result["peakObservedHeap"].(float64) > 128<<20 {
				t.Fatalf("small bounded workload retained >128 MiB: %s", out)
			}
			t.Log(string(out))
		})
	}
}
