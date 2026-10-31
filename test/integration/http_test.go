package integration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestAutomaticHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "http-app")
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "build", "-race", "-o", binary, ".")
	cmd.Dir = filepath.Join(root, "test/fixtures/http")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd = exec.CommandContext(ctx, binary)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("HTTP semantics/evidence: %v\n%s", err, out)
	}
}

func TestShutdownDeadlineAndOverflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "lifecycle")
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "build", "-race", "-o", binary, ".")
	cmd.Dir = filepath.Join(root, "test/fixtures/http")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, mode := range []string{"shutdown", "deadline", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.CommandContext(ctx, binary)
			cmd.Env = append(os.Environ(), "REPRO_LIFECYCLE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("lifecycle: %v\n%s", err, out)
			} else {
				t.Log(string(out))
			}
		})
	}
}
