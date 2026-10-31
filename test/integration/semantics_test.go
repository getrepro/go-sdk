package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCompiledSemanticsRaceAndBaseline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(root, "test/fixtures/semantics")
	before := map[string][32]byte{}
	for _, name := range []string{"go.mod", "go.sum", "main.go", "tagged.go", "performance.go", "source.go"} {
		data, e := os.ReadFile(filepath.Join(fixture, name))
		if e != nil {
			t.Fatal(e)
		}
		before[name] = sha256.Sum256(data)
	}
	defer func() {
		for name, hash := range before {
			data, e := os.ReadFile(filepath.Join(fixture, name))
			if e != nil || sha256.Sum256(data) != hash {
				t.Errorf("build changed application source %s", name)
			}
		}
	}()
	baseline := exec.CommandContext(ctx, "go", "run", "-tags=reprotesttag", ".")
	baseline.Dir = fixture
	baseline.Env = append(os.Environ(), "UNINSTRUMENTED=1", "REPRO_TAG_ASSERT=1")
	want, err := baseline.Output()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "semantics-race")
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "build", "-race", "-tags=reprotesttag", "-o", binary, ".")
	cmd.Dir = fixture
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("race build: %v\n%s", err, out)
	}
	for i := 0; i < 3; i++ {
		cmd = exec.CommandContext(ctx, binary)
		cmd.Env = append(os.Environ(), "REPRO_TAG_ASSERT=1")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		got, err := cmd.Output()
		if err != nil {
			t.Fatalf("instrumented semantics: %v\n%s", err, stderr.String())
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("application result changed\nplain=%s\nrepro=%s", want, got)
		}
		t.Log(stderr.String())
	}
}

func TestTestCommandCapturesTestOnlyCode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/repro-go"), "test", "-race", "-run", "^TestInstrumentedTestBinary$", "-count", "1", ".")
	cmd.Dir = filepath.Join(root, "test/fixtures/semantics")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("instrumented test: %v\n%s", err, out)
	}
}
