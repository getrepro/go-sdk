package build

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const engineVersion = "v1.0.0"
const sdkPath = "github.com/getrepro/go-sdk"

type Package struct {
	Dir, ImportPath, Name string
	ForTest               string
	GoFiles, CgoFiles     []string
	Module                *struct {
		Path, Dir string
		Main      bool
	}
	Error *struct{ Err string }
}

func Run(ctx context.Context, args []string) error {
	if len(args) == 0 || (args[0] != "build" && args[0] != "test" && args[0] != "install") {
		return errors.New("usage: repro-go build|test|install [go flags] [packages]")
	}
	engine, err := enginePath(ctx)
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp("", "repro-go-build-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	flags, targets, err := listArgs(args[1:])
	if err != nil {
		return err
	}
	listFlags := []string{"list", "-deps", "-json"}
	if args[0] == "test" {
		listFlags = append(listFlags, "-test")
	}
	list := exec.CommandContext(ctx, "go", append(append(listFlags, flags...), targets...)...)
	list.Stderr = os.Stderr
	output, err := list.Output()
	if err != nil {
		return fmt.Errorf("resolve build packages: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	var packages []Package
	for {
		var pkg Package
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if pkg.Error != nil {
			return errors.New(pkg.Error.Err)
		}
		pkg.ImportPath, _, _ = strings.Cut(pkg.ImportPath, " [")
		packages = append(packages, pkg)
	}
	overlay := map[string]string{}
	ranges, err := kafkaRanges(ctx, flags, packages)
	if err != nil {
		return err
	}
	functions := 0
	for _, pkg := range packages {
		if pkg.Module == nil || !pkg.Module.Main || strings.HasPrefix(pkg.ImportPath, sdkPath) {
			continue
		}
		for _, name := range append(pkg.GoFiles, pkg.CgoFiles...) {
			path := filepath.Join(pkg.Dir, name)
			if filepath.IsAbs(name) {
				path = name
			}
			rel, err := filepath.Rel(pkg.Module.Dir, path)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				continue
			}
			if _, exists := overlay[path]; exists {
				continue
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			code, n, err := transform(path, pkg.ImportPath, source, ranges[path])
			if err != nil {
				return err
			}
			if n == 0 {
				continue
			}
			dest := filepath.Join(tmp, fmt.Sprintf("source-%d.go", len(overlay)))
			if err := os.WriteFile(dest, code, 0600); err != nil {
				return err
			}
			overlay[path] = dest
			functions += n
		}
	}
	overlayFile := filepath.Join(tmp, "overlay.json")
	data, _ := json.Marshal(map[string]any{"Replace": overlay})
	if err := os.WriteFile(overlayFile, data, 0600); err != nil {
		return err
	}
	rules := baseRules()
	if err := addIntegrationRules(rules, packages); err != nil {
		return err
	}
	fingerprint, err := sdkFingerprint(packages)
	if err != nil {
		return err
	}
	versioned := make(map[string]any, len(rules))
	for name, rule := range rules {
		versioned[name+"_"+fingerprint] = rule
	}
	rules = versioned
	ruleData, err := yaml.Marshal(rules)
	if err != nil {
		return err
	}
	ruleFile := filepath.Join(tmp, "rules.yaml")
	if err := os.WriteFile(ruleFile, ruleData, 0600); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "repro-go: instrumenting %d application functions; engine %s\n", functions, engineVersion)
	cmdArgs := []string{"--rules", ruleFile, "go", args[0], "-overlay=" + overlayFile}
	cmdArgs = append(cmdArgs, args[1:]...)
	cmd := exec.CommandContext(ctx, engine, cmdArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// otelc hashes rules, but not the source of add_file rules. Include our source
// identity so changing SDK hooks cannot silently reuse stale runtime archives.
func sdkFingerprint(packages []Package) (string, error) {
	var dir string
	for _, pkg := range packages {
		if pkg.Module != nil && pkg.Module.Path == sdkPath {
			dir = pkg.Module.Dir
			break
		}
	}
	if dir == "" {
		return "", errors.New("repro-go: application must import github.com/getrepro/go-sdk")
	}
	h := sha256.New()
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != dir && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "test" || entry.Name() == "bin") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "go.mod") && entry.Name() != "engine.js" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		h.Write([]byte(rel))
		h.Write([]byte{0})
		h.Write(data)
		return nil
	})
	return fmt.Sprintf("%x", h.Sum(nil)[:8]), err
}

func enginePath(ctx context.Context) (string, error) {
	if path := os.Getenv("REPRO_OTELC"); path != "" {
		return path, nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(cache, "repro-go", engineVersion)
	path := filepath.Join(dir, "otelc")
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "go", "install", "go.opentelemetry.io/otelc/tool/cmd/otelc@"+engineVersion)
	cmd.Env = append(os.Environ(), "GOBIN="+dir)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("install pinned build engine: %w", err)
	}
	return path, nil
}

func listArgs(args []string) (flags, targets []string, err error) {
	values := map[string]bool{"-o": true, "-p": true, "-tags": true, "-ldflags": true, "-gcflags": true, "-asmflags": true, "-buildmode": true, "-mod": true, "-modfile": true, "-overlay": true, "-run": true, "-count": true, "-timeout": true, "-vet": true, "-bench": true, "-coverprofile": true, "-coverpkg": true, "-covermode": true, "-benchtime": true, "-parallel": true, "-cpu": true, "-shuffle": true, "-fuzz": true, "-fuzztime": true, "-fuzzminimizetime": true, "-exec": true, "-pkgdir": true, "-compiler": true, "-buildvcs": true}
	forward := map[string]bool{"-tags": true, "-mod": true, "-modfile": true, "-race": true, "-cover": true}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-args" {
			break
		}
		name, _, joined := strings.Cut(arg, "=")
		if name == "-overlay" || name == "-toolexec" {
			return nil, nil, fmt.Errorf("repro-go manages %s; overlapping instrumentation is not supported", name)
		}
		if values[name] && !joined {
			if i+1 == len(args) {
				return nil, nil, fmt.Errorf("missing value for %s", name)
			}
			if forward[name] {
				flags = append(flags, arg, args[i+1])
			}
			i++
			continue
		}
		if strings.HasPrefix(arg, "-") {
			if forward[name] {
				flags = append(flags, arg)
			}
			continue
		}
		targets = append(targets, arg)
	}
	if len(targets) == 0 {
		targets = []string{"."}
	}
	return
}

func baseRules() map[string]any {
	rules := map[string]any{
		"repro_bridge":          map[string]any{"target": sdkPath + "/instrumentation", "do": []any{map[string]any{"add_file": map[string]string{"file": "bridge_init.go", "path": sdkPath + "/instrumentation/gls"}}}},
		"repro_gls_field":       map[string]any{"target": "runtime", "where": map[string]string{"struct": "g"}, "do": []any{map[string]any{"add_struct_fields": map[string]any{"new_field": []any{map[string]string{"name": "otel_trace_context", "type": "interface{}"}}}}}},
		"repro_gls_file":        map[string]any{"target": "runtime", "do": []any{map[string]any{"add_file": map[string]string{"file": "runtime_gls.go", "path": sdkPath + "/instrumentation/gls"}}}},
		"repro_gls_prepare":     map[string]any{"target": "runtime", "where": map[string]string{"func": "newproc", "pattern": "^systemstack\\("}, "do": []any{map[string]any{"inject_code": map[string]string{"raw": "__reproChildContext := propagateReproContext(gp.otel_trace_context,pc)"}}}},
		"repro_gls_propagation": map[string]any{"target": "runtime", "where": map[string]string{"func": "newproc", "pattern": "^newg := newproc1\\(", "placement": "after"}, "do": []any{map[string]any{"inject_code": map[string]string{"raw": "newg.otel_trace_context = __reproChildContext"}}}},
		"repro_gls_finish":      map[string]any{"target": "runtime", "where": map[string]string{"func": "goexit1"}, "do": []any{map[string]any{"inject_code": map[string]string{"raw": "finishReproGoroutine()"}}}},
	}
	return rules
}
