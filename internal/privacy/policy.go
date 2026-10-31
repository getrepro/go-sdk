// Package privacy executes the same reviewed evaluator shipped in the Node SDK.
// The embedded program has no network, filesystem, process, or module access.
package privacy

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
)

//go:embed engine.js
var source string
var compileOnce sync.Once
var program *goja.Program
var compileErr error

type Config struct {
	APIBase      string
	Environment  string
	CaptureRaw   bool
	Policy       json.RawMessage
	FetchTimeout time.Duration
}
type Evaluator struct {
	mu             sync.Mutex
	vm             *goja.Runtime
	apply          goja.Callable
	Fingerprint    string
	StatementCount int
}

func Fetch(ctx context.Context, c Config, appID, secret, appName, service string) (json.RawMessage, error) {
	if c.CaptureRaw {
		return nil, nil
	}
	if len(c.Policy) > 0 {
		return c.Policy, nil
	}
	if c.APIBase == "" {
		return nil, nil
	}
	u, err := url.Parse(strings.TrimRight(c.APIBase, "/") + "/v1/sdk/privacy-policy")
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, errors.New("repro: invalid privacy APIBase")
	}
	env := c.Environment
	if env == "" {
		env = "dev"
	}
	query := u.Query()
	query.Set("environment", env)
	query.Set("serviceName", service)
	u.RawQuery = query.Encode()
	timeout := c.FetchTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-App-Id", appID)
	request.Header.Set("X-App-Secret", secret)
	request.Header.Set("X-App-Name", appName)
	// Do not forward app credentials through a redirect to an unrelated origin.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("repro: approved privacy policy could not be fetched")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("repro: privacy policy fetch returned HTTP %d", response.StatusCode)
	}
	const maxPolicyBytes = 16 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, maxPolicyBytes+1))
	if err != nil || len(data) > maxPolicyBytes {
		return nil, errors.New("repro: invalid or oversized privacy response")
	}
	var payload struct {
		Enabled *bool           `json:"enabled"`
		Policy  json.RawMessage `json:"policy"`
	}
	if err = json.Unmarshal(data, &payload); err != nil {
		return nil, errors.New("repro: invalid privacy response")
	}
	if payload.Enabled != nil && !*payload.Enabled {
		return nil, nil
	}
	if len(payload.Policy) == 0 || string(payload.Policy) == "null" {
		return nil, errors.New("repro: enabled privacy response is missing its policy")
	}
	return payload.Policy, nil
}

func New(raw json.RawMessage) (*Evaluator, error) {
	compileOnce.Do(func() { program, compileErr = goja.Compile("repro-privacy", source, true) })
	if compileErr != nil {
		return nil, compileErr
	}
	vm := goja.New()
	vm.SetMaxCallStackSize(512)
	_ = vm.Set("reproSHA256", func(value string) string { hash := sha256.Sum256([]byte(value)); return hex.EncodeToString(hash[:]) })
	if _, err := vm.RunProgram(program); err != nil {
		return nil, err
	}
	api := vm.Get("ReproPrivacy").ToObject(vm)
	configure, ok := goja.AssertFunction(api.Get("configure"))
	if !ok {
		return nil, errors.New("repro: missing policy entrypoint")
	}
	if len(raw) == 0 {
		raw = []byte("null")
	}
	value, err := configure(goja.Undefined(), vm.ToValue(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("repro: invalid approved policy: %w", err)
	}
	var info struct {
		Fingerprint    string `json:"fingerprint"`
		StatementCount int    `json:"statementCount"`
	}
	if err = json.Unmarshal([]byte(value.String()), &info); err != nil {
		return nil, err
	}
	if string(raw) != "null" && (info.StatementCount == 0 || info.Fingerprint == "") {
		return nil, errors.New("repro: supplied privacy policy has no valid statements")
	}
	apply, ok := goja.AssertFunction(api.Get("apply"))
	if !ok {
		return nil, errors.New("repro: missing policy evaluator")
	}
	return &Evaluator{vm: vm, apply: apply, Fingerprint: info.Fingerprint, StatementCount: info.StatementCount}, nil
}

func (e *Evaluator) Apply(ctx context.Context, surface string, value any, details map[string]any) (any, error) {
	if details == nil {
		details = map[string]any{}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	metadata, err := json.Marshal(details)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { e.vm.Interrupt("privacy_deadline"); close(stopped) })
	defer func() {
		if !stop() {
			<-stopped
		}
		e.vm.ClearInterrupt()
	}()
	result, err := e.apply(goja.Undefined(), e.vm.ToValue(surface), e.vm.ToValue(string(data)), e.vm.ToValue(string(metadata)))
	if err != nil {
		return nil, errors.New("repro: policy evaluation failed")
	}
	var out any
	decoder := json.NewDecoder(strings.NewReader(result.String()))
	decoder.UseNumber()
	if err = decoder.Decode(&out); err != nil {
		return nil, errors.New("repro: invalid policy output")
	}
	return out, nil
}
