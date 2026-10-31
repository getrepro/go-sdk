package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type Target struct {
	ActorIDs  []string          `json:"actorIds,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	ExpiresAt int64             `json:"expiresAt"`
}
type LivePolicy struct {
	SampleRate *float64 `json:"sampleRate,omitempty"`
	Targets    []Target `json:"targets,omitempty"`
}
type LiveConfig struct {
	APIBase                 string
	Policy                  LivePolicy
	DefineActorID           func(*http.Request) string
	AdmissionTimeout        time.Duration
	MaxConcurrentAdmissions int
}
type liveControl struct {
	policy atomic.Pointer[LivePolicy]
	slots  chan struct{}
	client *http.Client
}
type liveClaim struct {
	Captured       *bool  `json:"captured"`
	SessionID      string `json:"sessionId"`
	ChildSessionID string `json:"childSessionId"`
	ActionID       string `json:"actionId"`
}

func copyLivePolicy(policy LivePolicy) (*LivePolicy, error) {
	if policy.SampleRate != nil && (math.IsNaN(*policy.SampleRate) || math.IsInf(*policy.SampleRate, 0) || *policy.SampleRate < 0 || *policy.SampleRate > 1) {
		return nil, errors.New("repro: live sample rate must be between zero and one")
	}
	if len(policy.Targets) > 100 {
		return nil, errors.New("repro: too many live targets")
	}
	for _, target := range policy.Targets {
		if target.ExpiresAt <= 0 || len(target.ActorIDs) > 1000 {
			return nil, errors.New("repro: invalid live target")
		}
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	var copy LivePolicy
	err = json.Unmarshal(data, &copy)
	return &copy, err
}

// SetLiveCapture changes admission policy, not the evidence of work already
// admitted. The API owns chain sampling, targeting, rollover and usage quotas.
func (r *Runtime) SetLiveCapture(policy LivePolicy) error {
	copy, err := copyLivePolicy(policy)
	if err != nil {
		return err
	}
	if r.live == nil {
		return errors.New("repro: live mode is not enabled")
	}
	r.live.policy.Store(copy)
	return nil
}

func (r *Runtime) initLive() error {
	if r.config.Mode != "live" {
		return nil
	}
	base := r.config.Live.APIBase
	if base == "" {
		base = r.config.Privacy.APIBase
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("repro: live mode requires Live.APIBase or Privacy.APIBase")
	}
	r.config.Live.APIBase = strings.TrimRight(base, "/")
	count := r.config.Live.MaxConcurrentAdmissions
	if count <= 0 {
		count = 32
	}
	r.live = &liveControl{slots: make(chan struct{}, count), client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	return r.SetLiveCapture(r.config.Live.Policy)
}

func AdmitHTTP(request *http.Request) (session, action, actor string) {
	r := active.Load()
	if r == nil || r.live == nil || request.Method == "OPTIONS" {
		return "", "", ""
	}
	if InternalURL("http://" + request.Host + request.URL.Path) {
		return "", "", ""
	}
	actor = r.config.ActorID
	if resolve := r.config.Live.DefineActorID; resolve != nil {
		func() {
			defer func() {
				if recover() != nil {
					actor = ""
					r.recordError(errors.New("repro: actor resolver panicked"))
				}
			}()
			actor = resolve(request)
		}()
	}
	if strings.TrimSpace(actor) == "" {
		return "", "", ""
	}
	session, action = r.admit(request.Context(), actor)
	return
}

func (r *Runtime) admit(parent context.Context, actor string) (string, string) {
	select {
	case r.live.slots <- struct{}{}:
		defer func() { <-r.live.slots }()
	default:
		r.recordError(errors.New("repro: live admission concurrency limit reached"))
		return "", ""
	}
	timeout := r.config.Live.AdmissionTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	payload := map[string]any{"captureMode": "backend-only", "clientTime": time.Now().UnixMilli(), "actorId": actor, "actorIds": []string{actor}, "actorType": "service", "actorLabels": r.config.ActorLabels, "liveCapture": r.live.policy.Load()}
	data, _ := json.Marshal(payload)
	request, err := http.NewRequestWithContext(ctx, "POST", r.config.Live.APIBase+"/v1/live-sessions/claim", bytes.NewReader(data))
	if err != nil {
		r.recordError(err)
		return "", ""
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-App-Id", r.config.AppID)
	request.Header.Set("X-App-Secret", r.config.AppSecret)
	request.Header.Set("X-App-Name", r.config.AppName)
	response, err := r.live.client.Do(request)
	if err != nil {
		r.recordError(errors.New("repro: live admission unavailable; application request continues without capture"))
		return "", ""
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		r.recordError(fmt.Errorf("repro: live admission returned HTTP %d", response.StatusCode))
		return "", ""
	}
	var claim liveClaim
	if err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&claim); err != nil {
		r.recordError(errors.New("repro: invalid live admission response"))
		return "", ""
	}
	if claim.Captured != nil && !*claim.Captured {
		return "", ""
	}
	if claim.ChildSessionID != "" {
		claim.SessionID = claim.ChildSessionID
	}
	if claim.SessionID == "" {
		return "", ""
	}
	if claim.ActionID == "" {
		claim.ActionID = "A_" + NewID()
	}
	return claim.SessionID, claim.ActionID
}

// Message consumers have no HTTP request to resolve. Use the configured service
// actor and the same central live admission policy, never a separate sampler.
func AdmitMessage(ctx context.Context) (session, action, actor string) {
	r := active.Load()
	if r == nil || r.live == nil {
		return "", "", ""
	}
	actor = r.config.ActorID
	session, action = r.admit(ctx, actor)
	return
}
