// Package delivery keeps network failures and backpressure out of application paths.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrFull = errors.New("repro: delivery queue is full")
var ErrClosed = errors.New("repro: delivery queue is closed")

type Config struct {
	URL, AppID, AppSecret, AppName string
	MaxBytes                       int
	MaxAttempts                    int
	Client                         *http.Client
	Transform                      func([]byte) ([]byte, error)
}

type Queue struct {
	config  Config
	mu      sync.Mutex
	items   [][]byte
	bytes   int
	closed  bool
	err     error
	wake    chan struct{}
	changed chan struct{}
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

func New(c Config) *Queue {
	if c.MaxBytes <= 0 {
		c.MaxBytes = 16 << 20
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 10 * time.Second}
	}
	client := *c.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.Client = &client
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{config: c, wake: make(chan struct{}, 1), changed: make(chan struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel}
	go q.run()
	return q
}

// Enqueue takes ownership of an immutable encoded batch. Retries reuse its IDs and bytes.
func (q *Queue) Enqueue(data []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if q.bytes+len(data) > q.config.MaxBytes {
		return ErrFull
	}
	q.items = append(q.items, data)
	q.bytes += len(data)
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}

func (q *Queue) Flush(ctx context.Context) error {
	for {
		q.mu.Lock()
		empty, err, changed := len(q.items) == 0, q.err, q.changed
		q.mu.Unlock()
		if empty {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (q *Queue) Close(ctx context.Context) error {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	err := q.Flush(ctx)
	q.cancel()
	<-q.done
	return err
}

func (q *Queue) run() {
	defer close(q.done)
	for {
		q.mu.Lock()
		var data []byte
		if len(q.items) > 0 {
			data = q.items[0]
		}
		q.mu.Unlock()
		if data == nil {
			select {
			case <-q.ctx.Done():
				return
			case <-q.wake:
				continue
			}
		}
		err := q.send(data)
		q.mu.Lock()
		q.items[0] = nil
		q.items = q.items[1:]
		q.bytes -= len(data)
		if err != nil && q.err == nil {
			q.err = err
		}
		close(q.changed)
		q.changed = make(chan struct{})
		q.mu.Unlock()
		if q.ctx.Err() != nil {
			return
		}
	}
}

func (q *Queue) send(data []byte) error {
	if q.config.Transform != nil {
		var err error
		data, err = q.config.Transform(data)
		if err != nil {
			return err
		}
	}
	var batch struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(data, &batch); err != nil || len(batch.Events) == 0 {
		return errors.New("repro: invalid delivery batch")
	}
	var last error
	for attempt := 0; attempt < q.config.MaxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(q.ctx, http.MethodPost, q.config.URL, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-App-Id", q.config.AppID)
		req.Header.Set("X-App-Secret", q.config.AppSecret)
		req.Header.Set("X-App-Name", q.config.AppName)
		resp, err := q.config.Client.Do(req)
		delay := time.Duration(1<<min(attempt, 4)) * 100 * time.Millisecond
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8193))
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusAccepted {
				var receipt struct {
					Accepted *int `json:"accepted"`
					Dropped  *int `json:"dropped"`
				}
				if readErr != nil || len(body) > 8192 || json.Unmarshal(body, &receipt) != nil || receipt.Accepted == nil || receipt.Dropped == nil {
					return errors.New("repro: invalid ingest receipt; delivery not confirmed")
				}
				if *receipt.Accepted != len(batch.Events) || *receipt.Dropped != 0 {
					return fmt.Errorf("repro: ingest acknowledged %d of %d events, dropped %d", *receipt.Accepted, len(batch.Events), *receipt.Dropped)
				}
				return nil
			}
			last = fmt.Errorf("repro: ingest returned HTTP %d", resp.StatusCode)
			if resp.StatusCode != 429 && resp.StatusCode < 500 {
				return last
			}
			if parsed := retryAfter(resp.Header.Get("Retry-After"), time.Now()); parsed > delay {
				delay = parsed
			}
		} else {
			last = errors.New("repro: ingest connection failed")
		}
		if attempt+1 == q.config.MaxAttempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-q.ctx.Done():
			timer.Stop()
			return q.ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(now) {
		return deadline.Sub(now)
	}
	return 0
}
