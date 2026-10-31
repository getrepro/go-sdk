package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"
)

func redisTopology(ctx context.Context) (any, error) {
	var addresses map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("REDIS_DIAL_MAP")), &addresses); err != nil {
		return nil, err
	}
	// Docker Desktop exposes ports on the host, but topology discovery returns
	// internal container addresses. This changes transport routing only.
	remap := func(address string) string {
		if mapped := addresses[address]; mapped != "" {
			return mapped
		}
		return address
	}
	seeds := strings.Split(os.Getenv("REDIS_SEEDS"), ",")
	mode := os.Getenv("REDIS_TOPOLOGY")
	options := &redis.UniversalOptions{Addrs: seeds, Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, remap(address))
	}}
	rOptions := rueidis.ClientOption{InitAddress: seeds, DialFn: func(address string, dialer *net.Dialer, _ *tls.Config) (net.Conn, error) {
		return dialer.Dial("tcp", remap(address))
	}}
	if mode == "sentinel" {
		options.MasterName = "primary"
		rOptions.Sentinel.MasterSet = "primary"
	}
	if mode == "cluster" {
		options.IsClusterMode = true
	}
	client := redis.NewUniversalClient(options)
	defer client.Close()
	r, err := rueidis.NewClient(rOptions)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	values := map[string]string{}
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("inventory:%d", i)
		value := fmt.Sprintf("stock:%d", i*17)
		if err = client.Set(ctx, key, value, time.Minute).Err(); err != nil {
			return nil, err
		}
		got, e := r.Do(ctx, r.B().Get().Key(key).Build()).ToString()
		if e != nil {
			return nil, e
		}
		if got != value {
			return nil, fmt.Errorf("topology changed value: %s != %s", got, value)
		}
		values[key] = got
	}
	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "{order}:fee", 40, time.Minute)
		pipe.Incr(ctx, "{order}:attempts")
		return nil
	})
	if err != nil {
		return nil, err
	}
	if mode == "sentinel" {
		failoverCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		sentinel := redis.NewSentinelClient(&redis.Options{Addr: seeds[0]})
		defer sentinel.Close()
		before, err := sentinel.GetMasterAddrByName(failoverCtx, "primary").Result()
		if err != nil {
			return nil, err
		}
		// Wait until Sentinel knows the replica before asking it to promote it.
		for {
			replicas, err := sentinel.Replicas(failoverCtx, "primary").Result()
			if err != nil {
				return nil, err
			}
			if len(replicas) > 0 {
				break
			}
			select {
			case <-failoverCtx.Done():
				return nil, failoverCtx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err := sentinel.Failover(failoverCtx, "primary").Err(); err != nil {
			return nil, err
		}
		for {
			after, err := sentinel.GetMasterAddrByName(failoverCtx, "primary").Result()
			if err == nil && strings.Join(after, ":") != strings.Join(before, ":") {
				// Reuse both original clients: rebuilding them would hide broken
				// discovery or callback behavior during the topology transition.
				err = client.Set(failoverCtx, "inventory:after-failover", "stock:promoted", time.Minute).Err()
				if err == nil {
					value, readErr := r.Do(failoverCtx, r.B().Get().Key("inventory:after-failover").Build()).ToString()
					if readErr == nil && value == "stock:promoted" {
						values["inventory:after-failover"] = value
						break
					}
				}
			}
			select {
			case <-failoverCtx.Done():
				return nil, fmt.Errorf("existing clients did not recover after Sentinel failover: %w", failoverCtx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	if _, err = r.Do(ctx, r.B().Get().Key("nonexistent").Build()).ToString(); !rueidis.IsRedisNil(err) {
		return nil, fmt.Errorf("missing-key identity changed: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = client.Get(canceled, "inventory:0").Err(); err == nil {
		return nil, fmt.Errorf("cancellation ignored")
	}
	return values, nil
}
