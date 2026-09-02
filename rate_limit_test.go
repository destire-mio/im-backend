package main

import (
	"context"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedisRateLimitRejectedLocalAttemptsDoNotConsumeGlobalQuota(t *testing.T) {
	client, namespace := openTestRedis(t)
	limiter := &redisRateLimiter{client: client}
	ctx := context.Background()
	rules := rulesForAuthRequest("login", "203.0.113.1", "blocked_account", "blocked_device")
	for index := range rules {
		rules[index].key = namespace + ":" + rules[index].key
	}
	for attempt := 0; attempt < 1000; attempt++ {
		allowed, retryAfter, err := limiter.Allow(ctx, rules)
		if err != nil || allowed != (attempt < 10) {
			t.Fatalf("attempt %d: allowed=%t retry=%v err=%v", attempt+1, allowed, retryAfter, err)
		}
		if !allowed && (retryAfter <= 0 || retryAfter > 5*time.Minute) {
			t.Fatalf("rejected attempt retry-after = %v", retryAfter)
		}
	}
	for _, rule := range rules {
		count, err := client.Get(ctx, rule.key).Int64()
		if err != nil || count != 10 {
			t.Fatalf("counter %s = %d, err %v; want 10 admitted attempts", rule.key, count, err)
		}
	}
	freshRules := rulesForAuthRequest("login", "203.0.113.2", "fresh_account", "fresh_device")
	for index := range freshRules {
		freshRules[index].key = namespace + ":" + freshRules[index].key
	}
	if allowed, _, err := limiter.Allow(ctx, freshRules); err != nil || !allowed {
		t.Fatalf("unrelated user rejected after local flood: allowed=%t err=%v", allowed, err)
	}
}

func TestRedisRateLimitGlobalRejectionDoesNotChargeLocalWindows(t *testing.T) {
	client, namespace := openTestRedis(t)
	limiter := &redisRateLimiter{client: client}
	ctx := context.Background()
	global := rateLimitRule{key: namespace + ":global", limit: 1, window: time.Minute}
	if allowed, _, err := limiter.Allow(ctx, []rateLimitRule{global}); err != nil || !allowed {
		t.Fatalf("first admission: allowed=%t err=%v", allowed, err)
	}
	local := rateLimitRule{key: namespace + ":local", limit: 10, window: time.Minute}
	// Reversing rule order must not change admission or accounting.
	if allowed, retryAfter, err := limiter.Allow(ctx, []rateLimitRule{local, global}); err != nil || allowed || retryAfter <= 0 {
		t.Fatalf("global rejection: allowed=%t retry=%v err=%v", allowed, retryAfter, err)
	}
	if count, err := client.Exists(ctx, local.key).Result(); err != nil || count != 0 {
		t.Fatalf("global rejection created local window: count=%d err=%v", count, err)
	}
}

func TestRedisRateLimitAdmissionIsAtomicAndDoesNotExtendWindow(t *testing.T) {
	client, namespace := openTestRedis(t)
	limiter := &redisRateLimiter{client: client}
	ctx := context.Background()
	rules := []rateLimitRule{
		{key: namespace + ":global", limit: 100, window: time.Minute},
		{key: namespace + ":account", limit: 10, window: time.Minute},
	}
	var admitted atomic.Int64
	var wait sync.WaitGroup
	for range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			allowed, _, err := limiter.Allow(ctx, rules)
			if err != nil {
				t.Error(err)
			} else if allowed {
				admitted.Add(1)
			}
		}()
	}
	wait.Wait()
	if admitted.Load() != 10 {
		t.Fatalf("concurrent admissions = %d, want 10", admitted.Load())
	}
	for _, rule := range rules {
		if count, err := client.Get(ctx, rule.key).Int64(); err != nil || count != 10 {
			t.Fatalf("counter = %d err=%v, want 10", count, err)
		}
	}
	if err := client.PExpire(ctx, rules[1].key, 50*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	if allowed, retryAfter, err := limiter.Allow(ctx, rules); err != nil || allowed || retryAfter > 50*time.Millisecond {
		t.Fatalf("rejection extended window: allowed=%t retry=%v err=%v", allowed, retryAfter, err)
	}
	time.Sleep(60 * time.Millisecond)
	if allowed, _, err := limiter.Allow(ctx, rules); err != nil || !allowed {
		t.Fatalf("expired local window did not reopen: allowed=%t err=%v", allowed, err)
	}
}

func TestClientIPTrustsForwardedHeaderOnlyFromConfiguredProxy(t *testing.T) {
	request := httptest.NewRequest("POST", "/auth/login", nil)
	request.RemoteAddr = "203.0.113.9:12345"
	request.Header.Set("X-Forwarded-For", "198.51.100.7")
	app := &application{}
	if got := app.clientIP(request); got != "203.0.113.9" {
		t.Fatalf("untrusted forwarded IP = %q, want socket peer", got)
	}

	_, trustedNetwork, err := net.ParseCIDR("203.0.113.0/24")
	if err != nil {
		t.Fatalf("parse CIDR: %v", err)
	}
	app.trustedProxyNetworks = []*net.IPNet{trustedNetwork}
	if got := app.clientIP(request); got != "198.51.100.7" {
		t.Fatalf("trusted forwarded IP = %q, want gateway-provided client IP", got)
	}
}

func TestLoginRateLimitUsesIndependentGlobalIPAccountAndDeviceKeys(t *testing.T) {
	rules := rulesForAuthRequest("login", "203.0.113.9", "xiaozhu", "phone-installation")
	if len(rules) != 4 {
		t.Fatalf("login rule count = %d, want 4", len(rules))
	}
	seen := make(map[string]bool)
	for _, rule := range rules {
		if seen[rule.key] {
			t.Fatalf("duplicate rate-limit key %q", rule.key)
		}
		seen[rule.key] = true
		if rule.limit <= 0 || rule.window < time.Minute {
			t.Fatalf("invalid rule %+v", rule)
		}
	}
}
