package main

import (
	"net"
	"net/http/httptest"
	"testing"
	"time"
)

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
