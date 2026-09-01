package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type rateLimitRule struct {
	key    string
	limit  int64
	window time.Duration
}

type authRateLimiter interface {
	Allow(context.Context, []rateLimitRule) (bool, time.Duration, error)
}

type redisRateLimiter struct {
	client *redis.Client
}

var incrementWindows = redis.NewScript(`
local allowed = 1
local retry_after_ms = 0
for index, key in ipairs(KEYS) do
  local window_ms = tonumber(ARGV[(index - 1) * 2 + 1])
  local limit = tonumber(ARGV[(index - 1) * 2 + 2])
  local count = redis.call('INCR', key)
  if count == 1 then
    redis.call('PEXPIRE', key, window_ms)
  end
  if count > limit then
    allowed = 0
    local ttl = redis.call('PTTL', key)
    if ttl > retry_after_ms then retry_after_ms = ttl end
  end
end
return {allowed, retry_after_ms}
`)

func (limiter *redisRateLimiter) Allow(ctx context.Context, rules []rateLimitRule) (bool, time.Duration, error) {
	if len(rules) == 0 {
		return true, 0, nil
	}
	keys := make([]string, 0, len(rules))
	arguments := make([]any, 0, len(rules)*2)
	for _, rule := range rules {
		keys = append(keys, rule.key)
		arguments = append(arguments, rule.window.Milliseconds(), rule.limit)
	}
	result, err := incrementWindows.Run(ctx, limiter.client, keys, arguments...).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(result) != 2 {
		return false, 0, errors.New("unexpected Redis rate-limit response")
	}
	allowed, ok := result[0].(int64)
	if !ok {
		return false, 0, errors.New("invalid Redis rate-limit result")
	}
	retryMilliseconds, ok := result[1].(int64)
	if !ok {
		return false, 0, errors.New("invalid Redis rate-limit TTL")
	}
	return allowed == 1, time.Duration(retryMilliseconds) * time.Millisecond, nil
}

func (app *application) enforceAuthRateLimit(w http.ResponseWriter, r *http.Request, endpoint, account, device string) bool {
	if app.rateLimiter == nil {
		return true
	}
	rules := rulesForAuthRequest(endpoint, app.clientIP(r), account, device)
	allowed, retryAfter, err := app.rateLimiter.Allow(r.Context(), rules)
	if err != nil {
		if app.rateLimitFailOpen {
			return true
		}
		writeAPIError(
			w,
			r,
			http.StatusServiceUnavailable,
			"AUTH_RATE_LIMIT_UNAVAILABLE",
			"authentication rate limiter is temporarily unavailable",
			err,
		)
		return false
	}
	if !allowed {
		seconds := int64((retryAfter + time.Second - 1) / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
		writeAPIError(w, r, http.StatusTooManyRequests, "AUTH_RATE_LIMITED", "too many authentication attempts", nil)
		return false
	}
	return true
}

func rulesForAuthRequest(endpoint, clientIP, account, device string) []rateLimitRule {
	rules := []rateLimitRule{{key: "auth:rl:global:" + endpoint, limit: 1000, window: time.Minute}}
	switch endpoint {
	case "register":
		rules[0].limit = 200
		rules = append(rules, rateLimitRule{key: dimensionKey(endpoint, "ip", clientIP), limit: 10, window: time.Hour})
	case "login":
		rules = append(rules, rateLimitRule{key: dimensionKey(endpoint, "ip", clientIP), limit: 30, window: time.Minute})
		if account != "" {
			rules = append(rules, rateLimitRule{key: dimensionKey(endpoint, "account", account), limit: 10, window: 5 * time.Minute})
		}
	case "refresh":
		rules[0].limit = 2000
		rules = append(rules, rateLimitRule{key: dimensionKey(endpoint, "ip", clientIP), limit: 120, window: time.Minute})
	}
	if device != "" {
		rules = append(rules, rateLimitRule{key: dimensionKey(endpoint, "device", device), limit: 30, window: time.Minute})
	}
	return rules
}

func dimensionKey(endpoint, dimension, value string) string {
	digest := sha256.Sum256([]byte(value))
	return "auth:rl:" + endpoint + ":" + dimension + ":" + hex.EncodeToString(digest[:])
}

func (app *application) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peerIP := net.ParseIP(host)
	if peerIP == nil || !ipInNetworks(peerIP, app.trustedProxyNetworks) {
		return host
	}
	forwarded := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	if len(forwarded) == 0 {
		return host
	}
	clientIP := net.ParseIP(strings.TrimSpace(forwarded[0]))
	if clientIP == nil {
		return host
	}
	return clientIP.String()
}

func ipInNetworks(ip net.IP, networks []*net.IPNet) bool {
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func parseTrustedProxyNetworks(value string) ([]*net.IPNet, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	networks := make([]*net.IPNet, 0, len(parts))
	for _, part := range parts {
		_, network, err := net.ParseCIDR(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		networks = append(networks, network)
	}
	return networks, nil
}
