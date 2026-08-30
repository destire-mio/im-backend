package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultWebSocketPresenceNamespace = "im:ws"

var registerWebSocketPresence = redis.NewScript(`
local old = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return old or ''
`)

var refreshWebSocketPresence = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  redis.call('SADD', KEYS[2], ARGV[3])
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
  return 2
end
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  redis.call('SADD', KEYS[2], ARGV[3])
  redis.call('PEXPIRE', KEYS[2], ARGV[2])
  return 1
end
return 0
`)

var unregisterWebSocketPresence = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[2])
if redis.call('SCARD', KEYS[2]) == 0 then
  redis.call('DEL', KEYS[2])
end
return 1
`)

type webSocketPresenceRecord struct {
	UserID       int64  `json:"userId"`
	SessionID    int64  `json:"sessionId"`
	ConnectionID string `json:"connectionId"`
	InstanceID   string `json:"instanceId"`
}

func (record webSocketPresenceRecord) valid() bool {
	return record.UserID > 0 && record.SessionID > 0 && record.ConnectionID != "" && record.InstanceID != ""
}

func (record webSocketPresenceRecord) encoded() (string, error) {
	if !record.valid() {
		return "", errors.New("websocket presence identity is incomplete")
	}
	encoded, err := json.Marshal(record)
	return string(encoded), err
}

type webSocketPresenceRefresh int

const (
	webSocketPresenceLost webSocketPresenceRefresh = iota
	webSocketPresenceRenewed
	webSocketPresenceRecovered
)

type redisWebSocketPresence struct {
	client        *redis.Client
	namespace     string
	ttl           time.Duration
	renewInterval time.Duration
}

func newRedisWebSocketPresence(
	client *redis.Client,
	namespace string,
	ttl time.Duration,
	renewInterval time.Duration,
) (*redisWebSocketPresence, error) {
	if client == nil {
		return nil, errors.New("websocket presence requires Redis")
	}
	if namespace == "" {
		namespace = defaultWebSocketPresenceNamespace
	}
	if ttl <= 0 || renewInterval <= 0 || renewInterval >= ttl {
		return nil, errors.New("websocket presence requires a renewal interval shorter than its TTL")
	}
	return &redisWebSocketPresence{
		client:        client,
		namespace:     namespace,
		ttl:           ttl,
		renewInterval: renewInterval,
	}, nil
}

func (presence *redisWebSocketPresence) Register(
	ctx context.Context,
	record webSocketPresenceRecord,
) (*webSocketPresenceRecord, error) {
	encoded, err := record.encoded()
	if err != nil {
		return nil, err
	}
	oldValue, err := registerWebSocketPresence.Run(
		ctx,
		presence.client,
		[]string{presence.sessionKey(record.UserID, record.SessionID), presence.userSessionsKey(record.UserID)},
		encoded,
		presence.ttl.Milliseconds(),
		record.SessionID,
	).Text()
	if err != nil {
		return nil, err
	}
	if oldValue == "" {
		return nil, nil
	}
	var old webSocketPresenceRecord
	if err := json.Unmarshal([]byte(oldValue), &old); err != nil || !old.valid() {
		return nil, errors.New("replaced websocket presence record was invalid")
	}
	return &old, nil
}

func (presence *redisWebSocketPresence) Refresh(
	ctx context.Context,
	record webSocketPresenceRecord,
) (webSocketPresenceRefresh, error) {
	encoded, err := record.encoded()
	if err != nil {
		return webSocketPresenceLost, err
	}
	result, err := refreshWebSocketPresence.Run(
		ctx,
		presence.client,
		[]string{presence.sessionKey(record.UserID, record.SessionID), presence.userSessionsKey(record.UserID)},
		encoded,
		presence.ttl.Milliseconds(),
		record.SessionID,
	).Int()
	if err != nil {
		return webSocketPresenceLost, err
	}
	switch result {
	case int(webSocketPresenceRenewed):
		return webSocketPresenceRenewed, nil
	case int(webSocketPresenceRecovered):
		return webSocketPresenceRecovered, nil
	default:
		return webSocketPresenceLost, nil
	}
}

func (presence *redisWebSocketPresence) Unregister(
	ctx context.Context,
	record webSocketPresenceRecord,
) (bool, error) {
	encoded, err := record.encoded()
	if err != nil {
		return false, err
	}
	result, err := unregisterWebSocketPresence.Run(
		ctx,
		presence.client,
		[]string{presence.sessionKey(record.UserID, record.SessionID), presence.userSessionsKey(record.UserID)},
		encoded,
		record.SessionID,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (presence *redisWebSocketPresence) LookupUser(
	ctx context.Context,
	userID int64,
) ([]webSocketPresenceRecord, error) {
	if userID <= 0 {
		return nil, errors.New("websocket presence lookup requires a user")
	}
	recordsByUser, err := presence.LookupUsers(ctx, []int64{userID})
	if err != nil {
		return nil, err
	}
	return recordsByUser[userID], nil
}

type webSocketPresenceLookup struct {
	sessionIDs  []int64
	keys        []string
	staleValues []any
}

// LookupUsers resolves one batch snapshot in two Redis pipeline round trips:
// first all user session indexes, then all corresponding presence records.
// Duplicate user IDs are resolved only once and the result is discarded with
// the caller's Outbox batch.
func (presence *redisWebSocketPresence) LookupUsers(
	ctx context.Context,
	userIDs []int64,
) (map[int64][]webSocketPresenceRecord, error) {
	unique := make([]int64, 0, len(userIDs))
	seen := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID <= 0 {
			return nil, errors.New("websocket presence lookup requires positive users")
		}
		if _, found := seen[userID]; found {
			continue
		}
		seen[userID] = struct{}{}
		unique = append(unique, userID)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	recordsByUser := make(map[int64][]webSocketPresenceRecord, len(unique))
	if len(unique) == 0 {
		return recordsByUser, nil
	}

	sessionCommands := make(map[int64]*redis.StringSliceCmd, len(unique))
	if _, err := presence.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, userID := range unique {
			sessionCommands[userID] = pipe.SMembers(ctx, presence.userSessionsKey(userID))
		}
		return nil
	}); err != nil {
		return nil, err
	}

	lookups := make(map[int64]*webSocketPresenceLookup, len(unique))
	for _, userID := range unique {
		sessionValues, err := sessionCommands[userID].Result()
		if err != nil {
			return nil, err
		}
		lookup := &webSocketPresenceLookup{
			sessionIDs:  make([]int64, 0, len(sessionValues)),
			keys:        make([]string, 0, len(sessionValues)),
			staleValues: make([]any, 0),
		}
		for _, value := range sessionValues {
			sessionID, err := strconv.ParseInt(value, 10, 64)
			if err != nil || sessionID <= 0 {
				lookup.staleValues = append(lookup.staleValues, value)
				continue
			}
			lookup.sessionIDs = append(lookup.sessionIDs, sessionID)
			lookup.keys = append(lookup.keys, presence.sessionKey(userID, sessionID))
		}
		lookups[userID] = lookup
		recordsByUser[userID] = []webSocketPresenceRecord{}
	}

	valueCommands := make(map[int64]*redis.SliceCmd, len(unique))
	if _, err := presence.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, userID := range unique {
			if keys := lookups[userID].keys; len(keys) > 0 {
				valueCommands[userID] = pipe.MGet(ctx, keys...)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for _, userID := range unique {
		lookup := lookups[userID]
		command := valueCommands[userID]
		if command == nil {
			continue
		}
		values, err := command.Result()
		if err != nil {
			return nil, err
		}
		if len(values) != len(lookup.sessionIDs) {
			return nil, fmt.Errorf(
				"websocket presence lookup returned %d records for %d sessions",
				len(values),
				len(lookup.sessionIDs),
			)
		}
		records := make([]webSocketPresenceRecord, 0, len(values))
		for index, value := range values {
			if value == nil {
				lookup.staleValues = append(lookup.staleValues, lookup.sessionIDs[index])
				continue
			}
			encoded, ok := value.(string)
			if !ok {
				lookup.staleValues = append(lookup.staleValues, lookup.sessionIDs[index])
				continue
			}
			var record webSocketPresenceRecord
			if err := json.Unmarshal([]byte(encoded), &record); err != nil || !record.valid() || record.UserID != userID || record.SessionID != lookup.sessionIDs[index] {
				lookup.staleValues = append(lookup.staleValues, lookup.sessionIDs[index])
				continue
			}
			records = append(records, record)
		}
		recordsByUser[userID] = records
	}
	presence.removeStaleMembersForUsers(ctx, lookups)
	return recordsByUser, nil
}

func (presence *redisWebSocketPresence) removeStaleMembersForUsers(
	ctx context.Context,
	lookups map[int64]*webSocketPresenceLookup,
) {
	hasStaleValues := false
	for _, lookup := range lookups {
		if len(lookup.staleValues) > 0 {
			hasStaleValues = true
			break
		}
	}
	if !hasStaleValues {
		return
	}
	_, _ = presence.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for userID, lookup := range lookups {
			if len(lookup.staleValues) > 0 {
				pipe.SRem(ctx, presence.userSessionsKey(userID), lookup.staleValues...)
			}
		}
		return nil
	})
}

func (presence *redisWebSocketPresence) sessionKey(userID, sessionID int64) string {
	return fmt.Sprintf("%s:{user:%d}:session:%d", presence.namespace, userID, sessionID)
}

func (presence *redisWebSocketPresence) userSessionsKey(userID int64) string {
	return fmt.Sprintf("%s:{user:%d}:sessions", presence.namespace, userID)
}
