package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	realtimeCommandDeliver    = "deliver"
	realtimeCommandDisconnect = "disconnect"
)

type webSocketDeliveryRouter interface {
	Publish(context.Context, int64, []byte) (int, error)
}

type realtimeInstanceCommand struct {
	Type         string          `json:"type"`
	ReceiverID   int64           `json:"receiverId,omitempty"`
	ConnectionID string          `json:"connectionId,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

type redisRealtimeRouter struct {
	client     *redis.Client
	presence   *redisWebSocketPresence
	hub        *webSocketHub
	instanceID string
	namespace  string
	done       chan struct{}
	ready      chan struct{}
	readyOnce  sync.Once
	metrics    *applicationMetrics
}

func newRedisRealtimeRouter(
	client *redis.Client,
	presence *redisWebSocketPresence,
	hub *webSocketHub,
	instanceID string,
	metricObservers ...*applicationMetrics,
) (*redisRealtimeRouter, error) {
	if client == nil || presence == nil || hub == nil || instanceID == "" {
		return nil, errors.New("realtime router requires Redis, presence, hub and instance identity")
	}
	var metrics *applicationMetrics
	if len(metricObservers) > 0 {
		metrics = metricObservers[0]
	}
	return &redisRealtimeRouter{
		client:     client,
		presence:   presence,
		hub:        hub,
		instanceID: instanceID,
		namespace:  presence.namespace,
		done:       make(chan struct{}),
		ready:      make(chan struct{}),
		metrics:    metrics,
	}, nil
}

func (router *redisRealtimeRouter) Publish(ctx context.Context, receiverID int64, payload []byte) (int, error) {
	delivered, err := router.hub.Publish(ctx, receiverID, payload)
	if err != nil {
		router.observeRouting("local_hub", "error")
		return 0, err
	}
	if delivered == 0 {
		router.observeRouting("local_hub", "no_connection")
	} else {
		router.observeRouting("local_hub", "delivered")
	}

	records, err := router.presence.LookupUser(ctx, receiverID)
	if err != nil {
		router.observeRouting("presence_lookup", "error")
		return delivered, err
	}
	router.observeRouting("presence_lookup", "success")
	remoteInstances := make(map[string]struct{})
	for _, record := range records {
		if record.InstanceID != router.instanceID {
			remoteInstances[record.InstanceID] = struct{}{}
		}
	}
	if len(remoteInstances) == 0 {
		return delivered, nil
	}

	encoded, err := json.Marshal(realtimeInstanceCommand{
		Type:       realtimeCommandDeliver,
		ReceiverID: receiverID,
		Payload:    json.RawMessage(payload),
	})
	if err != nil {
		return delivered, err
	}
	for instanceID := range remoteInstances {
		subscribers, err := router.client.Publish(ctx, router.instanceChannel(instanceID), encoded).Result()
		if err != nil {
			router.observeRouting("redis_publish", "error")
			return delivered, err
		}
		if subscribers == 0 {
			router.observeRouting("redis_publish", "no_subscriber")
		} else {
			router.observeRouting("redis_publish", "delivered")
		}
		delivered += int(subscribers)
	}
	return delivered, nil
}

func (router *redisRealtimeRouter) DisconnectReplaced(
	ctx context.Context,
	record webSocketPresenceRecord,
) error {
	if !record.valid() {
		return errors.New("replaced websocket connection is invalid")
	}
	if record.InstanceID == router.instanceID {
		return router.hub.DisconnectConnection(ctx, record.UserID, record.ConnectionID)
	}
	encoded, err := json.Marshal(realtimeInstanceCommand{
		Type:         realtimeCommandDisconnect,
		ReceiverID:   record.UserID,
		ConnectionID: record.ConnectionID,
	})
	if err != nil {
		return err
	}
	_, err = router.client.Publish(ctx, router.instanceChannel(record.InstanceID), encoded).Result()
	if err != nil {
		router.observeRouting("redis_disconnect", "error")
	} else {
		router.observeRouting("redis_disconnect", "published")
	}
	return err
}

func (router *redisRealtimeRouter) DisconnectSession(ctx context.Context, userID, sessionID int64) error {
	localError := router.hub.DisconnectSession(ctx, userID, sessionID)
	records, err := router.presence.LookupUser(ctx, userID)
	if err != nil {
		return errors.Join(localError, err)
	}
	var disconnectErrors []error
	if localError != nil {
		disconnectErrors = append(disconnectErrors, localError)
	}
	for _, record := range records {
		if record.SessionID == sessionID {
			if err := router.DisconnectReplaced(ctx, record); err != nil {
				disconnectErrors = append(disconnectErrors, err)
			}
		}
	}
	return errors.Join(disconnectErrors...)
}

func (router *redisRealtimeRouter) DisconnectUser(ctx context.Context, userID int64) error {
	localError := router.hub.DisconnectUser(ctx, userID)
	records, err := router.presence.LookupUser(ctx, userID)
	if err != nil {
		return errors.Join(localError, err)
	}
	var disconnectErrors []error
	if localError != nil {
		disconnectErrors = append(disconnectErrors, localError)
	}
	for _, record := range records {
		if err := router.DisconnectReplaced(ctx, record); err != nil {
			disconnectErrors = append(disconnectErrors, err)
		}
	}
	return errors.Join(disconnectErrors...)
}

func (router *redisRealtimeRouter) Run(ctx context.Context) {
	defer close(router.done)
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		err := router.receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			router.observeRouting("redis_subscription", "error")
			log.Printf("realtime Redis subscription: %v", err)
		}
		delay := backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if backoff < 10*time.Second {
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
}

func (router *redisRealtimeRouter) receive(ctx context.Context) error {
	subscription := router.client.Subscribe(ctx, router.instanceChannel(router.instanceID))
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		return err
	}
	router.readyOnce.Do(func() { close(router.ready) })
	messages := subscription.Channel(redis.WithChannelSize(256))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-messages:
			if !ok {
				return errors.New("realtime Redis subscription closed")
			}
			if err := router.handleCommand(ctx, []byte(message.Payload)); err != nil {
				router.observeRouting("redis_receive", "error")
				log.Printf("realtime Redis command: %v", err)
			} else {
				router.observeRouting("redis_receive", "success")
			}
		}
	}
}

func (router *redisRealtimeRouter) handleCommand(ctx context.Context, payload []byte) error {
	var command realtimeInstanceCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	switch command.Type {
	case realtimeCommandDeliver:
		if command.ReceiverID <= 0 || len(command.Payload) == 0 {
			return errors.New("delivery command is incomplete")
		}
		_, err := router.hub.Publish(ctx, command.ReceiverID, command.Payload)
		return err
	case realtimeCommandDisconnect:
		if command.ReceiverID <= 0 || command.ConnectionID == "" {
			return errors.New("disconnect command is incomplete")
		}
		return router.hub.DisconnectConnection(ctx, command.ReceiverID, command.ConnectionID)
	default:
		return fmt.Errorf("unsupported command type %q", command.Type)
	}
}

func (router *redisRealtimeRouter) instanceChannel(instanceID string) string {
	return fmt.Sprintf("%s:instance:%s", router.namespace, instanceID)
}

func (router *redisRealtimeRouter) observeRouting(stage, result string) {
	if router.metrics != nil {
		router.metrics.realtimeRouting.WithLabelValues(stage, result).Inc()
	}
}

func newRealtimeInstanceID(configured string) (string, error) {
	base := configured
	if base == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return "", err
		}
		base = hostname
	}
	bootID, err := newToken()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s", base, bootID.raw[:12]), nil
}
