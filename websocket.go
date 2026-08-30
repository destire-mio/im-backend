package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	defaultWebSocketSendBuffer = 256
	defaultWebSocketReadLimit  = 1024
	defaultWebSocketWriteWait  = 10 * time.Second
	defaultWebSocketPingPeriod = 25 * time.Second
)

var errWebSocketHubStopped = errors.New("websocket hub is stopped")

type messageEventRecipient struct {
	UserID int64 `json:"userId"`
	Cursor int64 `json:"cursor"`
}

type messageCreatedEventPayload struct {
	Message    message                 `json:"message"`
	Recipients []messageEventRecipient `json:"recipients"`
}

// messageCreatedPendingPayload is payload version 3. It is durable, but it is
// not publishable until the sync projector allocates recipient cursors and
// atomically rewrites it to messageCreatedEventPayload (version 2).
type messageCreatedPendingPayload struct {
	Message message `json:"message"`
}

type webSocketEnvelope struct {
	Type    string  `json:"type"`
	EventID string  `json:"eventId"`
	Cursor  int64   `json:"cursor,omitempty"`
	Message message `json:"message"`
}

type webSocketClientConfig struct {
	SendBuffer int
	ReadLimit  int64
	WriteWait  time.Duration
	PingPeriod time.Duration
}

func defaultWebSocketClientConfig() webSocketClientConfig {
	return webSocketClientConfig{
		SendBuffer: defaultWebSocketSendBuffer,
		ReadLimit:  defaultWebSocketReadLimit,
		WriteWait:  defaultWebSocketWriteWait,
		PingPeriod: defaultWebSocketPingPeriod,
	}
}

type webSocketClient struct {
	userID       int64
	sessionID    int64
	connectionID string
	connection   *websocket.Conn
	send         chan []byte
	config       webSocketClientConfig
	cancel       context.CancelFunc
	closeOnce    sync.Once
	closed       chan struct{}
	metrics      *applicationMetrics
}

func newWebSocketClient(
	userID int64,
	sessionID int64,
	connectionID string,
	connection *websocket.Conn,
	config webSocketClientConfig,
	metrics *applicationMetrics,
) (*webSocketClient, error) {
	if userID <= 0 || sessionID <= 0 || connectionID == "" || connection == nil {
		return nil, errors.New("websocket client identity and connection are required")
	}
	if config.SendBuffer <= 0 || config.ReadLimit <= 0 || config.WriteWait <= 0 || config.PingPeriod <= 0 {
		return nil, errors.New("websocket client limits must be positive")
	}
	connection.SetReadLimit(config.ReadLimit)
	return &webSocketClient{
		userID:       userID,
		sessionID:    sessionID,
		connectionID: connectionID,
		connection:   connection,
		send:         make(chan []byte, config.SendBuffer),
		config:       config,
		closed:       make(chan struct{}),
		metrics:      metrics,
	}, nil
}

func (client *webSocketClient) run(ctx context.Context, hub *webSocketHub) {
	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- client.readPump(ctx) }()
	go func() { errorsChannel <- client.writePump(ctx) }()

	<-errorsChannel
	code := websocket.StatusNormalClosure
	reason := "connection closed"
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = websocket.StatusPolicyViolation
		reason = "access token expired"
	}

	unregisterContext, unregisterCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = hub.Unregister(unregisterContext, client, code, reason)
	unregisterCancel()
	if client.cancel != nil {
		client.cancel()
	}
	_ = client.connection.CloseNow()
}

func (client *webSocketClient) readPump(ctx context.Context) error {
	for {
		_, _, err := client.connection.Read(ctx)
		if err != nil {
			return err
		}
		return errors.New("client application messages are not supported on this websocket")
	}
}

func (client *webSocketClient) writePump(ctx context.Context) error {
	pingTicker := time.NewTicker(client.config.PingPeriod)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload := <-client.send:
			writeContext, cancel := context.WithTimeout(ctx, client.config.WriteWait)
			started := time.Now()
			err := client.connection.Write(writeContext, websocket.MessageText, payload)
			result := "success"
			if err != nil {
				result = "error"
			}
			client.metrics.ObserveWebSocketWrite(time.Since(started), result)
			cancel()
			if err != nil {
				client.observeIO("write", "error")
				return err
			}
			client.observeIO("write", "success")
		case <-pingTicker.C:
			pingContext, cancel := context.WithTimeout(ctx, client.config.WriteWait)
			err := client.connection.Ping(pingContext)
			cancel()
			if err != nil {
				client.observeIO("ping", "error")
				return err
			}
			client.observeIO("ping", "success")
		}
	}
}

func (client *webSocketClient) observeIO(operation, result string) {
	if client.metrics != nil {
		client.metrics.webSocketIO.WithLabelValues(operation, result).Inc()
	}
}

func (client *webSocketClient) shutdown(code websocket.StatusCode, reason string) {
	client.closeOnce.Do(func() {
		if client.closed != nil {
			close(client.closed)
		}
		if client.cancel != nil {
			client.cancel()
		}
		if client.connection == nil {
			return
		}
		go func() {
			if err := client.connection.Close(code, reason); err != nil {
				_ = client.connection.CloseNow()
			}
		}()
	})
}

type webSocketDelivery struct {
	receiverID int64
	payload    []byte
	result     chan int
}

type webSocketRegistration struct {
	client *webSocketClient
	result chan struct{}
}

type webSocketUnregister struct {
	client *webSocketClient
	code   websocket.StatusCode
	reason string
}

type webSocketDisconnect struct {
	userID       int64
	sessionID    int64
	connectionID string
	result       chan struct{}
}

type webSocketHub struct {
	clients              map[int64]map[string]*webSocketClient
	register             chan webSocketRegistration
	unregister           chan webSocketUnregister
	publish              chan webSocketDelivery
	disconnectSession    chan webSocketDisconnect
	disconnectUser       chan webSocketDisconnect
	disconnectConnection chan webSocketDisconnect
	done                 chan struct{}
	metrics              *applicationMetrics
}

func newWebSocketHub(metricObservers ...*applicationMetrics) *webSocketHub {
	var metrics *applicationMetrics
	if len(metricObservers) > 0 {
		metrics = metricObservers[0]
	}
	return &webSocketHub{
		clients:              make(map[int64]map[string]*webSocketClient),
		register:             make(chan webSocketRegistration, 128),
		unregister:           make(chan webSocketUnregister, 128),
		publish:              make(chan webSocketDelivery, 1024),
		disconnectSession:    make(chan webSocketDisconnect, 32),
		disconnectUser:       make(chan webSocketDisconnect, 32),
		disconnectConnection: make(chan webSocketDisconnect, 32),
		done:                 make(chan struct{}),
		metrics:              metrics,
	}
}

func (hub *webSocketHub) Run(ctx context.Context) {
	defer close(hub.done)
	for {
		select {
		case <-ctx.Done():
			for _, userClients := range hub.clients {
				for _, client := range userClients {
					hub.observeDisconnect("server_shutdown")
					client.shutdown(websocket.StatusGoingAway, "server shutting down")
				}
			}
			hub.clients = make(map[int64]map[string]*webSocketClient)
			if hub.metrics != nil {
				hub.metrics.webSocketConnections.Set(0)
			}
			return
		case registration := <-hub.register:
			client := registration.client
			userClients := hub.clients[client.userID]
			if userClients == nil {
				userClients = make(map[string]*webSocketClient)
				hub.clients[client.userID] = userClients
			}
			userClients[client.connectionID] = client
			if hub.metrics != nil {
				hub.metrics.webSocketConnections.Inc()
			}
			close(registration.result)
		case request := <-hub.unregister:
			hub.removeClient(request.client, request.code, request.reason)
		case delivery := <-hub.publish:
			delivery.result <- hub.deliver(delivery.receiverID, delivery.payload)
		case command := <-hub.disconnectSession:
			hub.disconnectMatching(command.userID, command.sessionID)
			close(command.result)
		case command := <-hub.disconnectUser:
			hub.disconnectMatching(command.userID, 0)
			close(command.result)
		case command := <-hub.disconnectConnection:
			hub.disconnectOne(command.userID, command.connectionID)
			close(command.result)
		}
	}
}

func (hub *webSocketHub) Register(ctx context.Context, client *webSocketClient) error {
	registration := webSocketRegistration{client: client, result: make(chan struct{})}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-hub.done:
		return errWebSocketHubStopped
	case hub.register <- registration:
	}
	select {
	case <-hub.done:
		return errWebSocketHubStopped
	case <-registration.result:
		return nil
	}
}

func (hub *webSocketHub) Unregister(
	ctx context.Context,
	client *webSocketClient,
	code websocket.StatusCode,
	reason string,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-hub.done:
		return errWebSocketHubStopped
	case hub.unregister <- webSocketUnregister{client: client, code: code, reason: reason}:
		return nil
	}
}

func (hub *webSocketHub) Publish(ctx context.Context, receiverID int64, payload []byte) (int, error) {
	result := make(chan int, 1)
	request := webSocketDelivery{receiverID: receiverID, payload: payload, result: result}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-hub.done:
		return 0, errWebSocketHubStopped
	case hub.publish <- request:
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-hub.done:
		return 0, errWebSocketHubStopped
	case delivered := <-result:
		return delivered, nil
	}
}

func (hub *webSocketHub) DisconnectSession(ctx context.Context, userID, sessionID int64) error {
	return hub.disconnect(ctx, hub.disconnectSession, webSocketDisconnect{userID: userID, sessionID: sessionID})
}

func (hub *webSocketHub) DisconnectUser(ctx context.Context, userID int64) error {
	return hub.disconnect(ctx, hub.disconnectUser, webSocketDisconnect{userID: userID})
}

func (hub *webSocketHub) DisconnectConnection(ctx context.Context, userID int64, connectionID string) error {
	if userID <= 0 || connectionID == "" {
		return errors.New("websocket connection identity is required")
	}
	return hub.disconnect(
		ctx,
		hub.disconnectConnection,
		webSocketDisconnect{userID: userID, connectionID: connectionID},
	)
}

func (hub *webSocketHub) disconnect(
	ctx context.Context,
	destination chan webSocketDisconnect,
	command webSocketDisconnect,
) error {
	command.result = make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-hub.done:
		return errWebSocketHubStopped
	case destination <- command:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-hub.done:
		return errWebSocketHubStopped
	case <-command.result:
		return nil
	}
}

func (hub *webSocketHub) deliver(receiverID int64, payload []byte) int {
	delivered := 0
	if len(hub.clients[receiverID]) == 0 && hub.metrics != nil {
		hub.metrics.webSocketDeliveries.WithLabelValues("no_connection").Inc()
	}
	for _, client := range hub.clients[receiverID] {
		select {
		case client.send <- payload:
			delivered++
			if hub.metrics != nil {
				hub.metrics.webSocketDeliveries.WithLabelValues("queued").Inc()
				hub.metrics.ObserveWebSocketQueueDepth(len(client.send))
			}
		default:
			if hub.metrics != nil {
				hub.metrics.webSocketDeliveries.WithLabelValues("slow_client").Inc()
				hub.metrics.ObserveWebSocketQueueDepth(len(client.send))
			}
			hub.removeClient(client, websocket.StatusPolicyViolation, "slow client")
		}
	}
	return delivered
}

func (hub *webSocketHub) disconnectMatching(userID, sessionID int64) {
	for _, client := range hub.clients[userID] {
		if sessionID == 0 || client.sessionID == sessionID {
			hub.removeClient(client, websocket.StatusPolicyViolation, "session revoked")
		}
	}
}

func (hub *webSocketHub) disconnectOne(userID int64, connectionID string) {
	client := hub.clients[userID][connectionID]
	if client != nil {
		hub.removeClient(client, websocket.StatusPolicyViolation, "connection replaced")
	}
}

func (hub *webSocketHub) removeClient(client *webSocketClient, code websocket.StatusCode, reason string) {
	userClients := hub.clients[client.userID]
	if userClients == nil || userClients[client.connectionID] != client {
		return
	}
	delete(userClients, client.connectionID)
	if hub.metrics != nil {
		hub.metrics.webSocketConnections.Dec()
	}
	if len(userClients) == 0 {
		delete(hub.clients, client.userID)
	}
	client.shutdown(code, reason)
	hub.observeDisconnect(webSocketDisconnectReason(reason))
}

func (hub *webSocketHub) observeDisconnect(reason string) {
	if hub.metrics != nil {
		hub.metrics.webSocketDisconnects.WithLabelValues(reason).Inc()
	}
}

func webSocketDisconnectReason(reason string) string {
	switch reason {
	case "slow client":
		return "slow_client"
	case "session revoked":
		return "session_revoked"
	case "connection replaced":
		return "connection_replaced"
	case "connection ownership lost":
		return "ownership_lost"
	case "access token expired":
		return "token_expired"
	case "connection closed":
		return "connection_closed"
	case "server shutting down":
		return "server_shutdown"
	default:
		return "other"
	}
}

type webSocketOutboxPublisher struct {
	router webSocketDeliveryRouter
}

func (publisher *webSocketOutboxPublisher) Publish(ctx context.Context, event outboxEvent) error {
	if event.EventType != "message.created" {
		return permanentPublishError(fmt.Errorf("unsupported websocket event type %q", event.EventType))
	}

	payload, err := decodeMessageCreatedEvent(event)
	if err != nil {
		return permanentPublishError(err)
	}
	for _, recipient := range payload.Recipients {
		envelope, err := json.Marshal(webSocketEnvelope{
			Type:    event.EventType,
			EventID: event.EventID,
			Cursor:  recipient.Cursor,
			Message: payload.Message,
		})
		if err != nil {
			return permanentPublishError(fmt.Errorf("encode websocket event: %w", err))
		}
		if _, err := publisher.router.Publish(ctx, recipient.UserID, envelope); err != nil {
			return retryablePublishError(fmt.Errorf("publish websocket event: %w", err), 0)
		}
	}
	return nil
}

func decodeMessageCreatedEvent(event outboxEvent) (messageCreatedEventPayload, error) {
	switch event.PayloadVersion {
	case 1:
		var legacy struct {
			MessageID       int64     `json:"messageId"`
			ClientMessageID string    `json:"clientMessageId"`
			SenderID        int64     `json:"senderId"`
			ReceiverID      int64     `json:"receiverId"`
			Content         string    `json:"content"`
			CreatedAt       time.Time `json:"createdAt"`
		}
		if err := json.Unmarshal(event.Payload, &legacy); err != nil {
			return messageCreatedEventPayload{}, fmt.Errorf("decode legacy message.created payload: %w", err)
		}
		if legacy.MessageID <= 0 || legacy.ReceiverID <= 0 {
			return messageCreatedEventPayload{}, errors.New("legacy message.created payload is incomplete")
		}
		return messageCreatedEventPayload{
			Message: message{
				ID:              legacy.MessageID,
				ClientMessageID: legacy.ClientMessageID,
				SenderID:        legacy.SenderID,
				ReceiverID:      legacy.ReceiverID,
				Content:         legacy.Content,
				CreatedAt:       legacy.CreatedAt,
			},
			Recipients: []messageEventRecipient{{
				UserID: legacy.ReceiverID,
			}},
		}, nil
	case 2:
		var payload messageCreatedEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return messageCreatedEventPayload{}, fmt.Errorf("decode message.created payload: %w", err)
		}
		if payload.Message.ID <= 0 || len(payload.Recipients) == 0 {
			return messageCreatedEventPayload{}, errors.New("message.created payload is incomplete")
		}
		for _, recipient := range payload.Recipients {
			if recipient.UserID <= 0 || recipient.Cursor <= 0 {
				return messageCreatedEventPayload{}, errors.New("message.created recipient is invalid")
			}
		}
		return payload, nil
	default:
		return messageCreatedEventPayload{}, fmt.Errorf("unsupported message.created payload version %d", event.PayloadVersion)
	}
}

func (app *application) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if app.webSocketHub == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "realtime messaging is unavailable"})
		return
	}

	connection, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("accept websocket: %v", err)
		return
	}
	observeHTTPStatus(w, http.StatusSwitchingProtocols)

	identifier, err := newToken()
	if err != nil {
		_ = connection.Close(websocket.StatusInternalError, "could not create connection")
		return
	}
	client, err := newWebSocketClient(
		authenticatedUserID(r.Context()),
		authenticatedSessionID(r.Context()),
		identifier.raw,
		connection,
		defaultWebSocketClientConfig(),
		app.metrics,
	)
	if err != nil {
		_ = connection.Close(websocket.StatusInternalError, "could not create connection")
		return
	}

	connectionContext, cancel := context.WithDeadline(context.Background(), authenticatedAccessTokenExpiry(r.Context()))
	defer cancel()
	client.cancel = cancel
	if err := app.webSocketHub.Register(connectionContext, client); err != nil {
		_ = connection.Close(websocket.StatusTryAgainLater, "realtime messaging is unavailable")
		return
	}

	var presenceRecord *webSocketPresenceRecord
	if app.webSocketPresence != nil && app.realtimeRouter != nil {
		record := webSocketPresenceRecord{
			UserID:       client.userID,
			SessionID:    client.sessionID,
			ConnectionID: client.connectionID,
			InstanceID:   app.realtimeRouter.instanceID,
		}
		presenceRecord = &record
		registerContext, registerCancel := context.WithTimeout(connectionContext, 2*time.Second)
		old, err := app.webSocketPresence.Register(registerContext, record)
		registerCancel()
		if err != nil {
			log.Printf("register websocket presence for session %d: %v", client.sessionID, err)
		} else if old != nil && old.ConnectionID != client.connectionID {
			disconnectContext, disconnectCancel := context.WithTimeout(connectionContext, 2*time.Second)
			if err := app.realtimeRouter.DisconnectReplaced(disconnectContext, *old); err != nil {
				log.Printf("disconnect replaced websocket %s: %v", old.ConnectionID, err)
			}
			disconnectCancel()
		}
		go app.maintainWebSocketPresence(connectionContext, client, record)
	}
	client.run(connectionContext, app.webSocketHub)
	if presenceRecord != nil {
		unregisterContext, unregisterCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if _, err := app.webSocketPresence.Unregister(unregisterContext, *presenceRecord); err != nil {
			log.Printf("unregister websocket presence for session %d: %v", client.sessionID, err)
		}
		unregisterCancel()
	}
}

func (app *application) maintainWebSocketPresence(
	ctx context.Context,
	client *webSocketClient,
	record webSocketPresenceRecord,
) {
	ticker := time.NewTicker(app.webSocketPresence.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshContext, cancel := context.WithTimeout(ctx, 2*time.Second)
			status, err := app.webSocketPresence.Refresh(refreshContext, record)
			cancel()
			if err != nil {
				log.Printf("refresh websocket presence for session %d: %v", client.sessionID, err)
				continue
			}
			if status != webSocketPresenceLost {
				continue
			}
			unregisterContext, unregisterCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = app.webSocketHub.Unregister(
				unregisterContext,
				client,
				websocket.StatusPolicyViolation,
				"connection ownership lost",
			)
			unregisterCancel()
			return
		}
	}
}
