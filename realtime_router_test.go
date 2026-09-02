package main

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type redisPipelineCountHook struct {
	pipelines atomic.Int32
	commands  atomic.Int32
}

func (hook *redisPipelineCountHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (hook *redisPipelineCountHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		return next(ctx, command)
	}
}

func (hook *redisPipelineCountHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		hook.pipelines.Add(1)
		hook.commands.Add(int32(len(commands)))
		return next(ctx, commands)
	}
}

func TestRedisPresenceFencesReplacedConnection(t *testing.T) {
	redisClient, namespace := openTestRedis(t)
	presence := mustTestPresence(t, redisClient, namespace, time.Second, 250*time.Millisecond)
	ctx := context.Background()
	old := webSocketPresenceRecord{UserID: 7, SessionID: 11, ConnectionID: "old", InstanceID: "instance-a"}
	current := webSocketPresenceRecord{UserID: 7, SessionID: 11, ConnectionID: "current", InstanceID: "instance-b"}

	replaced, err := presence.Register(ctx, old)
	if err != nil || replaced != nil {
		t.Fatalf("register old presence = replaced %+v, err %v", replaced, err)
	}
	replaced, err = presence.Register(ctx, current)
	if err != nil || replaced == nil || replaced.ConnectionID != old.ConnectionID {
		t.Fatalf("replace presence = replaced %+v, err %v", replaced, err)
	}
	if status, err := presence.Refresh(ctx, old); err != nil || status != webSocketPresenceLost {
		t.Fatalf("old refresh = status %v, err %v", status, err)
	}
	if removed, err := presence.Unregister(ctx, old); err != nil || removed {
		t.Fatalf("old unregister = removed %v, err %v", removed, err)
	}
	if status, err := presence.Refresh(ctx, current); err != nil || status != webSocketPresenceRenewed {
		t.Fatalf("current refresh = status %v, err %v", status, err)
	}

	records, err := presence.LookupUser(ctx, current.UserID)
	if err != nil || len(records) != 1 || records[0] != current {
		t.Fatalf("presence lookup = records %+v, err %v", records, err)
	}
	if removed, err := presence.Unregister(ctx, current); err != nil || !removed {
		t.Fatalf("current unregister = removed %v, err %v", removed, err)
	}
}

func TestRedisPresenceExpiresAndLazilyCleansUserIndex(t *testing.T) {
	redisClient, namespace := openTestRedis(t)
	presence := mustTestPresence(t, redisClient, namespace, 120*time.Millisecond, 40*time.Millisecond)
	record := webSocketPresenceRecord{UserID: 13, SessionID: 17, ConnectionID: "expires", InstanceID: "instance-a"}
	if _, err := presence.Register(context.Background(), record); err != nil {
		t.Fatalf("register expiring presence: %v", err)
	}
	time.Sleep(220 * time.Millisecond)
	if exists := redisClient.Exists(context.Background(), presence.userSessionsKey(record.UserID)).Val(); exists != 0 {
		t.Fatalf("expired user session index still exists")
	}
	records, err := presence.LookupUser(context.Background(), record.UserID)
	if err != nil || len(records) != 0 {
		t.Fatalf("expired presence lookup = records %+v, err %v", records, err)
	}
	if count := redisClient.SCard(context.Background(), presence.userSessionsKey(record.UserID)).Val(); count != 0 {
		t.Fatalf("stale user session count = %d, want 0", count)
	}
}

func TestRedisPresenceLookupUsersDeduplicatesIntoTwoPipelineRoundTrips(t *testing.T) {
	redisClient, namespace := openTestRedis(t)
	presence := mustTestPresence(t, redisClient, namespace, time.Second, 250*time.Millisecond)
	first := webSocketPresenceRecord{UserID: 61, SessionID: 71, ConnectionID: "first", InstanceID: "instance-a"}
	second := webSocketPresenceRecord{UserID: 67, SessionID: 73, ConnectionID: "second", InstanceID: "instance-b"}
	for _, record := range []webSocketPresenceRecord{first, second} {
		if _, err := presence.Register(context.Background(), record); err != nil {
			t.Fatalf("register batch Presence record: %v", err)
		}
	}

	hook := &redisPipelineCountHook{}
	redisClient.AddHook(hook)
	recordsByUser, err := presence.LookupUsers(context.Background(), []int64{61, 67, 61, 79, 67})
	if err != nil {
		t.Fatalf("lookup Presence batch: %v", err)
	}
	if records := recordsByUser[61]; len(records) != 1 || records[0] != first {
		t.Fatalf("first user Presence = %+v", records)
	}
	if records := recordsByUser[67]; len(records) != 1 || records[0] != second {
		t.Fatalf("second user Presence = %+v", records)
	}
	if records := recordsByUser[79]; len(records) != 0 {
		t.Fatalf("offline user Presence = %+v, want empty", records)
	}
	if calls := hook.pipelines.Load(); calls != 2 {
		t.Fatalf("Redis pipeline round trips = %d, want 2", calls)
	}
	if commands := hook.commands.Load(); commands != 5 {
		t.Fatalf("Redis commands = %d, want 5 (3 SMEMBERS + 2 MGET)", commands)
	}
}

func TestRedisRouterDeliversOncePerRemoteInstanceAndFansOutLocally(t *testing.T) {
	redisClient, namespace := openTestRedis(t)
	presence := mustTestPresence(t, redisClient, namespace, time.Second, 250*time.Millisecond)
	hubA, stopHubA := startTestHub(t)
	defer stopHubA()
	hubB, stopHubB := startTestHub(t)
	defer stopHubB()
	routerA := mustTestRouter(t, redisClient, presence, hubA, "instance-a")
	routerB := mustTestRouter(t, redisClient, presence, hubB, "instance-b")
	routerContext, cancelRouter := context.WithCancel(context.Background())
	go routerB.Run(routerContext)
	defer func() {
		cancelRouter()
		<-routerB.done
	}()
	select {
	case <-routerB.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("remote instance did not subscribe")
	}

	first := &webSocketClient{userID: 19, sessionID: 23, connectionID: "phone", send: make(chan []byte, 1)}
	second := &webSocketClient{userID: 19, sessionID: 29, connectionID: "desktop", send: make(chan []byte, 1)}
	if err := hubB.Register(context.Background(), first); err != nil {
		t.Fatalf("register first remote client: %v", err)
	}
	if err := hubB.Register(context.Background(), second); err != nil {
		t.Fatalf("register second remote client: %v", err)
	}
	for _, record := range []webSocketPresenceRecord{
		{UserID: 19, SessionID: 23, ConnectionID: "phone", InstanceID: "instance-b"},
		{UserID: 19, SessionID: 29, ConnectionID: "desktop", InstanceID: "instance-b"},
	} {
		if _, err := presence.Register(context.Background(), record); err != nil {
			t.Fatalf("register remote presence: %v", err)
		}
	}

	payload := []byte(`{"type":"message.created","message":{"id":99}}`)
	if _, err := routerA.Publish(context.Background(), 19, payload); err != nil {
		t.Fatalf("publish across instances: %v", err)
	}
	for name, destination := range map[string]<-chan []byte{"phone": first.send, "desktop": second.send} {
		select {
		case received := <-destination:
			if string(received) != string(payload) {
				t.Fatalf("%s payload = %q", name, received)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not receive remote delivery", name)
		}
	}
}

func TestRedisRouterDisconnectsReplacedConnectionOnOldInstance(t *testing.T) {
	redisClient, namespace := openTestRedis(t)
	presence := mustTestPresence(t, redisClient, namespace, time.Second, 250*time.Millisecond)
	hubA, stopHubA := startTestHub(t)
	defer stopHubA()
	hubB, stopHubB := startTestHub(t)
	defer stopHubB()
	routerA := mustTestRouter(t, redisClient, presence, hubA, "instance-a")
	routerB := mustTestRouter(t, redisClient, presence, hubB, "instance-b")
	routerContext, cancelRouter := context.WithCancel(context.Background())
	go routerB.Run(routerContext)
	defer func() {
		cancelRouter()
		<-routerB.done
	}()
	select {
	case <-routerB.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("old instance did not subscribe")
	}

	oldClient := &webSocketClient{userID: 31, sessionID: 37, connectionID: "old", send: make(chan []byte, 16)}
	if err := hubB.Register(context.Background(), oldClient); err != nil {
		t.Fatalf("register old client: %v", err)
	}
	oldRecord := webSocketPresenceRecord{UserID: 31, SessionID: 37, ConnectionID: "old", InstanceID: "instance-b"}
	if _, err := presence.Register(context.Background(), oldRecord); err != nil {
		t.Fatalf("register old presence: %v", err)
	}
	newRecord := webSocketPresenceRecord{UserID: 31, SessionID: 37, ConnectionID: "new", InstanceID: "instance-a"}
	replaced, err := presence.Register(context.Background(), newRecord)
	if err != nil || replaced == nil {
		t.Fatalf("replace presence = %+v, err %v", replaced, err)
	}
	if err := routerA.DisconnectReplaced(context.Background(), *replaced); err != nil {
		t.Fatalf("disconnect replaced connection: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		delivered, err := hubB.Publish(context.Background(), oldClient.userID, []byte("probe"))
		if err != nil {
			t.Fatalf("probe old hub: %v", err)
		}
		if delivered == 0 {
			break
		}
		<-oldClient.send
		if time.Now().After(deadline) {
			t.Fatal("replaced connection remained in old hub")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRedisRouterRevokesOnlyTargetSessionOnRemoteInstance(t *testing.T) {
	redisClient, namespace := openTestRedis(t)
	presence := mustTestPresence(t, redisClient, namespace, time.Second, 250*time.Millisecond)
	hubA, stopHubA := startTestHub(t)
	defer stopHubA()
	hubB, stopHubB := startTestHub(t)
	defer stopHubB()
	routerA := mustTestRouter(t, redisClient, presence, hubA, "instance-a")
	routerB := mustTestRouter(t, redisClient, presence, hubB, "instance-b")
	routerContext, cancelRouter := context.WithCancel(context.Background())
	go routerB.Run(routerContext)
	defer func() {
		cancelRouter()
		<-routerB.done
	}()
	select {
	case <-routerB.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("remote instance did not subscribe")
	}

	target := &webSocketClient{userID: 41, sessionID: 43, connectionID: "target", send: make(chan []byte, 4), closed: make(chan struct{})}
	other := &webSocketClient{userID: 41, sessionID: 47, connectionID: "other", send: make(chan []byte, 4), closed: make(chan struct{})}
	for _, client := range []*webSocketClient{target, other} {
		if err := hubB.Register(context.Background(), client); err != nil {
			t.Fatalf("register remote session: %v", err)
		}
		if _, err := presence.Register(context.Background(), webSocketPresenceRecord{
			UserID:       client.userID,
			SessionID:    client.sessionID,
			ConnectionID: client.connectionID,
			InstanceID:   "instance-b",
		}); err != nil {
			t.Fatalf("register remote session presence: %v", err)
		}
	}
	if err := routerA.DisconnectSession(context.Background(), target.userID, target.sessionID); err != nil {
		t.Fatalf("disconnect remote session: %v", err)
	}

	select {
	case <-target.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("target remote session was not closed")
	}
	select {
	case <-other.closed:
		t.Fatal("unrelated remote session was closed")
	default:
	}
	delivered, err := hubB.Publish(context.Background(), target.userID, []byte("probe"))
	if err != nil || delivered != 1 {
		t.Fatalf("remaining remote session publish = delivered %d, err %v", delivered, err)
	}
	<-other.send
}

func TestRedisRouterKeepsLocalDeliveryWhenRedisIsUnavailable(t *testing.T) {
	options := &redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  25 * time.Millisecond,
		ReadTimeout:  25 * time.Millisecond,
		WriteTimeout: 25 * time.Millisecond,
		MaxRetries:   0,
	}
	redisClient := redis.NewClient(options)
	defer redisClient.Close()
	presence := mustTestPresence(t, redisClient, "im:test:unavailable", time.Second, 250*time.Millisecond)
	hub, stopHub := startTestHub(t)
	defer stopHub()
	client := &webSocketClient{userID: 53, sessionID: 59, connectionID: "local", send: make(chan []byte, 1)}
	if err := hub.Register(context.Background(), client); err != nil {
		t.Fatalf("register local client: %v", err)
	}
	router := mustTestRouter(t, redisClient, presence, hub, "instance-local")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	delivered, err := router.Publish(ctx, client.userID, []byte("local"))
	if err == nil || delivered != 1 {
		t.Fatalf("Redis outage publish = delivered %d, err %v", delivered, err)
	}
	select {
	case payload := <-client.send:
		if string(payload) != "local" {
			t.Fatalf("local outage payload = %q", payload)
		}
	default:
		t.Fatal("local delivery was lost during Redis outage")
	}
}

func TestRedisBatchRouterKeepsLocalDeliveryWhenRedisIsUnavailable(t *testing.T) {
	options := &redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  25 * time.Millisecond,
		ReadTimeout:  25 * time.Millisecond,
		WriteTimeout: 25 * time.Millisecond,
		MaxRetries:   0,
	}
	redisClient := redis.NewClient(options)
	defer redisClient.Close()
	presence := mustTestPresence(t, redisClient, "im:test:batch-unavailable", time.Second, 250*time.Millisecond)
	hub, stopHub := startTestHub(t)
	defer stopHub()
	client := &webSocketClient{userID: 83, sessionID: 89, connectionID: "local", send: make(chan []byte, 1)}
	if err := hub.Register(context.Background(), client); err != nil {
		t.Fatalf("register local batch client: %v", err)
	}
	router := mustTestRouter(t, redisClient, presence, hub, "instance-local")
	prepareContext, cancelPrepare := context.WithTimeout(context.Background(), 250*time.Millisecond)
	batchRouter, err := router.PrepareDeliveryBatch(prepareContext, []int64{client.userID})
	cancelPrepare()
	if err != nil {
		t.Fatalf("prepare unavailable Redis batch router: %v", err)
	}
	delivered, err := batchRouter.Publish(context.Background(), client.userID, []byte("local-batch"))
	if err == nil || delivered != 1 {
		t.Fatalf("Redis outage batch publish = delivered %d, err %v", delivered, err)
	}
	select {
	case payload := <-client.send:
		if string(payload) != "local-batch" {
			t.Fatalf("local batch outage payload = %q", payload)
		}
	default:
		t.Fatal("local batch delivery was lost during Redis outage")
	}
}

func TestTwoApplicationInstancesDeliverOutboxEventToRemoteWebSocket(t *testing.T) {
	db := openTestDatabase(t)
	redisClient, namespace := openTestRedis(t)
	appA, stopA := newRedisBackedWebSocketTestApplication(t, db, redisClient, namespace, "instance-a")
	defer stopA()
	appB, stopB := newRedisBackedWebSocketTestApplication(t, db, redisClient, namespace, "instance-b")
	defer stopB()
	serverA := httptest.NewServer(appA.routes())
	defer serverA.Close()
	serverB := httptest.NewServer(appB.routes())
	defer serverB.Close()

	sender := registerTestAccount(t, db, serverA.URL, uniqueUsername("multi_s"), "Multi Sender")
	receiver := registerTestAccount(t, db, serverB.URL, uniqueUsername("multi_r"), "Multi Receiver")
	connection := dialAuthenticatedWebSocket(t, serverB.URL, receiver.Auth.AccessToken)
	defer connection.CloseNow()

	deadline := time.Now().Add(2 * time.Second)
	for {
		records, err := appA.webSocketPresence.LookupUser(context.Background(), receiver.User.ID)
		if err != nil {
			t.Fatalf("lookup remote application presence: %v", err)
		}
		if len(records) == 1 && records[0].InstanceID == "instance-b" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("remote websocket presence = %+v", records)
		}
		time.Sleep(10 * time.Millisecond)
	}

	created := createMessageThroughAPI(t, serverA.URL, sender.Auth.AccessToken, receiver.User.ID, "cross-instance")
	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	config.Concurrency = 1
	worker := mustTestWorker(t, db, &webSocketOutboxPublisher{
		router: appA.realtimeRouter,
	}, config)
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("run cross-instance outbox worker = processed %d, err %v", processed, err)
	}
	envelope := readWebSocketEnvelope(t, connection)
	if envelope.Message.ID != created.ID ||
		envelope.ConversationID != created.ConversationID ||
		envelope.ConversationSeq != created.ConversationSeq ||
		envelope.Cursor != 0 {
		t.Fatalf("cross-instance websocket envelope = %+v", envelope)
	}
}

func TestNewConnectionReplacesSameSessionOnAnotherApplicationInstance(t *testing.T) {
	db := openTestDatabase(t)
	redisClient, namespace := openTestRedis(t)
	appA, stopA := newRedisBackedWebSocketTestApplication(t, db, redisClient, namespace, "instance-a")
	defer stopA()
	appB, stopB := newRedisBackedWebSocketTestApplication(t, db, redisClient, namespace, "instance-b")
	defer stopB()
	serverA := httptest.NewServer(appA.routes())
	defer serverA.Close()
	serverB := httptest.NewServer(appB.routes())
	defer serverB.Close()

	account := registerTestAccount(t, db, serverB.URL, uniqueUsername("replace"), "Replace")
	oldConnection := dialAuthenticatedWebSocket(t, serverB.URL, account.Auth.AccessToken)
	defer oldConnection.CloseNow()
	waitForPresenceInstance(t, appA.webSocketPresence, account.User.ID, "instance-b")

	newConnection := dialAuthenticatedWebSocket(t, serverA.URL, account.Auth.AccessToken)
	defer newConnection.CloseNow()
	oldReadContext, oldReadCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer oldReadCancel()
	if _, _, err := oldConnection.Read(oldReadContext); err == nil {
		t.Fatal("old cross-instance websocket remained open after replacement")
	}
	waitForPresenceInstance(t, appA.webSocketPresence, account.User.ID, "instance-a")

	payload := []byte(`{"type":"replacement-active"}`)
	if _, err := appA.realtimeRouter.Publish(context.Background(), account.User.ID, payload); err != nil {
		t.Fatalf("publish to replacement connection: %v", err)
	}
	readContext, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()
	_, received, err := newConnection.Read(readContext)
	if err != nil || string(received) != string(payload) {
		t.Fatalf("replacement connection received %q, err %v", received, err)
	}
}

func openTestRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL is required for Redis integration tests")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse test Redis URL: %v", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Fatalf("connect to test Redis: %v", err)
	}
	namespace := fmt.Sprintf("im:test:ws:%d", time.Now().UnixNano())
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := client.Scan(context.Background(), cursor, namespace+"*", 100).Result()
			if err != nil {
				t.Errorf("scan test Redis keys: %v", err)
				break
			}
			if len(keys) > 0 {
				if err := client.Del(context.Background(), keys...).Err(); err != nil {
					t.Errorf("delete test Redis keys: %v", err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		client.Close()
	})
	return client, namespace
}

func mustTestPresence(
	t *testing.T,
	client *redis.Client,
	namespace string,
	ttl time.Duration,
	renew time.Duration,
) *redisWebSocketPresence {
	t.Helper()
	presence, err := newRedisWebSocketPresence(client, namespace, ttl, renew)
	if err != nil {
		t.Fatalf("create test presence: %v", err)
	}
	return presence
}

func mustTestRouter(
	t *testing.T,
	client *redis.Client,
	presence *redisWebSocketPresence,
	hub *webSocketHub,
	instanceID string,
) *redisRealtimeRouter {
	t.Helper()
	router, err := newRedisRealtimeRouter(client, presence, hub, instanceID)
	if err != nil {
		t.Fatalf("create test router: %v", err)
	}
	return router
}

func startTestHub(t *testing.T) (*webSocketHub, func()) {
	t.Helper()
	hub := newWebSocketHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	return hub, func() {
		cancel()
		select {
		case <-hub.done:
		case <-time.After(2 * time.Second):
			t.Error("test hub did not stop")
		}
	}
}

func newRedisBackedWebSocketTestApplication(
	t *testing.T,
	db *pgxpool.Pool,
	redisClient *redis.Client,
	namespace string,
	instanceID string,
) (*application, func()) {
	t.Helper()
	app := newTestApplication(t, db)
	hub, stopHub := startTestHub(t)
	presence := mustTestPresence(t, redisClient, namespace, 2*time.Second, 500*time.Millisecond)
	router := mustTestRouter(t, redisClient, presence, hub, instanceID)
	routerContext, cancelRouter := context.WithCancel(context.Background())
	go router.Run(routerContext)
	select {
	case <-router.ready:
	case <-time.After(2 * time.Second):
		cancelRouter()
		stopHub()
		t.Fatal("test application realtime router did not subscribe")
	}
	app.webSocketHub = hub
	app.webSocketPresence = presence
	app.realtimeRouter = router
	return app, func() {
		cancelRouter()
		select {
		case <-router.done:
		case <-time.After(2 * time.Second):
			t.Error("test application realtime router did not stop")
		}
		stopHub()
	}
}

func waitForPresenceInstance(
	t *testing.T,
	presence *redisWebSocketPresence,
	userID int64,
	instanceID string,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		records, err := presence.LookupUser(context.Background(), userID)
		if err != nil {
			t.Fatalf("lookup websocket presence: %v", err)
		}
		if len(records) == 1 && records[0].InstanceID == instanceID {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("presence for user %d = %+v, want instance %s", userID, records, instanceID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
