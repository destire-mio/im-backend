package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	multiInstanceHelperEnv       = "IM_MULTI_INSTANCE_HELPER"
	multiInstanceRoleEnv         = "IM_MULTI_INSTANCE_ROLE"
	multiInstanceNamespaceEnv    = "IM_MULTI_INSTANCE_NAMESPACE"
	multiInstanceReadyLinePrefix = "IM_MULTI_INSTANCE_READY="
)

// TestTwoProcessesDeliverOutboxEventToRemoteWebSocket verifies the process
// boundary that the in-process router tests cannot cover. Only process A runs
// an Outbox Worker, while the receiver WebSocket is owned by process B. A
// successful delivery must therefore cross Redis Pub/Sub.
func TestTwoProcessesDeliverOutboxEventToRemoteWebSocket(t *testing.T) {
	if os.Getenv(multiInstanceHelperEnv) != "" {
		t.Skip("parent-only process integration test")
	}
	db := openTestDatabase(t)
	redisClient, namespace := openTestRedis(t)

	processA := startMultiInstanceHelper(t, "a", namespace)
	processB := startMultiInstanceHelper(t, "b", namespace)

	sender := registerTestAccount(t, db, processA.url, uniqueUsername("process_s"), "Process Sender")
	receiver := registerTestAccount(t, db, processB.url, uniqueUsername("process_r"), "Process Receiver")
	connection := dialAuthenticatedWebSocket(t, processB.url, receiver.Auth.AccessToken)
	defer connection.CloseNow()

	presence := mustTestPresence(t, redisClient, namespace, 2*time.Second, 500*time.Millisecond)
	waitForPresenceInstance(t, presence, receiver.User.ID, "process-b")

	created := createMessageThroughAPI(
		t,
		processA.url,
		sender.Auth.AccessToken,
		receiver.User.ID,
		"cross-process",
	)
	envelope := readWebSocketEnvelope(t, connection)
	if envelope.Message.ID != created.ID ||
		envelope.ConversationID != created.ConversationID ||
		envelope.ConversationSeq != created.ConversationSeq ||
		envelope.Cursor != 0 {
		t.Fatalf("cross-process websocket envelope = %+v", envelope)
	}
}

// TestTwoProcessesDeliverConcurrentBurst distributes API requests across both
// processes while the sender and receiver WebSockets are owned by different
// processes. It is a correctness smoke under concurrency, not a capacity
// benchmark: every committed message must arrive exactly once on both sockets.
func TestTwoProcessesDeliverConcurrentBurst(t *testing.T) {
	if os.Getenv(multiInstanceHelperEnv) != "" {
		t.Skip("parent-only process integration test")
	}
	db := openTestDatabase(t)
	redisClient, namespace := openTestRedis(t)

	processA := startMultiInstanceHelper(t, "a", namespace)
	processB := startMultiInstanceHelper(t, "b", namespace)

	sender := registerTestAccount(t, db, processA.url, uniqueUsername("burst_s"), "Burst Sender")
	receiver := registerTestAccount(t, db, processB.url, uniqueUsername("burst_r"), "Burst Receiver")
	senderConnection := dialAuthenticatedWebSocket(t, processA.url, sender.Auth.AccessToken)
	defer senderConnection.CloseNow()
	receiverConnection := dialAuthenticatedWebSocket(t, processB.url, receiver.Auth.AccessToken)
	defer receiverConnection.CloseNow()

	presence := mustTestPresence(t, redisClient, namespace, 2*time.Second, 500*time.Millisecond)
	waitForPresenceInstance(t, presence, sender.User.ID, "process-a")
	waitForPresenceInstance(t, presence, receiver.User.ID, "process-b")

	const messageCount = 200
	senderReads := collectMultiInstanceMessages(senderConnection, messageCount, 10*time.Second)
	receiverReads := collectMultiInstanceMessages(receiverConnection, messageCount, 10*time.Second)

	created := make(chan int64, messageCount)
	errors := make(chan error, messageCount)
	slots := make(chan struct{}, 32)
	clientIDPrefix := uniqueOpaqueID("multi-instance-burst")
	var sends sync.WaitGroup
	for index := 0; index < messageCount; index++ {
		sends.Add(1)
		go func(index int) {
			defer sends.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			baseURL := processA.url
			if index%2 == 1 {
				baseURL = processB.url
			}
			messageID, err := createMultiInstanceMessage(
				baseURL,
				sender.Auth.AccessToken,
				receiver.User.ID,
				fmt.Sprintf("%s-%03d", clientIDPrefix, index),
				fmt.Sprintf("concurrent-burst-%03d", index),
			)
			if err != nil {
				errors <- err
				return
			}
			created <- messageID
		}(index)
	}
	sends.Wait()
	close(created)
	close(errors)
	for err := range errors {
		t.Errorf("send concurrent message: %v", err)
	}
	if t.Failed() {
		return
	}

	want := make(map[int64]struct{}, messageCount)
	for messageID := range created {
		want[messageID] = struct{}{}
	}
	if len(want) != messageCount {
		t.Fatalf("created unique messages = %d, want %d", len(want), messageCount)
	}
	assertMultiInstanceDeliveries(t, "sender", <-senderReads, want)
	assertMultiInstanceDeliveries(t, "receiver", <-receiverReads, want)
}

type multiInstanceReadResult struct {
	messageIDs []int64
	err        error
}

func collectMultiInstanceMessages(
	connection interface {
		Read(context.Context) (websocket.MessageType, []byte, error)
	},
	count int,
	timeout time.Duration,
) <-chan multiInstanceReadResult {
	result := make(chan multiInstanceReadResult, 1)
	go func() {
		defer close(result)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		messageIDs := make([]int64, 0, count)
		for len(messageIDs) < count {
			messageType, payload, err := connection.Read(ctx)
			if err != nil {
				result <- multiInstanceReadResult{messageIDs: messageIDs, err: err}
				return
			}
			if messageType != websocket.MessageText {
				result <- multiInstanceReadResult{messageIDs: messageIDs, err: fmt.Errorf("message type %v", messageType)}
				return
			}
			var envelope webSocketEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				result <- multiInstanceReadResult{messageIDs: messageIDs, err: fmt.Errorf("decode envelope: %w", err)}
				return
			}
			messageIDs = append(messageIDs, envelope.Message.ID)
		}
		result <- multiInstanceReadResult{messageIDs: messageIDs}
	}()
	return result
}

func createMultiInstanceMessage(baseURL, token string, receiverID int64, clientMessageID, content string) (int64, error) {
	body, err := json.Marshal(createMessageRequest{
		ClientMessageID: clientMessageID,
		ReceiverID:      receiverID,
		Content:         &content,
	})
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/messages", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("HTTP %d", response.StatusCode)
	}
	var created message
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return 0, err
	}
	return created.ID, nil
}

func assertMultiInstanceDeliveries(
	t *testing.T,
	connection string,
	result multiInstanceReadResult,
	want map[int64]struct{},
) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("%s websocket read %d/%d messages: %v", connection, len(result.messageIDs), len(want), result.err)
	}
	seen := make(map[int64]int, len(result.messageIDs))
	for _, messageID := range result.messageIDs {
		seen[messageID]++
	}
	for messageID := range want {
		if seen[messageID] != 1 {
			t.Errorf("%s websocket message %d deliveries = %d, want 1", connection, messageID, seen[messageID])
		}
	}
	for messageID, count := range seen {
		if _, exists := want[messageID]; !exists {
			t.Errorf("%s websocket received unexpected message %d (%d times)", connection, messageID, count)
		}
	}
}

// TestProcessFailureRecoversMissedNotificationThroughSync proves the durable
// boundary of multi-instance realtime delivery. Process B is killed without
// unregistering its Presence record, process A publishes while B has no Redis
// subscriber, and the receiver then reconnects to A and recovers the message
// from PostgreSQL through the conversation-scoped Sync API.
func TestProcessFailureRecoversMissedNotificationThroughSync(t *testing.T) {
	if os.Getenv(multiInstanceHelperEnv) != "" {
		t.Skip("parent-only process integration test")
	}
	db := openTestDatabase(t)
	redisClient, namespace := openTestRedis(t)

	processA := startMultiInstanceHelper(t, "a", namespace)
	processB := startMultiInstanceHelper(t, "b", namespace)

	sender := registerTestAccount(t, db, processA.url, uniqueUsername("failure_s"), "Failure Sender")
	receiver := registerTestAccount(t, db, processB.url, uniqueUsername("failure_r"), "Failure Receiver")
	failedConnection := dialAuthenticatedWebSocket(t, processB.url, receiver.Auth.AccessToken)
	defer failedConnection.CloseNow()

	presence := mustTestPresence(t, redisClient, namespace, 2*time.Second, 500*time.Millisecond)
	waitForPresenceInstance(t, presence, receiver.User.ID, "process-b")
	if err := processB.kill(); err != nil {
		t.Fatalf("kill process b: %v\nstderr:\n%s", err, processB.stderr.String())
	}

	created := createMessageThroughAPI(
		t,
		processA.url,
		sender.Auth.AccessToken,
		receiver.User.ID,
		"recover-after-process-failure",
	)
	waitForOutboxPublished(t, db, created.ID)

	reconnected := dialAuthenticatedWebSocket(t, processA.url, receiver.Auth.AccessToken)
	defer reconnected.CloseNow()
	waitForPresenceInstance(t, presence, receiver.User.ID, "process-a")

	deadline := time.Now().Add(2 * time.Second)
	for {
		page := syncConversationMessagesThroughAPI(
			t,
			processA.url,
			receiver.Auth.AccessToken,
			created.ConversationID,
			0,
			10,
		)
		for _, recovered := range page.Messages {
			if recovered.ID == created.ID {
				if recovered.ConversationID != created.ConversationID ||
					recovered.ConversationSeq != created.ConversationSeq {
					t.Fatalf(
						"recovered message cursor = conversation %d seq %d, want %d/%d",
						recovered.ConversationID,
						recovered.ConversationSeq,
						created.ConversationID,
						created.ConversationSeq,
					)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("message %d was not recovered through Sync", created.ID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForOutboxPublished(t *testing.T, db *pgxpool.Pool, messageID int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var published bool
		err := db.QueryRow(
			context.Background(),
			`SELECT published_at IS NOT NULL FROM outbox_events WHERE message_id = $1`,
			messageID,
		).Scan(&published)
		if err == nil && published {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Outbox event for message %d was not published: %v", messageID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMultiInstanceProcessHelper is launched as a child test binary by the
// parent test above. Process A owns the Outbox Worker; process B intentionally
// does not, which makes remote routing deterministic.
func TestMultiInstanceProcessHelper(t *testing.T) {
	if os.Getenv(multiInstanceHelperEnv) == "" {
		return
	}
	role := os.Getenv(multiInstanceRoleEnv)
	if role != "a" && role != "b" {
		t.Fatalf("unsupported helper role %q", role)
	}
	namespace := os.Getenv(multiInstanceNamespaceEnv)
	if namespace == "" {
		t.Fatal("multi-instance helper namespace is required")
	}

	db := openTestDatabase(t)
	redisURL := os.Getenv("TEST_REDIS_URL")
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse helper Redis URL: %v", err)
	}
	redisClient := redis.NewClient(options)
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("connect helper Redis: %v", err)
	}

	app, stopApplication := newRedisBackedWebSocketTestApplication(
		t,
		db,
		redisClient,
		namespace,
		"process-"+role,
	)
	defer stopApplication()

	workerContext, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	if role == "a" {
		config := defaultOutboxWorkerConfig()
		worker := mustMessageTestWorker(t, db, &webSocketOutboxPublisher{
			router:        app.realtimeRouter,
			batchPresence: true,
		}, config)
		go func() {
			defer close(workerDone)
			_ = worker.Run(workerContext)
		}()
	} else {
		close(workerDone)
	}
	defer func() {
		cancelWorker()
		select {
		case <-workerDone:
		case <-time.After(2 * time.Second):
			t.Error("helper Outbox Worker did not stop")
		}
	}()

	server := httptest.NewServer(app.routes())
	defer server.Close()
	fmt.Printf("%s%s\n", multiInstanceReadyLinePrefix, server.URL)

	shutdownContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	<-shutdownContext.Done()
}

type multiInstanceHelper struct {
	url       string
	command   *exec.Cmd
	wait      <-chan error
	stderr    *bytes.Buffer
	stopOnce  sync.Once
	stopError error
}

func startMultiInstanceHelper(t *testing.T, role, namespace string) *multiInstanceHelper {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestMultiInstanceProcessHelper$", "-test.v")
	command.Env = append(
		os.Environ(),
		multiInstanceHelperEnv+"=1",
		multiInstanceRoleEnv+"="+role,
		multiInstanceNamespaceEnv+"="+namespace,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("create process %s stdout pipe: %v", role, err)
	}
	stderr := &bytes.Buffer{}
	stderrPipe, err := command.StderrPipe()
	if err != nil {
		t.Fatalf("create process %s stderr pipe: %v", role, err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start process %s: %v", role, err)
	}

	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	go func() { _, _ = io.Copy(stderr, stderrPipe) }()
	ready := make(chan string, 1)
	go scanMultiInstanceHelperReady(stdout, ready)

	helper := &multiInstanceHelper{
		command: command,
		wait:    wait,
		stderr:  stderr,
	}
	t.Cleanup(func() {
		if err := helper.stop(); err != nil {
			t.Errorf("stop process %s: %v\nstderr:\n%s", role, err, stderr.String())
		}
	})

	select {
	case helper.url = <-ready:
		if helper.url == "" {
			t.Fatalf("process %s closed stdout before readiness\nstderr:\n%s", role, stderr.String())
		}
	case err := <-wait:
		t.Fatalf("process %s exited before readiness: %v\nstderr:\n%s", role, err, stderr.String())
	case <-time.After(5 * time.Second):
		t.Fatalf("process %s did not become ready\nstderr:\n%s", role, stderr.String())
	}
	return helper
}

func scanMultiInstanceHelperReady(output io.Reader, ready chan<- string) {
	defer close(ready)
	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, multiInstanceReadyLinePrefix) {
			ready <- strings.TrimPrefix(line, multiInstanceReadyLinePrefix)
			return
		}
	}
}

func (helper *multiInstanceHelper) stop() error {
	helper.stopOnce.Do(func() {
		if helper.command.Process == nil {
			return
		}
		if err := helper.command.Process.Signal(os.Interrupt); err != nil {
			helper.stopError = err
			return
		}
		select {
		case err := <-helper.wait:
			helper.stopError = err
		case <-time.After(3 * time.Second):
			if err := helper.command.Process.Kill(); err != nil {
				helper.stopError = err
				return
			}
			helper.stopError = <-helper.wait
		}
	})
	return helper.stopError
}

func (helper *multiInstanceHelper) kill() error {
	helper.stopOnce.Do(func() {
		if helper.command.Process == nil {
			return
		}
		if err := helper.command.Process.Kill(); err != nil {
			helper.stopError = err
			return
		}
		helper.stopError = <-helper.wait
		if helper.stopError != nil && strings.Contains(helper.stopError.Error(), "signal: killed") {
			helper.stopError = nil
		}
	})
	return helper.stopError
}
