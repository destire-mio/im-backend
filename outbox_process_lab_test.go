//go:build unix

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Exercises the existing application's claim/complete SQL, not a copied model.
// The shell runner creates a disposable migrated database for this test.
func TestOutboxRealProcessExpiredOwner(t *testing.T) {
	if os.Getenv("RUN_RELIABILITY_LAB") != "1" {
		t.Skip("run scripts/reliability-lab.sh")
	}
	db := openTestDatabase(t)
	ctx := context.Background()
	eventID := createPendingOutboxEvent(t, db)
	eventType := "test.process." + eventID
	if _, err := db.Exec(ctx, `UPDATE outbox_events SET event_type=$2 WHERE event_id=$1`, eventID, eventType); err != nil {
		t.Fatal(err)
	}
	a := startOutboxLabChild(t, eventType, "A")
	tokenA := a.line("CLAIM ")
	if err := a.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	expired := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.QueryRow(ctx, `SELECT locked_until<=clock_timestamp() FROM outbox_events WHERE event_id=$1`, eventID).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !expired {
		t.Fatal("actual lease did not expire")
	}
	b := startOutboxLabChild(t, eventType, "B")
	tokenB := b.line("CLAIM ")
	if tokenA == tokenB {
		t.Fatal("reclaim reused token")
	}
	// B holds the newer claim but has not completed: rejection cannot be
	// explained merely by a pre-existing published_at value.
	if err := a.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	a.continueWork()
	a.line("REJECTED all stale writes")
	a.wait()
	var current string
	var published bool
	if err := db.QueryRow(ctx, `SELECT lock_token::text,published_at IS NOT NULL FROM outbox_events WHERE event_id=$1`, eventID).Scan(&current, &published); err != nil {
		t.Fatal(err)
	}
	if "CLAIM "+current != tokenB || published {
		t.Fatal("old owner changed B's state")
	}
	b.continueWork()
	b.line("PUBLISHED")
	b.wait()
	var attempts int
	if err := db.QueryRow(ctx, `SELECT attempt_count,published_at IS NOT NULL FROM outbox_events WHERE event_id=$1`, eventID).Scan(&attempts, &published); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || !published {
		t.Fatalf("attempts=%d published=%v", attempts, published)
	}
	t.Log("real app Worker A claim -> SIGSTOP -> lease expires -> Worker B claim; A SIGCONT: single/batch publish, retry and dead-state writes all rejected; B completes")
}

func TestOutboxLabProcessHelper(t *testing.T) {
	if os.Getenv("OUTBOX_LAB_CHILD") != "1" {
		t.Skip("subprocess entry point")
	}
	db := openTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	config := testWorkerConfig()
	config.EventTypes = []string{os.Getenv("OUTBOX_LAB_EVENT_TYPE")}
	config.BatchSize = 1
	config.LeaseDuration = 600 * time.Millisecond
	config.AttemptTimeout = 100 * time.Millisecond
	worker := mustTestWorker(t, db, &testPublisher{}, config)
	claimed, err := worker.claim(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	event := claimed[0]
	fmt.Println("CLAIM " + event.LockToken)
	var line string
	if _, err = fmt.Scanln(&line); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("OUTBOX_LAB_OWNER") == "A" {
		results := []error{worker.markPublished(ctx, event), worker.markPublishedBatch(ctx, []outboxEvent{event}),
			worker.markFailed(ctx, event, errors.New("retryable")), worker.markFailed(ctx, event, permanentPublishError(errors.New("permanent")))}
		for _, err := range results {
			if !errors.Is(err, errOutboxLeaseLost) {
				t.Fatalf("old owner write=%v", err)
			}
		}
		fmt.Println("REJECTED all stale writes")
	} else {
		if err = worker.markPublished(ctx, event); err != nil {
			t.Fatal(err)
		}
		fmt.Println("PUBLISHED")
	}
}

type outboxLabChild struct {
	cmd      *exec.Cmd
	in       io.WriteCloser
	lines    chan string
	done     chan error
	t        *testing.T
	finished bool
}

func startOutboxLabChild(t *testing.T, eventType, owner string) *outboxLabChild {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &outboxLabChild{t: t, lines: make(chan string, 16), done: make(chan error, 1)}
	p.cmd = exec.Command(exe, "-test.run=^TestOutboxLabProcessHelper$", "-test.v")
	p.cmd.Env = append(os.Environ(), "OUTBOX_LAB_CHILD=1", "OUTBOX_LAB_EVENT_TYPE="+eventType, "OUTBOX_LAB_OWNER="+owner)
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.in, err = p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = os.Stderr
	if err = p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(out)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
		p.done <- p.cmd.Wait()
	}()
	t.Cleanup(func() {
		_ = p.in.Close()
		if !p.finished {
			_ = p.cmd.Process.Kill()
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				t.Error("child cleanup timeout")
			}
		}
	})
	return p
}
func (p *outboxLabChild) line(prefix string) string {
	p.t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				p.t.Fatalf("child exited before %s", prefix)
			}
			if strings.HasPrefix(line, prefix) {
				return line
			}
		case <-timer.C:
			p.t.Fatalf("child timeout before %s", prefix)
		}
	}
}
func (p *outboxLabChild) continueWork() {
	if _, err := io.WriteString(p.in, "continue\n"); err != nil {
		p.t.Fatal(err)
	}
}
func (p *outboxLabChild) wait() {
	select {
	case err := <-p.done:
		p.finished = true
		if err != nil {
			p.t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		p.t.Fatal("child exit timeout")
	}
}
