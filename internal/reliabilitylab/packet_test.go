//go:build unix

package reliabilitylab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestPacketConcurrentClaims(t *testing.T) {
	db, _ := labDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := &PacketStore{DB: db}
	must(t, store.Create(ctx, "burst", 10003, 100))
	users := make(map[string]string, 200)
	for i := range 200 {
		users[fmt.Sprintf("Bearer fixture-%d", i)] = fmt.Sprintf("user-%d", i)
	}
	server := httptest.NewServer(PacketHandler(store, func(r *http.Request) (string, bool) {
		user, ok := users[r.Header.Get("Authorization")]
		return user, ok
	}))
	defer server.Close()
	client := &http.Client{Timeout: 30 * time.Second}
	defer client.CloseIdleConnections()
	start := make(chan struct{})
	results := make(chan struct {
		r   Receipt
		err error
	}, 400)
	var wg sync.WaitGroup
	for i := range 400 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r, err := claimHTTP(client, server.URL, "burst", fmt.Sprintf("Bearer fixture-%d", i%200))
			results <- struct {
				r   Receipt
				err error
			}{r, err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	unique := map[string]Receipt{}
	success := 0
	exhausted := 0
	for result := range results {
		if errors.Is(result.err, ErrExhausted) {
			exhausted++
			continue
		}
		must(t, result.err)
		success++
		if previous, ok := unique[result.r.User]; ok && previous != result.r {
			t.Fatal("retry changed receipt")
		}
		unique[result.r.User] = result.r
	}
	if len(unique) != 100 || success != 200 || exhausted != 200 {
		t.Fatalf("unique=%d success=%d exhausted=%d", len(unique), success, exhausted)
	}
	assertPacket(t, db, "burst", 100, 10003, 100)
	var events int
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM credit_outbox`).Scan(&events))
	if events != 100 {
		t.Fatalf("events=%d", events)
	}
	t.Log("400 concurrent HTTP calls / 200 authenticated fixture users / 100 shares: 100 unique receipts, 200 successful original+retry responses, 200 exhausted; SQL sum=10003; outbox=100")
}

func assertPacket(t *testing.T, db *pgxpool.Pool, id string, claims int, sum int64, slots int) {
	t.Helper()
	var actualClaims, actualSlots int
	var actualSum int64
	must(t, db.QueryRow(context.Background(), `SELECT count(*),COALESCE(sum(amount),0) FROM packet_credits WHERE packet_id=$1`, id).Scan(&actualClaims, &actualSum))
	must(t, db.QueryRow(context.Background(), `SELECT count(*) FROM packet_slots WHERE packet_id=$1`, id).Scan(&actualSlots))
	if actualClaims != claims || actualSum != sum || actualSlots != slots {
		t.Fatalf("packet %s: claims=%d sum=%d slots=%d", id, actualClaims, actualSum, actualSlots)
	}
}

func TestPacketSQLFailureAndConstraints(t *testing.T) {
	db, _ := labDB(t)
	ctx := context.Background()
	store := &PacketStore{DB: db}
	must(t, store.Create(ctx, "rollback", 99, 3))
	_, err := db.Exec(ctx, `CREATE FUNCTION fail_credit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected ledger failure'; END $$;
        CREATE TRIGGER fail_credit BEFORE INSERT ON packet_credits FOR EACH ROW EXECUTE FUNCTION fail_credit()`)
	must(t, err)
	if _, err = store.Claim(ctx, "rollback", "u"); err == nil {
		t.Fatal("expected SQL error")
	}
	assertPacket(t, db, "rollback", 0, 0, 3)
	var claimed, events int
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM packet_slots WHERE claimed_by IS NOT NULL`).Scan(&claimed))
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM credit_outbox`).Scan(&events))
	if claimed != 0 || events != 0 {
		t.Fatal("partial claim survived rollback")
	}
	_, err = db.Exec(ctx, `DROP TRIGGER fail_credit ON packet_credits`)
	must(t, err)
	_, err = store.Claim(ctx, "rollback", "u")
	must(t, err)
	assertPacket(t, db, "rollback", 1, 33, 3)
	// Database, not only Go validation, rejects an underfunded allocation.
	tx, err := db.Begin(ctx)
	must(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO packets VALUES ('invalid',100,2); INSERT INTO packet_slots(packet_id,slot,amount) VALUES ('invalid',0,10),('invalid',1,10)`)
	must(t, err)
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("SQL accepted wrong preallocation sum")
	}
	_, err = db.Exec(ctx, `UPDATE packet_slots SET claimed_by='u' WHERE packet_id='rollback' AND slot=1`)
	if err == nil {
		t.Fatal("SQL accepted duplicate recipient")
	}
	t.Log("real ledger trigger exception rolled back slot+credit+outbox; retry succeeds; deferred sum constraint and unique recipient rejected invalid SQL")
}

func TestPacketProcessCrashRecovery(t *testing.T) {
	db, schema := labDB(t)
	ctx := context.Background()
	store := &PacketStore{DB: db}
	for _, point := range []string{"after_slot", "before_commit", "after_commit", "after_delivery"} {
		t.Run(point, func(t *testing.T) {
			must(t, store.Create(ctx, point, 81, 1))
			p := child(t, schema, "LAB_MODE=packet", "LAB_PACKET="+point, "LAB_POINT="+point)
			p.line("CHECKPOINT " + point)
			p.kill()
			var beforeCredits, beforeReceived, beforePending int
			must(t, db.QueryRow(ctx, `SELECT count(*) FROM packet_credits WHERE packet_id=$1`, point).Scan(&beforeCredits))
			must(t, db.QueryRow(ctx, `SELECT count(*) FROM received_credits WHERE packet_id=$1`, point).Scan(&beforeReceived))
			must(t, db.QueryRow(ctx, `SELECT count(*) FROM credit_outbox WHERE packet_id=$1`, point).Scan(&beforePending))
			committed, delivered := 0, 0
			if point == "after_commit" || point == "after_delivery" {
				committed = 1
			}
			if point == "after_delivery" {
				delivered = 1
			}
			if beforeCredits != committed || beforePending != committed || beforeReceived != delivered {
				t.Fatalf("at crash credits=%d pending=%d received=%d", beforeCredits, beforePending, beforeReceived)
			}
			// New OS process with a new pool resolves the ambiguous result from SQL.
			retry := child(t, schema, "LAB_MODE=packet", "LAB_PACKET="+point)
			line := retry.line("RECEIPT ")
			retry.wait()
			var r Receipt
			must(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "RECEIPT ")), &r))
			if r.Amount != 81 || r.Slot != 0 || r.User != "same-user" {
				t.Fatalf("receipt=%+v", r)
			}
			assertPacket(t, db, point, 1, 81, 1)
			var received, pending int
			must(t, db.QueryRow(ctx, `SELECT count(*) FROM received_credits WHERE packet_id=$1`, point).Scan(&received))
			must(t, db.QueryRow(ctx, `SELECT count(*) FROM credit_outbox WHERE packet_id=$1`, point).Scan(&pending))
			if received != 1 || pending != 0 {
				t.Fatalf("received=%d pending=%d", received, pending)
			}
			t.Logf("SIGKILL at %s; restarted process returns original receipt; one SQL credit + one downstream effect; outbox drained", point)
		})
	}
}

// TestLabProcessHelper is executed as an independent OS process by the harness.
// CHECKPOINT is written only after reaching the actual DB/lease boundary.
func TestLabProcessHelper(t *testing.T) {
	if os.Getenv("LAB_CHILD") != "1" {
		t.Skip("subprocess entry point")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := childDB(t)
	switch os.Getenv("LAB_MODE") {
	case "packet":
		store := &PacketStore{DB: db, checkpoint: func(point string) {
			if point == os.Getenv("LAB_POINT") {
				fmt.Println("CHECKPOINT " + point)
				var line string
				_, _ = fmt.Scanln(&line)
			}
		}}
		r, err := store.Claim(ctx, os.Getenv("LAB_PACKET"), "same-user")
		must(t, err)
		must(t, store.Deliver(ctx, r.Packet, r.User))
		data, err := json.Marshal(r)
		must(t, err)
		fmt.Println("RECEIPT " + string(data))
	case "lock":
		runLockChild(t, ctx, db)
	case "packet_http":
		store := &PacketStore{DB: db, checkpoint: func(point string) {
			if point == os.Getenv("LAB_POINT") {
				fmt.Println("CHECKPOINT " + point)
				var line string
				_, _ = fmt.Scanln(&line)
			}
		}}
		server := httptest.NewServer(PacketHandler(store, func(r *http.Request) (string, bool) {
			return "http-user", r.Header.Get("Authorization") == "Bearer fixture"
		}))
		defer server.Close()
		fmt.Println("URL " + server.URL)
		<-ctx.Done()
	case "cache_writer":
		c := &Cache{DB: db}
		must(t, c.Update(ctx, "crash-cache", "new"))
		fmt.Println("CHECKPOINT sql_committed")
		var line string
		_, _ = fmt.Scanln(&line)
	default:
		t.Fatal("unknown child mode")
	}
}

func claimHTTP(client *http.Client, base, packet, authorization string) (Receipt, error) {
	var receipt Receipt
	req, err := http.NewRequest(http.MethodPost, base+"/packets/"+packet+"/claims", nil)
	if err != nil {
		return receipt, err
	}
	req.Header.Set("Authorization", authorization)
	response, err := client.Do(req)
	if err != nil {
		return receipt, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return receipt, ErrExhausted
	}
	if response.StatusCode != http.StatusOK {
		return receipt, fmt.Errorf("claim HTTP status %d", response.StatusCode)
	}
	err = json.NewDecoder(response.Body).Decode(&receipt)
	return receipt, err
}

func TestPacketHTTPResponseLostAfterCommit(t *testing.T) {
	db, schema := labDB(t)
	store := &PacketStore{DB: db}
	ctx := context.Background()
	must(t, store.Create(ctx, "http-loss", 73, 1))
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	p := child(t, schema, "LAB_MODE=packet_http", "LAB_POINT=after_commit")
	url := strings.TrimPrefix(p.line("URL "), "URL ")
	lost := make(chan error, 1)
	go func() { _, err := claimHTTP(client, url, "http-loss", "Bearer fixture"); lost <- err }()
	p.line("CHECKPOINT after_commit")
	assertPacket(t, db, "http-loss", 1, 73, 1)
	p.kill()
	if err := <-lost; err == nil {
		t.Fatal("expected dropped HTTP response")
	}
	restarted := child(t, schema, "LAB_MODE=packet_http")
	newURL := strings.TrimPrefix(restarted.line("URL "), "URL ")
	first, err := claimHTTP(client, newURL, "http-loss", "Bearer fixture")
	must(t, err)
	second, err := claimHTTP(client, newURL, "http-loss", "Bearer fixture")
	must(t, err)
	if first != second || first.Amount != 73 || first.User != "http-user" {
		t.Fatalf("retry receipts: %+v %+v", first, second)
	}
	assertPacket(t, db, "http-loss", 1, 73, 1)
	// A real network call without fixture authentication never reaches Claim.
	if _, err = claimHTTP(client, newURL, "http-loss", ""); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("unauthenticated call=%v", err)
	}
	restarted.kill()
	t.Log("real HTTP request -> SQL COMMIT -> SIGKILL before response; client gets transport error; new server process returns identical durable receipt on retries")
}

func runLockChild(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	name := os.Getenv("LAB_RESOURCE")
	owner := os.Getenv("LAB_OWNER")
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("LAB_REDIS_ADDR")})
	defer client.Close()
	ttl := 10 * time.Second
	if owner == "A" {
		ttl = 600 * time.Millisecond
	}
	ok, err := client.SetNX(ctx, name, owner, ttl).Result()
	must(t, err)
	if !ok {
		t.Fatal("lease not acquired")
	}
	var token int64
	if os.Getenv("LAB_FENCED") == "1" {
		token, err = AcquireFence(ctx, db, name, ttl)
		must(t, err)
	}
	fmt.Printf("ACQUIRED %s %d\n", owner, token)
	if owner == "A" {
		// Parent sends SIGSTOP after ACQUIRED; stdin also gates us, removing races.
		var line string
		_, err = fmt.Scanln(&line)
		must(t, err)
	}
	if os.Getenv("LAB_FENCED") == "1" {
		err = WriteFenced(ctx, db, name, token, owner)
		if owner == "A" {
			if !errors.Is(err, ErrStaleOwner) {
				t.Fatalf("old owner err=%v", err)
			}
			fmt.Println("WRITE rejected")
		} else {
			must(t, err)
			fmt.Println("WRITE accepted")
		}
	} else {
		_, err = db.Exec(ctx, `UPDATE resources SET value=$2 WHERE name=$1`, name, owner)
		must(t, err)
		fmt.Println("WRITE accepted")
	}
	if owner == "A" {
		// Compare-and-delete protects B's Redis key, but cannot undo A's stale write.
		deleted, err := client.Eval(ctx, `if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end`, []string{name}, owner).Int()
		must(t, err)
		if deleted != 0 {
			t.Fatal("old owner deleted new owner's lock")
		}
	}
}

func TestLeaseExpiredOwnerRevives(t *testing.T) {
	db, schema := labDB(t)
	r := newRedis(t)
	ctx := context.Background()
	for _, fenced := range []bool{false, true} {
		t.Run(fmt.Sprint("fenced_", fenced), func(t *testing.T) {
			name := fmt.Sprint("resource-", fenced)
			_, err := db.Exec(ctx, `INSERT INTO resources(name,value) VALUES ($1,'initial')`, name)
			must(t, err)
			env := []string{"LAB_MODE=lock", "LAB_RESOURCE=" + name, "LAB_REDIS_ADDR=" + r.addr()}
			if fenced {
				env = append(env, "LAB_FENCED=1")
			}
			a := child(t, schema, append(env, "LAB_OWNER=A")...)
			a.line("ACQUIRED A ")
			must(t, a.cmd.Process.Signal(syscall.SIGSTOP))
			eventually(t, "A Redis lease expiration", 5*time.Second, func() bool { return r.client.Exists(ctx, name).Val() == 0 })
			if fenced {
				eventually(t, "SQL lease expiration", 5*time.Second, func() bool {
					var expired bool
					err := db.QueryRow(ctx, `SELECT until<=clock_timestamp() FROM resource_leases WHERE name=$1`, name).Scan(&expired)
					return err == nil && expired
				})
			}
			b := child(t, schema, append(env, "LAB_OWNER=B")...)
			b.line("ACQUIRED B ")
			b.line("WRITE accepted")
			b.wait()
			a.resume()
			if fenced {
				a.line("WRITE rejected")
			} else {
				a.line("WRITE accepted")
			}
			a.wait()
			var value string
			must(t, db.QueryRow(ctx, `SELECT value FROM resources WHERE name=$1`, name).Scan(&value))
			expected := "A"
			if fenced {
				expected = "B"
			}
			if value != expected {
				t.Fatalf("value=%s expected=%s", value, expected)
			}
			if value, err := r.client.Get(ctx, name).Result(); err != nil || value != "B" {
				t.Fatalf("B Redis lease = %s %v", value, err)
			}
			t.Logf("A SET NX PX -> SIGSTOP -> real TTL expiration -> B acquires/writes -> SIGCONT A; fenced=%v final value=%s, B lock preserved", fenced, value)
		})
	}
}
