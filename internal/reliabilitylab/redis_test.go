//go:build unix

package reliabilitylab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCacheSQLStaleRefillAfterBothDeletes(t *testing.T) {
	db, _ := labDB(t)
	r := newRedis(t)
	ctx := context.Background()
	c := &Cache{DB: db, Redis: r.client, TTL: 400 * time.Millisecond}
	must(t, c.Update(ctx, "race", "old"))
	must(t, c.Invalidate(ctx, "race"))
	loaded := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	finished := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
		<-finished
	}()
	old := *c
	old.afterLoad = func() { close(loaded); <-release }
	go func() {
		defer close(finished)
		value, err := old.Read(ctx, "race")
		if err == nil && value != "old" {
			err = fmt.Errorf("old reader value=%s", value)
		}
		done <- err
	}()
	<-loaded
	must(t, c.Update(ctx, "race", "new"))
	must(t, c.Invalidate(ctx, "race"))
	// Even a successful second delete cannot cover a reader paused longer than it.
	must(t, r.client.Del(ctx, "race").Err())
	close(release)
	released = true
	err := <-done
	must(t, err)
	value, err := r.client.Get(ctx, "race").Result()
	must(t, err)
	if value != "old" {
		t.Fatalf("stale refill=%s", value)
	}
	eventually(t, "stale cache TTL expiration", 3*time.Second, func() bool { return r.client.Exists(ctx, "race").Val() == 0 })
	value, err = c.Read(ctx, "race")
	must(t, err)
	if value != "new" {
		t.Fatal("SQL did not repair stale cache")
	}
	t.Log("real SQL old read -> SQL commit new -> DEL -> second DEL -> delayed old refill; stale value reproduced; TTL expires and SQL read repairs it")
}

func TestCacheInvalidationSurvivesProcessAndRedisFailure(t *testing.T) {
	db, schema := labDB(t)
	r := newRedis(t, "--appendonly", "yes", "--appendfsync", "always")
	ctx := context.Background()
	c := &Cache{DB: db, Redis: r.client, TTL: time.Minute}
	must(t, c.Update(ctx, "crash-cache", "old"))
	must(t, c.Invalidate(ctx, "crash-cache"))
	value, err := c.Read(ctx, "crash-cache")
	must(t, err)
	if value != "old" {
		t.Fatal(value)
	}
	writer := child(t, schema, "LAB_MODE=cache_writer")
	writer.line("CHECKPOINT sql_committed")
	writer.kill()
	r.kill()
	if err = c.Invalidate(ctx, "crash-cache"); err == nil {
		t.Fatal("DEL unexpectedly succeeded with Redis killed")
	}
	var pending int
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM cache_invalidations WHERE key='crash-cache'`).Scan(&pending))
	if pending != 1 {
		t.Fatal("invalidation lost")
	}
	value, err = c.Read(ctx, "crash-cache")
	must(t, err)
	if value != "new" {
		t.Fatal("Redis failure did not fall back to SQL")
	}
	r.start()
	value, err = r.client.Get(ctx, "crash-cache").Result()
	must(t, err)
	if value != "old" {
		t.Fatal("expected restored stale cache")
	}
	restarted := &Cache{DB: db, Redis: r.client, TTL: time.Minute}
	must(t, restarted.Invalidate(ctx, "crash-cache"))
	value, err = restarted.Read(ctx, "crash-cache")
	must(t, err)
	if value != "new" {
		t.Fatal(value)
	}
	must(t, db.QueryRow(ctx, `SELECT count(*) FROM cache_invalidations`).Scan(&pending))
	if pending != 0 {
		t.Fatal("intent not acknowledged")
	}
	t.Log("writer SIGKILL after SQL value+invalidation commit; Redis SIGKILL causes DEL error, intent retained and SQL fallback succeeds; AOF restores stale cache; retry DEL+ACK+refill repairs it")
}

func infoInt(t *testing.T, c *redis.Client, section, key string) int64 {
	t.Helper()
	s, err := c.Info(context.Background(), section).Result()
	must(t, err)
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, key+":") {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, key+":")), 10, 64)
			must(t, err)
			return n
		}
	}
	t.Fatalf("missing INFO %s", key)
	return 0
}

func TestRedisExpirationAndEviction(t *testing.T) {
	ctx := context.Background()
	t.Run("expiration", func(t *testing.T) {
		r := newRedis(t)
		must(t, r.client.Set(ctx, "ttl", "value", 150*time.Millisecond).Err())
		ttl, err := r.client.PTTL(ctx, "ttl").Result()
		must(t, err)
		if ttl <= 0 {
			t.Fatal("missing TTL")
		}
		eventually(t, "key expires", 3*time.Second, func() bool { return errors.Is(r.client.Get(ctx, "ttl").Err(), redis.Nil) })
		if infoInt(t, r.client, "stats", "expired_keys") < 1 {
			t.Fatal("expiration counter missing")
		}
		t.Log("PTTL positive -> GET nil -> expired_keys increases")
	})
	for _, policy := range []string{"noeviction", "allkeys-lru", "volatile-lru"} {
		t.Run(policy, func(t *testing.T) {
			r := newRedis(t)
			must(t, r.client.Set(ctx, "persistent", "keep", 0).Err())
			baseline := infoInt(t, r.client, "memory", "used_memory")
			must(t, r.client.ConfigSet(ctx, "maxmemory-policy", policy).Err())
			must(t, r.client.ConfigSet(ctx, "maxmemory", strconv.FormatInt(baseline+256*1024, 10)).Err())
			oom := false
			for i := range 120 {
				ttl := time.Duration(0)
				if policy == "volatile-lru" {
					ttl = time.Hour
				}
				err := r.client.Set(ctx, fmt.Sprintf("item:%d", i), strings.Repeat("x", 16384), ttl).Err()
				if err != nil {
					if !strings.Contains(err.Error(), "OOM") {
						t.Fatal(err)
					}
					oom = true
					break
				}
			}
			evicted := infoInt(t, r.client, "stats", "evicted_keys")
			if policy == "noeviction" {
				if !oom || evicted != 0 {
					t.Fatalf("oom=%v evicted=%d", oom, evicted)
				}
			} else if oom || evicted == 0 {
				t.Fatalf("oom=%v evicted=%d", oom, evicted)
			}
			if policy == "volatile-lru" {
				if value, err := r.client.Get(ctx, "persistent").Result(); err != nil || value != "keep" {
					t.Fatal("volatile policy evicted persistent key")
				}
			}
			t.Logf("policy=%s OOM=%v evicted_keys=%d", policy, oom, evicted)
		})
	}
}

func TestRedisPersistenceAfterSIGKILL(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"rdb", "aof-always"} {
		t.Run(mode, func(t *testing.T) {
			extra := []string{}
			if mode == "aof-always" {
				extra = []string{"--appendonly", "yes", "--appendfsync", "always"}
			}
			r := newRedis(t, extra...)
			must(t, r.client.Set(ctx, "before", "saved", 0).Err())
			must(t, r.client.Set(ctx, "expired-during-down", "temporary", 500*time.Millisecond).Err())
			if mode == "rdb" {
				must(t, r.client.Save(ctx).Err())
			}
			must(t, r.client.Set(ctx, "after", "acknowledged", 0).Err())
			r.kill()
			time.Sleep(550 * time.Millisecond)
			r.start()
			value, err := r.client.Get(ctx, "before").Result()
			must(t, err)
			if value != "saved" {
				t.Fatal(value)
			}
			value, err = r.client.Get(ctx, "after").Result()
			if mode == "rdb" {
				if !errors.Is(err, redis.Nil) {
					t.Fatalf("post-snapshot write survived: %s %v", value, err)
				}
			} else {
				must(t, err)
				if value != "acknowledged" {
					t.Fatal(value)
				}
			}
			if !errors.Is(r.client.Get(ctx, "expired-during-down").Err(), redis.Nil) {
				t.Fatal("expired key resurrected")
			}
			t.Logf("%s SIGKILL/restart: pre-checkpoint recovered; post-checkpoint recovered=%v; downtime-expired key absent", mode, mode == "aof-always")
		})
	}
}

type sentinelProcess struct {
	addr   string
	client *redis.SentinelClient
}

func newSentinel(t *testing.T, primary *redisProcess) sentinelProcess {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)
	config := fmt.Sprintf("port %d\nbind 127.0.0.1\ndir %s\nlogfile %s\nsentinel monitor lab-primary 127.0.0.1 %d 2\nsentinel down-after-milliseconds lab-primary 700\nsentinel failover-timeout lab-primary 6000\nsentinel parallel-syncs lab-primary 1\n", port, dir, filepath.Join(dir, "sentinel.log"), primary.port)
	path := filepath.Join(dir, "sentinel.conf")
	must(t, os.WriteFile(path, []byte(config), 0600))
	bin := os.Getenv("LAB_REDIS_SERVER")
	if bin == "" {
		bin = "redis-server"
	}
	cmd := exec.Command(bin, path, "--sentinel")
	must(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	c := redis.NewSentinelClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port), MaxRetries: -1, DialTimeout: 250 * time.Millisecond, ReadTimeout: 300 * time.Millisecond})
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("sentinel exit timeout")
		}
		_ = c.Close()
	})
	eventually(t, "Sentinel ready", 5*time.Second, func() bool { return c.Ping(context.Background()).Err() == nil })
	return sentinelProcess{addr: fmt.Sprintf("127.0.0.1:%d", port), client: c}
}

func TestRedisSentinelFailoverAndAcknowledgedLoss(t *testing.T) {
	primary := newRedis(t, "--repl-diskless-sync-delay", "0")
	replica := newRedis(t, "--replicaof", "127.0.0.1", strconv.Itoa(primary.port), "--repl-diskless-sync-delay", "0")
	ctx := context.Background()
	eventually(t, "replica connected", 15*time.Second, func() bool {
		s, err := replica.client.Info(ctx, "replication").Result()
		return err == nil && strings.Contains(s, "master_link_status:up")
	})
	sentinels := []sentinelProcess{newSentinel(t, primary), newSentinel(t, primary), newSentinel(t, primary)}
	for _, s := range sentinels {
		eventually(t, "Sentinel discovers two peers and one replica", 15*time.Second, func() bool {
			state, err := s.client.Master(ctx, "lab-primary").Result()
			return err == nil && state["num-other-sentinels"] == "2" && state["num-slaves"] == "1"
		})
	}
	addresses := []string{sentinels[0].addr, sentinels[1].addr, sentinels[2].addr}
	failover := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "lab-primary", SentinelAddrs: addresses, MaxRetries: -1, DialTimeout: 250 * time.Millisecond, ReadTimeout: 300 * time.Millisecond})
	defer failover.Close()
	must(t, failover.Ping(ctx).Err())
	// WAIT and SET share one physical connection. This is a barrier for the
	// first key, not a claim that WAIT makes all later writes strongly consistent.
	conn := primary.client.Conn()
	defer conn.Close()
	must(t, conn.Set(ctx, "replicated", "yes", 0).Err())
	copies, err := conn.Wait(ctx, 1, 3*time.Second).Result()
	must(t, err)
	if copies != 1 {
		t.Fatal("replication barrier not reached")
	}
	must(t, replica.cmd.Process.Signal(syscall.SIGSTOP))
	// SIGSTOP alone leaves the kernel receive buffer alive. Cut the actual
	// replication connection while the replica cannot reconnect, before writing.
	must(t, primary.client.Do(ctx, "CLIENT", "KILL", "TYPE", "replica").Err())
	if infoInt(t, primary.client, "replication", "connected_slaves") != 0 {
		t.Fatal("replication link still connected")
	}
	must(t, conn.Set(ctx, "ack-but-not-replicated", "lost", 0).Err())
	primary.kill()
	must(t, replica.cmd.Process.Signal(syscall.SIGCONT))
	eventually(t, "automatic Sentinel promotion", 30*time.Second, func() bool {
		addr, err := sentinels[0].client.GetMasterAddrByName(ctx, "lab-primary").Result()
		if err != nil || len(addr) != 2 || addr[1] != strconv.Itoa(replica.port) {
			return false
		}
		state, err := replica.client.Info(ctx, "replication").Result()
		return err == nil && strings.Contains(state, "role:master")
	})
	eventually(t, "existing failover client rediscovers writable primary", 10*time.Second, func() bool { return failover.Set(ctx, "new-primary-write", "ok", 0).Err() == nil })
	value, err := failover.Get(ctx, "replicated").Result()
	must(t, err)
	if value != "yes" {
		t.Fatal("replicated value lost")
	}
	if err := failover.Get(ctx, "ack-but-not-replicated").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expected lost acknowledged write: %v", err)
	}
	primary.start()
	eventually(t, "old primary rejoins as replica", 30*time.Second, func() bool {
		state, err := primary.client.Info(ctx, "replication").Result()
		return err == nil && strings.Contains(state, "role:slave") && strings.Contains(state, "master_link_status:up") && strings.Contains(state, "master_port:"+strconv.Itoa(replica.port))
	})
	err = primary.client.Set(ctx, "old-primary-write", "reject", 0).Err()
	if err == nil || !strings.Contains(err.Error(), "READONLY") {
		t.Fatalf("old primary write=%v", err)
	}
	value, err = primary.client.Get(ctx, "new-primary-write").Result()
	must(t, err)
	if value != "ok" {
		t.Fatal("old primary did not resync")
	}
	t.Log("three Sentinels / quorum 2: synchronized key survives; replica SIGSTOP + replication socket disconnect + acknowledged write + primary SIGKILL loses unsynced key; automatic promotion and existing client rediscovery pass; old primary rejoins READONLY and resyncs")
}
