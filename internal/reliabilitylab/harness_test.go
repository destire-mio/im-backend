//go:build unix

package reliabilitylab

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func labEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("RUN_RELIABILITY_LAB") != "1" {
		t.Skip("run scripts/reliability-lab.sh for isolated real fault tests")
	}
}

func labDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	labEnabled(t)
	url := os.Getenv("LAB_DATABASE_URL")
	if url == "" {
		t.Fatal("LAB_DATABASE_URL must identify a disposable database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	must(t, err)
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("reliability_%d", time.Now().UnixNano())
	_, err = admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize())
	must(t, err)
	t.Cleanup(func() {
		_, e := admin.Exec(ctx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		if e != nil {
			t.Error(e)
		}
	})
	config, err := pgxpool.ParseConfig(url)
	must(t, err)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 24
	db, err := pgxpool.NewWithConfig(ctx, config)
	must(t, err)
	t.Cleanup(db.Close)
	_, err = db.Exec(ctx, Schema)
	must(t, err)
	return db, schema
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, description string, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", description)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	must(t, l.Close())
	return port
}

type redisProcess struct {
	t      *testing.T
	dir    string
	port   int
	extra  []string
	cmd    *exec.Cmd
	done   chan error
	client *redis.Client
}

func newRedis(t *testing.T, extra ...string) *redisProcess {
	t.Helper()
	labEnabled(t)
	p := &redisProcess{t: t, dir: t.TempDir(), port: freePort(t), extra: extra}
	p.client = redis.NewClient(&redis.Options{Addr: p.addr(), MaxRetries: -1, DialTimeout: 250 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond})
	t.Cleanup(func() { p.kill(); _ = p.client.Close() })
	p.start()
	return p
}
func (p *redisProcess) addr() string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(p.port)) }
func (p *redisProcess) start() {
	p.t.Helper()
	bin := os.Getenv("LAB_REDIS_SERVER")
	if bin == "" {
		bin = "redis-server"
	}
	args := []string{"--bind", "127.0.0.1", "--port", strconv.Itoa(p.port), "--dir", p.dir, "--save", "", "--appendonly", "no", "--logfile", filepath.Join(p.dir, "redis.log")}
	p.cmd = exec.Command(bin, append(args, p.extra...)...)
	must(p.t, p.cmd.Start())
	p.done = make(chan error, 1)
	go func(cmd *exec.Cmd, done chan error) { done <- cmd.Wait() }(p.cmd, p.done)
	eventually(p.t, "Redis ready", 8*time.Second, func() bool {
		select {
		case err := <-p.done:
			data, _ := os.ReadFile(filepath.Join(p.dir, "redis.log"))
			p.done = nil
			p.t.Fatalf("Redis exited: %v %s", err, data)
		default:
		}
		return p.client.Ping(context.Background()).Err() == nil
	})
}
func (p *redisProcess) kill() {
	if p.done == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		p.t.Error("Redis did not exit")
	}
	p.done = nil
}

type childProcess struct {
	cmd     *exec.Cmd
	lines   chan string
	done    chan error
	input   *os.File
	t       *testing.T
	stopped bool
}

func child(t *testing.T, schema string, env ...string) *childProcess {
	t.Helper()
	executable, err := os.Executable()
	must(t, err)
	p := &childProcess{t: t, lines: make(chan string, 16), done: make(chan error, 1)}
	p.cmd = exec.Command(executable, "-test.run=^TestLabProcessHelper$", "-test.v")
	p.cmd.Env = append(os.Environ(), append([]string{"LAB_CHILD=1", "LAB_SCHEMA=" + schema}, env...)...)
	stdout, err := p.cmd.StdoutPipe()
	must(t, err)
	read, write, err := os.Pipe()
	must(t, err)
	p.input = write
	p.cmd.Stdin = read
	p.cmd.Stderr = os.Stderr
	must(t, p.cmd.Start())
	_ = read.Close()
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
		p.done <- p.cmd.Wait()
	}()
	t.Cleanup(func() { p.kill(); _ = write.Close() })
	return p
}
func (p *childProcess) line(prefix string) string {
	p.t.Helper()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				p.t.Fatalf("child exited before %q", prefix)
			}
			if strings.HasPrefix(line, prefix) {
				return line
			}
		case <-timer.C:
			p.t.Fatalf("child timeout before %q", prefix)
		}
	}
}
func (p *childProcess) resume() {
	must(p.t, p.cmd.Process.Signal(syscall.SIGCONT))
	_, err := p.input.WriteString("continue\n")
	must(p.t, err)
}
func (p *childProcess) wait() {
	if p.stopped {
		return
	}
	select {
	case err := <-p.done:
		must(p.t, err)
	case <-time.After(20 * time.Second):
		p.t.Fatal("child exit timeout")
	}
	p.stopped = true
}
func (p *childProcess) kill() {
	if p.stopped {
		return
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		p.t.Error("child kill timeout")
	}
	p.stopped = true
}

func childDB(t *testing.T) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig(os.Getenv("LAB_DATABASE_URL"))
	must(t, err)
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("LAB_SCHEMA")
	db, err := pgxpool.NewWithConfig(context.Background(), config)
	must(t, err)
	t.Cleanup(db.Close)
	return db
}
