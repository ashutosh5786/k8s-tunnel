package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s-tunnel/internal/client/connect"
	"k8s-tunnel/internal/server"
	"k8s-tunnel/pkg/tunnel"
)

func TestParseExposes(t *testing.T) {
	got, err := parseExposes([]string{
		"b-ash:8080:3000/http",
		"db-ash:5432",
		"b-ash:9090/grpc",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []connect.Exposure{
		{Name: "b-ash", Ports: []tunnel.Port{
			{Name: "http-8080", Port: 8080, LocalPort: 3000, AppProtocol: "http"},
			{Name: "grpc-9090", Port: 9090, LocalPort: 9090, AppProtocol: "grpc"},
		}},
		{Name: "db-ash", Ports: []tunnel.Port{
			{Name: "tcp-5432", Port: 5432, LocalPort: 5432},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}

	for _, bad := range []string{
		"b-ash",                 // no port
		":8080",                 // no name
		"b-ash:abc",             // port not a number
		"b-ash:0",               // port out of range
		"b-ash:70000",           // port out of range
		"b-ash:8080:xyz",        // local port not a number
		"b-ash:8080:3000:1",     // too many fields
		"b-ash:8080/",           // empty protocol
		"b-ash:8080:3000:1/tcp", // too many fields with protocol
	} {
		if out, err := parseExposes([]string{bad}); err == nil {
			t.Errorf("parseExposes(%q) accepted as %+v", bad, out)
		} else if !strings.Contains(err.Error(), bad) {
			t.Errorf("error for %q does not quote the input: %v", bad, err)
		}
	}
	if _, err := parseExposes([]string{"b-ash:8080:3000", "b-ash:8080:4000"}); err == nil {
		t.Error("the same service port twice for one name was accepted")
	}
}

func TestParseForwards(t *testing.T) {
	got, err := parseForwards([]string{"5432:postgres.db.svc:5432", "8081:c.ns.svc.cluster.local:8080"})
	if err != nil {
		t.Fatal(err)
	}
	want := []connect.Forward{
		// Forwards bind to loopback only, never to all interfaces.
		{Local: "127.0.0.1:5432", Target: "postgres.db.svc:5432"},
		{Local: "127.0.0.1:8081", Target: "c.ns.svc.cluster.local:8080"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}

	for _, bad := range []string{
		"5432",                 // no target
		"5432:postgres.db.svc", // target without port
		"abc:postgres:5432",    // local port not a number
		"0:postgres:5432",      // local port out of range
		"5432::5432",           // empty host
		"5432:postgres:99999",  // target port out of range
	} {
		if out, err := parseForwards([]string{bad}); err == nil {
			t.Errorf("parseForwards(%q) accepted as %+v", bad, out)
		}
	}
	if _, err := parseForwards([]string{"5432:a.svc:5432", "5432:b.svc:5432"}); err == nil {
		t.Error("the same local port twice was accepted")
	}
}

func TestSessionIDsAreValidAndUnique(t *testing.T) {
	// Must satisfy the server's rule for session ids (a DNS label).
	valid := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	seen := map[string]bool{}
	for range 100 {
		id := newSessionID()
		if !valid.MatchString(id) {
			t.Fatalf("session id %q is not a valid label", id)
		}
		if seen[id] {
			t.Fatalf("session id %q generated twice", id)
		}
		seen[id] = true
	}
}

func TestCommandLineErrors(t *testing.T) {
	isolate(t)
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"no command":      {nil, "no command"},
		"unknown command": {[]string{"teleport"}, "unknown command"},
		"hosts conflict":  {[]string{"connect", "--hosts", "--no-hosts"}, "cannot be used together"},
		"status with arg": {[]string{"status", "now"}, "takes no arguments"},
		"bad expose":      {[]string{"connect", "--server", "127.0.0.1:1", "--expose", "oops"}, "invalid --expose"},
		"bad forward":     {[]string{"connect", "--server", "127.0.0.1:1", "--forward", "oops"}, "invalid --forward"},
		"stray argument":  {[]string{"connect", "--expose", "b:80", "extra"}, "unexpected argument"},
		"unknown flag":    {[]string{"connect", "--nope"}, "not defined"},
		"empty user":      {[]string{"connect", "--user", "", "--expose", "b:80"}, "--user"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), tc.args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want one mentioning %q", name, err, tc.want)
		}
	}

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"help"}, &stdout, &stderr); err != nil {
		t.Errorf("help: %v", err)
	}
	if !strings.Contains(stdout.String(), "ashctl connect") {
		t.Errorf("help output: %q", stdout.String())
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func echo(t *testing.T, prefix string) (addr string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					fmt.Fprintf(c, "%s:%s\n", prefix, sc.Text())
				}
			}()
		}
	}()
	return ln.Addr().String(), ln.Addr().(*net.TCPAddr).Port
}

func ask(addr, msg string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	fmt.Fprintln(conn, msg)
	line, err := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSuffix(line, "\n"), err
}

// TestConnectCommandEndToEnd drives the real command line against a real
// server in direct mode, covering both traffic directions.
func TestConnectCommandEndToEnd(t *testing.T) {
	isolate(t)
	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sctx, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	go func() {
		server.New(server.Config{Replica: "local"}).Serve(sctx, serverLn)
		close(serverDone)
	}()
	defer func() { stopServer(); <-serverDone }()

	clusterAddr, _ := echo(t, "cluster-c")
	_, localPort := echo(t, "local-b")
	free, _ := net.Listen("tcp", "127.0.0.1:0")
	forwardPort := free.Addr().(*net.TCPAddr).Port
	free.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, []string{"connect", "--foreground", "--no-hosts",
			"--server", serverLn.Addr().String(),
			"--user", "ash", "--session", "e2e",
			"--expose", fmt.Sprintf("b-ash:8080:%d/http", localPort),
			"--forward", fmt.Sprintf("%d:%s", forwardPort, clusterAddr),
		}, stdout, stderr)
	}()

	// Without a cluster there is no Service; the status line reports the
	// server port, which is where a Service would send traffic.
	re := regexp.MustCompile(`exposed   server port (\d+) -> localhost:` + strconv.Itoa(localPort))
	var serverPort string
	deadline := time.Now().Add(10 * time.Second)
	for serverPort == "" {
		if m := re.FindStringSubmatch(stdout.String()); m != nil {
			serverPort = m[1]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never exposed.\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got, err := ask(net.JoinHostPort("127.0.0.1", strconv.Itoa(forwardPort)), "out"); err != nil || got != "cluster-c:out" {
		t.Errorf("outbound got %q, err %v", got, err)
	}
	if got, err := ask(net.JoinHostPort("127.0.0.1", serverPort), "in"); err != nil || got != "local-b:in" {
		t.Errorf("inbound got %q, err %v", got, err)
	}

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("command returned %v on interrupt", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("command did not exit on interrupt")
	}
	if !strings.Contains(stdout.String(), "connected to local as ash (session e2e)") {
		t.Errorf("stdout:\n%s", stdout.String())
	}
}
