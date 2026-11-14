package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s-tunnel/internal/server"
)

// fixedLister reports one fixed set of Services.
type fixedLister []server.ServiceInfo

func (f fixedLister) Services(context.Context, []string) ([]server.ServiceInfo, error) {
	return f, nil
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestHostsModeEndToEnd runs the real command with --hosts against a real
// server and checks that a cluster Service name ends up in the hosts file
// pointing at an address that reaches that Service through the tunnel.
func TestHostsModeEndToEnd(t *testing.T) {
	if ln, err := net.Listen("tcp", "127.77.0.1:0"); err != nil {
		t.Skipf("this machine cannot bind addresses in 127.0.0.0/8 other than 127.0.0.1: %v", err)
	} else {
		ln.Close()
	}
	const port = 39480

	// The "cluster": whatever address the server is asked to dial, it
	// reaches this echo service, and the address asked for is recorded.
	clusterAddr, _ := echo(t, "cart-in-cluster")
	var mu sync.Mutex
	var dialled []string
	clusterDial := func(ctx context.Context, network, address string) (net.Conn, error) {
		mu.Lock()
		dialled = append(dialled, address)
		mu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, network, clusterAddr)
	}

	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sctx, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	go func() {
		server.New(server.Config{
			Replica: "local",
			Dial:    clusterDial,
			Lister: fixedLister{
				{Namespace: "shop", Name: "cart", Ports: []int{port}},
				{Namespace: "data", Name: "postgres", Ports: []int{port}},
			},
		}).Serve(sctx, serverLn)
		close(serverDone)
	}()
	defer func() { stopServer(); <-serverDone }()

	hostsPath := filepath.Join(t.TempDir(), "hosts")
	original := "# test hosts file\n127.0.0.1 localhost\n"
	if err := os.WriteFile(hostsPath, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, []string{"connect", "--foreground",
			"--server", serverLn.Addr().String(),
			"--user", "ash", "--session", "hosts-e2e",
			"--hosts", "--hosts-file", hostsPath, "--home-namespace", "shop",
		}, stdout, stderr)
	}()

	line := regexp.MustCompile(`(?m)^(127\.77\.\d+\.\d+) cart\.shop cart\.shop\.svc cart\.shop\.svc\.cluster\.local cart # k8s-tunnel$`)
	var ip string
	deadline := time.Now().Add(15 * time.Second)
	for ip == "" {
		if m := line.FindStringSubmatch(readFile(t, hostsPath)); m != nil {
			ip = m[1]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hosts entry never appeared.\nhosts:\n%s\nstdout:\n%s\nstderr:\n%s",
				readFile(t, hostsPath), stdout.String(), stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	hostsNow := readFile(t, hostsPath)
	if !strings.HasPrefix(hostsNow, original) {
		t.Errorf("existing hosts content was changed:\n%s", hostsNow)
	}
	// Only the home namespace gets bare names.
	if !regexp.MustCompile(`(?m)^127\.77\.\d+\.\d+ postgres\.data postgres\.data\.svc postgres\.data\.svc\.cluster\.local # k8s-tunnel$`).MatchString(hostsNow) {
		t.Errorf("postgres entry missing or wrong:\n%s", hostsNow)
	}

	// What an application does after resolving the name: connect to that
	// address on the Service's own port.
	got, err := ask(net.JoinHostPort(ip, "39480"), "hello")
	if err != nil || got != "cart-in-cluster:hello" {
		t.Errorf("got %q, err %v", got, err)
	}
	mu.Lock()
	asked := append([]string(nil), dialled...)
	mu.Unlock()
	if len(asked) != 1 || asked[0] != "cart.shop.svc:39480" {
		t.Errorf("server was asked to dial %v, want [cart.shop.svc:39480]", asked)
	}

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("command returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("command did not exit")
	}
	if after := readFile(t, hostsPath); after != original {
		t.Errorf("hosts file not restored on exit:\n%q", after)
	}
	if _, err := ask(net.JoinHostPort(ip, "39480"), "x"); err == nil {
		t.Error("service address still answering after exit")
	}
	if !strings.Contains(stdout.String(), "resolving 2 cluster services by name") {
		t.Errorf("stdout:\n%s", stdout.String())
	}
}

func TestHostsModeFailsEarlyWhenHostsFileCannotBeChanged(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "hosts")
	var stdout, stderr bytes.Buffer
	// The server address is unreachable on purpose: the command must give
	// up over the hosts file before it tries to connect.
	err := run(context.Background(), []string{"connect",
		"--server", "127.0.0.1:1", "--user", "ash",
		"--hosts", "--hosts-file", missing,
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), missing) {
		t.Errorf("err = %v, want an error naming the hosts file", err)
	}
}

func TestCleanupRemovesLeftoverEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	original := "127.0.0.1 localhost\n"
	leftover := original +
		"127.77.0.1 cart.shop cart.shop.svc # k8s-tunnel\n" +
		"127.77.0.2 postgres.data.svc # k8s-tunnel\n"
	if err := os.WriteFile(path, []byte(leftover), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"cleanup", "--hosts-file", path}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != original {
		t.Errorf("after cleanup: %q, want %q", got, original)
	}
	if !strings.Contains(stdout.String(), path) {
		t.Errorf("stdout = %q", stdout.String())
	}

	if err := run(context.Background(), []string{"cleanup", "--hosts-file", filepath.Join(t.TempDir(), "nope")}, &stdout, &stderr); err == nil {
		t.Error("cleanup of a missing file reported success")
	}
}
