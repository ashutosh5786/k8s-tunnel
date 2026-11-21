package egress

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/hosts"
)

// The tests use their own loopback range so they cannot collide with a
// tunnel that is really running on this machine.
const testBase = "127.78.0.0"

// recordingDial sends every connection to one echo server and records the
// cluster address that was asked for.
type recordingDial struct {
	mu     sync.Mutex
	target string
	asked  []string
}

func (r *recordingDial) dial(ctx context.Context, network, address string) (net.Conn, error) {
	r.mu.Lock()
	r.asked = append(r.asked, address)
	r.mu.Unlock()
	return (&net.Dialer{}).DialContext(ctx, network, r.target)
}

func (r *recordingDial) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.asked) == 0 {
		return ""
	}
	return r.asked[len(r.asked)-1]
}

func newProxy(t *testing.T, opts ProxyOptions) *ServiceProxy {
	t.Helper()
	if opts.BaseIP == "" {
		opts.BaseIP = testBase
	}
	p, err := NewServiceProxy(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func requireLoopbackRange(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.78.0.1:0")
	if err != nil {
		t.Skipf("this machine cannot bind addresses in 127.0.0.0/8 other than 127.0.0.1: %v", err)
	}
	ln.Close()
}

func askLine(addr, msg string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintln(conn, msg)
	line, err := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSuffix(line, "\n"), err
}

func entryFor(entries []hosts.Entry, name string) (hosts.Entry, bool) {
	for _, e := range entries {
		for _, n := range e.Names {
			if n == name {
				return e, true
			}
		}
	}
	return hosts.Entry{}, false
}

func TestSyncServesEachServiceOnItsOwnAddress(t *testing.T) {
	requireLoopbackRange(t)
	rec := &recordingDial{target: echoServer(t, "cluster")}
	p := newProxy(t, ProxyOptions{Dial: rec.dial})

	entries := p.Sync([]Service{
		{Namespace: "shop", Name: "cart", Ports: []int{39080, 39090}},
		{Namespace: "data", Name: "postgres", Ports: []int{39080}}, // same port, different service
	})
	if len(entries) != 2 {
		t.Fatalf("entries = %+v", entries)
	}
	cart, _ := entryFor(entries, "cart.shop.svc")
	pg, _ := entryFor(entries, "postgres.data.svc")
	if cart.IP == "" || pg.IP == "" || cart.IP == pg.IP {
		t.Fatalf("addresses: cart %q, postgres %q", cart.IP, pg.IP)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.IP, "127.78.") {
			t.Errorf("address %s is outside the configured range", e.IP)
		}
	}

	// The same port on two addresses reaches two different Services.
	for addr, wantTarget := range map[string]string{
		cart.IP + ":39080": "cart.shop.svc:39080",
		cart.IP + ":39090": "cart.shop.svc:39090",
		pg.IP + ":39080":   "postgres.data.svc:39080",
	} {
		got, err := askLine(addr, "hi")
		if err != nil || got != "cluster:hi" {
			t.Errorf("%s: got %q, err %v", addr, got, err)
		}
		if rec.last() != wantTarget {
			t.Errorf("%s dialled %q, want %q", addr, rec.last(), wantTarget)
		}
	}
}

func TestEntryNames(t *testing.T) {
	p := newProxy(t, ProxyOptions{Dial: (&recordingDial{}).dial, ClusterDomain: "corp.internal", HomeNamespace: "shop"})
	entries := p.Sync([]Service{
		{Namespace: "shop", Name: "cart"},
		{Namespace: "data", Name: "postgres"},
		{Namespace: "shop", Name: "localhost"}, // must never become a bare name
	})
	cart, _ := entryFor(entries, "cart.shop")
	if want := []string{"cart.shop", "cart.shop.svc", "cart.shop.svc.corp.internal", "cart"}; !reflect.DeepEqual(cart.Names, want) {
		t.Errorf("home namespace names = %v, want %v", cart.Names, want)
	}
	pg, _ := entryFor(entries, "postgres.data")
	if want := []string{"postgres.data", "postgres.data.svc", "postgres.data.svc.corp.internal"}; !reflect.DeepEqual(pg.Names, want) {
		t.Errorf("other namespace names = %v, want %v", pg.Names, want)
	}
	if _, ok := entryFor(entries, "localhost"); ok {
		t.Error(`a Service named "localhost" was given the bare name`)
	}
	// Whatever the proxy produces must be acceptable to the hosts file.
	if _, err := hosts.Render(nil, entries); err != nil {
		t.Errorf("entries rejected by the hosts package: %v", err)
	}
}

func TestSyncSkipsNamesThatAreNotDNSLabels(t *testing.T) {
	p := newProxy(t, ProxyOptions{Dial: (&recordingDial{}).dial})
	entries := p.Sync([]Service{
		{Namespace: "shop", Name: "ok"},
		{Namespace: "shop", Name: "evil\n6.6.6.6 bank.example"},
		{Namespace: "shop", Name: "has.dot"},
		{Namespace: "Shop", Name: "upper-namespace"},
		{Namespace: "", Name: "no-namespace"},
		{Namespace: "shop", Name: ""},
		{Namespace: "shop", Name: strings.Repeat("a", 64)},
	})
	if len(entries) != 1 || entries[0].Names[0] != "ok.shop" {
		t.Errorf("entries = %+v, want only ok.shop", entries)
	}
}

func TestAddressesAreStableAndFreedAddressesAreNotSharedWhileInUse(t *testing.T) {
	p := newProxy(t, ProxyOptions{Dial: (&recordingDial{}).dial})
	a := Service{Namespace: "ns", Name: "a"}
	b := Service{Namespace: "ns", Name: "b"}
	c := Service{Namespace: "ns", Name: "c"}

	first := p.Sync([]Service{a, b})
	ipA, _ := entryFor(first, "a.ns")
	ipB, _ := entryFor(first, "b.ns")

	// A new Service sorting before the others must not shift them.
	second := p.Sync([]Service{{Namespace: "aa", Name: "new"}, a, b, c})
	if e, _ := entryFor(second, "a.ns"); e.IP != ipA.IP {
		t.Errorf("a moved from %s to %s", ipA.IP, e.IP)
	}
	if e, _ := entryFor(second, "b.ns"); e.IP != ipB.IP {
		t.Errorf("b moved from %s to %s", ipB.IP, e.IP)
	}
	seen := map[string]string{}
	for _, e := range second {
		if other, dup := seen[e.IP]; dup {
			t.Errorf("%s and %s share address %s", other, e.Names[0], e.IP)
		}
		seen[e.IP] = e.Names[0]
	}

	// Dropping a Service removes its entry.
	third := p.Sync([]Service{b})
	if len(third) != 1 || third[0].IP != ipB.IP {
		t.Errorf("after removal: %+v", third)
	}
}

func TestSyncOpensAndClosesPortsAsServicesChange(t *testing.T) {
	requireLoopbackRange(t)
	rec := &recordingDial{target: echoServer(t, "cluster")}
	p := newProxy(t, ProxyOptions{Dial: rec.dial})

	entries := p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{39180}}})
	ip := entries[0].IP
	if _, err := askLine(ip+":39180", "x"); err != nil {
		t.Fatalf("port not served: %v", err)
	}

	// The Service changes its ports.
	p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{39181}}})
	if _, err := askLine(ip+":39181", "x"); err != nil {
		t.Errorf("new port not served: %v", err)
	}
	if _, err := askLine(ip+":39180", "x"); err == nil {
		t.Error("removed port still served")
	}

	// The Service disappears.
	p.Sync(nil)
	if _, err := askLine(ip+":39181", "x"); err == nil {
		t.Error("port still served after the service was removed")
	}
}

func TestPortThatCannotListenDoesNotBlockTheRest(t *testing.T) {
	requireLoopbackRange(t)
	rec := &recordingDial{target: echoServer(t, "cluster")}
	p := newProxy(t, ProxyOptions{Dial: rec.dial})

	// Take the address the first Service will get, on one of its ports.
	busy, err := net.Listen("tcp", "127.78.0.1:39280")
	if err != nil {
		t.Skipf("cannot reserve the test port: %v", err)
	}
	defer busy.Close()

	entries := p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{39280, 39281}}})
	if len(entries) != 1 || entries[0].IP != "127.78.0.1" {
		t.Fatalf("entries = %+v", entries)
	}
	if got, err := askLine("127.78.0.1:39281", "x"); err != nil || got != "cluster:x" {
		t.Errorf("the free port is not served: %q, %v", got, err)
	}

	// Once the port is free again, the next sync picks it up.
	busy.Close()
	p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{39280, 39281}}})
	if got, err := askLine("127.78.0.1:39280", "x"); err != nil || got != "cluster:x" {
		t.Errorf("the freed port was not picked up: %q, %v", got, err)
	}
}

func TestInvalidPortsAreIgnored(t *testing.T) {
	p := newProxy(t, ProxyOptions{Dial: (&recordingDial{}).dial})
	entries := p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{0, -1, 70000}}})
	// The name still resolves; there is just nothing listening.
	if len(entries) != 1 {
		t.Errorf("entries = %+v", entries)
	}
}

func TestNewServiceProxyRejectsBadBaseAddress(t *testing.T) {
	for _, base := range []string{"10.0.0.0", "127.77.0.1", "127.77.5.0", "not-an-ip", "::1"} {
		if p, err := NewServiceProxy(ProxyOptions{BaseIP: base}); err == nil {
			p.Close()
			t.Errorf("base %q was accepted", base)
		}
	}
}

func TestCloseStopsEverything(t *testing.T) {
	requireLoopbackRange(t)
	rec := &recordingDial{target: echoServer(t, "cluster")}
	p := newProxy(t, ProxyOptions{Dial: rec.dial})
	entries := p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{39380}}})
	addr := entries[0].IP + ":39380"
	if _, err := askLine(addr, "x"); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, err := askLine(addr, "x"); err == nil {
		t.Error("still serving after Close")
	}
	if got := p.Sync([]Service{{Namespace: "shop", Name: "cart", Ports: []int{39380}}}); got != nil {
		t.Errorf("Sync after Close returned %+v", got)
	}
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRunKeepsHostsFileInStepAndCleansUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	original := "127.0.0.1 localhost\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	read := func() string {
		b, _ := os.ReadFile(path)
		return string(b)
	}

	var mu sync.Mutex
	services := []Service{{Namespace: "shop", Name: "cart"}}
	failing := true
	list := func(context.Context) ([]Service, error) {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			return nil, errors.New("tunnel is not connected")
		}
		return services, nil
	}

	p := newProxy(t, ProxyOptions{Dial: (&recordingDial{}).dial})
	ctx, cancel := context.WithCancel(context.Background())
	out := &lockedBuf{}
	done := make(chan struct{})
	go func() {
		p.Run(ctx, RunConfig{List: list, HostsPath: path, Interval: 30 * time.Millisecond, Out: out})
		close(done)
	}()

	// While the listing fails, the hosts file is left alone.
	time.Sleep(100 * time.Millisecond)
	if got := read(); got != original {
		t.Errorf("hosts file changed before any listing succeeded: %q", got)
	}

	mu.Lock()
	failing = false
	mu.Unlock()
	waitUntil(t, "first entries", func() bool { return strings.Contains(read(), "cart.shop.svc.cluster.local # k8s-tunnel") })

	// A new Service appears in the cluster.
	mu.Lock()
	services = append(services, Service{Namespace: "data", Name: "postgres"})
	mu.Unlock()
	waitUntil(t, "refreshed entries", func() bool { return strings.Contains(read(), "postgres.data.svc") })

	// A failed refresh keeps what was there instead of wiping it.
	mu.Lock()
	failing = true
	mu.Unlock()
	time.Sleep(120 * time.Millisecond)
	if !strings.Contains(read(), "postgres.data.svc") {
		t.Error("entries were dropped when a refresh failed")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if got := read(); got != original {
		t.Errorf("hosts file not restored on exit: %q", got)
	}
	for _, want := range []string{"resolving 1 cluster services", "resolving 2 cluster services"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status output is missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunReportsHostsFileProblems(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "hosts")
	p := newProxy(t, ProxyOptions{Dial: (&recordingDial{}).dial})
	ctx, cancel := context.WithCancel(context.Background())
	out := &lockedBuf{}
	done := make(chan struct{})
	go func() {
		p.Run(ctx, RunConfig{
			List:      func(context.Context) ([]Service, error) { return []Service{{Namespace: "a", Name: "b"}}, nil },
			HostsPath: missing, Interval: 20 * time.Millisecond, Out: out,
		})
		close(done)
	}()
	waitUntil(t, "an error to be reported", func() bool { return strings.Contains(out.String(), "could not update hosts file") })
	cancel()
	<-done
}
