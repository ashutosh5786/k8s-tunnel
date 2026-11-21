package server_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/server"
	"github.com/ashutosh5786/k8s-tunnel/pkg/tunnel"
)

// fakePublisher records what is published instead of talking to a cluster.
type fakePublisher struct {
	mu        sync.Mutex
	published map[string]server.Exposure
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{published: map[string]server.Exposure{}}
}

func (f *fakePublisher) Publish(_ context.Context, e server.Exposure) (string, error) {
	f.mu.Lock()
	f.published[e.Name] = e
	f.mu.Unlock()
	return e.Name + ".test.svc", nil
}

func (f *fakePublisher) Unpublish(_ context.Context, name, session string) error {
	f.mu.Lock()
	if e, ok := f.published[name]; ok && e.Session == session {
		delete(f.published, name)
	}
	f.mu.Unlock()
	return nil
}

func (f *fakePublisher) has(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.published[name]
	return ok
}

// waitFor polls until cond holds or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// echoServer answers every line with "<prefix>:<line>".
func echoServer(t *testing.T, prefix string) (addr string, port int) {
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

func roundTrip(t *testing.T, c net.Conn, msg string) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(c, msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return line[:len(line)-1]
}

type harness struct {
	t    *testing.T
	addr string
	pub  *fakePublisher
}

func startServer(t *testing.T, grace time.Duration) *harness {
	t.Helper()
	pub := newFakePublisher()
	srv := server.New(server.Config{Replica: "tunnel-server-0", Publisher: pub, Grace: grace})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down")
		}
	})
	return &harness{t: t, addr: ln.Addr().String(), pub: pub}
}

func (h *harness) connect(user, session string) *tunnel.Client {
	h.t.Helper()
	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := tunnel.Connect(ctx, conn, tunnel.Options{User: user, Session: session})
	if err != nil {
		h.t.Fatalf("connect: %v", err)
	}
	h.t.Cleanup(func() { c.Close() })
	return c
}

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestDialReachesClusterAddress(t *testing.T) {
	h := startServer(t, time.Minute)
	target, _ := echoServer(t, "c")
	c := h.connect("ash", "s1")
	if c.Replica() != "tunnel-server-0" {
		t.Errorf("replica = %q", c.Replica())
	}

	conn, err := c.DialContext(ctx5(t), "tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "hello"); got != "c:hello" {
		t.Errorf("got %q", got)
	}
}

func TestDialFailureIsReported(t *testing.T) {
	h := startServer(t, time.Minute)
	c := h.connect("ash", "s1")
	// A listener that is closed again gives an address nothing listens on.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()

	if _, err := c.DialContext(ctx5(t), "tcp", dead); err == nil {
		t.Fatal("dial to a dead address succeeded")
	}
	if _, err := c.DialContext(ctx5(t), "tcp", "no-port"); err == nil {
		t.Fatal("dial to a malformed address succeeded")
	}
}

func TestHalfCloseIsPassedThrough(t *testing.T) {
	h := startServer(t, time.Minute)
	// This server reads until the client stops sending, then replies.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		body, _ := io.ReadAll(c)
		fmt.Fprintf(c, "got %d bytes", len(body))
	}()

	c := h.connect("ash", "s1")
	conn, err := c.DialContext(ctx5(t), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprint(conn, "12345")
	conn.(interface{ CloseWrite() error }).CloseWrite()
	reply, err := io.ReadAll(conn)
	if err != nil || string(reply) != "got 5 bytes" {
		t.Fatalf("reply = %q, err = %v", reply, err)
	}
}

func TestExposeDeliversClusterTrafficToLocalPort(t *testing.T) {
	h := startServer(t, time.Minute)
	_, localPort := echoServer(t, "local-b")
	c := h.connect("ash", "s1")

	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Name: "http", Port: 8080, LocalPort: localPort, AppProtocol: "http"}})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Host != "b-ash.test.svc" || len(exp.Ports) != 1 {
		t.Fatalf("exposure = %+v", exp)
	}
	pub := h.pub.published["b-ash"]
	if pub.User != "ash" || pub.Session != "s1" || pub.Ports[0].Port != 8080 ||
		pub.Ports[0].ServerPort != exp.Ports[0].ServerPort || pub.Ports[0].AppProtocol != "http" {
		t.Fatalf("published = %+v", pub)
	}

	// Connecting to the server port is what a Service would do in a cluster.
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "ping"); got != "local-b:ping" {
		t.Errorf("got %q", got)
	}
}

func TestTwoUsersAreKeptApart(t *testing.T) {
	h := startServer(t, time.Minute)
	_, portA := echoServer(t, "ash")
	_, portB := echoServer(t, "sam")
	ca := h.connect("ash", "s-ash")
	cb := h.connect("sam", "s-sam")

	ea, err := ca.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: portA}})
	if err != nil {
		t.Fatal(err)
	}
	eb, err := cb.Expose(ctx5(t), "b-sam", []tunnel.Port{{Port: 8080, LocalPort: portB}})
	if err != nil {
		t.Fatal(err)
	}
	for want, exp := range map[string]*tunnel.Exposure{"ash": ea, "sam": eb} {
		conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort)))
		if err != nil {
			t.Fatal(err)
		}
		if got := roundTrip(t, conn, "x"); got != want+":x" {
			t.Errorf("traffic for %s reached %q", want, got)
		}
		conn.Close()
	}

	// A name owned by a live session cannot be taken by someone else.
	if _, err := cb.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: portB}}); err == nil {
		t.Error("sam took over ash's exposure name")
	}
}

func TestExposeRejectsBadRequests(t *testing.T) {
	h := startServer(t, time.Minute)
	c := h.connect("ash", "s1")
	for name, ports := range map[string][]tunnel.Port{
		"Bad_Name": {{Port: 8080}},
		"dup":      {{Port: 8080}, {Port: 8080, LocalPort: 9090}},
		"range":    {{Port: 70000}},
	} {
		if _, err := c.Expose(ctx5(t), name, ports); err == nil {
			t.Errorf("expose %q was accepted", name)
		}
	}
	if len(h.pub.published) != 0 {
		t.Errorf("rejected requests were published: %v", h.pub.published)
	}
}

func TestUnexposeRemovesImmediately(t *testing.T) {
	h := startServer(t, time.Minute)
	_, localPort := echoServer(t, "b")
	c := h.connect("ash", "s1")
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Unexpose(ctx5(t), "b-ash"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "service removal", func() bool { return !h.pub.has("b-ash") })
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort))
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		t.Error("server port still open after unexpose")
	}
}

func TestDisconnectUnpublishesAfterGrace(t *testing.T) {
	h := startServer(t, 200*time.Millisecond)
	_, localPort := echoServer(t, "b")
	c := h.connect("ash", "s1")
	if _, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}}); err != nil {
		t.Fatal(err)
	}
	c.Close()

	time.Sleep(50 * time.Millisecond)
	if !h.pub.has("b-ash") {
		t.Fatal("exposure unpublished before the grace period ended")
	}
	waitFor(t, "unpublish after grace", func() bool { return !h.pub.has("b-ash") })
}

func TestReconnectWithinGraceKeepsExposure(t *testing.T) {
	h := startServer(t, 300*time.Millisecond)
	_, localPort := echoServer(t, "b")
	ports := []tunnel.Port{{Port: 8080, LocalPort: localPort}}

	c1 := h.connect("ash", "s1")
	if _, err := c1.Expose(ctx5(t), "b-ash", ports); err != nil {
		t.Fatal(err)
	}
	c1.Close()

	c2 := h.connect("ash", "s1")
	exp, err := c2.Expose(ctx5(t), "b-ash", ports)
	if err != nil {
		t.Fatal(err)
	}
	// Well past the grace period the exposure must still be there and work.
	time.Sleep(600 * time.Millisecond)
	if !h.pub.has("b-ash") {
		t.Fatal("exposure was unpublished although the session reconnected")
	}
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "again"); got != "b:again" {
		t.Errorf("got %q", got)
	}
}

func TestBadHandshakeIsRejected(t *testing.T) {
	h := startServer(t, time.Minute)
	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := tunnel.Connect(ctx, conn, tunnel.Options{User: "ash", Session: "Not A Valid ID"}); err == nil {
		t.Fatal("invalid session id was accepted")
	}
}
