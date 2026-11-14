package tunnel_test

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s-tunnel/internal/protocol"
	"k8s-tunnel/internal/server"
	"k8s-tunnel/pkg/tunnel"
)

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// realServer runs the actual tunnel server and returns its address and a
// stop function.
func realServer(t *testing.T, cfg server.Config) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { server.New(cfg).Serve(ctx, ln); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

func connect(t *testing.T, addr string, opts tunnel.Options) *tunnel.Client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := tunnel.Connect(ctx5(t), conn, opts)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// fakeServer speaks just enough protocol to accept a hello, then hands the
// session to the test so it can act like a misbehaving server.
func fakeServer(t *testing.T, hello *protocol.Response) (addr string, sessions <-chan *protocol.Session) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ch := make(chan *protocol.Session, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		mux, err := protocol.NewServer(conn)
		if err != nil {
			return
		}
		st, err := mux.AcceptStream()
		if err != nil {
			return
		}
		var req protocol.Request
		if protocol.ReadMsg(st, &req) != nil {
			return
		}
		protocol.WriteMsg(st, hello)
		st.Close()
		ch <- mux
	}()
	return ln.Addr().String(), ch
}

// countingListener counts the connections a local port receives.
func countingListener(t *testing.T) (port int, hits *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	hits = new(atomic.Int32)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			hits.Add(1)
			c.Close()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, hits
}

// sendInbound plays the server asking the client to deliver a connection.
func sendInbound(t *testing.T, mux *protocol.Session, exposure string, port int) *protocol.Response {
	t.Helper()
	st, err := mux.OpenStream(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	st.SetDeadline(time.Now().Add(5 * time.Second))
	req := &protocol.Request{Type: protocol.TypeInbound, Inbound: &protocol.Inbound{Exposure: exposure, LocalPort: port}}
	if err := protocol.WriteMsg(st, req); err != nil {
		t.Fatal(err)
	}
	var resp protocol.Response
	if err := protocol.ReadMsg(st, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

func TestConnectReportsReplica(t *testing.T) {
	addr, _ := realServer(t, server.Config{Replica: "tunnel-server-3"})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	if c.Replica() != "tunnel-server-3" {
		t.Errorf("replica = %q", c.Replica())
	}
}

func TestConnectFailsWhenServerRejectsHello(t *testing.T) {
	addr, _ := fakeServer(t, protocol.Fail("go away"))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tunnel.Connect(ctx5(t), conn, tunnel.Options{User: "ash", Session: "s1"})
	if err == nil || !strings.Contains(err.Error(), "go away") {
		t.Fatalf("err = %v, want the server's reason", err)
	}
	// Connect owns the connection and must close it on failure.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, rerr := conn.Read(make([]byte, 1)); rerr == nil {
		t.Error("connection left open after a failed handshake")
	} else if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
		t.Error("connection left open after a failed handshake")
	}
}

func TestConnectHonoursContextWhenServerIsSilent(t *testing.T) {
	// A listener that accepts and then says nothing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			time.Sleep(10 * time.Second)
		}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = tunnel.Connect(ctx, conn, tunnel.Options{User: "ash", Session: "s1"})
	if err == nil {
		t.Fatal("connect to a silent server succeeded")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("connect took %v, the context allowed 200ms", took)
	}
}

func TestDialRejectsNonTCP(t *testing.T) {
	addr, _ := realServer(t, server.Config{})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	if _, err := c.DialContext(ctx5(t), "udp", "c:53"); err == nil {
		t.Error("udp dial was accepted")
	}
}

func TestDialIsCancelledByContext(t *testing.T) {
	// The server's cluster dial hangs until its own context ends.
	slow := func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	addr, _ := realServer(t, server.Config{Dial: slow, DialTimeout: 30 * time.Second})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.DialContext(ctx, "tcp", "slow.example:80")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want deadline exceeded", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("dial took %v, the context allowed 200ms", took)
	}
	// One cancelled dial must not break the session.
	if _, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080}}); err != nil {
		t.Errorf("session unusable after a cancelled dial: %v", err)
	}
}

func TestDoneClosesWhenServerStops(t *testing.T) {
	addr, stop := realServer(t, server.Config{})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	select {
	case <-c.Done():
		t.Fatal("Done closed while the server was running")
	default:
	}
	stop()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after the server stopped")
	}
	if _, err := c.DialContext(ctx5(t), "tcp", "c:80"); err == nil {
		t.Error("dial on a dead session succeeded")
	}
}

func TestExposeNeedsAPort(t *testing.T) {
	addr, _ := realServer(t, server.Config{})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	if _, err := c.Expose(ctx5(t), "b-ash", nil); err == nil {
		t.Error("expose with no ports was accepted")
	}
}

// The client must never connect to a local port it did not expose, even if
// the server asks. This is what stops a compromised server from reaching
// arbitrary services on the developer's machine.
func TestClientRefusesInboundForUnexposedPorts(t *testing.T) {
	addr, sessions := fakeServer(t, &protocol.Response{OK: true})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	mux := <-sessions
	defer mux.Close()
	secretPort, hits := countingListener(t)

	if resp := sendInbound(t, mux, "b-ash", secretPort); resp.OK {
		t.Error("client delivered to a port with no exposure at all")
	}

	// Expose a different port; the fake server must answer the request.
	exposedPort, exposedHits := countingListener(t)
	go func() {
		st, err := mux.AcceptStream()
		if err != nil {
			return
		}
		var req protocol.Request
		protocol.ReadMsg(st, &req)
		protocol.WriteMsg(st, &protocol.Response{OK: true, Expose: &protocol.ExposeReply{Name: req.Expose.Name}})
		st.Close()
	}()
	if _, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: exposedPort}}); err != nil {
		t.Fatal(err)
	}

	if resp := sendInbound(t, mux, "b-ash", secretPort); resp.OK {
		t.Error("client delivered to a port outside the exposure")
	}
	if resp := sendInbound(t, mux, "other-name", exposedPort); resp.OK {
		t.Error("client delivered under an exposure name it never created")
	}
	if resp := sendInbound(t, mux, "b-ash", exposedPort); !resp.OK {
		t.Errorf("client refused its own exposed port: %s", resp.Error)
	}
	time.Sleep(100 * time.Millisecond)
	if n := hits.Load(); n != 0 {
		t.Errorf("unexposed port received %d connections", n)
	}
	if n := exposedHits.Load(); n != 1 {
		t.Errorf("exposed port received %d connections, want 1", n)
	}
}

func TestFailedExposeDoesNotOpenThePort(t *testing.T) {
	addr, sessions := fakeServer(t, &protocol.Response{OK: true})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	mux := <-sessions
	defer mux.Close()
	port, hits := countingListener(t)

	go func() {
		st, err := mux.AcceptStream()
		if err != nil {
			return
		}
		var req protocol.Request
		protocol.ReadMsg(st, &req)
		protocol.WriteMsg(st, protocol.Fail("name in use"))
		st.Close()
	}()
	if _, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: port}}); err == nil {
		t.Fatal("expose succeeded although the server refused it")
	}
	if resp := sendInbound(t, mux, "b-ash", port); resp.OK {
		t.Error("a refused exposure still accepts inbound traffic")
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("port received %d connections after a refused expose", n)
	}
}

func TestUnexposeStopsDelivery(t *testing.T) {
	addr, _ := realServer(t, server.Config{})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1"})
	port, hits := countingListener(t)
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: port}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Unexpose(ctx5(t), "b-ash"); err != nil {
		t.Fatal(err)
	}
	serverAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort))
	if conn, err := net.DialTimeout("tcp", serverAddr, time.Second); err == nil {
		conn.Close()
	}
	time.Sleep(100 * time.Millisecond)
	if n := hits.Load(); n != 0 {
		t.Errorf("local port received %d connections after unexpose", n)
	}
}

func TestLocalHostOptionChoosesDeliveryAddress(t *testing.T) {
	// Listen only on a second loopback address; delivery to 127.0.0.1
	// would fail, so success proves the option is used.
	ln, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("127.0.0.2 not usable on this machine: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	got := make(chan struct{}, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
			got <- struct{}{}
		}
	}()

	addr, _ := realServer(t, server.Config{})
	c := connect(t, addr, tunnel.Options{User: "ash", Session: "s1", LocalHost: "127.0.0.2"})
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: port}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not delivered to the configured local host")
	}
}
