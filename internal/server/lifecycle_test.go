package server_test

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/protocol"
	"github.com/ashutosh5786/k8s-tunnel/internal/server"
	"github.com/ashutosh5786/k8s-tunnel/pkg/tunnel"
)

// startCustom runs a server with the given config and returns its address
// and a function that stops it and waits for Serve to return.
func startCustom(t *testing.T, cfg server.Config) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.New(cfg).Serve(ctx, ln) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
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
	}
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

func connectTo(t *testing.T, addr, user, session string) *tunnel.Client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := tunnel.Connect(ctx5(t), conn, tunnel.Options{User: user, Session: session})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func serverPortAddr(exp *tunnel.Exposure, i int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[i].ServerPort))
}

func TestMultiPortExposureRoutesEachPortSeparately(t *testing.T) {
	h := startServer(t, time.Minute)
	_, httpPort := echoServer(t, "http")
	_, grpcPort := echoServer(t, "grpc")
	c := h.connect("ash", "s1")

	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{
		{Name: "http", Port: 8080, LocalPort: httpPort},
		{Name: "grpc", Port: 9090, LocalPort: grpcPort},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Ports) != 2 || exp.Ports[0].ServerPort == exp.Ports[1].ServerPort {
		t.Fatalf("ports = %+v", exp.Ports)
	}
	for i, want := range []string{"http", "grpc"} {
		conn, err := net.Dial("tcp", serverPortAddr(exp, i))
		if err != nil {
			t.Fatal(err)
		}
		if got := roundTrip(t, conn, "x"); got != want+":x" {
			t.Errorf("port %s reached %q", want, got)
		}
		conn.Close()
	}
}

func TestLocalPortDefaultsToServicePort(t *testing.T) {
	h := startServer(t, time.Minute)
	_, port := echoServer(t, "b")
	c := h.connect("ash", "s1")
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: port}})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Ports[0].LocalPort != port {
		t.Errorf("local port = %d, want %d", exp.Ports[0].LocalPort, port)
	}
	conn, err := net.Dial("tcp", serverPortAddr(exp, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "x"); got != "b:x" {
		t.Errorf("got %q", got)
	}
}

func TestReExposingSameNameReplacesIt(t *testing.T) {
	h := startServer(t, time.Minute)
	_, oldPort := echoServer(t, "old")
	_, newPort := echoServer(t, "new")
	c := h.connect("ash", "s1")

	first, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: oldPort}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: newPort}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", serverPortAddr(second, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "x"); got != "new:x" {
		t.Errorf("got %q", got)
	}
	if first.Ports[0].ServerPort != second.Ports[0].ServerPort {
		if old, err := net.DialTimeout("tcp", serverPortAddr(first, 0), time.Second); err == nil {
			old.Close()
			t.Error("the replaced exposure's port is still open")
		}
	}
	if got := h.pub.published["b-ash"].Ports[0].ServerPort; got != second.Ports[0].ServerPort {
		t.Errorf("published port %d, want %d", got, second.Ports[0].ServerPort)
	}
}

func TestInboundIsDroppedWhenLocalServiceIsDown(t *testing.T) {
	h := startServer(t, time.Minute)
	// Nothing listens on this local port.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	c := h.connect("ash", "s1")
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: dead}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", serverPortAddr(exp, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The caller must get a closed connection, not a hang.
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if n, err := io.Copy(io.Discard, conn); n != 0 {
		t.Errorf("read %d bytes from a dead service", n)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Error("connection hung instead of closing")
	}

	// The session itself must survive one failed delivery.
	target, _ := echoServer(t, "c")
	out, err := c.DialContext(ctx5(t), "tcp", target)
	if err != nil {
		t.Fatalf("session broken after failed inbound: %v", err)
	}
	out.Close()
}

func TestSameSessionReconnectingReplacesOldConnection(t *testing.T) {
	h := startServer(t, time.Minute)
	c1 := h.connect("ash", "s1")
	c2 := h.connect("ash", "s1")

	select {
	case <-c1.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("old connection was not closed when the session reconnected")
	}
	target, _ := echoServer(t, "c")
	conn, err := c2.DialContext(ctx5(t), "tcp", target)
	if err != nil {
		t.Fatalf("new connection unusable: %v", err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "x"); got != "c:x" {
		t.Errorf("got %q", got)
	}
}

func TestShutdownUnpublishesAndClosesSessions(t *testing.T) {
	pub := newFakePublisher()
	addr, stop := startCustom(t, server.Config{Replica: "r0", Publisher: pub, Grace: time.Minute})
	_, localPort := echoServer(t, "b")
	c := connectTo(t, addr, "ash", "s1")
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}})
	if err != nil {
		t.Fatal(err)
	}

	stop()
	// A stopping replica removes its Services at once, without waiting
	// for the grace period, so traffic is not sent to a dead pod.
	if pub.has("b-ash") {
		t.Error("exposure still published after shutdown")
	}
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Error("client session still open after shutdown")
	}
	if conn, err := net.DialTimeout("tcp", serverPortAddr(exp, 0), time.Second); err == nil {
		conn.Close()
		t.Error("exposure port still open after shutdown")
	}
}

// sweepingPublisher adds the optional Sweep to the fake.
type sweepingPublisher struct {
	*fakePublisher
	swept chan struct{}
}

func (p *sweepingPublisher) Sweep(_ context.Context, live func(string) bool) error {
	p.mu.Lock()
	var names []string
	for name := range p.published {
		names = append(names, name)
	}
	p.mu.Unlock()
	// live takes the server's lock, so call it without holding ours.
	for _, name := range names {
		if !live(name) {
			p.mu.Lock()
			delete(p.published, name)
			p.mu.Unlock()
		}
	}
	close(p.swept)
	return nil
}

func TestStartupSweepRemovesLeftoversButKeepsLiveExposures(t *testing.T) {
	pub := &sweepingPublisher{fakePublisher: newFakePublisher(), swept: make(chan struct{})}
	// Left behind by an earlier run of this replica.
	pub.published["stale"] = server.Exposure{Name: "stale", Session: "gone"}

	addr, _ := startCustom(t, server.Config{Replica: "r0", Publisher: pub, Grace: 300 * time.Millisecond})
	_, localPort := echoServer(t, "b")
	c := connectTo(t, addr, "ash", "s1")
	if _, err := c.Expose(ctx5(t), "alive", []tunnel.Port{{Port: 8080, LocalPort: localPort}}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-pub.swept:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep never ran")
	}
	if pub.has("stale") {
		t.Error("leftover exposure survived the sweep")
	}
	if !pub.has("alive") {
		t.Error("sweep removed a live exposure")
	}
}

func TestSweepKeepsExposuresWaitingOutTheirGracePeriod(t *testing.T) {
	pub := &sweepingPublisher{fakePublisher: newFakePublisher(), swept: make(chan struct{})}
	addr, _ := startCustom(t, server.Config{Replica: "r0", Publisher: pub, Grace: 400 * time.Millisecond})
	_, localPort := echoServer(t, "b")

	// Connect, expose, and drop shortly before the sweep fires, so the
	// exposure is in its grace period when the sweep looks at it.
	time.Sleep(250 * time.Millisecond)
	c := connectTo(t, addr, "ash", "s1")
	if _, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}}); err != nil {
		t.Fatal(err)
	}
	c.Close()

	<-pub.swept
	if !pub.has("b-ash") {
		t.Error("sweep removed an exposure whose session may still reconnect")
	}
}

// rawSession opens a tunnel connection without the client library, to send
// things a well-behaved client never would.
func rawSession(t *testing.T, addr string) *protocol.Session {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	mux, err := protocol.NewClient(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mux.Close() })
	return mux
}

func rawCall(t *testing.T, mux *protocol.Session, req *protocol.Request) (*protocol.Response, error) {
	t.Helper()
	st, err := mux.OpenStream(ctx5(t))
	if err != nil {
		return nil, err
	}
	defer st.Close()
	st.SetDeadline(time.Now().Add(5 * time.Second))
	if err := protocol.WriteMsg(st, req); err != nil {
		return nil, err
	}
	var resp protocol.Response
	if err := protocol.ReadMsg(st, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func TestHandshakeRules(t *testing.T) {
	h := startServer(t, time.Minute)
	hello := func(version int, user, session string) *protocol.Request {
		return &protocol.Request{Type: protocol.TypeHello, Hello: &protocol.Hello{Version: version, User: user, Session: session}}
	}
	for name, tc := range map[string]struct {
		req  *protocol.Request
		want string
	}{
		"wrong version":  {hello(protocol.Version+1, "ash", "s1"), "version"},
		"empty user":     {hello(protocol.Version, "", "s1"), "user"},
		"bad session":    {hello(protocol.Version, "ash", "UPPER"), "session"},
		"not hello":      {&protocol.Request{Type: protocol.TypeDial, Dial: &protocol.Dial{Address: "c:80"}}, "hello"},
		"hello no body":  {&protocol.Request{Type: protocol.TypeHello}, "hello"},
		"overlong user":  {hello(protocol.Version, strings.Repeat("u", 300), "s1"), "user"},
		"session spaces": {hello(protocol.Version, "ash", "has space"), "session"},
	} {
		mux := rawSession(t, h.addr)
		resp, err := rawCall(t, mux, tc.req)
		if err != nil {
			t.Errorf("%s: no reply: %v", name, err)
			continue
		}
		if resp.OK || !strings.Contains(resp.Error, tc.want) {
			t.Errorf("%s: resp = %+v, want an error mentioning %q", name, resp, tc.want)
		}
		// A rejected handshake ends the connection.
		select {
		case <-mux.CloseChan():
		case <-time.After(5 * time.Second):
			t.Errorf("%s: connection left open after a rejected handshake", name)
		}
	}
}

func TestUnknownAndMalformedRequestsAreRefused(t *testing.T) {
	h := startServer(t, time.Minute)
	mux := rawSession(t, h.addr)
	if resp, err := rawCall(t, mux, &protocol.Request{Type: protocol.TypeHello,
		Hello: &protocol.Hello{Version: protocol.Version, User: "ash", Session: "s1"}}); err != nil || !resp.OK {
		t.Fatalf("hello: %+v %v", resp, err)
	}
	for name, req := range map[string]*protocol.Request{
		"unknown type":      {Type: "teleport"},
		"dial without body": {Type: protocol.TypeDial},
		"inbound to server": {Type: protocol.TypeInbound, Inbound: &protocol.Inbound{Exposure: "x", LocalPort: 1}},
		"expose no ports":   {Type: protocol.TypeExpose, Expose: &protocol.Expose{Name: "b-ash"}},
	} {
		resp, err := rawCall(t, mux, req)
		if err != nil {
			t.Errorf("%s: no reply: %v", name, err)
		} else if resp.OK {
			t.Errorf("%s: was accepted", name)
		}
	}
	// Bad requests must not cost the client its session.
	if resp, err := rawCall(t, mux, &protocol.Request{Type: protocol.TypeUnexpose,
		Unexpose: &protocol.Unexpose{Name: "never-existed"}}); err != nil || !resp.OK {
		t.Errorf("session unusable after bad requests: %+v %v", resp, err)
	}
}

func TestTooManyPortsIsRefused(t *testing.T) {
	h := startServer(t, time.Minute)
	c := h.connect("ash", "s1")
	ports := make([]tunnel.Port, 17)
	for i := range ports {
		ports[i] = tunnel.Port{Port: 8000 + i}
	}
	if _, err := c.Expose(ctx5(t), "b-ash", ports); err == nil {
		t.Error("an exposure with 17 ports was accepted")
	}
}

// failingPublisher refuses every publish.
type failingPublisher struct{ *fakePublisher }

func (failingPublisher) Publish(context.Context, server.Exposure) (string, error) {
	return "", context.DeadlineExceeded
}

func TestPublishFailureFailsExposeAndFreesTheName(t *testing.T) {
	addr, _ := startCustom(t, server.Config{Replica: "r0", Publisher: failingPublisher{newFakePublisher()}, Grace: time.Minute})
	_, localPort := echoServer(t, "b")
	c1 := connectTo(t, addr, "ash", "s1")
	if _, err := c1.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}}); err == nil {
		t.Fatal("expose succeeded although publishing failed")
	}
	// The failed attempt must not leave the name claimed: another session
	// gets the publish error, not "in use by another session".
	c2 := connectTo(t, addr, "sam", "s2")
	_, err := c2.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}})
	if err == nil || strings.Contains(err.Error(), "in use") {
		t.Errorf("err = %v, want a publish error", err)
	}
}

func TestServerWithoutPublisherStillExposesPorts(t *testing.T) {
	addr, _ := startCustom(t, server.Config{Replica: "r0"})
	_, localPort := echoServer(t, "b")
	c := connectTo(t, addr, "ash", "s1")
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Host != "" {
		t.Errorf("host = %q, want empty without a publisher", exp.Host)
	}
	conn, err := net.Dial("tcp", serverPortAddr(exp, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := roundTrip(t, conn, "x"); got != "b:x" {
		t.Errorf("got %q", got)
	}
}

func TestManyConcurrentConnectionsThroughOneSession(t *testing.T) {
	h := startServer(t, time.Minute)
	target, _ := echoServer(t, "c")
	_, localPort := echoServer(t, "b")
	c := h.connect("ash", "s1")
	exp, err := c.Expose(ctx5(t), "b-ash", []tunnel.Port{{Port: 8080, LocalPort: localPort}})
	if err != nil {
		t.Fatal(err)
	}

	const n = 40
	errs := make(chan string, 2*n)
	var wg sync.WaitGroup
	for i := range n {
		msg := "m" + strconv.Itoa(i)
		wg.Add(2)
		go func() { // outbound
			defer wg.Done()
			conn, err := c.DialContext(ctx5(t), "tcp", target)
			if err != nil {
				errs <- err.Error()
				return
			}
			defer conn.Close()
			if got := roundTrip(t, conn, msg); got != "c:"+msg {
				errs <- "outbound got " + got
			}
		}()
		go func() { // inbound
			defer wg.Done()
			conn, err := net.Dial("tcp", serverPortAddr(exp, 0))
			if err != nil {
				errs <- err.Error()
				return
			}
			defer conn.Close()
			if got := roundTrip(t, conn, msg); got != "b:"+msg {
				errs <- "inbound got " + got
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
