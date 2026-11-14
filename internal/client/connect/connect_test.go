package connect

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s-tunnel/internal/server"
	"k8s-tunnel/pkg/tunnel"
)

// recorder is a Publisher that remembers what is currently published.
type recorder struct {
	mu        sync.Mutex
	published map[string]server.Exposure
}

func (r *recorder) Publish(_ context.Context, e server.Exposure) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.published == nil {
		r.published = map[string]server.Exposure{}
	}
	r.published[e.Name] = e
	return e.Name + ".tunnel.svc", nil
}

func (r *recorder) Unpublish(_ context.Context, name, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.published, name)
	return nil
}

func (r *recorder) get(name string) (server.Exposure, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.published[name]
	return e, ok
}

// syncBuffer is a bytes.Buffer safe to read while Run writes to it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startServer(t *testing.T, pub server.Publisher) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		server.New(server.Config{Replica: "tunnel-server-0", Publisher: pub, Grace: time.Minute}).Serve(ctx, ln)
		close(done)
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

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

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// ask sends one line to addr and returns the reply, or an error.
func ask(addr, msg string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintln(conn, msg); err != nil {
		return "", err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSuffix(line, "\n"), err
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// runInBackground starts Run and returns a function that cancels it and
// reports what Run returned.
func runInBackground(t *testing.T, cfg Config) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- Run(ctx, cfg) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-result:
			case <-time.After(10 * time.Second):
				err = errors.New("Run did not return after cancel")
			}
		})
		return err
	}
	t.Cleanup(func() { stop() })
	return stop
}

func dialTo(addr *string, mu *sync.Mutex) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		mu.Lock()
		a := *addr
		mu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, "tcp", a)
	}
}

func TestRunExposesForwardsAndCleansUpOnExit(t *testing.T) {
	pub := &recorder{}
	serverAddr, _ := startServer(t, pub)
	clusterTarget, _ := echoServer(t, "cluster-c")
	_, localPort := echoServer(t, "local-b")
	forwardAddr := freeAddr(t)
	var mu sync.Mutex
	out := &syncBuffer{}

	stop := runInBackground(t, Config{
		User: "ash", Session: "s1",
		Transport: dialTo(&serverAddr, &mu),
		Exposures: []Exposure{{Name: "b-ash", Ports: []tunnel.Port{{Name: "http-8080", Port: 8080, LocalPort: localPort, AppProtocol: "http"}}}},
		Forwards:  []Forward{{Local: forwardAddr, Target: clusterTarget}},
		Out:       out,
	})

	// Outbound: the local forward reaches the cluster address.
	eventually(t, "forward to work", func() bool {
		got, err := ask(forwardAddr, "hi")
		return err == nil && got == "cluster-c:hi"
	})
	// Inbound: the published port reaches the local service.
	eventually(t, "exposure to be published", func() bool { _, ok := pub.get("b-ash"); return ok })
	exp, _ := pub.get("b-ash")
	if exp.User != "ash" || exp.Session != "s1" || exp.Ports[0].Port != 8080 || exp.Ports[0].AppProtocol != "http" {
		t.Errorf("published = %+v", exp)
	}
	inbound := net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort))
	if got, err := ask(inbound, "yo"); err != nil || got != "local-b:yo" {
		t.Errorf("inbound got %q, err %v", got, err)
	}

	if err := stop(); err != nil {
		t.Fatalf("Run returned %v on a clean stop", err)
	}
	// A clean exit removes the exposure at once and frees the local port.
	if _, ok := pub.get("b-ash"); ok {
		t.Error("exposure still published after a clean exit")
	}
	if _, err := ask(forwardAddr, "hi"); err == nil {
		t.Error("forward still listening after exit")
	}
	for _, want := range []string{
		"connected to tunnel-server-0 as ash (session s1)",
		"exposed   b-ash.tunnel.svc:8080 -> localhost:" + strconv.Itoa(localPort),
		"forward   " + forwardAddr + " -> " + clusterTarget,
		"disconnected",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status output is missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunReconnectsAndRestoresExposures(t *testing.T) {
	pubA, pubB := &recorder{}, &recorder{}
	serverAddr, stopA := startServer(t, pubA)
	clusterTarget, _ := echoServer(t, "cluster-c")
	_, localPort := echoServer(t, "local-b")
	forwardAddr := freeAddr(t)
	var mu sync.Mutex
	out := &syncBuffer{}

	stop := runInBackground(t, Config{
		User: "ash", Session: "s1",
		Transport:  dialTo(&serverAddr, &mu),
		Exposures:  []Exposure{{Name: "b-ash", Ports: []tunnel.Port{{Port: 8080, LocalPort: localPort}}}},
		Forwards:   []Forward{{Local: forwardAddr, Target: clusterTarget}},
		Out:        out,
		MinBackoff: 20 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
	})
	eventually(t, "first connection", func() bool { _, ok := pubA.get("b-ash"); return ok })

	// The replica goes away; a different one comes up elsewhere.
	stopA()
	// While disconnected, the forward must refuse quickly, not hang.
	if got, err := ask(forwardAddr, "hi"); err == nil {
		t.Errorf("forward answered %q while the tunnel was down", got)
	}
	addrB, _ := startServer(t, pubB)
	mu.Lock()
	serverAddr = addrB
	mu.Unlock()

	eventually(t, "exposure on the new replica", func() bool { _, ok := pubB.get("b-ash"); return ok })
	eventually(t, "forward to work again", func() bool {
		got, err := ask(forwardAddr, "back")
		return err == nil && got == "cluster-c:back"
	})
	exp, _ := pubB.get("b-ash")
	inbound := net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort))
	if got, err := ask(inbound, "yo"); err != nil || got != "local-b:yo" {
		t.Errorf("inbound after reconnect got %q, err %v", got, err)
	}
	if !strings.Contains(out.String(), "connection lost") {
		t.Errorf("status output never reported the drop:\n%s", out.String())
	}
	if err := stop(); err != nil {
		t.Errorf("Run returned %v", err)
	}
}

func TestRunKeepsRetryingWhileServerIsDown(t *testing.T) {
	pub := &recorder{}
	serverAddr, stopServer := startServer(t, pub)
	var mu sync.Mutex
	var attempts int
	transport := func(ctx context.Context) (net.Conn, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return dialTo(&serverAddr, &mu)(ctx)
	}
	out := &syncBuffer{}
	stop := runInBackground(t, Config{
		User: "ash", Session: "s1", Transport: transport, Out: out,
		Exposures:  []Exposure{{Name: "b-ash", Ports: []tunnel.Port{{Port: 8080}}}},
		MinBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond,
	})
	eventually(t, "first connection", func() bool { _, ok := pub.get("b-ash"); return ok })

	stopServer()
	// After the first success, failures are retried rather than fatal.
	eventually(t, "several reconnect attempts", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 5
	})
	if !strings.Contains(out.String(), "reconnect failed") {
		t.Errorf("status output never reported a failed reconnect:\n%s", out.String())
	}
	if err := stop(); err != nil {
		t.Errorf("Run returned %v after cancel during retries", err)
	}
}

func TestRunFailsFastWhenFirstConnectionFails(t *testing.T) {
	boom := errors.New("no ready tunnel-server pod")
	err := Run(context.Background(), Config{
		User: "ash", Session: "s1",
		Transport: func(context.Context) (net.Conn, error) { return nil, boom },
		Exposures: []Exposure{{Name: "b-ash", Ports: []tunnel.Port{{Port: 8080}}}},
	})
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the transport error", err)
	}
}

func TestRunFailsFastWhenServerRefusesExposure(t *testing.T) {
	serverAddr, _ := startServer(t, &recorder{})
	var mu sync.Mutex
	err := Run(context.Background(), Config{
		User: "ash", Session: "s1", Transport: dialTo(&serverAddr, &mu),
		Exposures: []Exposure{{Name: "Not_A_Valid_Name", Ports: []tunnel.Port{{Port: 8080}}}},
	})
	if err == nil || !strings.Contains(err.Error(), "expose Not_A_Valid_Name") {
		t.Errorf("err = %v, want an expose error naming the exposure", err)
	}
}

func TestRunFailsWhenForwardPortIsTaken(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	free := freeAddr(t)
	called := false

	err = Run(context.Background(), Config{
		User: "ash", Session: "s1",
		Transport: func(context.Context) (net.Conn, error) { called = true; return nil, errors.New("unused") },
		Forwards: []Forward{
			{Local: free, Target: "a.ns.svc:80"},
			{Local: taken.Addr().String(), Target: "b.ns.svc:80"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), taken.Addr().String()) {
		t.Fatalf("err = %v, want an error naming the taken address", err)
	}
	if called {
		t.Error("connected to the server although a forward could not start")
	}
	// The forward that did bind must have been released again.
	ln, err := net.Listen("tcp", free)
	if err != nil {
		t.Errorf("first forward's port was left bound: %v", err)
	} else {
		ln.Close()
	}
}
