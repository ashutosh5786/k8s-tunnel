package connect

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s-tunnel/pkg/tunnel"
)

// startSession runs a Session in the background and waits until it is
// connected.
func startSession(t *testing.T, cfg Config) (*Session, func() error) {
	t.Helper()
	sess := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- sess.Run(ctx) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-result:
			case <-time.After(10 * time.Second):
				t.Error("Run did not return after cancel")
			}
		})
		return err
	}
	t.Cleanup(func() { stop() })
	eventually(t, "session to connect", sess.Connected)
	return sess, stop
}

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func inboundAddr(exp *tunnel.Exposure) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(exp.Ports[0].ServerPort))
}

func TestExposeAndUnexposeOnARunningSession(t *testing.T) {
	pub := &recorder{}
	serverAddr, _ := startServer(t, pub)
	_, localPort := echoServer(t, "local-b")
	var mu sync.Mutex
	out := &syncBuffer{}
	sess, _ := startSession(t, Config{User: "ash", Session: "s1", Transport: dialTo(&serverAddr, &mu), Out: out})

	if got := sess.Exposures(); len(got) != 0 {
		t.Fatalf("a fresh session exposes %+v", got)
	}

	exp, err := sess.Expose(ctx5(t), Exposure{Name: "b-ash", Ports: []tunnel.Port{{Port: 8080, LocalPort: localPort}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := pub.get("b-ash"); !ok {
		t.Fatal("exposure was not published")
	}
	if got, err := ask(inboundAddr(exp), "hi"); err != nil || got != "local-b:hi" {
		t.Errorf("inbound got %q, err %v", got, err)
	}
	list := sess.Exposures()
	if len(list) != 1 || list[0].Name != "b-ash" || list[0].Host != "b-ash.tunnel.svc" {
		t.Errorf("exposures = %+v", list)
	}
	if !strings.Contains(out.String(), "exposed   b-ash.tunnel.svc:8080 -> localhost:"+strconv.Itoa(localPort)) {
		t.Errorf("status output:\n%s", out.String())
	}

	if err := sess.Unexpose(ctx5(t), "b-ash"); err != nil {
		t.Fatal(err)
	}
	if _, ok := pub.get("b-ash"); ok {
		t.Error("exposure still published after unexpose")
	}
	if _, err := ask(inboundAddr(exp), "hi"); err == nil {
		t.Error("exposed port still answering after unexpose")
	}
	if got := sess.Exposures(); len(got) != 0 {
		t.Errorf("exposures after unexpose = %+v", got)
	}
	if err := sess.Unexpose(ctx5(t), "b-ash"); err == nil || !strings.Contains(err.Error(), "not exposed") {
		t.Errorf("second unexpose: err = %v", err)
	}
}

func TestExposingTheSameNameAgainReplacesIt(t *testing.T) {
	serverAddr, _ := startServer(t, &recorder{})
	_, oldPort := echoServer(t, "old")
	_, newPort := echoServer(t, "new")
	var mu sync.Mutex
	sess, _ := startSession(t, Config{User: "ash", Session: "s1", Transport: dialTo(&serverAddr, &mu)})

	if _, err := sess.Expose(ctx5(t), Exposure{Name: "b-ash", Ports: []tunnel.Port{{Port: 8080, LocalPort: oldPort}}}); err != nil {
		t.Fatal(err)
	}
	exp, err := sess.Expose(ctx5(t), Exposure{Name: "b-ash", Ports: []tunnel.Port{{Port: 8080, LocalPort: newPort}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ask(inboundAddr(exp), "x"); err != nil || got != "new:x" {
		t.Errorf("got %q, err %v", got, err)
	}
	if list := sess.Exposures(); len(list) != 1 {
		t.Errorf("replacing made a second entry: %+v", list)
	}
}

// What was exposed while running must come back after a reconnect, on
// whichever replica the session lands, and what was unexposed must not.
func TestDynamicExposuresSurviveReconnect(t *testing.T) {
	pubA, pubB := &recorder{}, &recorder{}
	serverAddr, stopA := startServer(t, pubA)
	_, keepPort := echoServer(t, "keep")
	_, dropPort := echoServer(t, "drop")
	var mu sync.Mutex
	sess, _ := startSession(t, Config{
		User: "ash", Session: "s1", Transport: dialTo(&serverAddr, &mu),
		MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
	})
	for name, port := range map[string]int{"keep-ash": keepPort, "drop-ash": dropPort} {
		if _, err := sess.Expose(ctx5(t), Exposure{Name: name, Ports: []tunnel.Port{{Port: 8080, LocalPort: port}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sess.Unexpose(ctx5(t), "drop-ash"); err != nil {
		t.Fatal(err)
	}

	stopA()
	eventually(t, "session to notice the drop", func() bool { return !sess.Connected() })
	// While disconnected the exposure is still listed, as waiting.
	list := sess.Exposures()
	if len(list) != 1 || list[0].Name != "keep-ash" {
		t.Fatalf("exposures while disconnected = %+v", list)
	}
	if lines := Describe(list[0]); len(lines) != 1 || !strings.Contains(lines[0], "waiting to reconnect") {
		t.Errorf("description while disconnected = %v", lines)
	}
	// And a new one cannot be added until the connection is back.
	if _, err := sess.Expose(ctx5(t), Exposure{Name: "late", Ports: []tunnel.Port{{Port: 8080}}}); err == nil {
		t.Error("expose succeeded without a connection")
	}

	addrB, _ := startServer(t, pubB)
	mu.Lock()
	serverAddr = addrB
	mu.Unlock()
	eventually(t, "exposure to come back on the new replica", func() bool { _, ok := pubB.get("keep-ash"); return ok })
	if _, ok := pubB.get("drop-ash"); ok {
		t.Error("an unexposed name came back after reconnect")
	}
	if _, ok := pubB.get("late"); ok {
		t.Error("a refused expose was remembered")
	}
	eventually(t, "session to be connected again", sess.Connected)
	exp := sess.Exposures()[0]
	if got, err := ask(inboundAddr(exp), "x"); err != nil || got != "keep:x" {
		t.Errorf("after reconnect got %q, err %v", got, err)
	}
}

func TestRefusedExposeIsNotRemembered(t *testing.T) {
	pub := &recorder{}
	serverAddr, _ := startServer(t, pub)
	var mu sync.Mutex
	sess, _ := startSession(t, Config{User: "ash", Session: "s1", Transport: dialTo(&serverAddr, &mu)})

	if _, err := sess.Expose(ctx5(t), Exposure{Name: "Not_Valid", Ports: []tunnel.Port{{Port: 8080}}}); err == nil {
		t.Fatal("an invalid name was accepted")
	}
	if got := sess.Exposures(); len(got) != 0 {
		t.Errorf("a refused exposure is listed: %+v", got)
	}
	// The session must still be usable.
	if _, err := sess.Expose(ctx5(t), Exposure{Name: "ok-ash", Ports: []tunnel.Port{{Port: 8080}}}); err != nil {
		t.Errorf("session unusable after a refused expose: %v", err)
	}
}

func TestExitRemovesDynamicExposuresToo(t *testing.T) {
	pub := &recorder{}
	serverAddr, _ := startServer(t, pub)
	var mu sync.Mutex
	sess, stop := startSession(t, Config{
		User: "ash", Session: "s1", Transport: dialTo(&serverAddr, &mu),
		Exposures: []Exposure{{Name: "at-start", Ports: []tunnel.Port{{Port: 8080}}}},
	})
	if _, err := sess.Expose(ctx5(t), Exposure{Name: "added-later", Ports: []tunnel.Port{{Port: 9090}}}); err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"at-start", "added-later"} {
		if _, ok := pub.get(name); ok {
			t.Errorf("%s still published after exit", name)
		}
	}
}

func TestDescribe(t *testing.T) {
	published := &tunnel.Exposure{Name: "b", Host: "b.tunnel.svc", Ports: []tunnel.ExposedPort{
		{Port: 8080, LocalPort: 3000, ServerPort: 20001},
		{Port: 9090, LocalPort: 9090, ServerPort: 20002},
	}}
	want := []string{"b.tunnel.svc:8080 -> localhost:3000", "b.tunnel.svc:9090 -> localhost:9090"}
	if got := Describe(published); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("published: %v", got)
	}
	noService := &tunnel.Exposure{Name: "b", Ports: []tunnel.ExposedPort{{Port: 8080, LocalPort: 3000, ServerPort: 20001}}}
	if got := Describe(noService); len(got) != 1 || got[0] != "server port 20001 -> localhost:3000" {
		t.Errorf("without a Service: %v", got)
	}
}
