package main

import (
	"context"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/connect"
	"github.com/ashutosh5786/k8s-tunnel/internal/client/daemon"
	"github.com/ashutosh5786/k8s-tunnel/internal/server"
)

// fakeBackground stands in for the background process inside this test:
// a real session against a real server, with a real control channel, and
// a state file that points at them. The commands under test then behave
// exactly as they would against a detached process.
func fakeBackground(t *testing.T) {
	t.Helper()
	dir := isolate(t)

	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	go func() { server.New(server.Config{Replica: "local"}).Serve(ctx, serverLn); close(serverDone) }()

	sess := connect.New(connect.Config{
		User: "ash", Session: "s1",
		Transport: func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", serverLn.Addr().String())
		},
	})
	sessDone := make(chan struct{})
	go func() { sess.Run(ctx); close(sessDone) }()

	ln, err := daemon.ListenControl()
	if err != nil {
		t.Fatal(err)
	}
	token, _ := daemon.NewToken()
	controlDone := make(chan struct{})
	go func() { daemon.ServeControl(ctx, ln, token, controlHandler(sess)); close(controlDone) }()

	t.Cleanup(func() { cancel(); <-controlDone; <-sessDone; <-serverDone })
	deadline := time.Now().Add(10 * time.Second)
	for !sess.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("session never connected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := dir.Write(daemon.State{
		PID: os.Getpid(), Context: "dev", Session: "s1", Started: time.Now().UTC(),
		Control: ln.Addr().String(), Token: token,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExposeAndUnexposeCommands(t *testing.T) {
	fakeBackground(t)
	_, localPort := echo(t, "local-b")

	out, err := runCmd(t, "status")
	if err != nil || !strings.Contains(out, "Nothing exposed") {
		t.Errorf("status before: %q, err %v", out, err)
	}

	out, err = runCmd(t, "expose", "b-ash:8080:"+strconv.Itoa(localPort)+"/http", "b-ash:9090:"+strconv.Itoa(localPort))
	if err != nil {
		t.Fatalf("expose: %v\n%s", err, out)
	}
	m := regexp.MustCompile(`exposed   server port (\d+) -> localhost:`+strconv.Itoa(localPort)).FindAllStringSubmatch(out, -1)
	if len(m) != 2 {
		t.Fatalf("expose output:\n%s", out)
	}
	// The printed port is where the cluster's traffic arrives.
	if got, err := ask(net.JoinHostPort("127.0.0.1", m[0][1]), "hi"); err != nil || got != "local-b:hi" {
		t.Errorf("inbound got %q, err %v", got, err)
	}

	out, err = runCmd(t, "status")
	if err != nil || !strings.Contains(out, "Exposed:") || strings.Count(out, "-> localhost:"+strconv.Itoa(localPort)) < 2 {
		t.Errorf("status while exposed:\n%s (err %v)", out, err)
	}

	out, err = runCmd(t, "unexpose", "b-ash")
	if err != nil || !strings.Contains(out, "unexposed b-ash") {
		t.Errorf("unexpose: %q, err %v", out, err)
	}
	if _, err := ask(net.JoinHostPort("127.0.0.1", m[0][1]), "hi"); err == nil {
		t.Error("port still answering after unexpose")
	}
	if _, err := runCmd(t, "unexpose", "b-ash"); err == nil || !strings.Contains(err.Error(), "not exposed") {
		t.Errorf("unexposing twice: err = %v", err)
	}
}

func TestExposeReportsServerRefusalAndKeepsEarlierOnes(t *testing.T) {
	fakeBackground(t)
	// The second name is refused by the server; the first must stay.
	out, err := runCmd(t, "expose", "good-ash:8080", "Bad_Name:9090")
	if err == nil || !strings.Contains(err.Error(), "Bad_Name") {
		t.Errorf("err = %v", err)
	}
	if !strings.Contains(out, "exposed   server port") {
		t.Errorf("the successful exposure was not reported:\n%s", out)
	}
	status, _ := runCmd(t, "status")
	if !strings.Contains(status, "Exposed:") {
		t.Errorf("status:\n%s", status)
	}
}

func TestExposeNeedsAConnection(t *testing.T) {
	isolate(t)
	for _, args := range [][]string{{"expose", "b-ash:8080"}, {"unexpose", "b-ash"}} {
		_, err := runCmd(t, args...)
		if err == nil || !strings.Contains(err.Error(), "ashctl connect") {
			t.Errorf("%v: err = %v", args, err)
		}
	}
}

func TestExposeChecksArgumentsBeforeAnythingElse(t *testing.T) {
	isolate(t)
	for name, args := range map[string][]string{
		"expose nothing":   {"expose"},
		"unexpose nothing": {"unexpose"},
		"expose garbage":   {"expose", "oops"},
	} {
		_, err := runCmd(t, args...)
		if err == nil || strings.Contains(err.Error(), "not connected") {
			t.Errorf("%s: err = %v, want a complaint about the arguments", name, err)
		}
	}
}

func TestExposeAgainstAnOlderBackgroundProcess(t *testing.T) {
	dir := isolate(t)
	// A state file with no control details, as an older version wrote.
	if err := dir.Write(daemon.State{PID: os.Getpid(), Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	_, err := runCmd(t, "expose", "b-ash:8080")
	if err == nil || !strings.Contains(err.Error(), "disconnect and connect again") {
		t.Errorf("err = %v", err)
	}
}
