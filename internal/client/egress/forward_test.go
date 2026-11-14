package egress

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func echoServer(t *testing.T, prefix string) string {
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
	return ln.Addr().String()
}

// startForward serves a Forward on a free local port.
func startForward(t *testing.T, target string, dial Dialer) (addr string, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	f := &Forward{Listener: ln, Target: target, Dial: dial}
	go func() { f.Serve(ctx); close(finished) }()
	t.Cleanup(cancel)
	return ln.Addr().String(), cancel, finished
}

func directDial(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func TestForwardRelaysToTarget(t *testing.T) {
	target := echoServer(t, "c")
	var asked atomic.Value
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		asked.Store(network + " " + address)
		return directDial(ctx, network, address)
	}
	addr, _, _ := startForward(t, target, dial)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintln(conn, "hello")
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || line != "c:hello\n" {
		t.Fatalf("got %q, err %v", line, err)
	}
	// The dialer must be asked for the configured target, not the local
	// address the application connected to.
	if got := asked.Load(); got != "tcp "+target {
		t.Errorf("dialer was asked for %q, want %q", got, "tcp "+target)
	}
}

func TestForwardClosesLocalConnectionWhenDialFails(t *testing.T) {
	dial := func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("tunnel is not connected")
	}
	addr, _, _ := startForward(t, "c.ns.svc:80", dial)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The application must see a closed connection, not a hang.
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = io.Copy(io.Discard, conn)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection hung after a failed dial")
	}

	// One failure must not stop the forward from accepting more.
	if c2, err := net.DialTimeout("tcp", addr, time.Second); err != nil {
		t.Errorf("forward stopped accepting after a failed dial: %v", err)
	} else {
		c2.Close()
	}
}

func TestCancelStopsListenerAndActiveConnections(t *testing.T) {
	target := echoServer(t, "c")
	addr, cancel, done := startForward(t, target, directDial)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintln(conn, "x")
	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	// The connection that was open must be cut.
	_, err = io.Copy(io.Discard, conn)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Error("active connection survived cancel")
	}
	// And the local port must be released.
	if c2, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c2.Close()
		t.Error("listener still accepting after cancel")
	}
}
