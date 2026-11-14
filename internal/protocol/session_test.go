package protocol

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// tcpPair returns the two ends of a real TCP connection.
func tcpPair(t *testing.T) (client, server *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	s := <-accepted
	if s == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	return c.(*net.TCPConn), s.(*net.TCPConn)
}

func sessionPair(t *testing.T) (client, server *Session) {
	t.Helper()
	c, s := tcpPair(t)
	server, err := NewServer(s)
	if err != nil {
		t.Fatal(err)
	}
	client, err = NewClient(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func within(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out: %s", what)
	}
}

func TestEitherSideCanOpenStreams(t *testing.T) {
	client, server := sessionPair(t)
	ctx := context.Background()

	for name, pair := range map[string][2]*Session{
		"client opens": {client, server},
		"server opens": {server, client},
	} {
		opener, accepter := pair[0], pair[1]
		within(t, name, func() {
			go func() {
				st, err := accepter.AcceptStream()
				if err != nil {
					return
				}
				var req Request
				if ReadMsg(st, &req) == nil {
					WriteMsg(st, &Response{OK: true, Hello: &HelloReply{Replica: req.Hello.User}})
				}
				st.Close()
			}()
			st, err := opener.OpenStream(ctx)
			if err != nil {
				t.Errorf("%s: %v", name, err)
				return
			}
			defer st.Close()
			var resp Response
			if err := WriteMsg(st, &Request{Type: TypeHello, Hello: &Hello{User: name}}); err != nil {
				t.Errorf("%s: write: %v", name, err)
			}
			if err := ReadMsg(st, &resp); err != nil || resp.Hello.Replica != name {
				t.Errorf("%s: resp = %+v, err = %v", name, resp, err)
			}
		})
	}
}

func TestClosingSessionEndsPeer(t *testing.T) {
	client, server := sessionPair(t)
	client.Close()
	within(t, "peer notices close", func() { <-server.CloseChan() })
}

// TestPipeCarriesHalfClose joins a TCP connection to a stream and checks
// that "I am done sending" travels through while the reply still comes back.
func TestPipeCarriesHalfClose(t *testing.T) {
	client, server := sessionPair(t)
	app, appFar := tcpPair(t)       // app <-> pipe on the client side
	target, targetFar := tcpPair(t) // pipe on the server side <-> target

	go func() {
		st, err := server.AcceptStream()
		if err != nil {
			return
		}
		Pipe(st, target)
	}()
	go func() {
		st, err := client.OpenStream(context.Background())
		if err != nil {
			return
		}
		Pipe(appFar, st)
	}()
	// The target reads everything until end-of-input, then answers.
	go func() {
		body, _ := io.ReadAll(targetFar)
		fmt.Fprintf(targetFar, "got %d bytes", len(body))
		targetFar.Close()
	}()

	within(t, "half-close round trip", func() {
		fmt.Fprint(app, "hello")
		app.CloseWrite()
		reply, err := io.ReadAll(app)
		if err != nil || string(reply) != "got 5 bytes" {
			t.Errorf("reply = %q, err = %v", reply, err)
		}
	})
}

func TestPipeReturnsAndClosesBothWhenOneSideCloses(t *testing.T) {
	a, aFar := tcpPair(t)
	b, bFar := tcpPair(t)
	done := make(chan struct{})
	go func() { Pipe(aFar, b); close(done) }()

	fmt.Fprint(a, "x")
	buf := make([]byte, 1)
	if _, err := io.ReadFull(bFar, buf); err != nil || buf[0] != 'x' {
		t.Fatalf("data did not cross the pipe: %v", err)
	}
	a.Close()
	bFar.Close()
	within(t, "Pipe returns", func() { <-done })

	// Pipe must have closed its own ends, so using them now fails.
	if _, err := aFar.Write([]byte("y")); err == nil {
		t.Error("first connection still open after Pipe returned")
	}
	if _, err := b.Write([]byte("y")); err == nil {
		t.Error("second connection still open after Pipe returned")
	}
}

func TestPipeTearsDownOnBrokenConnection(t *testing.T) {
	a, aFar := tcpPair(t)
	b, bFar := tcpPair(t)
	done := make(chan struct{})
	go func() { Pipe(aFar, b); close(done) }()

	// An abrupt reset on one side must not leave the other side hanging.
	a.SetLinger(0)
	a.Close()
	within(t, "far side sees the teardown", func() {
		bFar.SetReadDeadline(time.Now().Add(5 * time.Second))
		io.Copy(io.Discard, bFar)
	})
	bFar.Close()
	within(t, "Pipe returns", func() { <-done })
}
