package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// startControl serves handle on a fresh control channel.
func startControl(t *testing.T, handle Handler) (addr, token string, stop context.CancelFunc) {
	t.Helper()
	ln, err := ListenControl()
	if err != nil {
		t.Fatal(err)
	}
	token, err = NewToken()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ServeControl(ctx, ln, token, handle); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String(), token, cancel
}

func echoHandler(_ context.Context, command string, args []string) (string, error) {
	return fmt.Sprintf("%s:%s\n", command, strings.Join(args, ",")), nil
}

func TestControlRoundTrip(t *testing.T) {
	addr, token, _ := startControl(t, echoHandler)
	out, err := Call(addr, token, "expose", []string{"b-ash:8080", "c-ash:9090"}, 5*time.Second)
	if err != nil || out != "expose:b-ash:8080,c-ash:9090\n" {
		t.Errorf("out = %q, err = %v", out, err)
	}
	// Each call is its own connection, so calls can follow each other.
	out, err = Call(addr, token, "list", nil, 5*time.Second)
	if err != nil || out != "list:\n" {
		t.Errorf("second call: out = %q, err = %v", out, err)
	}
}

// Any local user can reach a loopback port, so the token is what keeps
// other people from driving someone's connection.
func TestControlRejectsWrongToken(t *testing.T) {
	called := false
	addr, token, _ := startControl(t, func(context.Context, string, []string) (string, error) {
		called = true
		return "", nil
	})
	for name, bad := range map[string]string{
		"empty":      "",
		"other":      "0000",
		"prefix":     token[:len(token)-1],
		"with extra": token + "0",
	} {
		_, err := Call(addr, bad, "unexpose", []string{"b-ash"}, 5*time.Second)
		if err == nil || !strings.Contains(err.Error(), "not authorised") {
			t.Errorf("%s token: err = %v", name, err)
		}
	}
	if called {
		t.Error("the handler ran for an unauthorised request")
	}
}

func TestControlPassesHandlerErrorsAndPartialOutput(t *testing.T) {
	addr, token, _ := startControl(t, func(context.Context, string, []string) (string, error) {
		return "exposed   a\n", errors.New("expose b: name in use")
	})
	out, err := Call(addr, token, "expose", nil, 5*time.Second)
	if err == nil || err.Error() != "expose b: name in use" {
		t.Errorf("err = %v", err)
	}
	// What succeeded before the failure must still be shown.
	if out != "exposed   a\n" {
		t.Errorf("out = %q", out)
	}
}

func TestControlIgnoresGarbage(t *testing.T) {
	addr, token, _ := startControl(t, echoHandler)
	for _, junk := range []string{"not json\n", "\n", "{\"token\":\n"} {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		conn.Write([]byte(junk))
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 256)
		n, _ := conn.Read(buf)
		conn.Close()
		if strings.Contains(string(buf[:n]), `"ok":true`) {
			t.Errorf("garbage %q was accepted: %s", junk, buf[:n])
		}
	}
	// A client that connects and says nothing must not block others.
	idle, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	if _, err := Call(addr, token, "list", nil, 5*time.Second); err != nil {
		t.Errorf("channel unusable while another client idles: %v", err)
	}
}

func TestCallFailsClearlyWhenNothingListens(t *testing.T) {
	addr, token, stop := startControl(t, echoHandler)
	stop()
	// Give the listener a moment to close.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := Call(addr, token, "list", nil, time.Second)
		if err != nil {
			if !strings.Contains(err.Error(), "not answering") && !strings.Contains(err.Error(), "no answer") {
				t.Errorf("err = %v", err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("control channel still answering after it was stopped")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTokensAreLongAndDistinct(t *testing.T) {
	a, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewToken()
	if len(a) < 32 || a == b {
		t.Errorf("tokens %q and %q", a, b)
	}
}

func TestStateKeepsControlDetails(t *testing.T) {
	d := Dir(t.TempDir())
	want := State{PID: 1, Session: "s", Control: "127.0.0.1:5555", Token: "secret"}
	if err := d.Write(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := d.Read()
	if err != nil || !ok || got.Control != want.Control || got.Token != want.Token {
		t.Errorf("got %+v, ok %v, err %v", got, ok, err)
	}
}
