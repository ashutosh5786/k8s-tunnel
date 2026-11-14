package daemon

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

// The control channel lets later commands (expose, unexpose, ...) talk to
// the background process. It is a TCP listener on the loopback interface
// that accepts one JSON request per connection. Loopback is reachable by
// every local user, so each request must carry the secret token that the
// background process stored in its state file, which only its owner can
// read.

type controlRequest struct {
	Token   string   `json:"token"`
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
}

type controlResponse struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Output string `json:"output,omitempty"`
}

// Handler carries out one command and returns text to show the user.
type Handler func(ctx context.Context, command string, args []string) (string, error)

// NewToken returns a random secret for the control channel.
func NewToken() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// ListenControl opens the control listener on a free loopback port.
func ListenControl() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

// maxControlLine bounds a request so a stray connection cannot make the
// background process read without limit.
const maxControlLine = 1 << 20

// ServeControl answers control requests until ctx is cancelled. It closes
// the listener on return.
func ServeControl(ctx context.Context, ln net.Listener, token string, handle Handler) {
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go serveOne(ctx, conn, token, handle)
	}
}

func serveOne(ctx context.Context, conn net.Conn, token string, handle Handler) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Minute))
	reply := func(r controlResponse) { json.NewEncoder(conn).Encode(r) }

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 4096), maxControlLine)
	if !sc.Scan() {
		return
	}
	var req controlRequest
	if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
		reply(controlResponse{Error: "malformed request"})
		return
	}
	// Compared in constant time so the token cannot be guessed a
	// character at a time by measuring how long the refusal takes.
	if subtle.ConstantTimeCompare([]byte(req.Token), []byte(token)) != 1 {
		reply(controlResponse{Error: "not authorised"})
		return
	}
	out, err := handle(ctx, req.Command, req.Args)
	if err != nil {
		reply(controlResponse{Error: err.Error(), Output: out})
		return
	}
	reply(controlResponse{OK: true, Output: out})
}

// Call sends one command to the background process and returns its
// output. Output may be non-empty even when an error is returned, when
// part of the command succeeded.
func Call(addr, token, command string, args []string, timeout time.Duration) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("the background connection is not answering: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	if err := json.NewEncoder(conn).Encode(controlRequest{Token: token, Command: command, Args: args}); err != nil {
		return "", err
	}
	var resp controlResponse
	dec := json.NewDecoder(bufio.NewReader(conn))
	if err := dec.Decode(&resp); err != nil {
		return "", fmt.Errorf("no answer from the background connection: %w", err)
	}
	if !resp.OK {
		if resp.Error == "" {
			resp.Error = "command failed"
		}
		return resp.Output, errors.New(resp.Error)
	}
	return resp.Output, nil
}
