package protocol

import (
	"io"
	"net"
	"sync"
	"time"

	yamux "github.com/libp2p/go-yamux/v5"
)

// Session multiplexes many streams over one connection. Either side can
// open streams.
type Session = yamux.Session

// Stream is one logical connection inside a Session.
type Stream = yamux.Stream

// HandshakeTimeout bounds how long a peer may take to send or answer the
// first message on a stream.
const HandshakeTimeout = 15 * time.Second

func config() *yamux.Config {
	c := yamux.DefaultConfig()
	c.EnableKeepAlive = true
	c.KeepAliveInterval = 15 * time.Second
	c.ConnectionWriteTimeout = 20 * time.Second
	c.LogOutput = io.Discard
	return c
}

// NewServer starts the server side of a session on conn.
func NewServer(conn net.Conn) (*Session, error) {
	return yamux.Server(conn, config(), nil)
}

// NewClient starts the client side of a session on conn.
func NewClient(conn net.Conn) (*Session, error) {
	return yamux.Client(conn, config(), nil)
}

type closeWriter interface {
	CloseWrite() error
}

// Pipe copies bytes both ways between a and b until both directions end,
// then closes both. A clean end of one direction is passed on as a
// half-close so protocols that rely on it keep working.
func Pipe(a, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)
	oneWay := func(dst, src io.ReadWriteCloser) {
		defer wg.Done()
		if _, err := io.Copy(dst, src); err != nil {
			// A broken direction means the connection is gone; tear down
			// both sides so the other copy does not wait forever.
			a.Close()
			b.Close()
			return
		}
		if cw, ok := dst.(closeWriter); ok {
			cw.CloseWrite()
		} else {
			dst.Close()
		}
	}
	go oneWay(a, b)
	go oneWay(b, a)
	wg.Wait()
	a.Close()
	b.Close()
}
