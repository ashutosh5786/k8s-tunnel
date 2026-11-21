// Package tunnel is the public API for embedding k8s-tunnel in other
// programs, such as a sandbox controller.
//
// A Client runs over any net.Conn to a tunnel server (normally a
// port-forward to the server pod). It can dial addresses inside the
// cluster and expose local ports so the cluster can reach them.
package tunnel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/protocol"
)

// Options configures a Client.
type Options struct {
	// User names the person or system behind the session.
	User string
	// Session identifies this session. Reuse the same value when
	// reconnecting so the server treats it as the same session. It must be
	// a lowercase DNS label.
	Session string
	// LocalHost is where exposed ports are delivered. Defaults to
	// 127.0.0.1.
	LocalHost string
	Log       *slog.Logger
}

// Port describes one port of an exposure.
type Port struct {
	// Name is the Service port name, e.g. "http" or "grpc".
	Name string
	// Port is the port the cluster uses.
	Port int
	// LocalPort is the port on this machine. Zero means same as Port.
	LocalPort int
	// AppProtocol is set on the Service port for mesh protocol selection.
	AppProtocol string
}

// ExposedPort reports where a port ended up.
type ExposedPort struct {
	Name       string
	Port       int
	LocalPort  int
	ServerPort int
}

// Exposure is the result of Expose.
type Exposure struct {
	Name string
	// Host is the cluster DNS name of the exposure. Empty when the server
	// does not publish Services.
	Host  string
	Ports []ExposedPort
}

// Client is one tunnel session.
type Client struct {
	mux       *protocol.Session
	log       *slog.Logger
	localHost string
	replica   string

	mu      sync.Mutex
	exposed map[string]map[int]bool // exposure name -> allowed local ports
}

// Connect performs the handshake over conn and returns a ready Client.
// The Client owns conn from here on.
func Connect(ctx context.Context, conn net.Conn, opts Options) (*Client, error) {
	mux, err := protocol.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &Client{
		mux:       mux,
		log:       opts.Log,
		localHost: opts.LocalHost,
		exposed:   map[string]map[int]bool{},
	}
	if c.log == nil {
		c.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if c.localHost == "" {
		c.localHost = "127.0.0.1"
	}

	resp, err := c.call(ctx, &protocol.Request{Type: protocol.TypeHello, Hello: &protocol.Hello{
		Version: protocol.Version,
		User:    opts.User,
		Session: opts.Session,
	}})
	if err != nil {
		mux.Close()
		return nil, fmt.Errorf("tunnel handshake: %w", err)
	}
	if resp.Hello != nil {
		c.replica = resp.Hello.Replica
	}
	go c.acceptLoop()
	return c, nil
}

// Replica returns the name of the server replica this session reached.
func (c *Client) Replica() string { return c.replica }

// Done is closed when the session ends for any reason.
func (c *Client) Done() <-chan struct{} { return c.mux.CloseChan() }

// Close ends the session. The server keeps exposures published for its
// grace period in case the session reconnects.
func (c *Client) Close() error { return c.mux.Close() }

// open starts a stream, sends req and reads the response. On success the
// stream is returned with no deadline set.
func (c *Client) open(ctx context.Context, req *protocol.Request) (*protocol.Stream, *protocol.Response, error) {
	st, err := c.mux.OpenStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	deadline := time.Now().Add(protocol.HandshakeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	st.SetDeadline(deadline)
	// Deadlines alone do not honour cancellation, so reset on cancel.
	stop := context.AfterFunc(ctx, func() { st.Reset() })
	var resp protocol.Response
	err = protocol.WriteMsg(st, req)
	if err == nil {
		err = protocol.ReadMsg(st, &resp)
	}
	if !stop() && err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		st.Reset()
		if cerr := ctx.Err(); cerr != nil {
			err = cerr
		}
		return nil, nil, err
	}
	st.SetDeadline(time.Time{})
	return st, &resp, nil
}

// call is open for requests that carry no data after the response.
func (c *Client) call(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	st, resp, err := c.open(ctx, req)
	if err != nil {
		return nil, err
	}
	st.Close()
	return resp, nil
}

// DialContext connects to a cluster address as if from inside the cluster.
// Only TCP is supported. The signature matches net.Dialer.DialContext.
func (c *Client) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("tunnel: unsupported network %q", network)
	}
	st, _, err := c.open(ctx, &protocol.Request{Type: protocol.TypeDial, Dial: &protocol.Dial{Address: address}})
	if err != nil {
		return nil, err
	}
	return st, nil
}

// Expose makes local ports reachable in the cluster under name. Calling it
// again with the same name replaces the earlier exposure.
func (c *Client) Expose(ctx context.Context, name string, ports []Port) (*Exposure, error) {
	if len(ports) == 0 {
		return nil, errors.New("tunnel: expose needs at least one port")
	}
	req := &protocol.Expose{Name: name}
	allowed := map[int]bool{}
	for _, p := range ports {
		local := p.LocalPort
		if local == 0 {
			local = p.Port
		}
		allowed[local] = true
		req.Ports = append(req.Ports, protocol.PortSpec{Name: p.Name, Port: p.Port, LocalPort: local, AppProtocol: p.AppProtocol})
	}

	// Register before asking, so a connection that arrives the instant the
	// server starts listening is not refused.
	c.mu.Lock()
	prev, had := c.exposed[name]
	c.exposed[name] = allowed
	c.mu.Unlock()

	resp, err := c.call(ctx, &protocol.Request{Type: protocol.TypeExpose, Expose: req})
	if err != nil {
		c.mu.Lock()
		if had {
			c.exposed[name] = prev
		} else {
			delete(c.exposed, name)
		}
		c.mu.Unlock()
		return nil, err
	}
	out := &Exposure{Name: name}
	if r := resp.Expose; r != nil {
		out.Host = r.Host
		for _, p := range r.Ports {
			out.Ports = append(out.Ports, ExposedPort{Name: p.Name, Port: p.Port, LocalPort: p.LocalPort, ServerPort: p.ServerPort})
		}
	}
	return out, nil
}

// Service is a cluster Service and its TCP ports.
type Service struct {
	Namespace string
	Name      string
	Ports     []int
}

// Services lists the cluster's Services as the server sees them, limited
// to the given namespaces if any. It fails if the server has discovery
// turned off.
func (c *Client) Services(ctx context.Context, namespaces ...string) ([]Service, error) {
	st, _, err := c.open(ctx, &protocol.Request{Type: protocol.TypeServices, Services: &protocol.Services{Namespaces: namespaces}})
	if err != nil {
		return nil, err
	}
	defer st.Close()
	deadline := time.Now().Add(2 * time.Minute)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	st.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { st.Reset() })
	defer stop()

	var list []protocol.ServiceInfo
	if err := json.NewDecoder(io.LimitReader(st, protocol.MaxServicesBody)).Decode(&list); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		return nil, fmt.Errorf("tunnel: read service list: %w", err)
	}
	out := make([]Service, len(list))
	for i, s := range list {
		out[i] = Service{Namespace: s.Namespace, Name: s.Name, Ports: s.Ports}
	}
	return out, nil
}

// Unexpose removes an exposure immediately.
func (c *Client) Unexpose(ctx context.Context, name string) error {
	_, err := c.call(ctx, &protocol.Request{Type: protocol.TypeUnexpose, Unexpose: &protocol.Unexpose{Name: name}})
	if err == nil {
		c.mu.Lock()
		delete(c.exposed, name)
		c.mu.Unlock()
	}
	return err
}

func (c *Client) acceptLoop() {
	for {
		st, err := c.mux.AcceptStream()
		if err != nil {
			return
		}
		go c.handleInbound(st)
	}
}

// handleInbound connects a cluster connection to the local port it is for.
func (c *Client) handleInbound(st *protocol.Stream) {
	st.SetDeadline(time.Now().Add(protocol.HandshakeTimeout))
	var req protocol.Request
	if err := protocol.ReadMsg(st, &req); err != nil {
		st.Reset()
		return
	}
	in := req.Inbound
	if req.Type != protocol.TypeInbound || in == nil {
		protocol.WriteMsg(st, protocol.Fail("unexpected request type %q", req.Type))
		st.Close()
		return
	}
	// Only deliver to ports this client chose to expose, whatever the
	// server asks for.
	c.mu.Lock()
	ok := c.exposed[in.Exposure][in.LocalPort]
	c.mu.Unlock()
	if !ok {
		protocol.WriteMsg(st, protocol.Fail("port %d is not exposed", in.LocalPort))
		st.Close()
		return
	}

	addr := net.JoinHostPort(c.localHost, strconv.Itoa(in.LocalPort))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		c.log.Warn("local service not reachable", "exposure", in.Exposure, "addr", addr, "err", err)
		protocol.WriteMsg(st, protocol.Fail("local dial %s: %v", addr, err))
		st.Close()
		return
	}
	if err := protocol.WriteMsg(st, &protocol.Response{OK: true}); err != nil {
		conn.Close()
		st.Reset()
		return
	}
	st.SetDeadline(time.Time{})
	protocol.Pipe(st, conn)
}
