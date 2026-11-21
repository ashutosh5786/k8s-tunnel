// Package server implements the in-cluster side of the tunnel. One server
// serves many users: it dials cluster addresses on their behalf and opens a
// listening port for every local port they expose.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/protocol"
)

// Port is one port of a published exposure.
type Port struct {
	Name        string
	Port        int // port the cluster connects to
	ServerPort  int // port this server listens on for it
	AppProtocol string
}

// Exposure is a local service made reachable in the cluster.
type Exposure struct {
	Name    string
	User    string
	Session string
	Ports   []Port
}

// Publisher makes exposures addressable in the cluster, normally by
// managing one Service per exposure that points at this replica.
type Publisher interface {
	// Publish creates or updates the record for e and returns its host name.
	Publish(ctx context.Context, e Exposure) (host string, err error)
	// Unpublish removes the record if it still belongs to this session and
	// this replica. A record that has moved elsewhere is left alone.
	Unpublish(ctx context.Context, name, session string) error
}

// Sweeper is an optional Publisher extension that removes records left
// behind by a previous run of this replica.
type Sweeper interface {
	Sweep(ctx context.Context, live func(name string) bool) error
}

// ServiceInfo describes one cluster Service and its TCP ports.
type ServiceInfo = protocol.ServiceInfo

// Lister reports which Services exist in the cluster, so clients can make
// their names resolve locally without needing list rights of their own.
type Lister interface {
	// Services lists Services in the given namespaces, or in all
	// namespaces when none are given.
	Services(ctx context.Context, namespaces []string) ([]ServiceInfo, error)
}

// Config configures a Server.
type Config struct {
	// Replica is this server's identity, normally the pod name.
	Replica string
	// PortMin and PortMax bound the ports used for exposures. Zero lets
	// the operating system choose.
	PortMin, PortMax int
	// Publisher may be nil, in which case exposures are reachable only by
	// server port.
	Publisher Publisher
	// Lister may be nil, in which case clients cannot discover Services.
	Lister Lister
	// Dial connects to cluster addresses. Defaults to a plain TCP dial.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// DialTimeout bounds each cluster dial. Defaults to 10s.
	DialTimeout time.Duration
	// Grace is how long an exposure stays published after its session
	// drops, so a reconnect does not flap it. Defaults to 30s.
	Grace time.Duration
	Log   *slog.Logger
}

// Server is the shared tunnel server.
type Server struct {
	cfg   Config
	log   *slog.Logger
	ports *portAlloc

	mu       sync.Mutex
	sessions map[string]*session
	live     map[string]*exposure   // exposure name -> current owner
	timers   map[string]*time.Timer // exposure name -> pending unpublish
}

type session struct {
	id, user  string
	mux       *protocol.Session
	closed    bool                 // guarded by Server.mu
	exposures map[string]*exposure // guarded by Server.mu
}

type exposure struct {
	name      string
	session   string
	mu        sync.Mutex
	listeners []net.Listener
	closed    bool
}

func (e *exposure) add(ln net.Listener) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false
	}
	e.listeners = append(e.listeners, ln)
	return true
}

func (e *exposure) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for _, ln := range e.listeners {
		ln.Close()
	}
}

// New returns a Server with defaults filled in.
func New(cfg Config) *Server {
	if cfg.Dial == nil {
		cfg.Dial = (&net.Dialer{}).DialContext
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.Grace == 0 {
		cfg.Grace = 30 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{
		cfg:      cfg,
		log:      cfg.Log,
		ports:    newPortAlloc(cfg.PortMin, cfg.PortMax),
		sessions: map[string]*session{},
		live:     map[string]*exposure{},
		timers:   map[string]*time.Timer{},
	}
}

// Serve accepts tunnel connections on ln until ctx is cancelled. On return
// every session is closed and every exposure is unpublished.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	if sw, ok := s.cfg.Publisher.(Sweeper); ok {
		go s.sweepAfterGrace(ctx, sw)
	}

	var wg sync.WaitGroup
	var err error
	for {
		conn, aerr := ln.Accept()
		if aerr != nil {
			if ctx.Err() == nil {
				err = aerr
			}
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConn(ctx, conn)
		}()
	}
	cancel()
	s.shutdown()
	wg.Wait()
	return err
}

// sweepAfterGrace removes records from a previous run once clients have
// had time to reconnect and reclaim theirs.
func (s *Server) sweepAfterGrace(ctx context.Context, sw Sweeper) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(s.cfg.Grace):
	}
	live := func(name string) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, pending := s.timers[name]
		return s.live[name] != nil || pending
	}
	if err := sw.Sweep(ctx, live); err != nil {
		s.log.Warn("sweep failed", "err", err)
	}
}

func (s *Server) shutdown() {
	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	// Pending unpublishes die with the process. Their records carry this
	// replica's name, so the sweep on the next start removes them.
	for _, t := range s.timers {
		t.Stop()
	}
	s.timers = map[string]*time.Timer{}
	s.mu.Unlock()

	for _, sess := range sessions {
		s.closeSession(sess, true)
	}
}

var (
	sessionRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	nameRE    = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	mux, err := protocol.NewServer(conn)
	if err != nil {
		conn.Close()
		return
	}
	defer mux.Close()
	stop := context.AfterFunc(ctx, func() { mux.Close() })
	defer stop()

	sess, err := s.handshake(mux)
	if err != nil {
		// A connection that closes without sending anything is a health
		// probe from the kubelet, every few seconds. Keep those quiet.
		level := slog.LevelInfo
		if errors.Is(err, io.EOF) {
			level = slog.LevelDebug
		}
		s.log.Log(ctx, level, "handshake failed", "remote", conn.RemoteAddr().String(), "err", err)
		return
	}
	s.log.Info("session connected", "session", sess.id, "user", sess.user)
	defer func() {
		// When the server itself is stopping there will be no reconnect to
		// this replica, so unpublish at once instead of after the grace
		// period; otherwise traffic keeps being sent to a dead pod.
		s.closeSession(sess, ctx.Err() != nil)
		s.log.Info("session ended", "session", sess.id, "user", sess.user)
	}()

	for {
		st, err := mux.AcceptStream()
		if err != nil {
			return
		}
		go s.handleStream(ctx, sess, st)
	}
}

// handshake reads the Hello on the first stream and registers the session.
func (s *Server) handshake(mux *protocol.Session) (*session, error) {
	// AcceptStream has no deadline of its own, so bound it by closing the
	// session if the client never says hello.
	timer := time.AfterFunc(protocol.HandshakeTimeout, func() { mux.Close() })
	st, err := mux.AcceptStream()
	if err != nil {
		timer.Stop()
		return nil, err
	}
	defer st.Close()
	st.SetDeadline(time.Now().Add(protocol.HandshakeTimeout))

	var req protocol.Request
	err = protocol.ReadMsg(st, &req)
	timer.Stop()
	if err != nil {
		return nil, err
	}
	fail := func(format string, args ...any) (*session, error) {
		resp := protocol.Fail(format, args...)
		if protocol.WriteMsg(st, resp) == nil {
			// Returning closes the whole connection, which can overtake
			// the reply. Wait for the client to read it and close its side.
			st.CloseWrite()
			st.SetReadDeadline(time.Now().Add(2 * time.Second))
			io.Copy(io.Discard, st)
		}
		return nil, resp.Err()
	}
	h := req.Hello
	if req.Type != protocol.TypeHello || h == nil {
		return fail("first message must be hello")
	}
	if h.Version != protocol.Version {
		return fail("protocol version %d not supported, server speaks %d", h.Version, protocol.Version)
	}
	if !sessionRE.MatchString(h.Session) {
		return fail("invalid session id")
	}
	if h.User == "" || len(h.User) > 253 {
		return fail("invalid user")
	}

	sess := &session{id: h.Session, user: h.User, mux: mux, exposures: map[string]*exposure{}}
	s.mu.Lock()
	old := s.sessions[sess.id]
	s.sessions[sess.id] = sess
	s.mu.Unlock()
	if old != nil {
		// The same session reconnected before we noticed the old
		// connection was dead. The new one wins.
		old.mux.Close()
	}

	resp := &protocol.Response{OK: true, Hello: &protocol.HelloReply{Replica: s.cfg.Replica}}
	if err := protocol.WriteMsg(st, resp); err != nil {
		s.closeSession(sess, false)
		return nil, err
	}
	return sess, nil
}

// closeSession tears down a session's exposures. With now set they are
// unpublished immediately instead of after the grace period.
func (s *Server) closeSession(sess *session, now bool) {
	sess.mux.Close()
	s.mu.Lock()
	sess.closed = true
	if s.sessions[sess.id] == sess {
		delete(s.sessions, sess.id)
	}
	exps := make([]*exposure, 0, len(sess.exposures))
	for _, e := range sess.exposures {
		exps = append(exps, e)
	}
	sess.exposures = map[string]*exposure{}
	s.mu.Unlock()
	for _, e := range exps {
		s.release(e, now)
	}
}

// release closes an exposure's listeners and, if it is still the current
// owner of its name, schedules or performs the unpublish.
func (s *Server) release(e *exposure, now bool) {
	e.close()
	s.mu.Lock()
	if s.live[e.name] != e {
		// A newer exposure took over the name; its record must stay.
		s.mu.Unlock()
		return
	}
	delete(s.live, e.name)
	if now {
		s.mu.Unlock()
		s.unpublish(e.name, e.session)
		return
	}
	name, sessionID := e.name, e.session
	var t *time.Timer
	t = time.AfterFunc(s.cfg.Grace, func() {
		s.mu.Lock()
		if s.timers[name] != t {
			s.mu.Unlock()
			return
		}
		delete(s.timers, name)
		taken := s.live[name] != nil
		s.mu.Unlock()
		if !taken {
			s.unpublish(name, sessionID)
		}
	})
	s.timers[name] = t
	s.mu.Unlock()
}

func (s *Server) unpublish(name, sessionID string) {
	if s.cfg.Publisher == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := s.cfg.Publisher.Unpublish(ctx, name, sessionID); err != nil {
		s.log.Warn("unpublish failed", "exposure", name, "err", err)
	}
}

func (s *Server) handleStream(ctx context.Context, sess *session, st *protocol.Stream) {
	st.SetDeadline(time.Now().Add(protocol.HandshakeTimeout))
	var req protocol.Request
	if err := protocol.ReadMsg(st, &req); err != nil {
		st.Reset()
		return
	}
	switch {
	case req.Type == protocol.TypeDial && req.Dial != nil:
		s.handleDial(ctx, st, req.Dial)
		return
	case req.Type == protocol.TypeServices:
		s.handleServices(ctx, st, req.Services)
		return
	case req.Type == protocol.TypeExpose && req.Expose != nil:
		reply, err := s.expose(ctx, sess, req.Expose)
		if err != nil {
			protocol.WriteMsg(st, protocol.Fail("%v", err))
		} else {
			protocol.WriteMsg(st, &protocol.Response{OK: true, Expose: reply})
		}
	case req.Type == protocol.TypeUnexpose && req.Unexpose != nil:
		s.mu.Lock()
		e := sess.exposures[req.Unexpose.Name]
		delete(sess.exposures, req.Unexpose.Name)
		s.mu.Unlock()
		if e != nil {
			s.release(e, true)
		}
		protocol.WriteMsg(st, &protocol.Response{OK: true})
	default:
		protocol.WriteMsg(st, protocol.Fail("unknown request type %q", req.Type))
	}
	st.Close()
}

func (s *Server) handleDial(ctx context.Context, st *protocol.Stream, d *protocol.Dial) {
	if _, _, err := net.SplitHostPort(d.Address); err != nil {
		protocol.WriteMsg(st, protocol.Fail("invalid address %q", d.Address))
		st.Close()
		return
	}
	dctx, cancel := context.WithTimeout(ctx, s.cfg.DialTimeout)
	conn, err := s.cfg.Dial(dctx, "tcp", d.Address)
	cancel()
	if err != nil {
		protocol.WriteMsg(st, protocol.Fail("dial %s: %v", d.Address, err))
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

func (s *Server) handleServices(ctx context.Context, st *protocol.Stream, req *protocol.Services) {
	defer st.Close()
	if s.cfg.Lister == nil {
		protocol.WriteMsg(st, protocol.Fail("service discovery is not enabled on this server"))
		return
	}
	var namespaces []string
	if req != nil {
		namespaces = req.Namespaces
	}
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	list, err := s.cfg.Lister.Services(lctx, namespaces)
	cancel()
	if err != nil {
		protocol.WriteMsg(st, protocol.Fail("list services: %v", err))
		return
	}
	if list == nil {
		list = []ServiceInfo{}
	}
	if err := protocol.WriteMsg(st, &protocol.Response{OK: true}); err != nil {
		return
	}
	// A large cluster's listing takes longer to send than a handshake.
	st.SetDeadline(time.Now().Add(2 * time.Minute))
	json.NewEncoder(st).Encode(list)
}

const maxPortsPerExposure = 16

func (s *Server) expose(ctx context.Context, sess *session, req *protocol.Expose) (*protocol.ExposeReply, error) {
	if !nameRE.MatchString(req.Name) {
		return nil, fmt.Errorf("invalid exposure name %q: must be a lowercase DNS label", req.Name)
	}
	if len(req.Ports) == 0 || len(req.Ports) > maxPortsPerExposure {
		return nil, fmt.Errorf("an exposure needs between 1 and %d ports", maxPortsPerExposure)
	}
	seen := map[int]bool{}
	for i := range req.Ports {
		p := &req.Ports[i]
		if p.LocalPort == 0 {
			p.LocalPort = p.Port
		}
		if p.Port < 1 || p.Port > 65535 || p.LocalPort < 1 || p.LocalPort > 65535 {
			return nil, fmt.Errorf("invalid port %d", p.Port)
		}
		if seen[p.Port] {
			return nil, fmt.Errorf("port %d listed twice", p.Port)
		}
		seen[p.Port] = true
	}

	// Claim the name first so concurrent requests cannot both own it.
	e := &exposure{name: req.Name, session: sess.id}
	s.mu.Lock()
	if sess.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("session closed")
	}
	old := s.live[req.Name]
	if old != nil && old.session != sess.id {
		s.mu.Unlock()
		return nil, fmt.Errorf("exposure name %q is in use by another session", req.Name)
	}
	s.live[req.Name] = e
	sess.exposures[req.Name] = e
	if t := s.timers[req.Name]; t != nil {
		t.Stop()
		delete(s.timers, req.Name)
	}
	s.mu.Unlock()
	if old != nil {
		old.close()
	}

	// abandon undoes the claim. The published record is left as it was;
	// if it is stale the grace timer or the sweep removes it.
	abandon := func() {
		e.close()
		s.mu.Lock()
		if s.live[req.Name] == e {
			delete(s.live, req.Name)
		}
		if sess.exposures[req.Name] == e {
			delete(sess.exposures, req.Name)
		}
		s.mu.Unlock()
	}

	pub := Exposure{Name: req.Name, User: sess.user, Session: sess.id}
	reply := &protocol.ExposeReply{Name: req.Name}
	type bound struct {
		ln        net.Listener
		localPort int
	}
	var listeners []bound
	for _, p := range req.Ports {
		ln, port, err := s.ports.listen()
		if err != nil {
			abandon()
			return nil, err
		}
		if !e.add(ln) {
			ln.Close()
			abandon()
			return nil, fmt.Errorf("exposure %q was replaced", req.Name)
		}
		listeners = append(listeners, bound{ln, p.LocalPort})
		pub.Ports = append(pub.Ports, Port{Name: p.Name, Port: p.Port, ServerPort: port, AppProtocol: p.AppProtocol})
		reply.Ports = append(reply.Ports, protocol.ExposedPort{Name: p.Name, Port: p.Port, LocalPort: p.LocalPort, ServerPort: port})
	}

	if s.cfg.Publisher != nil {
		host, err := s.cfg.Publisher.Publish(ctx, pub)
		if err != nil {
			abandon()
			return nil, fmt.Errorf("publish %q: %w", req.Name, err)
		}
		reply.Host = host
	}

	for _, b := range listeners {
		go s.acceptLoop(sess, req.Name, b.ln, b.localPort)
	}
	s.log.Info("exposed", "exposure", req.Name, "session", sess.id, "user", sess.user, "ports", len(req.Ports))
	return reply, nil
}

func (s *Server) acceptLoop(sess *session, name string, ln net.Listener, localPort int) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.inbound(sess, name, localPort, conn)
	}
}

// inbound hands one cluster connection to the client that owns the port.
func (s *Server) inbound(sess *session, name string, localPort int, conn net.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), protocol.HandshakeTimeout)
	st, err := sess.mux.OpenStream(ctx)
	cancel()
	if err != nil {
		conn.Close()
		return
	}
	st.SetDeadline(time.Now().Add(protocol.HandshakeTimeout))
	req := &protocol.Request{Type: protocol.TypeInbound, Inbound: &protocol.Inbound{
		Exposure:   name,
		LocalPort:  localPort,
		RemoteAddr: conn.RemoteAddr().String(),
	}}
	var resp protocol.Response
	if err := protocol.WriteMsg(st, req); err == nil {
		err = protocol.ReadMsg(st, &resp)
		if err == nil {
			err = resp.Err()
		}
	}
	if err != nil {
		s.log.Debug("inbound refused", "exposure", name, "err", err)
		st.Reset()
		conn.Close()
		return
	}
	st.SetDeadline(time.Time{})
	protocol.Pipe(conn, st)
}
