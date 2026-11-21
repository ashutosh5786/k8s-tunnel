// Package connect keeps one tunnel session alive: it connects, exposes the
// requested local ports, serves local forwards, and reconnects when the
// connection drops.
package connect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/egress"
	"github.com/ashutosh5786/k8s-tunnel/internal/client/hosts"
	"github.com/ashutosh5786/k8s-tunnel/pkg/tunnel"
)

// Exposure is a local service to make reachable in the cluster.
type Exposure struct {
	Name  string
	Ports []tunnel.Port
}

// Forward makes one cluster address reachable on a local address.
type Forward struct {
	// Local is the local listen address, e.g. "127.0.0.1:5432".
	Local string
	// Target is the cluster address, e.g. "postgres.db.svc:5432".
	Target string
}

// Hosts turns on resolving cluster Service names locally through the
// hosts file.
type Hosts struct {
	// Path is the hosts file to manage.
	Path string
	// Namespaces limits which Services are resolved. Empty means all.
	Namespaces []string
	// HomeNamespace is the namespace whose Services also resolve by bare
	// name, as they would for a pod running there.
	HomeNamespace string
	// ClusterDomain is the cluster's DNS suffix. Defaults to
	// "cluster.local".
	ClusterDomain string
	// Interval is how often the Service list is refreshed. Defaults to
	// 30s.
	Interval time.Duration
}

// Config describes a session.
type Config struct {
	User    string
	Session string
	// Transport opens a connection to a tunnel server. It is called again
	// on every reconnect.
	Transport func(ctx context.Context) (net.Conn, error)
	Exposures []Exposure
	Forwards  []Forward
	// Hosts, when set, makes cluster Service names resolve locally.
	Hosts *Hosts
	// Out receives human-readable status lines. May be nil.
	Out io.Writer
	Log *slog.Logger
	// MinBackoff and MaxBackoff bound the wait between reconnect
	// attempts. They default to 1s and 30s.
	MinBackoff, MaxBackoff time.Duration
}

var errNotConnected = errors.New("not connected to the cluster right now")

// Session is one connection to the cluster. What it exposes can be changed
// while it runs, and it puts everything back after a reconnect.
type Session struct {
	cfg     Config
	current atomic.Pointer[tunnel.Client]

	// mu serialises every change to what is exposed, including the
	// re-exposing done after a reconnect, so the two cannot interleave.
	mu     sync.Mutex
	wanted map[string]Exposure         // what should be exposed, by name
	order  []string                    // names in the order they were added
	live   map[string]*tunnel.Exposure // what the server last reported
}

// New prepares a session. Call Run to connect.
func New(cfg Config) *Session {
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.MinBackoff == 0 {
		cfg.MinBackoff = time.Second
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	s := &Session{cfg: cfg, wanted: map[string]Exposure{}, live: map[string]*tunnel.Exposure{}}
	for _, e := range cfg.Exposures {
		if _, dup := s.wanted[e.Name]; !dup {
			s.order = append(s.order, e.Name)
		}
		s.wanted[e.Name] = e
	}
	return s
}

// Run is New followed by Session.Run, for callers that never change the
// session afterwards.
func Run(ctx context.Context, cfg Config) error {
	return New(cfg).Run(ctx)
}

// Run holds the session open until ctx is cancelled, reconnecting as
// needed. It returns an error only if the first connection fails or a
// local forward cannot listen, since those usually mean a wrong setting
// that retrying will not fix.
func (s *Session) Run(ctx context.Context) error {
	cfg := s.cfg
	// The forwards outlive individual connections and always use
	// whichever connection is current.
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		c := s.current.Load()
		if c == nil {
			return nil, errNotConnected
		}
		return c.DialContext(ctx, network, address)
	}

	// Fail before connecting if the hosts file cannot be changed, which
	// almost always means the terminal is not elevated.
	var proxy *egress.ServiceProxy
	if cfg.Hosts != nil {
		if err := hosts.CheckWritable(cfg.Hosts.Path); err != nil {
			return err
		}
		var err error
		proxy, err = egress.NewServiceProxy(egress.ProxyOptions{
			Dial:          dial,
			Log:           cfg.Log,
			ClusterDomain: cfg.Hosts.ClusterDomain,
			HomeNamespace: cfg.Hosts.HomeNamespace,
		})
		if err != nil {
			return err
		}
	}

	fctx, stopForwards := context.WithCancel(ctx)
	var forwards sync.WaitGroup
	defer func() {
		stopForwards()
		forwards.Wait()
	}()
	if proxy != nil {
		list := func(ctx context.Context) ([]egress.Service, error) {
			c := s.current.Load()
			if c == nil {
				return nil, errNotConnected
			}
			services, err := c.Services(ctx, cfg.Hosts.Namespaces...)
			if err != nil {
				return nil, err
			}
			out := make([]egress.Service, len(services))
			for i, svc := range services {
				out[i] = egress.Service{Namespace: svc.Namespace, Name: svc.Name, Ports: svc.Ports}
			}
			return out, nil
		}
		forwards.Add(1)
		go func() {
			defer forwards.Done()
			// Run removes the hosts entries again when it returns.
			proxy.Run(fctx, egress.RunConfig{List: list, HostsPath: cfg.Hosts.Path, Interval: cfg.Hosts.Interval, Out: cfg.Out})
		}()
	}
	// Bind every forward before serving any, so a port that is already
	// taken fails the run without leaving the others half started.
	listeners := make([]net.Listener, 0, len(cfg.Forwards))
	for _, f := range cfg.Forwards {
		ln, err := net.Listen("tcp", f.Local)
		if err != nil {
			for _, open := range listeners {
				open.Close()
			}
			return fmt.Errorf("forward %s: %w", f.Local, err)
		}
		listeners = append(listeners, ln)
	}
	for i, f := range cfg.Forwards {
		fw := &egress.Forward{Listener: listeners[i], Target: f.Target, Dial: dial, Log: cfg.Log}
		forwards.Add(1)
		go func() {
			defer forwards.Done()
			fw.Serve(fctx)
		}()
		fmt.Fprintf(cfg.Out, "forward   %s -> %s\n", listeners[i].Addr(), f.Target)
	}

	backoff := cfg.MinBackoff
	connectedOnce := false
	for {
		client, err := s.establish(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !connectedOnce {
				return err
			}
			fmt.Fprintf(cfg.Out, "reconnect failed: %v (retrying in %s)\n", err, backoff)
		} else {
			connectedOnce = true
			backoff = cfg.MinBackoff
			select {
			case <-client.Done():
				s.current.CompareAndSwap(client, nil)
				fmt.Fprintf(cfg.Out, "connection lost (reconnecting in %s)\n", backoff)
			case <-ctx.Done():
				s.shutdown(client)
				return nil
			}
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff = min(backoff*2, cfg.MaxBackoff)
	}
}

// establish opens one connection, exposes everything that is wanted on
// it, and only then makes it the current connection.
func (s *Session) establish(ctx context.Context) (*tunnel.Client, error) {
	conn, err := s.cfg.Transport(ctx)
	if err != nil {
		return nil, err
	}
	client, err := tunnel.Connect(ctx, conn, tunnel.Options{User: s.cfg.User, Session: s.cfg.Session, Log: s.cfg.Log})
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(s.cfg.Out, "connected to %s as %s (session %s)\n", orUnknown(client.Replica()), s.cfg.User, s.cfg.Session)

	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[string]*tunnel.Exposure{}
	for _, name := range s.order {
		exp, err := client.Expose(ctx, name, s.wanted[name].Ports)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("expose %s: %w", name, err)
		}
		live[name] = exp
		s.report(exp)
	}
	s.live = live
	s.current.Store(client)
	return client, nil
}

// report writes one status line per exposed port.
func (s *Session) report(exp *tunnel.Exposure) {
	for _, line := range Describe(exp) {
		fmt.Fprintf(s.cfg.Out, "exposed   %s\n", line)
	}
}

// Describe returns one line per port saying where the cluster reaches the
// exposure and which local port it leads to.
func Describe(exp *tunnel.Exposure) []string {
	var out []string
	for _, p := range exp.Ports {
		var where string
		switch {
		case exp.Host != "":
			where = fmt.Sprintf("%s:%d", exp.Host, p.Port)
		case p.ServerPort != 0:
			where = fmt.Sprintf("server port %d", p.ServerPort)
		default:
			where = fmt.Sprintf("%s:%d (waiting to reconnect)", exp.Name, p.Port)
		}
		out = append(out, fmt.Sprintf("%s -> localhost:%d", where, p.LocalPort))
	}
	return out
}

// Expose adds an exposure to the running session, or replaces the one
// with the same name. It is kept across reconnects until Unexpose.
func (s *Session) Expose(ctx context.Context, e Exposure) (*tunnel.Exposure, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	client := s.current.Load()
	if client == nil {
		return nil, errNotConnected
	}
	exp, err := client.Expose(ctx, e.Name, e.Ports)
	if err != nil {
		// Not remembered: a request the server refused must not be
		// retried on every reconnect.
		return nil, err
	}
	if _, known := s.wanted[e.Name]; !known {
		s.order = append(s.order, e.Name)
	}
	s.wanted[e.Name] = e
	s.live[e.Name] = exp
	s.report(exp)
	return exp, nil
}

// Unexpose removes an exposure from the running session.
func (s *Session) Unexpose(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, known := s.wanted[name]; !known {
		return fmt.Errorf("%q is not exposed", name)
	}
	// While disconnected there is nobody to tell; forgetting it is
	// enough, because the server drops it when the grace period ends.
	if client := s.current.Load(); client != nil {
		if err := client.Unexpose(ctx, name); err != nil {
			return err
		}
	}
	delete(s.wanted, name)
	delete(s.live, name)
	for i, n := range s.order {
		if n == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	fmt.Fprintf(s.cfg.Out, "unexposed %s\n", name)
	return nil
}

// Exposures returns what the session exposes, in the order it was added.
func (s *Session) Exposures() []*tunnel.Exposure {
	s.mu.Lock()
	defer s.mu.Unlock()
	connected := s.current.Load() != nil
	out := make([]*tunnel.Exposure, 0, len(s.order))
	for _, name := range s.order {
		if exp := s.live[name]; exp != nil && connected {
			out = append(out, exp)
			continue
		}
		// Not on the server at the moment: describe what is wanted.
		exp := &tunnel.Exposure{Name: name}
		for _, p := range s.wanted[name].Ports {
			local := p.LocalPort
			if local == 0 {
				local = p.Port
			}
			exp.Ports = append(exp.Ports, tunnel.ExposedPort{Name: p.Name, Port: p.Port, LocalPort: local})
		}
		out = append(out, exp)
	}
	return out
}

// Has reports whether exactly this exposure is already in place, so that
// applying a description twice does not disturb what is running.
func (s *Session) Has(e Exposure) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.wanted[e.Name]
	if !ok || s.live[e.Name] == nil || len(cur.Ports) != len(e.Ports) {
		return false
	}
	for i := range cur.Ports {
		if cur.Ports[i] != e.Ports[i] {
			return false
		}
	}
	return true
}

// Names returns the names of everything exposed, in the order added.
func (s *Session) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

// Connected reports whether the session has a live connection right now.
func (s *Session) Connected() bool { return s.current.Load() != nil }

// shutdown removes the exposures right away, so the cluster stops sending
// traffic instead of waiting out the server's grace period.
func (s *Session) shutdown(client *tunnel.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.mu.Lock()
	s.current.Store(nil)
	names := append([]string(nil), s.order...)
	s.mu.Unlock()
	for _, name := range names {
		if err := client.Unexpose(ctx, name); err != nil {
			s.cfg.Log.Warn("unexpose failed", "exposure", name, "err", err)
		}
	}
	client.Close()
	fmt.Fprintln(s.cfg.Out, "disconnected")
}

func orUnknown(s string) string {
	if s == "" {
		return "tunnel server"
	}
	return s
}
