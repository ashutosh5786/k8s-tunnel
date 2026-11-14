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

	"k8s-tunnel/internal/client/egress"
	"k8s-tunnel/pkg/tunnel"
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

// Config describes a session.
type Config struct {
	User    string
	Session string
	// Transport opens a connection to a tunnel server. It is called again
	// on every reconnect.
	Transport func(ctx context.Context) (net.Conn, error)
	Exposures []Exposure
	Forwards  []Forward
	// Out receives human-readable status lines. May be nil.
	Out io.Writer
	Log *slog.Logger
	// MinBackoff and MaxBackoff bound the wait between reconnect
	// attempts. They default to 1s and 30s.
	MinBackoff, MaxBackoff time.Duration
}

var errNotConnected = errors.New("tunnel is not connected")

// Run holds the session open until ctx is cancelled, reconnecting as
// needed. It returns an error only if the first connection fails or a
// local forward cannot listen, since those usually mean a wrong setting
// that retrying will not fix.
func Run(ctx context.Context, cfg Config) error {
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

	// The forwards outlive individual connections and always use
	// whichever session is current.
	var current atomic.Pointer[tunnel.Client]
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		c := current.Load()
		if c == nil {
			return nil, errNotConnected
		}
		return c.DialContext(ctx, network, address)
	}

	fctx, stopForwards := context.WithCancel(ctx)
	var forwards sync.WaitGroup
	defer func() {
		stopForwards()
		forwards.Wait()
	}()
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
		client, err := establish(ctx, cfg)
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
			current.Store(client)
			select {
			case <-client.Done():
				current.Store(nil)
				fmt.Fprintf(cfg.Out, "connection lost (reconnecting in %s)\n", backoff)
			case <-ctx.Done():
				current.Store(nil)
				shutdown(client, cfg)
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

// establish opens one connection and sets up every exposure on it.
func establish(ctx context.Context, cfg Config) (*tunnel.Client, error) {
	conn, err := cfg.Transport(ctx)
	if err != nil {
		return nil, err
	}
	client, err := tunnel.Connect(ctx, conn, tunnel.Options{User: cfg.User, Session: cfg.Session, Log: cfg.Log})
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(cfg.Out, "connected to %s as %s (session %s)\n", orUnknown(client.Replica()), cfg.User, cfg.Session)
	for _, e := range cfg.Exposures {
		exp, err := client.Expose(ctx, e.Name, e.Ports)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("expose %s: %w", e.Name, err)
		}
		for _, p := range exp.Ports {
			host := exp.Host
			if host == "" {
				host = "server port " + fmt.Sprint(p.ServerPort)
			} else {
				host = fmt.Sprintf("%s:%d", host, p.Port)
			}
			fmt.Fprintf(cfg.Out, "exposed   %s -> localhost:%d\n", host, p.LocalPort)
		}
	}
	return client, nil
}

// shutdown removes the exposures right away, so the cluster stops sending
// traffic instead of waiting out the server's grace period.
func shutdown(client *tunnel.Client, cfg Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, e := range cfg.Exposures {
		if err := client.Unexpose(ctx, e.Name); err != nil {
			cfg.Log.Warn("unexpose failed", "exposure", e.Name, "err", err)
		}
	}
	client.Close()
	fmt.Fprintln(cfg.Out, "disconnected")
}

func orUnknown(s string) string {
	if s == "" {
		return "tunnel server"
	}
	return s
}
