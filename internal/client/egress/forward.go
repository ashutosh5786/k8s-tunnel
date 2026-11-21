// Package egress sends local connections into the cluster. A Forward
// listens on a local address and relays every connection through the
// tunnel to one cluster address.
package egress

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/protocol"
)

// Dialer connects to a cluster address through the tunnel. It is called
// for every connection, so it may hand out whichever session is current.
type Dialer func(ctx context.Context, network, address string) (net.Conn, error)

// Forward relays connections accepted on Listener to Target.
type Forward struct {
	Listener net.Listener
	// Target is the cluster address in host:port form.
	Target string
	Dial   Dialer
	Log    *slog.Logger
}

// dialTimeout bounds how long a local connection waits for the cluster
// side before it is dropped.
const dialTimeout = 20 * time.Second

// Serve accepts connections until ctx is cancelled or the listener fails.
// It closes the listener on return.
func (f *Forward) Serve(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() { f.Listener.Close() })
	defer stop()
	defer f.Listener.Close()
	for {
		conn, err := f.Listener.Accept()
		if err != nil {
			return
		}
		go f.handle(ctx, conn)
	}
}

func (f *Forward) handle(ctx context.Context, local net.Conn) {
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	remote, err := f.Dial(dctx, "tcp", f.Target)
	cancel()
	if err != nil {
		if f.Log != nil {
			f.Log.Warn("forward failed", "target", f.Target, "err", err)
		}
		local.Close()
		return
	}
	// End the relay when the forward is shut down.
	stop := context.AfterFunc(ctx, func() {
		local.Close()
		remote.Close()
	})
	defer stop()
	protocol.Pipe(local, remote)
}
