package egress

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/hosts"
)

// Service is a cluster Service and its TCP ports.
type Service struct {
	Namespace string
	Name      string
	Ports     []int
}

// ProxyOptions configures a ServiceProxy.
type ProxyOptions struct {
	// Dial connects to a cluster address through the tunnel.
	Dial Dialer
	Log  *slog.Logger
	// ClusterDomain is the cluster's DNS suffix. Defaults to
	// "cluster.local".
	ClusterDomain string
	// HomeNamespace is the namespace the local service belongs to. Its
	// Services also resolve by bare name, as they would in a pod there.
	HomeNamespace string
	// BaseIP is the start of the loopback range used, one address per
	// Service. Defaults to 127.77.0.0, giving 127.77.0.1 upwards.
	BaseIP string
}

// maxServices is how many Services fit in the address range (a /16).
const maxServices = 65534

// ServiceProxy gives every cluster Service its own loopback address and
// listens there on the Service's ports, so that with matching hosts-file
// entries the Service's cluster name and port work unchanged locally.
type ServiceProxy struct {
	opts ProxyOptions
	base uint32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	services map[string]*proxied // "namespace/name" -> state
	used     map[uint32]bool     // offsets from base in use
	next     uint32
}

type proxied struct {
	offset uint32
	ip     string
	ports  map[int]context.CancelFunc // listening ports
	failed map[int]bool               // ports that could not listen, to log once
}

// NewServiceProxy returns an idle proxy. Call Sync to give it Services.
func NewServiceProxy(opts ProxyOptions) (*ServiceProxy, error) {
	if opts.ClusterDomain == "" {
		opts.ClusterDomain = "cluster.local"
	}
	if opts.BaseIP == "" {
		opts.BaseIP = "127.77.0.0"
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ip := net.ParseIP(opts.BaseIP).To4()
	if ip == nil || !ip.IsLoopback() || ip[2] != 0 || ip[3] != 0 {
		return nil, fmt.Errorf("base address %q must be a loopback address ending in .0.0", opts.BaseIP)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ServiceProxy{
		opts:     opts,
		base:     binary.BigEndian.Uint32(ip),
		ctx:      ctx,
		cancel:   cancel,
		services: map[string]*proxied{},
		used:     map[uint32]bool{},
		next:     1,
	}, nil
}

// labelRE is a DNS label. Names come from the cluster and end up in the
// hosts file, so anything else is skipped.
var labelRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// allocate returns a free offset, or false when the range is full.
func (p *ServiceProxy) allocate() (uint32, bool) {
	for range maxServices {
		off := p.next
		p.next++
		if p.next > maxServices {
			p.next = 1
		}
		// Skip .0 and .255 in the last octet; some systems treat them
		// specially even on loopback.
		if last := off & 0xff; last == 0 || last == 0xff {
			continue
		}
		if !p.used[off] {
			p.used[off] = true
			return off, true
		}
	}
	return 0, false
}

func (p *ServiceProxy) ipFor(offset uint32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], p.base+offset)
	return net.IP(b[:]).String()
}

// Sync makes the proxy serve exactly the given Services and returns the
// hosts-file entries that point their names at it. A Service keeps its
// address for as long as it stays in the list.
func (p *ServiceProxy) Sync(services []Service) []hosts.Entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return nil
	}

	want := map[string]Service{}
	for _, s := range services {
		if !labelRE.MatchString(s.Name) || !labelRE.MatchString(s.Namespace) {
			p.opts.Log.Debug("skipping service with an unusable name", "namespace", s.Namespace, "name", s.Name)
			continue
		}
		want[s.Namespace+"/"+s.Name] = s
	}

	for key, st := range p.services {
		if _, ok := want[key]; !ok {
			for _, stop := range st.ports {
				stop()
			}
			delete(p.used, st.offset)
			delete(p.services, key)
		}
	}

	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys) // stable address assignment on first sight

	var entries []hosts.Entry
	for _, key := range keys {
		s := want[key]
		st := p.services[key]
		if st == nil {
			off, ok := p.allocate()
			if !ok {
				p.opts.Log.Warn("no loopback address left for service", "service", key)
				continue
			}
			st = &proxied{offset: off, ip: p.ipFor(off), ports: map[int]context.CancelFunc{}, failed: map[int]bool{}}
			p.services[key] = st
		}

		wantPorts := map[int]bool{}
		for _, port := range s.Ports {
			if port >= 1 && port <= 65535 {
				wantPorts[port] = true
			}
		}
		for port, stop := range st.ports {
			if !wantPorts[port] {
				stop()
				delete(st.ports, port)
			}
		}
		for port := range wantPorts {
			if _, ok := st.ports[port]; ok {
				continue
			}
			addr := net.JoinHostPort(st.ip, strconv.Itoa(port))
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				if !st.failed[port] {
					st.failed[port] = true
					p.opts.Log.Warn("cannot listen for service port", "service", key, "addr", addr, "err", err)
				}
				continue
			}
			delete(st.failed, port)
			ctx, stop := context.WithCancel(p.ctx)
			st.ports[port] = stop
			fw := &Forward{
				Listener: ln,
				// The short form resolves through the server pod's DNS
				// search path, whatever the cluster's domain is.
				Target: net.JoinHostPort(s.Name+"."+s.Namespace+".svc", strconv.Itoa(port)),
				Dial:   p.opts.Dial,
				Log:    p.opts.Log,
			}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				fw.Serve(ctx)
			}()
		}

		qualified := s.Name + "." + s.Namespace
		names := []string{qualified, qualified + ".svc", qualified + ".svc." + p.opts.ClusterDomain}
		if s.Namespace == p.opts.HomeNamespace && s.Name != "localhost" {
			names = append(names, s.Name)
		}
		entries = append(entries, hosts.Entry{IP: st.ip, Names: names})
	}
	return entries
}

// Close stops every listener and open connection.
func (p *ServiceProxy) Close() {
	p.cancel()
	p.wg.Wait()
}

// RunConfig configures ServiceProxy.Run.
type RunConfig struct {
	// List returns the cluster's Services. It may fail while the tunnel
	// is reconnecting; the previous state is then kept.
	List func(ctx context.Context) ([]Service, error)
	// HostsPath is the hosts file to manage.
	HostsPath string
	// Interval is how often the listing is refreshed. Defaults to 30s.
	Interval time.Duration
	// Out receives status lines. May be nil.
	Out io.Writer
}

// Run keeps the proxy and the hosts file in step with the cluster until
// ctx is cancelled, then closes the proxy and removes its hosts entries.
func (p *ServiceProxy) Run(ctx context.Context, cfg RunConfig) {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}
	defer func() {
		p.Close()
		if err := hosts.Remove(cfg.HostsPath); err != nil {
			fmt.Fprintf(cfg.Out, "could not clean up hosts file: %v\n", err)
		}
	}()

	shown := -1
	synced := false
	attempts := 0
	for {
		lctx, cancel := context.WithTimeout(ctx, time.Minute)
		services, err := cfg.List(lctx)
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			p.opts.Log.Debug("service listing failed", "err", err)
		default:
			entries := p.Sync(services)
			if err := hosts.Update(cfg.HostsPath, entries); err != nil {
				fmt.Fprintf(cfg.Out, "could not update hosts file: %v\n", err)
			} else {
				synced = true
				if len(entries) != shown {
					shown = len(entries)
					fmt.Fprintf(cfg.Out, "resolving %d cluster services by name\n", shown)
				}
			}
		}
		wait := cfg.Interval
		if !synced {
			// Until the first success the tunnel is probably still
			// connecting, so look again very soon: names should work
			// the moment "connect" reports success. If it keeps failing
			// (discovery may be off on the server), slow down.
			attempts++
			wait = min(wait, 2*time.Second)
			if attempts <= 40 {
				wait = min(wait, 250*time.Millisecond)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
