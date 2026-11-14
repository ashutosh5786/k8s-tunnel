// Command tunnel is the client: it runs on the developer's machine and
// connects a local service to the cluster.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s-tunnel/internal/client/connect"
	"k8s-tunnel/internal/client/hosts"
	"k8s-tunnel/internal/kube"
	"k8s-tunnel/pkg/tunnel"
)

const usage = `k8s-tunnel connects a local service to a Kubernetes cluster.

Usage:
  tunnel connect [flags]     connect to the cluster until interrupted
  tunnel cleanup             remove hosts-file entries left by a crashed run

Examples:
  # Make localhost:3000 reachable in the cluster as Service "b-ash" port 8080,
  # and reach the cluster's Postgres on localhost:5432.
  tunnel connect --expose b-ash:8080:3000/http --forward 5432:postgres.db.svc:5432

  # Make every cluster Service name work locally (needs an elevated terminal).
  tunnel connect --hosts --expose b-ash:8080:3000/http

Run "tunnel connect -h" for all flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "tunnel:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return errors.New("no command given")
	}
	switch args[0] {
	case "connect":
		return runConnect(ctx, args[1:], stdout, stderr)
	case "cleanup":
		return runCleanup(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	default:
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// listFlag collects a flag that may be given several times.
type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func runConnect(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("tunnel connect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var exposes, forwards listFlag
	fs.Var(&exposes, "expose", "local port to expose: NAME:PORT[:LOCALPORT][/PROTOCOL], e.g. b-ash:8080:3000/http (repeatable)")
	fs.Var(&forwards, "forward", "cluster address to reach locally: LOCALPORT:HOST:PORT, e.g. 5432:postgres.db.svc:5432 (repeatable)")
	var (
		userName   = fs.String("user", defaultUser(), "name recorded on your exposures")
		session    = fs.String("session", "", "session id; reuse one to reclaim exposures after a restart (default: random)")
		kubeconfig = fs.String("kubeconfig", "", "kubeconfig file (default: KUBECONFIG or ~/.kube/config)")
		kubeCtx    = fs.String("context", "", "kubeconfig context (default: current context)")
		namespace  = fs.String("namespace", "k8s-tunnel", "namespace the tunnel server runs in")
		selector   = fs.String("selector", "app.kubernetes.io/name=k8s-tunnel", "label selector for tunnel server pods")
		port       = fs.Int("port", 7000, "tunnel server port inside the pod")
		server     = fs.String("server", "", "connect straight to HOST:PORT instead of through a cluster (for local testing)")
		debug      = fs.Bool("debug", false, "verbose logging")

		useHosts      = fs.Bool("hosts", false, "make cluster Service names resolve locally by managing hosts-file entries (needs an elevated terminal)")
		hostsFile     = fs.String("hosts-file", hosts.DefaultPath(), "hosts file to manage with --hosts")
		hostsNS       = fs.String("hosts-namespaces", "", "with --hosts, only resolve Services in these comma-separated namespaces (default: all)")
		homeNamespace = fs.String("home-namespace", "", "with --hosts, Services in this namespace also resolve by bare name, as for a pod running there")
		clusterDomain = fs.String("cluster-domain", "cluster.local", "the cluster's DNS suffix, used with --hosts")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	cfg := connect.Config{User: *userName, Session: *session, Out: stdout}
	if cfg.User == "" {
		return errors.New("--user is required: could not work out your user name")
	}
	if cfg.Session == "" {
		cfg.Session = newSessionID()
	}
	var err error
	if cfg.Exposures, err = parseExposes(exposes); err != nil {
		return err
	}
	if cfg.Forwards, err = parseForwards(forwards); err != nil {
		return err
	}
	if *useHosts {
		cfg.Hosts = &connect.Hosts{
			Path:          *hostsFile,
			HomeNamespace: *homeNamespace,
			ClusterDomain: *clusterDomain,
		}
		for _, ns := range strings.Split(*hostsNS, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				cfg.Hosts.Namespaces = append(cfg.Hosts.Namespaces, ns)
			}
		}
	} else if *hostsNS != "" || *homeNamespace != "" {
		return errors.New("--hosts-namespaces and --home-namespace only apply together with --hosts")
	}
	if len(cfg.Exposures) == 0 && len(cfg.Forwards) == 0 && cfg.Hosts == nil {
		return errors.New("nothing to do: give at least one --expose, --forward or --hosts")
	}
	level := slog.LevelWarn
	if *debug {
		level = slog.LevelDebug
	}
	cfg.Log = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	if *server != "" {
		cfg.Transport = func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", *server)
		}
	} else {
		cluster, err := kube.Load(*kubeconfig, *kubeCtx)
		if err != nil {
			return err
		}
		cfg.Transport = func(ctx context.Context) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			pod, err := kube.PickPod(ctx, cluster.Client, *namespace, *selector, cfg.Session)
			if err != nil {
				return nil, err
			}
			return cluster.PortForward(ctx, *namespace, pod, *port)
		}
	}
	return connect.Run(ctx, cfg)
}

// runCleanup removes hosts-file entries a previous run left behind, for
// example after the process was killed.
func runCleanup(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("tunnel cleanup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	hostsFile := fs.String("hosts-file", hosts.DefaultPath(), "hosts file to clean")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if err := hosts.Remove(*hostsFile); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "removed k8s-tunnel entries from %s\n", *hostsFile)
	return nil
}

func defaultUser() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		// Windows reports DOMAIN\name; keep the name.
		name := u.Username
		if i := strings.LastIndexAny(name, `\/`); i >= 0 {
			name = name[i+1:]
		}
		return name
	}
	return ""
}

func newSessionID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // the system random source is broken
	}
	return "s" + hex.EncodeToString(b[:])
}

func parsePort(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return n, nil
}

// parseExposes turns NAME:PORT[:LOCALPORT][/PROTOCOL] specs into
// exposures, merging specs that share a name into one multi-port exposure.
func parseExposes(specs []string) ([]connect.Exposure, error) {
	var out []connect.Exposure
	index := map[string]int{}
	for _, spec := range specs {
		bad := func(reason string) error {
			return fmt.Errorf("invalid --expose %q: %s (want NAME:PORT[:LOCALPORT][/PROTOCOL])", spec, reason)
		}
		body, proto, hasProto := strings.Cut(spec, "/")
		if hasProto && proto == "" {
			return nil, bad("empty protocol")
		}
		parts := strings.Split(body, ":")
		if len(parts) < 2 || len(parts) > 3 || parts[0] == "" {
			return nil, bad("wrong number of fields")
		}
		p := tunnel.Port{AppProtocol: proto}
		var err error
		if p.Port, err = parsePort(parts[1]); err != nil {
			return nil, bad(err.Error())
		}
		p.LocalPort = p.Port
		if len(parts) == 3 {
			if p.LocalPort, err = parsePort(parts[2]); err != nil {
				return nil, bad(err.Error())
			}
		}
		// Service port names tell the mesh which protocol to expect.
		if proto != "" {
			p.Name = fmt.Sprintf("%s-%d", proto, p.Port)
		} else {
			p.Name = fmt.Sprintf("tcp-%d", p.Port)
		}

		name := parts[0]
		i, ok := index[name]
		if !ok {
			i = len(out)
			index[name] = i
			out = append(out, connect.Exposure{Name: name})
		}
		for _, existing := range out[i].Ports {
			if existing.Port == p.Port {
				return nil, bad(fmt.Sprintf("port %d given twice for %s", p.Port, name))
			}
		}
		out[i].Ports = append(out[i].Ports, p)
	}
	return out, nil
}

// parseForwards turns LOCALPORT:HOST:PORT specs into forwards that listen
// on the loopback interface only.
func parseForwards(specs []string) ([]connect.Forward, error) {
	var out []connect.Forward
	seen := map[int]bool{}
	for _, spec := range specs {
		bad := func(reason string) error {
			return fmt.Errorf("invalid --forward %q: %s (want LOCALPORT:HOST:PORT)", spec, reason)
		}
		localStr, target, ok := strings.Cut(spec, ":")
		if !ok {
			return nil, bad("wrong number of fields")
		}
		local, err := parsePort(localStr)
		if err != nil {
			return nil, bad(err.Error())
		}
		host, portStr, err := net.SplitHostPort(target)
		if err != nil || host == "" {
			return nil, bad("target must be HOST:PORT")
		}
		if _, err := parsePort(portStr); err != nil {
			return nil, bad(err.Error())
		}
		if seen[local] {
			return nil, bad(fmt.Sprintf("local port %d given twice", local))
		}
		seen[local] = true
		out = append(out, connect.Forward{
			Local:  net.JoinHostPort("127.0.0.1", strconv.Itoa(local)),
			Target: target,
		})
	}
	return out, nil
}
