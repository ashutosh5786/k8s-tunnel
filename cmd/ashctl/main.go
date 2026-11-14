// Command ashctl is the client: it runs on the developer's machine and
// connects it to a cluster through the tunnel server.
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
	"os/exec"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s-tunnel/internal/client/connect"
	"k8s-tunnel/internal/client/daemon"
	"k8s-tunnel/internal/client/hosts"
	"k8s-tunnel/internal/kube"
	"k8s-tunnel/pkg/tunnel"
)

const usage = `ashctl connects your machine to a Kubernetes cluster.

Usage:
  ashctl connect [--context NAME]   connect and keep running in the background
  ashctl expose NAME:PORT[:LOCAL]   make a local port reachable in the cluster
  ashctl unexpose NAME              stop exposing it
  ashctl status                     show whether you are connected, and what is exposed
  ashctl disconnect                 stop the background connection
  ashctl logs                       show the background connection's output
  ashctl cleanup                    remove hosts-file entries left by a crash

Examples:
  # Connect to a cluster. From an administrator terminal, every cluster
  # Service name then works on this machine.
  ashctl connect --context dev

  # While connected, make localhost:3000 reachable in the cluster as
  # Service "b-ash" on port 8080, and later take it away again.
  ashctl expose b-ash:8080:3000/http
  ashctl unexpose b-ash

Run "ashctl connect -h" for all flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "ashctl:", err)
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
	case "expose":
		return runExpose(args[1:], stdout)
	case "unexpose":
		return runUnexpose(args[1:], stdout)
	case "status":
		return runStatus(args[1:], stdout)
	case "disconnect":
		return runDisconnect(args[1:], stdout)
	case "logs":
		return runLogs(args[1:], stdout)
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

// connectOpts holds the parsed flags of "ashctl connect".
type connectOpts struct {
	exposes, forwards listFlag

	user, session             string
	kubeconfig, kubeContext   string
	namespace, selector       string
	port                      int
	server                    string
	debug                     bool
	foreground, daemonChild   bool
	forceHosts, noHosts       bool
	hostsFile, hostsNS        string
	homeNamespace, clusterDNS string
}

func parseConnect(args []string, stderr io.Writer) (*connectOpts, error) {
	o := &connectOpts{}
	fs := flag.NewFlagSet("ashctl connect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.kubeContext, "context", "", "kubeconfig context to connect to (default: current context)")
	fs.Var(&o.exposes, "expose", "local port to expose: NAME:PORT[:LOCALPORT][/PROTOCOL], e.g. b-ash:8080:3000/http (repeatable)")
	fs.Var(&o.forwards, "forward", "cluster address to reach on a local port: LOCALPORT:HOST:PORT (repeatable)")
	fs.BoolVar(&o.foreground, "foreground", false, "stay attached to this terminal instead of running in the background")
	fs.BoolVar(&o.forceHosts, "hosts", false, "require cluster Service names to resolve locally, and fail if the hosts file cannot be changed")
	fs.BoolVar(&o.noHosts, "no-hosts", false, "do not touch the hosts file")
	fs.StringVar(&o.homeNamespace, "home-namespace", "", "Services in this namespace also resolve by bare name, as for a pod running there")
	fs.StringVar(&o.hostsNS, "hosts-namespaces", "", "only resolve Services in these comma-separated namespaces (default: all)")
	fs.StringVar(&o.hostsFile, "hosts-file", hosts.DefaultPath(), "hosts file to manage")
	fs.StringVar(&o.clusterDNS, "cluster-domain", "cluster.local", "the cluster's DNS suffix")
	fs.StringVar(&o.user, "user", defaultUser(), "name recorded on your exposures")
	fs.StringVar(&o.session, "session", "", "session id; reuse one to reclaim exposures after a restart (default: random)")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "kubeconfig file (default: KUBECONFIG or ~/.kube/config)")
	fs.StringVar(&o.namespace, "namespace", "k8s-tunnel", "namespace the tunnel server runs in")
	fs.StringVar(&o.selector, "selector", "app.kubernetes.io/name=k8s-tunnel", "label selector for tunnel server pods")
	fs.IntVar(&o.port, "port", 7000, "tunnel server port inside the pod")
	fs.StringVar(&o.server, "server", "", "connect straight to HOST:PORT instead of through a cluster (for local testing)")
	fs.BoolVar(&o.debug, "debug", false, "verbose logging")
	// Set by the parent on the background process it starts; not for
	// people to use.
	fs.BoolVar(&o.daemonChild, "daemon-child", false, "internal")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if o.forceHosts && o.noHosts {
		return nil, errors.New("--hosts and --no-hosts cannot be used together")
	}
	if o.user == "" {
		return nil, errors.New("--user is required: could not work out your user name")
	}
	return o, nil
}

// buildConfig turns the flags into a session description. Notes about
// choices it made on the user's behalf are written to notes.
func buildConfig(o *connectOpts, stdout, stderr, notes io.Writer) (connect.Config, error) {
	cfg := connect.Config{User: o.user, Session: o.session, Out: stdout}
	var err error
	if cfg.Exposures, err = parseExposes(o.exposes); err != nil {
		return cfg, err
	}
	if cfg.Forwards, err = parseForwards(o.forwards); err != nil {
		return cfg, err
	}

	// Resolving cluster names needs the hosts file, which only an
	// elevated process may change. Unless told otherwise, use it when
	// possible and carry on without it when not.
	if !o.noHosts {
		werr := hosts.CheckWritable(o.hostsFile)
		switch {
		case werr == nil:
			cfg.Hosts = &connect.Hosts{Path: o.hostsFile, HomeNamespace: o.homeNamespace, ClusterDomain: o.clusterDNS}
			for _, ns := range strings.Split(o.hostsNS, ",") {
				if ns = strings.TrimSpace(ns); ns != "" {
					cfg.Hosts.Namespaces = append(cfg.Hosts.Namespaces, ns)
				}
			}
		case o.forceHosts:
			return cfg, werr
		default:
			fmt.Fprintln(notes, "note: cluster Service names will not resolve on this machine, because the hosts file cannot be changed.")
			fmt.Fprintln(notes, "      Run ashctl from an administrator terminal (or with sudo) to enable that.")
		}
	}

	level := slog.LevelWarn
	if o.debug {
		level = slog.LevelDebug
	}
	cfg.Log = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	if o.server != "" {
		cfg.Transport = func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", o.server)
		}
		return cfg, nil
	}
	cluster, err := kube.Load(o.kubeconfig, o.kubeContext)
	if err != nil {
		return cfg, err
	}
	cfg.Transport = func(ctx context.Context) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		pod, err := kube.PickPod(ctx, cluster.Client, o.namespace, o.selector, cfg.Session)
		if err != nil {
			return nil, err
		}
		return cluster.PortForward(ctx, o.namespace, pod, o.port)
	}
	return cfg, nil
}

func runConnect(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	o, err := parseConnect(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if !o.foreground {
		return connectInBackground(ctx, o, args, stdout, stderr)
	}

	if o.session == "" {
		o.session = newSessionID()
	}
	cfg, err := buildConfig(o, stdout, stderr, stdout)
	if err != nil {
		return err
	}
	sess := connect.New(cfg)
	if o.daemonChild {
		dir, err := daemon.DefaultDir()
		if err != nil {
			return err
		}
		// The control channel is how later commands such as "ashctl
		// expose" reach this process.
		ln, err := daemon.ListenControl()
		if err != nil {
			return err
		}
		token, err := daemon.NewToken()
		if err != nil {
			ln.Close()
			return err
		}
		state := daemon.State{
			PID: os.Getpid(), Context: o.kubeContext, Session: o.session, Started: time.Now().UTC(),
			Control: ln.Addr().String(), Token: token,
		}
		if err := dir.Write(state); err != nil {
			ln.Close()
			return err
		}
		defer dir.Clear()
		var cancel context.CancelFunc
		ctx, cancel = dir.WatchStop(ctx)
		defer cancel()
		go daemon.ServeControl(ctx, ln, token, controlHandler(sess))
	}
	return sess.Run(ctx)
}

// controlHandler carries out commands sent to the background process.
func controlHandler(sess *connect.Session) daemon.Handler {
	return func(ctx context.Context, command string, args []string) (string, error) {
		var out strings.Builder
		switch command {
		case "expose":
			exposures, err := parseExposes(args)
			if err != nil {
				return "", err
			}
			for _, e := range exposures {
				exp, err := sess.Expose(ctx, e)
				if err != nil {
					return out.String(), fmt.Errorf("expose %s: %w", e.Name, err)
				}
				for _, line := range connect.Describe(exp) {
					fmt.Fprintf(&out, "exposed   %s\n", line)
				}
			}
		case "unexpose":
			for _, name := range args {
				if err := sess.Unexpose(ctx, name); err != nil {
					return out.String(), err
				}
				fmt.Fprintf(&out, "unexposed %s\n", name)
			}
		case "list":
			for _, exp := range sess.Exposures() {
				for _, line := range connect.Describe(exp) {
					fmt.Fprintf(&out, "  %s\n", line)
				}
			}
		default:
			return "", fmt.Errorf("unknown command %q", command)
		}
		return out.String(), nil
	}
}

// runControl sends a command to the background connection and prints
// what it answers.
func runControl(command string, args []string, stdout io.Writer) error {
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}
	s, running := dir.Running()
	if !running {
		return errors.New("not connected: run \"ashctl connect\" first")
	}
	if s.Control == "" {
		return errors.New("the running connection was started by an older ashctl: disconnect and connect again")
	}
	out, err := daemon.Call(s.Control, s.Token, command, args, time.Minute)
	io.WriteString(stdout, out)
	return err
}

func runExpose(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: ashctl expose NAME:PORT[:LOCALPORT][/PROTOCOL] ...   e.g. ashctl expose b-ash:8080:3000/http")
	}
	// Checked here as well, so a typing mistake is reported even when
	// nothing is connected.
	if _, err := parseExposes(args); err != nil {
		return err
	}
	return runControl("expose", args, stdout)
}

func runUnexpose(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: ashctl unexpose NAME ...")
	}
	return runControl("unexpose", args, stdout)
}

// How long "ashctl connect" waits for the background process to report
// that it is connected before returning anyway.
const connectWait = 90 * time.Second

// connectInBackground starts a detached copy of this program that holds
// the connection, waits until it is connected, and returns.
func connectInBackground(ctx context.Context, o *connectOpts, args []string, stdout, stderr io.Writer) error {
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}
	// Catch wrong flags and a bad kubeconfig here, where the user sees
	// them, rather than in the background process's log. This comes
	// first so a typing mistake is reported as such even when connected.
	checked, err := buildConfig(o, io.Discard, io.Discard, io.Discard)
	if err != nil {
		return err
	}
	if s, running := dir.Running(); running {
		where := ""
		if s.Context != "" {
			where = " to " + s.Context
		}
		return fmt.Errorf("already connected%s (process %d); run \"ashctl disconnect\" first", where, s.PID)
	}
	// With name resolution on, "connected" is not the whole story: wait
	// for the names too, so they work as soon as this command returns.
	awaitNames := checked.Hosts != nil && o.server == ""

	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(string(dir), 0o700); err != nil {
		return err
	}
	logFile, err := os.Create(dir.LogPath())
	if err != nil {
		return err
	}
	defer logFile.Close()

	childArgs := append([]string{"connect", "--foreground", "--daemon-child"}, args...)
	cmd := exec.Command(exe, childArgs...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	daemon.Detach(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start background process: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	// Relay the background process's output until it is connected.
	shown := 0
	relay := func() string {
		body, _ := os.ReadFile(dir.LogPath())
		if len(body) > shown {
			stdout.Write(body[shown:])
			shown = len(body)
		}
		return string(body)
	}
	deadline := time.Now().Add(connectWait)
	for {
		select {
		case err := <-exited:
			relay()
			if err == nil {
				err = errors.New("it exited")
			}
			return fmt.Errorf("the background process stopped before connecting: %w", err)
		case <-ctx.Done():
			// Interrupted while waiting: do not leave it half started.
			dir.RequestStop()
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
		if strings.Contains(relay(), "connected to ") {
			break
		}
		if time.Now().After(deadline) {
			fmt.Fprintln(stdout, "Still connecting in the background. Check with \"ashctl status\".")
			return nil
		}
	}
	// Give the lines that follow the connection (exposures, names) a
	// moment to arrive: stop once the output has been quiet for a second.
	quiet, giveUp := time.Now(), time.Now().Add(8*time.Second)
	for time.Now().Before(giveUp) {
		before := shown
		time.Sleep(200 * time.Millisecond)
		body := relay()
		if shown != before {
			quiet = time.Now()
		}
		if awaitNames && !strings.Contains(body, "resolving ") {
			continue
		}
		if time.Since(quiet) >= time.Second {
			break
		}
	}
	fmt.Fprintf(stdout, "\nRunning in the background (process %d). Use \"ashctl status\" and \"ashctl disconnect\".\n", cmd.Process.Pid)
	return cmd.Process.Release()
}

func noArgs(name string, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("\"ashctl %s\" takes no arguments", name)
	}
	return nil
}

func runStatus(args []string, stdout io.Writer) error {
	if err := noArgs("status", args); err != nil {
		return err
	}
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}
	s, running := dir.Running()
	if !running {
		fmt.Fprintln(stdout, "Not connected.")
		return nil
	}
	where := "the current context"
	if s.Context != "" {
		where = "context " + s.Context
	}
	fmt.Fprintf(stdout, "Connected to %s since %s (process %d, session %s).\n\n",
		where, s.Started.Local().Format("15:04:05"), s.PID, s.Session)
	if s.Control != "" {
		if list, err := daemon.Call(s.Control, s.Token, "list", nil, 10*time.Second); err == nil {
			if list == "" {
				fmt.Fprintln(stdout, "Nothing exposed. Use \"ashctl expose NAME:PORT[:LOCALPORT]\".")
			} else {
				fmt.Fprintf(stdout, "Exposed:\n%s", list)
			}
			fmt.Fprintln(stdout)
		}
	}
	if body, err := os.ReadFile(dir.LogPath()); err == nil {
		fmt.Fprintln(stdout, "Recent activity:")
		stdout.Write(lastLines(body, 10))
	}
	return nil
}

// lastLines returns the final n lines of body.
func lastLines(body []byte, n int) []byte {
	lines := strings.SplitAfter(string(body), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return []byte(strings.Join(lines, ""))
}

// How long "ashctl disconnect" waits for a clean shutdown before it
// ends the process by force.
const disconnectWait = 20 * time.Second

func runDisconnect(args []string, stdout io.Writer) error {
	if err := noArgs("disconnect", args); err != nil {
		return err
	}
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}
	s, running := dir.Running()
	if !running {
		fmt.Fprintln(stdout, "Not connected.")
		return nil
	}
	if err := dir.RequestStop(); err != nil {
		return err
	}
	if daemon.WaitExit(s.PID, disconnectWait) {
		dir.Clear()
		fmt.Fprintln(stdout, "Disconnected.")
		return nil
	}
	// It did not shut down by itself. Ending it by force skips its own
	// clean-up, so say what may be left behind.
	if p, err := os.FindProcess(s.PID); err == nil {
		p.Kill()
	}
	daemon.WaitExit(s.PID, 5*time.Second)
	dir.Clear()
	fmt.Fprintln(stdout, "Disconnected by force. If cluster names still resolve to dead addresses, run \"ashctl cleanup\" from an administrator terminal.")
	return nil
}

func runLogs(args []string, stdout io.Writer) error {
	if err := noArgs("logs", args); err != nil {
		return err
	}
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}
	body, err := os.ReadFile(dir.LogPath())
	if errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(stdout, "No log yet: ashctl has not been connected on this machine.")
		return nil
	}
	if err != nil {
		return err
	}
	_, err = stdout.Write(body)
	return err
}

// runCleanup removes hosts-file entries a previous run left behind, for
// example after the process was killed.
func runCleanup(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("ashctl cleanup", flag.ContinueOnError)
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
