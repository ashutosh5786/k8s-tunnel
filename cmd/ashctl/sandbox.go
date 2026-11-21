package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/ashutosh5786/ashbox/api/v1alpha1"

	"k8s-tunnel/internal/client/daemon"
	"k8s-tunnel/internal/client/sandbox"
	"k8s-tunnel/internal/kube"
)

const sandboxUsage = `ashctl sandbox manages ashbox sandboxes: changed copies of services that
only requests with the sandbox's routing key reach.

Usage:
  ashctl sandbox create NAME --fork NS/NAME[=CONTAINER:IMAGE] ... [--ttl 4h]
  ashctl sandbox create -f sandbox.yaml
  ashctl sandbox get NAME
  ashctl sandbox list
  ashctl sandbox delete NAME ...

Examples:
  ashctl sandbox create pr-42 --fork shop/cart=app:registry.example.com/cart:pr-42 --wait
  curl -H "baggage: routing-key=pr-42" http://cart.shop:8080/

Every subcommand takes --context and --kubeconfig. Run "ashctl sandbox create -h" for all flags.
`

// sandboxDeps lets tests replace the cluster with a fake.
var newSandboxClient = func(kubeconfig, context string) (*sandbox.Client, error) {
	cluster, err := kube.Load(kubeconfig, context)
	if err != nil {
		return nil, err
	}
	return sandbox.New(cluster.Config)
}

// clusterFlags are the flags every sandbox subcommand takes.
type clusterFlags struct {
	kubeconfig, context, baggageKey string
}

func (c *clusterFlags) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.context, "context", "", "kubeconfig context (default: current context)")
	fs.StringVar(&c.kubeconfig, "kubeconfig", "", "kubeconfig file (default: KUBECONFIG or ~/.kube/config)")
	fs.StringVar(&c.baggageKey, "baggage-key", envOr("ASHBOX_BAGGAGE_KEY", "routing-key"), "baggage entry the ashbox controller routes on (env ASHBOX_BAGGAGE_KEY)")
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func runSandbox(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, sandboxUsage)
		return errors.New("no sandbox command given")
	}
	switch args[0] {
	case "create":
		return runSandboxCreate(ctx, args[1:], stdout, stderr)
	case "get":
		return runSandboxGet(ctx, args[1:], stdout, stderr)
	case "list", "ls":
		return runSandboxList(ctx, args[1:], stdout, stderr)
	case "delete", "rm":
		return runSandboxDelete(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, sandboxUsage)
		return nil
	default:
		fmt.Fprint(stderr, sandboxUsage)
		return fmt.Errorf("unknown sandbox command %q", args[0])
	}
}

// parseInterspersed parses flags that may come before or after positional
// arguments, as in "ashctl sandbox create pr-42 --fork ...".
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

type createOpts struct {
	cluster     clusterFlags
	forks       listFlag
	file        string
	ttl         string
	description string
	wait        bool
	timeout     time.Duration
}

func runSandboxCreate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var o createOpts
	fs := flag.NewFlagSet("ashctl sandbox create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	o.cluster.bind(fs)
	fs.Var(&o.forks, "fork", "workload to fork: [rollout/]NAMESPACE/NAME[=CONTAINER:IMAGE] (repeatable)")
	fs.StringVar(&o.file, "f", "", "create from a Sandbox YAML file")
	fs.StringVar(&o.ttl, "ttl", "", "lifetime such as 4h or 14d (default 4h; from the file with -f)")
	fs.StringVar(&o.description, "description", "", "text shown with the sandbox")
	fs.BoolVar(&o.wait, "wait", false, "wait until the sandbox is Ready or Failed")
	fs.DurationVar(&o.timeout, "timeout", 5*time.Minute, "how long --wait waits")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	sb, err := buildSandbox(&o, pos)
	if err != nil {
		return err
	}
	sb.Labels = setLabel(sb.Labels, sandbox.LabelUser, sandbox.NameFromUser(defaultUser()))

	c, err := newSandboxClient(o.cluster.kubeconfig, o.cluster.context)
	if err != nil {
		return err
	}
	created, err := c.Create(ctx, sb)
	if apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("sandbox %s already exists: delete it first, or pick another name", sb.Name)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "created sandbox %s\n\n", created.Name)

	if o.wait {
		if created, err = waitSettled(ctx, c, created.Name, o.timeout, stdout); err != nil {
			return err
		}
	}
	sandbox.Describe(stdout, created, o.cluster.baggageKey, time.Now())
	if o.wait && created.Status.Phase == v1alpha1.PhaseFailed {
		return fmt.Errorf("sandbox %s failed", created.Name)
	}
	return nil
}

// buildSandbox makes the Sandbox from -f or from the flags.
func buildSandbox(o *createOpts, pos []string) (*v1alpha1.Sandbox, error) {
	var sb *v1alpha1.Sandbox
	if o.file != "" {
		if len(o.forks) > 0 {
			return nil, errors.New("use either -f or --fork, not both")
		}
		var err error
		if sb, err = sandbox.LoadFile(o.file); err != nil {
			return nil, err
		}
		switch {
		case len(pos) == 1 && sb.Name != "" && sb.Name != pos[0]:
			return nil, fmt.Errorf("name %q differs from the file's name %q", pos[0], sb.Name)
		case len(pos) == 1:
			sb.Name = pos[0]
		case len(pos) > 1:
			return nil, fmt.Errorf("unexpected argument %q", pos[1])
		}
	} else {
		if len(pos) != 1 {
			return nil, errors.New("usage: ashctl sandbox create NAME --fork NS/NAME[=CONTAINER:IMAGE] ... (or -f FILE)")
		}
		if len(o.forks) == 0 {
			return nil, errors.New("give at least one --fork, or use -f FILE")
		}
		forks, err := sandbox.ParseForks(o.forks)
		if err != nil {
			return nil, err
		}
		sb = &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: pos[0]}}
		sb.Spec.Forks = forks
	}

	if err := sandbox.ValidName(sb.Name); err != nil {
		return nil, err
	}
	if o.description != "" {
		sb.Spec.Description = o.description
	}
	switch {
	case o.ttl != "":
		sb.Spec.TTL = &v1alpha1.TTL{Duration: o.ttl}
	case sb.Spec.TTL == nil:
		sb.Spec.TTL = &v1alpha1.TTL{Duration: "4h"}
	}
	return sb, nil
}

func setLabel(m map[string]string, k, v string) map[string]string {
	if v == "" {
		return m
	}
	if m == nil {
		m = map[string]string{}
	}
	m[k] = v
	return m
}

// waitSettled polls until the sandbox is Ready or Failed.
func waitSettled(ctx context.Context, c *sandbox.Client, name string, timeout time.Duration, stdout io.Writer) (*v1alpha1.Sandbox, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := v1alpha1.Phase("")
	for {
		sb, err := c.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		if p := sb.Status.Phase; p != last {
			if p != "" {
				fmt.Fprintf(stdout, "phase %s\n", p)
			}
			last = p
		}
		switch sb.Status.Phase {
		case v1alpha1.PhaseReady, v1alpha1.PhaseFailed, v1alpha1.PhaseDegraded:
			fmt.Fprintln(stdout)
			return sb, nil
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf("sandbox %s not ready after %s (phase %q); check \"ashctl sandbox get %s\"", name, timeout, sb.Status.Phase, name)
			}
			return nil, ctx.Err()
		case <-time.After(waitPoll):
		}
	}
}

var waitPoll = 2 * time.Second

func runSandboxGet(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var cf clusterFlags
	fs := flag.NewFlagSet("ashctl sandbox get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cf.bind(fs)
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: ashctl sandbox get NAME")
	}
	c, err := newSandboxClient(cf.kubeconfig, cf.context)
	if err != nil {
		return err
	}
	sb, err := c.Get(ctx, pos[0])
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("no sandbox %q", pos[0])
	}
	if err != nil {
		return err
	}
	sandbox.Describe(stdout, sb, cf.baggageKey, time.Now())
	return nil
}

func runSandboxList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var cf clusterFlags
	var mine bool
	fs := flag.NewFlagSet("ashctl sandbox list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cf.bind(fs)
	fs.BoolVar(&mine, "mine", false, "only sandboxes created by you with ashctl")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(pos) > 0 {
		return fmt.Errorf("unexpected argument %q", pos[0])
	}
	c, err := newSandboxClient(cf.kubeconfig, cf.context)
	if err != nil {
		return err
	}
	selector := ""
	if mine {
		selector = sandbox.LabelUser + "=" + sandbox.NameFromUser(defaultUser())
	}
	items, err := c.List(ctx, selector)
	if err != nil {
		return err
	}
	sandbox.List(stdout, items, time.Now())
	return nil
}

func runSandboxDelete(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var cf clusterFlags
	fs := flag.NewFlagSet("ashctl sandbox delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cf.bind(fs)
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(pos) == 0 {
		return errors.New("usage: ashctl sandbox delete NAME ...")
	}
	c, err := newSandboxClient(cf.kubeconfig, cf.context)
	if err != nil {
		return err
	}
	for _, name := range pos {
		err := c.Delete(ctx, name)
		if apierrors.IsNotFound(err) {
			fmt.Fprintf(stdout, "no sandbox %s\n", name)
			continue
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "deleted sandbox %s\n", name)
	}
	return nil
}

// ---- ashctl local ----

const localUsage = `usage: ashctl local [rollout/]NAMESPACE/NAME --port PORT[:LOCALPORT] ... [--sandbox NAME]

Sends requests that carry your routing key for a cluster workload to a
service on this machine. Needs a running connection ("ashctl connect").

  ashctl local shop/checkout --port 8080:3000
  curl -H "baggage: routing-key=<you>" http://checkout.shop:8080/

"ashctl disconnect" deletes the sandbox again.
`

type localOpts struct {
	cluster         clusterFlags
	ports           listFlag
	sandboxName     string
	protocol        string
	ttl             string
	tunnelNamespace string
}

func runLocal(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var o localOpts
	fs := flag.NewFlagSet("ashctl local", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, localUsage); fs.PrintDefaults() }
	o.cluster.bind(fs)
	fs.Var(&o.ports, "port", "workload port to send here: PORT[:LOCALPORT] (repeatable)")
	fs.StringVar(&o.sandboxName, "sandbox", "", "sandbox (and routing key) to use (default: your user name)")
	fs.StringVar(&o.protocol, "protocol", "http", "protocol of the ports: http, grpc or tcp")
	fs.StringVar(&o.ttl, "ttl", "8h", "lifetime of a new sandbox, in case disconnect never runs")
	fs.StringVar(&o.tunnelNamespace, "tunnel-namespace", "k8s-tunnel", "namespace the tunnel server publishes Services in")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(pos) != 1 || len(o.ports) == 0 {
		return errors.New(localUsage)
	}
	from, err := sandbox.ParseWorkload(pos[0])
	if err != nil {
		return err
	}
	if o.sandboxName == "" {
		o.sandboxName = sandbox.NameFromUser(defaultUser())
	}
	if err := sandbox.ValidName(o.sandboxName); err != nil {
		return err
	}

	type portMap struct{ port, local int }
	var maps []portMap
	for _, p := range o.ports {
		remote, local, hasLocal := strings.Cut(p, ":")
		rp, err := parsePort(remote)
		if err != nil {
			return fmt.Errorf("invalid --port %q: %v", p, err)
		}
		lp := rp
		if hasLocal {
			if lp, err = parsePort(local); err != nil {
				return fmt.Errorf("invalid --port %q: %v", p, err)
			}
		}
		maps = append(maps, portMap{rp, lp})
	}

	// The connection must be running: it carries the traffic.
	dir, err := daemon.DefaultDir()
	if err != nil {
		return err
	}
	state, running := dir.Running()
	if !running {
		return errors.New("not connected: run \"ashctl connect\" first")
	}
	kubeContext := o.cluster.context
	if kubeContext == "" {
		kubeContext = state.Context
	}

	// 1. Publish the local ports as a Service through the tunnel.
	exposure := from.Name + "-" + o.sandboxName
	if err := sandbox.ValidName(exposure); err != nil {
		return fmt.Errorf("Service name %q is too long or invalid; use a shorter --sandbox", exposure)
	}
	var specs []string
	for _, m := range maps {
		spec := fmt.Sprintf("%s:%d:%d", exposure, m.port, m.local)
		if o.protocol != "" && o.protocol != "tcp" {
			spec += "/" + o.protocol
		}
		specs = append(specs, spec)
	}
	if err := runControl("expose", specs, stdout); err != nil {
		return err
	}

	// 2. Create or update the sandbox with a local entry for the workload.
	c, err := newSandboxClient(o.cluster.kubeconfig, kubeContext)
	if err != nil {
		return err
	}
	entry := v1alpha1.Local{
		Name:   from.Name,
		From:   from,
		Tunnel: v1alpha1.ServiceRef{Namespace: o.tunnelNamespace, Name: exposure},
	}
	for _, m := range maps {
		entry.Mappings = append(entry.Mappings, v1alpha1.LocalMapping{Port: int32(m.port), TunnelPort: int32(m.port)})
	}
	sb, err := upsertLocal(ctx, c, o.sandboxName, entry, state.Session, o.ttl)
	if err != nil {
		return err
	}
	if err := rememberLocal(dir, localRecord{Kubeconfig: o.cluster.kubeconfig, Context: kubeContext, Session: state.Session, Sandbox: sb.Name}); err != nil {
		fmt.Fprintf(stderr, "note: could not record the sandbox for clean-up on disconnect: %v\n", err)
	}

	fmt.Fprintf(stdout, "\nRequests for %s/%s with this header now reach this machine:\n  %s\n",
		from.Namespace, from.Name, sandbox.Header(o.cluster.baggageKey, sb.Name))
	fmt.Fprintf(stdout, "Check with \"ashctl sandbox get %s\". \"ashctl disconnect\" removes it.\n", sb.Name)
	return nil
}

// upsertLocal adds or replaces a local entry in the named sandbox, creating
// the sandbox when needed. It refuses to change a sandbox that another
// ashctl connection created.
func upsertLocal(ctx context.Context, c *sandbox.Client, name string, entry v1alpha1.Local, session, ttl string) (*v1alpha1.Sandbox, error) {
	sb, err := c.Get(ctx, name)
	if apierrors.IsNotFound(err) {
		sb = &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name}}
		sb.Labels = setLabel(sb.Labels, sandbox.LabelSession, session)
		sb.Labels = setLabel(sb.Labels, sandbox.LabelUser, sandbox.NameFromUser(defaultUser()))
		sb.Spec.Description = "local development with ashctl"
		sb.Spec.TTL = &v1alpha1.TTL{Duration: ttl}
		sb.Spec.Local = []v1alpha1.Local{entry}
		return c.Create(ctx, sb)
	}
	if err != nil {
		return nil, err
	}
	if owner := sb.Labels[sandbox.LabelSession]; owner != session {
		return nil, fmt.Errorf("sandbox %s belongs to another connection or was not made by \"ashctl local\"; use --sandbox to pick another name", name)
	}
	replaced := false
	for i := range sb.Spec.Local {
		if sb.Spec.Local[i].Name == entry.Name {
			sb.Spec.Local[i] = entry
			replaced = true
		}
	}
	if !replaced {
		sb.Spec.Local = append(sb.Spec.Local, entry)
	}
	return c.Update(ctx, sb)
}

// localRecord remembers a sandbox made by "ashctl local", so that
// "ashctl disconnect" can delete it.
type localRecord struct {
	Kubeconfig string `json:"kubeconfig,omitempty"`
	Context    string `json:"context,omitempty"`
	Session    string `json:"session"`
	Sandbox    string `json:"sandbox"`
}

func localRecordPath(dir daemon.Dir) string {
	return filepath.Join(string(dir), "local-sandboxes.json")
}

func readLocal(dir daemon.Dir) ([]localRecord, error) {
	body, err := os.ReadFile(localRecordPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var recs []localRecord
	err = json.Unmarshal(body, &recs)
	return recs, err
}

func rememberLocal(dir daemon.Dir, rec localRecord) error {
	recs, _ := readLocal(dir)
	for _, r := range recs {
		if r == rec {
			return nil
		}
	}
	recs = append(recs, rec)
	body, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(localRecordPath(dir), body, 0o600)
}

// deleteLocalSandboxes deletes the sandboxes "ashctl local" made, including
// any left by an earlier connection that ended without a disconnect.
// Failures are reported, never fatal: disconnect must still work when the
// cluster is unreachable.
func deleteLocalSandboxes(ctx context.Context, dir daemon.Dir, stdout io.Writer) {
	recs, err := readLocal(dir)
	if err != nil || len(recs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for _, r := range recs {
		c, err := newSandboxClient(r.Kubeconfig, r.Context)
		if err == nil {
			err = c.Delete(ctx, r.Sandbox)
		}
		switch {
		case err == nil:
			fmt.Fprintf(stdout, "deleted sandbox %s\n", r.Sandbox)
		case apierrors.IsNotFound(err):
		default:
			fmt.Fprintf(stdout, "note: could not delete sandbox %s: %v (delete it with \"ashctl sandbox delete %s\")\n", r.Sandbox, err, r.Sandbox)
		}
	}
	os.Remove(localRecordPath(dir))
}
