package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	v1alpha1 "github.com/ashutosh5786/ashbox/api/v1alpha1"

	"k8s-tunnel/internal/client/daemon"
	"k8s-tunnel/internal/client/sandbox"
)

// fakeSandboxes replaces the cluster with a fake for one test and records
// the kubeconfig contexts asked for.
func fakeSandboxes(t *testing.T) (*sandbox.Client, *[]string) {
	t.Helper()
	t.Setenv("ASHCTL_HOME", t.TempDir())
	d := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{sandbox.Resource: "SandboxList"})
	c := sandbox.NewFromDynamic(d)
	var contexts []string
	old := newSandboxClient
	newSandboxClient = func(_, ctx string) (*sandbox.Client, error) {
		contexts = append(contexts, ctx)
		return c, nil
	}
	t.Cleanup(func() { newSandboxClient = old })
	return c, &contexts
}

func runSB(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := run(context.Background(), args, &out, &errOut)
	return out.String() + errOut.String(), err
}

func TestSandboxCreateFromFlags(t *testing.T) {
	c, _ := fakeSandboxes(t)
	out, err := runSB(t, "sandbox", "create", "pr-42", "--fork", "shop/cart=app:cart:pr-42", "--fork", "shop/match", "--ttl", "2h")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, "created sandbox pr-42") || !strings.Contains(out, "baggage: routing-key=pr-42") {
		t.Errorf("output:\n%s", out)
	}
	sb, err := c.Get(context.Background(), "pr-42")
	if err != nil {
		t.Fatal(err)
	}
	if len(sb.Spec.Forks) != 2 || sb.Spec.TTL.Duration != "2h" || sb.Labels[sandbox.LabelUser] == "" {
		t.Fatalf("sandbox = %+v", sb)
	}

	if _, err := runSB(t, "sandbox", "create", "pr-42", "--fork", "shop/cart"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second create: %v", err)
	}
}

func TestSandboxCreateFromFile(t *testing.T) {
	c, _ := fakeSandboxes(t)
	file := filepath.Join(t.TempDir(), "sb.yaml")
	body := "apiVersion: ashbox.dev/v1alpha1\nkind: Sandbox\nmetadata:\n  name: from-file\nspec:\n  forks:\n    - forkOf: {kind: Deployment, namespace: shop, name: cart}\n"
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runSB(t, "sandbox", "create", "-f", file); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	sb, err := c.Get(context.Background(), "from-file")
	if err != nil || sb.Spec.TTL == nil || sb.Spec.TTL.Duration != "4h" {
		t.Fatalf("sandbox = %+v, %v (want default ttl 4h)", sb, err)
	}

	if _, err := runSB(t, "sandbox", "create", "other-name", "-f", file); err == nil {
		t.Fatal("name that differs from the file: want error")
	}
}

func TestSandboxCreateErrors(t *testing.T) {
	fakeSandboxes(t)
	for _, args := range [][]string{
		{"sandbox", "create", "pr-42"},
		{"sandbox", "create", "--fork", "shop/cart"},
		{"sandbox", "create", "PR_42", "--fork", "shop/cart"},
		{"sandbox", "create", "pr-42", "--fork", "cart"},
		{"sandbox", "create", "pr-42", "--fork", "shop/cart", "-f", "x.yaml"},
		{"sandbox", "nope"},
		{"sandbox"},
	} {
		if _, err := runSB(t, args...); err == nil {
			t.Errorf("%v: want error", args)
		}
	}
}

func TestSandboxGetListDelete(t *testing.T) {
	c, contexts := fakeSandboxes(t)
	ctx := context.Background()
	for _, name := range []string{"a1", "b2"} {
		sb := &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name}}
		sb.Status.Phase = v1alpha1.PhaseReady
		if _, err := c.Create(ctx, sb); err != nil {
			t.Fatal(err)
		}
	}

	out, err := runSB(t, "sandbox", "get", "a1", "--context", "dev")
	if err != nil || !strings.Contains(out, "Sandbox a1 - Ready") {
		t.Fatalf("get: %v\n%s", err, out)
	}
	if (*contexts)[len(*contexts)-1] != "dev" {
		t.Errorf("--context not used: %v", *contexts)
	}
	if _, err := runSB(t, "sandbox", "get", "zz"); err == nil || !strings.Contains(err.Error(), "no sandbox") {
		t.Errorf("get missing: %v", err)
	}

	out, err = runSB(t, "sandbox", "list")
	if err != nil || !strings.Contains(out, "a1") || !strings.Contains(out, "b2") {
		t.Fatalf("list: %v\n%s", err, out)
	}

	out, err = runSB(t, "sandbox", "delete", "a1", "zz")
	if err != nil || !strings.Contains(out, "deleted sandbox a1") || !strings.Contains(out, "no sandbox zz") {
		t.Fatalf("delete: %v\n%s", err, out)
	}
	if items, _ := c.List(ctx, ""); len(items) != 1 {
		t.Fatalf("after delete: %d sandboxes", len(items))
	}
}

func TestWaitSettled(t *testing.T) {
	c, _ := fakeSandboxes(t)
	sb := &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "w1"}}
	sb.Status.Phase = v1alpha1.PhaseReady
	if _, err := c.Create(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	got, err := waitSettled(context.Background(), c, "w1", time.Second, &out)
	if err != nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("waitSettled = %v, %v", got, err)
	}

	pending := &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "w2"}}
	if _, err := c.Create(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	old := waitPoll
	waitPoll = 10 * time.Millisecond
	defer func() { waitPoll = old }()
	if _, err := waitSettled(context.Background(), c, "w2", 50*time.Millisecond, &out); err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("timeout: %v", err)
	}
}

func TestUpsertLocal(t *testing.T) {
	c, _ := fakeSandboxes(t)
	ctx := context.Background()
	entry := func(name string, port int32) v1alpha1.Local {
		return v1alpha1.Local{
			Name: name, From: v1alpha1.WorkloadRef{Kind: v1alpha1.KindDeployment, Namespace: "shop", Name: name},
			Tunnel:   v1alpha1.ServiceRef{Namespace: "k8s-tunnel", Name: name + "-ash"},
			Mappings: []v1alpha1.LocalMapping{{Port: port, TunnelPort: port}},
		}
	}

	sb, err := upsertLocal(ctx, c, "ash", entry("checkout", 8080), "s1", "8h")
	if err != nil {
		t.Fatal(err)
	}
	if sb.Labels[sandbox.LabelSession] != "s1" || sb.Spec.TTL.Duration != "8h" || len(sb.Spec.Local) != 1 {
		t.Fatalf("created = %+v", sb)
	}

	if sb, err = upsertLocal(ctx, c, "ash", entry("cart", 9090), "s1", "8h"); err != nil || len(sb.Spec.Local) != 2 {
		t.Fatalf("add second entry: %+v, %v", sb, err)
	}
	if sb, err = upsertLocal(ctx, c, "ash", entry("checkout", 8081), "s1", "8h"); err != nil {
		t.Fatal(err)
	}
	if len(sb.Spec.Local) != 2 || sb.Spec.Local[0].Mappings[0].Port != 8081 {
		t.Fatalf("replace entry: %+v", sb.Spec.Local)
	}

	if _, err := upsertLocal(ctx, c, "ash", entry("x", 1), "s2", "8h"); err == nil || !strings.Contains(err.Error(), "another connection") {
		t.Fatalf("other session: %v", err)
	}
}

func TestLocalRecordsAndDisconnectCleanup(t *testing.T) {
	c, contexts := fakeSandboxes(t)
	ctx := context.Background()
	dir, err := daemon.DefaultDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(string(dir), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"ash", "keep"} {
		if _, err := c.Create(ctx, &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	rec := localRecord{Context: "dev", Session: "s1", Sandbox: "ash"}
	if err := rememberLocal(dir, rec); err != nil {
		t.Fatal(err)
	}
	_ = rememberLocal(dir, rec) // no duplicates
	_ = rememberLocal(dir, localRecord{Context: "dev", Session: "old", Sandbox: "gone"})
	if recs, _ := readLocal(dir); len(recs) != 2 {
		t.Fatalf("records = %+v", recs)
	}

	// Not connected: disconnect still removes recorded sandboxes.
	out, err := runSB(t, "disconnect")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "deleted sandbox ash") || !strings.Contains(out, "Not connected") {
		t.Errorf("output:\n%s", out)
	}
	if strings.Contains(out, "could not delete") {
		t.Errorf("a missing sandbox must not be reported:\n%s", out)
	}
	if _, err := c.Get(ctx, "keep"); err != nil {
		t.Error("a sandbox not made by ashctl local was deleted")
	}
	if recs, _ := readLocal(dir); len(recs) != 0 {
		t.Errorf("records left: %+v", recs)
	}
	if (*contexts)[0] != "dev" {
		t.Errorf("recorded context not used: %v", *contexts)
	}

	// Nothing recorded: no cluster contact at all.
	n := len(*contexts)
	if _, err := runSB(t, "disconnect"); err != nil {
		t.Fatal(err)
	}
	if len(*contexts) != n {
		t.Error("disconnect contacted the cluster with nothing recorded")
	}
}

func TestLocalArgErrors(t *testing.T) {
	fakeSandboxes(t)
	for _, args := range [][]string{
		{"local"},
		{"local", "shop/checkout"},
		{"local", "checkout", "--port", "8080"},
		{"local", "shop/checkout", "--port", "x"},
		{"local", "shop/checkout", "--port", "8080:0"},
		{"local", "shop/checkout", "--port", "8080", "--sandbox", "Bad_Name"},
	} {
		if _, err := runSB(t, args...); err == nil {
			t.Errorf("%v: want error", args)
		}
	}
	// Valid, but nothing is connected.
	if _, err := runSB(t, "local", "shop/checkout", "--port", "8080:3000"); err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("not connected: %v", err)
	}
}

func newTestFlagSet(cf *clusterFlags) *flag.FlagSet {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cf.bind(fs)
	return fs
}

func TestParseInterspersed(t *testing.T) {
	var cf clusterFlags
	fs := newTestFlagSet(&cf)
	pos, err := parseInterspersed(fs, []string{"a", "--context", "dev", "b"})
	if err != nil || len(pos) != 2 || pos[1] != "b" || cf.context != "dev" {
		t.Fatalf("pos = %v, ctx = %q, err = %v", pos, cf.context, err)
	}
}
