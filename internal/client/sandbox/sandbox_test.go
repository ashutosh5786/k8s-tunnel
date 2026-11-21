package sandbox

import (
	"bytes"
	"context"
	"errors"
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
)

func fakeClient() *Client {
	scheme := runtime.NewScheme()
	d := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{Resource: "SandboxList"})
	return NewFromDynamic(d)
}

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()

	sb := &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "pr-42", Labels: map[string]string{LabelSession: "s1"}}}
	sb.Spec.Forks = []v1alpha1.Fork{{ForkOf: v1alpha1.WorkloadRef{Kind: v1alpha1.KindDeployment, Namespace: "shop", Name: "cart"}}}
	if _, err := c.Create(ctx, sb); err != nil {
		t.Fatal(err)
	}
	other := &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "other"}}
	if _, err := c.Create(ctx, other); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(ctx, "pr-42")
	if err != nil || got.Spec.Forks[0].ForkOf.Name != "cart" {
		t.Fatalf("get = %+v, %v", got, err)
	}

	got.Spec.Description = "changed"
	if _, err := c.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if got, _ = c.Get(ctx, "pr-42"); got.Spec.Description != "changed" {
		t.Fatal("update lost")
	}

	all, err := c.List(ctx, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("list = %d, %v", len(all), err)
	}

	deleted, err := c.DeleteSession(ctx, "s1")
	if err != nil || len(deleted) != 1 || deleted[0] != "pr-42" {
		t.Fatalf("DeleteSession = %v, %v", deleted, err)
	}
	if all, _ = c.List(ctx, ""); len(all) != 1 || all[0].Name != "other" {
		t.Fatalf("after DeleteSession: %+v", all)
	}
	if err := c.Delete(ctx, "other"); err != nil {
		t.Fatal(err)
	}
}

func TestParseForks(t *testing.T) {
	forks, err := ParseForks([]string{
		"shop/cart=app:registry.example.com:5000/cart:pr-42",
		"shop/cart=sidecar:proxy:1.2",
		"rollout/shop/checkout",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(forks) != 2 {
		t.Fatalf("forks = %+v", forks)
	}
	imgs := forks[0].Customizations.Images
	if len(imgs) != 2 || imgs[0].Container != "app" || imgs[0].Image != "registry.example.com:5000/cart:pr-42" {
		t.Errorf("images = %+v", imgs)
	}
	if forks[1].ForkOf.Kind != v1alpha1.KindRollout || forks[1].ForkOf.Name != "checkout" || forks[1].Customizations != nil {
		t.Errorf("rollout fork = %+v", forks[1])
	}

	for _, bad := range [][]string{
		{"cart"},
		{"a/b/c/d"},
		{"shop/"},
		{"shop/cart=app"},
		{"shop/cart=:img"},
		{"shop/cart=app:x", "shop/cart=app:y"},
	} {
		if _, err := ParseForks(bad); err == nil {
			t.Errorf("ParseForks(%v): want error", bad)
		}
	}
}

func TestNames(t *testing.T) {
	if NameFromUser(`Ash.Smith_2`) != "ash-smith-2" {
		t.Errorf("NameFromUser = %q", NameFromUser(`Ash.Smith_2`))
	}
	for _, ok := range []string{"pr-42", "a", "ash"} {
		if err := ValidName(ok); err != nil {
			t.Errorf("ValidName(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "PR-42", "-a", "a-", "a_b", strings.Repeat("a", 64)} {
		if err := ValidName(bad); err == nil {
			t.Errorf("ValidName(%q): want error", bad)
		}
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	good := write("good.yaml", `
apiVersion: ashbox.dev/v1alpha1
kind: Sandbox
metadata:
  name: pr-42
spec:
  ttl: {duration: 4h}
  forks:
    - forkOf: {namespace: shop, name: cart}
`)
	sb, err := LoadFile(good)
	if err != nil || sb.Name != "pr-42" || sb.Spec.TTL.Duration != "4h" {
		t.Fatalf("LoadFile = %+v, %v", sb, err)
	}

	for name, body := range map[string]string{
		"typo.yaml": "spec:\n  forkz: []\n",
		"api.yaml":  "apiVersion: v1\nkind: Sandbox\n",
		"kind.yaml": "apiVersion: ashbox.dev/v1alpha1\nkind: Pod\n",
	} {
		if _, err := LoadFile(write(name, body)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestDescribe(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	down := metav1.NewTime(now.Add(-7 * time.Minute))
	exp := metav1.NewTime(now.Add(3*time.Hour + 30*time.Minute))
	sb := &v1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-42"},
		Spec: v1alpha1.SandboxSpec{
			Forks: []v1alpha1.Fork{
				{ForkOf: v1alpha1.WorkloadRef{Namespace: "shop", Name: "cart"},
					Customizations: &v1alpha1.Customizations{Images: []v1alpha1.ImageOverride{{Container: "app", Image: "cart:pr-42"}}}},
				{ForkOf: v1alpha1.WorkloadRef{Namespace: "shop", Name: "match"}},
			},
			Local: []v1alpha1.Local{{
				Name: "checkout", From: v1alpha1.WorkloadRef{Namespace: "shop", Name: "checkout"},
				Tunnel:   v1alpha1.ServiceRef{Namespace: "k8s-tunnel", Name: "checkout-ash"},
				Mappings: []v1alpha1.LocalMapping{{Port: 8080}},
			}},
		},
		Status: v1alpha1.SandboxStatus{
			Phase: v1alpha1.PhaseDegraded, RoutingKey: "pr-42", ExpiresAt: &exp,
			Forks: []v1alpha1.ForkStatus{
				{Name: "cart-pr-42", Ready: true},
				{Name: "match-pr-42", NotReadySince: &down, Reason: "CrashLoopBackOff"},
			},
		},
	}
	var buf bytes.Buffer
	Describe(&buf, sb, "routing-key", now)
	out := buf.String()
	for _, want := range []string{
		"Sandbox pr-42 - Degraded (1/2 forks ready)",
		"baggage: routing-key=pr-42",
		"in 3h30m",
		"app=cart:pr-42",
		"NOT READY for 7m: CrashLoopBackOff",
		"shop/checkout port 8080 -> laptop (tunnel Service k8s-tunnel/checkout-ash)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}

	buf.Reset()
	List(&buf, []v1alpha1.Sandbox{*sb}, now)
	if !strings.Contains(buf.String(), "pr-42") || !strings.Contains(buf.String(), "1/2") {
		t.Errorf("list:\n%s", buf.String())
	}
	buf.Reset()
	List(&buf, nil, now)
	if !strings.Contains(buf.String(), "No sandboxes") {
		t.Errorf("empty list: %q", buf.String())
	}
}

func TestWrapNotInstalled(t *testing.T) {
	if !errors.Is(ErrNotInstalled, ErrNotInstalled) {
		t.Fatal("sentinel")
	}
	if wrap(nil) != nil {
		t.Fatal("wrap(nil) must be nil")
	}
}
