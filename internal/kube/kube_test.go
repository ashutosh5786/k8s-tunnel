package kube

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	ns       = "tunnel"
	selector = "app.kubernetes.io/name=k8s-tunnel"
)

var appLabels = map[string]string{"app.kubernetes.io/name": "k8s-tunnel"}

func pod(name string, labels map[string]string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Status: corev1.PodStatus{Phase: phase, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: status},
		}},
	}
}

func readyPod(name string) *corev1.Pod { return pod(name, appLabels, corev1.PodRunning, true) }

func TestPickPodSkipsPodsThatCannotServe(t *testing.T) {
	terminating := readyPod("tunnel-server-3")
	now := metav1.NewTime(time.Now())
	terminating.DeletionTimestamp = &now
	noCondition := readyPod("tunnel-server-4")
	noCondition.Status.Conditions = nil

	c := fake.NewClientset(
		pod("tunnel-server-0", appLabels, corev1.PodRunning, false), // not ready
		pod("tunnel-server-1", appLabels, corev1.PodPending, true),  // not running
		readyPod("tunnel-server-2"),
		terminating,
		noCondition,
		pod("unrelated", map[string]string{"app": "other"}, corev1.PodRunning, true),
	)
	for i := range 20 {
		got, err := PickPod(context.Background(), c, ns, selector, fmt.Sprintf("session-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if got != "tunnel-server-2" {
			t.Fatalf("picked %q, the only usable pod is tunnel-server-2", got)
		}
	}
}

func TestPickPodIsStablePerKeyAndSpreadsKeys(t *testing.T) {
	c := fake.NewClientset(readyPod("tunnel-server-0"), readyPod("tunnel-server-1"), readyPod("tunnel-server-2"))
	ctx := context.Background()

	used := map[string]int{}
	for i := range 300 {
		key := fmt.Sprintf("session-%d", i)
		first, err := PickPod(ctx, c, ns, selector, key)
		if err != nil {
			t.Fatal(err)
		}
		again, _ := PickPod(ctx, c, ns, selector, key)
		if first != again {
			t.Fatalf("key %q picked %q then %q", key, first, again)
		}
		used[first]++
	}
	if len(used) != 3 {
		t.Fatalf("300 sessions used only %d of 3 replicas: %v", len(used), used)
	}
	for name, n := range used {
		if n < 50 {
			t.Errorf("replica %s got only %d of 300 sessions: %v", name, n, used)
		}
	}
}

func TestPickPodExplainsWhenNothingIsReady(t *testing.T) {
	c := fake.NewClientset(pod("tunnel-server-0", appLabels, corev1.PodRunning, false))
	_, err := PickPod(context.Background(), c, ns, selector, "s1")
	if err == nil {
		t.Fatal("picked a pod although none is ready")
	}
	for _, want := range []string{ns, selector, "1 not ready"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	if _, err := PickPod(context.Background(), fake.NewClientset(), ns, selector, "s1"); err == nil {
		t.Error("picked a pod in an empty namespace")
	}
}

const kubeconfig = `apiVersion: v1
kind: Config
current-context: dev
clusters:
- name: dev-cluster
  cluster: {server: "https://dev.example:6443"}
- name: prod-cluster
  cluster: {server: "https://prod.example:6443"}
users:
- name: me
  user: {token: not-a-real-token}
contexts:
- name: dev
  context: {cluster: dev-cluster, user: me}
- name: prod
  context: {cluster: prod-cluster, user: me}
`

func TestLoadUsesCurrentOrNamedContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	for contextName, wantHost := range map[string]string{
		"":     "https://dev.example:6443",
		"prod": "https://prod.example:6443",
	} {
		cl, err := Load(path, contextName)
		if err != nil {
			t.Fatalf("context %q: %v", contextName, err)
		}
		if cl.Config.Host != wantHost {
			t.Errorf("context %q: host = %q, want %q", contextName, cl.Config.Host, wantHost)
		}
	}
	if _, err := Load(path, "does-not-exist"); err == nil {
		t.Error("an unknown context was accepted")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing"), ""); err == nil {
		t.Error("a missing kubeconfig was accepted")
	}
}
