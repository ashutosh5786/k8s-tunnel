package kubepub

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"k8s-tunnel/internal/server"
)

const ns = "tunnel"

func exposure(name, user, session string, serverPort int) server.Exposure {
	return server.Exposure{Name: name, User: user, Session: session, Ports: []server.Port{
		{Name: "http", Port: 8080, ServerPort: serverPort, AppProtocol: "http"},
	}}
}

func get(t *testing.T, c *fake.Clientset, name string) *corev1.Service {
	t.Helper()
	svc, err := c.CoreV1().Services(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return svc
}

func exists(c *fake.Clientset, name string) bool {
	_, err := c.CoreV1().Services(ns).Get(context.Background(), name, metav1.GetOptions{})
	return !apierrors.IsNotFound(err)
}

func TestPublishCreatesServiceForThisReplica(t *testing.T) {
	c := fake.NewClientset()
	p := &Publisher{Client: c, Namespace: ns, PodName: "tunnel-server-0"}

	host, err := p.Publish(context.Background(), exposure("b-ash", "ash", "s1", 20001))
	if err != nil {
		t.Fatal(err)
	}
	if host != "b-ash.tunnel.svc" {
		t.Errorf("host = %q", host)
	}
	svc := get(t, c, "b-ash")
	if got := svc.Spec.Selector[podNameLabel]; got != "tunnel-server-0" {
		t.Errorf("selector pod = %q", got)
	}
	port := svc.Spec.Ports[0]
	if port.Port != 8080 || port.TargetPort.IntValue() != 20001 || *port.AppProtocol != "http" {
		t.Errorf("port = %+v", port)
	}
	if svc.Labels[SessionLabel] != "s1" || svc.Annotations[UserAnnotation] != "ash" {
		t.Errorf("labels = %v annotations = %v", svc.Labels, svc.Annotations)
	}
}

func TestReconnectToAnotherReplicaMovesService(t *testing.T) {
	c := fake.NewClientset()
	p0 := &Publisher{Client: c, Namespace: ns, PodName: "tunnel-server-0"}
	p1 := &Publisher{Client: c, Namespace: ns, PodName: "tunnel-server-1"}
	ctx := context.Background()

	if _, err := p0.Publish(ctx, exposure("b-ash", "ash", "s1", 20001)); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Publish(ctx, exposure("b-ash", "ash", "s1", 20555)); err != nil {
		t.Fatal(err)
	}
	svc := get(t, c, "b-ash")
	if svc.Spec.Selector[podNameLabel] != "tunnel-server-1" || svc.Spec.Ports[0].TargetPort.IntValue() != 20555 {
		t.Fatalf("service not re-pointed: %+v", svc.Spec)
	}

	// The old replica's delayed clean-up must not delete what replica 1 owns.
	if err := p0.Unpublish(ctx, "b-ash", "s1"); err != nil {
		t.Fatal(err)
	}
	if !exists(c, "b-ash") {
		t.Fatal("old replica deleted a service owned by the new replica")
	}
	if err := p1.Unpublish(ctx, "b-ash", "s1"); err != nil {
		t.Fatal(err)
	}
	if exists(c, "b-ash") {
		t.Fatal("owner could not delete its service")
	}
}

func TestPublishRefusesForeignServices(t *testing.T) {
	unmanaged := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: ns}}
	c := fake.NewClientset(unmanaged)
	p := &Publisher{Client: c, Namespace: ns, PodName: "tunnel-server-0"}
	ctx := context.Background()

	if _, err := p.Publish(ctx, exposure("payments", "ash", "s1", 20001)); err == nil {
		t.Error("overwrote a service the tunnel does not manage")
	}
	if err := p.Unpublish(ctx, "payments", "s1"); err != nil || !exists(c, "payments") {
		t.Error("unpublish touched a service the tunnel does not manage")
	}

	if _, err := p.Publish(ctx, exposure("b-shared", "ash", "s1", 20001)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Publish(ctx, exposure("b-shared", "sam", "s2", 20002)); err == nil {
		t.Error("another user took over a service in use")
	}
	// The same user with a new session (e.g. after a client restart) may.
	if _, err := p.Publish(ctx, exposure("b-shared", "ash", "s3", 20003)); err != nil {
		t.Errorf("same user could not reclaim: %v", err)
	}
}

func TestSweepRemovesOnlyThisReplicasStaleServices(t *testing.T) {
	c := fake.NewClientset()
	p0 := &Publisher{Client: c, Namespace: ns, PodName: "tunnel-server-0"}
	p1 := &Publisher{Client: c, Namespace: ns, PodName: "tunnel-server-1"}
	ctx := context.Background()
	for _, e := range []struct {
		p    *Publisher
		name string
	}{{p0, "stale"}, {p0, "alive"}, {p1, "other-replica"}} {
		if _, err := e.p.Publish(ctx, exposure(e.name, "ash", "s-"+e.name, 20001)); err != nil {
			t.Fatal(err)
		}
	}

	if err := p0.Sweep(ctx, func(name string) bool { return name == "alive" }); err != nil {
		t.Fatal(err)
	}
	if exists(c, "stale") {
		t.Error("stale service survived the sweep")
	}
	if !exists(c, "alive") || !exists(c, "other-replica") {
		t.Error("sweep removed a service it should have kept")
	}
}
