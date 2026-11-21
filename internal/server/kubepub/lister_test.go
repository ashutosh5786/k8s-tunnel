package kubepub

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ashutosh5786/k8s-tunnel/internal/server"
)

func svc(namespace, name string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       corev1.ServiceSpec{Ports: ports},
	}
}

func tcp(port int32) corev1.ServicePort {
	return corev1.ServicePort{Port: port, Protocol: corev1.ProtocolTCP}
}

func TestListerReturnsTCPServicesSorted(t *testing.T) {
	c := fake.NewClientset(
		svc("shop", "cart", tcp(8080), tcp(9090)),
		svc("data", "postgres", tcp(5432)),
		svc("shop", "api", corev1.ServicePort{Port: 80}), // protocol unset means TCP
		svc("kube-system", "kube-dns",
			corev1.ServicePort{Port: 53, Protocol: corev1.ProtocolUDP},
			corev1.ServicePort{Port: 53, Protocol: corev1.ProtocolTCP},
			corev1.ServicePort{Port: 9153, Protocol: corev1.ProtocolTCP}),
		svc("media", "rtp", corev1.ServicePort{Port: 5004, Protocol: corev1.ProtocolUDP}), // UDP only
		svc("ext", "external-name"), // no ports at all
	)
	got, err := (&Lister{Client: c}).Services(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []server.ServiceInfo{
		{Namespace: "data", Name: "postgres", Ports: []int{5432}},
		{Namespace: "kube-system", Name: "kube-dns", Ports: []int{53, 9153}},
		{Namespace: "shop", Name: "api", Ports: []int{80}},
		{Namespace: "shop", Name: "cart", Ports: []int{8080, 9090}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
}

func TestListerLimitsToRequestedNamespaces(t *testing.T) {
	c := fake.NewClientset(
		svc("shop", "cart", tcp(8080)),
		svc("data", "postgres", tcp(5432)),
		svc("other", "thing", tcp(1234)),
	)
	got, err := (&Lister{Client: c}).Services(context.Background(), []string{"shop", "data"})
	if err != nil {
		t.Fatal(err)
	}
	want := []server.ServiceInfo{
		{Namespace: "data", Name: "postgres", Ports: []int{5432}},
		{Namespace: "shop", Name: "cart", Ports: []int{8080}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
}

func TestListerCachesBrieflyThenRefreshes(t *testing.T) {
	c := fake.NewClientset(svc("shop", "cart", tcp(8080)))
	l := &Lister{Client: c, TTL: 150 * time.Millisecond}
	ctx := context.Background()

	first, err := l.Services(ctx, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("first = %+v, err %v", first, err)
	}
	if _, err := c.CoreV1().Services("shop").Create(ctx, svc("shop", "api", tcp(80)), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Within the TTL the cached listing is reused.
	if again, _ := l.Services(ctx, nil); len(again) != 1 {
		t.Errorf("cache not used: %+v", again)
	}
	// A different filter is a different cache entry and sees fresh data.
	if filtered, _ := l.Services(ctx, []string{"shop"}); len(filtered) != 2 {
		t.Errorf("filtered listing = %+v", filtered)
	}
	time.Sleep(200 * time.Millisecond)
	if later, _ := l.Services(ctx, nil); len(later) != 2 {
		t.Errorf("stale after TTL: %+v", later)
	}
}

func TestListerFilterOrderDoesNotMatter(t *testing.T) {
	c := fake.NewClientset(svc("a", "x", tcp(1)), svc("b", "y", tcp(2)))
	l := &Lister{Client: c}
	ctx := context.Background()
	ab, _ := l.Services(ctx, []string{"a", "b"})
	in := []string{"b", "a"}
	ba, _ := l.Services(ctx, in)
	if !reflect.DeepEqual(ab, ba) {
		t.Errorf("order changed the result: %+v vs %+v", ab, ba)
	}
	// The caller's slice must not be reordered behind its back.
	if in[0] != "b" {
		t.Errorf("caller's slice was modified: %v", in)
	}
}
