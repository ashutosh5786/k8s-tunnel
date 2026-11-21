package server_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/ashutosh5786/k8s-tunnel/internal/server"
	"github.com/ashutosh5786/k8s-tunnel/pkg/tunnel"
)

// fakeLister serves a fixed listing and records the filter it was given.
type fakeLister struct {
	list []server.ServiceInfo
	err  error
	got  [][]string
}

func (f *fakeLister) Services(_ context.Context, namespaces []string) ([]server.ServiceInfo, error) {
	f.got = append(f.got, namespaces)
	return f.list, f.err
}

func TestClientListsClusterServices(t *testing.T) {
	lister := &fakeLister{list: []server.ServiceInfo{
		{Namespace: "data", Name: "postgres", Ports: []int{5432}},
		{Namespace: "shop", Name: "cart", Ports: []int{8080, 9090}},
	}}
	addr, _ := startCustom(t, server.Config{Replica: "r0", Lister: lister})
	c := connectTo(t, addr, "ash", "s1")

	got, err := c.Services(ctx5(t), "shop", "data")
	if err != nil {
		t.Fatal(err)
	}
	want := []tunnel.Service{
		{Namespace: "data", Name: "postgres", Ports: []int{5432}},
		{Namespace: "shop", Name: "cart", Ports: []int{8080, 9090}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
	if !reflect.DeepEqual(lister.got, [][]string{{"shop", "data"}}) {
		t.Errorf("lister was asked for %v", lister.got)
	}
}

func TestEmptyListingIsNotAnError(t *testing.T) {
	addr, _ := startCustom(t, server.Config{Replica: "r0", Lister: &fakeLister{}})
	c := connectTo(t, addr, "ash", "s1")
	got, err := c.Services(ctx5(t))
	if err != nil || len(got) != 0 {
		t.Errorf("got %+v, err %v", got, err)
	}
}

// A real cluster's listing is far bigger than one protocol message may be.
func TestLargeListingIsDeliveredWhole(t *testing.T) {
	const n = 20000
	lister := &fakeLister{}
	for i := range n {
		lister.list = append(lister.list, server.ServiceInfo{
			Namespace: fmt.Sprintf("team-%03d", i%200),
			Name:      fmt.Sprintf("service-with-a-fairly-long-name-%05d", i),
			Ports:     []int{80, 443, 8080, 9090},
		})
	}
	addr, _ := startCustom(t, server.Config{Replica: "r0", Lister: lister})
	c := connectTo(t, addr, "ash", "s1")

	got, err := c.Services(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("got %d services, want %d", len(got), n)
	}
	last := got[n-1]
	if last.Name != lister.list[n-1].Name || !reflect.DeepEqual(last.Ports, []int{80, 443, 8080, 9090}) {
		t.Errorf("last entry = %+v", last)
	}
	// The session must be usable afterwards.
	if _, err := c.Services(ctx5(t)); err != nil {
		t.Errorf("second listing failed: %v", err)
	}
}

func TestListingErrorsReachTheClient(t *testing.T) {
	addr, _ := startCustom(t, server.Config{Replica: "r0", Lister: &fakeLister{err: errors.New("services is forbidden")}})
	c := connectTo(t, addr, "ash", "s1")
	if _, err := c.Services(ctx5(t)); err == nil || !strings.Contains(err.Error(), "services is forbidden") {
		t.Errorf("err = %v, want the server's reason", err)
	}

	// Without a lister the server says discovery is off.
	addr2, _ := startCustom(t, server.Config{Replica: "r0"})
	c2 := connectTo(t, addr2, "ash", "s2")
	if _, err := c2.Services(ctx5(t)); err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Errorf("err = %v, want a discovery-not-enabled error", err)
	}
}
