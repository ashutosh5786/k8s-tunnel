package server

import (
	"net"
	"strconv"
	"strings"
	"testing"
)

// freePort returns a port nothing is listening on right now.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestPortAllocStaysInRangeAndReusesFreedPorts(t *testing.T) {
	port := freePort(t)
	p := newPortAlloc(port, port)

	ln, got, err := p.listen()
	if err != nil {
		t.Fatal(err)
	}
	if got != port {
		t.Fatalf("got port %d, want %d", got, port)
	}

	// The only port is taken, so the range is exhausted.
	if ln2, _, err := p.listen(); err == nil {
		ln2.Close()
		t.Fatal("allocated a port that is already in use")
	} else if !strings.Contains(err.Error(), "no free port") {
		t.Errorf("err = %v", err)
	}

	// Once released it can be handed out again.
	ln.Close()
	ln3, got, err := p.listen()
	if err != nil {
		t.Fatalf("freed port not reusable: %v", err)
	}
	defer ln3.Close()
	if got != port {
		t.Errorf("got port %d, want %d", got, port)
	}
}

func TestPortAllocSkipsPortsHeldByOthers(t *testing.T) {
	// Find two adjacent free ports so the range has one busy and one free.
	var busy net.Listener
	var base int
	for range 50 {
		base = freePort(t)
		a, errA := net.Listen("tcp", ":"+strconv.Itoa(base))
		b, errB := net.Listen("tcp", ":"+strconv.Itoa(base+1))
		if errA == nil && errB == nil {
			busy = a
			b.Close()
			break
		}
		if a != nil {
			a.Close()
		}
		if b != nil {
			b.Close()
		}
	}
	if busy == nil {
		t.Skip("could not find two adjacent free ports")
	}
	defer busy.Close()

	p := newPortAlloc(base, base+1)
	ln, got, err := p.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if got != base+1 {
		t.Errorf("got port %d, want %d (the free one)", got, base+1)
	}
}

func TestPortAllocWithoutRangeLetsSystemChoose(t *testing.T) {
	p := newPortAlloc(0, 0)
	a, portA, err := p.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, portB, err := p.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if portA == 0 || portB == 0 || portA == portB {
		t.Errorf("ports = %d, %d", portA, portB)
	}
}
