package server

import (
	"fmt"
	"net"
	"strconv"
	"sync"
)

// portAlloc hands out listening ports from a fixed range. With an empty
// range it lets the operating system pick, which is what tests use.
type portAlloc struct {
	mu       sync.Mutex
	min, max int
	next     int
}

func newPortAlloc(min, max int) *portAlloc {
	return &portAlloc{min: min, max: max, next: min}
}

// listen opens a listener on a free port and returns the port it got.
func (p *portAlloc) listen() (net.Listener, int, error) {
	if p.min == 0 {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, 0, err
		}
		return ln, ln.Addr().(*net.TCPAddr).Port, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	size := p.max - p.min + 1
	for i := 0; i < size; i++ {
		port := p.next
		p.next++
		if p.next > p.max {
			p.next = p.min
		}
		// A port still held by another exposure simply fails to bind, so
		// the listener itself is the record of what is in use.
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			return ln, port, nil
		}
	}
	return nil, 0, fmt.Errorf("no free port in range %d-%d", p.min, p.max)
}
