// Package protocol defines the wire protocol and stream multiplexing shared
// by the client and the in-cluster server.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// Version is the protocol version sent in Hello. Both sides must match.
const Version = 1

// maxHeader bounds the size of a single message so a bad peer cannot make
// us allocate without limit.
const maxHeader = 64 << 10

// Type identifies what a stream is for. Every stream starts with exactly
// one Request and one Response; dial and inbound streams then carry raw
// bytes.
type Type string

const (
	// TypeHello is sent by the client on the first stream of a session.
	TypeHello Type = "hello"
	// TypeDial asks the server to connect to an address inside the cluster.
	TypeDial Type = "dial"
	// TypeExpose asks the server to make local ports reachable in the cluster.
	TypeExpose Type = "expose"
	// TypeUnexpose removes an exposure.
	TypeUnexpose Type = "unexpose"
	// TypeInbound is sent by the server when a cluster connection arrives
	// for an exposed port.
	TypeInbound Type = "inbound"
	// TypeServices asks the server which cluster Services exist. The
	// Response is followed on the same stream by a JSON array of
	// ServiceInfo, which can be far larger than a message may be.
	TypeServices Type = "services"
)

// Request is the first message on every stream.
type Request struct {
	Type     Type      `json:"type"`
	Hello    *Hello    `json:"hello,omitempty"`
	Dial     *Dial     `json:"dial,omitempty"`
	Expose   *Expose   `json:"expose,omitempty"`
	Unexpose *Unexpose `json:"unexpose,omitempty"`
	Inbound  *Inbound  `json:"inbound,omitempty"`
	Services *Services `json:"services,omitempty"`
}

// Hello identifies the user and the session. The session ID is chosen by
// the client and stays the same across reconnects.
type Hello struct {
	Version int    `json:"version"`
	User    string `json:"user"`
	Session string `json:"session"`
}

// Dial names a cluster address in host:port form.
type Dial struct {
	Address string `json:"address"`
}

// PortSpec describes one exposed port.
type PortSpec struct {
	// Name is the Service port name, e.g. "http" or "grpc".
	Name string `json:"name,omitempty"`
	// Port is the port the cluster uses to reach the service.
	Port int `json:"port"`
	// LocalPort is the port on the client machine. Zero means same as Port.
	LocalPort int `json:"localPort,omitempty"`
	// AppProtocol is passed through to the Service port, which the mesh
	// uses for protocol selection.
	AppProtocol string `json:"appProtocol,omitempty"`
}

// Expose asks for a named exposure with one or more ports.
type Expose struct {
	Name  string     `json:"name"`
	Ports []PortSpec `json:"ports"`
}

// Unexpose removes the exposure with the given name.
type Unexpose struct {
	Name string `json:"name"`
}

// Inbound announces a cluster connection for an exposed port.
type Inbound struct {
	Exposure   string `json:"exposure"`
	LocalPort  int    `json:"localPort"`
	RemoteAddr string `json:"remoteAddr,omitempty"`
}

// Services limits the listing to some namespaces. Empty means all.
type Services struct {
	Namespaces []string `json:"namespaces,omitempty"`
}

// ServiceInfo describes one cluster Service and its TCP ports.
type ServiceInfo struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Ports     []int  `json:"ports"`
}

// MaxServicesBody bounds the service listing a client will read.
const MaxServicesBody = 64 << 20

// Response answers a Request.
type Response struct {
	OK     bool         `json:"ok"`
	Error  string       `json:"error,omitempty"`
	Hello  *HelloReply  `json:"hello,omitempty"`
	Expose *ExposeReply `json:"expose,omitempty"`
}

// HelloReply tells the client which server replica it reached.
type HelloReply struct {
	Replica string `json:"replica"`
}

// ExposedPort reports where one exposed port ended up.
type ExposedPort struct {
	Name       string `json:"name,omitempty"`
	Port       int    `json:"port"`
	LocalPort  int    `json:"localPort"`
	ServerPort int    `json:"serverPort"`
}

// ExposeReply reports the cluster host name of an exposure.
type ExposeReply struct {
	Name  string        `json:"name"`
	Host  string        `json:"host,omitempty"`
	Ports []ExposedPort `json:"ports"`
}

// Fail builds an error Response.
func Fail(format string, args ...any) *Response {
	return &Response{Error: fmt.Sprintf(format, args...)}
}

// Err returns the Response's error, or nil when it succeeded.
func (r *Response) Err() error {
	if r.OK {
		return nil
	}
	if r.Error == "" {
		return fmt.Errorf("request failed")
	}
	return fmt.Errorf("%s", r.Error)
}

// WriteMsg writes v as a length-prefixed JSON message.
func WriteMsg(w io.Writer, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body) > maxHeader {
		return fmt.Errorf("message too large: %d bytes", len(body))
	}
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body)))
	copy(buf[4:], body)
	_, err = w.Write(buf)
	return err
}

// ReadMsg reads one length-prefixed JSON message into v. It reads exactly
// the message and nothing after it, so raw bytes may follow on the stream.
func ReadMsg(r io.Reader, v any) error {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n > maxHeader {
		return fmt.Errorf("message too large: %d bytes", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}
