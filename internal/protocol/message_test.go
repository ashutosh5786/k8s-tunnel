package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestMessageRoundTripLeavesFollowingBytes(t *testing.T) {
	want := Request{Type: TypeExpose, Expose: &Expose{Name: "b-ash", Ports: []PortSpec{
		{Name: "http", Port: 8080, LocalPort: 3000, AppProtocol: "http"},
		{Name: "grpc", Port: 9090},
	}}}
	var buf bytes.Buffer
	if err := WriteMsg(&buf, &want); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("raw bytes after the header")

	var got Request
	if err := ReadMsg(&buf, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	// Dial and inbound streams carry payload right after the header, so
	// reading a message must not consume any of it.
	if rest := buf.String(); rest != "raw bytes after the header" {
		t.Errorf("ReadMsg consumed payload, left %q", rest)
	}
}

func TestEveryRequestTypeRoundTrips(t *testing.T) {
	for _, want := range []Request{
		{Type: TypeHello, Hello: &Hello{Version: Version, User: "ash", Session: "s1"}},
		{Type: TypeDial, Dial: &Dial{Address: "c.ns.svc.cluster.local:8080"}},
		{Type: TypeUnexpose, Unexpose: &Unexpose{Name: "b-ash"}},
		{Type: TypeInbound, Inbound: &Inbound{Exposure: "b-ash", LocalPort: 3000, RemoteAddr: "10.0.0.7:5555"}},
	} {
		var buf bytes.Buffer
		if err := WriteMsg(&buf, &want); err != nil {
			t.Fatal(err)
		}
		var got Request
		if err := ReadMsg(&buf, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %+v, want %+v", want.Type, got, want)
		}
	}
}

func TestReadRejectsOversizedMessage(t *testing.T) {
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], maxHeader+1)
	var req Request
	err := ReadMsg(bytes.NewReader(head[:]), &req)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Errorf("err = %v, want a too-large error", err)
	}
}

func TestWriteRejectsOversizedMessage(t *testing.T) {
	req := Request{Type: TypeDial, Dial: &Dial{Address: strings.Repeat("a", maxHeader)}}
	var buf bytes.Buffer
	if err := WriteMsg(&buf, &req); err == nil {
		t.Error("oversized message was written")
	}
	if buf.Len() != 0 {
		t.Errorf("%d bytes written for a rejected message", buf.Len())
	}
}

func TestReadReportsTruncation(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, &Request{Type: TypeDial, Dial: &Dial{Address: "c:80"}}); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	var req Request

	if err := ReadMsg(bytes.NewReader(nil), &req); !errors.Is(err, io.EOF) {
		t.Errorf("empty stream: err = %v, want EOF", err)
	}
	if err := ReadMsg(bytes.NewReader(full[:2]), &req); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("cut in length prefix: err = %v", err)
	}
	if err := ReadMsg(bytes.NewReader(full[:len(full)-3]), &req); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("cut in body: err = %v", err)
	}
}

func TestReadRejectsInvalidJSON(t *testing.T) {
	body := []byte("{not json")
	buf := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body)))
	var req Request
	if err := ReadMsg(bytes.NewReader(append(buf, body...)), &req); err == nil {
		t.Error("invalid JSON was accepted")
	}
}

func TestResponseErr(t *testing.T) {
	if err := (&Response{OK: true}).Err(); err != nil {
		t.Errorf("ok response: %v", err)
	}
	if err := Fail("dial %s: refused", "c:80").Err(); err == nil || err.Error() != "dial c:80: refused" {
		t.Errorf("err = %v", err)
	}
	// A failure with no text must still be an error.
	if err := (&Response{}).Err(); err == nil {
		t.Error("failed response with no message returned nil")
	}
}
