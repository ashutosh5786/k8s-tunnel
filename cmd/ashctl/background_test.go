package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/daemon"
)

// isolate points the background-process files at a temporary directory so
// a test can never see or disturb a real connection on this machine.
func isolate(t *testing.T) daemon.Dir {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("ASHCTL_HOME", dir)
	return daemon.Dir(dir)
}

func runCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), args, &stdout, &stderr)
	return stdout.String(), err
}

func TestStatusDisconnectAndLogsWhenNotConnected(t *testing.T) {
	isolate(t)
	for _, cmd := range []string{"status", "disconnect"} {
		out, err := runCmd(t, cmd)
		if err != nil || !strings.Contains(out, "Not connected") {
			t.Errorf("%s: out = %q, err = %v", cmd, out, err)
		}
	}
	out, err := runCmd(t, "logs")
	if err != nil || !strings.Contains(out, "No log yet") {
		t.Errorf("logs: out = %q, err = %v", out, err)
	}
}

func TestStatusShowsConnectionAndRecentOutput(t *testing.T) {
	dir := isolate(t)
	// This test process stands in for the background process: it is
	// certainly alive.
	started := time.Now().Add(-time.Minute).UTC()
	if err := dir.Write(daemon.State{PID: os.Getpid(), Context: "dev", Session: "s123", Started: started}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(dir.LogPath(), []byte("connected to k8s-tunnel-0 as ash (session s123)\nresolving 12 cluster services by name\n"), 0o600)

	out, err := runCmd(t, "status")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Connected to context dev", "session s123", "resolving 12 cluster services by name"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output is missing %q:\n%s", want, out)
		}
	}

	logs, err := runCmd(t, "logs")
	if err != nil || !strings.Contains(logs, "connected to k8s-tunnel-0") {
		t.Errorf("logs = %q, err = %v", logs, err)
	}
}

func TestStatusForgetsABackgroundProcessThatDied(t *testing.T) {
	dir := isolate(t)
	// No process has this id.
	if err := dir.Write(daemon.State{PID: 2147483600, Context: "dev", Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	out, err := runCmd(t, "status")
	if err != nil || !strings.Contains(out, "Not connected") {
		t.Errorf("out = %q, err = %v", out, err)
	}
	if _, ok, _ := dir.Read(); ok {
		t.Error("record of the dead process was kept")
	}
}

func TestConnectRefusesWhenAlreadyConnected(t *testing.T) {
	dir := isolate(t)
	if err := dir.Write(daemon.State{PID: os.Getpid(), Context: "dev", Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	_, err := runCmd(t, "connect", "--server", "127.0.0.1:1", "--no-hosts")
	if err == nil || !strings.Contains(err.Error(), "already connected to dev") || !strings.Contains(err.Error(), "ashctl disconnect") {
		t.Errorf("err = %v", err)
	}
}

// Wrong settings must be reported by "ashctl connect" itself, not buried
// in the background process's log.
func TestBackgroundConnectReportsBadSettingsDirectly(t *testing.T) {
	dir := isolate(t)
	missing := filepath.Join(t.TempDir(), "no-such-kubeconfig")
	for name, args := range map[string][]string{
		"bad expose":     {"connect", "--server", "127.0.0.1:1", "--no-hosts", "--expose", "oops"},
		"bad kubeconfig": {"connect", "--no-hosts", "--kubeconfig", missing},
	} {
		if _, err := runCmd(t, args...); err == nil {
			t.Errorf("%s: no error", name)
		}
		if _, ok, _ := dir.Read(); ok {
			t.Errorf("%s: a background process was recorded", name)
		}
		if _, err := os.Stat(dir.LogPath()); err == nil {
			t.Errorf("%s: a background process was started", name)
		}
	}
}

func TestHostsAreUsedWhenPossibleAndSkippedWhenNot(t *testing.T) {
	writable := filepath.Join(t.TempDir(), "hosts")
	os.WriteFile(writable, []byte("127.0.0.1 localhost\n"), 0o644)
	missing := filepath.Join(t.TempDir(), "nope", "hosts")

	build := func(args ...string) (hostsOn bool, notes string, err error) {
		t.Helper()
		o, perr := parseConnect(append([]string{"--server", "127.0.0.1:1"}, args...), io.Discard)
		if perr != nil {
			t.Fatal(perr)
		}
		var n bytes.Buffer
		cfg, err := buildConfig(o, io.Discard, io.Discard, &n)
		return cfg.Hosts != nil, n.String(), err
	}

	// By default: on when the file can be changed, with no fuss.
	on, notes, err := build("--hosts-file", writable, "--home-namespace", "shop", "--hosts-namespaces", "shop, data")
	if err != nil || !on || notes != "" {
		t.Errorf("writable: on=%v notes=%q err=%v", on, notes, err)
	}
	// By default: off, with an explanation, when it cannot.
	on, notes, err = build("--hosts-file", missing)
	if err != nil || on || !strings.Contains(notes, "administrator") {
		t.Errorf("unwritable: on=%v notes=%q err=%v", on, notes, err)
	}
	// --hosts turns that into an error.
	if _, _, err = build("--hosts", "--hosts-file", missing); err == nil {
		t.Error("--hosts accepted a hosts file that cannot be changed")
	}
	// --no-hosts stays off and says nothing.
	on, notes, err = build("--no-hosts", "--hosts-file", writable)
	if err != nil || on || notes != "" {
		t.Errorf("--no-hosts: on=%v notes=%q err=%v", on, notes, err)
	}
}

func TestHostsNamespacesAreParsed(t *testing.T) {
	writable := filepath.Join(t.TempDir(), "hosts")
	os.WriteFile(writable, nil, 0o644)
	o, err := parseConnect([]string{"--server", "127.0.0.1:1", "--hosts-file", writable,
		"--hosts-namespaces", " shop ,data,, ", "--home-namespace", "shop", "--cluster-domain", "corp.internal"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := buildConfig(o, io.Discard, io.Discard, io.Discard)
	if err != nil || cfg.Hosts == nil {
		t.Fatalf("hosts = %+v, err %v", cfg.Hosts, err)
	}
	if got := strings.Join(cfg.Hosts.Namespaces, "|"); got != "shop|data" {
		t.Errorf("namespaces = %q", got)
	}
	if cfg.Hosts.HomeNamespace != "shop" || cfg.Hosts.ClusterDomain != "corp.internal" || cfg.Hosts.Path != writable {
		t.Errorf("hosts = %+v", cfg.Hosts)
	}
}

func TestLastLines(t *testing.T) {
	for _, tc := range []struct {
		in   string
		n    int
		want string
	}{
		{"a\nb\nc\n", 2, "b\nc\n"},
		{"a\nb\nc", 2, "b\nc"}, // no final newline
		{"a\n", 5, "a\n"},
		{"", 3, ""},
		{"a\nb\n", 0, ""},
	} {
		if got := string(lastLines([]byte(tc.in), tc.n)); got != tc.want {
			t.Errorf("lastLines(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
		}
	}
}
