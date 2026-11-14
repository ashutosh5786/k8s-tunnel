package hosts

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var two = []Entry{
	{IP: "127.77.0.1", Names: []string{"c.shop", "c.shop.svc", "c.shop.svc.cluster.local"}},
	{IP: "127.77.0.2", Names: []string{"db.data.svc"}},
}

func render(t *testing.T, existing string, entries []Entry) string {
	t.Helper()
	out, err := Render([]byte(existing), entries)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestRenderAppendsMarkedLinesAndKeepsTheRest(t *testing.T) {
	existing := "# my hosts\n127.0.0.1 localhost\n\n10.0.0.5 nas   # home server\n"
	got := render(t, existing, two)
	want := existing +
		"127.77.0.1 c.shop c.shop.svc c.shop.svc.cluster.local # k8s-tunnel\n" +
		"127.77.0.2 db.data.svc # k8s-tunnel\n"
	if got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

func TestRenderReplacesOldEntriesInsteadOfAccumulating(t *testing.T) {
	existing := "127.0.0.1 localhost\n"
	first := render(t, existing, two)
	second := render(t, first, []Entry{{IP: "127.77.0.9", Names: []string{"new.ns.svc"}}})
	want := existing + "127.77.0.9 new.ns.svc # k8s-tunnel\n"
	if second != want {
		t.Errorf("got:\n%q\nwant:\n%q", second, want)
	}
	// Applying the same entries twice changes nothing.
	if again := render(t, second, []Entry{{IP: "127.77.0.9", Names: []string{"new.ns.svc"}}}); again != second {
		t.Errorf("not idempotent:\n%q\nvs\n%q", again, second)
	}
}

func TestRemovingRestoresTheOriginalFileExactly(t *testing.T) {
	for name, original := range map[string]string{
		"unix":                 "# comment\n127.0.0.1 localhost\n",
		"windows line endings": "# comment\r\n127.0.0.1 localhost\r\n",
		"no final newline":     "127.0.0.1 localhost",
		"empty":                "",
		"blank lines and tabs": "\n\n127.0.0.1\tlocalhost\n\n",
	} {
		with := render(t, original, two)
		if with == original {
			t.Errorf("%s: entries were not added", name)
		}
		back := render(t, with, nil)
		// Appending may have to add one line ending to a file that
		// lacked a final newline; nothing else may differ.
		if back != original && back != original+"\n" {
			t.Errorf("%s: after removal got %q, want %q", name, back, original)
		}
	}
}

func TestRenderUsesTheFilesLineEndings(t *testing.T) {
	got := render(t, "127.0.0.1 localhost\r\n", two[:1])
	want := "127.0.0.1 localhost\r\n127.77.0.1 c.shop c.shop.svc c.shop.svc.cluster.local # k8s-tunnel\r\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// Managed lines with Windows endings must still be recognised.
	if back := render(t, got, nil); back != "127.0.0.1 localhost\r\n" {
		t.Errorf("CRLF managed line not removed: %q", back)
	}
}

func TestOnlyLinesEndingWithTheMarkerAreManaged(t *testing.T) {
	existing := strings.Join([]string{
		"10.0.0.1 a # k8s-tunnel notes that are mine", // marker not at the end
		"# k8s-tunnel is great",                       // a comment mentioning it
		"10.0.0.2 k8s-tunnel",                         // a host with that name
		"127.77.0.5 stale.ns.svc # k8s-tunnel",        // really ours
		"127.77.0.6 stale2.ns.svc # k8s-tunnel  \t",   // ours, trailing spaces
		"",
	}, "\n")
	got := render(t, existing, nil)
	want := strings.Join([]string{
		"10.0.0.1 a # k8s-tunnel notes that are mine",
		"# k8s-tunnel is great",
		"10.0.0.2 k8s-tunnel",
		"",
	}, "\n")
	if got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

// Names come from the cluster. A hostile one must not be able to inject
// extra hosts lines or point real sites somewhere else.
func TestRenderRefusesDangerousEntries(t *testing.T) {
	for name, e := range map[string]Entry{
		"newline in name":    {IP: "127.77.0.1", Names: []string{"a\n6.6.6.6 bank.example"}},
		"space in name":      {IP: "127.77.0.1", Names: []string{"a bank.example"}},
		"comment in name":    {IP: "127.77.0.1", Names: []string{"a#b"}},
		"uppercase":          {IP: "127.77.0.1", Names: []string{"Bank.Example"}},
		"leading dot":        {IP: "127.77.0.1", Names: []string{".example"}},
		"trailing dot":       {IP: "127.77.0.1", Names: []string{"example."}},
		"empty name":         {IP: "127.77.0.1", Names: []string{""}},
		"no names":           {IP: "127.77.0.1"},
		"localhost":          {IP: "127.77.0.1", Names: []string{"localhost"}},
		"non-loopback ip":    {IP: "6.6.6.6", Names: []string{"c.shop.svc"}},
		"not an ip":          {IP: "127.77.0.1 evil", Names: []string{"c.shop.svc"}},
		"overlong name":      {IP: "127.77.0.1", Names: []string{strings.Repeat("a", 254)}},
		"underscore":         {IP: "127.77.0.1", Names: []string{"a_b.svc"}},
		"one bad among good": {IP: "127.77.0.1", Names: []string{"ok.svc", "bad name"}},
	} {
		existing := []byte("127.0.0.1 localhost\n")
		if out, err := Render(existing, []Entry{two[0], e}); err == nil {
			t.Errorf("%s: accepted, produced %q", name, out)
		}
	}
}

func TestUpdateAndRemoveOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	original := "127.0.0.1 localhost\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	read := func() string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	if err := Update(path, two); err != nil {
		t.Fatal(err)
	}
	if got := read(); !strings.Contains(got, "127.77.0.2 db.data.svc # k8s-tunnel") || !strings.HasPrefix(got, original) {
		t.Errorf("after update: %q", got)
	}
	if err := Remove(path); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != original {
		t.Errorf("after remove: %q, want %q", got, original)
	}
	// Removing when there is nothing to remove is fine.
	if err := Remove(path); err != nil {
		t.Errorf("second remove: %v", err)
	}
}

func TestUpdateDoesNotRewriteAnUnchangedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	if err := os.WriteFile(path, []byte("127.0.0.1 localhost\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Update(path, two); err != nil {
		t.Fatal(err)
	}
	// Make the file read-only: an update that changes nothing must still
	// succeed, because it must not write at all.
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0o644)
	if err := Update(path, two); err != nil {
		t.Errorf("unchanged update wrote to the file: %v", err)
	}
}

func TestUpdateRejectsBadEntriesWithoutTouchingTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	original := "127.0.0.1 localhost\n"
	os.WriteFile(path, []byte(original), 0o644)
	if err := Update(path, []Entry{{IP: "8.8.8.8", Names: []string{"dns.example"}}}); err == nil {
		t.Fatal("bad entry accepted")
	}
	if b, _ := os.ReadFile(path); string(b) != original {
		t.Errorf("file changed by a rejected update: %q", b)
	}
}

func TestMissingAndReadOnlyFilesGiveUsefulErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	if err := Update(missing, two); err == nil || !strings.Contains(err.Error(), missing) {
		t.Errorf("missing file: err = %v", err)
	}
	if err := CheckWritable(missing); err == nil {
		t.Error("CheckWritable accepted a missing file")
	}

	path := filepath.Join(t.TempDir(), "hosts")
	os.WriteFile(path, []byte("127.0.0.1 localhost\n"), 0o444)
	defer os.Chmod(path, 0o644)
	if os.Geteuid() == 0 {
		t.Skip("root can write read-only files")
	}
	err := CheckWritable(path)
	if err == nil {
		t.Fatal("CheckWritable accepted a read-only file")
	}
	hint := "sudo"
	if runtime.GOOS == "windows" {
		hint = "administrator"
	}
	if !strings.Contains(err.Error(), hint) {
		t.Errorf("error does not say how to fix it: %v", err)
	}
	// CheckWritable must not have changed the file.
	if b, _ := os.ReadFile(path); string(b) != "127.0.0.1 localhost\n" {
		t.Errorf("CheckWritable modified the file: %q", b)
	}
}

func TestCheckWritableDoesNotModifyAWritableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	os.WriteFile(path, []byte("127.0.0.1 localhost\n"), 0o644)
	if err := CheckWritable(path); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "127.0.0.1 localhost\n" {
		t.Errorf("file changed: %q", b)
	}
}

func TestDefaultPath(t *testing.T) {
	p := DefaultPath()
	if runtime.GOOS == "windows" {
		if !strings.HasSuffix(strings.ToLower(p), `\system32\drivers\etc\hosts`) {
			t.Errorf("path = %q", p)
		}
	} else if p != "/etc/hosts" {
		t.Errorf("path = %q", p)
	}
}
