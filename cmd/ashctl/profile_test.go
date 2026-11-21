package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ashutosh5786/k8s-tunnel/internal/client/profile"
)

func writeProfile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ashctl.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGenerateWritesTheTemplateAndNeverOverwritesByAccident(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ashctl.yaml")

	out, err := runCmd(t, "generate", "-o", path)
	if err != nil || !strings.Contains(out, path) || !strings.Contains(out, "ashctl apply") {
		t.Fatalf("out = %q, err = %v", out, err)
	}
	if got := readFile(t, path); got != profile.Template {
		t.Error("the written file is not the template")
	}

	// Someone has edited the file; generating again must not destroy it.
	os.WriteFile(path, []byte("context: mine\n"), 0o644)
	if _, err := runCmd(t, "generate", "-o", path); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("err = %v, want a refusal to overwrite", err)
	}
	if got := readFile(t, path); got != "context: mine\n" {
		t.Errorf("an existing file was overwritten: %q", got)
	}

	// --force replaces it.
	if _, err := runCmd(t, "generate", "-o", path, "--force"); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != profile.Template {
		t.Error("--force did not replace the file")
	}
}

func TestGenerateDefaultsToCurrentFolder(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, err := runCmd(t, "generate"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("ashctl.yaml"); err != nil {
		t.Errorf("ashctl.yaml was not created in the current folder: %v", err)
	}
}

func TestApplyOnARunningConnection(t *testing.T) {
	fakeBackground(t)
	_, cartPort := echo(t, "cart")
	_, dbPort := echo(t, "db")
	cart := strconv.Itoa(cartPort)

	path := writeProfile(t, `
expose:
  - name: cart-${USER}
    port: 8080
    localPort: `+cart+`
    protocol: http
  - name: db-${USER}
    port: 5432
    localPort: `+strconv.Itoa(dbPort)+`
`)
	out, err := runCmd(t, "apply", "-f", path, "--user", "Ash")
	if err != nil {
		t.Fatalf("apply: %v\n%s", err, out)
	}
	if strings.Count(out, "exposed   ") != 2 || !strings.Contains(out, "-> localhost:"+cart) {
		t.Errorf("first apply:\n%s", out)
	}
	status, _ := runCmd(t, "status")
	if strings.Count(status, "-> localhost:") < 2 {
		t.Errorf("status after apply:\n%s", status)
	}

	// Applying the same file again must not touch what is running.
	out, err = runCmd(t, "apply", "-f", path, "--user", "Ash")
	if err != nil || strings.Contains(out, "exposed   ") ||
		!strings.Contains(out, "unchanged cart-ash") || !strings.Contains(out, "unchanged db-ash") {
		t.Errorf("second apply: err %v\n%s", err, out)
	}

	// Something added by hand is left alone by a plain apply ...
	if _, err := runCmd(t, "expose", "by-hand:9000:"+cart); err != nil {
		t.Fatal(err)
	}
	smaller := writeProfile(t, "expose:\n  - name: cart-${USER}\n    port: 8080\n    localPort: "+cart+"\n    protocol: http\n")
	out, err = runCmd(t, "apply", "-f", smaller, "--user", "Ash")
	if err != nil || strings.Contains(out, "unexposed") {
		t.Errorf("plain apply removed something: err %v\n%s", err, out)
	}
	// ... and removed, together with what left the file, by --prune.
	out, err = runCmd(t, "apply", "-f", smaller, "--user", "Ash", "--prune")
	if err != nil || !strings.Contains(out, "unexposed db-ash") || !strings.Contains(out, "unexposed by-hand") ||
		!strings.Contains(out, "unchanged cart-ash") {
		t.Errorf("apply --prune: err %v\n%s", err, out)
	}
	status, _ = runCmd(t, "status")
	if strings.Count(status, "-> localhost:") != 1 {
		t.Errorf("status after prune:\n%s", status)
	}
}

func TestApplyPicksUpAChangedEntry(t *testing.T) {
	fakeBackground(t)
	_, oldPort := echo(t, "old")
	_, newPort := echo(t, "new")
	body := func(port int) string {
		return "expose:\n  - name: cart\n    port: 8080\n    localPort: " + strconv.Itoa(port) + "\n"
	}
	if _, err := runCmd(t, "apply", "-f", writeProfile(t, body(oldPort))); err != nil {
		t.Fatal(err)
	}
	out, err := runCmd(t, "apply", "-f", writeProfile(t, body(newPort)))
	if err != nil || !strings.Contains(out, "-> localhost:"+strconv.Itoa(newPort)) || strings.Contains(out, "unchanged") {
		t.Errorf("err %v\n%s", err, out)
	}
}

func TestApplyWithNothingToExpose(t *testing.T) {
	fakeBackground(t)
	out, err := runCmd(t, "apply", "-f", writeProfile(t, profile.Template))
	if err != nil || !strings.Contains(out, "Nothing to expose") {
		t.Errorf("out = %q, err = %v", out, err)
	}
}

func TestApplyRefusesAFileForAnotherCluster(t *testing.T) {
	fakeBackground(t) // connected to context "dev"
	path := writeProfile(t, "context: prod\nexpose:\n  - name: cart\n    port: 8080\n")
	out, err := runCmd(t, "apply", "-f", path)
	if err == nil || !strings.Contains(err.Error(), `"prod"`) || !strings.Contains(err.Error(), `"dev"`) {
		t.Errorf("err = %v", err)
	}
	if strings.Contains(out, "exposed") {
		t.Errorf("something was exposed on the wrong cluster:\n%s", out)
	}
	// The same context is fine.
	if _, err := runCmd(t, "apply", "-f", writeProfile(t, "context: dev\n")); err != nil {
		t.Errorf("matching context refused: %v", err)
	}
}

func TestApplyMentionsSettingsThatNeedAReconnect(t *testing.T) {
	fakeBackground(t)
	path := writeProfile(t, "homeNamespace: shop\nforward:\n  - localPort: 5432\n    target: postgres.data.svc:5432\n")
	out, err := runCmd(t, "apply", "-f", path)
	if err != nil || !strings.Contains(out, "only take effect when connecting") {
		t.Errorf("out = %q, err = %v", out, err)
	}
}

func TestApplyReportsFileProblemsBeforeDoingAnything(t *testing.T) {
	isolate(t)
	missing := filepath.Join(t.TempDir(), "ashctl.yaml")
	if _, err := runCmd(t, "apply", "-f", missing); err == nil || !strings.Contains(err.Error(), "ashctl generate") {
		t.Errorf("missing file: err = %v", err)
	}
	bad := writeProfile(t, "expose:\n  - name: Bad_Name\n    port: 8080\n")
	if _, err := runCmd(t, "apply", "-f", bad); err == nil || !strings.Contains(err.Error(), "expose[0].name") {
		t.Errorf("bad file: err = %v", err)
	}
	typo := writeProfile(t, "exposes:\n  - name: cart\n    port: 8080\n")
	if _, err := runCmd(t, "apply", "-f", typo); err == nil || !strings.Contains(err.Error(), "exposes") {
		t.Errorf("misspelt field: err = %v", err)
	}
}

// With no connection, apply connects using the file. The connection
// itself needs a detached process, so this checks the part that can fail
// first: a wrong setting in the file is reported, not hidden.
func TestApplyWithoutAConnectionUsesTheFileToConnect(t *testing.T) {
	dir := isolate(t)
	path := writeProfile(t, "context: no-such-context\nexpose:\n  - name: cart\n    port: 8080\n")
	_, err := runCmd(t, "apply", "-f", path)
	if err == nil || !strings.Contains(err.Error(), "no-such-context") {
		t.Errorf("err = %v, want the file's context to have been used", err)
	}
	if _, ok, _ := dir.Read(); ok {
		t.Error("a background process was recorded although connecting failed")
	}
}
