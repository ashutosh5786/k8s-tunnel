package profile

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const full = `
context: dev
namespace: tunnel-system
homeNamespace: shop
hostsNamespaces: [shop, data]
expose:
  - name: cart-${USER}
    port: 8080
    localPort: 3000
    protocol: http
  - name: cart-${USER}
    port: 9090
    protocol: grpc
  - name: db-${USER}
    port: 5432
forward:
  - localPort: 5433
    target: postgres.data.svc:5432
`

func TestParseFullFile(t *testing.T) {
	f, err := Parse([]byte(full), "Ash.Singh")
	if err != nil {
		t.Fatal(err)
	}
	want := &File{
		Context: "dev", Namespace: "tunnel-system", HomeNamespace: "shop", HostsNamespaces: []string{"shop", "data"},
		Expose: []Expose{
			{Name: "cart-ash-singh", Port: 8080, LocalPort: 3000, Protocol: "http"},
			{Name: "cart-ash-singh", Port: 9090, LocalPort: 9090, Protocol: "grpc"}, // localPort defaults to port
			{Name: "db-ash-singh", Port: 5432, LocalPort: 5432},
		},
		Forward: []Forward{{LocalPort: 5433, Target: "postgres.data.svc:5432"}},
	}
	if !reflect.DeepEqual(f, want) {
		t.Errorf("got  %+v\nwant %+v", f, want)
	}
	if got := f.ExposeSpecs(); !reflect.DeepEqual(got, []string{
		"cart-ash-singh:8080:3000/http", "cart-ash-singh:9090:9090/grpc", "db-ash-singh:5432:5432",
	}) {
		t.Errorf("expose specs = %v", got)
	}
	if got := f.ForwardSpecs(); !reflect.DeepEqual(got, []string{"5433:postgres.data.svc:5432"}) {
		t.Errorf("forward specs = %v", got)
	}
}

func TestEmptyAndCommentOnlyFilesAreValid(t *testing.T) {
	for name, body := range map[string]string{"empty": "", "comments": "# nothing here\n\n# still nothing\n"} {
		f, err := Parse([]byte(body), "ash")
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !reflect.DeepEqual(f, &File{}) {
			t.Errorf("%s: got %+v", name, f)
		}
	}
}

// The generated file must be valid as written, and must set nothing up
// until someone edits it.
func TestTemplateIsValidAndInert(t *testing.T) {
	f, err := Parse([]byte(Template), "ash")
	if err != nil {
		t.Fatalf("the generated template does not parse: %v", err)
	}
	if !reflect.DeepEqual(f, &File{}) {
		t.Errorf("an unedited template is not empty: %+v", f)
	}
	for _, field := range []string{"context", "homeNamespace", "hostsNamespaces", "expose", "forward", "namespace",
		"name", "port", "localPort", "protocol", "target", "${USER}", "ashctl apply"} {
		if !strings.Contains(Template, field) {
			t.Errorf("template does not explain %q", field)
		}
	}
}

// Uncommenting every example in the template must give a file that works,
// otherwise the examples teach something wrong.
func TestTemplateExamplesWorkWhenUncommented(t *testing.T) {
	// An example starts at a line like "#key:" (no space after the hash)
	// and runs until the next blank line. Everything else is explanation.
	var out []string
	inExample := false
	for _, line := range strings.Split(Template, "\n") {
		switch {
		case line == "":
			inExample = false
		case strings.HasPrefix(line, "#") && len(line) > 1 && line[1] != ' ':
			inExample = true
			out = append(out, line[1:])
		case inExample && strings.HasPrefix(line, "#"):
			out = append(out, line[1:])
		}
	}
	f, err := Parse([]byte(strings.Join(out, "\n")), "ash")
	if err != nil {
		t.Fatalf("uncommented template does not parse: %v\n%s", err, strings.Join(out, "\n"))
	}
	if f.Context != "dev" || f.HomeNamespace != "shop" || f.Namespace != "k8s-tunnel" ||
		len(f.HostsNamespaces) != 2 || len(f.Expose) != 2 || len(f.Forward) != 1 {
		t.Fatalf("uncommented template = %+v", f)
	}
	if f.Expose[0].Name != "cart-ash" || f.Expose[0].LocalPort != 3000 || f.Expose[1].Protocol != "grpc" {
		t.Errorf("expose = %+v", f.Expose)
	}
}

func TestMistakesAreReportedWithTheirPlace(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"misspelt field":      {"contxt: dev\n", "contxt"},
		"misspelt in entry":   {"expose:\n  - name: a\n    port: 80\n    locaPort: 3000\n", "locaPort"},
		"bad name":            {"expose:\n  - name: Cart_Service\n    port: 80\n", "expose[0].name"},
		"name too long":       {"expose:\n  - name: " + strings.Repeat("a", 64) + "\n    port: 80\n", "expose[0].name"},
		"no name":             {"expose:\n  - port: 80\n", "expose[0].name"},
		"no port":             {"expose:\n  - name: a\n", "expose[0].port"},
		"port too big":        {"expose:\n  - name: a\n    port: 70000\n", "expose[0].port"},
		"bad local port":      {"expose:\n  - name: a\n    port: 80\n    localPort: -1\n", "expose[0].localPort"},
		"bad protocol":        {"expose:\n  - name: a\n    port: 80\n    protocol: HTTP/2\n", "expose[0].protocol"},
		"same port twice":     {"expose:\n  - name: a\n    port: 80\n  - name: a\n    port: 80\n    localPort: 81\n", "expose[1]"},
		"second entry bad":    {"expose:\n  - name: a\n    port: 80\n  - name: b\n    port: 0\n", "expose[1].port"},
		"forward no port":     {"forward:\n  - localPort: 5432\n    target: postgres.data.svc\n", "forward[0].target"},
		"forward no host":     {"forward:\n  - localPort: 5432\n    target: \":5432\"\n", "forward[0].target"},
		"forward bad local":   {"forward:\n  - localPort: 0\n    target: a:1\n", "forward[0].localPort"},
		"forward local twice": {"forward:\n  - localPort: 5432\n    target: a:1\n  - localPort: 5432\n    target: b:1\n", "forward[1]"},
		"port as text":        {"expose:\n  - name: a\n    port: eighty\n", "port"},
		"not yaml":            {"expose: [unclosed\n", ""},
	} {
		_, err := Parse([]byte(tc.body), "ash")
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not point at %q", name, err, tc.want)
		}
	}
}

func TestUserPlaceholder(t *testing.T) {
	body := []byte("expose:\n  - name: cart-${USER}\n    port: 80\n")
	for user, want := range map[string]string{
		"ash":          "cart-ash",
		"Ash":          "cart-ash",
		`CORP\Ash.S`:   "cart-corp-ash-s",
		"ash@corp.com": "cart-ash-corp-com",
		"_ash_":        "cart-ash",
	} {
		f, err := Parse(body, user)
		if err != nil {
			t.Errorf("user %q: %v", user, err)
			continue
		}
		if f.Expose[0].Name != want {
			t.Errorf("user %q: name = %q, want %q", user, f.Expose[0].Name, want)
		}
	}
	// With no usable user name the placeholder cannot be filled in.
	for _, user := range []string{"", "___"} {
		if _, err := Parse(body, user); err == nil || !strings.Contains(err.Error(), "${USER}") {
			t.Errorf("user %q: err = %v", user, err)
		}
	}
	// A file that does not use the placeholder does not need a user.
	if _, err := Parse([]byte("expose:\n  - name: cart\n    port: 80\n"), ""); err != nil {
		t.Errorf("file without placeholder: %v", err)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ashctl.yaml")
	if _, err := Load(path, "ash"); err == nil || !strings.Contains(err.Error(), "ashctl generate") {
		t.Errorf("missing file: err = %v, want a hint to generate one", err)
	}
	os.WriteFile(path, []byte("context: dev\nexpose:\n  - name: a\n    port: 80\n"), 0o644)
	f, err := Load(path, "ash")
	if err != nil || f.Context != "dev" || len(f.Expose) != 1 {
		t.Errorf("f = %+v, err %v", f, err)
	}
	os.WriteFile(path, []byte("expose:\n  - name: BAD\n    port: 80\n"), 0o644)
	if _, err := Load(path, "ash"); err == nil || !strings.Contains(err.Error(), path) {
		t.Errorf("bad file: err = %v, want it to name the file", err)
	}
}
