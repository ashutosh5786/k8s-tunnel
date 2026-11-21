package sandbox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/ashutosh5786/ashbox/api/v1alpha1"
)

// The template must load as written (strict: no unknown fields), so a
// developer only changes values and never fixes syntax.
func TestTemplateLoadsAsWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sb.yaml")
	if err := os.WriteFile(path, []byte(Template("pr-7", "rk")), 0o600); err != nil {
		t.Fatal(err)
	}
	sb, err := LoadFile(path)
	if err != nil {
		t.Fatalf("template does not load: %v", err)
	}
	if sb.Name != "pr-7" || sb.Spec.TTL == nil || sb.Spec.TTL.Duration != "4h" || len(sb.Spec.Forks) != 1 || len(sb.Spec.Local) != 0 {
		t.Fatalf("sandbox = %+v", sb)
	}
	if sb.Spec.KeySource != "" || sb.Spec.RoutingKey != "" {
		t.Errorf("template sets a key by default: %+v", sb.Spec)
	}
	if body := Template("pr-7", "rk"); !strings.Contains(body, "baggage: rk=<routing key>") || !strings.Contains(body, "sandbox get pr-7") || strings.Contains(body, "{{") {
		t.Errorf("placeholders not filled:\n%s", body)
	}
}

func TestTemplateDefaults(t *testing.T) {
	body := Template("", "")
	if !strings.Contains(body, "name: "+DefaultTemplateName) || !strings.Contains(body, "baggage: routing-key=<routing key>") {
		t.Errorf("defaults not used:\n%s", body)
	}
}

// Marshal writes a manifest that loads back to the same spec, with no
// status or server fields.
func TestMarshalRoundTrip(t *testing.T) {
	forks, err := ParseForks([]string{"shop/cart=app:cart:pr-42", "rollout/shop/match"})
	if err != nil {
		t.Fatal(err)
	}
	sb := &v1alpha1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: "pr-42", ResourceVersion: "9"}}
	sb.Spec.Forks = forks
	sb.Spec.TTL = &v1alpha1.TTL{Duration: "2h"}
	sb.Status.Phase = v1alpha1.PhaseReady

	b, err := Marshal(sb)
	if err != nil {
		t.Fatal(err)
	}
	body := string(b)
	for _, bad := range []string{"status", "creationTimestamp", "resourceVersion"} {
		if strings.Contains(body, bad) {
			t.Errorf("manifest has %q:\n%s", bad, body)
		}
	}
	if !strings.HasPrefix(body, "apiVersion: ashbox.dev/v1alpha1\nkind: Sandbox\n") {
		t.Errorf("manifest head:\n%s", body)
	}
	if sb.Status.Phase != v1alpha1.PhaseReady {
		t.Error("Marshal changed its input")
	}

	path := filepath.Join(t.TempDir(), "sb.yaml")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	back, err := LoadFile(path)
	if err != nil {
		t.Fatalf("manifest does not load: %v\n%s", err, body)
	}
	if back.Name != "pr-42" || len(back.Spec.Forks) != 2 || back.Spec.Forks[1].ForkOf.Kind != v1alpha1.KindRollout || back.Spec.TTL.Duration != "2h" {
		t.Fatalf("round trip = %+v", back)
	}
}
