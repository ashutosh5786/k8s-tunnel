// Package sandbox creates, reads and deletes ashbox Sandbox objects for
// ashctl. ashbox (github.com/ashutosh5786/ashbox) runs the controller in the
// cluster; this package is only its client.
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/ashutosh5786/ashbox/api/v1alpha1"
)

// Resource is the Sandbox resource in the cluster.
var Resource = schema.GroupVersionResource{Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version, Resource: "sandboxes"}

// Labels ashctl puts on the sandboxes it creates.
const (
	// LabelSession ties a sandbox to an ashctl connection, so disconnecting
	// deletes it.
	LabelSession = "ashbox.dev/ashctl-session"
	// LabelUser records the ashctl user.
	LabelUser = "ashbox.dev/ashctl-user"
)

// ErrNotInstalled means the cluster has no Sandbox resource.
var ErrNotInstalled = errors.New("ashbox is not installed in this cluster (no sandboxes.ashbox.dev resource)")

// Client talks to the Sandbox resource.
type Client struct {
	res dynamic.NamespaceableResourceInterface
}

// New makes a client from a REST config.
func New(cfg *rest.Config) (*Client, error) {
	d, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return NewFromDynamic(d), nil
}

// NewFromDynamic makes a client from a dynamic client (tests use a fake).
func NewFromDynamic(d dynamic.Interface) *Client {
	return &Client{res: d.Resource(Resource)}
}

func toUnstructured(sb *v1alpha1.Sandbox) (*unstructured.Unstructured, error) {
	sb.APIVersion = v1alpha1.GroupVersion.String()
	sb.Kind = "Sandbox"
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(sb)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: m}, nil
}

func fromUnstructured(u *unstructured.Unstructured) (*v1alpha1.Sandbox, error) {
	sb := &v1alpha1.Sandbox{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, sb)
	return sb, err
}

func wrap(err error) error {
	if apierrors.IsNotFound(err) && strings.Contains(err.Error(), "the server could not find the requested resource") {
		return ErrNotInstalled
	}
	return err
}

// Create creates a sandbox.
func (c *Client) Create(ctx context.Context, sb *v1alpha1.Sandbox) (*v1alpha1.Sandbox, error) {
	u, err := toUnstructured(sb)
	if err != nil {
		return nil, err
	}
	out, err := c.res.Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return nil, wrap(err)
	}
	return fromUnstructured(out)
}

// Update replaces a sandbox's spec and labels.
func (c *Client) Update(ctx context.Context, sb *v1alpha1.Sandbox) (*v1alpha1.Sandbox, error) {
	u, err := toUnstructured(sb)
	if err != nil {
		return nil, err
	}
	out, err := c.res.Update(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		return nil, wrap(err)
	}
	return fromUnstructured(out)
}

// Get reads a sandbox.
func (c *Client) Get(ctx context.Context, name string) (*v1alpha1.Sandbox, error) {
	u, err := c.res.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, wrap(err)
	}
	return fromUnstructured(u)
}

// List reads sandboxes, optionally filtered by a label selector.
func (c *Client) List(ctx context.Context, selector string) ([]v1alpha1.Sandbox, error) {
	ul, err := c.res.List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, wrap(err)
	}
	out := make([]v1alpha1.Sandbox, 0, len(ul.Items))
	for i := range ul.Items {
		sb, err := fromUnstructured(&ul.Items[i])
		if err != nil {
			return nil, err
		}
		out = append(out, *sb)
	}
	return out, nil
}

// Delete deletes a sandbox. Its forks and routes go with it.
func (c *Client) Delete(ctx context.Context, name string) error {
	return wrap(c.res.Delete(ctx, name, metav1.DeleteOptions{}))
}

// DeleteSession deletes the sandboxes created by one ashctl connection and
// returns their names.
func (c *Client) DeleteSession(ctx context.Context, session string) ([]string, error) {
	list, err := c.List(ctx, LabelSession+"="+session)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, sb := range list {
		if err := c.Delete(ctx, sb.Name); err != nil && !apierrors.IsNotFound(err) {
			return names, err
		}
		names = append(names, sb.Name)
	}
	return names, nil
}

// LoadFile reads a Sandbox from a YAML or JSON file. Unknown fields are an
// error, so typing mistakes do not pass silently.
func LoadFile(path string) (*v1alpha1.Sandbox, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sb := &v1alpha1.Sandbox{}
	if err := yaml.UnmarshalStrict(body, sb); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if sb.APIVersion != "" && sb.APIVersion != v1alpha1.GroupVersion.String() {
		return nil, fmt.Errorf("%s: apiVersion is %q, want %q", path, sb.APIVersion, v1alpha1.GroupVersion.String())
	}
	if sb.Kind != "" && sb.Kind != "Sandbox" {
		return nil, fmt.Errorf("%s: kind is %q, want Sandbox", path, sb.Kind)
	}
	return sb, nil
}

var nameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidName checks a sandbox name: a DNS label of at most 63 characters,
// because it becomes the routing key and part of object names and labels.
func ValidName(name string) error {
	if len(name) == 0 || len(name) > 63 || !nameRE.MatchString(name) {
		return fmt.Errorf("invalid sandbox name %q: use lower-case letters, digits and '-', at most 63 characters", name)
	}
	return nil
}

// NameFromUser makes a sandbox name from a user name ("Ash.Smith" -> "ash-smith").
func NameFromUser(user string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(user) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	name := strings.Trim(b.String(), "-")
	if len(name) > 63 {
		name = strings.Trim(name[:63], "-")
	}
	return name
}

// ParseWorkload reads "[rollout/]NAMESPACE/NAME".
func ParseWorkload(s string) (v1alpha1.WorkloadRef, error) {
	parts := strings.Split(s, "/")
	ref := v1alpha1.WorkloadRef{Kind: v1alpha1.KindDeployment}
	switch {
	case len(parts) == 2:
	case len(parts) == 3 && strings.EqualFold(parts[0], "rollout"):
		ref.Kind = v1alpha1.KindRollout
		parts = parts[1:]
	case len(parts) == 3 && strings.EqualFold(parts[0], "deployment"):
		parts = parts[1:]
	default:
		return ref, fmt.Errorf("invalid workload %q (want [rollout/]NAMESPACE/NAME)", s)
	}
	if parts[0] == "" || parts[1] == "" {
		return ref, fmt.Errorf("invalid workload %q (want [rollout/]NAMESPACE/NAME)", s)
	}
	ref.Namespace, ref.Name = parts[0], parts[1]
	return ref, nil
}

// ParseForks reads --fork values "[rollout/]NAMESPACE/NAME[=CONTAINER:IMAGE]".
// Values naming the same workload are merged, so several containers of one
// workload can get new images.
func ParseForks(specs []string) ([]v1alpha1.Fork, error) {
	var out []v1alpha1.Fork
	index := map[string]int{}
	for _, spec := range specs {
		bad := func(reason string) error {
			return fmt.Errorf("invalid --fork %q: %s (want [rollout/]NAMESPACE/NAME[=CONTAINER:IMAGE])", spec, reason)
		}
		target, image, hasImage := strings.Cut(spec, "=")
		ref, err := ParseWorkload(target)
		if err != nil {
			return nil, bad("bad workload")
		}
		key := string(ref.Kind) + "/" + ref.Namespace + "/" + ref.Name
		i, ok := index[key]
		if !ok {
			i = len(out)
			index[key] = i
			out = append(out, v1alpha1.Fork{ForkOf: ref})
		}
		if !hasImage {
			continue
		}
		container, img, ok := strings.Cut(image, ":")
		if !ok || container == "" || img == "" {
			return nil, bad("image must be CONTAINER:IMAGE")
		}
		f := &out[i]
		if f.Customizations == nil {
			f.Customizations = &v1alpha1.Customizations{}
		}
		for _, existing := range f.Customizations.Images {
			if existing.Container == container {
				return nil, bad(fmt.Sprintf("container %q given twice", container))
			}
		}
		f.Customizations.Images = append(f.Customizations.Images, v1alpha1.ImageOverride{Container: container, Image: img})
	}
	return out, nil
}

// Header is the request header that reaches a sandbox.
func Header(baggageKey, routingKey string) string {
	return fmt.Sprintf("baggage: %s=%s", baggageKey, routingKey)
}
