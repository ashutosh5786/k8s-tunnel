// Package profile reads the ashctl.yaml file that describes what a
// project wants from the tunnel, so it can be set up with one command and
// shared in the project's repository.
package profile

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"
)

// DefaultName is the file "ashctl apply" and "ashctl generate" use when
// none is named.
const DefaultName = "ashctl.yaml"

// File is the content of an ashctl.yaml.
type File struct {
	// Context is the kubeconfig context to connect to. Empty means the
	// current one.
	Context string `json:"context,omitempty"`
	// Namespace is where the tunnel server runs. Empty means the default.
	Namespace string `json:"namespace,omitempty"`
	// HomeNamespace is the namespace whose Services also resolve by bare
	// name.
	HomeNamespace string `json:"homeNamespace,omitempty"`
	// HostsNamespaces limits which namespaces' Services resolve locally.
	HostsNamespaces []string `json:"hostsNamespaces,omitempty"`
	// Expose lists local services to make reachable in the cluster.
	Expose []Expose `json:"expose,omitempty"`
	// Forward lists cluster addresses to reach on local ports.
	Forward []Forward `json:"forward,omitempty"`
}

// Expose is one local port made reachable in the cluster. Entries with
// the same name become one Service with several ports.
type Expose struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`
	LocalPort int    `json:"localPort,omitempty"`
	Protocol  string `json:"protocol,omitempty"`
}

// Forward is one cluster address reachable on a local port.
type Forward struct {
	LocalPort int    `json:"localPort"`
	Target    string `json:"target"`
}

var (
	labelRE    = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	protocolRE = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	notLabelRE = regexp.MustCompile(`[^a-z0-9-]+`)
)

// SanitizeUser turns a user name into something that can be part of a
// Service name: lower case, with anything else replaced by a dash.
func SanitizeUser(user string) string {
	s := notLabelRE.ReplaceAllString(strings.ToLower(user), "-")
	return strings.Trim(s, "-")
}

// Load reads and checks the file at path. ${USER} in exposure names is
// replaced by user, so one shared file gives every developer their own
// Service names.
func Load(path, user string) (*File, error) {
	body, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s not found: create one with \"ashctl generate\"", path)
	}
	if err != nil {
		return nil, err
	}
	f, err := Parse(body, user)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

// Parse is Load for content already in memory.
func Parse(body []byte, user string) (*File, error) {
	var f File
	// Strict, so a misspelt field is an error and not silently ignored.
	if err := yaml.UnmarshalStrict(body, &f); err != nil {
		return nil, err
	}
	user = SanitizeUser(user)
	for i := range f.Expose {
		name := f.Expose[i].Name
		if strings.Contains(name, "${USER}") {
			if user == "" {
				return nil, fmt.Errorf("expose[%d].name uses ${USER}, but your user name is not known; pass --user", i)
			}
			f.Expose[i].Name = strings.ReplaceAll(name, "${USER}", user)
		}
	}
	if err := f.validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

func validPort(n int) bool { return n >= 1 && n <= 65535 }

func (f *File) validate() error {
	type key struct {
		name string
		port int
	}
	seen := map[key]bool{}
	for i := range f.Expose {
		e := &f.Expose[i]
		at := fmt.Sprintf("expose[%d]", i)
		if !labelRE.MatchString(e.Name) {
			return fmt.Errorf("%s.name %q must be a lowercase name of letters, digits and dashes, starting with a letter", at, e.Name)
		}
		if !validPort(e.Port) {
			return fmt.Errorf("%s.port %d must be between 1 and 65535", at, e.Port)
		}
		if e.LocalPort == 0 {
			e.LocalPort = e.Port
		}
		if !validPort(e.LocalPort) {
			return fmt.Errorf("%s.localPort %d must be between 1 and 65535", at, e.LocalPort)
		}
		if e.Protocol != "" && !protocolRE.MatchString(e.Protocol) {
			return fmt.Errorf("%s.protocol %q must be a lowercase word such as http or grpc", at, e.Protocol)
		}
		k := key{e.Name, e.Port}
		if seen[k] {
			return fmt.Errorf("%s: port %d is listed twice for %s", at, e.Port, e.Name)
		}
		seen[k] = true
	}
	locals := map[int]bool{}
	for i, fw := range f.Forward {
		at := fmt.Sprintf("forward[%d]", i)
		if !validPort(fw.LocalPort) {
			return fmt.Errorf("%s.localPort %d must be between 1 and 65535", at, fw.LocalPort)
		}
		host, port, err := net.SplitHostPort(fw.Target)
		n, perr := strconv.Atoi(port)
		if err != nil || host == "" || perr != nil || !validPort(n) {
			return fmt.Errorf("%s.target %q must be HOST:PORT, for example postgres.db.svc:5432", at, fw.Target)
		}
		if locals[fw.LocalPort] {
			return fmt.Errorf("%s: local port %d is listed twice", at, fw.LocalPort)
		}
		locals[fw.LocalPort] = true
	}
	return nil
}

// ExposeSpecs returns the exposures in the NAME:PORT:LOCALPORT[/PROTOCOL]
// form the command line uses.
func (f *File) ExposeSpecs() []string {
	out := make([]string, 0, len(f.Expose))
	for _, e := range f.Expose {
		spec := fmt.Sprintf("%s:%d:%d", e.Name, e.Port, e.LocalPort)
		if e.Protocol != "" {
			spec += "/" + e.Protocol
		}
		out = append(out, spec)
	}
	return out
}

// ForwardSpecs returns the forwards in the LOCALPORT:HOST:PORT form the
// command line uses.
func (f *File) ForwardSpecs() []string {
	out := make([]string, 0, len(f.Forward))
	for _, fw := range f.Forward {
		out = append(out, fmt.Sprintf("%d:%s", fw.LocalPort, fw.Target))
	}
	return out
}

// Template is the starting file "ashctl generate" writes. Every field is
// explained and shown with an example, and all of them are commented out,
// so applying the file unchanged connects and exposes nothing.
const Template = `# ashctl.yaml
#
# Describes what this project needs from the cluster tunnel. In this
# folder, run:
#
#   ashctl apply
#
# It connects if you are not connected yet, then exposes everything listed
# under "expose". Run it again after editing this file; only what changed
# is touched. Things you exposed by hand with "ashctl expose" are left
# alone unless you run "ashctl apply --prune".
#
# Remove the leading "#" from a line to turn it on. Every field is
# optional.

# context: the cluster to connect to, as named in your kubeconfig
# ("kubectl config get-contexts" lists them). Left out, your current
# kubectl context is used.
#
#context: dev

# homeNamespace: the namespace your service normally runs in. Services in
# that namespace can then be called by bare name, exactly as they could
# from a pod there: "cart:8080" as well as "cart.shop.svc.cluster.local:8080".
# This only has an effect when ashctl runs elevated (administrator
# terminal on Windows, sudo on Linux), because that is when cluster names
# resolve on your machine.
#
#homeNamespace: shop

# hostsNamespaces: only make Services in these namespaces resolve on your
# machine. Left out, all namespaces are used. Useful in large clusters.
#
#hostsNamespaces:
#  - shop
#  - data

# expose: services running on your machine that the cluster should be
# able to call. Each entry becomes a Service in the tunnel's namespace,
# reachable from the cluster as NAME.k8s-tunnel.svc:PORT.
#
#   name       The Service name. Must be unique in the cluster, so include
#              ${USER}, which is replaced by your user name. That way
#              everyone sharing this file gets their own Service.
#              Example: cart-${USER}
#   port       The port the cluster uses to call it. Normally the port the
#              real service uses.
#              Example: 8080
#   localPort  The port your service listens on, on your machine. Left
#              out, it is the same as "port".
#              Example: 3000
#   protocol   What the port carries: http or grpc. A service mesh needs
#              this to route by request header. Leave it out for plain TCP
#              such as a database.
#              Example: http
#
# Two entries with the same name become one Service with two ports.
#
#expose:
#  - name: cart-${USER}
#    port: 8080
#    localPort: 3000
#    protocol: http
#  - name: cart-${USER}
#    port: 9090
#    protocol: grpc

# forward: cluster addresses to reach on a local port. You only need this
# when cluster names do not resolve on your machine (ashctl not run
# elevated). Forwards are set up when connecting; to change them,
# disconnect and apply again.
#
#   localPort  The port to open on your machine.
#              Example: 5432
#   target     The cluster address, as HOST:PORT.
#              Example: postgres.data.svc:5432
#
#forward:
#  - localPort: 5432
#    target: postgres.data.svc:5432

# namespace: the namespace the tunnel server is installed in. Left out,
# "k8s-tunnel" is used. Change it only if your cluster's tunnel was
# installed somewhere else.
#
#namespace: k8s-tunnel
`
