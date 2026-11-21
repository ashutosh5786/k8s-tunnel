package sandbox

import (
	"strings"

	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/ashutosh5786/ashbox/api/v1alpha1"
)

// DefaultFile is the file "ashctl sandbox generate" writes when no -o is given.
const DefaultFile = "sandbox.yaml"

// DefaultTemplateName is the sandbox name in a template when none is given.
const DefaultTemplateName = "my-sandbox"

// Template returns the commented starting file "ashctl sandbox generate"
// writes. The fork is an example: the file parses as written, but its fork
// must point at a real workload before it is created.
func Template(name, baggageKey string) string {
	if name == "" {
		name = DefaultTemplateName
	}
	if baggageKey == "" {
		baggageKey = "routing-key"
	}
	return strings.NewReplacer("{{name}}", name, "{{key}}", baggageKey).Replace(template)
}

const template = `# sandbox.yaml
#
# An ashbox sandbox: changed copies ("forks") of services in the cluster.
# Only requests with the sandbox's routing key reach the forks:
#
#   baggage: {{key}}=<routing key>
#
# "ashctl sandbox get {{name}}" shows the exact header. Other requests go
# to the normal services, which do not change.
#
# 1. Change the fork below to your own workload and image.
# 2. Run:  ashctl sandbox create -f sandbox.yaml --wait
# 3. Check: ashctl sandbox get {{name}}
#
# Remove the leading "#" from a line to turn it on.

apiVersion: ashbox.dev/v1alpha1
kind: Sandbox
metadata:
  # Use lower-case letters, digits and "-". It must be unique in the
  # cluster. The name is not the routing key.
  name: {{name}}
spec:
  # The routing key. It never changes after creation. Choose one way:
  #
  # keySource: a stable identity. The key is derived from it, so the same
  # keySource always gives the same key, also after you delete the sandbox
  # and create it again. Use one per pull request.
  #keySource: github.com/acme/shop#42
  #
  # routingKey: the exact key: 8 to 32 lower-case letters, digits and "-".
  #routingKey: cart-pricing-test
  #
  # Neither: a random key, new for each sandbox.

  # description: text that "ashctl sandbox get" shows.
  #description: Try the new cart pricing

  # ttl: how long the sandbox lives. Then ashbox deletes it, with all its
  # forks and routes. Examples: 30m, 4h, 14d.
  # offsetFrom: createdAt (default) counts from creation; updatedAt
  # counts from the last change, so a sandbox in use lives longer.
  ttl:
    duration: 4h
    #offsetFrom: updatedAt

  # forks: workloads to copy. Each fork runs one pod next to its baseline
  # and gets the keyed requests for the baseline's Services.
  #
  #   forkOf            The baseline workload.
  #     kind            Deployment (default) or Rollout (Argo Rollouts).
  #     namespace       Its namespace.
  #     name            Its name.
  #   customizations    What is different in the fork. All are optional.
  #     images          A new image for each named container.
  #     env             Env vars to add or replace, as in a pod spec.
  #     patch           A patch for the fork Deployment, applied last.
  #       type          strategic, merge or json.
  #       value         The patch, as YAML or JSON text.
  forks:
    - forkOf:
        kind: Deployment
        namespace: shop
        name: cart
      customizations:
        images:
          - container: app
            image: registry.example.com/cart:pr-42
        #env:
        #  - name: LOG_LEVEL
        #    value: debug
        #patch:
        #  type: strategic
        #  value: |
        #    spec:
        #      template:
        #        spec:
        #          containers:
        #            - name: app
        #              resources:
        #                limits:
        #                  memory: 1Gi

  # local: send keyed requests for a workload to a service on your
  # machine, through k8s-tunnel. "ashctl local" writes this part for you.
  # Write it by hand only for special cases.
  #
  #   name        A name for the entry, unique in this sandbox.
  #   from        The baseline workload, as in forkOf.
  #   tunnel      The Service that "ashctl connect --expose" made in the
  #               k8s-tunnel namespace.
  #   mappings    port: the baseline Service port.
  #               tunnelPort: the tunnel Service port (default: port).
  #
  #local:
  #  - name: checkout
  #    from:
  #      namespace: shop
  #      name: checkout
  #    tunnel:
  #      namespace: k8s-tunnel
  #      name: checkout-ash
  #    mappings:
  #      - port: 8080
`

// Marshal renders a sandbox as a clean manifest: apiVersion, kind,
// metadata and spec only, with no status or server-set fields.
func Marshal(sb *v1alpha1.Sandbox) ([]byte, error) {
	cp := sb.DeepCopy()
	cp.Status = v1alpha1.SandboxStatus{}
	u, err := toUnstructured(cp)
	if err != nil {
		return nil, err
	}
	delete(u.Object, "status")
	if md, ok := u.Object["metadata"].(map[string]any); ok {
		for _, k := range []string{"creationTimestamp", "resourceVersion", "uid", "generation", "managedFields"} {
			delete(md, k)
		}
	}
	return yaml.Marshal(u.Object)
}
