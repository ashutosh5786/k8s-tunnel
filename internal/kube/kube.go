// Package kube connects the client to a cluster: it loads kubeconfig
// contexts, picks a tunnel-server pod, and opens a port-forward to it
// through the API server.
package kube

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// Cluster is an authenticated connection to one cluster.
type Cluster struct {
	Config *rest.Config
	Client kubernetes.Interface
}

// Load reads the kubeconfig the same way kubectl does. An empty path uses
// KUBECONFIG or ~/.kube/config; an empty context uses the current one.
func Load(kubeconfigPath, contextName string) (*Cluster, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfigPath
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Cluster{Config: cfg, Client: client}, nil
}

// PickPod chooses a ready tunnel-server pod. The same key picks the same
// pod while the set of ready pods is unchanged, so a reconnecting session
// tends to return to its replica; different keys spread across replicas.
func PickPod(ctx context.Context, client kubernetes.Interface, namespace, selector, key string) (string, error) {
	list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("list tunnel-server pods: %w", err)
	}
	var ready []string
	for i := range list.Items {
		if isReady(&list.Items[i]) {
			ready = append(ready, list.Items[i].Name)
		}
	}
	if len(ready) == 0 {
		return "", fmt.Errorf("no ready tunnel-server pod in namespace %q matching %q (found %d not ready)",
			namespace, selector, len(list.Items))
	}
	sort.Strings(ready)
	h := fnv.New32a()
	h.Write([]byte(key))
	return ready[int(h.Sum32()%uint32(len(ready)))], nil
}

func isReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// PortForward opens a connection to a port on a pod through the API
// server. Closing the returned connection ends the port-forward.
func (c *Cluster) PortForward(ctx context.Context, namespace, pod string, port int) (net.Conn, error) {
	url := c.Client.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("portforward").URL()

	transport, upgrader, err := spdy.RoundTripperFor(c.Config)
	if err != nil {
		return nil, err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	// Prefer WebSockets as kubectl does, falling back to SPDY for older
	// clusters and proxies that cannot upgrade.
	if ws, err := portforward.NewSPDYOverWebsocketDialer(url, c.Config); err == nil {
		dialer = portforward.NewFallbackDialer(ws, dialer, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
	}

	stop := make(chan struct{})
	ready := make(chan struct{})
	var errOut strings.Builder
	// The forwarder listens on a loopback port chosen by the system; the
	// tunnel then runs over a plain TCP connection to that port.
	fw, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"},
		[]string{"0:" + strconv.Itoa(port)}, stop, ready, io.Discard, &errOut)
	if err != nil {
		return nil, err
	}
	failed := make(chan error, 1)
	go func() { failed <- fw.ForwardPorts() }()

	var once sync.Once
	closeForward := func() { once.Do(func() { close(stop) }) }
	select {
	case <-ready:
	case err := <-failed:
		closeForward()
		if err == nil {
			err = errors.New("port-forward ended before it was ready")
		}
		return nil, fmt.Errorf("port-forward to %s/%s: %w", namespace, pod, err)
	case <-ctx.Done():
		closeForward()
		return nil, ctx.Err()
	}

	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		closeForward()
		return nil, fmt.Errorf("port-forward to %s/%s: no local port: %v", namespace, pod, err)
	}
	local := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[0].Local)))
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", local)
	if err != nil {
		closeForward()
		return nil, err
	}
	return &forwardedConn{Conn: conn, stop: closeForward}, nil
}

// forwardedConn ends the port-forward when the connection is closed.
type forwardedConn struct {
	net.Conn
	stop func()
}

func (c *forwardedConn) Close() error {
	err := c.Conn.Close()
	c.stop()
	return err
}
