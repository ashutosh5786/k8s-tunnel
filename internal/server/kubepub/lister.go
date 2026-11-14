package kubepub

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"k8s-tunnel/internal/server"
)

// Lister implements server.Lister by listing Services through the
// Kubernetes API. Results are cached briefly, because every connected
// client asks for the same listing on a timer.
type Lister struct {
	Client kubernetes.Interface
	// TTL is how long a listing is reused. Defaults to 10s.
	TTL time.Duration

	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	at   time.Time
	list []server.ServiceInfo
}

var _ server.Lister = (*Lister)(nil)

// Services lists Services with at least one TCP port, sorted by namespace
// and name.
func (l *Lister) Services(ctx context.Context, namespaces []string) ([]server.ServiceInfo, error) {
	namespaces = append([]string(nil), namespaces...)
	sort.Strings(namespaces)
	key := strings.Join(namespaces, ",")
	ttl := l.TTL
	if ttl == 0 {
		ttl = 10 * time.Second
	}

	l.mu.Lock()
	if c, ok := l.cache[key]; ok && time.Since(c.at) < ttl {
		l.mu.Unlock()
		return c.list, nil
	}
	l.mu.Unlock()

	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}
	var out []server.ServiceInfo
	for _, ns := range namespaces {
		list, err := l.Client.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			svc := &list.Items[i]
			info := server.ServiceInfo{Namespace: svc.Namespace, Name: svc.Name}
			for _, p := range svc.Spec.Ports {
				// An empty protocol means TCP. The tunnel carries TCP only.
				if p.Protocol == "" || p.Protocol == corev1.ProtocolTCP {
					info.Ports = append(info.Ports, int(p.Port))
				}
			}
			if len(info.Ports) > 0 {
				out = append(out, info)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})

	l.mu.Lock()
	if l.cache == nil {
		l.cache = map[string]cached{}
	}
	l.cache[key] = cached{at: time.Now(), list: out}
	l.mu.Unlock()
	return out, nil
}
