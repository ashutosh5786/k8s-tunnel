// Package kubepub publishes exposures as Kubernetes Services. Each Service
// selects the one server pod that holds the tunnel, so the Kubernetes API
// is the registry of who owns what and no other store is needed.
package kubepub

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"github.com/ashutosh5786/k8s-tunnel/internal/server"
)

const (
	// ManagedByLabel marks Services owned by the tunnel.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	ManagedByValue = "k8s-tunnel"
	// SessionLabel holds the session that owns the Service.
	SessionLabel = "k8s-tunnel.io/session"
	// ReplicaLabel holds the server pod the Service points at.
	ReplicaLabel = "k8s-tunnel.io/replica"
	// UserAnnotation holds the user name, which need not be a valid label.
	UserAnnotation = "k8s-tunnel.io/user"

	// podNameLabel is set by the StatefulSet controller on every pod and
	// lets a Service select exactly one replica.
	podNameLabel = "statefulset.kubernetes.io/pod-name"
)

// Publisher implements server.Publisher and server.Sweeper.
type Publisher struct {
	Client    kubernetes.Interface
	Namespace string
	// PodName is this replica's pod name.
	PodName string
}

var (
	_ server.Publisher = (*Publisher)(nil)
	_ server.Sweeper   = (*Publisher)(nil)
)

func (p *Publisher) desired(e server.Exposure) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.Name,
			Namespace: p.Namespace,
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionLabel:   e.Session,
				ReplicaLabel:   p.PodName,
			},
			Annotations: map[string]string{UserAnnotation: e.User},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{podNameLabel: p.PodName},
		},
	}
	for _, port := range e.Ports {
		sp := corev1.ServicePort{
			Name:       port.Name,
			Protocol:   corev1.ProtocolTCP,
			Port:       int32(port.Port),
			TargetPort: intstr.FromInt32(int32(port.ServerPort)),
		}
		if sp.Name == "" {
			// Kubernetes requires names once a Service has several ports.
			sp.Name = fmt.Sprintf("tcp-%d", port.Port)
		}
		if port.AppProtocol != "" {
			ap := port.AppProtocol
			sp.AppProtocol = &ap
		}
		svc.Spec.Ports = append(svc.Spec.Ports, sp)
	}
	return svc
}

// Publish creates the Service for e, or re-points an existing one at this
// replica. It refuses to touch a Service the tunnel does not manage or one
// that belongs to a different user's session.
func (p *Publisher) Publish(ctx context.Context, e server.Exposure) (string, error) {
	want := p.desired(e)
	host := fmt.Sprintf("%s.%s.svc", e.Name, p.Namespace)
	svcs := p.Client.CoreV1().Services(p.Namespace)

	cur, err := svcs.Get(ctx, e.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := svcs.Create(ctx, want, metav1.CreateOptions{}); err != nil {
			return "", err
		}
		return host, nil
	}
	if err != nil {
		return "", err
	}
	if cur.Labels[ManagedByLabel] != ManagedByValue {
		return "", fmt.Errorf("service %q exists and is not managed by k8s-tunnel", e.Name)
	}
	// The same session may always reclaim its Service, wherever it
	// reconnected. A new session may take over only its own user's.
	if cur.Labels[SessionLabel] != e.Session && cur.Annotations[UserAnnotation] != e.User {
		return "", fmt.Errorf("service %q is in use by %s", e.Name, cur.Annotations[UserAnnotation])
	}

	upd := cur.DeepCopy()
	if upd.Labels == nil {
		upd.Labels = map[string]string{}
	}
	if upd.Annotations == nil {
		upd.Annotations = map[string]string{}
	}
	for k, v := range want.Labels {
		upd.Labels[k] = v
	}
	for k, v := range want.Annotations {
		upd.Annotations[k] = v
	}
	upd.Spec.Selector = want.Spec.Selector
	upd.Spec.Ports = want.Spec.Ports
	if _, err := svcs.Update(ctx, upd, metav1.UpdateOptions{}); err != nil {
		return "", err
	}
	return host, nil
}

// Unpublish deletes the Service if it still belongs to the given session
// and still points at this replica. If the session reconnected to another
// replica, that replica owns the Service now and it is left alone.
func (p *Publisher) Unpublish(ctx context.Context, name, session string) error {
	svcs := p.Client.CoreV1().Services(p.Namespace)
	cur, err := svcs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if cur.Labels[ManagedByLabel] != ManagedByValue ||
		cur.Labels[SessionLabel] != session ||
		cur.Labels[ReplicaLabel] != p.PodName {
		return nil
	}
	return p.delete(ctx, cur)
}

// delete removes exactly the object that was inspected, so a Service that
// changed hands in between is not deleted by mistake.
func (p *Publisher) delete(ctx context.Context, svc *corev1.Service) error {
	err := p.Client.CoreV1().Services(p.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &svc.UID, ResourceVersion: &svc.ResourceVersion},
	})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// Sweep deletes Services that point at this replica but have no live
// exposure, which are left over from a crash or restart.
func (p *Publisher) Sweep(ctx context.Context, live func(name string) bool) error {
	sel := labels.SelectorFromSet(labels.Set{ManagedByLabel: ManagedByValue, ReplicaLabel: p.PodName})
	list, err := p.Client.CoreV1().Services(p.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return err
	}
	for i := range list.Items {
		svc := &list.Items[i]
		if live(svc.Name) {
			continue
		}
		if err := p.delete(ctx, svc); err != nil {
			return err
		}
	}
	return nil
}
