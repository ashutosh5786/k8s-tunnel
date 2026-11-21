package sandbox

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	v1alpha1 "github.com/ashutosh5786/ashbox/api/v1alpha1"
)

// Describe prints one sandbox: phase, routing header, forks and local entries.
func Describe(w io.Writer, sb *v1alpha1.Sandbox, baggageKey string, now time.Time) {
	st := sb.Status
	phase := string(st.Phase)
	if phase == "" {
		phase = "Unknown (no status yet: is the ashbox controller running?)"
	}
	ready := 0
	for _, f := range st.Forks {
		if f.Ready {
			ready++
		}
	}
	if len(st.Forks) > 0 {
		phase = fmt.Sprintf("%s (%d/%d forks ready)", phase, ready, len(st.Forks))
	}

	fmt.Fprintf(w, "Sandbox %s - %s\n", sb.Name, phase)
	if sb.Spec.Description != "" {
		fmt.Fprintf(w, "  %s\n", sb.Spec.Description)
	}
	if key := sb.EffectiveRoutingKey(); key != "" {
		fmt.Fprintf(w, "Header:   %s\n", Header(baggageKey, key))
	} else {
		fmt.Fprintln(w, "Header:   (routing key not assigned yet)")
	}
	if sb.Spec.KeySource != "" {
		fmt.Fprintf(w, "Key from: %s\n", sb.Spec.KeySource)
	}
	if st.ExpiresAt != nil {
		fmt.Fprintf(w, "Expires:  %s (in %s)\n", st.ExpiresAt.Local().Format("2006-01-02 15:04"), until(now, st.ExpiresAt.Time))
	}

	if len(sb.Spec.Forks) > 0 {
		fmt.Fprintln(w, "Forks:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		// The controller reports forks in spec order.
		for i, f := range sb.Spec.Forks {
			var fs v1alpha1.ForkStatus
			seen := i < len(st.Forks)
			if seen {
				fs = st.Forks[i]
			}
			state := "pending"
			switch {
			case seen && fs.Ready:
				state = "ready"
			case seen && fs.NotReadySince != nil:
				state = fmt.Sprintf("NOT READY for %s", until(fs.NotReadySince.Time, now))
				if fs.Reason != "" {
					state += ": " + fs.Reason
				}
			case seen && fs.Reason != "":
				state = "not ready: " + fs.Reason
			}
			fmt.Fprintf(tw, "  %s/%s\t%s\t%s\n", f.ForkOf.Namespace, f.ForkOf.Name, images(f), state)
		}
		tw.Flush()
	}

	if len(sb.Spec.Local) > 0 {
		fmt.Fprintln(w, "Local:")
		for _, l := range sb.Spec.Local {
			ports := make([]string, 0, len(l.Mappings))
			for _, m := range l.Mappings {
				ports = append(ports, fmt.Sprint(m.Port))
			}
			fmt.Fprintf(w, "  %s/%s port %s -> laptop (tunnel Service %s/%s)\n",
				l.From.Namespace, l.From.Name, strings.Join(ports, ","), l.Tunnel.Namespace, l.Tunnel.Name)
		}
	}

	for _, c := range st.Conditions {
		if c.Status != "True" {
			fmt.Fprintf(w, "Problem:  %s: %s %s\n", c.Type, c.Reason, c.Message)
		}
	}
	if c := meta.FindStatusCondition(st.Conditions, v1alpha1.ConditionRoutesProgrammed); c != nil && c.Status == "True" {
		fmt.Fprintln(w, "Routes:   programmed")
	}
}

func images(f v1alpha1.Fork) string {
	if f.Customizations == nil || len(f.Customizations.Images) == 0 {
		return "(baseline image)"
	}
	var parts []string
	for _, i := range f.Customizations.Images {
		parts = append(parts, i.Container+"="+i.Image)
	}
	return strings.Join(parts, " ")
}

// List prints a table of sandboxes.
func List(w io.Writer, items []v1alpha1.Sandbox, now time.Time) {
	if len(items) == 0 {
		fmt.Fprintln(w, "No sandboxes.")
		return
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tKEY\tPHASE\tFORKS\tLOCAL\tEXPIRES\tOWNER")
	for _, sb := range items {
		ready := 0
		for _, f := range sb.Status.Forks {
			if f.Ready {
				ready++
			}
		}
		expires := "-"
		if sb.Status.ExpiresAt != nil {
			expires = "in " + until(now, sb.Status.ExpiresAt.Time)
		}
		phase := string(sb.Status.Phase)
		if phase == "" {
			phase = "-"
		}
		owner := sb.Labels[LabelUser]
		if owner == "" {
			owner = sb.Annotations[v1alpha1.AnnotationCreatedBy]
		}
		if owner == "" {
			owner = "-"
		}
		key := sb.EffectiveRoutingKey()
		if key == "" {
			key = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%d\t%s\t%s\n", sb.Name, key, phase, ready, len(sb.Spec.Forks), len(sb.Spec.Local), expires, owner)
	}
	tw.Flush()
}

// until formats the time from a to b, rounded, never negative.
func until(a, b time.Time) string {
	d := b.Sub(a)
	if d < 0 {
		d = 0
	}
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	case d >= time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
}
