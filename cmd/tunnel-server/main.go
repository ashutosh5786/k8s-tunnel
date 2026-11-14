// Command tunnel-server runs inside the cluster. It terminates client
// tunnels, relays cluster traffic to exposed local ports, and dials cluster
// addresses on behalf of clients.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"k8s-tunnel/internal/server"
	"k8s-tunnel/internal/server/kubepub"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tunnel-server:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen    = flag.String("listen", ":7000", "address clients connect to, reached by port-forward")
		portRange = flag.String("port-range", "20000-29999", "ports used for exposed local services")
		grace     = flag.Duration("grace", 30*time.Second, "how long an exposure stays published after its session drops")
		publish   = flag.Bool("publish", true, "create a Kubernetes Service per exposure; turn off to run outside a cluster")
		podName   = flag.String("pod-name", os.Getenv("POD_NAME"), "this pod's name (env POD_NAME)")
		namespace = flag.String("namespace", os.Getenv("POD_NAMESPACE"), "this pod's namespace (env POD_NAMESPACE)")
		debug     = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	min, max, err := parseRange(*portRange)
	if err != nil {
		return err
	}
	cfg := server.Config{
		Replica: *podName,
		PortMin: min,
		PortMax: max,
		Grace:   *grace,
		Log:     log,
	}
	if *publish {
		if *podName == "" || *namespace == "" {
			return fmt.Errorf("-pod-name and -namespace (or POD_NAME and POD_NAMESPACE) are required when publishing")
		}
		rc, err := rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("in-cluster config: %w (use -publish=false outside a cluster)", err)
		}
		client, err := kubernetes.NewForConfig(rc)
		if err != nil {
			return err
		}
		cfg.Publisher = &kubepub.Publisher{Client: client, Namespace: *namespace, PodName: *podName}
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("tunnel-server listening", "addr", ln.Addr().String(), "replica", *podName, "ports", *portRange, "publish", *publish)
	err = server.New(cfg).Serve(ctx, ln)
	log.Info("tunnel-server stopped")
	return err
}

func parseRange(s string) (min, max int, err error) {
	lo, hi, ok := strings.Cut(s, "-")
	if ok {
		min, err = strconv.Atoi(lo)
		if err == nil {
			max, err = strconv.Atoi(hi)
		}
	}
	if !ok || err != nil || min < 1024 || max > 65535 || min > max {
		return 0, 0, fmt.Errorf("invalid -port-range %q: want MIN-MAX within 1024-65535", s)
	}
	return min, max, nil
}
