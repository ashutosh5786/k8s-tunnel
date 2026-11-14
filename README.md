# k8s-tunnel

Run one service on your laptop as if it were inside the cluster.

- **Inbound:** requests carrying a routing-key header are routed (Istio ambient) to the in-cluster tunnel server, which relays them to the local service.
- **Outbound:** the local service calls cluster FQDNs unmodified; the traffic is sent through the tunnel and dialled from inside the cluster.

Works with any cluster reachable through your kubeconfig (EKS, AKS, GKE).

## Layout

| Path | Purpose |
|---|---|
| `cmd/tunnel` | Client binary: CLI and TUI, runs on the laptop |
| `cmd/tunnel-server` | In-cluster server, runs as a scalable Deployment |
| `pkg/tunnel` | Public Go API for embedding the tunnel in other projects |
| `internal/protocol` | Wire protocol and stream multiplexing shared by client and server |
| `internal/kube` | Kubeconfig loading, port-forward transport, Service discovery |
| `internal/client/egress` | Loopback listeners that send local traffic into the cluster |
| `internal/client/hosts` | Hosts-file entries for cluster FQDNs |
| `internal/client/daemon` | Background mode, profiles, reconnect |
| `internal/server/ingress` | Receives routed requests and dispatches them by routing key |
| `internal/server/registry` | Tracks connected client tunnels |
| `internal/tui` | Terminal dashboard |
| `deploy` | Kubernetes and Istio manifests for the server |

## Build

Binaries are written to `bin/`. Both `tunnel` and `tunnel-server` are built.

### For the current machine

```bash
go build -o bin/ ./cmd/...
```

### For Linux on AWS Graviton (arm64)

The build is pure Go (`CGO_ENABLED=0`), so it cross-compiles from any OS and produces static binaries.

From PowerShell on Windows:

```powershell
$env:CGO_ENABLED="0"; $env:GOOS="linux"; $env:GOARCH="arm64"; go build -trimpath -ldflags="-s -w" -o bin/linux-arm64/ ./cmd/...; Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED
```

From bash (Linux, macOS, Git Bash):

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/linux-arm64/ ./cmd/...
```

Or with make:

```bash
make build-linux-arm64
```

For x86-64 Linux, use `GOARCH=amd64` or `make build-linux-amd64`.
