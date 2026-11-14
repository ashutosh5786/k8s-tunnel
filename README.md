# k8s-tunnel

Run one service on your laptop as if it were inside the cluster.

- **Inbound:** each exposed local port gets a Service in the cluster that points at the tunnel server, which relays connections to the local service. Routing to that Service (for example by a routing-key header in Istio) is up to the mesh.
- **Outbound:** connections from the laptop are sent through the tunnel and dialled from inside the cluster.

One shared server serves all users and can run as several replicas. Works with any cluster reachable through your kubeconfig (EKS, AKS, GKE).

## Status

Working: the tunnel server, the client library, and a `tunnel connect` command that exposes local ports, forwards single addresses, and can make cluster Service names resolve locally.

Not built yet: saved profiles, background mode, and the terminal dashboard.

## Usage

```bash
tunnel connect --expose b-ash:8080:3000/http --forward 5432:postgres.db.svc:5432
```

That does two things until you press Ctrl-C:

- Creates the Service `b-ash` in the tunnel namespace. Anything in the cluster that connects to `b-ash.k8s-tunnel.svc:8080` reaches `localhost:3000` on your machine.
- Listens on `localhost:5432` and sends those connections to `postgres.db.svc:5432` from inside the cluster.

It reconnects by itself if the connection drops, and removes its Services when it exits.

### Cluster names on your machine

```bash
tunnel connect --hosts --home-namespace shop --expose b-ash:8080:3000/http
```

With `--hosts`, every cluster Service name works locally with its own port, so a service whose configuration holds addresses like `cart.shop.svc.cluster.local:8080` runs unchanged. It needs a terminal started as administrator (Windows) or `sudo` (Linux), because it edits the hosts file.

How it works:

- The client asks the tunnel server which Services exist, and refreshes that every 30 seconds.
- Each Service gets its own loopback address (`127.77.0.1` upwards). The client listens there on the Service's ports and sends connections through the tunnel.
- The hosts file gets one line per Service mapping `name.namespace`, `name.namespace.svc` and `name.namespace.svc.cluster.local` to that address. With `--home-namespace`, Services in that namespace also resolve by bare name.
- Every line it writes ends in `# k8s-tunnel`. Nothing else in the file is touched, and the lines are removed on exit.

If the client is killed before it can clean up, remove the leftover lines with:

```bash
tunnel cleanup
```

Limits: TCP only; names resolve only for Services (not individual pods); a new Service takes up to 30 seconds to appear; macOS is not supported yet because it needs each loopback address added to the interface first. A name of the form `name.namespace` can shadow a real internet host when the namespace is also a top-level domain, for example a Service `web` in namespace `dev` shadows `web.dev`. Use `--hosts-namespaces` to limit which namespaces are resolved.

### Flags

| Flag | Meaning |
|---|---|
| `--hosts` | Make cluster Service names resolve locally. Needs an elevated terminal. |
| `--hosts-namespaces a,b` | With `--hosts`, only resolve Services in these namespaces. Default: all. |
| `--home-namespace NS` | With `--hosts`, Services in this namespace also resolve by bare name. |
| `--cluster-domain` | The cluster's DNS suffix. Default `cluster.local`. |
| `--hosts-file` | Hosts file to manage. Defaults to the system one. |
| `--expose NAME:PORT[:LOCALPORT][/PROTOCOL]` | Expose a local port. Repeat it for more ports; specs with the same name become one multi-port Service. The protocol (`http`, `grpc`, ...) is set as the Service port's `appProtocol`, which the mesh needs for header-based routing. |
| `--forward LOCALPORT:HOST:PORT` | Reach a cluster address on a local port. Repeatable. Listens on loopback only. |
| `--context`, `--kubeconfig` | Which cluster to use. Defaults to your current kubectl context. |
| `--namespace` | Namespace the tunnel server runs in. Default `k8s-tunnel`. |
| `--selector` | Label selector for the server pods. Default `app.kubernetes.io/name=k8s-tunnel`. |
| `--port` | Server port inside the pod. Default 7000. |
| `--user` | Name recorded on your Services. Defaults to your OS user name. |
| `--session` | Session id. Reuse one to reclaim your Services after restarting the client. |
| `--server HOST:PORT` | Connect straight to a server, skipping the cluster. For local testing. |

## Try it without a cluster

Start the server with Service publishing turned off:

```bash
./bin/tunnel-server -publish=false -listen 127.0.0.1:7000
```

In another terminal, connect to it directly:

```bash
./bin/tunnel connect --server 127.0.0.1:7000 --expose b-ash:8080:3000 --forward 8081:example.com:80
```

The client prints the server port chosen for the exposure. Connecting to that port on localhost reaches `localhost:3000`, which is what a Service would do in a cluster.

## Deploy the server

Build and push the image. This builds for both x86-64 and arm64 (Graviton) nodes:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t REGISTRY/k8s-tunnel-server:0.1.0 --push .
```

Install the chart:

```bash
helm install k8s-tunnel deploy/chart/k8s-tunnel --namespace k8s-tunnel --create-namespace --set image.repository=REGISTRY/k8s-tunnel-server --set image.tag=0.1.0
```

See `deploy/chart/k8s-tunnel/values.yaml` for replicas, the port range, who may use the tunnel, and Istio ambient enrolment.

### Permissions

| Who | Access | Scope |
|---|---|---|
| The server | Full access to `services` | Its own namespace only |
| The server, for `--hosts` | Read-only access to `services` | Cluster-wide; turn off with `discovery.enabled=false` |
| People using the tunnel | `get`/`list` on `pods`, `get`/`create` on `pods/portforward` | The tunnel namespace only |

The read-only Service listing is the only cluster-wide permission, and people using the tunnel need none of their own. The chart creates a `<release>-user` Role for people; bind it with `users.subjects` or your own RoleBinding. Being able to port-forward to the server pods is what grants use of the tunnel.

## Layout

| Path | Purpose |
|---|---|
| `cmd/tunnel` | Client command, runs on the laptop |
| `cmd/tunnel-server` | In-cluster server, runs as a StatefulSet |
| `pkg/tunnel` | Public Go API for embedding the tunnel in other projects |
| `internal/protocol` | Wire protocol and stream multiplexing shared by client and server |
| `internal/kube` | Kubeconfig loading, server pod selection, port-forward transport |
| `internal/client/connect` | Keeps a session alive: connect, expose, forward, reconnect |
| `internal/client/egress` | Local listeners that send connections into the cluster, including one loopback address per Service |
| `internal/client/hosts` | Hosts-file entries for cluster Service names |
| `internal/client/daemon` | Background mode and profiles (not built yet) |
| `internal/server` | Shared tunnel server: sessions, cluster dials, listeners for exposed ports |
| `internal/server/kubepub` | Publishes each exposure as a Service that selects the owning replica; lists cluster Services for clients |
| `internal/tui` | Terminal dashboard (not built yet) |
| `deploy/chart/k8s-tunnel` | Helm chart for the server |

## Test

The tests need no cluster. They run the real server and client against each other on localhost and use a fake Kubernetes API for the Service logic.

```bash
go test ./...
```

Run them repeatedly to look for flaky behaviour:

```bash
go test -count=20 ./...
```

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
