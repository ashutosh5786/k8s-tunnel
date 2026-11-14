# k8s-tunnel

Run one service on your laptop as if it were inside the cluster.

- **Inbound:** each exposed local port gets a Service in the cluster that points at the tunnel server, which relays connections to the local service. Routing to that Service (for example by a routing-key header in Istio) is up to the mesh.
- **Outbound:** connections from the laptop are sent through the tunnel and dialled from inside the cluster.

One shared server serves all users and can run as several replicas. Works with any cluster reachable through your kubeconfig (EKS, AKS, GKE).

## Status

Working: the tunnel server, the client library, and the `ashctl` client, which connects in the background, exposes local ports, forwards single addresses, and makes cluster Service names resolve locally.

Not built yet: saved profiles, several connections at once, adding an exposure to a running connection, and the terminal dashboard.

## Usage

```bash
ashctl connect --context dev
```

That connects to the cluster and returns. The connection keeps running in the background, independent of the terminal, and reconnects by itself if it drops.

| Command | What it does |
|---|---|
| `ashctl connect [--context NAME]` | Connect and keep running in the background |
| `ashctl status` | Show whether you are connected, and what is exposed and forwarded |
| `ashctl disconnect` | Stop the connection and remove its Services and hosts entries |
| `ashctl logs` | Show the background connection's output |
| `ashctl cleanup` | Remove hosts-file entries left behind by a crash |

One connection runs at a time. Everything the connection should do is given to `connect`:

```bash
ashctl connect --context dev --expose b-ash:8080:3000/http --forward 5432:postgres.db.svc:5432
```

- `--expose` creates the Service `b-ash` in the tunnel namespace. Anything in the cluster that connects to `b-ash.k8s-tunnel.svc:8080` reaches `localhost:3000` on your machine.
- `--forward` listens on `localhost:5432` and sends those connections to `postgres.db.svc:5432` from inside the cluster.

Add `--foreground` to stay attached to the terminal instead, until Ctrl-C.

### Cluster names on your machine

When `ashctl connect` is run from a terminal started as administrator (Windows) or with `sudo` (Linux), every cluster Service name works locally with its own port. A service whose configuration holds addresses like `cart.shop.svc.cluster.local:8080` then runs unchanged, and no `--forward` is needed.

This needs elevation because it edits the hosts file. Without elevation `ashctl` still connects, says that names will not resolve, and `--expose` and `--forward` work as usual. Use `--hosts` to make that an error instead, or `--no-hosts` to never touch the hosts file.

How it works:

- The client asks the tunnel server which Services exist, and refreshes that every 30 seconds.
- Each Service gets its own loopback address (`127.77.0.1` upwards). The client listens there on the Service's ports and sends connections through the tunnel.
- The hosts file gets one line per Service mapping `name.namespace`, `name.namespace.svc` and `name.namespace.svc.cluster.local` to that address. With `--home-namespace`, Services in that namespace also resolve by bare name.
- Every line it writes ends in `# k8s-tunnel`. Nothing else in the file is touched, and the lines are removed on disconnect.

If the background process is killed before it can clean up, remove the leftover lines with:

```bash
ashctl cleanup
```

Limits: TCP only; names resolve only for Services (not individual pods); a new Service takes up to 30 seconds to appear; macOS is not supported yet because it needs each loopback address added to the interface first. A name of the form `name.namespace` can shadow a real internet host when the namespace is also a top-level domain, for example a Service `web` in namespace `dev` shadows `web.dev`. Use `--hosts-namespaces` to limit which namespaces are resolved.

### Flags of `ashctl connect`

| Flag | Meaning |
|---|---|
| `--context`, `--kubeconfig` | Which cluster to use. Defaults to your current kubectl context. |
| `--expose NAME:PORT[:LOCALPORT][/PROTOCOL]` | Expose a local port. Repeat it for more ports; specs with the same name become one multi-port Service. The protocol (`http`, `grpc`, ...) is set as the Service port's `appProtocol`, which the mesh needs for header-based routing. |
| `--forward LOCALPORT:HOST:PORT` | Reach a cluster address on a local port. Repeatable. Listens on loopback only. |
| `--foreground` | Stay attached to the terminal instead of running in the background. |
| `--hosts` | Require cluster names to resolve locally; fail if the hosts file cannot be changed. |
| `--no-hosts` | Never touch the hosts file. |
| `--hosts-namespaces a,b` | Only resolve Services in these namespaces. Default: all. |
| `--home-namespace NS` | Services in this namespace also resolve by bare name. |
| `--cluster-domain` | The cluster's DNS suffix. Default `cluster.local`. |
| `--hosts-file` | Hosts file to manage. Defaults to the system one. |
| `--namespace` | Namespace the tunnel server runs in. Default `k8s-tunnel`. |
| `--selector` | Label selector for the server pods. Default `app.kubernetes.io/name=k8s-tunnel`. |
| `--port` | Server port inside the pod. Default 7000. |
| `--user` | Name recorded on your Services. Defaults to your OS user name. |
| `--session` | Session id. Reuse one to reclaim your Services after restarting the client. |
| `--server HOST:PORT` | Connect straight to a server, skipping the cluster. For local testing. |

The background connection keeps its state and log in `%AppData%\ashctl` on Windows and `~/.config/ashctl` on Linux.

## Try it without a cluster

Start the server with Service publishing turned off:

```bash
./bin/tunnel-server -publish=false -listen 127.0.0.1:7000
```

In another terminal, connect to it directly:

```bash
./bin/ashctl connect --foreground --no-hosts --server 127.0.0.1:7000 --expose b-ash:8080:3000 --forward 8081:example.com:80
```

The client prints the server port chosen for the exposure. Connecting to that port on localhost reaches `localhost:3000`, which is what a Service would do in a cluster.

## Local cluster and demo application

For a Docker-based local cluster (Docker Desktop's Kubernetes or kind), one script builds the server image, loads it into the nodes and installs the chart:

```powershell
powershell -ExecutionPolicy Bypass -File deploy\local\deploy.ps1
```

`deploy/demo` holds a small two-service application and a walkthrough for trying the tunnel with it.

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
| `cmd/ashctl` | Client command, runs on the laptop |
| `cmd/tunnel-server` | In-cluster server, runs as a StatefulSet |
| `pkg/tunnel` | Public Go API for embedding the tunnel in other projects |
| `internal/protocol` | Wire protocol and stream multiplexing shared by client and server |
| `internal/kube` | Kubeconfig loading, server pod selection, port-forward transport |
| `internal/client/connect` | Keeps a session alive: connect, expose, forward, reconnect |
| `internal/client/egress` | Local listeners that send connections into the cluster, including one loopback address per Service |
| `internal/client/hosts` | Hosts-file entries for cluster Service names |
| `internal/client/daemon` | Background mode: which process holds the connection, its log, and the request to stop it |
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

Binaries are written to `bin/`. Both `ashctl` and `tunnel-server` are built.

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
