# Demo application

A two-service application for trying the tunnel: a web frontend that asks a `color-api` for a colour.

| In the cluster | Calls | Purpose |
|---|---|---|
| `frontend` | `color-api` (the `red` image, in the cluster) | The application as it normally runs |
| `frontend-preview` | `color-api-dev` in the tunnel namespace, which is your laptop | Stands in for a routing-key rule in a service mesh |

The idea: the cluster keeps running red. You run the blue version on your laptop. The normal frontend stays red; the preview frontend shows blue, served from your machine.

## 1. Install the demo

The tunnel server must already be installed (`deploy\local\deploy.ps1`).

```powershell
kubectl apply -f deploy\demo\demo.yaml
```

```powershell
kubectl get pods -n default
```

Wait until `color-api`, `frontend` and `frontend-preview` are all `1/1 Running`. The images are built for arm64; on an amd64 machine Docker Desktop runs them under emulation, so the first start is slow. If a pod stays in `CrashLoopBackOff` with `exec format error` in its logs, emulation is not available on your nodes.

## 2. Run the blue version on your laptop

```powershell
docker run --rm -p 8081:8080 ashutosh5786/color-api:blue
```

Leave it running. It is your "local service B", listening on `localhost:8081`.

## 3. Connect

In another terminal:

```powershell
.\bin\ashctl.exe connect --context docker-desktop --expose color-api-dev:8080:8081/http --forward 9000:frontend.default.svc:8080 --forward 9001:frontend-preview.default.svc:8080
```

It returns once connected and keeps running in the background. This one command:

- exposes your local port 8081 in the cluster as the Service `color-api-dev`, which `frontend-preview` calls;
- makes the cluster's `frontend` reachable at `http://localhost:9000`;
- makes the cluster's `frontend-preview` reachable at `http://localhost:9001`.

## 4. Look

- <http://localhost:9000> shows **red**: frontend → color-api, all inside the cluster.
- <http://localhost:9001> shows **blue**: frontend-preview (cluster) → tunnel → the container on your laptop.

The request log of the container from step 2 shows a line for every call that came from the cluster.

Stop the blue container and reload <http://localhost:9001>: the colour call fails, because the cluster is really calling your laptop.

## 5. With cluster names (administrator terminal)

Run the same connect command from a terminal started as administrator. `ashctl` then also makes cluster Service names resolve on your machine, so these work in a browser with no `--forward` at all:

- <http://frontend.default.svc.cluster.local:8080>
- <http://frontend-preview.default.svc.cluster.local:8080>
- <http://color-api.default.svc.cluster.local:8080/color>

## Clean up

```powershell
.\bin\ashctl.exe disconnect
```

```powershell
kubectl delete -f deploy\demo\demo.yaml
```
