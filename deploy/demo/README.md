# Demo application

A two-service application for trying the tunnel: a web frontend that asks a `color-api` for a colour.

| In the cluster | Calls | Purpose |
|---|---|---|
| `frontend` | `color-api` (in the cluster, set to red) | The application as it normally runs |
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
docker run --rm -p 8081:8080 -e BG_COLOR=blue ashutosh5786/color-api:blue
```

The colour comes from `BG_COLOR`, not from the image tag; both tags are the same image and default to blue. Try `-e BG_COLOR=green` to make the point that it is your own version.

Leave it running. It is your "local service B", listening on `localhost:8081`.

## 3. Connect

In a terminal started as administrator, so that cluster names resolve on your machine:

```powershell
.\bin\ashctl.exe connect --context docker-desktop
```

It returns once connected and keeps running in the background. Skip this if you are already connected.

## 4. Look at the cluster's own version

- <http://frontend.default.svc.cluster.local:8080> shows **red**: frontend → color-api, all inside the cluster.
- <http://color-api.default.svc.cluster.local:8080/color> is the API itself.

A newly installed Service can take up to 30 seconds to become resolvable.

## 5. Put your laptop in the path

```powershell
.\bin\ashctl.exe expose color-api-dev:8080:8081/http
```

This exposes your local port 8081 in the cluster as the Service `color-api-dev`, which `frontend-preview` calls.

- <http://frontend-preview.default.svc.cluster.local:8080> shows **blue**: frontend-preview (cluster) → tunnel → the container on your laptop.
- <http://frontend.default.svc.cluster.local:8080> still shows **red**.

The request log of the container from step 2 shows a line for every call that came from the cluster.

Take it away again and the preview's colour call fails, because the cluster really was calling your laptop:

```powershell
.\bin\ashctl.exe unexpose color-api-dev
```

## Without an administrator terminal

Names will not resolve, so reach the two frontends through local ports instead:

```powershell
.\bin\ashctl.exe connect --context docker-desktop --forward 9000:frontend.default.svc:8080 --forward 9001:frontend-preview.default.svc:8080
```

Then use <http://localhost:9000> and <http://localhost:9001>. `ashctl expose` works the same.

## Clean up

```powershell
.\bin\ashctl.exe disconnect
```

```powershell
kubectl delete -f deploy\demo\demo.yaml
```
