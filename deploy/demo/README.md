# Demo application

The walkthrough for this demo is in the main [README](../../README.md#demo).

This folder holds `demo.yaml`, which installs three things into the `default` namespace:

| Workload | Calls | Purpose |
|---|---|---|
| `color-api` | nothing | The API as it runs in the cluster, set to answer red |
| `frontend` | `color-api` | The application as it normally runs |
| `frontend-preview` | `color-api-dev` in the tunnel namespace, which leads to your machine | Stands in for a routing-key rule in a service mesh |

Notes:

- The colour an API instance answers comes from its `BG_COLOR` setting and defaults to blue. The `red` and `blue` image tags are the same image.
- Everything is in `default` because the frontend image calls `color-api.default.svc.cluster.local`.
- `frontend-preview` uses the same image as `frontend` with a different nginx configuration, mounted from a ConfigMap. It looks the tunnel's Service up on every request, so it starts and keeps running whether or not anyone is connected.
- The images are built for arm64. On amd64 nodes they need emulation, which Docker Desktop normally provides.
