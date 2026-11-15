#!/usr/bin/env bash
# Builds the tunnel server and installs it into a local Docker-based
# cluster (Docker Desktop's Kubernetes or kind). Run from the repository
# root in a bash shell:
#
#   bash deploy/local/deploy.sh
#
# Needs go, docker and kubectl. Helm is run from a container, so it does
# not have to be installed.
set -euo pipefail

IMAGE=k8s-tunnel-server:dev
NAMESPACE=k8s-tunnel
HELM_IMAGE=alpine/helm:3.16.4

# Stop Git Bash on Windows from rewriting container paths like /charts.
export MSYS_NO_PATHCONV=1
root=$(pwd -W 2>/dev/null || pwd)

if [ ! -f go.mod ]; then
  echo "Run this from the repository root (the folder with go.mod)." >&2
  exit 1
fi

context=$(kubectl config current-context)
echo "==> cluster: $context"
arch=$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')

echo "==> building tunnel-server for linux/$arch"
mkdir -p dist/image
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -ldflags="-s -w" -o dist/image/tunnel-server ./cmd/tunnel-server
docker build -q -f Dockerfile.local -t "$IMAGE" dist/image

# Cluster nodes are containers with their own image store, so the image
# has to be copied into each of them.
echo "==> loading image into cluster nodes"
docker save -o dist/image.tar "$IMAGE"
for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  echo "    $node"
  docker cp dist/image.tar "$node:/k8s-tunnel-image.tar"
  docker exec "$node" ctr -n k8s.io images import /k8s-tunnel-image.tar >/dev/null
  docker exec "$node" rm /k8s-tunnel-image.tar
done

echo "==> rendering chart"
docker run --rm -v "$root/deploy/chart:/charts" "$HELM_IMAGE" template k8s-tunnel /charts/k8s-tunnel \
  --namespace "$NAMESPACE" --set image.repository="${IMAGE%%:*}" --set image.tag="${IMAGE##*:}" "$@" \
  > dist/k8s-tunnel.yaml

echo "==> installing into namespace $NAMESPACE"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f dist/k8s-tunnel.yaml
# The image tag does not change between builds, so restart to pick up a
# rebuilt image.
kubectl -n "$NAMESPACE" rollout restart statefulset/k8s-tunnel
kubectl -n "$NAMESPACE" rollout status statefulset/k8s-tunnel --timeout=120s

echo
echo "Installed. Connect with:"
echo "  sudo ./bin/ashctl connect --context $context --kubeconfig \"\$HOME/.kube/config\" --user \"\$USER\""
echo "(sudo makes cluster names resolve on this machine; on Windows use an administrator terminal instead)"
