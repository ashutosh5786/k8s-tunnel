# Builds the tunnel server and installs it into a local Docker-based
# cluster (Docker Desktop's Kubernetes or kind). Run from the repository
# root in PowerShell:
#
#   powershell -ExecutionPolicy Bypass -File deploy\local\deploy.ps1
#
# Needs go, docker and kubectl. Helm is run from a container, so it does
# not have to be installed.

$Image = "k8s-tunnel-server"
$Tag = "dev"
$Namespace = "k8s-tunnel"
$HelmImage = "alpine/helm:3.16.4"

# Stops the script when a program fails; PowerShell does not do that for
# external programs by itself.
function Check($what) {
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $what" -ForegroundColor Red
        exit 1
    }
}

$root = (Get-Location).Path
if (-not (Test-Path "$root\go.mod")) {
    Write-Host "Run this from the repository root (the folder with go.mod)." -ForegroundColor Red
    exit 1
}

$context = kubectl config current-context
Check "kubectl config current-context"
Write-Host "==> cluster: $context"

$arch = kubectl get nodes -o "jsonpath={.items[0].status.nodeInfo.architecture}"
Check "reading node architecture"
$nodes = (kubectl get nodes -o "jsonpath={.items[*].metadata.name}") -split " "
Check "listing nodes"

Write-Host "==> building tunnel-server for linux/$arch"
New-Item -ItemType Directory -Force "$root\dist\image" | Out-Null
$env:CGO_ENABLED = "0"; $env:GOOS = "linux"; $env:GOARCH = $arch
go build -trimpath -ldflags="-s -w" -o "$root\dist\image\tunnel-server" ./cmd/tunnel-server
$buildExit = $LASTEXITCODE
Remove-Item Env:CGO_ENABLED, Env:GOOS, Env:GOARCH
if ($buildExit -ne 0) { Write-Host "FAILED: go build" -ForegroundColor Red; exit 1 }

docker build -q -f "$root\Dockerfile.local" -t "${Image}:${Tag}" "$root\dist\image" | Out-Null
Check "docker build"

# Cluster nodes are containers with their own image store, so the image
# has to be copied into each of them.
Write-Host "==> loading image into cluster nodes"
docker save -o "$root\dist\image.tar" "${Image}:${Tag}"
Check "docker save"
foreach ($node in $nodes) {
    Write-Host "    $node"
    docker cp "$root\dist\image.tar" "${node}:/k8s-tunnel-image.tar" | Out-Null
    Check "copying image to $node"
    docker exec $node ctr -n k8s.io images import /k8s-tunnel-image.tar | Out-Null
    Check "importing image on $node"
    docker exec $node rm /k8s-tunnel-image.tar
}

Write-Host "==> rendering chart"
$manifest = docker run --rm -v "${root}\deploy\chart:/charts" $HelmImage template k8s-tunnel /charts/k8s-tunnel --namespace $Namespace --set "image.repository=$Image" --set "image.tag=$Tag" @args
Check "helm template"
# Written as plain ASCII: the default encoding here would be UTF-16,
# which kubectl cannot read.
$manifest | Out-File -Encoding ascii "$root\dist\k8s-tunnel.yaml"

Write-Host "==> installing into namespace $Namespace"
kubectl get namespace $Namespace 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    kubectl create namespace $Namespace
    Check "creating namespace"
}
kubectl apply -f "$root\dist\k8s-tunnel.yaml"
Check "kubectl apply"
# The image tag does not change between builds, so restart to pick up a
# rebuilt image.
kubectl -n $Namespace rollout restart statefulset/k8s-tunnel
Check "rollout restart"
kubectl -n $Namespace rollout status statefulset/k8s-tunnel --timeout=120s
Check "waiting for the server pod"

Write-Host ""
Write-Host "Installed. Connect with:" -ForegroundColor Green
Write-Host "  .\bin\ashctl.exe connect"
