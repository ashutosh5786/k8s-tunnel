# Image for the in-cluster tunnel server.
#
# Multi-architecture build (amd64 and arm64/Graviton) with buildx:
#   docker buildx build --platform linux/amd64,linux/arm64 -t REGISTRY/k8s-tunnel-server:TAG --push .
#
# Single-architecture build for the machine you are on:
#   docker build -t k8s-tunnel-server:dev .

# The build stage runs on the build machine's own architecture and
# cross-compiles, which is much faster than emulating the target.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/tunnel-server ./cmd/tunnel-server

# Static binary on a minimal base: no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tunnel-server /tunnel-server
USER 65532:65532
EXPOSE 7000
ENTRYPOINT ["/tunnel-server"]
