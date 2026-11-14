BIN := bin

.PHONY: build build-linux-arm64 build-linux-amd64 test vet clean

build:
	go build -o $(BIN)/ ./cmd/...

# Linux on AWS Graviton (arm64)
build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o $(BIN)/linux-arm64/ ./cmd/...

build-linux-amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o $(BIN)/linux-amd64/ ./cmd/...

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf $(BIN) dist
