BINARY  = dbflow-agent
VERSION = 1.0.0
LDFLAGS = -ldflags="-s -w -X main.agentVersion=$(VERSION)"
DIST    = dist

.PHONY: all run tidy build-mac build-mac-arm build-win build-linux clean

## Run locally (dev)
run:
	go run .

## Download deps
tidy:
	go mod tidy

## Build all platforms
all: tidy build-mac build-mac-arm build-win build-linux
	@echo ""
	@echo "✓ Binaries in ./$(DIST)/"
	@ls -lh $(DIST)/

## macOS (Intel)
build-mac:
	@mkdir -p $(DIST)
	GOOS=darwin  GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-darwin-amd64   .

## macOS (Apple Silicon)
build-mac-arm:
	@mkdir -p $(DIST)
	GOOS=darwin  GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-darwin-arm64   .

## Windows (64-bit)
build-win:
	@mkdir -p $(DIST)
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-windows-amd64.exe .

## Linux (64-bit)
build-linux:
	@mkdir -p $(DIST)
	GOOS=linux   GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/$(BINARY)-linux-amd64    .

clean:
	rm -rf $(DIST)
