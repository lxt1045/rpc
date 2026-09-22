.PHONY: all fmt vet test build clean

all: fmt vet test build

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './pb/*')

vet:
	go vet ./...

test:
	go test -count=1 -timeout 300s ./...

build:
	go build ./...

clean:
	rm -rf ../../socks-faux-trunk-kcp-client ../../socks-faux-trunk-kcp-server
