.PHONY: all fmt vet test build clean

all: fmt vet test build

fmt:
gofmt -w $$(find . -name '*.go' -not -path './pb/*')

vet:
go vet ./...

test:
go test -count=1 ./...

build:
go build ./...
