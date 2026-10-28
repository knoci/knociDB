GO ?= go
PKG ?= ./...

.PHONY: all fmt vet test race cover lint bench clean

all: fmt vet test

fmt:
	$(GO) fmt $(PKG)

vet:
	$(GO) vet $(PKG)

test:
	$(GO) test -count=1 -timeout 15m $(PKG)

race:
	$(GO) test -race -count=1 -timeout 20m $(PKG)

cover:
	$(GO) test -count=1 -coverprofile=coverage.out -timeout 15m $(PKG)

lint:
	golangci-lint run --timeout=5m

bench:
	$(GO) test -run '^$' -bench . -benchmem ./benchmark

clean:
	rm -rf .gocache .gotmp coverage.out coverage.txt coverage.html
