GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_CONFIG ?= ../../.github/.golangci.yml
GOLANGCI_LINT_BASE_REV ?= main

# Return the path to binary $(1) from mise tool $(2) of the repository's mise.toml, installing it
# if needed, like the Makefile of the repository does. The proto tools have the versions that it pins.
mise-tool = $(or \
	$(shell mise config get --file ../../mise.toml 'tools."$(2)"' >/dev/null && \
		mise exec go $(2) -c 'command -v $(1)'), \
	$(error Cannot resolve $(1); check ../../mise.toml and run mise install))

BUF = $(call mise-tool,buf,go:github.com/bufbuild/buf/cmd/buf)
PROTOC_GEN_GO = $(call mise-tool,protoc-gen-go,go:google.golang.org/protobuf/cmd/protoc-gen-go)
PROTOC_GEN_GO_GRPC = $(call mise-tool,protoc-gen-go-grpc,go:google.golang.org/grpc/cmd/protoc-gen-go-grpc)

.PHONY: test lint fmt tidy proto

test:
	go test -race -shuffle on -tags test_dep ./...
	go test -race -shuffle on ./...

lint:
	$(GOLANGCI_LINT) run --timeout 10m --new-from-rev=$(GOLANGCI_LINT_BASE_REV) --config=$(GOLANGCI_LINT_CONFIG) ./...
	go vet ./...
	go fix -diff ./...

fmt:
	go fix ./...
	$(GOLANGCI_LINT) fmt --config=$(GOLANGCI_LINT_CONFIG) ./...

tidy:
	go mod tidy

proto:
	$(BUF) lint proto
	PATH="$(dir $(PROTOC_GEN_GO)):$(dir $(PROTOC_GEN_GO_GRPC)):$$PATH" $(BUF) generate proto
