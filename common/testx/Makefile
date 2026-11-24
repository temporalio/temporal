GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_CONFIG ?= ../../.github/.golangci.yml
GOLANGCI_LINT_BASE_REV ?= main
BUF_VERSION ?= v1.6.0
PROTOC_GEN_GO_VERSION ?= v1.36.6
PROTOC_GEN_GO_GRPC_VERSION ?= v1.3.0
BUF ?= go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
LOCALBIN := $(CURDIR)/.bin

.PHONY: test lint fmt tidy proto

test:
	go test -race -shuffle on -tags test_dep ./...
	go test -race -shuffle on ./...

lint:
	$(GOLANGCI_LINT) run --timeout 10m --new-from-rev=$(GOLANGCI_LINT_BASE_REV) --config=$(GOLANGCI_LINT_CONFIG) --build-tags=test_dep ./...
	go vet ./...
	go vet -tags test_dep ./...
	go fix -diff ./...

fmt:
	go fix ./...
	$(GOLANGCI_LINT) fmt --config=$(GOLANGCI_LINT_CONFIG) ./...

tidy:
	go mod tidy

proto:
	GOBIN=$(LOCALBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(LOCALBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	$(BUF) lint proto
	$(BUF) generate proto
