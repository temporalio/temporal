GOLANGCI_LINT ?= golangci-lint
GOLANGCI_LINT_CONFIG ?= ../.github/.golangci.yml
GOLANGCI_LINT_BASE_REV ?= main

.PHONY: test lint tidy

test:
	go test ./...

lint:
	$(GOLANGCI_LINT) run --timeout 10m --new-from-rev=$(GOLANGCI_LINT_BASE_REV) --config=$(GOLANGCI_LINT_CONFIG) ./...
	go vet ./...

tidy:
	go mod tidy
