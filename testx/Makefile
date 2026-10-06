GOLANGCI_LINT_VERSION ?= v2.13.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
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
