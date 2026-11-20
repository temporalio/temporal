testx_repo_root := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

GOLANGCI_LINT_CONFIG ?= $(testx_repo_root)/.github/.golangci.yml
GOLANGCI_LINT_BASE_REV ?= main

.PHONY: test lint fmt tidy

test:
	go test ./...

lint:
	$(GOLANGCI_LINT) run --timeout 10m --new-from-rev=$(GOLANGCI_LINT_BASE_REV) --config=$(GOLANGCI_LINT_CONFIG) ./...
	go vet ./...
	go fix -diff ./...

fmt:
	go fix ./...
	$(GOLANGCI_LINT) fmt --config=$(GOLANGCI_LINT_CONFIG) ./...

tidy:
	go mod tidy

ifndef GOLANGCI_LINT
GOLANGCI_LINT := $(testx_repo_root)/.bin/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.14.0

.PHONY: testx-golangci-lint
lint fmt: testx-golangci-lint
testx-golangci-lint:
	@$(MAKE) --no-print-directory -C "$(testx_repo_root)" "$(GOLANGCI_LINT)" \
		'GOLANGCI_LINT_BOOTSTRAP=go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)'
endif
