testx_makefile_dir := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
testx_repo_root := $(abspath $(testx_makefile_dir)../..)
testx_golangci_lint := $(testx_repo_root)/.bin/golangci-lint

GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT ?= $(testx_golangci_lint)
GOLANGCI_LINT_CONFIG ?= $(abspath $(testx_makefile_dir)../../.github/.golangci.yml)
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

ifeq ($(GOLANGCI_LINT),$(testx_golangci_lint))
lint fmt: testx-golangci-lint
endif

.PHONY: testx-golangci-lint
testx-golangci-lint:
	@$(MAKE) --no-print-directory -C "$(testx_repo_root)" "$(testx_golangci_lint)" \
		'GOLANGCI_LINT_BOOTSTRAP=go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)'
