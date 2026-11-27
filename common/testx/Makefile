ONELINT ?= mise exec go:github.com/golangci/golangci-lint/v2/cmd/golangci-lint go:github.com/temporalio/onelint -- onelint
GOLANGCI_LINT_BASE_REV ?= main

.PHONY: test lint lint-fix fmt

test:
	go test ./...

# onelint uses this module's .onelint.yaml.
lint:
	$(ONELINT) check --new-from-rev=$(GOLANGCI_LINT_BASE_REV)

lint-fix:
	$(ONELINT) fix --new-from-rev=$(GOLANGCI_LINT_BASE_REV)

fmt:
	$(ONELINT) fmt
