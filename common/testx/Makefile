ONELINT ?= go run github.com/temporalio/onelint@v0.5.0
GOLANGCI_LINT_BASE_REV ?= main

.PHONY: test lint fmt tidy

test:
	go test ./...

# onelint uses the repository's .onelint.yaml.
lint:
	$(ONELINT) check --since=$(GOLANGCI_LINT_BASE_REV)

fmt:
	$(ONELINT) fix --only=format,gofix ./...

tidy:
	go mod tidy
