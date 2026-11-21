DRAFTAPI := .bin/draftapi
$(DRAFTAPI): cmd/tools/draftapi/main.go $(wildcard tools/draftapi/*.go) go.mod go.sum
	@mkdir -p $(@D)
	go build -o $@ ./cmd/tools/draftapi

.PHONY: lint-draft-api
lint-draft-api: $(DRAFTAPI)
	@go vet -tags $(ALL_TEST_TAGS) -vettool="$(DRAFTAPI)" $(LINT_CODE_TARGETS)
