############################# Main targets #############################
# Install all tools and builds binaries.
install: bins

# Rebuild binaries (used by Dockerfile).
bins: temporal-server temporal-cassandra-tool temporal-sql-tool tdbg

# Install all tools, recompile proto files, run all possible checks and tests (long but comprehensive).
all: clean proto bins check test

# Used in CI.
ci-build-misc: \
	print-go-version \
	clean-tools \
	proto \
	go-generate \
	buf-breaking \
	shell-check \
	goimports \
	gomodtidy \
	ensure-no-changes

# Delete all build artifacts
clean: clean-bins clean-tools clean-test-output

# Recompile proto files.
proto: lint-protos lint-api protoc proto-codegen
########################################################################

.PHONY: proto protoc install bins ci-build-misc clean

##### Arguments ######
GOOS        ?= $(shell go env GOOS)
GOARCH      ?= $(shell go env GOARCH)
GOPATH      ?= $(shell go env GOPATH)
# Disable cgo by default.
CGO_ENABLED ?= 0

PERSISTENCE_TYPE ?= nosql
PERSISTENCE_DRIVER ?= cassandra

# Optional args to create multiple keyspaces:
# make install-schema TEMPORAL_DB=temporal2 VISIBILITY_DB=temporal_visibility2
TEMPORAL_DB ?= temporal
VISIBILITY_DB ?= temporal_visibility

# The `disable_grpc_modules` build tag excludes gRPC dependencies from cloud.google.com/go/storage,
# reducing binary size by 16MB since we only use the REST client (storage.NewClient), not the
# gRPC client (storage.NewGRPCClient). Related issue: https://github.com/googleapis/google-cloud-go/issues/12343
ALL_BUILD_TAGS := disable_grpc_modules,$(BUILD_TAG)
ALL_TEST_TAGS := $(ALL_BUILD_TAGS),test_dep,$(TEST_TAG)
BUILD_TAG_FLAG := -tags $(ALL_BUILD_TAGS)
TEST_TAG_FLAG := -tags $(ALL_TEST_TAGS)

# 20 minutes is the upper bound defined for all tests. (Tests in CI take up to about 14:30 now)
# If you change this, also change .github/workflows/run-tests.yml!
# The timeout in the GH workflow must be larger than this to avoid GH timing out the action,
# which causes the a job run to not produce any logs and hurts the debugging experience.
TEST_TIMEOUT ?= 35m

# Number of retries for *-coverage targets.
MAX_TEST_ATTEMPTS ?= 3

# Whether or not to test with the race detector. All of (1 on y yes t true) are true values.
TEST_RACE_FLAG ?= on
# Whether or not to shuffle tests. All of (1 on y yes t true) are true values.
TEST_SHUFFLE_FLAG ?= on
# Common test args used in the various test suite targets.
COMPILED_TEST_ARGS := -timeout=$(TEST_TIMEOUT) \
		     $(if $(filter 1 on y yes t true, $(TEST_RACE_FLAG)),-race,) \
		     $(if $(filter 1 on y yes t true, $(TEST_SHUFFLE_FLAG)),-shuffle on,) \
		     $(TEST_PARALLEL_FLAGS) \
		     $(TEST_ARGS) \
		     $(TEST_TAG_FLAG)

##### Variables ######

ROOT := $(shell git rev-parse --show-toplevel)
LOCALBIN := .bin
STAMPDIR := .stamp
export PATH := $(ROOT)/$(LOCALBIN):$(PATH)
GOINSTALL := GOBIN=$(ROOT)/$(LOCALBIN) go install

OTEL ?= false
ifeq ($(OTEL),true)
	export OTEL_BSP_SCHEDULE_DELAY=100 # in ms
	export OTEL_EXPORTER_OTLP_TRACES_INSECURE=true
	export OTEL_TRACES_EXPORTER=otlp
	export TEMPORAL_OTEL_DEBUG=true
endif

MODULE_ROOT := $(lastword $(shell grep -e "^module " go.mod))
COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

define NEWLINE


endef

PROTO_ROOT := proto
PROTO_FILES = $(shell find ./$(PROTO_ROOT)/internal -name "*.proto")
CHASM_PROTO_FILES = $(shell find ./chasm/lib -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
API_BINPB := $(PROTO_ROOT)/api.binpb
# Note: If you change the value of INTERNAL_BINPB, you'll have to add logic to
# develop/buf-breaking.sh to handle the old and new values at once.
INTERNAL_BINPB := $(PROTO_ROOT)/image.bin
CHASM_BINPB := $(PROTO_ROOT)/chasm.bin
PROTO_OUT := api

ALL_SRC         := $(shell find . -name "*.go")
ALL_SRC         += go.mod
ALL_SCRIPTS     := $(shell find . -name "*.sh")

MAIN_BRANCH    := main

# If you update these dirs, please also update in CategoryDirs find_altered_tests.go
TEST_DIRS       := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))
FUNCTIONAL_TEST_ROOT          := ./tests
FUNCTIONAL_TEST_XDC_ROOT      := ./tests/xdc
FUNCTIONAL_TEST_NDC_ROOT      := ./tests/ndc
DB_INTEGRATION_TEST_ROOT      := ./common/persistence/tests
DB_TOOL_INTEGRATION_TEST_ROOT := ./tools/tests
INTEGRATION_TEST_DIRS := $(DB_INTEGRATION_TEST_ROOT) $(DB_TOOL_INTEGRATION_TEST_ROOT) ./temporaltest
ifeq ($(UNIT_TEST_DIRS),)
UNIT_TEST_DIRS := $(filter-out $(FUNCTIONAL_TEST_ROOT)% $(FUNCTIONAL_TEST_XDC_ROOT)% $(FUNCTIONAL_TEST_NDC_ROOT)% $(DB_INTEGRATION_TEST_ROOT)% $(DB_TOOL_INTEGRATION_TEST_ROOT)% ./temporaltest%,$(TEST_DIRS))
endif
SYSTEM_WORKFLOWS_ROOT := ./service/worker

# Pinning modernc.org/sqlite to this version until https://gitlab.com/cznic/sqlite/-/issues/196 is resolved.
PINNED_DEPENDENCIES := \
	modernc.org/sqlite@v1.34.1 \
	modernc.org/libc@v1.55.3 \

# Code coverage & test report output files.
TEST_OUTPUT_ROOT        := ./.testoutput
NEW_COVER_PROFILE       = $(TEST_OUTPUT_ROOT)/coverage.$(shell xxd -p -l 16 /dev/urandom).out   # generates a new filename each time it's substituted
NEW_REPORT              = $(TEST_OUTPUT_ROOT)/junit.$(shell xxd -p -l 16 /dev/urandom).xml   # generates a new filename each time it's substituted
COVERPKG_FLAG 		    = -coverpkg=$(shell go list ./... | paste -sd "," -)

# DB
SQL_USER ?= temporal
SQL_PASSWORD ?= temporal

# Only prints output if the exit code is non-zero
define silent_exec
    @output=$$($(1) 2>&1); \
    status=$$?; \
    if [ $$status -ne 0 ]; then \
        echo "$$output"; \
    fi; \
    exit $$status
endef

##### Tools #####
print-go-version:
	@go version

clean-tools:
	@printf $(COLOR) "Delete tools..."
	@rm -rf $(STAMPDIR)
	@rm -rf $(LOCALBIN)

$(STAMPDIR):
	@mkdir -p $(STAMPDIR)

$(LOCALBIN):
	@mkdir -p $(LOCALBIN)

# When updating the version, update the golangci-lint GHA workflow as well.
.PHONY: golangci-lint
GOLANGCI_LINT_BASE_REV ?= $(MAIN_BRANCH)
GOLANGCI_LINT_FIX ?= true
GOLANGCI_LINT_VERSION := v2.4.0
GOLANGCI_LINT := $(LOCALBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# Don't get confused, there is a single linter called gci, which is a part of the mega linter we use is called golangci-lint.
GCI_VERSION := v0.13.6
GCI := $(LOCALBIN)/gci-$(GCI_VERSION)
$(GCI): $(LOCALBIN)
	$(call go-install-tool,$(GCI),github.com/daixiang0/gci,$(GCI_VERSION))

GOTESTSUM_VER := v1.12.3
GOTESTSUM := $(LOCALBIN)/gotestsum-$(GOTESTSUM_VER)
$(GOTESTSUM): | $(LOCALBIN)
	$(call go-install-tool,$(GOTESTSUM),gotest.tools/gotestsum,$(GOTESTSUM_VER))

API_LINTER_VER := v1.32.3
API_LINTER := $(LOCALBIN)/api-linter-$(API_LINTER_VER)
$(API_LINTER): | $(LOCALBIN)
	$(call go-install-tool,$(API_LINTER),github.com/googleapis/api-linter/cmd/api-linter,$(API_LINTER_VER))

BUF_VER := v1.6.0
BUF := $(LOCALBIN)/buf-$(BUF_VER)
$(BUF): | $(LOCALBIN)
	$(call go-install-tool,$(BUF),github.com/bufbuild/buf/cmd/buf,$(BUF_VER))

GO_API_VER = $(shell go list -m -f '{{.Version}}' go.temporal.io/api \
	|| (echo "failed to fetch version for go.temporal.io/api" >&2))
PROTOGEN := $(LOCALBIN)/protogen-$(GO_API_VER)
$(PROTOGEN): | $(LOCALBIN)
	$(call go-install-tool,$(PROTOGEN),go.temporal.io/api/cmd/protogen,$(GO_API_VER))

ACTIONLINT_VER := v1.7.7
ACTIONLINT := $(LOCALBIN)/actionlint-$(ACTIONLINT_VER)
$(ACTIONLINT): | $(LOCALBIN)
	$(call go-install-tool,$(ACTIONLINT),github.com/rhysd/actionlint/cmd/actionlint,$(ACTIONLINT_VER))

WORKFLOWCHECK_VER := master # TODO: pin this specific version once 0.3.0 follow-up is released
WORKFLOWCHECK := $(LOCALBIN)/workflowcheck-$(WORKFLOWCHECK_VER)
$(WORKFLOWCHECK): | $(LOCALBIN)
	$(call go-install-tool,$(WORKFLOWCHECK),go.temporal.io/sdk/contrib/tools/workflowcheck,$(WORKFLOWCHECK_VER))

# The following tools need to have a consistent name, so we use a versioned stamp file to ensure the version we want is installed
# while installing to an unversioned binary name.
GOIMPORTS_VER := v0.36.0
GOIMPORTS := $(LOCALBIN)/goimports
$(STAMPDIR)/goimports-$(GOIMPORTS_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(GOIMPORTS),golang.org/x/tools/cmd/goimports,$(GOIMPORTS_VER))
	@touch $@
$(GOIMPORTS): $(STAMPDIR)/goimports-$(GOIMPORTS_VER)

GOWRAP_VER := v1.4.3
GOWRAP := $(LOCALBIN)/gowrap
$(STAMPDIR)/gowrap-$(GOWRAP_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(GOWRAP),github.com/hexdigest/gowrap/cmd/gowrap,$(GOWRAP_VER))
	@touch $@
$(GOWRAP): $(STAMPDIR)/gowrap-$(GOWRAP_VER)

GOMAJOR_VER := v0.14.0
GOMAJOR := $(LOCALBIN)/gomajor
$(STAMPDIR)/gomajor-$(GOMAJOR_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(GOMAJOR),github.com/icholy/gomajor,$(GOMAJOR_VER))
	@touch $@
$(GOMAJOR): $(STAMPDIR)/gomajor-$(GOMAJOR_VER)

# Mockgen is called by name throughout the codebase, so we need to keep the binary name consistent
MOCKGEN_VER := v0.6.0
MOCKGEN := $(LOCALBIN)/mockgen
$(STAMPDIR)/mockgen-$(MOCKGEN_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(MOCKGEN),go.uber.org/mock/mockgen,$(MOCKGEN_VER))
	@touch $@
$(MOCKGEN): $(STAMPDIR)/mockgen-$(MOCKGEN_VER)

STRINGER_VER := v0.36.0
STRINGER := $(LOCALBIN)/stringer
$(STAMPDIR)/stringer-$(STRINGER_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(STRINGER),golang.org/x/tools/cmd/stringer,$(STRINGER_VER))
	@touch $@
$(STRINGER): $(STAMPDIR)/stringer-$(STRINGER_VER)

PROTOC_GEN_GO_VER := v1.36.6
PROTOC_GEN_GO := $(LOCALBIN)/protoc-gen-go
$(STAMPDIR)/protoc-gen-go-$(PROTOC_GEN_GO_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(PROTOC_GEN_GO),google.golang.org/protobuf/cmd/protoc-gen-go,$(PROTOC_GEN_GO_VER))
	@touch $@
$(PROTOC_GEN_GO): $(STAMPDIR)/protoc-gen-go-$(PROTOC_GEN_GO_VER)

PROTOC_GEN_GO_GRPC_VER := v1.3.0
PROTOC_GEN_GO_GRPC := $(LOCALBIN)/protoc-gen-go-grpc
$(STAMPDIR)/protoc-gen-go-grpc-$(PROTOC_GEN_GO_GRPC_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(PROTOC_GEN_GO_GRPC),google.golang.org/grpc/cmd/protoc-gen-go-grpc,$(PROTOC_GEN_GO_GRPC_VER))
	@touch $@
$(PROTOC_GEN_GO_GRPC): $(STAMPDIR)/protoc-gen-go-grpc-$(PROTOC_GEN_GO_GRPC_VER)

PROTOC_GEN_GO_HELPERS := $(LOCALBIN)/protoc-gen-go-helpers
$(STAMPDIR)/protoc-gen-go-helpers-$(GO_API_VER): | $(STAMPDIR) $(LOCALBIN)
	$(call go-install-tool,$(PROTOC_GEN_GO_HELPERS),go.temporal.io/api/cmd/protoc-gen-go-helpers,$(GO_API_VER))
	@touch $@
$(PROTOC_GEN_GO_HELPERS): $(STAMPDIR)/protoc-gen-go-helpers-$(GO_API_VER)

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary (ideally with version)
# $2 - package url which can be installed
# $3 - specific version of package
# This is courtesy of https://github.com/kubernetes-sigs/kubebuilder/pull/3718
define go-install-tool
@[ -f $(1) ] || { \
set -e; \
package=$(2)@$(3) ;\
printf $(COLOR) "Downloading $${package}" ;\
tmpdir=$$(mktemp -d) ;\
GOBIN=$${tmpdir} go install $${package} ;\
mv $${tmpdir}/$$(basename "$$(echo "$(1)" | sed "s/-$(3)$$//")") $(1) ;\
rm -rf $${tmpdir} ;\
}
endef

##### Proto #####
$(API_BINPB): go.mod go.sum $(PROTO_FILES)
	@printf $(COLOR) "Generating proto dependencies image..."
	@./cmd/tools/getproto/run.sh --out $@

$(INTERNAL_BINPB): $(API_BINPB) $(PROTO_FILES)
	@printf $(COLOR) "Generate proto image..."
	@protoc --descriptor_set_in=$(API_BINPB) -I=$(PROTO_ROOT)/internal $(PROTO_FILES) -o $@

$(CHASM_BINPB): $(API_BINPB) $(INTERNAL_BINPB) $(CHASM_PROTO_FILES)
	@printf $(COLOR) "Generate CHASM proto image..."
	@protoc --descriptor_set_in=$(API_BINPB):$(INTERNAL_BINPB) -I=. $(CHASM_PROTO_FILES) -o $@

protoc: $(PROTOGEN) $(MOCKGEN) $(GOIMPORTS) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC) $(PROTOC_GEN_GO_HELPERS) $(API_BINPB)
	@go run ./cmd/tools/protogen \
		-root=$(ROOT) \
		-proto-out=$(PROTO_OUT) \
		-proto-root=$(PROTO_ROOT) \
		-protogen=$(PROTOGEN) \
		-api-binpb=$(API_BINPB) \
		-goimports=$(GOIMPORTS) \
		-mockgen=$(MOCKGEN) \
		$(PROTO_DIRS)

proto-codegen:
	@printf $(COLOR) "Generate service clients..."
	@go generate -run genrpcwrappers ./client/...
	@printf $(COLOR) "Generate server interceptors..."
	@go generate ./common/rpc/interceptor/logtags/...
	@printf $(COLOR) "Generate search attributes helpers..."
	@go generate -run gensearchattributehelpers ./common/searchattribute/...

update-go-api:
	@printf $(COLOR) "Update go.temporal.io/api@master..."
	@go get -u go.temporal.io/api@master

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f temporal-server
	@rm -f temporal-server-debug
	@rm -f temporal-cassandra-tool
	@rm -f tdbg
	@rm -f temporal-sql-tool

temporal-server: $(ALL_SRC)
	@printf $(COLOR) "Build temporal-server with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o temporal-server ./cmd/server

tdbg: $(ALL_SRC)
	@printf $(COLOR) "Build tdbg with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o tdbg ./cmd/tools/tdbg

temporal-cassandra-tool: $(ALL_SRC)
	@printf $(COLOR) "Build temporal-cassandra-tool with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o temporal-cassandra-tool ./cmd/tools/cassandra

temporal-sql-tool: $(ALL_SRC)
	@printf $(COLOR) "Build temporal-sql-tool with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o temporal-sql-tool ./cmd/tools/sql

temporal-server-debug: $(ALL_SRC)
	@printf $(COLOR) "Build temporal-server-debug with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG),TEMPORAL_DEBUG -o temporal-server-debug ./cmd/server

##### Checks #####
goimports: fmt-imports $(GOIMPORTS)
	@printf $(COLOR) "Run goimports for all files..."
	@UNGENERATED_FILES=$$(find . -type f -name '*.go' -print0 | xargs -0 grep -L -e "Code generated by .* DO NOT EDIT." || true) && \
		$(GOIMPORTS) -w $$UNGENERATED_FILES

lint-actions: $(ACTIONLINT)
	@printf $(COLOR) "Linting GitHub actions..."
	@$(ACTIONLINT)

lint-code: $(GOLANGCI_LINT)
	@printf $(COLOR) "Linting code..."
	@$(GOLANGCI_LINT) run --verbose --build-tags $(ALL_TEST_TAGS) --timeout 10m --fix=$(GOLANGCI_LINT_FIX) --new-from-rev=$(GOLANGCI_LINT_BASE_REV) --config=.github/.golangci.yml

fmt-imports: $(GCI) # Don't get confused, there is a single linter called gci, which is a part of the mega linter we use is called golangci-lint.
	@printf $(COLOR) "Formatting imports..."
	@$(GCI) write --skip-generated -s standard -s default ./*

lint: lint-code lint-actions lint-api lint-protos
	@printf $(COLOR) "Run linters..."

lint-api: $(API_LINTER) $(API_BINPB)
	@printf $(COLOR) "Linting proto API..."
	$(call silent_exec, $(API_LINTER) --set-exit-status -I=$(PROTO_ROOT)/internal --descriptor-set-in $(API_BINPB) --config=$(PROTO_ROOT)/api-linter.yaml $(PROTO_FILES))

lint-protos: $(BUF) $(INTERNAL_BINPB) $(CHASM_BINPB)
	@printf $(COLOR) "Linting proto definitions..."
	@$(BUF) lint $(INTERNAL_BINPB)
	@$(BUF) lint --config chasm/lib/buf.yaml $(CHASM_BINPB)

# Edit proto/internal/buf.yaml to exclude specific files from this check.
# TODO: buf breaking check for CHASM protos.
buf-breaking: $(BUF) $(API_BINPB) $(INTERNAL_BINPB)
	@printf $(COLOR) "Run buf breaking proto changes check..."
	@env BUF=$(BUF) API_BINPB=$(API_BINPB) INTERNAL_BINPB=$(INTERNAL_BINPB) CHASM_BINPB=$(CHASM_BINPB) MAIN_BRANCH=$(MAIN_BRANCH) \
		./develop/buf-breaking.sh

shell-check:
	@printf $(COLOR) "Run shellcheck for script files..."
	@shellcheck $(ALL_SCRIPTS)

workflowcheck: $(WORKFLOWCHECK)
	@printf $(COLOR) "Run workflowcheck for system workflows..."
	for dir in $(SYSTEM_WORKFLOWS_ROOT)/*/ ; do \
		echo "Running workflowcheck on $$dir" ; \
		$(WORKFLOWCHECK) "$$dir" ; \
	done

check: lint shell-check

##### Tests #####
clean-test-output:
	@printf $(COLOR) "Delete test output..."
	@rm -rf $(TEST_OUTPUT_ROOT)
	@go clean -testcache

build-tests:
	@printf $(COLOR) "Build tests..."
	@CGO_ENABLED=$(CGO_ENABLED) go test $(TEST_TAG_FLAG) -exec="true" -count=0 $(TEST_DIRS)

unit-test: clean-test-output
	@printf $(COLOR) "Run unit tests..."
	@CGO_ENABLED=$(CGO_ENABLED) go test $(UNIT_TEST_DIRS) $(COMPILED_TEST_ARGS) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

integration-test: clean-test-output
	@printf $(COLOR) "Run integration tests..."
	@CGO_ENABLED=$(CGO_ENABLED) go test $(INTEGRATION_TEST_DIRS) $(COMPILED_TEST_ARGS) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

functional-test: clean-test-output
	@printf $(COLOR) "Run functional tests..."
	@CGO_ENABLED=$(CGO_ENABLED) go test $(FUNCTIONAL_TEST_ROOT) $(COMPILED_TEST_ARGS) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@CGO_ENABLED=$(CGO_ENABLED) go test $(FUNCTIONAL_TEST_NDC_ROOT) $(COMPILED_TEST_ARGS) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@CGO_ENABLED=$(CGO_ENABLED) go test $(FUNCTIONAL_TEST_XDC_ROOT) $(COMPILED_TEST_ARGS) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

functional-with-fault-injection-test: clean-test-output
	@printf $(COLOR) "Run integration tests with fault injection..."
	@CGO_ENABLED=$(CGO_ENABLED) go test $(FUNCTIONAL_TEST_ROOT) $(COMPILED_TEST_ARGS) -enableFaultInjection=true -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@CGO_ENABLED=$(CGO_ENABLED) go test $(FUNCTIONAL_TEST_NDC_ROOT) $(COMPILED_TEST_ARGS) -enableFaultInjection=true -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@CGO_ENABLED=$(CGO_ENABLED) go test $(FUNCTIONAL_TEST_XDC_ROOT) $(COMPILED_TEST_ARGS) -enableFaultInjection=true -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

test: unit-test integration-test functional-test

##### Coverage & Reporting #####
$(TEST_OUTPUT_ROOT):
	@mkdir -p $(TEST_OUTPUT_ROOT)

prepare-coverage-test: $(GOTESTSUM) $(TEST_OUTPUT_ROOT)

unit-test-coverage: prepare-coverage-test
	@printf $(COLOR) "Run unit tests with coverage..."
	go run ./cmd/tools/test-runner test --gotestsum-path=$(GOTESTSUM) --max-attempts=$(MAX_TEST_ATTEMPTS) --junitfile=$(NEW_REPORT) -- \
		$(COMPILED_TEST_ARGS) -coverprofile=$(NEW_COVER_PROFILE) $(UNIT_TEST_DIRS)

integration-test-coverage: prepare-coverage-test
	@printf $(COLOR) "Run integration tests with coverage..."
	go run ./cmd/tools/test-runner test --gotestsum-path=$(GOTESTSUM) --max-attempts=$(MAX_TEST_ATTEMPTS) --junitfile=$(NEW_REPORT) -- \
		$(COMPILED_TEST_ARGS) -coverprofile=$(NEW_COVER_PROFILE) $(INTEGRATION_TEST_DIRS)

# This should use the same build flags as functional-test-coverage and functional-test-{xdc,ndc}-coverage for best build caching.
pre-build-functional-test-coverage: prepare-coverage-test
	go test -c -cover -o /dev/null $(FUNCTIONAL_TEST_ROOT) $(TEST_ARGS) $(TEST_TAG_FLAG) $(COVERPKG_FLAG)

functional-test-coverage: prepare-coverage-test
	@printf $(COLOR) "Run functional tests with coverage with $(PERSISTENCE_DRIVER) driver..."
	go run ./cmd/tools/test-runner test --gotestsum-path=$(GOTESTSUM) --max-attempts=$(MAX_TEST_ATTEMPTS) --junitfile=$(NEW_REPORT) -- \
		$(COMPILED_TEST_ARGS) -coverprofile=$(NEW_COVER_PROFILE) $(COVERPKG_FLAG) $(FUNCTIONAL_TEST_ROOT) \
		-args -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER)

functional-test-xdc-coverage: prepare-coverage-test
	@printf $(COLOR) "Run functional test for cross DC with coverage with $(PERSISTENCE_DRIVER) driver..."
	go run ./cmd/tools/test-runner test --gotestsum-path=$(GOTESTSUM) --max-attempts=$(MAX_TEST_ATTEMPTS) --junitfile=$(NEW_REPORT) -- \
		$(COMPILED_TEST_ARGS) -coverprofile=$(NEW_COVER_PROFILE) $(COVERPKG_FLAG) $(FUNCTIONAL_TEST_XDC_ROOT) \
		-args -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER)

functional-test-ndc-coverage: prepare-coverage-test
	@printf $(COLOR) "Run functional test for NDC with coverage with $(PERSISTENCE_DRIVER) driver..."
	go run ./cmd/tools/test-runner test --gotestsum-path=$(GOTESTSUM) --max-attempts=$(MAX_TEST_ATTEMPTS) --junitfile=$(NEW_REPORT) -- \
		$(COMPILED_TEST_ARGS) -coverprofile=$(NEW_COVER_PROFILE) $(COVERPKG_FLAG) $(FUNCTIONAL_TEST_NDC_ROOT) \
		-args -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER)

report-test-crash: $(TEST_OUTPUT_ROOT)
	@printf $(COLOR) "Generate test crash junit report..."
	@go run ./cmd/tools/test-runner report-crash --gotestsum=report-crash \
		--junitfile=$(TEST_OUTPUT_ROOT)/junit.crash.xml \
		--crashreportname=$(CRASH_REPORT_NAME)

##### Schema #####
install-schema-cass-es: temporal-cassandra-tool install-schema-es
	@printf $(COLOR) "Install Cassandra schema..."
	./temporal-cassandra-tool drop -k $(TEMPORAL_DB) -f
	./temporal-cassandra-tool create -k $(TEMPORAL_DB) --rf 1
	./temporal-cassandra-tool -k $(TEMPORAL_DB) setup-schema -v 0.0
	./temporal-cassandra-tool -k $(TEMPORAL_DB) update-schema -d ./schema/cassandra/temporal/versioned

install-schema-mysql: install-schema-mysql8

install-schema-mysql8: temporal-sql-tool
	@printf $(COLOR) "Install MySQL schema..."
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(TEMPORAL_DB) drop -f
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(TEMPORAL_DB) create
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(TEMPORAL_DB) setup-schema -v 0.0
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(TEMPORAL_DB) update-schema -d ./schema/mysql/v8/temporal/versioned
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(VISIBILITY_DB) drop  -f
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(VISIBILITY_DB) create
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(VISIBILITY_DB) setup-schema -v 0.0
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) --pl mysql8 --db $(VISIBILITY_DB) update-schema -d ./schema/mysql/v8/visibility/versioned

install-schema-postgresql: install-schema-postgresql12

install-schema-postgresql12: temporal-sql-tool
	@printf $(COLOR) "Install Postgres schema..."
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(TEMPORAL_DB) drop -f
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(TEMPORAL_DB) create
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(TEMPORAL_DB) setup -v 0.0
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(TEMPORAL_DB) update-schema -d ./schema/postgresql/v12/temporal/versioned
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(VISIBILITY_DB) drop -f
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(VISIBILITY_DB) create
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(VISIBILITY_DB) setup-schema -v 0.0
	./temporal-sql-tool -u $(SQL_USER) --pw $(SQL_PASSWORD) -p 5432 --pl postgres12 --db $(VISIBILITY_DB) update-schema -d ./schema/postgresql/v12/visibility/versioned

install-schema-es:
	@printf $(COLOR) "Install Elasticsearch schema..."
	curl --fail -X PUT "http://127.0.0.1:9200/_cluster/settings" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/cluster_settings_v7.json --write-out "\n"
	curl --fail -X PUT "http://127.0.0.1:9200/_template/temporal_visibility_v1_template" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/index_template_v7.json --write-out "\n"
# No --fail here because create index is not idempotent operation.
	curl -X PUT "http://127.0.0.1:9200/temporal_visibility_v1_dev" --write-out "\n"
# curl -X PUT "http://127.0.0.1:9200/temporal_visibility_v1_secondary" --write-out "\n"

install-schema-es-secondary:
	@printf $(COLOR) "Install Elasticsearch schema..."
	curl --fail -X PUT "http://127.0.0.1:8200/_cluster/settings" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/cluster_settings_v7.json --write-out "\n"
	curl --fail -X PUT "http://127.0.0.1:8200/_template/temporal_visibility_v1_template" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/index_template_v7.json --write-out "\n"
# No --fail here because create index is not idempotent operation.
	curl -X PUT "http://127.0.0.1:8200/temporal_visibility_v1_secondary" --write-out "\n"

install-schema-xdc: temporal-cassandra-tool
	@printf $(COLOR)  "Install Cassandra schema (active)..."
	./temporal-cassandra-tool drop -k temporal_cluster_a -f
	./temporal-cassandra-tool create -k temporal_cluster_a --rf 1
	./temporal-cassandra-tool -k temporal_cluster_a setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_cluster_a update-schema -d ./schema/cassandra/temporal/versioned

	@printf $(COLOR)  "Install Cassandra schema (standby)..."
	./temporal-cassandra-tool drop -k temporal_cluster_b -f
	./temporal-cassandra-tool create -k temporal_cluster_b --rf 1
	./temporal-cassandra-tool -k temporal_cluster_b setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_cluster_b update-schema -d ./schema/cassandra/temporal/versioned

	@printf $(COLOR)  "Install Cassandra schema (other)..."
	./temporal-cassandra-tool drop -k temporal_cluster_c -f
	./temporal-cassandra-tool create -k temporal_cluster_c --rf 1
	./temporal-cassandra-tool -k temporal_cluster_c setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_cluster_c update-schema -d ./schema/cassandra/temporal/versioned

	@printf $(COLOR) "Install Elasticsearch schemas..."
	curl --fail -X PUT "http://127.0.0.1:9200/_cluster/settings" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/cluster_settings_v7.json --write-out "\n"
	curl --fail -X PUT "http://127.0.0.1:9200/_template/temporal_visibility_v1_template" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/index_template_v7.json --write-out "\n"
# No --fail here because create index is not idempotent operation.
	curl -X DELETE http://localhost:9200/temporal_visibility_v1_dev_cluster_a
	curl -X DELETE http://localhost:9200/temporal_visibility_v1_dev_cluster_b
	curl -X DELETE http://localhost:9200/temporal_visibility_v1_dev_cluster_c
	curl -X PUT "http://127.0.0.1:9200/temporal_visibility_v1_dev_cluster_a" --write-out "\n"
	curl -X PUT "http://127.0.0.1:9200/temporal_visibility_v1_dev_cluster_b" --write-out "\n"
	curl -X PUT "http://127.0.0.1:9200/temporal_visibility_v1_dev_cluster_c" --write-out "\n"

##### Run server #####
DOCKER_COMPOSE_FILES     := -f ./develop/docker-compose/docker-compose.yml -f ./develop/docker-compose/docker-compose.$(GOOS).yml
DOCKER_COMPOSE_CDC_FILES := -f ./develop/docker-compose/docker-compose.cdc.yml -f ./develop/docker-compose/docker-compose.cdc.$(GOOS).yml
start-dependencies:
	docker compose $(DOCKER_COMPOSE_FILES) up

stop-dependencies:
	docker compose $(DOCKER_COMPOSE_FILES) down

start-dependencies-dual:
	docker compose $(DOCKER_COMPOSE_FILES) -f ./develop/docker-compose/docker-compose.secondary-es.yml up

stop-dependencies-dual:
	docker compose $(DOCKER_COMPOSE_FILES) -f ./develop/docker-compose/docker-compose.secondary-es.yml down

start-dependencies-cdc:
	docker compose $(DOCKER_COMPOSE_FILES) $(DOCKER_COMPOSE_CDC_FILES) up

stop-dependencies-cdc:
	docker compose $(DOCKER_COMPOSE_FILES) $(DOCKER_COMPOSE_CDC_FILES) down

start: start-sqlite

start-cass-es: temporal-server
	./temporal-server --env development-cass-es --allow-no-auth start

start-cass-es-dual: temporal-server
	./temporal-server --env development-cass-es-dual --allow-no-auth start

start-cass-es-custom: temporal-server
	./temporal-server --env development-cass-es-custom --allow-no-auth start

start-es-fi: temporal-server
	./temporal-server --env development-cass-es-fi --allow-no-auth start

start-mysql: start-mysql8

start-mysql8: temporal-server
	./temporal-server --env development-mysql8 --allow-no-auth start

start-mysql-es: temporal-server
	./temporal-server --env development-mysql-es --allow-no-auth start

start-postgres: start-postgres12

start-postgres12: temporal-server
	./temporal-server --env development-postgres12 --allow-no-auth start

start-sqlite: temporal-server
	./temporal-server --env development-sqlite --allow-no-auth start

start-sqlite-file: temporal-server
	./temporal-server --env development-sqlite-file --allow-no-auth start

start-xdc-cluster-a: temporal-server
	./temporal-server --env development-cluster-a --allow-no-auth start

start-xdc-cluster-b: temporal-server
	./temporal-server --env development-cluster-b --allow-no-auth start

start-xdc-cluster-c: temporal-server
	./temporal-server --env development-cluster-c --allow-no-auth start

##### Grafana #####
update-dashboards:
	@printf $(COLOR) "Update dashboards submodule from remote..."
	git submodule update --force --init --remote develop/docker-compose/grafana/provisioning/temporalio-dashboards

##### Auxiliary #####
gomodtidy:
	@printf $(COLOR) "go mod tidy..."
	@go mod tidy

update-dependencies:
	@printf $(COLOR) "Update dependencies (minor versions only) ..."
	@go get -u -t $(PINNED_DEPENDENCIES) ./...
	@go mod tidy

update-dependencies-major: $(GOMAJOR)
	@printf $(COLOR) "Major version upgrades available:"
	@$(GOMAJOR) list -major
	@echo ""
	@printf $(COLOR) "Update dependencies (major versions only) ..."
	@$(GOMAJOR) get -major all
	@go mod tidy

go-generate: $(MOCKGEN) $(GOIMPORTS) $(STRINGER) $(GOWRAP)
	@printf $(COLOR) "Process go:generate directives..."
	@go generate ./...

ensure-no-changes:
	@printf $(COLOR) "Check for local changes..."
	@printf $(COLOR) "========================================================================"
	@git status --porcelain
	@test -z "`git status --porcelain`" || (printf $(COLOR) "========================================================================"; printf $(RED) "Above files are not regenerated properly. Regenerate them and try again."; exit 1)
