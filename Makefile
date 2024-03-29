############################# Main targets #############################
# Install all tools and builds binaries.
install: update-tools bins

# Rebuild binaries (used by Dockerfile).
bins: temporal-server temporal-cassandra-tool temporal-sql-tool tdbg

# Install all tools, recompile proto files, run all possible checks and tests (long but comprehensive).
all: update-tools clean proto bins check test

# Used in CI
ci-build-misc: print-go-version ci-update-tools proto bins shell-check copyright-check go-generate gomodtidy ensure-no-changes

# Delete all build artifacts
clean: clean-bins clean-test-results

# Recompile proto files.
proto: clean-proto buf-lint api-linter protoc service-clients goimports-proto proto-mocks copyright-proto

# Update proto submodule from remote and recompile proto files.
update-proto: update-proto-submodule proto gomodtidy
########################################################################

.PHONY: proto proto-mocks protoc

##### Arguments ######

GOOS        ?= $(shell go env GOOS)
GOARCH      ?= $(shell go env GOARCH)
GOPATH      ?= $(shell go env GOPATH)
# Disable cgo by default.
CGO_ENABLED ?= 0

TEST_ARGS ?= -race
PERSISTENCE_TYPE ?= nosql
PERSISTENCE_DRIVER ?= cassandra

# Optional args to create multiple keyspaces:
# make install-schema TEMPORAL_DB=temporal2 VISIBILITY_DB=temporal_visibility2
TEMPORAL_DB ?= temporal
VISIBILITY_DB ?= temporal_visibility

# Always use "protolegacy" tag to allow disabling utf-8 validation on proto messages
# during proto library transition.
ALL_BUILD_TAGS := protolegacy,$(BUILD_TAG)
ALL_TEST_TAGS := $(ALL_BUILD_TAGS),$(TEST_TAG)
BUILD_TAG_FLAG := -tags $(ALL_BUILD_TAGS)
TEST_TAG_FLAG := -tags $(ALL_TEST_TAGS)


##### Variables ######

GOBIN := $(if $(shell go env GOBIN),$(shell go env GOBIN),$(GOPATH)/bin)
PATH := $(GOBIN):$(PATH)

MODULE_ROOT := $(lastword $(shell grep -e "^module " go.mod))
COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

define NEWLINE


endef

TEST_TIMEOUT := 30m


PROTO_ROOT := proto
PROTO_FILES = $(shell find ./$(PROTO_ROOT)/internal -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_IMPORTS = -I=$(PROTO_ROOT)/internal -I=$(PROTO_ROOT)/api -I=$(PROTO_ROOT)/dependencies
PROTO_OPTS = paths=source_relative:$(PROTO_OUT)
PROTO_OUT := api
PROTO_ENUMS := $(shell grep -R '^enum ' $(PROTO_ROOT) | cut -d ' ' -f2)
PROTO_PATHS = paths=source_relative:$(PROTO_OUT)

ALL_SRC         := $(shell find . -name "*.go")
ALL_SRC         += go.mod
ALL_SCRIPTS     := $(shell find . -name "*.sh")

MAIN_BRANCH    := main

TEST_DIRS       := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))
FUNCTIONAL_TEST_ROOT          := ./tests
FUNCTIONAL_TEST_XDC_ROOT      := ./tests/xdc
FUNCTIONAL_TEST_NDC_ROOT      := ./tests/ndc
DB_INTEGRATION_TEST_ROOT      := ./common/persistence/tests
DB_TOOL_INTEGRATION_TEST_ROOT := ./tools/tests
INTEGRATION_TEST_DIRS := $(DB_INTEGRATION_TEST_ROOT) $(DB_TOOL_INTEGRATION_TEST_ROOT) ./temporaltest ./internal/temporalite
UNIT_TEST_DIRS := $(filter-out $(FUNCTIONAL_TEST_ROOT)% $(FUNCTIONAL_TEST_XDC_ROOT)% $(FUNCTIONAL_TEST_NDC_ROOT)% $(DB_INTEGRATION_TEST_ROOT)% $(DB_TOOL_INTEGRATION_TEST_ROOT)% ./temporaltest% ./internal/temporalite%,$(TEST_DIRS))

# github.com/urfave/cli/v2@v2.4.0             - needs to accept comma in values before unlocking https://github.com/urfave/cli/pull/1241.
PINNED_DEPENDENCIES := \
	github.com/go-sql-driver/mysql@v1.5.0 \
	github.com/urfave/cli/v2@v2.4.0

# Code coverage & test report output files.
TEST_OUTPUT_ROOT        := ./.testoutput
NEW_COVER_PROFILE       = $(TEST_OUTPUT_ROOT)/$(shell xxd -p -l 16 /dev/urandom).cover.out   # generates a new filename each time it's substituted
SUMMARY_COVER_PROFILE  := $(TEST_OUTPUT_ROOT)/summary.cover.out
NEW_REPORT              = $(TEST_OUTPUT_ROOT)/$(shell xxd -p -l 16 /dev/urandom).junit.xml   # generates a new filename each time it's substituted

# DB
SQL_USER ?= temporal
SQL_PASSWORD ?= temporal

# Need the following option to have integration and functional tests count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
INTEGRATION_TEST_COVERPKG := -coverpkg="$(MODULE_ROOT)/common/persistence/..."
FUNCTIONAL_TEST_COVERPKG := -coverpkg="$(MODULE_ROOT)/client/...,$(MODULE_ROOT)/common/...,$(MODULE_ROOT)/service/...,$(MODULE_ROOT)/temporal/..."

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

update-goimports:
	@printf $(COLOR) "Install/update goimports..."
	@go install golang.org/x/tools/cmd/goimports@latest

update-linters:
	@printf $(COLOR) "Install/update linters..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.53.3

update-mockgen:
	@printf $(COLOR) "Install/update mockgen tool..."
	@go install github.com/golang/mock/mockgen@v1.7.0-rc.1

update-gotestsum:
	@printf $(COLOR) "Install/update gotestsum..."
	@go install gotest.tools/gotestsum@v1.11

update-proto-plugins:
	@printf $(COLOR) "Install/update proto plugins..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@go install -modfile build/go.mod go.temporal.io/api/cmd/protoc-gen-go-helpers
	@go install -modfile build/go.mod go.temporal.io/api/cmd/protogen

update-proto-linters:
	@printf $(COLOR) "Install/update proto linters..."
	@go install github.com/googleapis/api-linter/cmd/api-linter@v1.32.3
	@go install github.com/bufbuild/buf/cmd/buf@v1.6.0

update-tctl:
	@printf $(COLOR) "Install/update tctl..."
	@go install github.com/temporalio/tctl/cmd/tctl@latest

update-cli:
	@printf $(COLOR) "Install/update cli..."
	curl -sSf https://temporal.download/cli.sh | sh

update-ui:
	@printf $(COLOR) "Install/update temporal ui-server..."
	@go install github.com/temporalio/ui-server/cmd/server@latest

update-tools: update-goimports update-linters update-mockgen update-proto-plugins update-proto-linters update-gotestsum

# update-linters is not included because in CI linters are run by github actions.
ci-update-tools: update-goimports update-mockgen update-proto-plugins update-proto-linters update-gotestsum

##### Proto #####
$(PROTO_OUT):
	@mkdir -p $(PROTO_OUT)

# We depend on gomodtidy to ensure that go.mod is up to date before we delete the generated files
# in case protogen fails, and we need to rerun it.
clean-proto: gomodtidy
	@rm -rf $(PROTO_OUT)/*

update-proto-submodule:
	@printf $(COLOR) "Update proto submodule from remote..."
	git submodule update --force --remote $(PROTO_ROOT)/api

install-proto-submodule:
	@printf $(COLOR) "Install proto submodule..."
	git submodule update --init $(PROTO_ROOT)/api

protoc: clean-proto $(PROTO_OUT)
	@protogen \
		-I=proto/api \
		-I=proto/dependencies \
		--root=proto/internal \
		--rewrite-enum=BuildId_State:BuildId \
		-p go-grpc_out=$(PROTO_PATHS) \
		-p go-helpers_out=$(PROTO_PATHS)
	@mv -f "$(PROTO_OUT)/temporal/server/api/"* "$(PROTO_OUT)"

# All gRPC generated service files paths relative to PROTO_OUT.
PROTO_GRPC_SERVICES = $(patsubst $(PROTO_OUT)/%,%,$(shell find $(PROTO_OUT) -name "service.pb.go" -o -name "service_grpc.pb.go"))
service_name = $(firstword $(subst /, ,$(1)))
mock_file_name = $(call service_name,$(1))mock/$(subst $(call service_name,$(1))/,,$(1:go=mock.go))

proto-mocks: protoc
	@printf $(COLOR) "Generate proto mocks..."
	$(foreach PROTO_GRPC_SERVICE,$(PROTO_GRPC_SERVICES),\
		@cd $(PROTO_OUT) && \
		mockgen -copyright_file ../LICENSE -package $(call service_name,$(PROTO_GRPC_SERVICE))mock -source $(PROTO_GRPC_SERVICE) -destination $(call mock_file_name,$(PROTO_GRPC_SERVICE)) \
	$(NEWLINE))

service-clients: install-mocksync
	@printf $(COLOR) "Generate service clients..."
	@go generate ./client/...

update-go-api:
	@printf $(COLOR) "Update go.temporal.io/api@master..."
	@go get -u go.temporal.io/api@master

goimports-proto:
	@printf $(COLOR) "Run goimports for proto files..."
	@goimports -w $(PROTO_OUT)

copyright-proto:
	@printf $(COLOR) "Update license headers for proto files..."
	@go run ./cmd/tools/copyright/licensegen.go --scanDir $(PROTO_OUT)

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f temporal-server
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

install-mocksync:
	@printf $(COLOR) "Build mocksync..."
	@go install ./cmd/tools/mocksync

##### Checks #####
copyright-check:
	@printf $(COLOR) "Check license header..."
	@go run ./cmd/tools/copyright/licensegen.go --verifyOnly

copyright:
	@printf $(COLOR) "Fix license header..."
	@go run ./cmd/tools/copyright/licensegen.go

goimports: MERGE_BASE ?= $(shell test -d .git && git merge-base $(MAIN_BRANCH) HEAD)
goimports: MODIFIED_FILES := $(shell test -d .git && git diff --name-status $(MERGE_BASE) -- | cut -f2)
goimports:
	@printf $(COLOR) "Run goimports for modified files..."
	@printf "Merge base: $(MERGE_BASE)\n"
	@printf "Modified files: $(MODIFIED_FILES)\n"
	@goimports -w $(filter %.go, $(MODIFIED_FILES))

lint:
	@printf $(COLOR) "Run linters..."
	@golangci-lint run --verbose --timeout 10m --fix=true --new-from-rev=$(MAIN_BRANCH) --config=.golangci.yml

api-linter:
	@printf $(COLOR) "Run api-linter..."
	$(call silent_exec, api-linter --set-exit-status $(PROTO_IMPORTS) --config=$(PROTO_ROOT)/api-linter.yaml $(PROTO_FILES))

buf-lint:
	@printf $(COLOR) "Run buf linter..."
	@(cd $(PROTO_ROOT) && buf lint)

buf-build:
	@printf $(COLOR) "Build image.bin with buf..."
	@(cd $(PROTO_ROOT) && buf build -o image.bin)

buf-breaking:
	@printf $(COLOR) "Run buf breaking changes check against image.bin..."
	@(cd $(PROTO_ROOT) && buf check breaking --against image.bin)

shell-check:
	@printf $(COLOR) "Run shellcheck for script files..."
	@shellcheck $(ALL_SCRIPTS)

check: copyright-check lint shell-check

##### Tests #####
clean-test-results:
	@rm -f test.log $(TEST_OUTPUT_ROOT)/*
	@go clean -testcache

build-tests:
	@printf $(COLOR) "Build tests..."
	@go test $(TEST_TAG_FLAG) -exec="true" -count=0 $(TEST_DIRS)

unit-test: clean-test-results
	@printf $(COLOR) "Run unit tests..."
	@go test $(UNIT_TEST_DIRS) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(TEST_ARGS) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

integration-test: clean-test-results
	@printf $(COLOR) "Run integration tests..."
	@go test $(INTEGRATION_TEST_DIRS) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(TEST_ARGS) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

functional-test: clean-test-results
	@printf $(COLOR) "Run functional tests..."
	@go test $(FUNCTIONAL_TEST_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(TEST_ARGS) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@go test $(FUNCTIONAL_TEST_NDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(TEST_ARGS) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
# Need to run xdc tests with race detector off because of ringpop bug causing data race issue.
	@go test $(FUNCTIONAL_TEST_XDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

functional-with-fault-injection-test: clean-test-results
	@printf $(COLOR) "Run integration tests with fault injection..."
	@go test $(FUNCTIONAL_TEST_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(TEST_ARGS) -PersistenceFaultInjectionRate=0.005 -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@go test $(FUNCTIONAL_TEST_NDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(TEST_ARGS) -PersistenceFaultInjectionRate=0.005 -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
# Need to run xdc tests with race detector off because of ringpop bug causing data race issue.
	@go test $(FUNCTIONAL_TEST_XDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) -PersistenceFaultInjectionRate=0.005 -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) 2>&1 | tee -a test.log
	@! grep -q "^--- FAIL" test.log

test: unit-test integration-test functional-test functional-with-fault-injection-test

##### Coverage & Reporting #####
$(TEST_OUTPUT_ROOT):
	@mkdir -p $(TEST_OUTPUT_ROOT)

prepare-coverage-test: update-gotestsum $(TEST_OUTPUT_ROOT)

unit-test-coverage: prepare-coverage-test
	@printf $(COLOR) "Run unit tests with coverage..."
	@gotestsum --junitfile $(NEW_REPORT) -- \
		$(UNIT_TEST_DIRS) -timeout=$(TEST_TIMEOUT) -race $(TEST_TAG_FLAG) -coverprofile=$(NEW_COVER_PROFILE)

integration-test-coverage: prepare-coverage-test
	@printf $(COLOR) "Run integration tests with coverage..."
	@gotestsum --junitfile $(NEW_REPORT) -- \
		$(INTEGRATION_TEST_DIRS) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) $(INTEGRATION_TEST_COVERPKG) -coverprofile=$(NEW_COVER_PROFILE)

# This should use the same build flags as functional-test-coverage for best build caching.
pre-build-functional-test-coverage: prepare-coverage-test
	@go test -c -o /dev/null $(FUNCTIONAL_TEST_ROOT) -race $(TEST_TAG_FLAG) $(FUNCTIONAL_TEST_COVERPKG)

functional-test-coverage: prepare-coverage-test
	@printf $(COLOR) "Run functional tests with coverage with $(PERSISTENCE_DRIVER) driver..."
	@gotestsum --junitfile $(NEW_REPORT) -- \
		$(FUNCTIONAL_TEST_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_ARGS) $(TEST_TAG_FLAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(FUNCTIONAL_TEST_COVERPKG) -coverprofile=$(NEW_COVER_PROFILE)

functional-test-xdc-coverage: prepare-coverage-test
	@printf $(COLOR) "Run functional test for cross DC with coverage with $(PERSISTENCE_DRIVER) driver..."
	@gotestsum --junitfile $(NEW_REPORT) -- \
		$(FUNCTIONAL_TEST_XDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG_FLAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(FUNCTIONAL_TEST_COVERPKG) -coverprofile=$(NEW_COVER_PROFILE)

functional-test-ndc-coverage: prepare-coverage-test
	@printf $(COLOR) "Run functional test for NDC with coverage with $(PERSISTENCE_DRIVER) driver..."
	@gotestsum --junitfile $(NEW_REPORT) -- \
		$(FUNCTIONAL_TEST_NDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_ARGS) $(TEST_TAG_FLAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(FUNCTIONAL_TEST_COVERPKG) -coverprofile=$(NEW_COVER_PROFILE)

.PHONY: $(SUMMARY_COVER_PROFILE)
$(SUMMARY_COVER_PROFILE):
	@printf $(COLOR) "Combine coverage reports to $(SUMMARY_COVER_PROFILE)..."
	@rm -f $(SUMMARY_COVER_PROFILE) $(SUMMARY_COVER_PROFILE).html
	@if [ -z "$(wildcard $(TEST_OUTPUT_ROOT)/*.cover.out)" ]; then \
		echo "No coverage data, aborting!" && exit 1; \
	fi
	@echo "mode: atomic" > $(SUMMARY_COVER_PROFILE)
	$(foreach COVER_PROFILE,$(wildcard $(TEST_OUTPUT_ROOT)/*.cover.out),\
		@printf "Add %s...\n" $(COVER_PROFILE); \
		@grep -v -e "[Mm]ocks\?.go" -e "^mode: \w\+" $(COVER_PROFILE) >> $(SUMMARY_COVER_PROFILE) || true \
	$(NEWLINE))

coverage-report: $(SUMMARY_COVER_PROFILE)
	@printf $(COLOR) "Generate HTML report from $(SUMMARY_COVER_PROFILE) to $(SUMMARY_COVER_PROFILE).html..."
	@go tool cover -html=$(SUMMARY_COVER_PROFILE) -o $(SUMMARY_COVER_PROFILE).html

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

start-dependencies-cdc:
	docker compose $(DOCKER_COMPOSE_FILES) $(DOCKER_COMPOSE_CDC_FILES) up

stop-dependencies-cdc:
	docker compose $(DOCKER_COMPOSE_FILES) $(DOCKER_COMPOSE_CDC_FILES) down

start: start-sqlite

start-cass-es: temporal-server
	./temporal-server --env development-cass-es --allow-no-auth start

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
	@printf $(COLOR) "Update dependencies..."
	@go get -u -t $(PINNED_DEPENDENCIES) ./...
	@go mod tidy

go-generate: install-mocksync
	@printf $(COLOR) "Process go:generate directives..."
	@go generate ./...

ensure-no-changes:
	@printf $(COLOR) "Check for local changes..."
	@printf $(COLOR) "========================================================================"
	@git diff --name-status --exit-code || (printf $(COLOR) "========================================================================"; printf $(RED) "Above files are not regenerated properly. Regenerate them and try again."; exit 1)
