############################# Main targets #############################
# Install all tools and builds binaries.
install: update-tools bins

# Rebuild binaries.
bins: clean-bins temporal-server tctl temporal-cassandra-tool temporal-sql-tool

# Install all tools, recompile proto files, run all possible checks and tests (long but comprehensive).
all: update-tools clean proto bins check test

# Delete all build artefacts.
clean: clean-bins clean-test-results

# Recompile proto files.
proto: clean-proto install-proto-submodule buf-lint api-linter protoc fix-proto-path proto-mock goimports-proto copyright-proto

# Update proto submodule from remote and recompile proto files.
update-proto: clean-proto update-proto-submodule buf-lint api-linter protoc fix-proto-path update-go-api proto-mock goimports-proto copyright-proto gomodtidy

# Build all docker images.
docker-images:
	docker build --build-arg "TARGET=server" -t temporalio/server:$(DOCKER_IMAGE_TAG) .
	docker build --build-arg "TARGET=tctl" -t temporalio/tctl:$(DOCKER_IMAGE_TAG) .
	docker build --build-arg "TARGET=auto-setup" -t temporalio/auto-setup:$(DOCKER_IMAGE_TAG) .
	docker build --build-arg "TARGET=admin-tools" -t temporalio/admin-tools:$(DOCKER_IMAGE_TAG) .
########################################################################

.PHONY: proto

##### Variables ######
ifndef GOOS
GOOS := $(shell go env GOOS)
endif

ifndef GOARCH
GOARCH := $(shell go env GOARCH)
endif

ifndef GOPATH
GOPATH := $(shell go env GOPATH)
endif

GOBIN := $(if $(shell go env GOBIN),$(shell go env GOBIN),$(GOPATH)/bin)
export PATH := $(GOBIN):$(PATH)

MODULE_ROOT := go.temporal.io/server
BUILD := ./build
COLOR := "\e[1;36m%s\e[0m\n"

define NEWLINE


endef

TEST_TIMEOUT := 20m
TEST_ARG ?= -race -v -timeout $(TEST_TIMEOUT)

INTEG_TEST_ROOT        := ./host
INTEG_TEST_OUT_DIR     := host
INTEG_TEST_XDC_ROOT    := ./host/xdc
INTEG_TEST_XDC_OUT_DIR := hostxdc
INTEG_TEST_NDC_ROOT    := ./host/ndc
INTEG_TEST_NDC_OUT_DIR := hostndc

GO_BUILD_LDFLAGS_CMD      := $(abspath ./scripts/go-build-ldflags.sh)
GO_BUILD_LDFLAGS          := $(shell $(GO_BUILD_LDFLAGS_CMD) LDFLAG)

DOCKER_IMAGE_TAG := $(shell whoami | tr -d " ")-local

ifndef PERSISTENCE_TYPE
override PERSISTENCE_TYPE := nosql
endif

ifndef PERSISTENCE_DRIVER
override PERSISTENCE_DRIVER := cassandra
endif

ifndef TEST_RUN_COUNT
override TEST_RUN_COUNT := 1
endif

ifdef TEST_TAG
override TEST_TAG := -tags $(TEST_TAG)
endif

PROTO_ROOT := proto
PROTO_FILES = $(shell find ./$(PROTO_ROOT)/internal -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_IMPORTS := -I=$(PROTO_ROOT)/internal -I=$(PROTO_ROOT)/api -I=$(GOPATH)/src/github.com/temporalio/gogo-protobuf/protobuf
PROTO_OUT := api

ALL_SRC         := $(shell find . -name "*.go" | grep -v -e "^$(PROTO_OUT)")
TEST_DIRS       := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))
INTEG_TEST_DIRS := $(filter $(INTEG_TEST_ROOT)/ $(INTEG_TEST_NDC_ROOT)/,$(TEST_DIRS))
UNIT_TEST_DIRS  := $(filter-out $(INTEG_TEST_ROOT)% $(INTEG_TEST_XDC_ROOT)% $(INTEG_TEST_NDC_ROOT)%,$(TEST_DIRS))

# Code coverage output files.
COVER_ROOT                 := $(BUILD)/coverage
UNIT_COVER_FILE            := $(COVER_ROOT)/unit_cover.out
INTEG_COVER_FILE           := $(COVER_ROOT)/integ_$(PERSISTENCE_TYPE)_$(PERSISTENCE_DRIVER)_cover.out
INTEG_XDC_COVER_FILE       := $(COVER_ROOT)/integ_xdc_$(PERSISTENCE_TYPE)_$(PERSISTENCE_DRIVER)_cover.out
INTEG_NDC_COVER_FILE       := $(COVER_ROOT)/integ_ndc_$(PERSISTENCE_TYPE)_$(PERSISTENCE_DRIVER)_cover.out

# NoSQL Cassandra
INTEG_NOSQL_CASS_COVER_FILE         := $(COVER_ROOT)/integ_nosql_cassandra_cover.out
INTEG_XDC_NOSQL_CASS_COVER_FILE     := $(COVER_ROOT)/integ_xdc_nosql_cassandra_cover.out
INTEG_NDC_NOSQL_CASS_COVER_FILE     := $(COVER_ROOT)/integ_ndc_nosql_cassandra_cover.out
# SQL MySQL
INTEG_SQL_MYSQL_COVER_FILE          := $(COVER_ROOT)/integ_sql_mysql_cover.out
INTEG_XDC_SQL_MYSQL_COVER_FILE      := $(COVER_ROOT)/integ_xdc_sql_mysql_cover.out
INTEG_NDC_SQL_MYSQL_COVER_FILE      := $(COVER_ROOT)/integ_ndc_sql_mysql_cover.out
# SQL PostgreSQL
INTEG_SQL_POSTGRESQL_COVER_FILE     := $(COVER_ROOT)/integ_sql_postgres_cover.out
INTEG_XDC_SQL_POSTGRESQL_COVER_FILE := $(COVER_ROOT)/integ_xdc_sql_postgres_cover.out
INTEG_NDC_SQL_POSTGRESQL_COVER_FILE := $(COVER_ROOT)/integ_ndc_sql_postgres_cover.out

# Need the following option to have integration tests count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
GOCOVERPKG_ARG := -coverpkg="$(MODULE_ROOT)/common/...,$(MODULE_ROOT)/service/...,$(MODULE_ROOT)/client/...,$(MODULE_ROOT)/tools/..."

##### Tools #####
update-checkers:
	@printf $(COLOR) "Install/update check tools..."
	GO111MODULE=off go get -u golang.org/x/tools/cmd/goimports
	GO111MODULE=off go get -u golang.org/x/lint/golint
	GO111MODULE=off go get -u honnef.co/go/tools/cmd/staticcheck
	GO111MODULE=off go get -u github.com/kisielk/errcheck
	GO111MODULE=off go get -u github.com/googleapis/api-linter/cmd/api-linter
	GO111MODULE=off go get -u github.com/bufbuild/buf/cmd/buf

update-mockgen:
	@printf $(COLOR) "Install/update mockgen tool..."
	GO111MODULE=off go get -u github.com/golang/mock/mockgen

update-proto-plugins:
	@printf $(COLOR) "Install/update proto plugins..."
	GO111MODULE=off go get -u github.com/temporalio/gogo-protobuf/protoc-gen-gogoslick
	GO111MODULE=off go get -u google.golang.org/grpc

update-tools: update-checkers update-mockgen update-proto-plugins

##### Proto #####
$(PROTO_OUT):
	@mkdir -p $(PROTO_OUT)

clean-proto:
	@rm -rf $(PROTO_OUT)/*

update-proto-submodule:
	@printf $(COLOR) "Update proto submodule from remote..."
	git submodule update --force --remote $(PROTO_ROOT)/api

install-proto-submodule:
	@printf $(COLOR) "Install proto submodule..."
	git submodule update --init $(PROTO_ROOT)/api

protoc: $(PROTO_OUT)
	@printf $(COLOR) "Build proto files..."
# Run protoc separately for each directory because of different package names.
	$(foreach PROTO_DIR,$(PROTO_DIRS),protoc $(PROTO_IMPORTS) --gogoslick_out=Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc,paths=source_relative:$(PROTO_OUT) $(PROTO_DIR)*.proto$(NEWLINE))

fix-proto-path:
	mv -f $(PROTO_OUT)/temporal/server/api/* $(PROTO_OUT) && rm -rf $(PROTO_OUT)/temporal

# All gRPC generated service files pathes relative to PROTO_OUT.
PROTO_GRPC_SERVICES = $(patsubst $(PROTO_OUT)/%,%,$(shell find $(PROTO_OUT) -name "service.pb.go"))
service_name = $(firstword $(subst /, ,$(1)))
mock_file_name = $(call service_name,$(1))mock/$(subst $(call service_name,$(1))/,,$(1:go=mock.go))

proto-mock: $(PROTO_OUT)
	@printf $(COLOR) "Generate proto mocks..."
	$(foreach PROTO_GRPC_SERVICE,$(PROTO_GRPC_SERVICES),cd $(PROTO_OUT) && mockgen -package $(call service_name,$(PROTO_GRPC_SERVICE))mock -source $(PROTO_GRPC_SERVICE) -destination $(call mock_file_name,$(PROTO_GRPC_SERVICE))$(NEWLINE) )

update-go-api:
	@printf $(COLOR) "Update go.temporal.io/api..."
	@go get -u go.temporal.io/api@master

goimports-proto:
	@printf $(COLOR) "Run goimports..."
	@goimports -w $(PROTO_OUT)

copyright-proto:
	@printf $(COLOR) "Update license headers..."
	@go run ./cmd/tools/copyright/licensegen.go --scanDir $(PROTO_OUT)

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f tctl
	@rm -f temporal-server
	@rm -f temporal-cassandra-tool
	@rm -f temporal-sql-tool

temporal-server:
	@printf $(COLOR) "Build temporal-server with OS: $(GOOS), ARCH: $(GOARCH)..."
	go build -ldflags '$(GO_BUILD_LDFLAGS)' -o temporal-server cmd/server/main.go

tctl:
	@printf $(COLOR) "Build tctl with OS: $(GOOS), ARCH: $(GOARCH)..."
	go build -o tctl cmd/tools/cli/main.go

temporal-cassandra-tool:
	@printf $(COLOR) "Build temporal-cassandra-tool with OS: $(GOOS), ARCH: $(GOARCH)..."
	go build -o temporal-cassandra-tool cmd/tools/cassandra/main.go

temporal-sql-tool:
	@printf $(COLOR) "Build temporal-sql-tool with OS: $(GOOS), ARCH: $(GOARCH)..."
	go build -o temporal-sql-tool cmd/tools/sql/main.go

##### Checks #####
copyright:
	@printf $(COLOR) "Check license header..."
	@go run ./cmd/tools/copyright/licensegen.go --verifyOnly || true

lint:
	@printf $(COLOR) "Run linter..."
	@golint ./...

vet:
	@printf $(COLOR) "Run go vet..."
	@go vet ./... || true

goimports-check:
	@printf $(COLOR) "Run goimports checks..."
# Use $(ALL_SRC) here to avoid checking generated files.
	@goimports -l $(ALL_SRC) || true

goimports:
	@printf $(COLOR) "Run goimports..."
	@goimports -local "go.temporal.io" -w $(ALL_SRC)

staticcheck:
	@printf $(COLOR) "Run staticcheck..."
	@staticcheck -fail none ./...

errcheck:
	@printf $(COLOR) "Run errcheck..."
	@errcheck ./... || true

api-linter:
	@printf $(COLOR) "Run api-linter..."
	@api-linter --set-exit-status $(PROTO_IMPORTS) --config=$(PROTO_ROOT)/api-linter.yaml $(PROTO_FILES)

buf-lint:
	@printf $(COLOR) "Run buf linter..."
	@(cd $(PROTO_ROOT) && buf check lint)

buf-build:
	@printf $(COLOR) "Build image.bin with buf..."
	@(cd $(PROTO_ROOT) && buf image build -o image.bin)

buf-breaking:
	@printf $(COLOR) "Run buf breaking changes check against image.bin..."
	@(cd $(PROTO_ROOT) && buf check breaking --against-input image.bin)

check: copyright goimports-check lint vet staticcheck errcheck

##### Tests #####
clean-test-results:
	@rm -f test.log

unit-test: clean-test-results
	@printf $(COLOR) "Run unit tests..."
	$(foreach UNIT_TEST_DIR,$(UNIT_TEST_DIRS), @go test -timeout $(TEST_TIMEOUT) -race $(UNIT_TEST_DIR) $(TEST_TAG) | tee -a test.log$(NEWLINE))
	@! grep -q "^--- FAIL" test.log

integration-test: clean-test-results
	@printf $(COLOR) "Run integration tests..."
	$(foreach INTEG_TEST_DIR,$(INTEG_TEST_DIRS), @go test -timeout $(TEST_TIMEOUT) -race $(INTEG_TEST_DIR) $(TEST_TAG) | tee -a test.log$(NEWLINE))
# Need to run xdc tests with race detector off because of ringpop bug causing data race issue.
	@go test -timeout $(TEST_TIMEOUT) $(INTEG_TEST_XDC_ROOT) $(TEST_TAG) | tee -a test.log
	@! grep -q "^--- FAIL" test.log

test: unit-test integration-test

##### Coverage #####
clean-build-results:
	@rm -rf $(BUILD)
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)

cover_profile: clean-build-results
	@echo "mode: atomic" > $(UNIT_COVER_FILE)

	@echo Running package tests:
	@for dir in $(UNIT_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "^mode: \w\+" >> $(UNIT_COVER_FILE); \
	done;

cover_integration_profile: clean-build-results
	@echo "mode: atomic" > $(INTEG_COVER_FILE)

	@echo Running integration test with $(PERSISTENCE_TYPE) $(PERSISTENCE_DRIVER)
	@mkdir -p $(BUILD)/$(INTEG_TEST_OUT_DIR)
	@time go test $(INTEG_TEST_ROOT) $(TEST_ARG) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_OUT_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_OUT_DIR)/coverage.out | grep -v "^mode: \w\+" >> $(INTEG_COVER_FILE)

cover_xdc_profile: clean-build-results
	@echo "mode: atomic" > $(INTEG_XDC_COVER_FILE)

	@echo Running integration test for cross dc with $(PERSISTENCE_TYPE) $(PERSISTENCE_DRIVER)
	@mkdir -p $(BUILD)/$(INTEG_TEST_XDC_OUT_DIR)
	@time go test -v -timeout $(TEST_TIMEOUT) $(INTEG_TEST_XDC_ROOT) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_XDC_OUT_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_XDC_OUT_DIR)/coverage.out | grep -v "^mode: \w\+" | grep -v "mode: set" >> $(INTEG_XDC_COVER_FILE)

cover_ndc_profile: clean-build-results
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(INTEG_NDC_COVER_FILE)

	@echo Running integration test for N DC with $(PERSISTENCE_TYPE) $(PERSISTENCE_DRIVER)
	@mkdir -p $(BUILD)/$(INTEG_TEST_NDC_OUT_DIR)
	@time go test -v -timeout $(TEST_TIMEOUT) $(INTEG_TEST_NDC_ROOT) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_NDC_OUT_DIR)/coverage.out -count=$(TEST_RUN_COUNT) || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_NDC_OUT_DIR)/coverage.out | grep -v "^mode: \w\+" | grep -v "mode: set" >> $(INTEG_NDC_COVER_FILE)

$(COVER_ROOT)/cover.out: $(UNIT_COVER_FILE) $(INTEG_NOSQL_CASS_COVER_FILE) $(INTEG_XDC_NOSQL_CASS_COVER_FILE) $(INTEG_NDC_NOSQL_CASS_COVER_FILE) $(INTEG_SQL_MYSQL_COVER_FILE) $(INTEG_XDC_SQL_MYSQL_COVER_FILE) $(INTEG_NDC_SQL_MYSQL_COVER_FILE) $(INTEG_SQL_POSTGRESQL_COVER_FILE) $(INTEG_XDC_SQL_POSTGRESQL_COVER_FILE) $(INTEG_NDC_SQL_POSTGRESQL_COVER_FILE)
	@echo "mode: atomic" > $(COVER_ROOT)/cover.out
	cat $(UNIT_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	# NoSQL Cassandra
	cat $(INTEG_NOSQL_CASS_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_XDC_NOSQL_CASS_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_NDC_NOSQL_CASS_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	# SQL MySQL
	cat $(INTEG_SQL_MYSQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_XDC_SQL_MYSQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_NDC_SQL_MYSQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	# SQL PostgreSQL
	cat $(INTEG_SQL_POSTGRESQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_XDC_SQL_POSTGRESQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_NDC_SQL_POSTGRESQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP "$(PROTO_OUT)|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out

cover: $(COVER_ROOT)/cover.out
	go tool cover -html=$(COVER_ROOT)/cover.out;

cover_ci: $(COVER_ROOT)/cover.out
	goveralls -coverprofile=$(COVER_ROOT)/cover.out -service=buildkite || echo Coveralls failed;

##### Schema #####
install-schema: temporal-cassandra-tool
	@printf $(COLOR) "Install Cassandra schema..."
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility update-schema -d ./schema/cassandra/visibility/versioned

install-schema-mysql-pre5720: temporal-sql-tool
	@printf $(COLOR) "Install MySQL schema..."
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' create --db temporal
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal update-schema -d ./schema/mysql/v57/temporal/versioned
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' create --db temporal_visibility
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal_visibility setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal_visibility update-schema -d ./schema/mysql/v57/visibility/versioned

install-schema-mysql: temporal-sql-tool
	@printf $(COLOR) "Install MySQL schema..."
	./temporal-sql-tool --ep 127.0.0.1 -u root --pw root create --db temporal
	./temporal-sql-tool --ep 127.0.0.1 -u root --pw root --db temporal setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 -u root --pw root --db temporal update-schema -d ./schema/mysql/v57/temporal/versioned
	./temporal-sql-tool --ep 127.0.0.1 -u root --pw root create --db temporal_visibility
	./temporal-sql-tool --ep 127.0.0.1 -u root --pw root --db temporal_visibility setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 -u root --pw root --db temporal_visibility update-schema -d ./schema/mysql/v57/visibility/versioned

install-schema-postgresql: temporal-sql-tool
	@printf $(COLOR) "Install Postgres schema..."
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u temporal -pw temporal --pl postgres create --db temporal
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u temporal -pw temporal --pl postgres --db temporal setup -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u temporal -pw temporal --pl postgres --db temporal update-schema -d ./schema/postgresql/v96/temporal/versioned
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u temporal -pw temporal --pl postgres create --db temporal_visibility
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u temporal -pw temporal --pl postgres --db temporal_visibility setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u temporal -pw temporal --pl postgres --db temporal_visibility update-schema -d ./schema/postgresql/v96/visibility/versioned

install-schema-cdc: temporal-cassandra-tool
	@printf $(COLOR)  "Set up temporal_active key space..."
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_active --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_active setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_active update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility_active --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_active setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_active update-schema -d ./schema/cassandra/visibility/versioned

	@printf $(COLOR) "Set up temporal_standby key space..."
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_standby --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_standby setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_standby update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility_standby --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_standby setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_standby update-schema -d ./schema/cassandra/visibility/versioned

	@printf $(COLOR) "Set up temporal_other key space..."
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_other --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_other setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_other update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility_other --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_other setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_other update-schema -d ./schema/cassandra/visibility/versioned

##### Run server #####
start-dependencies:
	docker-compose -f docker/dependencies/docker-compose.yml -f docker/dependencies/docker-compose.$(GOOS).yml up

stop-dependencies:
	docker-compose -f docker/dependencies/docker-compose.yml -f docker/dependencies/docker-compose.$(GOOS).yml down

start: temporal-server
	./temporal-server start

start-cdc-active: temporal-server
	./temporal-server --zone active start

start-cdc-standby: temporal-server
	./temporal-server --zone standby start

start-cdc-other: temporal-server
	./temporal-server --zone other start

##### Auxilary #####
go-generate:
	@printf $(COLOR) "Regenerate everything..."
	@go generate ./...
	@goimports -w $(ALL_SRC)

gomodtidy:
	@printf $(COLOR) "go mod tidy..."
	@go mod tidy
