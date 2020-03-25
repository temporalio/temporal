.PHONY: test bins clean cover cover_ci unit-test
PROJECT_ROOT = github.com/temporalio/temporal

export PATH := $(shell go env GOPATH)/bin:$(PATH)

ifndef GOOS
GOOS := $(shell go env GOOS)
endif

ifndef GOARCH
GOARCH := $(shell go env GOARCH)
endif

default: test

TEST_TIMEOUT = 20m
TEST_ARG ?= -race -v -timeout $(TEST_TIMEOUT)
BUILD := ./build
TOOLS_CMD_ROOT=./cmd/tools
INTEG_TEST_ROOT=./host
INTEG_TEST_DIR=host
INTEG_TEST_XDC_ROOT=./host/xdc
INTEG_TEST_XDC_DIR=hostxdc
INTEG_TEST_NDC_ROOT=./host/ndc
INTEG_TEST_NDC_DIR=hostndc

GO_BUILD_LDFLAGS_CMD      := $(abspath ./scripts/go-build-ldflags.sh)
GO_BUILD_LDFLAGS          := $(shell $(GO_BUILD_LDFLAGS_CMD) LDFLAG)

ifndef PERSISTENCE_TYPE
override PERSISTENCE_TYPE = cassandra
endif

ifndef TEST_RUN_COUNT
override TEST_RUN_COUNT = 1
endif

ifdef TEST_TAG
override TEST_TAG := -tags $(TEST_TAG)
endif

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# filter out the src files for tools
TOOLS_SRC := $(shell find ./tools -name "*.go")
TOOLS_SRC += $(TOOLS_CMD_ROOT)

# all directories with *_test.go files in them (exclude host/xdc)
TEST_DIRS := $(filter-out $(INTEG_TEST_XDC_ROOT)%, $(sort $(dir $(filter %_test.go,$(ALL_SRC)))))

# all tests other than integration test fall into the pkg_test category
PKG_TEST_DIRS := $(filter-out $(INTEG_TEST_ROOT)%,$(TEST_DIRS))

# Code coverage output files
COVER_ROOT                 := $(BUILD)/coverage
UNIT_COVER_FILE            := $(COVER_ROOT)/unit_cover.out
INTEG_COVER_FILE           := $(COVER_ROOT)/integ_$(PERSISTENCE_TYPE)_cover.out
INTEG_XDC_COVER_FILE       := $(COVER_ROOT)/integ_xdc_$(PERSISTENCE_TYPE)_cover.out
INTEG_CASS_COVER_FILE      := $(COVER_ROOT)/integ_cassandra_cover.out
INTEG_XDC_CASS_COVER_FILE  := $(COVER_ROOT)/integ_xdc_cassandra_cover.out
INTEG_SQL_COVER_FILE       := $(COVER_ROOT)/integ_sql_cover.out
INTEG_XDC_SQL_COVER_FILE   := $(COVER_ROOT)/integ_xdc_sql_cover.out
INTEG_NDC_COVER_FILE       := $(COVER_ROOT)/integ_ndc_$(PERSISTENCE_TYPE)_cover.out
INTEG_NDC_CASS_COVER_FILE  := $(COVER_ROOT)/integ_ndc_cassandra_cover.out
INTEG_NDC_SQL_COVER_FILE   := $(COVER_ROOT)/integ_ndc_sql_cover.out

# Need the following option to have integration tests
# count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
GOCOVERPKG_ARG := -coverpkg="$(PROJECT_ROOT)/common/...,$(PROJECT_ROOT)/service/...,$(PROJECT_ROOT)/client/...,$(PROJECT_ROOT)/tools/..."

#================================= protobuf ===================================
PROTO_ROOT := proto
# List only subdirectories with *.proto files (sort to remove duplicates).
# Note: using "shell find" instead of "wildcard" because "wildcard" caches directory structure.
PROTO_DIRS = $(sort $(dir $(shell find $(PROTO_ROOT) -name "*.proto" | grep -v temporal-proto)))
PROTO_SERVICES = $(shell find $(PROTO_ROOT) -name "*service.proto" | grep -v temporal-proto)
PROTO_IMPORT := $(PROTO_ROOT):$(PROTO_ROOT)/temporal-proto:$(GOPATH)/src/github.com/gogo/protobuf/protobuf
PROTO_GEN := .gen/proto

$(PROTO_GEN):
	mkdir -p $(PROTO_GEN)

clean-proto:
	rm -rf $(PROTO_GEN)/*

update-proto-submodule:
	git submodule update --remote $(PROTO_ROOT)/temporal-proto

install-proto-submodule:
	git submodule update --init $(PROTO_ROOT)/temporal-proto

protoc: $(PROTO_GEN)
#   run protoc separately for each directory because of different package names
	$(foreach PROTO_DIR,$(PROTO_DIRS),protoc --proto_path=$(PROTO_IMPORT) --gogoslick_out=Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc,paths=source_relative:$(PROTO_GEN) $(PROTO_DIR)*.proto;)

# All GRPC generated service files pathes relative to PROTO_ROOT
PROTO_GRPC_SERVICES = $(patsubst $(PROTO_GEN)/%,%,$(shell find $(PROTO_GEN) -name "service.pb.go"))
dir_no_slash = $(patsubst %/,%,$(dir $(1)))
dirname = $(notdir $(call dir_no_slash,$(1)))

proto-mock: $(PROTO_GEN)
	go get github.com/golang/mock/mockgen@latest
	@echo "Generate proto mocks..."
	@$(foreach PROTO_GRPC_SERVICE,$(PROTO_GRPC_SERVICES),cd $(PROTO_GEN) && mockgen -package $(call dirname,$(PROTO_GRPC_SERVICE))mock -source $(PROTO_GRPC_SERVICE) -destination $(call dir_no_slash,$(PROTO_GRPC_SERVICE))mock/$(notdir $(PROTO_GRPC_SERVICE:go=mock.go)) )

update-proto: clean-proto update-proto-submodule grpc-install protoc proto-mock

proto: clean-proto install-proto-submodule grpc-install protoc proto-mock

#==============================================================================

grpc-install:
	GO111MODULE=off go get -u github.com/gogo/protobuf/protoc-gen-gogoslick
	GO111MODULE=off go get -u google.golang.org/grpc

copyright: cmd/tools/copyright/licensegen.go
	GOOS= GOARCH= go run ./cmd/tools/copyright/licensegen.go --verifyOnly

temporal-cassandra-tool: $(TOOLS_SRC)
	@echo "compiling temporal-cassandra-tool with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o temporal-cassandra-tool cmd/tools/cassandra/main.go

temporal-sql-tool: $(TOOLS_SRC)
	@echo "compiling temporal-sql-tool with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o temporal-sql-tool cmd/tools/sql/main.go

tctl: $(TOOLS_SRC)
	@echo "compiling tctl with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o tctl cmd/tools/cli/main.go

temporal-server: $(ALL_SRC)
	@echo "compiling temporal-server with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -ldflags '$(GO_BUILD_LDFLAGS)' -i -o temporal-server cmd/server/main.go

temporal-canary: $(ALL_SRC)
	@echo "compiling temporal-canary with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o temporal-canary cmd/canary/main.go

go-generate:
	go get github.com/golang/mock/mockgen@latest
	@echo "running go generate ./..."
	@go generate ./...

lint:
	@echo "running linter"
	@lintFail=0; for file in $(ALL_SRC); do \
		golint "$$file"; \
		if [ $$? -eq 1 ]; then lintFail=1; fi; \
	done; \
	if [ $$lintFail -eq 1 ]; then exit 1; fi;
	@OUTPUT=`gofmt -l $(ALL_SRC) 2>&1`; \
	if [ "$$OUTPUT" ]; then \
		echo "Run 'make fmt'. gofmt must be run on the following files:"; \
		echo "$$OUTPUT"; \
		exit 1; \
	fi

goimports:
	GO111MODULE=off go get golang.org/x/tools/cmd/goimports
	@echo "running goimports"
	@goimports -local "github.com/temporalio/temporal" -w $(ALL_SRC)

bins: proto goimports lint copyright temporal-cassandra-tool temporal-sql-tool tctl temporal-server temporal-canary

test: bins
	@rm -f test
	@rm -f test.log
	@for dir in $(TEST_DIRS); do \
		go test -timeout $(TEST_TIMEOUT) -race -coverprofile=$@ "$$dir" $(TEST_TAG) | tee -a test.log; \
	done;

unit-test:
	@rm -f test
	@rm -f test.log
	@for dir in $(PKG_TEST_DIRS); do \
		go test -timeout $(TEST_TIMEOUT) -race -coverprofile=$@ "$$dir" $(TEST_TAG) | tee -a test.log; \
	done;

release: go-generate test

# need to run xdc tests with race detector off because of ringpop bug causing data race issue
test_xdc: bins
	@rm -f test
	@rm -f test.log
	@for dir in $(INTEG_TEST_XDC_ROOT); do \
		go test -timeout $(TEST_TIMEOUT) -coverprofile=$@ "$$dir" $(TEST_TAG) | tee -a test.log; \
	done;

cover_profile: clean bins
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(UNIT_COVER_FILE)

	@echo Running package tests:
	@for dir in $(PKG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "^mode: \w\+" >> $(UNIT_COVER_FILE); \
	done;

cover_integration_profile: clean bins
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(INTEG_COVER_FILE)

	@echo Running integration test with $(PERSISTENCE_TYPE)
	@mkdir -p $(BUILD)/$(INTEG_TEST_DIR)
	@time go test $(INTEG_TEST_ROOT) $(TEST_ARG) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_DIR)/coverage.out | grep -v "^mode: \w\+" >> $(INTEG_COVER_FILE)

cover_xdc_profile: clean bins
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(INTEG_XDC_COVER_FILE)

	@echo Running integration test for cross dc with $(PERSISTENCE_TYPE)
	@mkdir -p $(BUILD)/$(INTEG_TEST_XDC_DIR)
	@time go test -v -timeout $(TEST_TIMEOUT) $(INTEG_TEST_XDC_ROOT) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_XDC_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_XDC_DIR)/coverage.out | grep -v "^mode: \w\+" | grep -v "mode: set" >> $(INTEG_XDC_COVER_FILE)

cover_ndc_profile: clean bins
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(INTEG_NDC_COVER_FILE)

	@echo Running integration test for 3+ dc with $(PERSISTENCE_TYPE)
	@mkdir -p $(BUILD)/$(INTEG_TEST_NDC_DIR)
	@time go test -v -timeout $(TEST_TIMEOUT) $(INTEG_TEST_NDC_ROOT) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_NDC_DIR)/coverage.out -count=$(TEST_RUN_COUNT) || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_NDC_DIR)/coverage.out | grep -v "^mode: \w\+" | grep -v "mode: set" >> $(INTEG_NDC_COVER_FILE)

$(COVER_ROOT)/cover.out: $(UNIT_COVER_FILE) $(INTEG_CASS_COVER_FILE) $(INTEG_XDC_CASS_COVER_FILE) $(INTEG_SQL_COVER_FILE) $(INTEG_XDC_SQL_COVER_FILE)
	@echo "mode: atomic" > $(COVER_ROOT)/cover.out
	cat $(UNIT_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP ".gen|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_CASS_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP ".gen|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_XDC_CASS_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP ".gen|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_SQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP ".gen|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out
	cat $(INTEG_XDC_SQL_COVER_FILE) | grep -v "^mode: \w\+" | grep -vP ".gen|[Mm]ock[s]?" >> $(COVER_ROOT)/cover.out

cover: $(COVER_ROOT)/cover.out
	go tool cover -html=$(COVER_ROOT)/cover.out;

cover_ci: $(COVER_ROOT)/cover.out
	goveralls -coverprofile=$(COVER_ROOT)/cover.out -service=buildkite || echo Coveralls failed;

clean:
	rm -f temporal
	rm -f temporal-server
	rm -f temporal-canary
	rm -f temporal-sql-tool
	rm -f temporal-cassandra-tool
	rm -Rf $(BUILD)

install-schema: bins
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility update-schema -d ./schema/cassandra/visibility/versioned

install-schema-mysql-pre5720: bins
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' create --db temporal
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal update-schema -d ./schema/mysql/v57/temporal/versioned
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' create --db temporal_visibility
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal_visibility setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 --ca tx_isolation='READ-COMMITTED' --db temporal_visibility update-schema -d ./schema/mysql/v57/visibility/versioned

install-schema-mysql: bins
	./temporal-sql-tool --ep 127.0.0.1 create --db temporal
	./temporal-sql-tool --ep 127.0.0.1 --db temporal setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 --db temporal update-schema -d ./schema/mysql/v57/temporal/versioned
	./temporal-sql-tool --ep 127.0.0.1 create --db temporal_visibility
	./temporal-sql-tool --ep 127.0.0.1 --db temporal_visibility setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 --db temporal_visibility update-schema -d ./schema/mysql/v57/visibility/versioned

install-schema-postgres: bins
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw temporal --pl postgres create --db temporal
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw temporal --pl postgres --db temporal setup -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw temporal --pl postgres --db temporal update-schema -d ./schema/postgres/temporal/versioned
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw temporal --pl postgres create --db temporal_visibility
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw temporal --pl postgres --db temporal_visibility setup-schema -v 0.0
	./temporal-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw temporal --pl postgres --db temporal_visibility update-schema -d ./schema/postgres/visibility/versioned

start: bins
	./temporal-server start

install-schema-cdc: bins
	@echo Setting up temporal_active key space
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_active --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_active setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_active update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility_active --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_active setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_active update-schema -d ./schema/cassandra/visibility/versioned

	@echo Setting up temporal_standby key space
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_standby --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_standby setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_standby update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility_standby --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_standby setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_standby update-schema -d ./schema/cassandra/visibility/versioned

	@echo Setting up temporal_other key space
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_other --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_other setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_other update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool --ep 127.0.0.1 create -k temporal_visibility_other --rf 1
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_other setup-schema -v 0.0
	./temporal-cassandra-tool --ep 127.0.0.1 -k temporal_visibility_other update-schema -d ./schema/cassandra/visibility/versioned

start-cdc-active: bins
	./temporal-server --zone active start

start-cdc-standby: bins
	./temporal-server --zone standby start

start-cdc-other: bins
	./temporal-server --zone other start

start-canary: bins
	./temporal-canary start
