.PHONY: git-submodules test bins clean cover cover_ci
PROJECT_ROOT = github.com/uber/cadence

export PATH := $(shell go env GOPATH)/bin:$(PATH)

ifndef GOOS
GOOS := $(shell go env GOOS)
endif

ifndef GOARCH
GOARCH := $(shell go env GOARCH)
endif

THRIFT_GENDIR=.gen

default: test

# define the list of thrift files the service depends on
# (if you have some)
THRIFTRW_SRCS = \
  idls/thrift/cadence.thrift \
  idls/thrift/health.thrift \
  idls/thrift/history.thrift \
  idls/thrift/matching.thrift \
  idls/thrift/replicator.thrift \
  idls/thrift/indexer.thrift \
  idls/thrift/shared.thrift \
  idls/thrift/admin.thrift \
  idls/thrift/sqlblobs.thrift \
  idls/thrift/checksum.thrift \

PROGS = cadence
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

define thriftrwrule
THRIFTRW_GEN_SRC += $(THRIFT_GENDIR)/go/$1/$1.go

$(THRIFT_GENDIR)/go/$1/$1.go:: $2
	@mkdir -p $(THRIFT_GENDIR)/go
	thriftrw --plugin=yarpc --pkg-prefix=$(PROJECT_ROOT)/$(THRIFT_GENDIR)/go/ --out=$(THRIFT_GENDIR)/go $2
endef

$(foreach tsrc,$(THRIFTRW_SRCS),$(eval $(call \
	thriftrwrule,$(basename $(notdir \
	$(shell echo $(tsrc) | tr A-Z a-z))),$(tsrc))))

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

git-submodules:
	git submodule update --init --recursive

yarpc-install:
	GO111MODULE=off go get -u github.com/myitcv/gobin
	GOOS= GOARCH= gobin -mod=readonly go.uber.org/thriftrw
	GOOS= GOARCH= gobin -mod=readonly go.uber.org/yarpc/encoding/thrift/thriftrw-plugin-yarpc

clean_thrift:
	rm -rf .gen

thriftc: yarpc-install git-submodules $(THRIFTRW_GEN_SRC) copyright

copyright: cmd/tools/copyright/licensegen.go
	GOOS= GOARCH= go run ./cmd/tools/copyright/licensegen.go --verifyOnly

cadence-cassandra-tool: $(TOOLS_SRC)
	@echo "compiling cadence-cassandra-tool with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o cadence-cassandra-tool cmd/tools/cassandra/main.go

cadence-sql-tool: $(TOOLS_SRC)
	@echo "compiling cadence-sql-tool with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o cadence-sql-tool cmd/tools/sql/main.go

cadence: $(TOOLS_SRC)
	@echo "compiling cadence with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o cadence cmd/tools/cli/main.go

cadence-server: $(ALL_SRC)
	@echo "compiling cadence-server with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -ldflags '$(GO_BUILD_LDFLAGS)' -i -o cadence-server cmd/server/main.go

cadence-canary: $(ALL_SRC)
	@echo "compiling cadence-canary with OS: $(GOOS), ARCH: $(GOARCH)"
	go build -i -o cadence-canary cmd/canary/main.go

go-generate:
	GO111MODULE=off go get -u github.com/myitcv/gobin
	GOOS= GOARCH= gobin -mod=readonly github.com/golang/mock/mockgen
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

fmt:
	GO111MODULE=off go get -u github.com/myitcv/gobin
	GOOS= GOARCH= gobin -mod=readonly golang.org/x/tools/cmd/goimports
	@echo "running goimports"
	@goimports -local "github.com/uber/cadence" -w $(ALL_SRC)

bins_nothrift: fmt lint copyright cadence-cassandra-tool cadence-sql-tool cadence cadence-server cadence-canary

bins: thriftc bins_nothrift

test: bins
	@rm -f test
	@rm -f test.log
	@for dir in $(TEST_DIRS); do \
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

cover_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(UNIT_COVER_FILE)

	@echo Running package tests:
	@for dir in $(PKG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "^mode: \w\+" >> $(UNIT_COVER_FILE); \
	done;

cover_integration_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(INTEG_COVER_FILE)

	@echo Running integration test with $(PERSISTENCE_TYPE)
	@mkdir -p $(BUILD)/$(INTEG_TEST_DIR)
	@time go test $(INTEG_TEST_ROOT) $(TEST_ARG) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_DIR)/coverage.out | grep -v "^mode: \w\+" >> $(INTEG_COVER_FILE)

cover_xdc_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@mkdir -p $(COVER_ROOT)
	@echo "mode: atomic" > $(INTEG_XDC_COVER_FILE)

	@echo Running integration test for cross dc with $(PERSISTENCE_TYPE)
	@mkdir -p $(BUILD)/$(INTEG_TEST_XDC_DIR)
	@time go test -v -timeout $(TEST_TIMEOUT) $(INTEG_TEST_XDC_ROOT) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_XDC_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_XDC_DIR)/coverage.out | grep -v "^mode: \w\+" | grep -v "mode: set" >> $(INTEG_XDC_COVER_FILE)

cover_ndc_profile: clean bins_nothrift
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
	rm -f cadence
	rm -f cadence-server
	rm -f cadence-canary
	rm -f cadence-sql-tool
	rm -f cadence-cassandra-tool
	rm -Rf $(BUILD)

install-schema: bins
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned

install-schema-mysql: bins
	./cadence-sql-tool --ep 127.0.0.1 create --db cadence
	./cadence-sql-tool --ep 127.0.0.1 --db cadence setup-schema -v 0.0
	./cadence-sql-tool --ep 127.0.0.1 --db cadence update-schema -d ./schema/mysql/v57/cadence/versioned
	./cadence-sql-tool --ep 127.0.0.1 create --db cadence_visibility
	./cadence-sql-tool --ep 127.0.0.1 --db cadence_visibility setup-schema -v 0.0
	./cadence-sql-tool --ep 127.0.0.1 --db cadence_visibility update-schema -d ./schema/mysql/v57/visibility/versioned

install-schema-postgres: bins
	./cadence-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw cadence --pl postgres create --db cadence
	./cadence-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw cadence --pl postgres --db cadence setup -v 0.0
	./cadence-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw cadence --pl postgres --db cadence update-schema -d ./schema/postgres/cadence/versioned
	./cadence-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw cadence --pl postgres create --db cadence_visibility
	./cadence-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw cadence --pl postgres --db cadence_visibility setup-schema -v 0.0
	./cadence-sql-tool --ep 127.0.0.1 -p 5432 -u postgres -pw cadence --pl postgres --db cadence_visibility update-schema -d ./schema/postgres/visibility/versioned

start: bins
	./cadence-server start

install-schema-cdc: bins
	@echo Setting up cadence_active key space
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_active --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_active setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_active update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility_active --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility_active setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility_active update-schema -d ./schema/cassandra/visibility/versioned

	@echo Setting up cadence_standby key space
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_standby --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_standby setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_standby update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility_standby --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility_standby setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility_standby update-schema -d ./schema/cassandra/visibility/versioned

	@echo Setting up cadence_other key space
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_other --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_other setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_other update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility_other --rf 1
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility_other setup-schema -v 0.0
	./cadence-cassandra-tool --ep 127.0.0.1 -k cadence_visibility_other update-schema -d ./schema/cassandra/visibility/versioned

start-cdc-active: bins
	./cadence-server --zone active start

start-cdc-standby: bins
	./cadence-server --zone standby start

start-cdc-other: bins
	./cadence-server --zone other start

start-canary: bins
	./cadence-canary start
