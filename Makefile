.PHONY: test bins clean cover cover_ci
PROJECT_ROOT = github.com/uber/cadence

export PATH := $(GOPATH)/bin:$(PATH)

THRIFT_GENDIR=.gen

# default target
default: test

# define the list of thrift files the service depends on
# (if you have some)
THRIFTRW_SRCS = \
  idl/github.com/uber/cadence/cadence.thrift \
  idl/github.com/uber/cadence/health.thrift \
  idl/github.com/uber/cadence/history.thrift \
  idl/github.com/uber/cadence/matching.thrift \
  idl/github.com/uber/cadence/replicator.thrift \
  idl/github.com/uber/cadence/indexer.thrift \
  idl/github.com/uber/cadence/shared.thrift \
  idl/github.com/uber/cadence/admin.thrift \
  idl/github.com/uber/cadence/sqlblobs.thrift \

PROGS = cadence
TEST_ARG ?= -race -v -timeout 40m
BUILD := ./build
TOOLS_CMD_ROOT=./cmd/tools
INTEG_TEST_ROOT=./host
INTEG_TEST_DIR=host
INTEG_TEST_XDC_ROOT=./hostxdc
INTEG_TEST_XDC_DIR=hostxdc

ifndef EVENTSV2
override EVENTSV2 = false
endif

ifndef PERSISTENCE_TYPE
override PERSISTENCE_TYPE = cassandra
endif

ifdef TEST_TAG
override TEST_TAG := -tags $(TEST_TAG)
endif

define thriftrwrule
THRIFTRW_GEN_SRC += $(THRIFT_GENDIR)/go/$1/$1.go

$(THRIFT_GENDIR)/go/$1/$1.go:: $2
	@mkdir -p $(THRIFT_GENDIR)/go
	$(GOPATH)/bin/thriftrw --plugin=yarpc --pkg-prefix=$(PROJECT_ROOT)/$(THRIFT_GENDIR)/go/ --out=$(THRIFT_GENDIR)/go $2
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

# all directories with *_test.go files in them (exclude hostxdc)
TEST_DIRS := $(filter-out $(INTEG_TEST_XDC_ROOT)%, $(sort $(dir $(filter %_test.go,$(ALL_SRC)))))

# all tests other than integration test fall into the pkg_test category
PKG_TEST_DIRS := $(filter-out $(INTEG_TEST_ROOT)%,$(TEST_DIRS))

# Need the following option to have integration tests
# count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
GOCOVERPKG_ARG := -coverpkg="$(PROJECT_ROOT)/common/...,$(PROJECT_ROOT)/service/...,$(PROJECT_ROOT)/client/...,$(PROJECT_ROOT)/tools/..."

dep-ensured:
	./install-dep.sh
	dep ensure

yarpc-install:
	go get './vendor/go.uber.org/thriftrw'
	go get './vendor/go.uber.org/yarpc/encoding/thrift/thriftrw-plugin-yarpc'

clean_thrift:
	rm -rf .gen

thriftc: yarpc-install $(THRIFTRW_GEN_SRC)

copyright: cmd/tools/copyright/licensegen.go
	GOOS= GOARCH= go run ./cmd/tools/copyright/licensegen.go --verifyOnly

cadence-cassandra-tool: dep-ensured $(TOOLS_SRC)
	go build -i -o cadence-cassandra-tool cmd/tools/cassandra/main.go

cadence: dep-ensured $(TOOLS_SRC)
	go build -i -o cadence cmd/tools/cli/main.go

cadence-server: dep-ensured $(ALL_SRC)
	go build -i -o cadence-server cmd/server/cadence.go cmd/server/server.go

bins_nothrift: lint copyright cadence-cassandra-tool cadence cadence-server

bins: thriftc bins_nothrift

test: dep-ensured bins
	@rm -f test
	@rm -f test.log
	@for dir in $(TEST_DIRS); do \
		go test -timeout 20m -race -coverprofile=$@ "$$dir" $(TEST_TAG) | tee -a test.log; \
	done;

test_eventsV2: dep-ensured bins
	@rm -f test_eventsV2
	@rm -f test_eventsV2.log
	@echo Running integration test
	@for dir in $(INTEG_TEST_ROOT); do \
    		go test -timeout 20m -coverprofile=$@ "$$dir" -v $(TEST_TAG) -eventsV2=true | tee -a test_eventsV2.log; \
    done;

test_eventsV2_xdc: dep-ensured bins
	@rm -f test_eventsV2_xdc
	@rm -f test_eventsV2_xdc.log
	@echo Running integration test for cross dc:
	@for dir in $(INTEG_TEST_XDC_ROOT); do \
		go test -timeout 20m -coverprofile=$@ "$$dir" -v -eventsV2xdc=true | tee -a test_eventsV2_xdc.log; \
	done;

# need to run xdc tests with race detector off because of ringpop bug causing data race issue
test_xdc: dep-ensured bins
	@rm -f test
	@rm -f test.log
	@for dir in $(INTEG_TEST_XDC_ROOT); do \
		go test -timeout 20m -coverprofile=$@ "$$dir" | tee -a test.log; \
	done;

cover_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@echo "mode: atomic" > $(BUILD)/cover.out

	@echo Running package tests:
	@for dir in $(PKG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out; \
	done;

cover_integration_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@echo "mode: atomic" > $(BUILD)/cover.out

	@echo Running integration test with $(PERSISTENCE_TYPE) and eventsV2 $(EVENTSV2)
	@mkdir -p $(BUILD)/$(INTEG_TEST_DIR)
	@time go test $(INTEG_TEST_ROOT) $(TEST_ARG) $(TEST_TAG) -eventsV2=$(EVENTSV2) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_DIR)/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out

cover_xdc_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@echo "mode: atomic" > $(BUILD)/cover.out

	@echo Running integration test for cross dc with $(PERSISTENCE_TYPE)
	@mkdir -p $(BUILD)/$(INTEG_TEST_XDC_DIR)
	@time go test $(INTEG_TEST_XDC_ROOT) -persistenceType=$(PERSISTENCE_TYPE) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/$(INTEG_TEST_XDC_DIR)/coverage.out || exit 1;
	@cat $(BUILD)/$(INTEG_TEST_XDC_DIR)/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out

cover: cover_profile
	go tool cover -html=$(BUILD)/cover.out;

cover_ci: cover_profile
	goveralls -coverprofile=$(BUILD)/cover.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"; \

cover_integration_ci: cover_integration_profile
	goveralls -coverprofile=$(BUILD)/cover.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"; \

cover_xdc_ci: cover_xdc_profile
	goveralls -coverprofile=$(BUILD)/cover.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"; \

lint: dep-ensured
	@echo Running linter
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
	@gofmt -w $(ALL_SRC)

clean:
	rm -f cadence
	rm -f cadence-cassandra-tool
	rm -f cadence-server
	rm -Rf $(BUILD)

install-schema: bins
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence --rf 1
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence setup-schema -v 0.0
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility --rf 1
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility setup-schema -v 0.0
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned

start: bins
	./cadence-server start

install-schema-cdc: bins
	@echo Setting up cadence_active key space
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_active --rf 1
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_active setup-schema -v 0.0
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_active update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility_active --rf 1
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility_active setup-schema -v 0.0
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility_active update-schema -d ./schema/cassandra/visibility/versioned

	@echo Setting up cadence_standby key space
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_standby --rf 1
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_standby setup-schema -v 0.0
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_standby update-schema -d ./schema/cassandra/cadence/versioned
	./cadence-cassandra-tool --ep 127.0.0.1 create -k cadence_visibility_standby --rf 1
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility_standby setup-schema -v 0.0
	./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility_standby update-schema -d ./schema/cassandra/visibility/versioned

start-cdc-active: bins
	./cadence-server --zone active start

start-cdc-standby: bins
	./cadence-server --zone standby start
