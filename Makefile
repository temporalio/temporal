.PHONY: test bins clean cover cover_ci
PROJECT_ROOT = github.com/uber/cadence

export PATH := $(GOPATH)/bin:$(PATH)

THRIFT_GENDIR=.gen

# default target
default: test

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/github.com/uber/cadence/cadence.thrift \
	idl/github.com/uber/cadence/shared.thrift \
  idl/github.com/uber/cadence/history.thrift \
  idl/github.com/uber/cadence/matching.thrift \

PROGS = cadence
TEST_ARG ?= -race -v -timeout 5m
BUILD := ./build
TOOLS_CMD_ROOT=./cmd/tools
INTEG_TEST_ROOT=./host

export PATH := $(GOPATH)/bin:$(PATH)

THRIFT_GEN=$(GOPATH)/bin/thrift-gen

define thriftrule
THRIFT_GEN_SRC += $(THRIFT_GENDIR)/go/$1/tchan-$1.go

$(THRIFT_GENDIR)/go/$1/tchan-$1.go:: $2 $(THRIFT_GEN)
	@mkdir -p $(THRIFT_GENDIR)/go
	$(ECHO_V)$(THRIFT_GEN) --generateThrift --packagePrefix $(PROJECT_ROOT)/$(THRIFT_GENDIR)/go/ --inputFile $2 --outputDir $(THRIFT_GENDIR)/go \
		$(foreach template,$(THRIFT_TEMPLATES), --template $(template))
endef

$(foreach tsrc,$(THRIFT_SRCS),$(eval $(call \
	thriftrule,$(basename $(notdir \
	$(shell echo $(tsrc) | tr A-Z a-z))),$(tsrc))))

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# filter out the src files for tools
TOOLS_SRC := $(shell find ./tools -name "*.go")
TOOLS_SRC += $(TOOLS_CMD_ROOT)

# all directories with *_test.go files in them
TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))

# dirs that contain integration tests, these need to be treated
# differently to get correct code coverage
INTEG_TEST_DIRS := $(filter $(INTEG_TEST_ROOT)%,$(TEST_DIRS))
# all tests other than integration test fall into the pkg_test category
PKG_TEST_DIRS := $(filter-out $(INTEG_TEST_ROOT)%,$(TEST_DIRS))


# Need the following option to have integration tests
# count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
GOCOVERPKG_ARG := -coverpkg="$(PROJECT_ROOT)/common/...,$(PROJECT_ROOT)/service/...,$(PROJECT_ROOT)/client/...,$(PROJECT_ROOT)/tools/..."

vendor/glide.updated: glide.lock glide.yaml
	glide install
	touch vendor/glide.updated

clean_thrift:
	rm -rf .gen

thriftc: clean_thrift vendor/glide.updated $(THRIFT_GEN_SRC)

cadence-cassandra-tool: vendor/glide.updated $(TOOLS_SRC)
	go build -i -o cadence-cassandra-tool cmd/tools/cassandra/main.go

cadence: vendor/glide.updated main.go
	go build -i -o cadence main.go

bins_nothrift: lint cadence-cassandra-tool cadence

bins: thriftc bins_nothrift

test: bins
	@rm -f test
	@rm -f test.log
	@for dir in $(TEST_DIRS); do \
		go test -coverprofile=$@ "$$dir" | tee -a test.log; \
	done;

cover_profile: clean bins_nothrift
	@mkdir -p $(BUILD)
	@echo "mode: atomic" > $(BUILD)/cover.out

	@echo Running integration tests:
	@time for dir in $(INTEG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out; \
	done

	@echo Running package tests:
	@for dir in $(PKG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out; \
	done;

cover: cover_profile
	go tool cover -html=$(BUILD)/cover.out;

cover_ci: cover_profile
	goveralls -coverprofile=$(BUILD)/cover.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"; \

lint:
	@lintFail=0; for file in $(ALL_SRC); do \
		golint -set_exit_status "$$file"; \
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
	rm -Rf $(BUILD)
