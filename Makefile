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

# all directories with *_test.go files in them
TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))

thriftc: $(THRIFT_GEN_SRC)

bins: thriftc
	glide install
	go build -i -o cadence main.go

test: bins
	@rm -f test
	@rm -f test.log
	@for dir in $(TEST_DIRS); do \
		go test -coverprofile=$@ "$$dir" | tee -a test.log; \
	done;

cover_profile: clean bins
	@echo Testing packages:
	@for dir in $(TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
	done;

cover: cover_profile
	@for dir in $(TEST_DIRS); do \
		go tool cover -html=$(BUILD)/"$$dir"/coverage.out; \
	done

cover_ci: cover_profile
	@for dir in $(TEST_DIRS); do \
		goveralls -coverprofile=$(BUILD)/"$$dir"/coverage.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"; \
	done

clean:
	rm -rf .gen
	rm -rf cadence
