PROJECT_ROOT = github.com/uber/cadence

jenkins::
	source .jenkins/test.sh

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/github.com/uber/cadence/cadence.thrift \
	idl/github.com/uber/cadence/shared.thrift \
  idl/github.com/uber/cadence/history.thrift \
  idl/github.com/uber/cadence/matching.thrift \

# list all executables
PROGS = cadence

cadence: main.go \
	$(wildcard config/*.go)  \
	$(wildcard service/*.go) \

-include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init

jenkins::
	.jenkins/cleanup.sh
