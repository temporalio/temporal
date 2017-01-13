PROJECT_ROOT = code.uber.internal/devexp/minions

jenkins::
	source .jenkins/test.sh

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/code.uber.internal/devexp/minions/minions.thrift \
	idl/code.uber.internal/devexp/minions/shared.thrift \
        idl/code.uber.internal/devexp/minions/history.thrift \
        idl/code.uber.internal/devexp/minions/matching.thrift \

# list all executables
PROGS = minions

minions: main.go \
	$(wildcard config/*.go)  \
	$(wildcard service/*.go) \

-include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init

jenkins::
	.jenkins/cleanup.sh
