PROJECT_ROOT = code.uber.internal/devexp/minions

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/code.uber.internal/devexp/minions/minions.thrift

# list all executables
PROGS = minions

minions: main.go \
	$(wildcard config/*.go)  \
	$(wildcard service/*.go) \

-include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init
