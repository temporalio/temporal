PROJECT_ROOT = code.uber.internal/devexp/minions

# define the list of thrift files the service depends on
# (if you have some)
THRIFT_SRCS = idl/code.uber.internal/devexp/minions/minions.thrift

# list all executables
PROGS = minions \
	cmd/stress/stress \
	cmd/demo/demo \

minions: main.go \
	$(wildcard config/*.go)  \
	$(wildcard service/*.go) \

cmd/stress/stress: cmd/stress/main.go \
        $(wildcard health/driver/*.go) \
        $(wildcard health/stress/*.go) \
				$(wildcard test/flow/*.go) \
				$(wildcard test/workflow/*.go) \
				$(wildcard common/*.go) \
				$(wildcard common/**/*.go) \
				$(wildcard workflow/*.go) \
				$(wildcard persistence/*.go) \
				$(wildcard store/*.go) \

cmd/demo/demo: cmd/demo/*.go \
				$(wildcard test/flow/*.go) \
				$(wildcard test/workflow/*.go) \
				$(wildcard common/*.go) \
				$(wildcard common/**/*.go) \
				$(wildcard workflow/*.go) \
				$(wildcard persistence/*.go) \
				$(wildcard store/*.go) \

-include go-build/rules.mk

go-build/rules.mk:
	git submodule update --init
