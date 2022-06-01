#!/bin/sh

# These mocks need to be manually fixed up after generation because gomock
# uses the types in the internal package instead of the public type aliases.
mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/client Client | sed \
	-e 's,internal,client,g' \
	-e 's,client "go.temporal.io/sdk/client","go.temporal.io/sdk/client",' \
	> client_mock.go
# The only portable way to embed a newline in sed is with a literal
# backslash+newline, but shellcheck doesn't like it.
# shellcheck disable=SC1004
mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/worker Worker | sed \
	-e 's,internal.RegisterWorkflowOptions,workflow.RegisterOptions,g' \
	-e 's,internal.RegisterActivityOptions,activity.RegisterOptions,g' \
	-e 's,internal "go.temporal.io/sdk/internal",workflow "go.temporal.io/sdk/workflow"\
	activity "go.temporal.io/sdk/activity",' \
	> worker_mock.go
