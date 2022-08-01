#!/bin/sh
# shellcheck disable=SC1004
# The only portable way to embed a newline in sed is with a literal
# backslash+newline, but shellcheck doesn't like it.

# These mocks need to be manually fixed up after generation because gomock
# uses the types in the internal package instead of the public type aliases.
mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/client Client | sed \
	-e 's,\<internal\>,client,g' \
	-e '/"go.temporal.io\/sdk\/converter"/d' \
	-e 's,client "go.temporal.io/sdk/client",client "go.temporal.io/sdk/client"\
	converter "go.temporal.io/sdk/converter",' \
	> client_mock.go
mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/worker Worker | sed \
	-e 's,internal.RegisterWorkflowOptions,workflow.RegisterOptions,g' \
	-e 's,internal.RegisterActivityOptions,activity.RegisterOptions,g' \
	-e 's,internal "go.temporal.io/sdk/internal",activity "go.temporal.io/sdk/activity"\
	workflow "go.temporal.io/sdk/workflow",' \
	> worker_mock.go
mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/client WorkflowRun | sed \
	-e 's,\<internal\>,client,g' \
	> workflowrun_mock.go
