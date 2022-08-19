#!/bin/sh

# These mocks need to be manually fixed up after generation because gomock
# uses the types in the internal package instead of the public type aliases.
mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/client Client | \
  sed -e 's,internal,client,g' | \
  goimports > client_mock.go

mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/worker Worker | \
  sed -e 's,internal.RegisterWorkflowOptions,workflow.RegisterOptions,g' \
      -e 's,internal.RegisterActivityOptions,activity.RegisterOptions,g' \
      -e 's,internal "go.temporal.io/sdk/internal",activity "go.temporal.io/sdk/activity"\n\tworkflow "go.temporal.io/sdk/workflow",' | \
  goimports > worker_mock.go

mockgen -copyright_file ../../../LICENSE -package "$GOPACKAGE" go.temporal.io/sdk/client WorkflowRun | \
  sed -e 's,internal,client,g' | \
  goimports > workflowrun_mock.go
