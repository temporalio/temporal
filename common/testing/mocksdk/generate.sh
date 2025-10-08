#!/bin/sh

# These mocks need to be manually fixed up after generation because gomock
# uses the types in the internal package instead of the public type aliases.

mockgen -package "$GOPACKAGE" go.temporal.io/sdk/client Client | \
  sed -e 's,internal,client,g' \
  > client_mock.go

mockgen -package "$GOPACKAGE" go.temporal.io/sdk/worker Worker | \
  sed -e 's,internal.RegisterWorkflowOptions,workflow.RegisterOptions,g' \
      -e 's,internal.RegisterActivityOptions,activity.RegisterOptions,g' \
      -e 's,internal.DynamicRegisterWorkflowOptions,workflow.DynamicRegisterOptions,g' \
      -e 's,internal.DynamicRegisterActivityOptions,activity.DynamicRegisterOptions,g' \
      -e 's,internal "go.temporal.io/sdk/internal",activity "go.temporal.io/sdk/activity"\n\tworkflow "go.temporal.io/sdk/workflow",' \
  > worker_mock.go

mockgen -package "$GOPACKAGE" go.temporal.io/sdk/client WorkflowRun | \
  sed -e 's,internal,client,g' \
  > workflowrun_mock.go
