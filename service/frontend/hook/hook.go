// Package hook defines extension points that stable frontend code exposes
// for experimental features to inject behaviour without modifying stable files.
//
// Stable code calls the hook; experimental code (//go:build experimental)
// replaces the default no-op in its init(). Because this package is separate
// from both frontend and frontend/services, there is no import cycle.
package hook

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
)

// StartWorkflow is called by the stable StartWorkflowExecution handler before
// any processing. The default is a no-op. An experimental feature replaces it
// in an //go:build experimental init() to inject behaviour without touching
// workflow_handler.go.
var StartWorkflow StartWorkflowHook = noopStartWorkflowHook{}

type StartWorkflowHook interface {
	PreStart(ctx context.Context, req *workflowservice.StartWorkflowExecutionRequest) error
}

type noopStartWorkflowHook struct{}

func (noopStartWorkflowHook) PreStart(_ context.Context, _ *workflowservice.StartWorkflowExecutionRequest) error {
	return nil
}
