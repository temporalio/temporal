//go:build experimental

package services

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/service/frontend/hook"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── Pattern 1: frontend wrapper ───────────────────────────────────────────────
//
// exampleHandler adds new RPCs (experimental_method, experimental_message).
// The stable WorkflowServiceServer is untouched.

type exampleHandler struct{}

func (exampleHandler) Echo(_ context.Context, req *workflowservice.EchoRequest) (*workflowservice.EchoResponse, error) {
	return &workflowservice.EchoResponse{Payload: req.GetPayload()}, nil
}

// exampleWorkflowWrapper intercepts existing stable RPCs to add experimental
// behaviour before delegating to the stable handler. Embedding the interface
// means every other method passes through untouched.
//
// This is the right pattern when the experimental logic lives at the API
// boundary (frontend). For behaviour deeper in the stack, see Pattern 2.
type exampleWorkflowWrapper struct {
	workflowservice.WorkflowServiceServer
}

func (w *exampleWorkflowWrapper) StartWorkflowExecution(
	ctx context.Context,
	req *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	// experimental_enum_value: react to the FOO conflict policy.
	if req.GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO {
		return nil, status.Error(codes.InvalidArgument, "WORKFLOW_ID_CONFLICT_POLICY_FOO is not yet supported")
	}

	// experimental_field (overlay): read foo_text from the request overlay.
	if overlay, ok, err := workflowservice.GetStartWorkflowExecutionRequestOverlay(req); err != nil {
		return nil, err
	} else if ok && overlay.GetFooText() != "" {
		// A real feature would use overlay.GetFooText() to influence behaviour.
		_ = overlay.GetFooText()
	}

	return w.WorkflowServiceServer.StartWorkflowExecution(ctx, req)
}

// ── Pattern 2: hook interface ─────────────────────────────────────────────────
//
// For experimental behaviour that needs to run inside an existing stable
// handler (e.g. deep in history), stable code defines a hook interface with a
// no-op default. The experimental feature replaces it here without touching
// the stable file.
//
// Stable code calls the hook like this (no build tag required):
//
//	if err := hook.StartWorkflow.PreStart(ctx, req); err != nil {
//	    return nil, err
//	}
//
// Because the hook variable lives in a stable file and defaults to no-op,
// stable builds are completely unaffected.

type exampleStartWorkflowHook struct{}

func (exampleStartWorkflowHook) PreStart(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest) error {
	// Runs inside the stable StartWorkflowExecution handler before any
	// stable processing. Safe to read experimental fields here because
	// this file is //go:build experimental.
	if _, ok, err := workflowservice.GetStartWorkflowExecutionRequestOverlay(req); err != nil {
		return err
	} else if ok {
		// feature logic here
	}
	return nil
}

func init() {
	// Register the hook so stable code picks it up at startup.
	hook.StartWorkflow = exampleStartWorkflowHook{}

	register("example", variant{registerWorkflow: func(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
		wrapped := &exampleWorkflowWrapper{workflow}
		registerServiceOverlay(server, workflowservice.WorkflowService_ServiceDesc, wrapped, workflowservice.ExampleWorkflowService_ServiceDesc, exampleHandler{})
	}})
}
