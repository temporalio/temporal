//go:build experimental

package frontend

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startWorkflowExperimentalCheck is called by the stable StartWorkflowExecution
// handler. Each experimental feature adds its checks here; stable code needs
// only the one call-site in workflow_handler.go.
func startWorkflowExperimentalCheck(req *workflowservice.StartWorkflowExecutionRequest) error {
	// experimental_enum_value: react to the FOO conflict policy.
	if req.GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO {
		return status.Error(codes.InvalidArgument, "WORKFLOW_ID_CONFLICT_POLICY_FOO is not yet supported")
	}

	// experimental_field (overlay): read foo_text.
	if overlay, ok, err := workflowservice.GetStartWorkflowExecutionRequestOverlay(req); err != nil {
		return err
	} else if ok {
		_ = overlay.GetFooText() // a real feature would use this
	}

	return nil
}
