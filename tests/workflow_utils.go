package tests

import (
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type testEnv interface {
	T() *testing.T
	Namespace() namespace.Name
	FrontendClient() workflowservice.WorkflowServiceClient
}

func mustStartWorkflow(s testEnv, tv *testvars.TestVars) string {
	s.T().Helper()
	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(s, tv))
	if err != nil {
		s.T().Fatalf("Failed to start workflow: %v", err)
	}
	return startResp.GetRunId()
}

func startWorkflowRequest(s testEnv, tv *testvars.TestVars) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    tv.Any().String(),
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	}
}
