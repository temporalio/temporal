package scheduler_test

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/mock/gomock"
)

func TestKnownIssue_TransientCancelFailureRemainsPending(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	target := &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}
	env.mockHistoryClient.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewDeadlineExceeded("ambiguous cancel result"))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialCancelWorkflows:  []*commonpb.WorkflowExecution{target},
		ExpectedCancelWorkflows: 1,
	})
}

func TestKnownIssue_TransientTerminateFailureRemainsPending(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	target := &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}
	env.mockHistoryClient.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewDeadlineExceeded("ambiguous terminate result"))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialTerminateWorkflows:  []*commonpb.WorkflowExecution{target},
		ExpectedTerminateWorkflows: 1,
	})
}
