package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"google.golang.org/grpc/codes"
)

// schedulerRPCProfiles describes outcomes the scheduler can distinguish at its
// service boundaries. Request matching remains the responsibility of callers.
type schedulerRPCProfiles struct{}

func (schedulerRPCProfiles) startSucceeded() rpcgen.Behavior[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse] {
	return rpcgen.Derived("started", func(request *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-" + request.GetRequestId()}
	})
}

func (schedulerRPCProfiles) startRetryable() rpcgen.Behavior[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse] {
	return rpcgen.Retryable[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) startRetryableWithCode(code codes.Code) rpcgen.Behavior[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse] {
	return rpcgen.Retryable[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse](code)
}

func (schedulerRPCProfiles) startAmbiguousCommit() rpcgen.Behavior[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse] {
	return rpcgen.AmbiguousCommit[*workflowservice.StartWorkflowExecutionRequest](&workflowservice.StartWorkflowExecutionResponse{RunId: "committed-run"})
}

func (schedulerRPCProfiles) describeRunning() rpcgen.Behavior[*historyservice.DescribeWorkflowExecutionRequest, *historyservice.DescribeWorkflowExecutionResponse] {
	return rpcgen.Derived("running", func(request *historyservice.DescribeWorkflowExecutionRequest) *historyservice.DescribeWorkflowExecutionResponse {
		return &historyservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: request.GetRequest().GetExecution(), Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}}
	})
}

func (schedulerRPCProfiles) describeCompleted() rpcgen.Behavior[*historyservice.DescribeWorkflowExecutionRequest, *historyservice.DescribeWorkflowExecutionResponse] {
	return rpcgen.Derived("completed", func(request *historyservice.DescribeWorkflowExecutionRequest) *historyservice.DescribeWorkflowExecutionResponse {
		return &historyservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: request.GetRequest().GetExecution(), Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}}
	})
}

func (schedulerRPCProfiles) cancelAccepted() rpcgen.Behavior[*historyservice.RequestCancelWorkflowExecutionRequest, *historyservice.RequestCancelWorkflowExecutionResponse] {
	return rpcgen.Success[*historyservice.RequestCancelWorkflowExecutionRequest](&historyservice.RequestCancelWorkflowExecutionResponse{})
}

func (schedulerRPCProfiles) terminateAccepted() rpcgen.Behavior[*historyservice.TerminateWorkflowExecutionRequest, *historyservice.TerminateWorkflowExecutionResponse] {
	return rpcgen.Success[*historyservice.TerminateWorkflowExecutionRequest](&historyservice.TerminateWorkflowExecutionResponse{})
}

func (schedulerRPCProfiles) cancelRetryable() rpcgen.Behavior[*historyservice.RequestCancelWorkflowExecutionRequest, *historyservice.RequestCancelWorkflowExecutionResponse] {
	return rpcgen.Retryable[*historyservice.RequestCancelWorkflowExecutionRequest, *historyservice.RequestCancelWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) terminateRetryable() rpcgen.Behavior[*historyservice.TerminateWorkflowExecutionRequest, *historyservice.TerminateWorkflowExecutionResponse] {
	return rpcgen.Retryable[*historyservice.TerminateWorkflowExecutionRequest, *historyservice.TerminateWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) migrationStarted() rpcgen.Behavior[*historyservice.StartWorkflowExecutionRequest, *historyservice.StartWorkflowExecutionResponse] {
	return rpcgen.Derived("migration-started", func(request *historyservice.StartWorkflowExecutionRequest) *historyservice.StartWorkflowExecutionResponse {
		return &historyservice.StartWorkflowExecutionResponse{RunId: "migration-" + request.GetStartRequest().GetRequestId()}
	})
}

func (schedulerRPCProfiles) migrationRetryable() rpcgen.Behavior[*historyservice.StartWorkflowExecutionRequest, *historyservice.StartWorkflowExecutionResponse] {
	return rpcgen.Retryable[*historyservice.StartWorkflowExecutionRequest, *historyservice.StartWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) migrationTerminal() rpcgen.Behavior[*historyservice.StartWorkflowExecutionRequest, *historyservice.StartWorkflowExecutionResponse] {
	return rpcgen.Behavior[*historyservice.StartWorkflowExecutionRequest, *historyservice.StartWorkflowExecutionResponse]{
		Label: "terminal-invalid-argument", Err: serviceerror.NewInvalidArgument("injected terminal failure"),
	}
}

func TestSchedulerRPCProfilesUseExplicitScriptExpectations(t *testing.T) {
	var contract rpctest.RPCContract
	schedulerRPCProfiles{}.startRetryable().Expect(&contract, "StartWorkflowExecution", "retry", func(request *workflowservice.StartWorkflowExecutionRequest) bool {
		return request.GetRequestId() == "retry"
	})
	_, err := contract.Invoke(t.Context(), "StartWorkflowExecution", &workflowservice.StartWorkflowExecutionRequest{RequestId: "retry"})
	require.Error(t, err)
	require.NoError(t, contract.AssertSatisfied())
}
