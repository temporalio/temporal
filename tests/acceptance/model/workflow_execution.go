package model

import (
	"cmp"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowExecution struct {
		stamp.Model[*WorkflowExecution]
		stamp.Scope[*Workflow]

		Request   *workflowservice.StartWorkflowExecutionRequest
		RequestID string
		Status    enumspb.WorkflowExecutionStatus

		IsRunning  stamp.Marker
		IsComplete stamp.Marker
	}
)

func (w *WorkflowExecution) GetNamespace() *Namespace {
	return w.GetScope().GetNamespace()
}

func (w *WorkflowExecution) OnStartWorkflowExecution(
	in IncomingAction[*workflowservice.StartWorkflowExecutionRequest],
) func(OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
	w.Request = in.Request

	switch {
	case in.Request.WorkflowIdReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING &&
		in.Request.WorkflowIdConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED:
		in.AddValidationError("Invalid WorkflowIDReusePolicy")
	}

	var newStart bool
	in.Request.RequestId = cmp.Or(in.Request.RequestId, uuid.New())
	if w.RequestID != in.Request.RequestId {
		w.RequestID = in.Request.RequestId
		w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		newStart = true
	}

	return func(out OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
		switch {
		case out.ResponseErr == nil:
			if newStart {
				w.SetID(out.Response.RunId)
			}
		}
	}
}

func (w *WorkflowExecution) OnExecuteMultiOperationRequest(
	_ IncomingAction[*workflowservice.ExecuteMultiOperationRequest],
) func(OutgoingAction[*workflowservice.ExecuteMultiOperationResponse]) {
	// TODO
	return func(o OutgoingAction[*workflowservice.ExecuteMultiOperationResponse]) {}
}

func (w *WorkflowExecution) OnDescribeWorkflowExecution(
	_ IncomingAction[*workflowservice.DescribeWorkflowExecutionRequest],
) func(out OutgoingAction[*workflowservice.DescribeWorkflowExecutionResponse]) {
	return func(out OutgoingAction[*workflowservice.DescribeWorkflowExecutionResponse]) {
		// TODO
	}
}

func (w *WorkflowExecution) OnTerminateWorkflowExecution(
	_ IncomingAction[*workflowservice.TerminateWorkflowExecutionRequest],
) func(out OutgoingAction[*workflowservice.TerminateWorkflowExecutionResponse]) {
	return func(out OutgoingAction[*workflowservice.TerminateWorkflowExecutionResponse]) {
		switch {
		case out.ResponseErr == nil:
			w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		}
	}
}

func (w *WorkflowExecution) OnRespondWorkflowTaskCompleted(
	in IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) func(out OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {
	for _, commands := range in.Request.Commands {
		switch commands.CommandType {
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		}
	}
	return func(out OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {}
}

func (w *WorkflowExecution) Verify() {
	w.IsRunning.Set(w.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	w.IsComplete.Set(w.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

func (w *WorkflowExecution) Next(ctx stamp.GenContext) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: string(w.GetScope().GetID()),
		RunId:      "", // TODO: use current ID
	}
}
