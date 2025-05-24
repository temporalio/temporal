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

		RequestID string
		Status    enumspb.WorkflowExecutionStatus
	}
)

func (w *WorkflowExecution) GetNamespace() *Namespace {
	return w.GetScope().GetNamespace()
}

func (w *WorkflowExecution) OnWorkflowStart(
	in IncomingAction[*workflowservice.StartWorkflowExecutionRequest],
) func(OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
	var newStart bool

	in.Request.RequestId = cmp.Or(in.Request.RequestId, uuid.New())
	if w.RequestID != in.Request.RequestId {
		w.RequestID = in.Request.RequestId
		w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		newStart = true
	}

	return func(out OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
		if resp := out.Response; resp != nil {
			if newStart {
				w.SetID(out.Response.RunId)
			}
		}
	}
}

func (w *WorkflowExecution) OnRespondWorkflowTerminate(
	in IncomingAction[*workflowservice.TerminateWorkflowExecutionRequest],
) {
	w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
}

func (w *WorkflowExecution) OnRespondWorkflowTaskCompleted(
	in IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) {
	for _, commands := range in.Request.Commands {
		switch commands.CommandType {
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			w.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		}
	}
}

func (w *WorkflowExecution) IsRunning() stamp.Prop[bool] {
	return stamp.NewProp(w, func(*stamp.PropContext) bool {
		return w.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
}

func (w *WorkflowExecution) IsNotRunning() stamp.Prop[bool] {
	return stamp.NewProp(w, func(*stamp.PropContext) bool {
		return w.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
}

func (w *WorkflowExecution) IsComplete() stamp.Prop[bool] {
	return stamp.NewProp(w, func(*stamp.PropContext) bool {
		return w.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	})
}

func (w *WorkflowExecution) Next(ctx stamp.GenContext) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: string(w.GetScope().GetID()),
		RunId:      "", // TODO: use current ID
	}
}
