package action

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

var (
	UpdateWaitStages = stamp.GenEnum("UpdateWaitStages",
		enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED,
		enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
		enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
	)
)

type UpdateWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowUpdate]
	stamp.ActionStartable
	WorkflowExecution stamp.Arbitrary[*commonpb.WorkflowExecution]
	WaitStage         stamp.Gen[enumspb.UpdateWorkflowExecutionLifecycleStage]
	UpdateID          stamp.Gen[stamp.ID]
	UpdateHandler     stamp.Gen[stamp.ID]
	Payload           Payloads
	Header            Header
}

func (w UpdateWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.UpdateWorkflowExecutionRequest {
	workflowExecution := w.WorkflowExecution.Next(ctx)
	return &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         string(w.GetActor().GetScope().GetNamespace().GetID()),
		WorkflowExecution: workflowExecution,
		WaitPolicy: &updatepb.WaitPolicy{
			LifecycleStage: w.WaitStage.NextOrDefault(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED),
		},
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{
				UpdateId: "upd-" + string(w.UpdateID.Next(ctx.AllowRandom())),
				Identity: string(w.GetActor().GetID()),
			},
			Input: &updatepb.Input{
				Name:   "upd-handler-" + string(w.UpdateHandler.Next(ctx.AllowRandom())),
				Header: w.Header.Next(ctx),
				Args:   w.Payload.Next(ctx),
			},
		},
	}
}

type WorkflowUpdateRequest *updatepb.Request

type PollWorkflowExecutionUpdate struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowUpdate]
	stamp.ActionStartable
	Identity       stamp.Gen[string] // defaults to actor's identity
	WorkflowUpdate *model.WorkflowUpdate
	WaitStage      stamp.Gen[enumspb.UpdateWorkflowExecutionLifecycleStage]
}

func (w PollWorkflowExecutionUpdate) Next(ctx stamp.GenContext) *workflowservice.PollWorkflowExecutionUpdateRequest {
	wfe := w.WorkflowUpdate.GetScope()
	return &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace: string(wfe.GetNamespace().GetID()),
		UpdateRef: &updatepb.UpdateRef{
			WorkflowExecution: wfe.Next(ctx),
			UpdateId:          string(w.WorkflowUpdate.GetID()),
		},
		Identity: w.Identity.NextOrDefault(ctx, string(w.GetActor().GetID())),
		WaitPolicy: &updatepb.WaitPolicy{
			LifecycleStage: w.WaitStage.NextOrDefault(ctx, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED),
		},
	}
}

type UpdateWithStart struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowUpdate]
	stamp.ActionStartable
	UpdateWorkflow UpdateWorkflowExecution
	StartWorkflow  StartWorkflowExecution
}

func (w UpdateWithStart) Next(ctx stamp.GenContext) *workflowservice.ExecuteMultiOperationRequest {
	w.StartWorkflow.ActionActor = w.ActionActor // TODO: do this automatically
	startWorkflow := w.StartWorkflow.Next(ctx)

	w.UpdateWorkflow.ActionActor = w.ActionActor
	if w.UpdateWorkflow.WorkflowExecution == nil {
		w.UpdateWorkflow.WorkflowExecution = WorkflowRef{
			WorkflowID: stamp.ID(startWorkflow.WorkflowId),
		}
	}
	startUpdateWorkflow := w.UpdateWorkflow.Next(ctx)

	return &workflowservice.ExecuteMultiOperationRequest{
		Namespace: string(w.GetActor().GetScope().GetNamespace().GetID()),
		Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: startWorkflow,
				},
			},
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: startUpdateWorkflow,
				},
			},
		},
	}
}
