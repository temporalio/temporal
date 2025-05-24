package action

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type UpdateWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTargetAsync[*model.WorkflowUpdate]
	WorkflowExecution *model.WorkflowExecution `validate:"required"`
	WaitStage         stamp.Gen[WorkflowUpdateWaitStage]
	UpdateID          stamp.Gen[stamp.ID]
	UpdateHandler     stamp.Gen[stamp.ID]
	Payload           Payloads
	Header            Header
}

func (w UpdateWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.UpdateWorkflowExecutionRequest {
	return &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         string(w.WorkflowExecution.GetNamespace().GetID()),
		WorkflowExecution: w.WorkflowExecution.Next(ctx),
		WaitPolicy: &updatepb.WaitPolicy{
			LifecycleStage: enumspb.UpdateWorkflowExecutionLifecycleStage(w.WaitStage.Next(ctx)),
		},
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{
				UpdateId: string(w.UpdateID.Next(ctx.AllowRandom())),
				Identity: string(w.GetActor().GetID()),
			},
			Input: &updatepb.Input{
				Name:   string(w.UpdateHandler.Next(ctx.AllowRandom())),
				Header: w.Header.Next(ctx),
				Args:   w.Payload.Next(ctx),
			},
		},
	}
}

type WorkflowUpdateRequest *updatepb.Request

type PollWorkflowExecutionUpdate struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTargetAsync[*model.WorkflowUpdate]
	Identity       stamp.Gen[string] // defaults to actor's identity
	WorkflowUpdate *model.WorkflowUpdate
	WaitStage      stamp.Gen[enumspb.UpdateWorkflowExecutionLifecycleStage]
}

func (w PollWorkflowExecutionUpdate) Next(ctx stamp.GenContext) *workflowservice.PollWorkflowExecutionUpdateRequest {
	wfe := w.WorkflowUpdate.GetScope()
	return &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace: string(wfe.GetNamespace().GetID()),
		UpdateRef: &updatepb.UpdateRef{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: string(wfe.GetScope().GetID()),
				RunId:      string(wfe.GetID()),
			},
			UpdateId: string(w.WorkflowUpdate.GetID()),
		},
		Identity: w.Identity.NextOrDefault(ctx, string(w.GetActor().GetID())),
		WaitPolicy: &updatepb.WaitPolicy{
			LifecycleStage: w.WaitStage.Next(ctx),
		},
	}
}

type ExecuteUpdateWithStart struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowUpdate]
	StartWorkflowUpdate UpdateWorkflowExecution
	StartWorkflow       StartWorkflowExecution
	TaskQueue           *model.TaskQueue
}

func (w ExecuteUpdateWithStart) Next(ctx stamp.GenContext) *workflowservice.ExecuteMultiOperationRequest {
	return &workflowservice.ExecuteMultiOperationRequest{
		Namespace: string(w.TaskQueue.GetNamespace().GetID()),
		//Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
		//	{
		//
		//	},
		//}
	}
}

//	resp, err := c.workflowServiceClient.ExecuteMultiOperation(
//		NewContext(),
//		&workflowservice.ExecuteMultiOperationRequest{
//			Namespace: NamespaceName.Get(ns.Get(taskQueue.Get(wf))).String(),
//			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
//				{
//					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
//						StartWorkflowExecution: startWorkflowRequest(wf.ModelType),
//					},
//				},
//				{
//					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
//						UpdateWorkflow: startUpdateRequest(upd.ModelType),
//					},
//				},
//			},
//		})

type WorkflowUpdateWaitStage enumspb.UpdateWorkflowExecutionLifecycleStage

func (_ WorkflowUpdateWaitStage) DefaultGen() stamp.Gen[WorkflowUpdateWaitStage] {
	return stamp.GenJust(WorkflowUpdateWaitStage(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED))
}
