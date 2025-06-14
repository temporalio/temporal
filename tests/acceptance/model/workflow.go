package model

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	Workflow struct {
		stamp.Model[*Workflow]
		stamp.Scope[*Namespace]
		//stamp.Parent[*TaskQueue]
		// TODO: stamp.Embed[*WorkflowTask] for embedding sub-modules that share the same ID

		CurRunID string
	}
)

func (w *Workflow) GetNamespace() *Namespace {
	return w.GetScope()
}

func (w *Workflow) OnStartWorkflowExecutionRequest(
	_ IncomingAction[*workflowservice.StartWorkflowExecutionRequest],
) func(OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
	return func(out OutgoingAction[*workflowservice.StartWorkflowExecutionResponse]) {
		// TODO
	}
}

func (w *Workflow) OnExecuteMultiOperationRequest(
	_ IncomingAction[*workflowservice.ExecuteMultiOperationRequest],
) func(OutgoingAction[*workflowservice.ExecuteMultiOperationResponse]) {
	// TODO
	return func(o OutgoingAction[*workflowservice.ExecuteMultiOperationResponse]) {}
}

func (w *Workflow) OnGetWorkflowHistory(
	_ IncomingAction[*workflowservice.GetWorkflowExecutionHistoryRequest],
) func(OutgoingAction[*workflowservice.GetWorkflowExecutionHistoryResponse]) {
	return func(out OutgoingAction[*workflowservice.GetWorkflowExecutionHistoryResponse]) {
		// TODO
	}
}

func (w *Workflow) Verify() {}

func (w *Workflow) Next(ctx stamp.GenContext) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{
		WorkflowId: string(w.GetID()),
		RunId:      "",
	}
}

//func (w *Workflow) DefaultWorkflowTaskTimeout2() stamp.Rule {
//	return stamp.NewRule(
//		w,
//		func(records []stamp.Record) bool {
//			for _, record := range records {
//				if record, ok := record.(WorkflowRunEventRecord); ok {
//					attributes := record.Attrs.(*historypb.WorkflowExecutionStartedEventAttributes)
//					if attributes.WorkflowTaskTimeout != nil {
//						return attributes.WorkflowTaskTimeout.AsDuration() == 10*time.Second
//					}
//					return false
//				}
//			}
//			return true
//		},
//		stamp.WithExample(true,
//			WorkflowRunEventRecord{
//				Type: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
//				Attrs: &historypb.WorkflowExecutionStartedEventAttributes{
//					WorkflowTaskTimeout: durationpb.New(10 * time.Second),
//				},
//			}),
//	)
//}
