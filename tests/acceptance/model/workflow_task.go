package model

import (
	"encoding/base64"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

var (
// SpeculativeWorkflowTaskLost = testmarker.New(
//
//	"WorkflowTask.Speculative.Lost",
//	"Speculative Workflow Task was lost.")
//
// SpeculativeWorkflowTaskStale = testmarker.New(
//
//	"WorkflowTask.Speculative.Stale",
//	"Speculative Workflow Task was stale.")
//
// SpeculativeWorkflowTaskConverted = testmarker.New(
//
//	"WorkflowTask.Speculative.Converted",
//	"Speculative Workflow Task was converted to a normal Workflow Task.")
)

type (
	WorkflowTask struct {
		stamp.Model[*WorkflowTask]
		stamp.Scope[*WorkflowExecution]

		Token       string
		Speculative bool
	}
)

func (w *WorkflowTask) GetWorkflow() *Workflow {
	return w.GetScope().GetScope()
}

func (w *WorkflowTask) GetNamespace() *Namespace {
	return w.GetScope().GetNamespace()
}

func (w *WorkflowTask) OnPollWorkflowTaskQueue(
	_ IncomingAction[*workflowservice.PollWorkflowTaskQueueRequest],
) func(OutgoingAction[*workflowservice.PollWorkflowTaskQueueResponse]) {
	return func(out OutgoingAction[*workflowservice.PollWorkflowTaskQueueResponse]) {
		w.Token = base64.StdEncoding.EncodeToString(out.Response.TaskToken)
	}
}

func (w *WorkflowTask) OnRespondWorkflowTaskCompleted(
	_ IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) {
	// TODO
}

func (w *WorkflowTask) WasPolled() stamp.Prop[bool] {
	return stamp.NewProp(w, func(*stamp.PropContext) bool {
		return w.Token != ""
	})
}
