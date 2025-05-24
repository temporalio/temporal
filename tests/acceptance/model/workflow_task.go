package model

import (
	"encoding/base64"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowTask struct {
		stamp.Model[*WorkflowTask]
		stamp.Scope[*WorkflowExecution]

		Token       string
		Speculative bool

		Polled                           stamp.Marker `doc:"polled by the worker"`
		SpeculativeWorkflowTaskLost      stamp.Marker `doc:"speculative and lost"`
		SpeculativeWorkflowTaskStale     stamp.Marker `doc:"speculative and stale"`
		SpeculativeWorkflowTaskConverted stamp.Marker `doc:"converted from speculative to a normal WFT"`
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
		w.Polled.Set(true)
	}
}

func (w *WorkflowTask) OnRespondWorkflowTaskCompleted(
	_ IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) func(OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {
	// TODO
	return func(out OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {}
}

func (w *WorkflowTask) Verify() {}
