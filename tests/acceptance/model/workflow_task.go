package model

import (
	"encoding/base64"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowTask struct {
		stamp.Model[*WorkflowTask]
		stamp.Scope[*WorkflowExecution]

		Token       string
		Speculative bool
		Poll        *workflowservice.PollWorkflowTaskQueueResponse
		Response    *workflowservice.RespondWorkflowTaskCompletedRequest

		Polled                           stamp.Marker `doc:"polled by the worker"`
		SuggestedContinueAsNew           stamp.Marker `doc:"suggested continue-as-new"`
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
		switch {
		case out.ResponseErr == nil:
			w.Poll = out.Response
			w.Token = base64.StdEncoding.EncodeToString(out.Response.TaskToken)
			w.Polled.Set(true)

			if events := out.Response.History.Events; len(events) > 0 {
				lastEvent := events[len(events)-1]
				lastEventAttrs := lastEvent.Attributes.(*historypb.HistoryEvent_WorkflowTaskStartedEventAttributes).WorkflowTaskStartedEventAttributes
				w.SuggestedContinueAsNew.Set(lastEventAttrs.SuggestContinueAsNew)
			}
		default:
			panic("unhandled OnPollWorkflowTaskQueue error: " + out.ResponseErr.Error())
		}
	}
}

func (w *WorkflowTask) OnRespondWorkflowTaskCompleted(
	req IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) func(OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {
	w.Response = req.Request
	return func(out OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {
		switch {
		case out.ResponseErr == nil:
			if newWFT := out.Response.WorkflowTask; newWFT != nil {
				// TODO: instead we should return a new WorkflowTask model instance
				w.Token = base64.StdEncoding.EncodeToString(newWFT.TaskToken)
			}
		default:
			panic("unhandled OnRespondWorkflowTaskCompleted error: " + out.ResponseErr.Error())
		}
	}
}

func (w *WorkflowTask) Verify() {}
