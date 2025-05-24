package model

import (
	"fmt"

	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowUpdate struct {
		stamp.Model[*WorkflowUpdate]
		stamp.Scope[*WorkflowExecution]

		Request    *protocolpb.Message // TODO: race condition when accessed in generator
		acceptance *updatepb.Acceptance
		failure    *updatepb.Rejection
		response   *updatepb.Response

		Completed                 stamp.Marker `doc:"completed successfully"`
		Accepted                  stamp.Marker `doc:"accepted by the update validator on the worker"`
		Rejected                  stamp.Marker `doc:"rejected by the update validator on the worker"`
		CompletedWithAccepted     stamp.Marker `doc:"accepted *and* completed by the worker in a single WorkflowTask"`
		CompletedAfterAccepted    stamp.Marker `doc:"completed by worker in a separate WorkflowTask from the Acceptance"`
		ResurrectedFromAcceptance stamp.Marker `doc:"resurrected via Acceptance response from the worker after being lost"`
		ResurrectedFromRejection  stamp.Marker `doc:"resurrected via Rejection response from the worker after being lost"`
	}
)

func (u *WorkflowUpdate) GetWorkflow() *Workflow {
	return u.GetScope().GetScope()
}

func (u *WorkflowUpdate) OnUpdateWorkflowExecution(
	_ IncomingAction[*workflowservice.UpdateWorkflowExecutionRequest],
) func(OutgoingAction[*workflowservice.UpdateWorkflowExecutionResponse]) {
	// TODO
	return func(o OutgoingAction[*workflowservice.UpdateWorkflowExecutionResponse]) {}
}

func (u *WorkflowUpdate) OnPollWorkflowTaskQueue(
	_ IncomingAction[*workflowservice.PollWorkflowTaskQueueRequest],
) func(OutgoingAction[*workflowservice.PollWorkflowTaskQueueResponse]) {
	return func(out OutgoingAction[*workflowservice.PollWorkflowTaskQueueResponse]) {
		for _, msg := range u.ownMessages(out.Response.Messages) {
			u.Require.Nil(u.Request, "request is already set")
			u.Request = msg
		}
		u.Require.NotNil(u.Request, "request is nil")
	}
}

func (u *WorkflowUpdate) OnRespondWorkflowTaskCompleted(
	in IncomingAction[*workflowservice.RespondWorkflowTaskCompletedRequest],
) func(OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {
	for _, msg := range u.ownMessages(in.Request.Messages) {
		body, err := msg.Body.UnmarshalNew()
		if err != nil {
			// TODO: record error
			continue
		}
		switch t := body.(type) {
		case *updatepb.Acceptance:
			u.Accepted.Set(true)
			u.acceptance = t
		case *updatepb.Rejection:
			u.Rejected.Set(true)
			u.failure = t
		case *updatepb.Response:
			u.Completed.Set(true)
			u.response = t
		default:
			panic(fmt.Sprintf("unknown message type: %T", body))
		}
	}

	return func(out OutgoingAction[*workflowservice.RespondWorkflowTaskCompletedResponse]) {}
}

// - verify speculative task queue was used *when expected* (check persistence writes ...)
// - only one roundtrip to worker if there are no faults
// - server receives correct worker outcome
// - update request message has correct eventId
// - ResetHistoryEventId is zero?
// - poll after completion returns same result (and UpdateRef contains correct RunID, too)
// - on completion: history contains correct events
// - on rejection: history is reset (or not if not possible!)
// - Update is correctly processed when worker *also* sends another command
// - Update is delivered as Normal WFT when it's on the WF's first one
// - when there's a buffered event (signal), then ...

// with Alex
// - when receiving a history from a Poll, the last event must be WFTStarted with
//   a scheduledEvent attribute pointing to the previous event, which is WFTScheduled
// - Update received by worker matches requested Update: Update ID, Update Handler etc.
// - eventually you either get an error or an Outcome
// - Update response from worker matches WorkflowWorker's response data: Update ID etc.
// - when you poll after completion, you get the same outcome
// - if Update is completed, the history will end with
//   WorkflowExecutionUpdateAccepted and WorkflowExecutionUpdateCompleted,
//   where `AcceptedEventId` points to WorkflowExecutionUpdateAccepted,
//   and `AcceptedRequestSequencingEventId` points at its `WorkflowTaskScheduled`
// - if a speculative Update completes successfully, the speculative WFT is part of the
//   history
// - (need different generators for "with RunID" and "without RunID"

//u.Assert.Equal(string(id), updMsg.AcceptedRequest.Meta.UpdateId)
//u.Assert.Equal(string(id), updMsg.AcceptedRequestMessageId)
// TODO: updMsg.AcceptedRequestSequencingEventId

//u.Assert.Equal(string(id), updMsg.RejectedRequest.Meta.UpdateId)
//u.Assert.Equal(string(id), updMsg.RejectedRequestMessageId)
//u.Assert.NotZero(updMsg.Failure)
//u.Assert.NotZero(updMsg.Failure.Message)
// TODO: updMsg.RejectedRequestSequencingEventId

//u.Assert.Equal(string(id), updMsg.Meta.UpdateId)
//u.Assert.NotNil(updMsg.Outcome)

// TODO: listen for parent events; such as state change to "dead"

func (u *WorkflowUpdate) Verify() {
	// TODO
}

func (u *WorkflowUpdate) ownMessages(
	messages []*protocolpb.Message,
) []*protocolpb.Message {
	var result []*protocolpb.Message
	for _, msg := range messages {
		if string(u.GetID()) == msg.ProtocolInstanceId {
			result = append(result, msg)
		}
	}
	return result
}
