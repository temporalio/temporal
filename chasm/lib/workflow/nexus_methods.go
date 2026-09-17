package workflow

import (
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ nexusoperation.OperationStore = (*Workflow)(nil)

// addNexusOperation adds a Nexus operation component to the workflow.
func (w *Workflow) addNexusOperation(
	ctx chasm.MutableContext,
	key int64,
	op *nexusoperation.Operation,
) {
	if w.Operations == nil {
		w.Operations = make(chasm.Map[int64, *nexusoperation.Operation])
	}
	w.Operations[key] = chasm.NewComponentField(ctx, op)
}

// removeNexusOperation removes a Nexus operation from the workflow.
func (w *Workflow) removeNexusOperation(key int64) {
	delete(w.Operations, key)
}

// pendingNexusOperationCount returns the number of pending Nexus operations in the workflow.
func (w *Workflow) pendingNexusOperationCount() int {
	return len(w.Operations)
}

// OnNexusOperationStarted adds a NexusOperationStarted history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationStarted(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
	operationToken string,
	startTime *time.Time,
	links []*commonpb.Link,
) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	_, err := addAndApplyHistoryEvent[StartedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				OperationToken:   operationToken,
				RequestId:        op.GetRequestId(),
			},
		}
		e.Links = links
		if startTime != nil {
			// For completion-before-start, use the callback-provided start time for the synthetic started event.
			e.EventTime = timestamppb.New(*startTime)
		}
	})
	return err
}

// OnNexusOperationCanceled adds a NexusOperationCanceled history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationCanceled(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
	cause *failurepb.Failure,
) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	_, err := addAndApplyHistoryEvent[CanceledEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
			NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        op.GetRequestId(),
				Failure:          createNexusOperationFailure(op, scheduledEventID, cause),
			},
		}
	})
	return err
}

// OnNexusOperationFailed adds a NexusOperationFailed history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationFailed(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
	cause *failurepb.Failure,
) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	_, err := addAndApplyHistoryEvent[FailedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
			NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        op.GetRequestId(),
				Failure:          createNexusOperationFailure(op, scheduledEventID, cause),
			},
		}
	})
	return err
}

// OnNexusOperationCompleted adds a NexusOperationCompleted history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationCompleted(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
	result *commonpb.Payload,
	links []*commonpb.Link,
) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	_, err := addAndApplyHistoryEvent[CompletedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				RequestId:        op.GetRequestId(),
				Result:           result,
			},
		}
		e.Links = links
	})
	return err
}

// OnNexusOperationTimedOut adds a NexusOperationTimedOut history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationTimedOut(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
	cause *failurepb.Failure,
	_ bool,
) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	_, err := addAndApplyHistoryEvent[TimedOutEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
			NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        op.GetRequestId(),
				Failure:          createNexusOperationFailure(op, scheduledEventID, cause),
			},
		}
	})
	return err
}

func (w *Workflow) OnNexusOperationCancellationCompleted(ctx chasm.MutableContext, op *nexusoperation.Operation) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	cancelParentData := &chasmworkflowpb.NexusCancellationParentData{}
	if err := op.Cancellation.Get(ctx).GetParentData().UnmarshalTo(cancelParentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus cancellation parent data: %v", err)
	}

	_, err := addAndApplyHistoryEvent[CancelRequestCompletedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestCompletedEventAttributes{
			NexusOperationCancelRequestCompletedEventAttributes: &historypb.NexusOperationCancelRequestCompletedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				RequestedEventId: cancelParentData.GetRequestedEventId(),
			},
		}
		// nolint:revive // We must mutate here even if the linter doesn't like it.
		e.WorkerMayIgnore = true // For compatibility with older SDKs.
	})
	return err
}

func (w *Workflow) OnNexusOperationCancellationFailed(ctx chasm.MutableContext, op *nexusoperation.Operation, failure *failurepb.Failure) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}

	cancelParentData := &chasmworkflowpb.NexusCancellationParentData{}
	if err := op.Cancellation.Get(ctx).GetParentData().UnmarshalTo(cancelParentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus cancellation parent data: %v", err)
	}

	_, err := addAndApplyHistoryEvent[CancelRequestFailedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestFailedEventAttributes{
			NexusOperationCancelRequestFailedEventAttributes: &historypb.NexusOperationCancelRequestFailedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				RequestedEventId: cancelParentData.GetRequestedEventId(),
				Failure:          failure,
			},
		}
		// nolint:revive // We must mutate here even if the linter doesn't like it.
		e.WorkerMayIgnore = true // For compatibility with older SDKs.
	})
	return err
}

// NexusOperationInvocationData loads invocation data from the scheduled history event.
func (w *Workflow) NexusOperationInvocationData(
	ctx chasm.Context,
	op *nexusoperation.Operation,
) (nexusoperation.InvocationData, error) {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return nexusoperation.InvocationData{}, serviceerror.NewInternalf(
			"failed to unmarshal nexus operation parent data: %v", err,
		)
	}

	event, err := w.LoadHistoryEvent(ctx, parentData.GetScheduledEventToken())
	if err != nil {
		return nexusoperation.InvocationData{}, err
	}

	attrs := event.GetNexusOperationScheduledEventAttributes()
	execKey := ctx.ExecutionKey()
	nsEntry := ctx.NamespaceEntry()

	nexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  nsEntry.Name().String(),
		WorkflowId: execKey.BusinessID,
		RunId:      execKey.RunID,
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   event.GetEventId(),
				EventType: event.GetEventType(),
			},
		},
	})

	return nexusoperation.InvocationData{
		Input:      attrs.GetInput(),
		Header:     attrs.GetNexusHeader(),
		NexusLinks: []nexus.Link{nexusLink},
	}, nil
}

func (w *Workflow) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// Retrieve the completion data from the underlying mutable state via MSPointer
	return w.MSPointer.GetNexusCompletion(ctx, requestID)
}

// BuildPendingNexusOperationInfos reads nexus operations from the workflow and converts them to API format.
func (w *Workflow) BuildPendingNexusOperationInfos(
	ctx chasm.Context,
	circuitBreaker func(endpoint string) bool,
) ([]*workflowpb.PendingNexusOperationInfo, error) {
	var result []*workflowpb.PendingNexusOperationInfo
	for key, field := range w.Operations {
		op := field.Get(ctx)

		if op.GetStatus() == nexusoperationpb.OPERATION_STATUS_UNSPECIFIED {
			return nil, serviceerror.NewInternal("Nexus operation with UNSPECIFIED state")
		}

		state := nexusoperation.PendingOperationState(op.GetStatus())
		if state == enumspb.PENDING_NEXUS_OPERATION_STATE_UNSPECIFIED {
			// Operation is not pending.
			continue
		}

		blockedReason := ""
		if state == enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED && circuitBreaker(op.GetEndpoint()) {
			state = enumspb.PENDING_NEXUS_OPERATION_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}

		info := &workflowpb.PendingNexusOperationInfo{
			Endpoint:                op.GetEndpoint(),
			Service:                 op.GetService(),
			Operation:               op.GetOperation(),
			OperationId:             op.GetOperationToken(),
			OperationToken:          op.GetOperationToken(),
			ScheduledEventId:        key,
			ScheduleToCloseTimeout:  op.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout:  op.GetScheduleToStartTimeout(),
			StartToCloseTimeout:     op.GetStartToCloseTimeout(),
			ScheduledTime:           op.GetScheduledTime(),
			State:                   state,
			Attempt:                 op.GetAttempt(),
			LastAttemptCompleteTime: op.GetLastAttemptCompleteTime(),
			LastAttemptFailure:      op.GetLastAttemptFailure(),
			NextAttemptScheduleTime: op.GetNextAttemptScheduleTime(),
			BlockedReason:           blockedReason,
		}

		if cancel, ok := op.Cancellation.TryGet(ctx); ok {
			state := nexusoperation.CancellationAPIState(cancel.Status)
			blockedReason := ""

			if state == enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED && circuitBreaker(info.Endpoint) {
				state = enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
				blockedReason = "The circuit breaker is open."
			}

			info.CancellationInfo = &workflowpb.NexusOperationCancellationInfo{
				RequestedTime:           cancel.RequestedTime,
				State:                   state,
				Attempt:                 cancel.Attempt,
				LastAttemptCompleteTime: cancel.LastAttemptCompleteTime,
				LastAttemptFailure:      cancel.LastAttemptFailure,
				NextAttemptScheduleTime: cancel.NextAttemptScheduleTime,
				BlockedReason:           blockedReason,
			}
		}

		result = append(result, info)
	}
	return result, nil
}

// createNexusOperationFailure creates a NexusOperationExecutionFailure wrapping the given cause.
func createNexusOperationFailure(op *nexusoperation.Operation, scheduledEventID int64, cause *failurepb.Failure) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "nexus operation completed unsuccessfully",
		FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
			NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
				Endpoint:         op.GetEndpoint(),
				Service:          op.GetService(),
				Operation:        op.GetOperation(),
				OperationToken:   op.GetOperationToken(),
				ScheduledEventId: scheduledEventID,
			},
		},
		Cause: cause,
	}
}
