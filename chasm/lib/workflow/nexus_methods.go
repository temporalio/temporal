package workflow

import (
	"errors"
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

// removeNexusOperationIfTerminal drops an operation whose cancel has resolved if it was already
// terminal (kept resident only to deliver the cancel). A still-"started" op is left as-is.
func (w *Workflow) removeNexusOperationIfTerminal(ctx chasm.MutableContext, op *nexusoperation.Operation) error {
	if op.LifecycleState(ctx) == chasm.LifecycleStateRunning {
		return nil
	}
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}
	w.removeNexusOperation(parentData.GetScheduledEventId())
	return nil
}

func (w *Workflow) OnNexusOperationCancellationCompleted(ctx chasm.MutableContext, op *nexusoperation.Operation) error {
	if !w.IsRunning() {
		// Workflow already closed: resolve the cancellation and drop an already-terminal op, but skip
		// the history event (sealed). A still-"started" op stays "started".
		if err := nexusoperation.TransitionCancellationSucceeded.Apply(op.Cancellation.Get(ctx), ctx, nexusoperation.EventCancellationSucceeded{}); err != nil {
			return err
		}
		return w.removeNexusOperationIfTerminal(ctx, op)
	}
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
	if !w.IsRunning() {
		// Workflow already closed: record the failed cancellation and drop an already-terminal op, but
		// skip the history event (sealed). A still-"started" op stays "started".
		if err := nexusoperation.TransitionCancellationFailed.Apply(op.Cancellation.Get(ctx), ctx, nexusoperation.EventCancellationFailed{Failure: failure}); err != nil {
			return err
		}
		return w.removeNexusOperationIfTerminal(ctx, op)
	}
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

// RequestCancelPendingNexusOperations requests cancellation of every STARTED async Nexus
// operation owned by the workflow, recording a NexusOperationCancelRequested event for each so
// the cancellation is visible in the caller's history. It is intended to be called from
// workflow-close paths (Nexus auto-close policy) while mutable state is still writable, i.e.
// before the workflow close event is added.
//
// Semantics worth noting:
//   - Only STARTED operations are cancelled — they are the only ones with a running handler (and a
//     token) to notify. Not-yet-started operations (SCHEDULED/BACKING_OFF) are skipped: once the
//     workflow closes their start task is dropped (the Operation, unlike Cancellation, is not
//     detached), so they can never reach STARTED, and requesting a cancel would only record an
//     undeliverable event. A start racing the close (in-flight when the workflow closes) is likewise
//     abandoned — the caller never recorded a token to cancel with.
//   - The Nexus protocol's CancelOperation carries no reason/identity, so the cause of the
//     cancellation is only recorded caller-side (via the event added here), not forwarded to
//     the handler.
//
// Must be called from the pre-close hooks (before the close event is added) so the recorded
// NexusOperationCancelRequested events land in the caller's history ahead of the close event.
//
// The auto-close distinction is event-sourced: the NexusOperationCancelRequested event is stamped with
// the system principal, which the event's Apply copies onto the cancellation. This keeps it correct
// when a reset rebuilds a still-pending cancellation from history.
//
// CONSIDER(stephanos): this fans out inline within the workflow-close transaction, one event +
// cancellation component per pending operation. The count is bounded by
// nexusoperation.MaxConcurrentOperationsPerWorkflow (default 2000), so it is not unbounded, but a
// close transaction carrying up to that many events is still heavy. If that becomes a problem,
// mirror the child-workflow parent-close design and offload large fan-outs to the system worker pool.
func (w *Workflow) RequestCancelPendingNexusOperations(ctx chasm.MutableContext) error {
	for scheduledEventID, field := range w.Operations {
		op := field.Get(ctx)
		// Only STARTED operations have a running handler (and a token) to cancel; skip the rest.
		if op.GetStatus() != nexusoperationpb.OPERATION_STATUS_STARTED {
			continue
		}
		if err := w.requestAutoCloseCancel(ctx, scheduledEventID, op); err != nil {
			return err
		}
	}
	return nil
}

// OnWorkflowReset applies the Nexus auto-close policy to a run that a workflow reset is superseding.
//
// A reset is a force-close for exactly the operations the reset run does not adopt, and a no-op for
// the ones it does. Adoption is decided by the reset point alone: the reset run is rebuilt by
// replaying history up to adoptedThroughEventID, so an operation whose NexusOperationScheduled event
// falls at or before it comes back on the new run (same request ID, same operation token, and the
// handler's callback token still resolves to the new run). NexusOperationScheduled is never
// cherry-pickable, so an operation scheduled after the reset point is dropped for good — its handler
// has no caller left, which is precisely what the policy exists to clean up.
//
// Two things follow, and both are needed:
//
//   - Dropped and STARTED (scheduledEventID > adoptedThroughEventID): request the cancel, exactly as
//     any other force-close would. Reset's own termination goes through workflow.TerminateWorkflow
//     rather than the terminate API, so no other close hook covers this run.
//   - Adopted with a pending system-initiated cancellation: abort it. Such a cancellation only
//     exists because the caller was force-closed earlier (terminate-then-reset is the common shape),
//     and it is detached, so it keeps retrying on the closed run long after the reset. Left running
//     it would cancel the handler that the reset run has just re-adopted. A user-initiated
//     cancellation needs no handling — it is attached and stops with the run.
//
// Pass adoptedThroughEventID = 0 for a run that is not the reset's base run: the reset run is
// rebuilt from the base branch, so none of its operations are adopted.
//
// Must be called while mutable state is still writable and before the close event is added. On an
// already-closed run only the abort half runs; there is no history to append to, and that run's own
// close hook already applied the policy.
func (w *Workflow) OnWorkflowReset(ctx chasm.MutableContext, adoptedThroughEventID int64, policyRequestCancel bool) error {
	for scheduledEventID, field := range w.Operations {
		op := field.Get(ctx)
		if scheduledEventID <= adoptedThroughEventID {
			if _, err := op.AbortAutoCloseCancellation(ctx); err != nil {
				return err
			}
			continue
		}
		// Dropped by the reset. Same rule as any force-close: only STARTED operations have a handler
		// (and a token) to cancel.
		if !policyRequestCancel || !w.IsRunning() {
			continue
		}
		if op.GetStatus() != nexusoperationpb.OPERATION_STATUS_STARTED {
			continue
		}
		if err := w.requestAutoCloseCancel(ctx, scheduledEventID, op); err != nil {
			return err
		}
	}
	return nil
}

// NeedsResetHandling reports whether OnWorkflowReset would do anything, using a read-only context.
// Callers must probe with this first: a reset walks runs it does not otherwise touch (the
// continue-as-new chain), and taking a mutable context on one of those leaves its mutable state
// dirty for a transaction that never commits it.
func (w *Workflow) NeedsResetHandling(ctx chasm.Context, adoptedThroughEventID int64, policyRequestCancel bool) bool {
	for scheduledEventID, field := range w.Operations {
		op := field.Get(ctx)
		if scheduledEventID <= adoptedThroughEventID {
			if cancellation, ok := op.Cancellation.TryGet(ctx); ok && nexusoperation.IsAbortableAutoCloseCancellation(cancellation) {
				return true
			}
			continue
		}
		if policyRequestCancel && w.IsRunning() && op.GetStatus() == nexusoperationpb.OPERATION_STATUS_STARTED {
			return true
		}
	}
	return false
}

// OnNexusOperationAutoCloseCancelRequested event-sources an auto-close cancel request for a single
// operation. Used by the operation's own schedule-to-close timeout, where the operation (not the
// workflow) is closing while the caller keeps running.
func (w *Workflow) OnNexusOperationAutoCloseCancelRequested(ctx chasm.MutableContext, op *nexusoperation.Operation) error {
	parentData := &chasmworkflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewInternalf("failed to unmarshal nexus operation parent data: %v", err)
	}
	return w.requestAutoCloseCancel(ctx, parentData.GetScheduledEventId(), op)
}

// requestAutoCloseCancel records a NexusOperationCancelRequested event stamped with the system
// principal; the event's Apply reads it to flag the cancellation as auto-close. Event-sourcing the
// flag (vs. a post-hoc component write) keeps it correct across reset. No-op if one already exists.
func (w *Workflow) requestAutoCloseCancel(ctx chasm.MutableContext, scheduledEventID int64, op *nexusoperation.Operation) error {
	if existing, ok := op.Cancellation.TryGet(ctx); ok {
		if nexusoperation.IsSystemCancellation(existing) {
			return nil // already system-initiated; nothing to supersede
		}
		// A pending user-initiated cancellation is attached and would be aborted along with the closing
		// run. Abort it explicitly first — event-sourced, so a reset rebuilds the same sequence — which
		// leaves it terminal and lets the system-initiated request below supersede it.
		if err := w.OnNexusOperationCancellationFailed(ctx, op, &failurepb.Failure{
			Message: "cancellation superseded by the caller-close policy",
		}); err != nil {
			return err
		}
	}

	_, err := addAndApplyHistoryEvent[CancelRequestedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		}
		// System principal → Apply flags it auto-close. Survives close-transaction stamping, which
		// only fills nil principals.
		e.Principal = nexusoperation.SystemPrincipal()
		// nolint:revive // We must mutate here even if the linter doesn't like it.
		e.WorkerMayIgnore = true // For compatibility with older SDKs.
	})
	if err != nil {
		if errors.Is(err, nexusoperation.ErrCancellationAlreadyRequested) ||
			errors.Is(err, nexusoperation.ErrOperationAlreadyCompleted) {
			return nil
		}
		return err
	}
	return nil
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
