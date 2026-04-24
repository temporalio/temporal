package workflow

import (
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Workflow struct {
	chasm.UnimplementedComponent

	// For now, workflow state is managed by mutable_state_impl, not CHASM engine, leaving it empty as CHASM expects a
	// state object.
	*emptypb.Empty

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the workflow.
	Callbacks chasm.Map[string, *callback.Callback]

	// Operations map is used to store the Nexus operations for the workflow, keyed by scheduled event ID.
	Operations chasm.Map[int64, *nexusoperation.Operation]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		MSPointer: msPointer,
	}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise, tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}

func (w *Workflow) ContextMetadata(_ chasm.Context) map[string]string {
	// TODO: Export workflow metadata from the CHASM workflow root instead of CloseTransaction().
	return nil
}

func (w *Workflow) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewInternal("workflow root Terminate should not be called")
}

// AddCompletionCallbacks creates completion callbacks using the CHASM implementation.
// maxCallbacksPerWorkflow is the configured maximum number of callbacks allowed per workflow.
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacksPerWorkflow int,
) error {
	// Check CHASM max callbacks limit
	currentCallbackCount := len(w.Callbacks)
	if len(completionCallbacks)+currentCallbackCount > maxCallbacksPerWorkflow {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to a workflow (%d callbacks already attached)",
			maxCallbacksPerWorkflow,
			currentCallbackCount,
		)
	}

	// Initialize map if needed
	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	// Add each callback
	for idx, cb := range completionCallbacks {
		chasmCB := &callbackspb.Callback{
			Links: cb.GetLinks(),
		}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			chasmCB.Variant = &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			return serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
		}

		// requestID (unique per API call) + idx (position within the request) ensures unique, idempotent callback IDs.
		// Unlike HSM callbacks, CHASM replicates entire trees rather than replaying events, so deterministic
		// cross-cluster IDs based on event version are not needed.
		id := fmt.Sprintf("%s-%d", requestID, idx)

		// Create and add callback
		callbackObj := callback.NewCallback(requestID, eventTime, &callbackspb.CallbackState{}, chasmCB)
		w.Callbacks[id] = chasm.NewComponentField(ctx, callbackObj)
	}
	return nil
}

// AddNexusOperation adds a Nexus operation component to the workflow.
func (w *Workflow) AddNexusOperation(
	ctx chasm.MutableContext,
	key int64,
	op *nexusoperation.Operation,
) {
	if w.Operations == nil {
		w.Operations = make(chasm.Map[int64, *nexusoperation.Operation])
	}
	w.Operations[key] = chasm.NewComponentField(ctx, op)
}

// AddAndApplyHistoryEvent adds a history event to the workflow and applies the corresponding event definition,
// looked up by Go type. This is the preferred way to add and apply events as it provides go-to-definition navigation.
func AddAndApplyHistoryEvent[D EventDefinition](
	w *Workflow,
	ctx chasm.MutableContext,
	setAttributes func(*historypb.HistoryEvent),
) (*historypb.HistoryEvent, error) {
	def, ok := EventDefinitionByGoType[D](workflowContextFromChasm(ctx).registry)
	if !ok {
		return nil, serviceerror.NewInternalf("no event definition registered for Go type %T", (*D)(nil))
	}
	event := w.AddHistoryEvent(def.Type(), setAttributes)
	return event, def.Apply(ctx, w, event)
}

// AddAndApplyHistoryEventByEventType adds a history event to the workflow and applies the corresponding event definition.
func (w *Workflow) AddAndApplyHistoryEventByEventType(
	ctx chasm.MutableContext,
	t enumspb.EventType,
	setAttributes func(*historypb.HistoryEvent),
) (*historypb.HistoryEvent, error) {
	event := w.AddHistoryEvent(t, setAttributes)
	def, ok := workflowContextFromChasm(ctx).registry.EventDefinitionByEventType(t)
	if !ok {
		return nil, serviceerror.NewInternalf("no event definition registered for %v", t)
	}
	return event, def.Apply(ctx, w, event)
}

// HasAnyBufferedEvent returns true if the workflow has any buffered event matching the given filter.
func (w *Workflow) HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool {
	return w.MSPointer.HasAnyBufferedEvent(filter)
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

	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
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
	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, func(e *historypb.HistoryEvent) {
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
	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
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

	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
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
	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, func(e *historypb.HistoryEvent) {
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

	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED, func(e *historypb.HistoryEvent) {
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

	_, err := w.AddAndApplyHistoryEventByEventType(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED, func(e *historypb.HistoryEvent) {
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

// RemoveNexusOperation removes a Nexus operation from the workflow.
func (w *Workflow) RemoveNexusOperation(key int64) {
	delete(w.Operations, key)
}

// PendingNexusOperationCount returns the number of pending Nexus operations in the workflow.
func (w *Workflow) PendingNexusOperationCount() int {
	return len(w.Operations)
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

	token, err := proto.Marshal(&tokenspb.HistoryEventRef{
		EventId:      parentData.GetScheduledEventId(),
		EventBatchId: parentData.GetScheduledEventBatchId(),
	})
	if err != nil {
		return nexusoperation.InvocationData{}, serviceerror.NewInternalf(
			"failed to marshal history event ref: %v", err,
		)
	}

	event, err := w.LoadHistoryEvent(ctx, token)
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
