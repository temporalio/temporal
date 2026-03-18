package workflow

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	workflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/historybuilder"
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

	// Operations map is used to store the Nexus operations for the workflow.
	Operations chasm.Map[string, *nexusoperation.Operation]
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

// ProcessCloseCallbacks triggers "WorkflowClosed" callbacks using the CHASM implementation.
// It iterates through all callbacks and schedules WorkflowClosed ones that are in STANDBY state.
func (w *Workflow) ProcessCloseCallbacks(ctx chasm.MutableContext) error {
	// Iterate through all callbacks and schedule WorkflowClosed ones
	for _, field := range w.Callbacks {
		cb := field.Get(ctx)
		// Only process callbacks in STANDBY state (not already triggered)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
			continue
		}
		// Trigger the callback by transitioning to SCHEDULED state
		if err := callback.TransitionScheduled.Apply(cb, ctx, callback.EventScheduled{}); err != nil {
			return err
		}
	}
	return nil
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
			return fmt.Errorf("unsupported callback variant: %T", variant)
		}

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
	key string,
	op *nexusoperation.Operation,
) {
	if w.Operations == nil {
		w.Operations = make(chasm.Map[string, *nexusoperation.Operation])
	}
	w.Operations[key] = chasm.NewComponentField(ctx, op)
}

// AddHistoryEvent adds a history event to the workflow.
func (w *Workflow) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	return w.MSPointer.AddHistoryEvent(t, setAttributes)
}

// eventRegistry retrieves the EventRegistry from the CHASM context.
func eventRegistry(ctx chasm.Context) EventRegistry {
	return ctx.Value(eventRegistryChasmCtxKey).(EventRegistry)
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
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return fmt.Errorf("failed to unmarshal nexus operation parent data: %w", err)
	}

	eventType := enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED
	event := w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				OperationToken:   op.GetOperationToken(),
				OperationId:      op.GetOperationToken(),
				RequestId:        op.GetRequestId(),
			},
		}
	})

	def, ok := eventRegistry(ctx).EventDefinition(eventType)
	if !ok {
		return fmt.Errorf("no event definition registered for %v", eventType)
	}
	return def.Apply(ctx, w, event)
}

// OnNexusOperationCancelled adds a NexusOperationCanceled history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationCancelled(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return fmt.Errorf("failed to unmarshal nexus operation parent data: %w", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	eventType := enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED
	event := w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
			NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        op.GetRequestId(),
				Failure:          createNexusOperationFailure(op, scheduledEventID),
			},
		}
	})

	def, ok := eventRegistry(ctx).EventDefinition(eventType)
	if !ok {
		return fmt.Errorf("no event definition registered for %v", eventType)
	}
	return def.Apply(ctx, w, event)
}

// OnNexusOperationFailed adds a NexusOperationFailed history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationFailed(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return fmt.Errorf("failed to unmarshal nexus operation parent data: %w", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	eventType := enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED
	event := w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
			NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        op.GetRequestId(),
				Failure:          createNexusOperationFailure(op, scheduledEventID),
			},
		}
	})

	def, ok := eventRegistry(ctx).EventDefinition(eventType)
	if !ok {
		return fmt.Errorf("no event definition registered for %v", eventType)
	}
	return def.Apply(ctx, w, event)
}

// OnNexusOperationCompleted adds a NexusOperationCompleted history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationCompleted(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return fmt.Errorf("failed to unmarshal nexus operation parent data: %w", err)
	}

	eventType := enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
	event := w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				RequestId:        op.GetRequestId(),
				// TODO: Figure out how to populate result for completed event.
				// Result:           op.GetResult(),
			},
		}
	})

	def, ok := eventRegistry(ctx).EventDefinition(eventType)
	if !ok {
		return fmt.Errorf("no event definition registered for %v", eventType)
	}
	return def.Apply(ctx, w, event)
}

// OnNexusOperationTimedOut adds a NexusOperationTimedOut history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationTimedOut(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return fmt.Errorf("failed to unmarshal nexus operation parent data: %w", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	eventType := enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
	event := w.AddHistoryEvent(eventType, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
			NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				RequestId:        op.GetRequestId(),
				Failure:          createNexusOperationFailure(op, scheduledEventID),
			},
		}
	})

	def, ok := eventRegistry(ctx).EventDefinition(eventType)
	if !ok {
		return fmt.Errorf("no event definition registered for %v", eventType)
	}
	return def.Apply(ctx, w, event)
}

// createNexusOperationFailure creates a NexusOperationExecutionFailure wrapping the operation's last attempt failure.
func createNexusOperationFailure(op *nexusoperation.Operation, scheduledEventID int64) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "nexus operation completed unsuccessfully",
		FailureInfo: &failurepb.Failure_NexusOperationExecutionFailureInfo{
			NexusOperationExecutionFailureInfo: &failurepb.NexusOperationFailureInfo{
				Endpoint:  op.GetEndpoint(),
				Service:   op.GetService(),
				Operation: op.GetOperation(),
				// TODO: Check if this is correct.
				OperationToken:   op.GetOperationToken(),
				OperationId:      op.GetOperationToken(),
				ScheduledEventId: scheduledEventID,
			},
		},
		Cause: op.GetLastAttemptFailure(),
	}
}

// RemoveNexusOperation removes a Nexus operation from the workflow.
func (w *Workflow) RemoveNexusOperation(key string) {
	delete(w.Operations, key)
}

// PendingNexusOperationCount returns the number of pending Nexus operations in the workflow.
func (w *Workflow) PendingNexusOperationCount() int {
	return len(w.Operations)
}

func (w *Workflow) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// Retrieve the completion data from the underlying mutable state via MSPointer
	return w.MSPointer.GetNexusCompletion(ctx, requestID)
}
