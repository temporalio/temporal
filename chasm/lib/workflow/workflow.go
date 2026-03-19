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

// AddHistoryEvent adds a history event to the workflow and applies the corresponding event definition.
func (w *Workflow) AddHistoryEvent(
	ctx chasm.MutableContext,
	t enumspb.EventType,
	setAttributes func(*historypb.HistoryEvent),
) (*historypb.HistoryEvent, error) {
	event := w.MSPointer.AddHistoryEvent(t, setAttributes)
	def, ok := eventRegistry(ctx).EventDefinition(t)
	if !ok {
		return nil, fmt.Errorf("no event definition registered for %v", t)
	}
	return event, def.Apply(ctx, w, event)
}

// eventRegistry retrieves the EventRegistry from the CHASM context.
func eventRegistry(ctx chasm.Context) EventRegistry {
	reg, ok := ctx.Value(eventRegistryChasmCtxKey).(EventRegistry)
	if !ok {
		return nil
	}
	return reg
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
	links []*commonpb.Link,
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewFailedPreconditionf("failed to unmarshal nexus operation parent data: %v", err)
	}

	_, err := w.AddHistoryEvent(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: parentData.GetScheduledEventId(),
				OperationToken:   operationToken,
				RequestId:        op.GetRequestId(),
			},
		}
		e.Links = links
	})
	return err
}

// OnNexusOperationCancelled adds a NexusOperationCanceled history event to the workflow and applies
// the corresponding event definition.
func (w *Workflow) OnNexusOperationCancelled(
	ctx chasm.MutableContext,
	op *nexusoperation.Operation,
	cause *failurepb.Failure,
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewFailedPreconditionf("failed to unmarshal nexus operation parent data: %v", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	_, err := w.AddHistoryEvent(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, func(e *historypb.HistoryEvent) {
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
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewFailedPreconditionf("failed to unmarshal nexus operation parent data: %v", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	_, err := w.AddHistoryEvent(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
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
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewFailedPreconditionf("failed to unmarshal nexus operation parent data: %v", err)
	}

	_, err := w.AddHistoryEvent(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
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
) error {
	parentData := &workflowpb.NexusOperationParentData{}
	if err := op.GetParentData().UnmarshalTo(parentData); err != nil {
		return serviceerror.NewFailedPreconditionf("failed to unmarshal nexus operation parent data: %v", err)
	}

	scheduledEventID := parentData.GetScheduledEventId()
	_, err := w.AddHistoryEvent(ctx, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, func(e *historypb.HistoryEvent) {
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
