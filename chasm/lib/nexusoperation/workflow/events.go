package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	workflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

// ScheduledEventDefinition handles the NexusOperationScheduled history event.
type ScheduledEventDefinition struct{}

func (d ScheduledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ScheduledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
}

func (d ScheduledEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationScheduledEventAttributes()

	parentData, err := anypb.New(&workflowpb.NexusOperationParentData{
		ScheduledEventId:      event.GetEventId(),
		ScheduledEventBatchId: attrs.GetWorkflowTaskCompletedEventId(),
	})
	if err != nil {
		return serviceerror.NewInternalf("failed to marshal parent data: %v", err)
	}

	op := nexusoperation.NewOperation(&nexusoperationpb.OperationState{
		EndpointId:             attrs.GetEndpointId(),
		Endpoint:               attrs.GetEndpoint(),
		Service:                attrs.GetService(),
		Operation:              attrs.GetOperation(),
		ScheduledTime:          event.GetEventTime(),
		ScheduleToStartTimeout: attrs.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    attrs.GetStartToCloseTimeout(),
		ScheduleToCloseTimeout: attrs.GetScheduleToCloseTimeout(),
		RequestId:              attrs.GetRequestId(),
		ParentData:             parentData,
		Attempt:                0,
	})

	if err := nexusoperation.TransitionScheduled.Apply(op, ctx, nexusoperation.EventScheduled{}); err != nil {
		return err
	}

	wf.AddNexusOperation(ctx, event.GetEventId(), op)

	return nil
}

func (d ScheduledEventDefinition) CherryPick(_ chasm.MutableContext, _ *chasmworkflow.Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return chasmworkflow.ErrEventNotCherryPickable
}

// CancelRequestedEventDefinition handles the NexusOperationCancelRequested history event.
type CancelRequestedEventDefinition struct {
}

func (d CancelRequestedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d CancelRequestedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED
}

func (d CancelRequestedEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationCancelRequestedEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		// Operation may have already completed (buffered terminal event). Ignore.
		return nil
	}

	op := field.Get(ctx)
	cancelParentData, err := anypb.New(&workflowpb.NexusCancellationParentData{
		RequestedEventId: event.GetEventId(),
	})
	if err != nil {
		return serviceerror.NewInternalf("failed to marshal cancellation parent data: %v", err)
	}

	return op.RequestCancel(ctx, &nexusoperationpb.CancellationState{
		ParentData: cancelParentData,
	})
}

func (d CancelRequestedEventDefinition) CherryPick(_ chasm.MutableContext, _ *chasmworkflow.Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return chasmworkflow.ErrEventNotCherryPickable
}

// CancelRequestCompletedEventDefinition handles the NexusOperationCancelRequestCompleted history event.
type CancelRequestCompletedEventDefinition struct {
}

func (d CancelRequestCompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d CancelRequestCompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED
}

func (d CancelRequestCompletedEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationCancelRequestCompletedEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	// Cancellation must be present to deliver a cancel request.
	cancellation := field.Get(ctx).Cancellation.Get(ctx)
	return nexusoperation.TransitionCancellationSucceeded.Apply(cancellation, ctx, nexusoperation.EventCancellationSucceeded{})
}

func (d CancelRequestCompletedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// CancelRequestFailedEventDefinition handles the NexusOperationCancelRequestFailed history event.
type CancelRequestFailedEventDefinition struct {
}

func (d CancelRequestFailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d CancelRequestFailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED
}

func (d CancelRequestFailedEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationCancelRequestFailedEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	// Cancellation must be present to deliver a cancel request.
	cancellation := field.Get(ctx).Cancellation.Get(ctx)
	return nexusoperation.TransitionCancellationFailed.Apply(cancellation, ctx, nexusoperation.EventCancellationFailed{
		Failure: attrs.GetFailure(),
	})
}

func (d CancelRequestFailedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// StartedEventDefinition handles the NexusOperationStarted history event.
type StartedEventDefinition struct {
}

func (d StartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d StartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED
}

func (d StartedEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationStartedEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	op := field.Get(ctx)

	startTime := event.GetEventTime().AsTime()
	return nexusoperation.TransitionStarted.Apply(op, ctx, nexusoperation.EventStarted{
		OperationToken: attrs.GetOperationToken(),
		StartTime:      &startTime,
	})
}

func (d StartedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// CompletedEventDefinition handles the NexusOperationCompleted history event.
type CompletedEventDefinition struct {
}

func (d CompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d CompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
}

func (d CompletedEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationCompletedEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	op := field.Get(ctx)

	completeTime := event.GetEventTime().AsTime()
	if err := nexusoperation.TransitionSucceeded.Apply(op, ctx, nexusoperation.EventSucceeded{
		Result:       attrs.GetResult(),
		CompleteTime: &completeTime,
	}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(attrs.GetScheduledEventId())
	return nil
}

func (d CompletedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// FailedEventDefinition handles the NexusOperationFailed history event.
type FailedEventDefinition struct {
}

func (d FailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d FailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED
}

func (d FailedEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationFailedEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	op := field.Get(ctx)

	completeTime := event.GetEventTime().AsTime()
	if err := nexusoperation.TransitionFailed.Apply(op, ctx, nexusoperation.EventFailed{
		CompleteTime: &completeTime,
		Failure:      attrs.GetFailure().GetCause(),
	}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(attrs.GetScheduledEventId())
	return nil
}

func (d FailedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// CanceledEventDefinition handles the NexusOperationCanceled history event.
type CanceledEventDefinition struct {
}

func (d CanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d CanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED
}

func (d CanceledEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationCanceledEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	op := field.Get(ctx)

	completeTime := event.GetEventTime().AsTime()
	if err := nexusoperation.TransitionCanceled.Apply(op, ctx, nexusoperation.EventCanceled{
		CompleteTime: &completeTime,
		Failure:      attrs.GetFailure().GetCause(),
	}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(attrs.GetScheduledEventId())
	return nil
}

func (d CanceledEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// TimedOutEventDefinition handles the NexusOperationTimedOut history event.
type TimedOutEventDefinition struct {
}

func (d TimedOutEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d TimedOutEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
}

func (d TimedOutEventDefinition) Apply(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetNexusOperationTimedOutEventAttributes()
	field, ok := wf.Operations[attrs.GetScheduledEventId()]
	if !ok {
		return serviceerror.NewNotFoundf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId())
	}
	op := field.Get(ctx)

	if err := nexusoperation.TransitionTimedOut.Apply(op, ctx, nexusoperation.EventTimedOut{
		Failure: attrs.GetFailure().GetCause(),
	}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(attrs.GetScheduledEventId())
	return nil
}

func (d TimedOutEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}
