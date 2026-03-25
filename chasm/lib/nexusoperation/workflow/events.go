package workflow

import (
	"fmt"

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

// registerEvents registers all event definitions (handlers) for nexus operations.
func registerEvents(
	registry *chasmworkflow.Registry,
	config *nexusoperation.Config,
	nexusProcessor *chasm.NexusEndpointProcessor,
) error {
	if err := registry.RegisterEventDefinition(newScheduledEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newCancelRequestedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newCancelRequestCompletedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newCancelRequestFailedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newStartedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newCompletedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newFailedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEventDefinition(newCanceledEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	return registry.RegisterEventDefinition(newTimedOutEventDefinition(config, nexusProcessor))
}

type baseNexusEventDefinition struct {
	config         *nexusoperation.Config
	nexusProcessor *chasm.NexusEndpointProcessor
}

// ScheduledEventDefinition handles the NexusOperationScheduled history event.
type ScheduledEventDefinition struct {
	baseNexusEventDefinition
}

func newScheduledEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *ScheduledEventDefinition {
	return &ScheduledEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
}

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
		return serviceerror.NewInvalidArgument(fmt.Sprintf("failed to marshal parent data: %v", err))
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
		Attempt:                1,
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
	baseNexusEventDefinition
}

func newCancelRequestedEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *CancelRequestedEventDefinition {
	return &CancelRequestedEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewInvalidArgument(fmt.Sprintf("failed to marshal cancellation parent data: %v", err))
	}

	return op.Cancel(ctx, cancelParentData)
}

func (d CancelRequestedEventDefinition) CherryPick(_ chasm.MutableContext, _ *chasmworkflow.Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return chasmworkflow.ErrEventNotCherryPickable
}

// CancelRequestCompletedEventDefinition handles the NexusOperationCancelRequestCompleted history event.
type CancelRequestCompletedEventDefinition struct {
	baseNexusEventDefinition
}

func newCancelRequestCompletedEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *CancelRequestCompletedEventDefinition {
	return &CancelRequestCompletedEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	cancellation, ok := op.Cancellation.TryGet(ctx)
	if !ok {
		// No cancellation child — nothing to do.
		return nil
	}
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
	baseNexusEventDefinition
}

func newCancelRequestFailedEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *CancelRequestFailedEventDefinition {
	return &CancelRequestFailedEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	cancellation, ok := op.Cancellation.TryGet(ctx)
	if !ok {
		// No cancellation child — nothing to do.
		return nil
	}
	return nexusoperation.TransitionCancellationFailed.Apply(cancellation, ctx, nexusoperation.EventCancellationFailed{})
}

func (d CancelRequestFailedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// StartedEventDefinition handles the NexusOperationStarted history event.
type StartedEventDefinition struct {
	baseNexusEventDefinition
}

func newStartedEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *StartedEventDefinition {
	return &StartedEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	// TODO: Store event.Links on the Operation for standalone mode, where links won't be available via history.

	if err := nexusoperation.TransitionStarted.Apply(op, ctx, nexusoperation.EventStarted{
		OperationToken: attrs.GetOperationToken(),
	}); err != nil {
		return err
	}

	// If cancellation was already requested, schedule sending the cancellation request now that we have
	// an operation token.
	cancellation, ok := op.Cancellation.TryGet(ctx)
	if ok && cancellation.StateMachineState() == nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED {
		return nexusoperation.TransitionCancellationScheduled.Apply(cancellation, ctx, nexusoperation.EventCancellationScheduled{})
	}

	return nil
}

func (d StartedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// CompletedEventDefinition handles the NexusOperationCompleted history event.
type CompletedEventDefinition struct {
	baseNexusEventDefinition
}

func newCompletedEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *CompletedEventDefinition {
	return &CompletedEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	if err := nexusoperation.TransitionSucceeded.Apply(op, ctx, nexusoperation.EventSucceeded{}); err != nil {
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
	baseNexusEventDefinition
}

func newFailedEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *FailedEventDefinition {
	return &FailedEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	if err := nexusoperation.TransitionFailed.Apply(op, ctx, nexusoperation.EventFailed{}); err != nil {
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
	baseNexusEventDefinition
}

func newCanceledEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *CanceledEventDefinition {
	return &CanceledEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	if err := nexusoperation.TransitionCanceled.Apply(op, ctx, nexusoperation.EventCanceled{}); err != nil {
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
	baseNexusEventDefinition
}

func newTimedOutEventDefinition(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *TimedOutEventDefinition {
	return &TimedOutEventDefinition{
		baseNexusEventDefinition{
			config:         config,
			nexusProcessor: nexusProcessor,
		},
	}
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
		return serviceerror.NewNotFound(fmt.Sprintf("nexus operation not found for scheduled event ID %d", attrs.GetScheduledEventId()))
	}
	op := field.Get(ctx)

	if err := nexusoperation.TransitionTimedOut.Apply(op, ctx, nexusoperation.EventTimedOut{}); err != nil {
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
