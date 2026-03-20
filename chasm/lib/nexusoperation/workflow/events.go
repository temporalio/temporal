package workflow

import (
	"fmt"
	"strconv"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	workflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// getOperation looks up a Nexus operation from the workflow by its scheduled event ID.
func getOperation(
	ctx chasm.MutableContext,
	wf *chasmworkflow.Workflow,
	scheduledEventID int64,
) (*nexusoperation.Operation, string, error) {
	key := strconv.FormatInt(scheduledEventID, 10)
	field, ok := wf.Operations[key]
	if !ok {
		return nil, "", fmt.Errorf("nexus operation not found for scheduled event ID %d", scheduledEventID)
	}
	return field.Get(ctx), key, nil
}

// ScheduledEventDefinition

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

	scheduledTime := event.GetEventTime()
	if scheduledTime == nil {
		scheduledTime = timestamppb.Now()
	}

	parentData, err := anypb.New(&workflowpb.NexusOperationParentData{
		ScheduledEventId:      event.GetEventId(),
		ScheduledEventBatchId: attrs.GetWorkflowTaskCompletedEventId(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal parent data: %w", err)
	}

	op := nexusoperation.NewOperation(&nexusoperationpb.OperationState{
		EndpointId:             attrs.GetEndpointId(),
		Endpoint:               attrs.GetEndpoint(),
		Service:                attrs.GetService(),
		Operation:              attrs.GetOperation(),
		ScheduledTime:          scheduledTime,
		ScheduleToStartTimeout: attrs.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    attrs.GetStartToCloseTimeout(),
		ScheduleToCloseTimeout: attrs.GetScheduleToCloseTimeout(),
		RequestId:              attrs.GetRequestId(),
		ParentData:             parentData,
		Attempt:                1,
	})

	key := strconv.FormatInt(event.GetEventId(), 10)
	wf.AddNexusOperation(ctx, key, op)

	return nil
}

func (d ScheduledEventDefinition) CherryPick(_ chasm.MutableContext, _ *chasmworkflow.Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return chasmworkflow.ErrEventNotCherryPickable
}

// CancelRequestedEventDefinition

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
	key := strconv.FormatInt(attrs.GetScheduledEventId(), 10)
	field, ok := wf.Operations[key]
	if !ok {
		// Operation may have already completed (buffered terminal event). Ignore.
		return nil
	}

	op := field.Get(ctx)
	cancelParentData, err := anypb.New(&workflowpb.NexusCancellationParentData{
		RequestedEventId: event.GetEventId(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal cancellation parent data: %w", err)
	}

	return op.Cancel(ctx, cancelParentData)
}

func (d CancelRequestedEventDefinition) CherryPick(_ chasm.MutableContext, _ *chasmworkflow.Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return chasmworkflow.ErrEventNotCherryPickable
}

// CancelRequestCompletedEventDefinition

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
	op, _, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

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

// CancelRequestFailedEventDefinition

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
	op, _, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

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

// StartedEventDefinition

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
	op, _, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

	return nexusoperation.TransitionStarted.Apply(op, ctx, nexusoperation.EventStarted{
		OperationToken: attrs.GetOperationToken(),
		FromBackingOff: op.Status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	})
}

func (d StartedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// CompletedEventDefinition

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
	op, key, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

	if err := nexusoperation.TransitionSucceeded.Apply(op, ctx, nexusoperation.EventSucceeded{}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(key)
	return nil
}

func (d CompletedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// FailedEventDefinition

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
	op, key, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

	if err := nexusoperation.TransitionFailed.Apply(op, ctx, nexusoperation.EventFailed{}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(key)
	return nil
}

func (d FailedEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// CanceledEventDefinition

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
	op, key, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

	if err := nexusoperation.TransitionCanceled.Apply(op, ctx, nexusoperation.EventCanceled{}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(key)
	return nil
}

func (d CanceledEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}

// TimedOutEventDefinition

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
	op, key, err := getOperation(ctx, wf, attrs.GetScheduledEventId())
	if err != nil {
		return err
	}

	if err := nexusoperation.TransitionTimedOut.Apply(op, ctx, nexusoperation.EventTimedOut{}); err != nil {
		return err
	}
	wf.RemoveNexusOperation(key)
	return nil
}

func (d TimedOutEventDefinition) CherryPick(ctx chasm.MutableContext, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return chasmworkflow.ErrEventNotCherryPickable
	}
	return d.Apply(ctx, wf, event)
}
