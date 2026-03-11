package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/chasm/lib/workflow/workflowregistry"
)

// registerEvents registers all event definitions (handlers) for nexus operations.
func registerEvents(
	registry *workflowregistry.Registry,
	config *nexusoperation.Config,
	nexusProcessor *chasm.NexusEndpointProcessor,
) error {
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, newScheduledEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED, newCancelRequestedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED, newCancelRequestCompletedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED, newCancelRequestFailedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, newStartedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, newCompletedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, newFailedEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, newCanceledEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	if err := registry.RegisterEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, newTimedOutEventDefinition(config, nexusProcessor)); err != nil {
		return err
	}
	return nil
}

// TODO: Implement these event definitions.
type baseNexusEventDefinition struct {
	config         *nexusoperation.Config
	nexusProcessor *chasm.NexusEndpointProcessor
}
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
	// token, err := hsm.GenerateEventLoadToken(event)
	// if err != nil {
	// 	return err
	// }
	// _, err = AddChild(root, strconv.FormatInt(event.EventId, 10), event, token)
	// return err
	return nil
}

func (d ScheduledEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return workflowregistry.ErrNotCherryPickable
}

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
	// _, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	return o.Cancel(node, event.EventTime.AsTime(), event.EventId)
	// })
	// return err
	return nil
}

func (d CancelRequestedEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return workflowregistry.ErrNotCherryPickable
}

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
	// _, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	child, err := o.CancelationNode(node)
	// 	if err != nil {
	// 		return hsm.TransitionOutput{}, err
	// 	}
	// 	if child != nil {
	// 		return hsm.TransitionOutput{}, hsm.MachineTransition(child, func(c Cancelation) (hsm.TransitionOutput, error) {
	// 			return TransitionCancelationSucceeded.Apply(c, EventCancelationSucceeded{
	// 				Time: event.EventTime.AsTime(),
	// 				Node: child,
	// 			})
	// 		})
	// 	}
	// 	return hsm.TransitionOutput{}, nil
	// })
	// return err
	return nil
}

func (d CancelRequestCompletedEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return workflowregistry.ErrNotCherryPickable
}

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
	// _, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	child, err := o.CancelationNode(node)
	// 	if err != nil {
	// 		return hsm.TransitionOutput{}, err
	// 	}
	// 	if child != nil {
	// 		return hsm.TransitionOutput{}, hsm.MachineTransition(child, func(c Cancelation) (hsm.TransitionOutput, error) {
	// 			return TransitionCancelationFailed.Apply(c, EventCancelationFailed{
	// 				Time:    event.EventTime.AsTime(),
	// 				Failure: event.GetNexusOperationCancelRequestFailedEventAttributes().GetFailure(),
	// 				Node:    child,
	// 			})
	// 		})
	// 	}
	// 	return hsm.TransitionOutput{}, nil
	// })
	// return err
	return nil
}

func (d CancelRequestFailedEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return workflowregistry.ErrNotCherryPickable
}

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
	// _, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	return TransitionStarted.Apply(o, EventStarted{
	// 		Time:       event.EventTime.AsTime(),
	// 		Node:       node,
	// 		Attributes: event.GetNexusOperationStartedEventAttributes(),
	// 	})
	// })
	// return err
	return nil
}

func (d StartedEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
	// 	return workflowregistry.ErrNotCherryPickable
	// }
	// return d.Apply(root, event)
	return nil
}

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
	// node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	return TransitionSucceeded.Apply(o, EventSucceeded{
	// 		Time: event.EventTime.AsTime(),
	// 		Node: node,
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }
	// return node.Parent.DeleteChild(node.Key)
	return nil
}

func (d CompletedEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
	// 	return workflowregistry.ErrNotCherryPickable
	// }
	// return d.Apply(root, event)
	return nil
}

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
	// node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	return TransitionFailed.Apply(o, EventFailed{
	// 		Time:       event.EventTime.AsTime(),
	// 		Attributes: event.GetNexusOperationFailedEventAttributes(),
	// 		Node:       node,
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }
	// return node.Parent.DeleteChild(node.Key)
	return nil
}

func (d FailedEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
	// 	return workflowregistry.ErrNotCherryPickable
	// }
	// return d.Apply(root, event)
	return nil
}

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
	// node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	return TransitionCanceled.Apply(o, EventCanceled{
	// 		Time: event.EventTime.AsTime(),
	// 		Node: node,
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }
	// return node.Parent.DeleteChild(node.Key)
	return nil
}

func (d CanceledEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
	// 	return workflowregistry.ErrNotCherryPickable
	// }
	// return d.Apply(root, event)
	return nil
}

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
	// node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
	// 	return TransitionTimedOut.Apply(o, EventTimedOut{
	// 		Node: node,
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }
	// return node.Parent.DeleteChild(node.Key)
	return nil
}

func (d TimedOutEventDefinition) CherryPick(ctx chasm.Context, wf *chasmworkflow.Workflow, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
	// 	return workflowregistry.ErrNotCherryPickable
	// }
	// return d.Apply(root, event)
	return nil
}
