package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

type commandHandler struct {
	config           *nexusoperations.Config
	endpointRegistry commonnexus.EndpointRegistry
}

func (ch *commandHandler) HandleScheduleCommand(
	ctx context.Context,
	ms historyi.MutableState,
	validator workflow.CommandValidator,
	workflowTaskCompletedEventID int64,
	command *commandpb.Command,
) error {
	ns := ms.GetNamespaceEntry()
	nsName := ms.GetNamespaceEntry().Name().String()

	if !ch.config.Enabled() {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled",
		}
	}

	attrs := command.GetScheduleNexusOperationCommandAttributes()
	if attrs == nil {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: "empty ScheduleNexusOperationCommandAttributes",
		}
	}

	var endpointID string
	endpoint, err := ch.endpointRegistry.GetByName(ctx, ns.ID(), attrs.GetEndpoint())
	if err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("endpoint %q not found", attrs.GetEndpoint()),
			}
		} else if errors.As(err, new(*serviceerror.PermissionDenied)) {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("caller namespace %q unauthorized for %q", ns.Name(), attrs.GetEndpoint()),
			}
		} else {
			return err
		}
	} else {
		endpointID = endpoint.GetId()
	}

	if len(attrs.GetService()) > ch.config.MaxServiceNameLength(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Service exceeds length limit of %d",
				ch.config.MaxServiceNameLength(nsName),
			),
		}
	}

	if len(attrs.GetOperation()) > ch.config.MaxOperationNameLength(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Operation exceeds length limit of %d",
				ch.config.MaxOperationNameLength(nsName),
			),
		}
	}

	if err := timestamp.ValidateAndCapProtoDuration(attrs.GetScheduleToCloseTimeout()); err != nil {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.ScheduleToCloseTimeout is invalid: %v", err),
		}
	}

	if !validator.IsValidPayloadSize(attrs.GetInput().Size()) {
		return workflow.FailWorkflowTaskError{
			Cause:             enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message:           "ScheduleNexusOperationCommandAttributes.Input exceeds size limit",
			TerminateWorkflow: true,
		}
	}

	headerLength := 0
	lowerCaseHeader := make(map[string]string, len(attrs.GetNexusHeader()))
	for k, v := range attrs.GetNexusHeader() {
		lowerK := strings.ToLower(k)
		lowerCaseHeader[lowerK] = v
		headerLength += len(lowerK) + len(v)
		if slices.Contains(ch.config.DisallowedOperationHeaders(), lowerK) {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("ScheduleNexusOperationCommandAttributes.NexusHeader contains a disallowed header key: %q", k),
			}
		}
	}

	if headerLength > ch.config.MaxOperationHeaderSize(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: "ScheduleNexusOperationCommandAttributes.NexusHeader exceeds size limit",
		}
	}

	root := ms.HSM()
	coll := nexusoperations.MachineCollection(root)
	maxPendingOperations := ch.config.MaxConcurrentOperations(nsName)
	if coll.Size() >= ch.config.MaxConcurrentOperations(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_NEXUS_OPERATIONS_LIMIT_EXCEEDED,
			Message: fmt.Sprintf("workflow has reached the pending nexus operation limit of %d for this namespace", maxPendingOperations),
		}
	}

	// Trim timeout to workflow run timeout.
	runTimeout := ms.GetExecutionInfo().GetWorkflowRunTimeout().AsDuration()
	opTimeout := attrs.GetScheduleToCloseTimeout().AsDuration()
	if runTimeout > 0 && (opTimeout == 0 || opTimeout > runTimeout) {
		attrs.SetScheduleToCloseTimeout(ms.GetExecutionInfo().GetWorkflowRunTimeout())
		opTimeout = attrs.GetScheduleToCloseTimeout().AsDuration()
	}

	// Trim timeout to max allowed timeout.
	if maxTimeout := ch.config.MaxOperationScheduleToCloseTimeout(nsName); maxTimeout > 0 && opTimeout > maxTimeout {
		attrs.SetScheduleToCloseTimeout(durationpb.New(maxTimeout))
	}

	event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, func(he *historypb.HistoryEvent) {
		he.SetNexusOperationScheduledEventAttributes(historypb.NexusOperationScheduledEventAttributes_builder{
			Endpoint:                     attrs.GetEndpoint(),
			EndpointId:                   endpointID,
			Service:                      attrs.GetService(),
			Operation:                    attrs.GetOperation(),
			Input:                        attrs.GetInput(),
			ScheduleToCloseTimeout:       attrs.GetScheduleToCloseTimeout(),
			NexusHeader:                  lowerCaseHeader,
			RequestId:                    uuid.NewString(),
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		}.Build())
		he.SetUserMetadata(command.GetUserMetadata())
	})

	return nexusoperations.ScheduledEventDefinition{}.Apply(root, event)
}

func (ch *commandHandler) HandleCancelCommand(
	ctx context.Context,
	ms historyi.MutableState,
	validator workflow.CommandValidator,
	workflowTaskCompletedEventID int64,
	command *commandpb.Command,
) error {
	if !ch.config.Enabled() {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled",
		}
	}

	attrs := command.GetRequestCancelNexusOperationCommandAttributes()
	if attrs == nil {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: "empty CancelNexusOperationCommandAttributes",
		}
	}

	coll := nexusoperations.MachineCollection(ms.HSM())
	nodeID := strconv.FormatInt(attrs.GetScheduledEventId(), 10)
	node, err := coll.Node(nodeID)
	hasBufferedEvent := ms.HasAnyBufferedEvent(makeNexusOperationTerminalEventFilter(attrs.GetScheduledEventId()))
	if err != nil {
		if errors.Is(err, hsm.ErrStateMachineNotFound) {
			if !hasBufferedEvent {
				return workflow.FailWorkflowTaskError{
					Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
					Message: fmt.Sprintf("requested cancelation for a non-existing or already completed operation with scheduled event ID of %d", attrs.GetScheduledEventId()),
				}
			}
			// Fallthrough and apply the event, there's special logic that will handle state machine not found below.
		} else {
			return err
		}
	}

	if node != nil {
		// TODO(bergundy): Remove this when operation auto-deletes itself on terminal state.
		// Operation may already be in a terminal state because it doesn't yet delete itself. We don't want to accept
		// cancelation in this case.
		op, err := hsm.MachineData[nexusoperations.Operation](node)
		if err != nil {
			return err
		}
		// The operation is already in a terminal state and the terminal NexusOperation event has not just been buffered.
		// We allow the workflow to request canceling an operation that has just completed while a workflow task is in
		// flight since it cannot know about the state of the operation.
		if !nexusoperations.TransitionCanceled.Possible(op) && !hasBufferedEvent {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("requested cancelation for an already complete operation with scheduled event ID of %d", attrs.GetScheduledEventId()),
			}
		}
		// END TODO
	}

	// Always create the event even if there's a buffered completion to avoid breaking replay in the SDK.
	// The event will be applied before the completion since buffered events are reordered and put at the end of the
	// batch, after command events from the workflow task.
	event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED, func(he *historypb.HistoryEvent) {
		he.SetNexusOperationCancelRequestedEventAttributes(historypb.NexusOperationCancelRequestedEventAttributes_builder{
			ScheduledEventId:             attrs.GetScheduledEventId(),
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		}.Build())
		he.SetUserMetadata(command.GetUserMetadata())
	})

	err = nexusoperations.CancelRequestedEventDefinition{}.Apply(ms.HSM(), event)

	// Cancel spawns a child Cancelation machine, if that machine already exists we got a duplicate cancelation request.
	if errors.Is(err, hsm.ErrStateMachineAlreadyExists) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf("cancelation was already requested for an operation with scheduled event ID %d", attrs.GetScheduledEventId()),
		}
	} else if errors.Is(err, hsm.ErrStateMachineNotFound) {
		// This may happen if there's a buffered completion. Ignore.
		return nil
	}

	return err
}

func RegisterCommandHandlers(reg *workflow.CommandHandlerRegistry, endpointRegistry commonnexus.EndpointRegistry, config *nexusoperations.Config) error {
	h := commandHandler{config: config, endpointRegistry: endpointRegistry}
	if err := reg.Register(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION, h.HandleScheduleCommand); err != nil {
		return err
	}
	return reg.Register(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION, h.HandleCancelCommand)
}

func makeNexusOperationTerminalEventFilter(scheduledEventID int64) func(event *historypb.HistoryEvent) bool {
	return func(event *historypb.HistoryEvent) bool {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			return event.GetNexusOperationCompletedEventAttributes().GetScheduledEventId() == scheduledEventID
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
			return event.GetNexusOperationFailedEventAttributes().GetScheduledEventId() == scheduledEventID
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
			return event.GetNexusOperationCanceledEventAttributes().GetScheduledEventId() == scheduledEventID
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
			return event.GetNexusOperationTimedOutEventAttributes().GetScheduledEventId() == scheduledEventID
		default:
			return false
		}
	}
}
