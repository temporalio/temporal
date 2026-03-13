package workflow

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/chasm/lib/workflow/command"
	workflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type commandHandler struct {
	config         *nexusoperation.Config
	nexusProcessor *chasm.NexusEndpointProcessor
}

func registerCommandHandlers(
	registry *command.Registry,
	config *nexusoperation.Config,
	nexusProcessor *chasm.NexusEndpointProcessor,
) error {
	h := &commandHandler{config: config, nexusProcessor: nexusProcessor}

	if err := registry.Register(
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		h.handleScheduleCommand,
	); err != nil {
		return err
	}
	return registry.Register(
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
		h.handleCancelCommand,
	)
}

//nolint:revive // cognitive-complexity: this is a direct port of the HSM command handler
func (ch *commandHandler) handleScheduleCommand(
	chasmCtx chasm.MutableContext,
	wf *chasmworkflow.Workflow,
	validator command.Validator,
	cmd *commandpb.Command,
	opts command.HandlerOptions,
) error {
	ns := chasmCtx.NamespaceEntry()
	nsName := ns.Name().String()

	if !ch.config.Enabled() {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled",
		}
	}

	if !ch.config.ChasmNexusEnabled(nsName) {
		return command.ErrNotSupported
	}

	attrs := cmd.GetScheduleNexusOperationCommandAttributes()
	if attrs == nil {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: "empty ScheduleNexusOperationCommandAttributes",
		}
	}

	requestID := uuid.NewString()
	var endpointID string
	// Skip endpoint registry lookup for __temporal_system endpoint
	if attrs.Endpoint == commonnexus.SystemEndpoint {
		if len(attrs.NexusHeader) > 0 {
			return command.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("ScheduleNexusOperationCommandAttributes.NexusHeader must be empty when using %s endpoint", commonnexus.SystemEndpoint),
			}
		}
		// Run ProcessInput for validation.
		_, err := ch.nexusProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
			Namespace: ns,
			RequestID: requestID,
			// Links are not needed for validation.
		}, attrs.Service, attrs.Operation, attrs.Input)
		if err != nil {
			var handlerErr *nexus.HandlerError
			if errors.As(err, &handlerErr) {
				//nolint:exhaustive
				switch handlerErr.Type {
				case nexus.HandlerErrorTypeNotFound, nexus.HandlerErrorTypeBadRequest:
					return command.FailWorkflowTaskError{
						Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
						Message: handlerErr.Message,
					}
				}
			}
			return err
		}
	} else {
		endpoint, err := chasmCtx.EndpointByName(attrs.Endpoint)
		if err != nil {
			if errors.As(err, new(*serviceerror.NotFound)) {
				return command.FailWorkflowTaskError{
					Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
					Message: fmt.Sprintf("endpoint %q not found", attrs.Endpoint),
				}
			}
			if errors.As(err, new(*serviceerror.PermissionDenied)) {
				return command.FailWorkflowTaskError{
					Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
					Message: fmt.Sprintf("caller namespace %q unauthorized for %q", ns.Name(), attrs.Endpoint),
				}
			}
			return err
		}
		endpointID = endpoint.Id
	}

	if len(attrs.Service) > ch.config.MaxServiceNameLength(nsName) {
		return command.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Service exceeds length limit of %d",
				ch.config.MaxServiceNameLength(nsName),
			),
		}
	}

	if len(attrs.Operation) > ch.config.MaxOperationNameLength(nsName) {
		return command.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Operation exceeds length limit of %d",
				ch.config.MaxOperationNameLength(nsName),
			),
		}
	}

	if err := timestamp.ValidateAndCapProtoDuration(attrs.ScheduleToCloseTimeout); err != nil {
		return command.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.ScheduleToCloseTimeout is invalid: %v", err),
		}
	}

	if err := timestamp.ValidateAndCapProtoDuration(attrs.ScheduleToStartTimeout); err != nil {
		return command.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.ScheduleToStartTimeout is invalid: %v", err),
		}
	}

	if err := timestamp.ValidateAndCapProtoDuration(attrs.StartToCloseTimeout); err != nil {
		return command.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.StartToCloseTimeout is invalid: %v", err),
		}
	}

	if !validator.IsValidPayloadSize(attrs.Input.Size()) {
		return command.FailWorkflowTaskError{
			Cause:             enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message:           "ScheduleNexusOperationCommandAttributes.Input exceeds size limit",
			TerminateWorkflow: true,
		}
	}

	headerLength := 0
	lowerCaseHeader := make(map[string]string, len(attrs.NexusHeader))
	for k, v := range attrs.NexusHeader {
		lowerK := strings.ToLower(k)
		lowerCaseHeader[lowerK] = v
		headerLength += len(lowerK) + len(v)
		if slices.Contains(ch.config.DisallowedOperationHeaders(), lowerK) {
			return command.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("ScheduleNexusOperationCommandAttributes.NexusHeader contains a disallowed header key: %q", k),
			}
		}
	}

	if headerLength > ch.config.MaxOperationHeaderSize(nsName) {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: "ScheduleNexusOperationCommandAttributes.NexusHeader exceeds size limit",
		}
	}

	maxPendingOperations := ch.config.MaxConcurrentOperations(nsName)
	if wf.PendingNexusOperationCount() >= maxPendingOperations {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_NEXUS_OPERATIONS_LIMIT_EXCEEDED,
			Message: fmt.Sprintf("workflow has reached the pending nexus operation limit of %d for this namespace", maxPendingOperations),
		}
	}

	// Trim timeout to workflow run timeout.
	runTimeout := wf.WorkflowRunTimeout()
	opTimeout := attrs.ScheduleToCloseTimeout.AsDuration()
	if runTimeout > 0 && (opTimeout == 0 || opTimeout > runTimeout) {
		attrs.ScheduleToCloseTimeout = durationpb.New(runTimeout)
		opTimeout = runTimeout
	}

	// Trim timeout to max allowed timeout.
	if maxTimeout := ch.config.MaxOperationScheduleToCloseTimeout(nsName); maxTimeout > 0 && opTimeout > maxTimeout {
		attrs.ScheduleToCloseTimeout = durationpb.New(maxTimeout)
	}

	// Trim secondary timeouts to the primary timeout.
	scheduleToCloseTimeout := attrs.ScheduleToCloseTimeout.AsDuration()
	scheduleToStartTimeout := attrs.ScheduleToStartTimeout.AsDuration()
	startToCloseTimeout := attrs.StartToCloseTimeout.AsDuration()

	if scheduleToCloseTimeout > 0 {
		if scheduleToStartTimeout > scheduleToCloseTimeout {
			attrs.ScheduleToStartTimeout = attrs.ScheduleToCloseTimeout
		}
		if startToCloseTimeout > scheduleToCloseTimeout {
			attrs.StartToCloseTimeout = attrs.ScheduleToCloseTimeout
		}
	}

	event := wf.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Endpoint:                     attrs.Endpoint,
				EndpointId:                   endpointID,
				Service:                      attrs.Service,
				Operation:                    attrs.Operation,
				Input:                        attrs.Input,
				ScheduleToCloseTimeout:       attrs.ScheduleToCloseTimeout,
				ScheduleToStartTimeout:       attrs.ScheduleToStartTimeout,
				StartToCloseTimeout:          attrs.StartToCloseTimeout,
				NexusHeader:                  lowerCaseHeader,
				RequestId:                    requestID,
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
			},
		}
		he.UserMetadata = cmd.UserMetadata
	})

	scheduledTime := event.GetEventTime()
	if scheduledTime == nil {
		scheduledTime = timestamppb.Now()
	}

	parentData, err := anypb.New(&workflowpb.NexusOperationParentData{
		ScheduledEventId:      event.GetEventId(),
		ScheduledEventBatchId: opts.WorkflowTaskCompletedEventID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal parent data: %w", err)
	}

	op := nexusoperation.NewOperation(&nexusoperationpb.OperationState{
		EndpointId:             endpointID,
		Endpoint:               attrs.Endpoint,
		Service:                attrs.Service,
		Operation:              attrs.Operation,
		ScheduledTime:          scheduledTime,
		ScheduleToStartTimeout: attrs.ScheduleToStartTimeout,
		StartToCloseTimeout:    attrs.StartToCloseTimeout,
		ScheduleToCloseTimeout: attrs.ScheduleToCloseTimeout,
		RequestId:              requestID,
		ParentData:             parentData,
		Attempt:                1,
	})

	key := strconv.FormatInt(event.GetEventId(), 10)
	wf.AddNexusOperation(chasmCtx, key, op)

	return nil
}

func (ch *commandHandler) handleCancelCommand(
	chasmCtx chasm.MutableContext,
	wf *chasmworkflow.Workflow,
	validator command.Validator,
	cmd *commandpb.Command,
	opts command.HandlerOptions,
) error {
	if !ch.config.Enabled() {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled",
		}
	}

	nsName := chasmCtx.NamespaceEntry().Name().String()
	if !ch.config.ChasmNexusEnabled(nsName) {
		return command.ErrNotSupported
	}

	attrs := cmd.GetRequestCancelNexusOperationCommandAttributes()
	if attrs == nil {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: "empty CancelNexusOperationCommandAttributes",
		}
	}

	key := strconv.FormatInt(attrs.ScheduledEventId, 10)
	operationField, operationFound := wf.Operations[key]
	hasBufferedEvent := func() bool {
		return wf.HasAnyBufferedEvent(makeNexusOperationTerminalEventFilter(attrs.ScheduledEventId))
	}

	if !operationFound && !hasBufferedEvent() {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf("requested cancelation for a non-existing or already completed operation with scheduled event ID of %d", attrs.ScheduledEventId),
		}
	}

	// Always create the event even if there's a buffered completion to avoid breaking replay in the SDK.
	// The event will be applied before the completion since buffered events are reordered and put at the end of the
	// batch, after command events from the workflow task.
	event := wf.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId:             attrs.ScheduledEventId,
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
			},
		}
		he.UserMetadata = cmd.UserMetadata
	})

	if !operationFound {
		// Operation not found but there's a buffered terminal event. The workflow couldn't know
		// the operation completed while its task was in flight. Ignore.
		return nil
	}

	op := operationField.Get(chasmCtx)
	cancelParentData, err := anypb.New(&workflowpb.NexusCancellationParentData{
		RequestedEventId: event.GetEventId(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal cancellation parent data: %w", err)
	}
	err = op.Cancel(chasmCtx, cancelParentData)
	if errors.Is(err, nexusoperation.ErrCancellationAlreadyRequested) {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf("cancelation was already requested for an operation with scheduled event ID %d", attrs.ScheduledEventId),
		}
	}
	return err
}

func makeNexusOperationTerminalEventFilter(scheduledEventID int64) func(event *historypb.HistoryEvent) bool {
	return func(event *historypb.HistoryEvent) bool {
		switch event.EventType {
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
