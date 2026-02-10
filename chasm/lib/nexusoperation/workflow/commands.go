package workflow

import (
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
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/chasm/lib/workflow/command"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type commandHandler struct {
	config           *nexusoperation.Config
	endpointRegistry commonnexus.EndpointRegistry
}

func registerCommandHandlers(
	registry *command.Registry,
	config *nexusoperation.Config,
	endpointRegistry commonnexus.EndpointRegistry,
) error {
	h := &commandHandler{config: config, endpointRegistry: endpointRegistry}

	if err := registry.Register(
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		h.handleScheduleCommand,
	); err != nil {
		return err
	}
	return registry.Register(
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
		handleCancelCommand,
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
	ns := chasmCtx.GetNamespaceEntry()
	nsName := ns.Name().String()

	if !ch.config.Enabled() {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled",
		}
	}

	attrs := cmd.GetScheduleNexusOperationCommandAttributes()
	if attrs == nil {
		return command.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: "empty ScheduleNexusOperationCommandAttributes",
		}
	}

	var endpointID string
	endpoint, err := ch.endpointRegistry.GetByName(chasmCtx.GetContext(), ns.ID(), attrs.Endpoint)
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

	requestID := uuid.NewString()
	event := chasmCtx.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, func(he *historypb.HistoryEvent) {
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

	eventToken, err := proto.Marshal(&tokenspb.HistoryEventRef{
		EventId:      event.GetEventId(),
		EventBatchId: opts.WorkflowTaskCompletedEventID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal scheduled event token: %w", err)
	}

	op := nexusoperation.NewOperation(&nexusoperationpb.OperationState{
		EndpointId:             endpointID,
		Endpoint:               attrs.Endpoint,
		Service:                attrs.Service,
		Operation:              attrs.Operation,
		ScheduledTime:          scheduledTime,
		ScheduleToCloseTimeout: attrs.ScheduleToCloseTimeout,
		RequestId:              requestID,
		ScheduledEventToken:    eventToken,
		Attempt:                1,
	})

	if err := nexusoperation.TransitionScheduled.Apply(op, chasmCtx, nexusoperation.EventScheduled{}); err != nil {
		return err
	}

	key := strconv.FormatInt(event.GetEventId(), 10)
	wf.AddNexusOperation(chasmCtx, key, op)

	return nil
}

func handleCancelCommand(
	chasmCtx chasm.MutableContext,
	_ *chasmworkflow.Workflow,
	validator command.Validator,
	cmd *commandpb.Command,
	opts command.HandlerOptions,
) error {
	// TODO: Implement CHASM nexus operation cancellation
	return serviceerror.NewUnimplemented("CHASM nexus operation cancellation not yet implemented")
}
