// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

type commandHandler struct {
	config           *nexusoperations.Config
	endpointRegistry commonnexus.EndpointRegistry
	logger           log.Logger
}

func (ch *commandHandler) HandleScheduleCommand(
	ctx context.Context,
	ms workflow.MutableState,
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
	endpoint, err := ch.endpointRegistry.GetByName(ctx, ns.ID(), attrs.Endpoint)
	if err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			if !ch.config.EndpointNotFoundAlwaysNonRetryable(nsName) {
				return workflow.FailWorkflowTaskError{
					Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
					Message: fmt.Sprintf("endpoint %q not found", attrs.Endpoint),
				}
			}
			// Ignore, and let the operation fail when the task is executed.
		} else {
			return err
		}
	} else {
		endpointID = endpoint.Id
	}

	if len(attrs.Service) > ch.config.MaxServiceNameLength(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Service exceeds length limit of %d",
				ch.config.MaxServiceNameLength(nsName),
			),
		}
	}

	if len(attrs.Operation) > ch.config.MaxOperationNameLength(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Operation exceeds length limit of %d",
				ch.config.MaxOperationNameLength(nsName),
			),
		}
	}

	if err := timestamp.ValidateProtoDuration(attrs.ScheduleToCloseTimeout); err != nil {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.ScheduleToCloseTimeout is invalid: %v", err),
		}
	}

	if !validator.IsValidPayloadSize(attrs.Input.Size()) {
		return workflow.FailWorkflowTaskError{
			Cause:             enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message:           "ScheduleNexusOperationCommandAttributes.Input exceeds size limit",
			TerminateWorkflow: true,
		}
	}

	headerLength := 0
	for k, v := range attrs.NexusHeader {
		headerLength += len(k) + len(v)
		if slices.Contains(ch.config.DisallowedOperationHeaders(nsName), strings.ToLower(k)) {
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
	runTimeout := ms.GetExecutionInfo().WorkflowRunTimeout.AsDuration()
	opTimeout := attrs.ScheduleToCloseTimeout.AsDuration()
	if opTimeout == 0 {
		if headerOpTimeout, ok := attrs.GetNexusHeader()["Operation-Timeout"]; ok {
			parsedHeaderOpTimeout, err := time.ParseDuration(headerOpTimeout)
			if err != nil {
				// Operation timeout is not required, so do not fail the command on parsing errors.
				ch.logger.Warn(fmt.Sprintf("unable to parse Operation-Timeout header: %v", headerOpTimeout), tag.Error(err))
			} else {
				opTimeout = parsedHeaderOpTimeout
			}
		}
	}
	if runTimeout > 0 && (opTimeout == 0 || opTimeout > runTimeout) {
		attrs.ScheduleToCloseTimeout = ms.GetExecutionInfo().WorkflowRunTimeout
		opTimeout = attrs.ScheduleToCloseTimeout.AsDuration()
	}

	// Trim timeout to max allowed timeout.
	if maxTimeout := ch.config.MaxOperationScheduleToCloseTimeout(nsName); maxTimeout > 0 && opTimeout > maxTimeout {
		attrs.ScheduleToCloseTimeout = durationpb.New(maxTimeout)
	}

	event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Endpoint:                     attrs.Endpoint,
				EndpointId:                   endpointID,
				Service:                      attrs.Service,
				Operation:                    attrs.Operation,
				Input:                        attrs.Input,
				ScheduleToCloseTimeout:       attrs.ScheduleToCloseTimeout,
				NexusHeader:                  attrs.NexusHeader,
				RequestId:                    uuid.NewString(),
				WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			},
		}
		he.UserMetadata = command.UserMetadata
	})

	return nexusoperations.ScheduledEventDefinition{}.Apply(root, event)
}

func (ch *commandHandler) HandleCancelCommand(
	ctx context.Context,
	ms workflow.MutableState,
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
	nodeID := strconv.FormatInt(attrs.ScheduledEventId, 10)
	node, err := coll.Node(nodeID)
	if err != nil {
		if errors.Is(err, hsm.ErrStateMachineNotFound) {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("requested cancelation for a non-existing operation with scheduled event ID of %d", attrs.ScheduledEventId),
				// TODO(bergundy): Message: fmt.Sprintf("requested cancelation for a non-existing or already completed operation with scheduled event ID of %d", attrs.ScheduledEventId),
			}
		}
		return err
	}
	// TODO(bergundy): Remove this when operation auto-deletes itself on terminal state.
	// Operation may already be in a terminal state because it doesn't yet delete itself. We don't want to accept
	// cancelation in this case.
	op, err := hsm.MachineData[nexusoperations.Operation](node)
	if err != nil {
		return err
	}
	if !nexusoperations.TransitionCanceled.Possible(op) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf("requested cancelation for an already complete operation with scheduled event ID of %d", attrs.ScheduledEventId),
		}
	}

	event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId:             attrs.ScheduledEventId,
				WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			},
		}
		he.UserMetadata = command.UserMetadata
	})

	err = nexusoperations.CancelRequestedEventDefinition{}.Apply(ms.HSM(), event)

	// Cancel spawns a child Cancelation machine, if that machine already exists we got a duplicate cancelation request.
	if errors.Is(err, hsm.ErrStateMachineAlreadyExists) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf("cancelation was already requested for an operation with scheduled event ID of %d", attrs.ScheduledEventId),
		}
	}

	// TODO(bergundy): When we support machine deletion, this err may be an hsm.ErrStateMachineNotFound.
	// We'll need to check buffered events and verify that there aren't any terminal events in there.
	// Ideally the framework can abstract that for us though.
	return err
}

func RegisterCommandHandlers(reg *workflow.CommandHandlerRegistry, endpointRegistry commonnexus.EndpointRegistry, config *nexusoperations.Config, logger log.Logger) error {
	h := commandHandler{
		config:           config,
		endpointRegistry: endpointRegistry,
		logger:           logger,
	}
	if err := reg.Register(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION, h.HandleScheduleCommand); err != nil {
		return err
	}
	return reg.Register(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION, h.HandleCancelCommand)
}
