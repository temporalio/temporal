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
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/plugins/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/workflow"
)

type commandHandler struct {
	config *nexusoperations.Config
}

func (ch *commandHandler) HandleScheduleCommand(
	ms workflow.MutableState,
	validator workflow.CommandValidator,
	workflowTaskCompletedEventID int64,
	command *commandpb.Command,
) error {
	nsName := ms.GetNamespaceEntry().Name().String()
	if !ch.config.Enabled(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled for this workflow's namespace",
		}
	}

	attrs := command.GetScheduleNexusOperationCommandAttributes()
	if attrs == nil {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: "empty ScheduleNexusOperationCommandAttributes",
		}
	}

	// TODO: validate service is in outgoing service registry when we have this information in the namespace entry.

	if len(attrs.Operation) > ch.config.MaxOperationNameLength(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message: fmt.Sprintf(
				"ScheduleNexusOperationCommandAttributes.Operation exceeds length limit of %d",
				ch.config.MaxOperationNameLength(nsName),
			),
		}
	}

	if !validator.IsValidPayloadSize(attrs.Input.Size()) {
		return workflow.FailWorkflowTaskError{
			Cause:        enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES,
			Message:      "ScheduleNexusOperationCommandAttributes.Input exceeds size limit",
			FailWorkflow: true,
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
	opTimeout := attrs.Timeout.AsDuration()
	if runTimeout > 0 && (opTimeout == 0 || opTimeout > runTimeout) {
		attrs.Timeout = ms.GetExecutionInfo().WorkflowRunTimeout
	}

	event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Service:                      attrs.Service,
				Operation:                    attrs.Operation,
				Input:                        attrs.Input,
				Timeout:                      attrs.Timeout,
				Header:                       attrs.Header,
				RequestId:                    uuid.NewString(),
				WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			},
		}
	})
	_, err := nexusoperations.AddChild(root, strconv.FormatInt(event.EventId, 10), event)
	return err
}

func (ch *commandHandler) HandleCancelCommand(
	ms workflow.MutableState,
	validator workflow.CommandValidator,
	workflowTaskCompletedEventID int64,
	command *commandpb.Command,
) error {
	nsName := ms.GetNamespaceEntry().Name().String()
	if !ch.config.Enabled(nsName) {
		return workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED,
			Message: "Nexus operations disabled for this workflow's namespace",
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
			}
		}
		return err
	}
	event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId:             attrs.ScheduledEventId,
				WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			},
		}
	})
	return coll.Transition(nodeID, func(o nexusoperations.Operation) (hsm.TransitionOutput, error) {
		output, err := o.Cancel(node, event.EventTime.AsTime())
		// Cancel spawns a child Cancelation machine, if that machine already exists we got a duplicate cancelation request.
		if errors.Is(err, hsm.ErrStateMachineAlreadyExists) {
			return output, workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES,
				Message: fmt.Sprintf("cancelation was already requested for an operation with scheduled event ID of %d", attrs.ScheduledEventId),
			}
		}
		return output, err
	})
}

func RegisterCommandHandlers(reg *workflow.CommandHandlerRegistry, config *nexusoperations.Config) error {
	h := commandHandler{config: config}
	if err := reg.Register(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION, h.HandleScheduleCommand); err != nil {
		return err
	}
	return reg.Register(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION, h.HandleCancelCommand)
}
