// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination history_builder_mock.go

package definition

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	HistoryMutation struct {
		// events to be persist to events table
		DBEventsBatches [][]*historypb.HistoryEvent
		// events to be buffer in execution table
		DBBufferBatch []*historypb.HistoryEvent
		// whether to clear buffer events on DB
		DBClearBuffer bool
		// accumulated buffered events, equal to all buffer events from execution table
		MemBufferBatch []*historypb.HistoryEvent
		// scheduled to started event ID mapping for flushed buffered event
		ScheduledIDToStartedID map[int64]int64
	}

	HistoryBuilder interface {
		AddWorkflowExecutionStartedEvent(
			startTime time.Time,
			request *historyservice.StartWorkflowExecutionRequest,
			resetPoints *workflowpb.ResetPoints,
			prevRunID string,
			firstRunID string,
			originalRunID string,
		) *historypb.HistoryEvent

		AddWorkflowTaskScheduledEvent(
			taskQueue *taskqueuepb.TaskQueue,
			startToCloseTimeout *time.Duration,
			attempt int32,
			now time.Time,
		) *historypb.HistoryEvent

		AddWorkflowTaskStartedEvent(
			scheduledEventID int64,
			requestID string,
			identity string,
			now time.Time,
		) *historypb.HistoryEvent

		AddWorkflowTaskCompletedEvent(
			scheduledEventID int64,
			startedEventID int64,
			identity string,
			checksum string,
		) *historypb.HistoryEvent
		AddWorkflowTaskTimedOutEvent(
			scheduledEventID int64,
			startedEventID int64,
			timeoutType enumspb.TimeoutType,
		) *historypb.HistoryEvent
		AddWorkflowTaskFailedEvent(
			scheduledEventID int64,
			startedEventID int64,
			cause enumspb.WorkflowTaskFailedCause,
			failure *failurepb.Failure,
			identity string,
			baseRunID string,
			newRunID string,
			forkEventVersion int64,
			checksum string,
		) *historypb.HistoryEvent
		AddActivityTaskScheduledEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.ScheduleActivityTaskCommandAttributes,
		) *historypb.HistoryEvent
		AddActivityTaskStartedEvent(
			scheduledEventID int64,
			attempt int32,
			requestID string,
			identity string,
			lastFailure *failurepb.Failure,
		) *historypb.HistoryEvent
		AddActivityTaskCompletedEvent(
			scheduledEventID int64,
			startedEventID int64,
			identity string,
			result *commonpb.Payloads,
		) *historypb.HistoryEvent
		AddActivityTaskFailedEvent(
			scheduledEventID int64,
			startedEventID int64,
			failure *failurepb.Failure,
			retryState enumspb.RetryState,
			identity string,
		) *historypb.HistoryEvent
		AddActivityTaskTimedOutEvent(
			scheduledEventID,
			startedEventID int64,
			timeoutFailure *failurepb.Failure,
			retryState enumspb.RetryState,
		) *historypb.HistoryEvent
		AddCompletedWorkflowEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.CompleteWorkflowExecutionCommandAttributes,
			newExecutionRunID string,
		) *historypb.HistoryEvent
		AddFailWorkflowEvent(
			workflowTaskCompletedEventID int64,
			retryState enumspb.RetryState,
			command *commandpb.FailWorkflowExecutionCommandAttributes,
			newExecutionRunID string,
		) *historypb.HistoryEvent
		AddTimeoutWorkflowEvent(
			retryState enumspb.RetryState,
			newExecutionRunID string,
		) *historypb.HistoryEvent
		AddWorkflowExecutionTerminatedEvent(
			reason string,
			details *commonpb.Payloads,
			identity string,
		) *historypb.HistoryEvent
		AddContinuedAsNewEvent(
			workflowTaskCompletedEventID int64,
			newRunID string,
			command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
		) *historypb.HistoryEvent
		AddTimerStartedEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.StartTimerCommandAttributes,
		) *historypb.HistoryEvent
		AddTimerFiredEvent(
			startedEventID int64,
			timerID string,
		) *historypb.HistoryEvent
		AddActivityTaskCancelRequestedEvent(
			workflowTaskCompletedEventID int64,
			scheduledEventID int64,
		) *historypb.HistoryEvent
		AddActivityTaskCanceledEvent(
			scheduledEventID int64,
			startedEventID int64,
			latestCancelRequestedEventID int64,
			details *commonpb.Payloads,
			identity string,
		) *historypb.HistoryEvent
		AddTimerCanceledEvent(
			workflowTaskCompletedEventID int64,
			startedEventID int64,
			timerID string,
			identity string,
		) *historypb.HistoryEvent
		AddWorkflowExecutionCancelRequestedEvent(
			request *historyservice.RequestCancelWorkflowExecutionRequest,
		) *historypb.HistoryEvent
		AddWorkflowExecutionCanceledEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.CancelWorkflowExecutionCommandAttributes,
		) *historypb.HistoryEvent
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
			targetNamespaceID namespace.ID,
		) *historypb.HistoryEvent
		AddRequestCancelExternalWorkflowExecutionFailedEvent(
			workflowTaskCompletedEventID int64,
			initiatedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			workflowID string,
			runID string,
			cause enumspb.CancelExternalWorkflowExecutionFailedCause,
		) *historypb.HistoryEvent
		AddExternalWorkflowExecutionCancelRequested(
			initiatedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			workflowID string,
			runID string,
		) *historypb.HistoryEvent
		AddSignalExternalWorkflowExecutionInitiatedEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
			targetNamespaceID namespace.ID,
		) *historypb.HistoryEvent
		AddUpsertWorkflowSearchAttributesEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
		) *historypb.HistoryEvent
		AddWorkflowPropertiesModifiedEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.ModifyWorkflowPropertiesCommandAttributes,
		) *historypb.HistoryEvent
		AddSignalExternalWorkflowExecutionFailedEvent(
			workflowTaskCompletedEventID int64,
			initiatedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			workflowID string,
			runID string,
			control string,
			cause enumspb.SignalExternalWorkflowExecutionFailedCause,
		) *historypb.HistoryEvent
		AddExternalWorkflowExecutionSignaled(
			initiatedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			workflowID string,
			runID string,
			control string,
		) *historypb.HistoryEvent
		AddMarkerRecordedEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.RecordMarkerCommandAttributes,
		) *historypb.HistoryEvent
		AddWorkflowExecutionSignaledEvent(
			signalName string,
			input *commonpb.Payloads,
			identity string,
			header *commonpb.Header,
		) *historypb.HistoryEvent
		AddStartChildWorkflowExecutionInitiatedEvent(
			workflowTaskCompletedEventID int64,
			command *commandpb.StartChildWorkflowExecutionCommandAttributes,
			targetNamespaceID namespace.ID,
		) *historypb.HistoryEvent
		AddChildWorkflowExecutionStartedEvent(
			initiatedID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			workflowType *commonpb.WorkflowType,
			header *commonpb.Header,
		) *historypb.HistoryEvent
		AddStartChildWorkflowExecutionFailedEvent(
			workflowTaskCompletedEventID int64,
			initiatedID int64,
			cause enumspb.StartChildWorkflowExecutionFailedCause,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			workflowID string,
			workflowType *commonpb.WorkflowType,
			control string,
		) *historypb.HistoryEvent
		AddChildWorkflowExecutionCompletedEvent(
			initiatedID int64,
			startedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			workflowType *commonpb.WorkflowType,
			result *commonpb.Payloads,
		) *historypb.HistoryEvent
		AddChildWorkflowExecutionFailedEvent(
			initiatedID int64,
			startedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			workflowType *commonpb.WorkflowType,
			failure *failurepb.Failure,
			retryState enumspb.RetryState,
		) *historypb.HistoryEvent
		AddChildWorkflowExecutionCanceledEvent(
			initiatedID int64,
			startedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			workflowType *commonpb.WorkflowType,
			details *commonpb.Payloads,
		) *historypb.HistoryEvent
		AddChildWorkflowExecutionTerminatedEvent(
			initiatedID int64,
			startedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			workflowType *commonpb.WorkflowType,
		) *historypb.HistoryEvent
		AddChildWorkflowExecutionTimedOutEvent(
			initiatedID int64,
			startedEventID int64,
			targetNamespace namespace.Name,
			targetNamespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			workflowType *commonpb.WorkflowType,
			retryState enumspb.RetryState,
		) *historypb.HistoryEvent
		HasBufferEvents() bool
		BufferEventSize() int
		NextEventID() int64
		FlushBufferToCurrentBatch() map[int64]int64
		FlushAndCreateNewBatch()
		Finish(
			flushBufferEvent bool,
		) (*HistoryMutation, error)
		HasActivityFinishEvent(
			scheduledEventID int64,
		) bool
		GetAndRemoveTimerFireEvent(
			timerID string,
		) *historypb.HistoryEvent
	}
)
