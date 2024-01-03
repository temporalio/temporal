// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package historybuilder

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

const (
	HistoryBuilderStateMutable HistoryBuilderState = iota
	HistoryBuilderStateImmutable
	HistoryBuilderStateSealed
)

type (
	EventFactory struct {
		timeSource clock.TimeSource
		version    int64
	}

	HistoryBuilder struct {
		EventStore
		EventFactory
	}

	HistoryBuilderState int

	HistoryMutation struct {
		// events to be persisted to events table
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

	TaskIDGenerator func(number int) ([]int64, error)

	BufferedEventFilter func(*historypb.HistoryEvent) bool
)

func NewMutableHistoryBuilder(
	timeSource clock.TimeSource,
	taskIDGenerator TaskIDGenerator,
	version int64,
	nextEventID int64,
	dbBufferBatch []*historypb.HistoryEvent,
	metricsHandler metrics.Handler,
) *HistoryBuilder {
	return &HistoryBuilder{
		EventStore: EventStore{
			state:           HistoryBuilderStateMutable,
			timeSource:      timeSource,
			taskIDGenerator: taskIDGenerator,

			version:     version,
			nextEventID: nextEventID,

			workflowFinished: false,

			dbBufferBatch:          dbBufferBatch,
			dbClearBuffer:          false,
			memEventsBatches:       nil,
			memLatestBatch:         nil,
			memBufferBatch:         nil,
			scheduledIDToStartedID: make(map[int64]int64),

			metricsHandler: metricsHandler,
		},
		EventFactory: EventFactory{timeSource: timeSource, version: version},
	}
}

func NewImmutableHistoryBuilder(
	histories ...[]*historypb.HistoryEvent,
) *HistoryBuilder {
	lastHistory := histories[len(histories)-1]
	lastEvent := lastHistory[len(lastHistory)-1]
	return &HistoryBuilder{
		EventStore: EventStore{
			state:           HistoryBuilderStateImmutable,
			timeSource:      nil,
			taskIDGenerator: nil,

			version:     lastEvent.GetVersion(),
			nextEventID: lastEvent.GetEventId() + 1,

			workflowFinished: false,

			dbBufferBatch:          nil,
			dbClearBuffer:          false,
			memEventsBatches:       histories,
			memLatestBatch:         nil,
			memBufferBatch:         nil,
			scheduledIDToStartedID: nil,

			metricsHandler: nil,
		},
		EventFactory: EventFactory{},
	}
}

func (b *HistoryBuilder) IsDirty() bool {
	return b.EventStore.IsDirty()
}

// NOTE:
// originalRunID is the runID when the WorkflowExecutionStarted event is written
// firstRunID is the very first runID along the chain of ContinueAsNew and Reset

func (b *HistoryBuilder) AddWorkflowExecutionStartedEvent(
	startTime time.Time,
	request *historyservice.StartWorkflowExecutionRequest,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowExecutionStartedEvent(
		startTime,
		request,
		resetPoints,
		prevRunID,
		firstRunID,
		originalRunID,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}
func (b *HistoryBuilder) AddWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *durationpb.Duration,
	attempt int32,
	scheduleTime time.Time,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowTaskScheduledEvent(taskQueue, startToCloseTimeout, attempt, scheduleTime)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	identity string,
	startTime time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowTaskStartedEvent(
		scheduledEventID,
		requestID,
		identity,
		startTime,
		suggestContinueAsNew,
		historySizeBytes,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	checksum string,
	workerVersionStamp *commonpb.WorkerVersionStamp,
	sdkMetadata *sdkpb.WorkflowTaskCompletedMetadata,
	meteringMetadata *commonpb.MeteringMetadata,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		identity,
		checksum,
		workerVersionStamp,
		sdkMetadata,
		meteringMetadata,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowTaskTimedOutEvent(scheduledEventID, startedEventID, timeoutType)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
	checksum string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		cause,
		failure,
		identity,
		baseRunID,
		newRunID,
		forkEventVersion,
		checksum,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskScheduledEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskStartedEvent(scheduledEventID, attempt, requestID, identity, lastFailure)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskCompletedEvent(scheduledEventID, startedEventID, identity, result)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		failure,
		retryState,
		identity,
	)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskTimedOutEvent(
	scheduledEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		timeoutFailure,
		retryState,
	)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildCompletedWorkflowEvent(workflowTaskCompletedEventID, command, newExecutionRunID)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) (*historypb.HistoryEvent, int64) {
	event := b.EventFactory.BuildFailWorkflowEvent(
		workflowTaskCompletedEventID,
		retryState,
		command,
		newExecutionRunID,
	)

	return b.EventStore.appendEvents(event)
}

func (b *HistoryBuilder) AddTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildTimeoutWorkflowEvent(retryState, newExecutionRunID)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowExecutionTerminatedEvent(reason, details, identity)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionUpdateAcceptedEvent(
	protocolInstanceID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowExecutionUpdateAcceptedEvent(
		protocolInstanceID,
		acceptedRequestMessageId,
		acceptedRequestSequencingEventId,
		acceptedRequest,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	updResp *updatepb.Response,
) (*historypb.HistoryEvent, int64) {
	event := b.EventFactory.BuildWorkflowExecutionUpdateCompletedEvent(acceptedEventID, updResp)
	return b.EventStore.appendEvents(event)
}

func (b *HistoryBuilder) AddContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildContinuedAsNewEvent(workflowTaskCompletedEventID, newRunID, command)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildTimerStartedEvent(workflowTaskCompletedEventID, command)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildTimerFiredEvent(startedEventID, timerID)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskCancelRequestedEvent(workflowTaskCompletedEventID, scheduledEventID)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskCanceledEvent(
	scheduledEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildActivityTaskCanceledEvent(
		scheduledEventID,
		startedEventID,
		latestCancelRequestedEventID,
		details,
		identity,
	)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	startedEventID int64,
	timerID string,
	identity string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildTimerCanceledEvent(workflowTaskCompletedEventID, startedEventID, timerID, identity)

	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionCancelRequestedEvent(
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowExecutionCancelRequestedEvent(request)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildRequestCancelExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID,
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		cause,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionCancelRequested(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildExternalWorkflowExecutionCancelRequested(
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowPropertiesModifiedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowPropertiesModifiedEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildSignalExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID,
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		control,
		cause,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionSignaled(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildExternalWorkflowExecutionSignaled(
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		control,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddMarkerRecordedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildMarkerRecordedEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	skipGenerateWorkflowTask bool,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildWorkflowExecutionSignaledEvent(
		signalName,
		input,
		identity,
		header,
		skipGenerateWorkflowTask,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionStartedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildChildWorkflowExecutionStartedEvent(
		initiatedID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		header,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildChildWorkflowExecutionFailedEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		failure,
		retryState,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildChildWorkflowExecutionCompletedEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		result,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	workflowType *commonpb.WorkflowType,
	control string,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildStartChildWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID,
		initiatedID,
		cause,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		workflowType,
		control,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	details *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildChildWorkflowExecutionCanceledEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		details,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildChildWorkflowExecutionTerminatedEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.EventFactory.BuildChildWorkflowExecutionTimedOutEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		retryState,
	)
	event, _ = b.EventStore.appendEvents(event)
	return event
}
