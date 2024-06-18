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
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
)

type EventStore struct {
	state           HistoryBuilderState
	timeSource      clock.TimeSource
	taskIDGenerator TaskIDGenerator

	version     int64
	nextEventID int64

	// workflow finished
	workflowFinished bool

	// buffer events in DB
	dbBufferBatch []*historypb.HistoryEvent
	dbClearBuffer bool

	// in mem events
	memEventsBatches [][]*historypb.HistoryEvent
	memLatestBatch   []*historypb.HistoryEvent
	memBufferBatch   []*historypb.HistoryEvent

	// scheduled to started event ID mapping
	scheduledIDToStartedID map[int64]int64

	metricsHandler metrics.Handler
}

func (b *EventStore) IsDirty() bool {
	return len(b.memEventsBatches) > 0 ||
		len(b.memLatestBatch) > 0 ||
		len(b.memBufferBatch) > 0 ||
		len(b.scheduledIDToStartedID) > 0
}

func (b *EventStore) AllocateEventID() int64 {
	result := b.nextEventID
	b.nextEventID++
	return result
}

func (b *EventStore) NextEventID() int64 {
	return b.nextEventID
}

func (b *EventStore) LastEventVersion() (int64, bool) {
	if len(b.memLatestBatch) != 0 {
		lastEvent := b.memLatestBatch[len(b.memLatestBatch)-1]
		return lastEvent.GetVersion(), true
	}

	if len(b.memEventsBatches) != 0 {
		lastBatch := b.memEventsBatches[len(b.memEventsBatches)-1]
		lastEvent := lastBatch[len(lastBatch)-1]
		return lastEvent.GetVersion(), true
	}

	// buffered events are not real events yet, so not taken into account here

	return common.EmptyVersion, false
}

func (b *EventStore) add(
	event *historypb.HistoryEvent,
) (*historypb.HistoryEvent, int64) {
	b.assertMutable()
	if b.workflowFinished {
		panic("history builder unable to add new event after workflow finish")
	}
	if b.finishEvent(event.GetEventType()) {
		b.workflowFinished = true
	}

	batchID := common.EmptyEventID
	if b.bufferEvent(event.GetEventType()) {
		event.EventId = common.BufferedEventID
		b.memBufferBatch = append(b.memBufferBatch, event)
	} else {
		event.EventId = b.AllocateEventID()
		b.memLatestBatch = append(b.memLatestBatch, event)
		batchID = b.memLatestBatch[0].EventId
	}
	return event, batchID
}

func (b *EventStore) HasBufferEvents() bool {
	return len(b.dbBufferBatch) > 0 || len(b.memBufferBatch) > 0
}

// HasAnyBufferedEvent returns true if there is at least one buffered event that matches the provided filter.
func (b *EventStore) HasAnyBufferedEvent(predicate BufferedEventFilter) bool {
	for _, event := range b.memBufferBatch {
		if predicate(event) {
			return true
		}
	}
	for _, event := range b.dbBufferBatch {
		if predicate(event) {
			return true
		}
	}
	return false
}

func (b *EventStore) NumBufferedEvents() int {
	return len(b.dbBufferBatch) + len(b.memBufferBatch)
}

func (b *EventStore) SizeInBytesOfBufferedEvents() int {
	size := 0
	for _, ev := range b.dbBufferBatch {
		size += proto.Size(ev)
	}
	for _, ev := range b.memBufferBatch {
		size += proto.Size(ev)
	}
	return size
}

func (b *EventStore) FlushBufferToCurrentBatch() map[int64]int64 {
	if len(b.dbBufferBatch) == 0 && len(b.memBufferBatch) == 0 {
		return b.scheduledIDToStartedID
	}

	b.assertMutable()

	if b.workflowFinished {
		// in case this case happen
		// 1. request cancel activity
		// 2. workflow task complete
		// above will generate 2 then 1
		b.dbBufferBatch = nil
		b.memBufferBatch = nil
		return b.scheduledIDToStartedID
	}

	b.dbClearBuffer = b.dbClearBuffer || len(b.dbBufferBatch) > 0
	bufferBatch := append(b.dbBufferBatch, b.memBufferBatch...)
	b.dbBufferBatch = nil
	b.memBufferBatch = nil

	// 0th reorder events in case casandra reorder the buffered events
	// TODO eventually remove this ordering
	bufferBatch = b.reorderBuffer(bufferBatch)

	// 1st assign event ID
	for _, event := range bufferBatch {
		event.EventId = b.AllocateEventID()
	}

	// 2nd wire event ID, e.g. activity, child workflow
	b.wireEventIDs(bufferBatch)

	b.memLatestBatch = append(b.memLatestBatch, bufferBatch...)

	return b.scheduledIDToStartedID
}

func (b *EventStore) FlushAndCreateNewBatch() {
	b.assertNotSealed()
	if len(b.memLatestBatch) == 0 {
		return
	}

	b.memEventsBatches = append(b.memEventsBatches, b.memLatestBatch)
	b.memLatestBatch = nil
}

func (b *EventStore) Finish(
	flushBufferEvent bool,
) (*HistoryMutation, error) {
	defer func() {
		b.state = HistoryBuilderStateSealed
	}()

	if flushBufferEvent {
		_ = b.FlushBufferToCurrentBatch()
	}
	b.FlushAndCreateNewBatch()

	dbEventsBatches := b.memEventsBatches
	dbClearBuffer := b.dbClearBuffer
	dbBufferBatch := b.memBufferBatch
	memBufferBatch := b.dbBufferBatch
	memBufferBatch = append(memBufferBatch, dbBufferBatch...)
	scheduledIDToStartedID := b.scheduledIDToStartedID

	b.memEventsBatches = nil
	b.memBufferBatch = nil
	b.memLatestBatch = nil
	b.dbClearBuffer = false
	b.dbBufferBatch = nil
	b.scheduledIDToStartedID = nil

	if err := b.assignTaskIDs(dbEventsBatches); err != nil {
		return nil, err
	}

	return &HistoryMutation{
		DBEventsBatches:        dbEventsBatches,
		DBClearBuffer:          dbClearBuffer,
		DBBufferBatch:          dbBufferBatch,
		MemBufferBatch:         memBufferBatch,
		ScheduledIDToStartedID: scheduledIDToStartedID,
	}, nil
}

func (b *EventStore) assignTaskIDs(
	dbEventsBatches [][]*historypb.HistoryEvent,
) error {
	b.assertNotSealed()

	if b.state == HistoryBuilderStateImmutable {
		return nil
	}

	taskIDCount := 0
	for i := 0; i < len(dbEventsBatches); i++ {
		taskIDCount += len(dbEventsBatches[i])
	}
	taskIDs, err := b.taskIDGenerator(taskIDCount)
	if err != nil {
		return err
	}

	taskIDPointer := 0
	height := len(dbEventsBatches)
	for i := 0; i < height; i++ {
		width := len(dbEventsBatches[i])
		for j := 0; j < width; j++ {
			dbEventsBatches[i][j].TaskId = taskIDs[taskIDPointer]
			taskIDPointer++
		}
	}
	return nil
}

func (b *EventStore) assertMutable() {
	if b.state != HistoryBuilderStateMutable {
		panic("history builder is mutated while not in mutable state")
	}
}

func (b *EventStore) assertNotSealed() {
	if b.state == HistoryBuilderStateSealed {
		panic("history builder is in sealed state")
	}
}

func (b *EventStore) bufferEvent(
	eventType enumspb.EventType,
) bool {
	switch eventType {
	case // do not buffer for workflow state change
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return false

	case // workflow task event should not be buffered
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
		return false

	case // events generated directly from commands should not be buffered
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		// CommandTypeCancelTimer is an exception. This command will be mapped
		// to workflow.EventTypeTimerCanceled.
		// This event should not be buffered. Ref: historyEngine, search for "workflow.CommandTypeCancelTimer"
		enumspb.EVENT_TYPE_TIMER_CANCELED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_MARKER_RECORDED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED:
		// do not buffer event if event is directly generated from a corresponding command
		return false

	case // events generated directly from messages should not be buffered
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
		return false

	default:
		return true
	}
}

func (b *EventStore) finishEvent(
	eventType enumspb.EventType,
) bool {
	switch eventType {
	case
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return true

	default:
		return false
	}
}

//nolint:revive
func (b *EventStore) wireEventIDs(
	bufferEvents []*historypb.HistoryEvent,
) {
	for _, event := range bufferEvents {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attributes := event.GetActivityTaskStartedEventAttributes()
			scheduledEventID := attributes.GetScheduledEventId()
			b.scheduledIDToStartedID[scheduledEventID] = event.GetEventId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			attributes := event.GetActivityTaskFailedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			attributes := event.GetActivityTaskTimedOutEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			attributes := event.GetActivityTaskCanceledEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
			initiatedEventID := attributes.GetInitiatedEventId()
			b.scheduledIDToStartedID[initiatedEventID] = event.GetEventId()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		}
	}
}

// TODO remove this function once we keep all info in DB, e.g. activity / timer / child workflow
//  to deprecate
//  * HasActivityFinishEvent
//  * hasActivityFinishEvent

func (b *EventStore) reorderBuffer(
	bufferEvents []*historypb.HistoryEvent,
) []*historypb.HistoryEvent {
	b.emitOutOfOrderBufferedEvents(bufferEvents)
	reorderBuffer := make([]*historypb.HistoryEvent, 0, len(bufferEvents))
	reorderEvents := make([]*historypb.HistoryEvent, 0, len(bufferEvents))
	for _, event := range bufferEvents {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
			// TODO: This implementation detail is hurting extensibility of workflows and the ability to add external
			// components.
			enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
			enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
			reorderBuffer = append(reorderBuffer, event)
		default:
			reorderEvents = append(reorderEvents, event)
		}
	}

	return append(reorderEvents, reorderBuffer...)
}

func (b *EventStore) emitOutOfOrderBufferedEvents(bufferedEvents []*historypb.HistoryEvent) {

	if b.metricsHandler == nil {
		return
	}

	completedActivities := make(map[int64]enumspb.EventType)
	completedChildWorkflows := make(map[int64]enumspb.EventType)
	completedNexusOperations := make(map[int64]enumspb.EventType)
	for _, event := range bufferedEvents {
		switch event.GetEventType() {
		// Activity.
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			if completeEventType, seenCompleted := completedActivities[event.GetEventId()]; seenCompleted {
				metrics.OutOfOrderBufferedEventsCounter.With(b.metricsHandler).Record(1, metrics.OperationTag(completeEventType.String()))
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			completedActivities[event.GetActivityTaskCompletedEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			completedActivities[event.GetActivityTaskFailedEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			completedActivities[event.GetActivityTaskTimedOutEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			completedActivities[event.GetActivityTaskCanceledEventAttributes().GetStartedEventId()] = event.GetEventType()

		// Child Workflow.
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			if completeEventType, seenCompleted := completedChildWorkflows[event.GetEventId()]; seenCompleted {
				metrics.OutOfOrderBufferedEventsCounter.With(b.metricsHandler).Record(1, metrics.OperationTag(completeEventType.String()))
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			completedChildWorkflows[event.GetChildWorkflowExecutionCompletedEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			completedChildWorkflows[event.GetChildWorkflowExecutionFailedEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			completedChildWorkflows[event.GetChildWorkflowExecutionTimedOutEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			completedChildWorkflows[event.GetChildWorkflowExecutionCanceledEventAttributes().GetStartedEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			completedChildWorkflows[event.GetChildWorkflowExecutionTerminatedEventAttributes().GetStartedEventId()] = event.GetEventType()

		// Nexus Operation.
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:
			if completeEventType, seenCompleted := completedNexusOperations[event.GetNexusOperationStartedEventAttributes().GetScheduledEventId()]; seenCompleted {
				metrics.OutOfOrderBufferedEventsCounter.With(b.metricsHandler).Record(1, metrics.OperationTag(completeEventType.String()))
			}
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			completedNexusOperations[event.GetNexusOperationCompletedEventAttributes().GetScheduledEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
			completedNexusOperations[event.GetNexusOperationFailedEventAttributes().GetScheduledEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
			completedNexusOperations[event.GetNexusOperationTimedOutEventAttributes().GetScheduledEventId()] = event.GetEventType()
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
			completedNexusOperations[event.GetNexusOperationCanceledEventAttributes().GetScheduledEventId()] = event.GetEventType()
		}
	}
}

func (b *EventStore) HasActivityFinishEvent(
	scheduledEventID int64,
) bool {

	if hasActivityFinishEvent(scheduledEventID, b.dbBufferBatch) {
		return true
	}

	if hasActivityFinishEvent(scheduledEventID, b.memBufferBatch) {
		return true
	}

	if hasActivityFinishEvent(scheduledEventID, b.memLatestBatch) {
		return true
	}

	for _, batch := range b.memEventsBatches {
		if hasActivityFinishEvent(scheduledEventID, batch) {
			return true
		}
	}

	return false
}

func hasActivityFinishEvent(
	scheduledEventID int64,
	events []*historypb.HistoryEvent,
) bool {
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			if event.GetActivityTaskFailedEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			if event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			if event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}
		}
	}

	return false
}

func (b *EventStore) GetAndRemoveTimerFireEvent(
	timerID string,
) *historypb.HistoryEvent {
	var timerFireEvent *historypb.HistoryEvent

	b.dbBufferBatch, timerFireEvent = deleteTimerFiredEvent(timerID, b.dbBufferBatch)
	if timerFireEvent != nil {
		b.dbClearBuffer = true
		return timerFireEvent
	}

	b.memBufferBatch, timerFireEvent = deleteTimerFiredEvent(timerID, b.memBufferBatch)
	if timerFireEvent != nil {
		b.dbClearBuffer = true
		return timerFireEvent
	}

	return nil
}

func deleteTimerFiredEvent(
	timerID string,
	events []*historypb.HistoryEvent,
) ([]*historypb.HistoryEvent, *historypb.HistoryEvent) {
	// go over all history events. if we find a timer fired event for the given
	// timerID, clear it
	timerFiredIdx := -1
	for idx, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED &&
			event.GetTimerFiredEventAttributes().GetTimerId() == timerID {
			timerFiredIdx = idx
			break
		}
	}
	if timerFiredIdx == -1 {
		return events, nil
	}

	timerEvent := events[timerFiredIdx]
	return append(events[:timerFiredIdx], events[timerFiredIdx+1:]...), timerEvent
}
