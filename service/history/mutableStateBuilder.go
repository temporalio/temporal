// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"errors"
	"fmt"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

const (
	emptyUUID = "emptyUuid"
)

type (
	mutableStateBuilder struct {
		pendingActivityInfoIDs          map[int64]*persistence.ActivityInfo // Schedule Event ID -> Activity Info.
		pendingActivityInfoByActivityID map[string]int64                    // Activity ID -> Schedule Event ID of the activity.
		updateActivityInfos             []*persistence.ActivityInfo         // Modified activities from last update.
		deleteActivityInfo              *int64                              // Deleted activities from last update.

		pendingTimerInfoIDs map[string]*persistence.TimerInfo // User Timer ID -> Timer Info.
		updateTimerInfos    []*persistence.TimerInfo          // Modified timers from last update.
		deleteTimerInfos    []string                          // Deleted timers from last update.

		pendingChildExecutionInfoIDs map[int64]*persistence.ChildExecutionInfo // Initiated Event ID -> Child Execution Info
		updateChildExecutionInfos    []*persistence.ChildExecutionInfo         // Modified ChildExecution Infos since last update
		deleteChildExecutionInfo     *int64                                    // Deleted ChildExecution Info since last update

		pendingRequestCancelInfoIDs map[int64]*persistence.RequestCancelInfo // Initiated Event ID -> RequestCancelInfo
		updateRequestCancelInfos    []*persistence.RequestCancelInfo         // Modified RequestCancel Infos since last update, for persistence update
		deleteRequestCancelInfo     *int64                                   // Deleted RequestCancel Info since last update, for persistence update

		pendingSignalInfoIDs map[int64]*persistence.SignalInfo // Initiated Event ID -> SignalInfo
		updateSignalInfos    []*persistence.SignalInfo         // Modified SignalInfo since last update
		deleteSignalInfo     *int64                            // Deleted SignalInfo since last update

		pendingSignalRequestedIDs map[string]struct{} // Set of signaled requestIds
		updateSignalRequestedIDs  map[string]struct{} // Set of signaled requestIds since last update
		deleteSignalRequestedID   string              // Deleted signaled requestId

		bufferedEvents       []*persistence.SerializedHistoryEventBatch // buffered history events that are already persisted
		updateBufferedEvents *persistence.SerializedHistoryEventBatch   // buffered history events that needs to be persisted
		clearBufferedEvents  bool                                       // delete buffered events from persistence

		executionInfo    *persistence.WorkflowExecutionInfo // Workflow mutable state info.
		replicationState *persistence.ReplicationState
		continueAsNew    *persistence.CreateWorkflowExecutionRequest
		hBuilder         *historyBuilder
		eventSerializer  historyEventSerializer
		config           *Config
		logger           bark.Logger
	}

	mutableStateSessionUpdates struct {
		newEventsBuilder           *historyBuilder
		updateActivityInfos        []*persistence.ActivityInfo
		deleteActivityInfo         *int64
		updateTimerInfos           []*persistence.TimerInfo
		deleteTimerInfos           []string
		updateChildExecutionInfos  []*persistence.ChildExecutionInfo
		deleteChildExecutionInfo   *int64
		updateCancelExecutionInfos []*persistence.RequestCancelInfo
		deleteCancelExecutionInfo  *int64
		updateSignalInfos          []*persistence.SignalInfo
		deleteSignalInfo           *int64
		updateSignalRequestedIDs   []string
		deleteSignalRequestedID    string
		continueAsNew              *persistence.CreateWorkflowExecutionRequest
		newBufferedEvents          *persistence.SerializedHistoryEventBatch
		clearBufferedEvents        bool
	}

	// TODO: This should be part of persistence layer
	decisionInfo struct {
		ScheduleID      int64
		StartedID       int64
		RequestID       string
		DecisionTimeout int32
		Tasklist        string // This is only needed to communicate tasklist used after AddDecisionTaskScheduledEvent
		Attempt         int64
		Timestamp       int64
	}
)

func newMutableStateBuilder(config *Config, logger bark.Logger) *mutableStateBuilder {
	s := &mutableStateBuilder{
		updateActivityInfos:             []*persistence.ActivityInfo{},
		pendingActivityInfoIDs:          make(map[int64]*persistence.ActivityInfo),
		pendingActivityInfoByActivityID: make(map[string]int64),
		pendingTimerInfoIDs:             make(map[string]*persistence.TimerInfo),
		updateTimerInfos:                []*persistence.TimerInfo{},
		deleteTimerInfos:                []string{},
		updateChildExecutionInfos:       []*persistence.ChildExecutionInfo{},
		pendingChildExecutionInfoIDs:    make(map[int64]*persistence.ChildExecutionInfo),
		updateRequestCancelInfos:        []*persistence.RequestCancelInfo{},
		pendingRequestCancelInfoIDs:     make(map[int64]*persistence.RequestCancelInfo),
		updateSignalInfos:               []*persistence.SignalInfo{},
		pendingSignalInfoIDs:            make(map[int64]*persistence.SignalInfo),
		updateSignalRequestedIDs:        make(map[string]struct{}),
		pendingSignalRequestedIDs:       make(map[string]struct{}),
		eventSerializer:                 newJSONHistoryEventSerializer(),
		config:                          config,
		logger:                          logger,
	}
	s.executionInfo = &persistence.WorkflowExecutionInfo{
		NextEventID:        firstEventID,
		State:              persistence.WorkflowStateCreated,
		CloseStatus:        persistence.WorkflowCloseStatusNone,
		LastProcessedEvent: emptyEventID,
	}
	s.hBuilder = newHistoryBuilder(s, logger)

	return s
}

func (e *mutableStateBuilder) Load(state *persistence.WorkflowMutableState) {
	e.pendingActivityInfoIDs = state.ActivitInfos
	e.pendingTimerInfoIDs = state.TimerInfos
	e.pendingChildExecutionInfoIDs = state.ChildExecutionInfos
	e.pendingRequestCancelInfoIDs = state.RequestCancelInfos
	e.pendingSignalInfoIDs = state.SignalInfos
	e.pendingSignalRequestedIDs = state.SignalRequestedIDs
	e.executionInfo = state.ExecutionInfo

	e.replicationState = state.ReplicationState
	e.bufferedEvents = state.BufferedEvents
	for _, ai := range state.ActivitInfos {
		e.pendingActivityInfoByActivityID[ai.ActivityID] = ai.ScheduleID
	}
}

func (e *mutableStateBuilder) FlushBufferedEvents() error {
	// put new events into 2 buckets:
	//  1) if the event was added while there was in-flight decision, then put it in buffered bucket
	//  2) otherwise, put it in committed bucket
	var newBufferedEvents []*workflow.HistoryEvent
	var newCommittedEvents []*workflow.HistoryEvent
	for _, event := range e.hBuilder.history {
		if event.GetEventId() == bufferedEventID {
			newBufferedEvents = append(newBufferedEvents, event)
		} else {
			newCommittedEvents = append(newCommittedEvents, event)
		}
	}

	// no decision in-flight, flush all buffered events to committed bucket
	if !e.HasInFlightDecisionTask() {
		flush := func(bufferedEventBatch *persistence.SerializedHistoryEventBatch) error {
			// TODO: get serializer based on eventBatch's EncodingType when we support multiple encoding
			eventBatch, err := e.hBuilder.serializer.Deserialize(bufferedEventBatch)
			if err != nil {
				logging.LogHistoryDeserializationErrorEvent(e.logger, err, "Unable to serialize execution history for update.")
				return err
			}
			for _, event := range eventBatch.Events {
				newCommittedEvents = append(newCommittedEvents, event)
			}
			return nil
		}

		// flush persisted buffered events
		for _, bufferedEventBatch := range e.bufferedEvents {
			if err := flush(bufferedEventBatch); err != nil {
				return err
			}
		}
		// flush pending buffered events
		if e.updateBufferedEvents != nil {
			if err := flush(e.updateBufferedEvents); err != nil {
				return err
			}
		}

		// flush new buffered events that were not saved to persistence yet
		newCommittedEvents = append(newCommittedEvents, newBufferedEvents...)
		newBufferedEvents = nil

		// remove the persisted buffered events from persistence if there is any
		e.clearBufferedEvents = e.clearBufferedEvents || len(e.bufferedEvents) > 0
		e.bufferedEvents = nil
		// clear pending buffered events
		e.updateBufferedEvents = nil
	}

	e.hBuilder.history = newCommittedEvents
	// make sure all new committed events have correct EventID
	e.assignEventIDToBufferedEvents()

	// if decision is not closed yet, and there are new buffered events, then put those to the pending buffer
	if e.HasInFlightDecisionTask() && len(newBufferedEvents) > 0 {
		// decision in-flight, and some new events needs to be buffered
		bufferedBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), newBufferedEvents)
		serializedEvents, err := e.hBuilder.serializer.Serialize(bufferedBatch)
		if err != nil {
			logging.LogHistorySerializationErrorEvent(e.logger, err, "Unable to serialize execution history for update.")
			return err
		}
		e.updateBufferedEvents = serializedEvents
	}

	return nil
}

func (e *mutableStateBuilder) ApplyReplicationStateUpdates(failoverVersion, lastEventID int64) {
	e.replicationState.CurrentVersion = failoverVersion
	e.replicationState.LastWriteVersion = failoverVersion
	// TODO: Rename this to NextEventID to stay consistent naming convention with rest of code base
	e.replicationState.LastWriteEventID = lastEventID
}

func (e *mutableStateBuilder) CloseUpdateSession() (*mutableStateSessionUpdates, error) {
	if err := e.FlushBufferedEvents(); err != nil {
		return nil, err
	}

	updates := &mutableStateSessionUpdates{
		newEventsBuilder:           e.hBuilder,
		updateActivityInfos:        e.updateActivityInfos,
		deleteActivityInfo:         e.deleteActivityInfo,
		updateTimerInfos:           e.updateTimerInfos,
		deleteTimerInfos:           e.deleteTimerInfos,
		updateChildExecutionInfos:  e.updateChildExecutionInfos,
		deleteChildExecutionInfo:   e.deleteChildExecutionInfo,
		updateCancelExecutionInfos: e.updateRequestCancelInfos,
		deleteCancelExecutionInfo:  e.deleteRequestCancelInfo,
		updateSignalInfos:          e.updateSignalInfos,
		deleteSignalInfo:           e.deleteSignalInfo,
		updateSignalRequestedIDs:   getSignalRequestedIDs(e.updateSignalRequestedIDs),
		deleteSignalRequestedID:    e.deleteSignalRequestedID,
		continueAsNew:              e.continueAsNew,
		newBufferedEvents:          e.updateBufferedEvents,
		clearBufferedEvents:        e.clearBufferedEvents,
	}

	// Clear all updates to prepare for the next session
	e.hBuilder = newHistoryBuilder(e, e.logger)
	e.updateActivityInfos = []*persistence.ActivityInfo{}
	e.deleteActivityInfo = nil
	e.updateTimerInfos = []*persistence.TimerInfo{}
	e.deleteTimerInfos = []string{}
	e.updateChildExecutionInfos = []*persistence.ChildExecutionInfo{}
	e.deleteChildExecutionInfo = nil
	e.updateRequestCancelInfos = []*persistence.RequestCancelInfo{}
	e.deleteRequestCancelInfo = nil
	e.updateSignalInfos = []*persistence.SignalInfo{}
	e.deleteSignalInfo = nil
	e.updateSignalRequestedIDs = make(map[string]struct{})
	e.deleteSignalRequestedID = ""
	e.continueAsNew = nil
	e.clearBufferedEvents = false
	if e.updateBufferedEvents != nil {
		e.bufferedEvents = append(e.bufferedEvents, e.updateBufferedEvents)
		e.updateBufferedEvents = nil
	}

	return updates, nil
}

func (e *mutableStateBuilder) createReplicationTask() *persistence.HistoryReplicationTask {
	return &persistence.HistoryReplicationTask{
		FirstEventID:        e.GetLastFirstEventID(),
		NextEventID:         e.GetNextEventID(),
		Version:             e.replicationState.CurrentVersion,
		LastReplicationInfo: e.replicationState.LastReplicationInfo,
	}
}

func getSignalRequestedIDs(signalReqIDs map[string]struct{}) []string {
	var result []string
	for k := range signalReqIDs {
		result = append(result, k)
	}
	return result
}

func (e *mutableStateBuilder) assignEventIDToBufferedEvents() {
	newCommittedEvents := e.hBuilder.history

	scheduledIDToStartedID := make(map[int64]int64)
	for _, event := range newCommittedEvents {
		if event.GetEventId() != bufferedEventID {
			continue
		}

		eventID := e.executionInfo.NextEventID
		event.EventId = common.Int64Ptr(eventID)
		e.executionInfo.NextEventID++

		switch event.GetEventType() {
		case workflow.EventTypeActivityTaskStarted:
			attributes := event.ActivityTaskStartedEventAttributes
			scheduledID := attributes.GetScheduledEventId()
			scheduledIDToStartedID[scheduledID] = eventID
			if ai, ok := e.GetActivityInfo(scheduledID); ok {
				ai.StartedID = eventID
				e.updateActivityInfos = append(e.updateActivityInfos, ai)
			}
		case workflow.EventTypeChildWorkflowExecutionStarted:
			attributes := event.ChildWorkflowExecutionStartedEventAttributes
			initiatedID := attributes.GetInitiatedEventId()
			scheduledIDToStartedID[initiatedID] = eventID
			if ci, ok := e.GetChildExecutionInfo(initiatedID); ok {
				ci.StartedID = eventID
				e.updateChildExecutionInfos = append(e.updateChildExecutionInfos, ci)
			}
		case workflow.EventTypeActivityTaskCompleted:
			attributes := event.ActivityTaskCompletedEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeActivityTaskFailed:
			attributes := event.ActivityTaskFailedEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeActivityTaskTimedOut:
			attributes := event.ActivityTaskTimedOutEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeActivityTaskCanceled:
			attributes := event.ActivityTaskCanceledEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeChildWorkflowExecutionCompleted:
			attributes := event.ChildWorkflowExecutionCompletedEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeChildWorkflowExecutionFailed:
			attributes := event.ChildWorkflowExecutionFailedEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeChildWorkflowExecutionTimedOut:
			attributes := event.ChildWorkflowExecutionTimedOutEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeChildWorkflowExecutionCanceled:
			attributes := event.ChildWorkflowExecutionCanceledEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		case workflow.EventTypeChildWorkflowExecutionTerminated:
			attributes := event.ChildWorkflowExecutionTerminatedEventAttributes
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = common.Int64Ptr(startedID)
			}
		}
	}
}

func (e *mutableStateBuilder) isStickyTaskListEnabled() bool {
	return len(e.executionInfo.StickyTaskList) > 0
}

func (e *mutableStateBuilder) createNewHistoryEvent(eventType workflow.EventType) *workflow.HistoryEvent {
	eventID := e.executionInfo.NextEventID
	if e.shouldBufferEvent(eventType) {
		eventID = bufferedEventID
	} else {
		// only increase NextEventID if event is not buffered
		e.executionInfo.NextEventID++
	}

	return e.createNewHistoryEventWithTimestamp(eventID, eventType, time.Now().UnixNano())
}

func (e *mutableStateBuilder) shouldBufferEvent(eventType workflow.EventType) bool {
	switch eventType {
	case // do not buffer for workflow state change
		workflow.EventTypeWorkflowExecutionStarted,
		workflow.EventTypeWorkflowExecutionCompleted,
		workflow.EventTypeWorkflowExecutionFailed,
		workflow.EventTypeWorkflowExecutionTimedOut,
		workflow.EventTypeWorkflowExecutionTerminated,
		workflow.EventTypeWorkflowExecutionContinuedAsNew,
		workflow.EventTypeWorkflowExecutionCanceled:
		return false
	case // decision event should not be buffered
		workflow.EventTypeDecisionTaskScheduled,
		workflow.EventTypeDecisionTaskStarted,
		workflow.EventTypeDecisionTaskCompleted,
		workflow.EventTypeDecisionTaskFailed,
		workflow.EventTypeDecisionTaskTimedOut:
		return false
	case // events generated directly from decisions should not be buffered
		// workflow complete, failed, cancelled and continue-as-new events are duplication of above
		// just put is here for reference
		// workflow.EventTypeWorkflowExecutionCompleted,
		// workflow.EventTypeWorkflowExecutionFailed,
		// workflow.EventTypeWorkflowExecutionCanceled,
		// workflow.EventTypeWorkflowExecutionContinuedAsNew,
		workflow.EventTypeActivityTaskScheduled,
		workflow.EventTypeActivityTaskCancelRequested,
		workflow.EventTypeTimerStarted,
		// DecisionTypeCancelTimer is an excption. This decision will be mapped
		// to either workflow.EventTypeTimerCanceled, or workflow.EventTypeCancelTimerFailed.
		// So both should not be buffered. Ref: historyEngine, search for "workflow.DecisionTypeCancelTimer"
		workflow.EventTypeTimerCanceled,
		workflow.EventTypeCancelTimerFailed,
		workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated,
		workflow.EventTypeMarkerRecorded,
		workflow.EventTypeStartChildWorkflowExecutionInitiated,
		workflow.EventTypeSignalExternalWorkflowExecutionInitiated:
		// do not buffer event if event is directly generated from a corresponding decision

		// sanity check there is no decision on the fly
		if e.HasInFlightDecisionTask() {
			msg := fmt.Sprintf("history mutable state is processing event: %v while there is decision pending. "+
				"domainID: %v, workflow ID: %v, run ID: %v.", eventType, e.executionInfo.DomainID, e.executionInfo.WorkflowID, e.executionInfo.RunID)
			panic(msg)
		}
		return false
	default:
		return true
	}
}

func (e *mutableStateBuilder) createNewHistoryEventWithTimestamp(eventID int64, eventType workflow.EventType,
	timestamp int64) *workflow.HistoryEvent {
	ts := common.Int64Ptr(timestamp)
	historyEvent := &workflow.HistoryEvent{}
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = ts
	historyEvent.EventType = common.EventTypePtr(eventType)

	return historyEvent
}

func (e *mutableStateBuilder) getWorkflowType() *workflow.WorkflowType {
	wType := &workflow.WorkflowType{}
	wType.Name = common.StringPtr(e.executionInfo.WorkflowTypeName)

	return wType
}

func (e *mutableStateBuilder) getLastUpdatedTimestamp() int64 {
	lastUpdated := e.executionInfo.LastUpdatedTimestamp.UnixNano()
	if e.executionInfo.StartTimestamp.UnixNano() >= lastUpdated {
		// This could happen due to clock skews
		// ensure that the lastUpdatedTimestamp is always greater than the StartTimestamp
		lastUpdated = e.executionInfo.StartTimestamp.UnixNano() + 1
	}

	return lastUpdated
}

func (e *mutableStateBuilder) previousDecisionStartedEvent() int64 {
	return e.executionInfo.LastProcessedEvent
}

func (e *mutableStateBuilder) GetActivityScheduledEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ai.ScheduledEvent)
}

func (e *mutableStateBuilder) GetActivityStartedEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ai.StartedEvent)
}

// GetActivityInfo gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityInfo(scheduleEventID int64) (*persistence.ActivityInfo, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	return ai, ok
}

// GetActivityByActivityID gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityByActivityID(activityID string) (*persistence.ActivityInfo, bool) {
	eventID, ok := e.pendingActivityInfoByActivityID[activityID]
	if !ok {
		return nil, false
	}

	ai, ok := e.pendingActivityInfoIDs[eventID]
	return ai, ok
}

// GetScheduleIDByActivityID return scheduleID given activityID
func (e *mutableStateBuilder) GetScheduleIDByActivityID(activityID string) (int64, bool) {
	scheduleID, ok := e.pendingActivityInfoByActivityID[activityID]
	return scheduleID, ok
}

// GetChildExecutionInfo gives details about a child execution that is currently in progress.
func (e *mutableStateBuilder) GetChildExecutionInfo(initiatedEventID int64) (*persistence.ChildExecutionInfo, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	return ci, ok
}

// GetChildExecutionInitiatedEvent reads out the ChildExecutionInitiatedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionInitiatedEvent(initiatedEventID int64) (*workflow.HistoryEvent, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ci.InitiatedEvent)
}

// GetChildExecutionStartedEvent reads out the ChildExecutionStartedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionStartedEvent(initiatedEventID int64) (*workflow.HistoryEvent, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ci.StartedEvent)
}

// GetRequestCancelInfo gives details about a request cancellation that is currently in progress.
func (e *mutableStateBuilder) GetRequestCancelInfo(initiatedEventID int64) (*persistence.RequestCancelInfo, bool) {
	ri, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	return ri, ok
}

// GetSignalInfo get details about a signal request that is currently in progress.
func (e *mutableStateBuilder) GetSignalInfo(initiatedEventID int64) (*persistence.SignalInfo, bool) {
	ri, ok := e.pendingSignalInfoIDs[initiatedEventID]
	return ri, ok
}

// GetCompletionEvent retrieves the workflow completion event from mutable state
func (e *mutableStateBuilder) GetCompletionEvent() (*workflow.HistoryEvent, bool) {
	serializedEvent := e.executionInfo.CompletionEvent
	if serializedEvent == nil {
		return nil, false
	}

	return e.getHistoryEvent(serializedEvent)
}

// DeletePendingChildExecution deletes details about a ChildExecutionInfo.
func (e *mutableStateBuilder) DeletePendingChildExecution(initiatedEventID int64) error {
	_, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find child execution with initiated event id: %v in mutable state",
			initiatedEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingChildExecutionInfoIDs, initiatedEventID)

	e.deleteChildExecutionInfo = common.Int64Ptr(initiatedEventID)
	return nil
}

// DeletePendingRequestCancel deletes details about a RequestCancelInfo.
func (e *mutableStateBuilder) DeletePendingRequestCancel(initiatedEventID int64) error {
	_, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find request cancellation with initiated event id: %v in mutable state",
			initiatedEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingRequestCancelInfoIDs, initiatedEventID)

	e.deleteRequestCancelInfo = common.Int64Ptr(initiatedEventID)
	return nil
}

// DeletePendingSignal deletes details about a SignalInfo
func (e *mutableStateBuilder) DeletePendingSignal(initiatedEventID int64) error {
	_, ok := e.pendingSignalInfoIDs[initiatedEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find signal request with initiated event id: %v in mutable state",
			initiatedEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingSignalInfoIDs, initiatedEventID)

	e.deleteSignalInfo = common.Int64Ptr(initiatedEventID)
	return nil
}

func (e *mutableStateBuilder) writeCompletionEventToMutableState(completionEvent *workflow.HistoryEvent) error {
	// First check to see if this is a Child Workflow
	if e.hasParentExecution() {
		serializedEvent, err := e.eventSerializer.Serialize(completionEvent)
		if err != nil {
			return err
		}

		// Store the completion result within mutable state so we can communicate the result to parent execution
		// during the processing of DeleteTransferTask
		e.executionInfo.CompletionEvent = serializedEvent
	}

	return nil
}

func (e *mutableStateBuilder) hasPendingTasks() bool {
	return len(e.pendingActivityInfoIDs) > 0 || len(e.pendingTimerInfoIDs) > 0
}

func (e *mutableStateBuilder) hasParentExecution() bool {
	return e.executionInfo.ParentDomainID != "" && e.executionInfo.ParentWorkflowID != ""
}

func (e *mutableStateBuilder) updateActivityProgress(ai *persistence.ActivityInfo,
	request *workflow.RecordActivityTaskHeartbeatRequest) {
	ai.Details = request.Details
	ai.LastHeartBeatUpdatedTime = time.Now()
	e.updateActivityInfos = append(e.updateActivityInfos, ai)
}

// UpdateActivity updates an activity
func (e *mutableStateBuilder) UpdateActivity(ai *persistence.ActivityInfo) error {
	_, ok := e.pendingActivityInfoIDs[ai.ScheduleID]
	if !ok {
		return fmt.Errorf("Unable to find activity with schedule event id: %v in mutable state", ai.ScheduleID)
	}
	e.updateActivityInfos = append(e.updateActivityInfos, ai)
	return nil
}

// DeleteActivity deletes details about an activity.
func (e *mutableStateBuilder) DeleteActivity(scheduleEventID int64) error {
	a, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find activity with schedule event id: %v in mutable state", scheduleEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingActivityInfoIDs, scheduleEventID)

	_, ok = e.pendingActivityInfoByActivityID[a.ActivityID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find activity: %v in mutable state", a.ActivityID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingActivityInfoByActivityID, a.ActivityID)

	e.deleteActivityInfo = common.Int64Ptr(scheduleEventID)
	return nil
}

// GetUserTimer gives details about a user timer.
func (e *mutableStateBuilder) GetUserTimer(timerID string) (bool, *persistence.TimerInfo) {
	a, ok := e.pendingTimerInfoIDs[timerID]
	return ok, a
}

// UpdateUserTimer updates the user timer in progress.
func (e *mutableStateBuilder) UpdateUserTimer(timerID string, ti *persistence.TimerInfo) {
	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos = append(e.updateTimerInfos, ti)
}

// DeleteUserTimer deletes an user timer.
func (e *mutableStateBuilder) DeleteUserTimer(timerID string) error {
	_, ok := e.pendingTimerInfoIDs[timerID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find pending timer: %v", timerID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingTimerInfoIDs, timerID)

	e.deleteTimerInfos = append(e.deleteTimerInfos, timerID)
	return nil
}

// GetPendingDecision returns details about the in-progress decision task
func (e *mutableStateBuilder) GetPendingDecision(scheduleEventID int64) (*decisionInfo, bool) {
	di := &decisionInfo{
		ScheduleID:      e.executionInfo.DecisionScheduleID,
		StartedID:       e.executionInfo.DecisionStartedID,
		RequestID:       e.executionInfo.DecisionRequestID,
		DecisionTimeout: e.executionInfo.DecisionTimeout,
		Attempt:         e.executionInfo.DecisionAttempt,
		Timestamp:       e.executionInfo.DecisionTimestamp,
	}
	if scheduleEventID == di.ScheduleID {
		return di, true
	}
	return nil, false
}

func (e *mutableStateBuilder) HasPendingDecisionTask() bool {
	return e.executionInfo.DecisionScheduleID != emptyEventID
}

func (e *mutableStateBuilder) HasInFlightDecisionTask() bool {
	return e.executionInfo.DecisionStartedID > 0
}

func (e *mutableStateBuilder) HasBufferedEvents() bool {
	if len(e.bufferedEvents) > 0 || e.updateBufferedEvents != nil {
		return true
	}

	for _, event := range e.hBuilder.history {
		if event.GetEventId() == bufferedEventID {
			return true
		}
	}

	return false
}

// UpdateDecision updates a decision task.
func (e *mutableStateBuilder) UpdateDecision(di *decisionInfo) {
	e.executionInfo.DecisionScheduleID = di.ScheduleID
	e.executionInfo.DecisionStartedID = di.StartedID
	e.executionInfo.DecisionRequestID = di.RequestID
	e.executionInfo.DecisionTimeout = di.DecisionTimeout
	e.executionInfo.DecisionAttempt = di.Attempt
	e.executionInfo.DecisionTimestamp = di.Timestamp

	e.logger.Debugf("Decision Updated: {Schedule: %v, Started: %v, ID: %v, Timeout: %v, Attempt: %v, Timestamp: %v}",
		di.ScheduleID, di.StartedID, di.RequestID, di.DecisionTimeout, di.Attempt, di.Timestamp)
}

// DeleteDecision deletes a decision task.
func (e *mutableStateBuilder) DeleteDecision() {
	emptyDecisionInfo := &decisionInfo{
		ScheduleID:      emptyEventID,
		StartedID:       emptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: 0,
		Attempt:         0,
		Timestamp:       0,
	}
	e.UpdateDecision(emptyDecisionInfo)
}

func (e *mutableStateBuilder) FailDecision() {
	// Clear stickiness whenever decision fails
	e.clearStickyness()

	failDecisionInfo := &decisionInfo{
		ScheduleID:      emptyEventID,
		StartedID:       emptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: 0,
		Attempt:         e.executionInfo.DecisionAttempt + 1,
	}
	e.UpdateDecision(failDecisionInfo)
}

func (e *mutableStateBuilder) clearStickyness() {
	e.executionInfo.StickyTaskList = ""
	e.executionInfo.StickyScheduleToStartTimeout = 0
	e.executionInfo.ClientLibraryVersion = ""
	e.executionInfo.ClientFeatureVersion = ""
	e.executionInfo.ClientImpl = ""
}

// GetLastFirstEventID returns last first event ID
// first event ID is the ID of a batch of events in a single history events record
func (e *mutableStateBuilder) GetLastFirstEventID() int64 {
	return e.executionInfo.LastFirstEventID
}

// GetNextEventID returns next event ID
func (e *mutableStateBuilder) GetNextEventID() int64 {
	return e.executionInfo.NextEventID
}

func (e *mutableStateBuilder) isWorkflowExecutionRunning() bool {
	return e.executionInfo.State != persistence.WorkflowStateCompleted
}

func (e *mutableStateBuilder) isCancelRequested() (bool, string) {
	if e.executionInfo.CancelRequested {
		return e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID
	}

	return false, ""
}

func (e *mutableStateBuilder) isSignalRequested(requestID string) bool {
	if _, ok := e.pendingSignalRequestedIDs[requestID]; ok {
		return true
	}
	return false
}

func (e *mutableStateBuilder) addSignalRequested(requestID string) {
	if e.pendingSignalRequestedIDs == nil {
		e.pendingSignalRequestedIDs = make(map[string]struct{})
	}
	if e.updateSignalRequestedIDs == nil {
		e.updateSignalRequestedIDs = make(map[string]struct{})
	}
	e.pendingSignalRequestedIDs[requestID] = struct{}{} // add requestID to set
	e.updateSignalRequestedIDs[requestID] = struct{}{}
}

func (e *mutableStateBuilder) deleteSignalRequested(requestID string) {
	delete(e.pendingSignalRequestedIDs, requestID)
	e.deleteSignalRequestedID = requestID
}

func (e *mutableStateBuilder) getHistoryEvent(serializedEvent []byte) (*workflow.HistoryEvent, bool) {
	event, err := e.eventSerializer.Deserialize(serializedEvent)
	if err != nil {
		return nil, false
	}

	return event, true
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEventForContinueAsNew(domainID string,
	execution workflow.WorkflowExecution, previousExecutionState *mutableStateBuilder,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	taskList := previousExecutionState.executionInfo.TaskList
	if attributes.TaskList != nil {
		taskList = *attributes.TaskList.Name
	}
	tl := &workflow.TaskList{}
	tl.Name = common.StringPtr(taskList)

	workflowType := previousExecutionState.executionInfo.WorkflowTypeName
	if attributes.WorkflowType != nil {
		workflowType = *attributes.WorkflowType.Name
	}
	wType := &workflow.WorkflowType{}
	wType.Name = common.StringPtr(workflowType)

	decisionTimeout := previousExecutionState.executionInfo.DecisionTimeoutValue
	if attributes.TaskStartToCloseTimeoutSeconds != nil {
		decisionTimeout = *attributes.TaskStartToCloseTimeoutSeconds
	}

	createRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(previousExecutionState.executionInfo.DomainID),
		WorkflowId:                          common.StringPtr(*execution.WorkflowId),
		TaskList:                            tl,
		WorkflowType:                        wType,
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(decisionTimeout),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(*attributes.ExecutionStartToCloseTimeoutSeconds),
		Input:    attributes.Input,
		Identity: nil,
	}

	return e.AddWorkflowExecutionStartedEvent(domainID, execution, createRequest)
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEvent(domainID string, execution workflow.WorkflowExecution,
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	eventID := e.GetNextEventID()
	if eventID != firstEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowStarted, eventID, "")
		return nil
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(request)
	e.ReplicateWorkflowExecutionStartedEvent(domainID, execution, request.GetRequestId(),
		event.WorkflowExecutionStartedEventAttributes)

	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionStartedEvent(domainID string,
	execution workflow.WorkflowExecution, requestID string, event *workflow.WorkflowExecutionStartedEventAttributes) {
	e.executionInfo.DomainID = domainID
	e.executionInfo.WorkflowID = execution.GetWorkflowId()
	e.executionInfo.RunID = execution.GetRunId()
	e.executionInfo.TaskList = event.TaskList.GetName()
	e.executionInfo.WorkflowTypeName = event.WorkflowType.GetName()
	e.executionInfo.WorkflowTimeout = event.GetExecutionStartToCloseTimeoutSeconds()
	e.executionInfo.DecisionTimeoutValue = event.GetTaskStartToCloseTimeoutSeconds()

	e.executionInfo.State = persistence.WorkflowStateCreated
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusNone
	e.executionInfo.LastProcessedEvent = emptyEventID
	e.executionInfo.CreateRequestID = requestID
	e.executionInfo.DecisionScheduleID = emptyEventID
	e.executionInfo.DecisionStartedID = emptyEventID
	e.executionInfo.DecisionRequestID = emptyUUID
	e.executionInfo.DecisionTimeout = 0
}

func (e *mutableStateBuilder) AddDecisionTaskScheduledEvent() *decisionInfo {
	if e.HasPendingDecisionTask() {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskScheduled, e.GetNextEventID(),
			fmt.Sprintf("{Pending Decision ScheduleID: %v}", e.executionInfo.DecisionScheduleID))
		return nil
	}

	// Tasklist and decision timeout should already be set from workflow execution started event
	taskList := e.executionInfo.TaskList
	if e.isStickyTaskListEnabled() {
		taskList = e.executionInfo.StickyTaskList
	}
	startToCloseTimeoutSeconds := e.executionInfo.DecisionTimeoutValue

	// Flush any buffered events before creating the decision, otherwise it will result in invalid IDs for transient
	// decision and will cause in timeout processing to not work for transient decisions
	if err := e.FlushBufferedEvents(); err != nil {
		return nil
	}

	var newDecisionEvent *workflow.HistoryEvent
	scheduleID := e.GetNextEventID() // we will generate the schedule event later for repeatedly failing decisions
	// Avoid creating new history events when decisions are continuously failing
	if e.executionInfo.DecisionAttempt == 0 {
		newDecisionEvent = e.hBuilder.AddDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds,
			e.executionInfo.DecisionAttempt)
		scheduleID = newDecisionEvent.GetEventId()
	}

	return e.ReplicateDecisionTaskScheduledEvent(scheduleID, taskList, startToCloseTimeoutSeconds)
}

func (e *mutableStateBuilder) ReplicateDecisionTaskScheduledEvent(scheduleID int64, taskList string,
	startToCloseTimeoutSeconds int32) *decisionInfo {
	di := &decisionInfo{
		ScheduleID:      scheduleID,
		StartedID:       emptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: startToCloseTimeoutSeconds,
		Tasklist:        taskList,
		Attempt:         e.executionInfo.DecisionAttempt,
	}

	e.UpdateDecision(di)
	return di
}

func (e *mutableStateBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	request *workflow.PollForDecisionTaskRequest) (*workflow.HistoryEvent, *decisionInfo) {
	hasPendingDecision := e.HasPendingDecisionTask()
	di, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || di.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskStarted, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, Exist: %v, Value: %v}", hasPendingDecision, scheduleEventID, ok, e))
		return nil, nil
	}

	var event *workflow.HistoryEvent
	scheduleID := di.ScheduleID
	startedID := scheduleID + 1
	tasklist := request.TaskList.GetName()
	timestamp := time.Now().UnixNano()
	// First check to see if new events came since transient decision was scheduled
	if di.Attempt > 0 && di.ScheduleID != e.GetNextEventID() {
		// Also create a new DecisionTaskScheduledEvent since new events came in when it was scheduled
		scheduleEvent := e.hBuilder.AddDecisionTaskScheduledEvent(tasklist, di.DecisionTimeout, 0)
		scheduleID = scheduleEvent.GetEventId()
		di.Attempt = 0
	}

	// Avoid creating new history events when decisions are continuously failing
	if di.Attempt == 0 {
		// Now create DecisionTaskStartedEvent
		event = e.hBuilder.AddDecisionTaskStartedEvent(scheduleID, requestID, request.GetIdentity())
		startedID = event.GetEventId()
		timestamp = int64(0)
	}

	di = e.ReplicateDecisionTaskStartedEvent(di, scheduleID, startedID, requestID, timestamp)
	return event, di
}

func (e *mutableStateBuilder) ReplicateDecisionTaskStartedEvent(di *decisionInfo, scheduleID, startedID int64,
	requestID string, timestamp int64) *decisionInfo {
	// Replicator calls it with a nil decision info, and it is safe to always lookup the decision in this case as it
	// does not have to deal with transient decision case.
	if di == nil {
		di, _ = e.GetPendingDecision(scheduleID)
	}

	e.executionInfo.State = persistence.WorkflowStateRunning
	// Update mutable decision state
	di = &decisionInfo{
		ScheduleID:      scheduleID,
		StartedID:       startedID,
		RequestID:       requestID,
		DecisionTimeout: di.DecisionTimeout,
		Attempt:         di.Attempt,
		Timestamp:       timestamp,
	}

	e.UpdateDecision(di)
	return di
}

func (e *mutableStateBuilder) createTransientDecisionEvents(di *decisionInfo, identity string) (*workflow.HistoryEvent,
	*workflow.HistoryEvent) {
	tasklist := e.executionInfo.TaskList
	scheduledEvent := newDecisionTaskScheduledEventWithInfo(di.ScheduleID, di.Timestamp, tasklist, di.DecisionTimeout,
		di.Attempt)
	startedEvent := newDecisionTaskStartedEventWithInfo(di.StartedID, di.Timestamp, di.ScheduleID, di.RequestID,
		identity)

	return scheduledEvent, startedEvent
}

func (e *mutableStateBuilder) BeforeAddDecisionTaskCompletedEvent() {
	// Make sure to delete decision before adding events.  Otherwise they are buffered rather than getting appended
	e.DeleteDecision()
}

func (e *mutableStateBuilder) AfterAddDecisionTaskCompletedEvent(startedID int64) {
	e.executionInfo.LastProcessedEvent = startedID
}

func (e *mutableStateBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	di, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || di.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
			startedEventID, ok))
		return nil
	}

	e.BeforeAddDecisionTaskCompletedEvent()
	if di.Attempt > 0 {
		// Create corresponding DecisionTaskSchedule and DecisionTaskStarted events for decisions we have been retrying
		scheduledEvent := e.hBuilder.AddDecisionTaskScheduledEvent(e.executionInfo.TaskList, di.DecisionTimeout, di.Attempt)
		startedEvent := e.hBuilder.AddDecisionTaskStartedEvent(scheduledEvent.GetEventId(), di.RequestID,
			request.GetIdentity())
		startedEventID = startedEvent.GetEventId()
	}
	// Now write the completed event
	event := e.hBuilder.AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	e.AfterAddDecisionTaskCompletedEvent(startedEventID)
	return event
}

func (e *mutableStateBuilder) ReplicateDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64) {
	e.BeforeAddDecisionTaskCompletedEvent()
	e.AfterAddDecisionTaskCompletedEvent(startedEventID)
}

func (e *mutableStateBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	dt, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || dt.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskTimedOut, e.GetNextEventID(),
			fmt.Sprintf("{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
				startedEventID, ok))
		return nil
	}

	var event *workflow.HistoryEvent
	// Avoid creating new history events when decisions are continuously timing out
	if dt.Attempt == 0 {
		event = e.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, workflow.TimeoutTypeStartToClose)
	}

	e.ReplicateDecisionTaskTimedOutEvent(scheduleEventID, startedEventID)
	return event
}

func (e *mutableStateBuilder) ReplicateDecisionTaskTimedOutEvent(scheduleID, startedID int64) {
	e.FailDecision()
}

func (e *mutableStateBuilder) AddDecisionTaskScheduleToStartTimeoutEvent(scheduleEventID int64) *workflow.HistoryEvent {
	if e.executionInfo.DecisionScheduleID != scheduleEventID || e.executionInfo.DecisionStartedID > 0 {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskTimedOut, e.GetNextEventID(),
			fmt.Sprintf("{DecisionScheduleID: %v, DecisionStartedID: %v, ScheduleEventID: %v}",
				e.executionInfo.DecisionScheduleID, e.executionInfo.DecisionStartedID, scheduleEventID))
		return nil
	}

	// Clear stickiness whenever decision fails
	e.clearStickyness()

	event := e.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, 0, workflow.TimeoutTypeScheduleToStart)

	e.ReplicateDecisionTaskTimedOutEvent(scheduleEventID, emptyEventID)
	return event
}

func (e *mutableStateBuilder) AddDecisionTaskFailedEvent(scheduleEventID int64,
	startedEventID int64, cause workflow.DecisionTaskFailedCause, details []byte,
	identity string) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	dt, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || dt.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskFailed, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
			startedEventID, ok))
		return nil
	}

	var event *workflow.HistoryEvent
	// Only emit DecisionTaskFailedEvent for the very first time
	if dt.Attempt == 0 {
		event = e.hBuilder.AddDecisionTaskFailedEvent(scheduleEventID, startedEventID, cause, details, identity)
	}

	e.ReplicateDecisionTaskFailedEvent(scheduleEventID, startedEventID)
	return event
}

func (e *mutableStateBuilder) ReplicateDecisionTaskFailedEvent(scheduleID, startedID int64) {
	e.FailDecision()
}

func (e *mutableStateBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) (*workflow.HistoryEvent, *persistence.ActivityInfo) {
	if ai, ok := e.GetActivityInfo(e.GetNextEventID()); ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskScheduled, ai.ScheduleID, fmt.Sprintf(
			"{Exist: %v, Value: %v}", ok, ai.StartedID))
		return nil, nil
	}

	if attributes.ActivityId == nil {
		return nil, nil
	}

	event := e.hBuilder.AddActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	ai := e.ReplicateActivityTaskScheduledEvent(event)
	return event, ai
}

func (e *mutableStateBuilder) ReplicateActivityTaskScheduledEvent(
	event *workflow.HistoryEvent) *persistence.ActivityInfo {
	attributes := event.ActivityTaskScheduledEventAttributes
	scheduleEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil
	}

	scheduleEventID := *event.EventId
	var scheduleToStartTimeout int32
	if attributes.ScheduleToStartTimeoutSeconds == nil || attributes.GetScheduleToStartTimeoutSeconds() <= 0 {
		scheduleToStartTimeout = e.config.DefaultScheduleToStartActivityTimeoutInSecs
	} else {
		scheduleToStartTimeout = attributes.GetScheduleToStartTimeoutSeconds()
	}

	var scheduleToCloseTimeout int32
	if attributes.ScheduleToCloseTimeoutSeconds == nil || attributes.GetScheduleToCloseTimeoutSeconds() <= 0 {
		scheduleToCloseTimeout = e.config.DefaultScheduleToCloseActivityTimeoutInSecs
	} else {
		scheduleToCloseTimeout = attributes.GetScheduleToCloseTimeoutSeconds()
	}

	var startToCloseTimeout int32
	if attributes.StartToCloseTimeoutSeconds == nil || attributes.GetStartToCloseTimeoutSeconds() <= 0 {
		startToCloseTimeout = e.config.DefaultStartToCloseActivityTimeoutInSecs
	} else {
		startToCloseTimeout = attributes.GetStartToCloseTimeoutSeconds()
	}

	var heartbeatTimeout int32
	if attributes.HeartbeatTimeoutSeconds != nil {
		heartbeatTimeout = attributes.GetHeartbeatTimeoutSeconds()
	}

	ai := &persistence.ActivityInfo{
		ScheduleID:               scheduleEventID,
		ScheduledEvent:           scheduleEvent,
		ScheduledTime:            time.Unix(0, *event.Timestamp),
		StartedID:                emptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               common.StringDefault(attributes.ActivityId),
		ScheduleToStartTimeout:   scheduleToStartTimeout,
		ScheduleToCloseTimeout:   scheduleToCloseTimeout,
		StartToCloseTimeout:      startToCloseTimeout,
		HeartbeatTimeout:         heartbeatTimeout,
		CancelRequested:          false,
		CancelRequestID:          emptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
	}

	e.pendingActivityInfoIDs[scheduleEventID] = ai
	e.pendingActivityInfoByActivityID[ai.ActivityID] = scheduleEventID
	e.updateActivityInfos = append(e.updateActivityInfos, ai)

	return ai
}

func (e *mutableStateBuilder) AddActivityTaskStartedEvent(ai *persistence.ActivityInfo, scheduleEventID int64,
	requestID string, request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskStarted, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, Exist: %v}", scheduleEventID, ok))
		return nil
	}

	event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, requestID, request)

	e.ReplicateActivityTaskStartedEvent(event)
	return event
}

func (e *mutableStateBuilder) ReplicateActivityTaskStartedEvent(event *workflow.HistoryEvent) {
	attributes := event.ActivityTaskStartedEventAttributes
	scheduleID := attributes.GetScheduledEventId()
	ai, _ := e.GetActivityInfo(scheduleID)

	ai.StartedID = event.GetEventId()
	ai.RequestID = attributes.GetRequestId()
	ai.StartedTime = time.Unix(0, event.GetTimestamp())
	e.updateActivityInfos = append(e.updateActivityInfos, ai)
}

func (e *mutableStateBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	event := e.hBuilder.AddActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)
	if err := e.ReplicateActivityTaskCompletedEvent(event); err != nil {
		return nil
	}

	return event
}

func (e *mutableStateBuilder) ReplicateActivityTaskCompletedEvent(event *workflow.HistoryEvent) error {
	attributes := event.ActivityTaskCompletedEventAttributes
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskFailed, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskFailedEvent(scheduleEventID, startedEventID, request)
}

func (e *mutableStateBuilder) AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID int64,
	timeoutType workflow.TimeoutType, lastHeartBeatDetails []byte) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID ||
		((timeoutType == workflow.TimeoutTypeStartToClose || timeoutType == workflow.TimeoutTypeHeartbeat) &&
			ai.StartedID == emptyEventID) {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskTimedOut, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, TimeOutType: %v, Exist: %v}", scheduleEventID, startedEventID,
			timeoutType, ok))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails)
}

func (e *mutableStateBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID, identity string) (*workflow.HistoryEvent, *persistence.ActivityInfo, bool) {
	actCancelReqEvent := e.hBuilder.AddActivityTaskCancelRequestedEvent(decisionCompletedEventID, activityID)

	ai, isRunning := e.GetActivityByActivityID(activityID)
	if !isRunning || ai.CancelRequested {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCancelRequest, e.GetNextEventID(), fmt.Sprintf(
			"{isRunning: %v, ActivityID: %v}", isRunning, activityID))
		return nil, nil, false
	}

	// - We have the activity dispatched to worker.
	// - The activity might not be heartbeat'ing, but the activity can still call RecordActivityHeartBeat()
	//   to see cancellation while reporting progress of the activity.
	ai.CancelRequested = true
	ai.CancelRequestID = *actCancelReqEvent.EventId
	e.updateActivityInfos = append(e.updateActivityInfos, ai)

	return actCancelReqEvent, ai, isRunning
}

func (e *mutableStateBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *workflow.HistoryEvent {
	return e.hBuilder.AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID, activityID, cause)
}

func (e *mutableStateBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details []byte, identity string) *workflow.HistoryEvent {
	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	// Verify cancel request as well.
	if !ai.CancelRequested {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{No outstanding cancel request. ScheduleID: %v, ActivityID: %v, Exist: %v, Value: %v}",
			scheduleEventID, ai.ActivityID, ok, ai.StartedID))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskCanceledEvent(scheduleEventID, startedEventID, latestCancelRequestedEventID,
		details, identity)
}

func (e *mutableStateBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionCompleteWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	event := e.hBuilder.AddCompletedWorkflowEvent(decisionCompletedEventID, attributes)
	e.ReplicateWorkflowExecutionCompletedEvent(event)
	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCompletedEvent(event *workflow.HistoryEvent) {
	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusCompleted
	e.writeCompletionEventToMutableState(event)
}

func (e *mutableStateBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionFailWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusFailed
	event := e.hBuilder.AddFailWorkflowEvent(decisionCompletedEventID, attributes)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddTimeoutWorkflowEvent() *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimeoutWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusTimedOut
	event := e.hBuilder.AddTimeoutWorkflowEvent()
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted || e.executionInfo.CancelRequested {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionRequestCancelWorkflow, e.GetNextEventID(),
			fmt.Sprintf("{State: %v, CancelRequested: %v, RequestID: %v}", e.executionInfo.State,
				e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID))

		return nil
	}

	e.executionInfo.CancelRequested = true
	if request.CancelRequest.RequestId != nil {
		e.executionInfo.CancelRequestID = *request.CancelRequest.RequestId
	}

	return e.hBuilder.AddWorkflowExecutionCancelRequestedEvent(cause, request)
}

func (e *mutableStateBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusCanceled
	event := e.hBuilder.AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	decisionCompletedEventID int64, cancelRequestID string,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.RequestCancelInfo) {
	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	if event == nil {
		return nil, nil
	}

	initiatedEventID := *event.EventId
	ri := &persistence.RequestCancelInfo{
		InitiatedID:     initiatedEventID,
		CancelRequestID: cancelRequestID,
	}

	e.pendingRequestCancelInfoIDs[initiatedEventID] = ri
	e.updateRequestCancelInfos = append(e.updateRequestCancelInfos, ri)

	return event, ri
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCancelRequested, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))

		return nil
	}

	if e.DeletePendingRequestCancel(initiatedID) == nil {
		return e.hBuilder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	}

	return nil
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID, initiatedID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCancelFailed, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))

		return nil
	}

	if e.DeletePendingRequestCancel(initiatedID) == nil {
		return e.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID,
			domain, workflowID, runID, cause)
	}

	return nil
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	signalRequestID string, request *workflow.SignalExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {

	event := e.hBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	if event == nil {
		return nil
	}

	initiatedEventID := *event.EventId
	ri := &persistence.SignalInfo{
		InitiatedID:     initiatedEventID,
		SignalRequestID: signalRequestID,
		SignalName:      request.GetSignalName(),
		Input:           request.Input,
		Control:         request.Control,
	}

	e.pendingSignalInfoIDs[initiatedEventID] = ri
	e.updateSignalInfos = append(e.updateSignalInfos, ri)

	return event
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionSignaled(initiatedID int64,
	domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	_, ok := e.GetSignalInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignalRequested, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	if err := e.DeletePendingSignal(initiatedID); err == nil {
		return e.hBuilder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	}

	logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignalRequested, e.GetNextEventID(),
		fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
	return nil
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID, initiatedID int64, domain, workflowID, runID string,
	control []byte, cause workflow.SignalExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {

	_, ok := e.GetSignalInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignalFailed, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))

		return nil
	}

	if e.DeletePendingSignal(initiatedID) == nil {
		return e.hBuilder.AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID,
			domain, workflowID, runID, control, cause)
	}

	logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignalRequested, e.GetNextEventID(),
		fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
	return nil
}

func (e *mutableStateBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	timerID := *request.TimerId
	isTimerRunning, ti := e.GetUserTimer(timerID)
	if isTimerRunning {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerStarted, e.GetNextEventID(), fmt.Sprintf(
			"{IsTimerRunning: %v, TimerID: %v, StartedID: %v}", isTimerRunning, timerID, ti.StartedID))
		return nil, nil
	}

	event := e.hBuilder.AddTimerStartedEvent(decisionCompletedEventID, request)

	fireTimeout := time.Duration(*request.StartToFireTimeoutSeconds) * time.Second
	// TODO: Time skew need to be taken in to account.
	expiryTime := time.Now().Add(fireTimeout)
	ti = &persistence.TimerInfo{
		TimerID:    timerID,
		ExpiryTime: expiryTime,
		StartedID:  *event.EventId,
		TaskID:     TimerTaskStatusNone,
	}

	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos = append(e.updateTimerInfos, ti)

	return event, ti
}

func (e *mutableStateBuilder) AddTimerFiredEvent(startedEventID int64, timerID string) *workflow.HistoryEvent {
	isTimerRunning, _ := e.GetUserTimer(timerID)
	if !isTimerRunning {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerFired, e.GetNextEventID(), fmt.Sprintf(
			"{startedEventID: %v, Exist: %v, TimerID: %v}", startedEventID, isTimerRunning, timerID))
		return nil
	}

	// Timer is running.
	err := e.DeleteUserTimer(timerID)
	if err != nil {
		return nil
	}

	return e.hBuilder.AddTimerFiredEvent(startedEventID, timerID)
}

func (e *mutableStateBuilder) AddTimerCanceledEvent(decisionCompletedEventID int64,
	attributes *workflow.CancelTimerDecisionAttributes, identity string) *workflow.HistoryEvent {
	timerID := *attributes.TimerId
	isTimerRunning, ti := e.GetUserTimer(timerID)
	if !isTimerRunning {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{IsTimerRunning: %v, timerID: %v}", isTimerRunning, timerID))
		return nil
	}

	// Timer is running.
	err := e.DeleteUserTimer(timerID)
	if err != nil {
		return nil
	}
	return e.hBuilder.AddTimerCanceledEvent(ti.StartedID, decisionCompletedEventID, timerID, identity)
}

func (e *mutableStateBuilder) AddCancelTimerFailedEvent(decisionCompletedEventID int64,
	attributes *workflow.CancelTimerDecisionAttributes, identity string) *workflow.HistoryEvent {
	// No Operation: We couldn't cancel it probably TIMER_ID_UNKNOWN
	timerID := *attributes.TimerId
	return e.hBuilder.AddCancelTimerFailedEvent(timerID, decisionCompletedEventID,
		timerCancelationMsgTimerIDUnknown, identity)
}

func (e *mutableStateBuilder) AddRecordMarkerEvent(decisionCompletedEventID int64,
	attributes *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {

	return e.hBuilder.AddMarkerRecordedEvent(decisionCompletedEventID, attributes)
}

func (e *mutableStateBuilder) AddWorkflowExecutionTerminatedEvent(
	request *workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowTerminated, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusTerminated
	event := e.hBuilder.AddWorkflowExecutionTerminatedEvent(request)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddWorkflowExecutionSignaled(
	request *workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignaled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	return e.hBuilder.AddWorkflowExecutionSignaledEvent(request)
}

func (e *mutableStateBuilder) AddContinueAsNewEvent(decisionCompletedEventID int64, domainID, newRunID string,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *mutableStateBuilder,
	error) {
	if e.hasPendingTasks() || e.HasPendingDecisionTask() {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionContinueAsNew, e.GetNextEventID(), fmt.Sprintf(
			"{OutStandingActivityTasks: %v, HasPendingDecision: %v}", len(e.pendingActivityInfoIDs),
			e.HasPendingDecisionTask()))
	}
	prevRunID := e.executionInfo.RunID
	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusContinuedAsNew
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(e.executionInfo.WorkflowID),
		RunId:      common.StringPtr(newRunID),
	}

	newStateBuilder := newMutableStateBuilder(e.config, e.logger)
	startedEvent := newStateBuilder.AddWorkflowExecutionStartedEventForContinueAsNew(domainID, newExecution, e,
		attributes)
	if startedEvent == nil {
		return nil, nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}

	di := newStateBuilder.AddDecisionTaskScheduledEvent()
	if di == nil {
		return nil, nil, &workflow.InternalServiceError{Message: "Failed to add decision started event."}
	}

	parentDomainID := ""
	var parentExecution *workflow.WorkflowExecution
	initiatedID := emptyEventID
	if e.hasParentExecution() {
		parentDomainID = e.executionInfo.ParentDomainID
		parentExecution = &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(e.executionInfo.ParentWorkflowID),
			RunId:      common.StringPtr(e.executionInfo.ParentRunID),
		}
		initiatedID = e.executionInfo.InitiatedID
	}

	var replicationState *persistence.ReplicationState
	var replicationTasks []persistence.Task
	if e.replicationState != nil {
		failoverVersion := e.replicationState.CurrentVersion
		replicationState = &persistence.ReplicationState{
			CurrentVersion:   failoverVersion,
			StartVersion:     failoverVersion,
			LastWriteVersion: failoverVersion,
			LastWriteEventID: di.ScheduleID,
		}

		replicationTask := &persistence.HistoryReplicationTask{
			FirstEventID:        firstEventID,
			NextEventID:         newStateBuilder.GetNextEventID(),
			Version:             failoverVersion,
			LastReplicationInfo: nil,
		}
		replicationTasks = append(replicationTasks, replicationTask)
	}

	e.continueAsNew = &persistence.CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            newExecution,
		ParentDomainID:       parentDomainID,
		ParentExecution:      parentExecution,
		InitiatedID:          initiatedID,
		TaskList:             newStateBuilder.executionInfo.TaskList,
		WorkflowTypeName:     newStateBuilder.executionInfo.WorkflowTypeName,
		WorkflowTimeout:      newStateBuilder.executionInfo.WorkflowTimeout,
		DecisionTimeoutValue: newStateBuilder.executionInfo.DecisionTimeoutValue,
		ExecutionContext:     nil,
		NextEventID:          newStateBuilder.GetNextEventID(),
		LastProcessedEvent:   common.EmptyEventID,
		TransferTasks: []persistence.Task{&persistence.DecisionTask{
			DomainID:   domainID,
			TaskList:   newStateBuilder.executionInfo.TaskList,
			ScheduleID: di.ScheduleID,
		}},
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionStartToCloseTimeout: di.DecisionTimeout,
		ContinueAsNew:               true,
		PreviousRunID:               prevRunID,
		ReplicationState:            replicationState,
		ReplicationTasks:            replicationTasks,
	}

	return e.hBuilder.AddContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes), newStateBuilder, nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	createRequestID string, attributes *workflow.StartChildWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent,
	*persistence.ChildExecutionInfo) {
	event := e.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

	initiatedEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil, nil
	}

	initiatedEventID := *event.EventId
	ci := &persistence.ChildExecutionInfo{
		InitiatedID:     initiatedEventID,
		InitiatedEvent:  initiatedEvent,
		StartedID:       emptyEventID,
		CreateRequestID: createRequestID,
	}

	e.pendingChildExecutionInfoIDs[initiatedEventID] = ci
	e.updateChildExecutionInfos = append(e.updateChildExecutionInfos, ci)

	return event, ci
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionStartedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID int64) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionStarted, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	event := e.hBuilder.AddChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID)

	startedEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil
	}

	ci.StartedID = *event.EventId
	ci.StartedEvent = startedEvent
	e.updateChildExecutionInfos = append(e.updateChildExecutionInfos, ci)

	return event
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause workflow.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionStartChildExecutionFailed, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCompletedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionCompletedEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionFailedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionFailed, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionFailedEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCanceledEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionCanceledEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTerminatedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionTerminated, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionTerminatedEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTimedOutEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionTimedOut, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionTimedOutEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}
