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
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

const (
	emptyUUID = "emptyUuid"
)

type (
	mutableState interface {
		AddActivityTaskCancelRequestedEvent(int64, string, string) (*workflow.HistoryEvent, *persistence.ActivityInfo, bool)
		AddActivityTaskCanceledEvent(int64, int64, int64, []uint8, string) *workflow.HistoryEvent
		AddActivityTaskCompletedEvent(int64, int64, *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent
		AddActivityTaskFailedEvent(int64, int64, *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent
		AddActivityTaskScheduledEvent(int64, *workflow.ScheduleActivityTaskDecisionAttributes) (*workflow.HistoryEvent, *persistence.ActivityInfo)
		AddActivityTaskStartedEvent(*persistence.ActivityInfo, int64, string, string) *workflow.HistoryEvent
		AddActivityTaskTimedOutEvent(int64, int64, workflow.TimeoutType, []uint8) *workflow.HistoryEvent
		AddCancelTimerFailedEvent(int64, *workflow.CancelTimerDecisionAttributes, string) *workflow.HistoryEvent
		AddChildWorkflowExecutionCanceledEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionCompletedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionFailedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionStartedEvent(*string, *workflow.WorkflowExecution, *workflow.WorkflowType, int64) *workflow.HistoryEvent
		AddChildWorkflowExecutionTerminatedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionTimedOutEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent
		AddCompletedWorkflowEvent(int64, *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddContinueAsNewEvent(int64, *cache.DomainCacheEntry, string, *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, mutableState, error)
		AddDecisionTaskCompletedEvent(int64, int64, *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent
		AddDecisionTaskFailedEvent(int64, int64, workflow.DecisionTaskFailedCause, []uint8, string) *workflow.HistoryEvent
		AddDecisionTaskScheduleToStartTimeoutEvent(int64) *workflow.HistoryEvent
		AddDecisionTaskScheduledEvent() *decisionInfo
		AddDecisionTaskStartedEvent(int64, string, *workflow.PollForDecisionTaskRequest) (*workflow.HistoryEvent, *decisionInfo)
		AddDecisionTaskTimedOutEvent(int64, int64) *workflow.HistoryEvent
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) *workflow.HistoryEvent
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, []uint8) *workflow.HistoryEvent
		AddFailWorkflowEvent(int64, *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddRecordMarkerEvent(int64, *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent
		AddRequestCancelActivityTaskFailedEvent(int64, string, string) *workflow.HistoryEvent
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.RequestCancelInfo)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, []uint8, workflow.SignalExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *workflow.SignalExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.SignalInfo)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, workflow.ChildWorkflowExecutionFailedCause, *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *workflow.StartChildWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.ChildExecutionInfo)
		AddTimeoutWorkflowEvent() *workflow.HistoryEvent
		AddTimerCanceledEvent(int64, *workflow.CancelTimerDecisionAttributes, string) *workflow.HistoryEvent
		AddTimerFiredEvent(int64, string) *workflow.HistoryEvent
		AddTimerStartedEvent(int64, *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo)
		AddWorkflowExecutionCancelRequestedEvent(string, *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent
		AddWorkflowExecutionCanceledEvent(int64, *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddWorkflowExecutionSignaled(*workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent
		AddWorkflowExecutionStartedEvent(workflow.WorkflowExecution, *h.StartWorkflowExecutionRequest) *workflow.HistoryEvent
		AddWorkflowExecutionStartedEventForContinueAsNew(string, *h.ParentExecutionInfo, workflow.WorkflowExecution, mutableState, *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddWorkflowExecutionTerminatedEvent(*workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent
		AfterAddDecisionTaskCompletedEvent(int64)
		BeforeAddDecisionTaskCompletedEvent()
		BufferReplicationTask(*h.ReplicateEventsRequest) error
		ClearStickyness()
		CloseUpdateSession() (*mutableStateSessionUpdates, error)
		CopyToPersistence() *persistence.WorkflowMutableState
		CreateNewHistoryEvent(eventType workflow.EventType) *workflow.HistoryEvent
		CreateNewHistoryEventWithTimestamp(eventType workflow.EventType, timestamp int64) *workflow.HistoryEvent
		CreateRetryTimer(*persistence.ActivityInfo, string) persistence.Task
		CreateReplicationTask() *persistence.HistoryReplicationTask
		CreateTransientDecisionEvents(di *decisionInfo, identity string) (*workflow.HistoryEvent, *workflow.HistoryEvent)
		DeleteActivity(int64) error
		DeleteBufferedReplicationTask(int64)
		DeleteDecision()
		DeletePendingChildExecution(int64)
		DeletePendingRequestCancel(int64)
		DeleteSignalRequested(requestID string)
		DeletePendingSignal(int64)
		DeleteUserTimer(string)
		FailDecision()
		FlushBufferedEvents() error
		GetActivityByActivityID(string) (*persistence.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistence.ActivityInfo, bool)
		GetActivityScheduledEvent(int64) (*workflow.HistoryEvent, bool)
		GetActivityStartedEvent(int64) (*workflow.HistoryEvent, bool)
		GetBufferedHistory(*persistence.SerializedHistoryEventBatch) *workflow.History
		GetBufferedReplicationTask(int64) (*persistence.BufferedReplicationTask, bool)
		GetChildExecutionInfo(int64) (*persistence.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*workflow.HistoryEvent, bool)
		GetChildExecutionStartedEvent(int64) (*workflow.HistoryEvent, bool)
		GetCompletionEvent() (*workflow.HistoryEvent, bool)
		GetContinueAsNew() *persistence.CreateWorkflowExecutionRequest
		GetCurrentVersion() int64
		GetExecutionInfo() *persistence.WorkflowExecutionInfo
		GetHistoryBuilder() *historyBuilder
		GetHistoryEvent(serializedEvent []byte) (*workflow.HistoryEvent, bool)
		GetInFlightDecisionTask() (*decisionInfo, bool)
		GetLastFirstEventID() int64
		GetLastUpdatedTimestamp() int64
		GetLastWriteVersion() int64
		GetNextEventID() int64
		GetPendingDecision(int64) (*decisionInfo, bool)
		GetPendingActivityInfos() map[int64]*persistence.ActivityInfo
		GetPendingTimerInfos() map[string]*persistence.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistence.ChildExecutionInfo
		GetReplicationState() *persistence.ReplicationState
		GetRequestCancelInfo(int64) (*persistence.RequestCancelInfo, bool)
		GetRetryBackoffDuration(errReason string) time.Duration
		GetScheduleIDByActivityID(string) (int64, bool)
		GetSignalInfo(int64) (*persistence.SignalInfo, bool)
		GetStartVersion() int64
		GetUserTimer(string) (bool, *persistence.TimerInfo)
		GetWorkflowType() *workflow.WorkflowType
		HasBufferedEvents() bool
		HasBufferedReplicationTasks() bool
		HasInFlightDecisionTask() bool
		HasParentExecution() bool
		HasPendingDecisionTask() bool
		IsCancelRequested() (bool, string)
		IsSignalRequested(requestID string) bool
		IsStickyTaskListEnabled() bool
		IsWorkflowExecutionRunning() bool
		Load(*persistence.WorkflowMutableState)
		ReplicateActivityTaskCancelRequestedEvent(*workflow.HistoryEvent)
		ReplicateActivityTaskCanceledEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(*workflow.HistoryEvent) *persistence.ActivityInfo
		ReplicateActivityTaskStartedEvent(*workflow.HistoryEvent)
		ReplicateActivityTaskTimedOutEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionCompletedEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionStartedEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionTimedOutEvent(*workflow.HistoryEvent)
		ReplicateDecisionTaskCompletedEvent(int64, int64)
		ReplicateDecisionTaskFailedEvent(int64, int64)
		ReplicateDecisionTaskScheduledEvent(int64, int64, string, int32, int64) *decisionInfo
		ReplicateDecisionTaskStartedEvent(*decisionInfo, int64, int64, int64, string, int64) *decisionInfo
		ReplicateDecisionTaskTimedOutEvent(int64, int64)
		ReplicateExternalWorkflowExecutionCancelRequested(*workflow.HistoryEvent)
		ReplicateExternalWorkflowExecutionSignaled(*workflow.HistoryEvent)
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) *persistence.RequestCancelInfo
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) *persistence.SignalInfo
		ReplicateStartChildWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateStartChildWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) *persistence.ChildExecutionInfo
		ReplicateTimerCanceledEvent(*workflow.HistoryEvent)
		ReplicateTimerFiredEvent(*workflow.HistoryEvent)
		ReplicateTimerStartedEvent(*workflow.HistoryEvent) *persistence.TimerInfo
		ReplicateWorkflowExecutionCancelRequestedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionCanceledEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionCompletedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionContinuedAsNewEvent(string, string, *workflow.HistoryEvent, *workflow.HistoryEvent, *decisionInfo, mutableState)
		ReplicateWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionStartedEvent(string, *string, workflow.WorkflowExecution, string, *workflow.WorkflowExecutionStartedEventAttributes)
		ReplicateWorkflowExecutionTerminatedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionTimedoutEvent(*workflow.HistoryEvent)
		ResetSnapshot(string) *persistence.ResetMutableStateRequest
		SetHistoryBuilder(hBuilder *historyBuilder)
		UpdateActivity(*persistence.ActivityInfo) error
		UpdateActivityProgress(ai *persistence.ActivityInfo, request *workflow.RecordActivityTaskHeartbeatRequest)
		UpdateDecision(*decisionInfo)
		UpdateReplicationStateVersion(int64, bool)
		UpdateReplicationStateLastEventID(string, int64, int64)
		UpdateUserTimer(string, *persistence.TimerInfo)
	}

	mutableStateBuilder struct {
		pendingActivityInfoIDs          map[int64]*persistence.ActivityInfo    // Schedule Event ID -> Activity Info.
		pendingActivityInfoByActivityID map[string]int64                       // Activity ID -> Schedule Event ID of the activity.
		updateActivityInfos             map[*persistence.ActivityInfo]struct{} // Modified activities from last update.
		deleteActivityInfos             map[int64]struct{}                     // Deleted activities from last update.

		pendingTimerInfoIDs map[string]*persistence.TimerInfo   // User Timer ID -> Timer Info.
		updateTimerInfos    map[*persistence.TimerInfo]struct{} // Modified timers from last update.
		deleteTimerInfos    map[string]struct{}                 // Deleted timers from last update.

		pendingChildExecutionInfoIDs map[int64]*persistence.ChildExecutionInfo    // Initiated Event ID -> Child Execution Info
		updateChildExecutionInfos    map[*persistence.ChildExecutionInfo]struct{} // Modified ChildExecution Infos since last update
		deleteChildExecutionInfo     *int64                                       // Deleted ChildExecution Info since last update

		pendingRequestCancelInfoIDs map[int64]*persistence.RequestCancelInfo    // Initiated Event ID -> RequestCancelInfo
		updateRequestCancelInfos    map[*persistence.RequestCancelInfo]struct{} // Modified RequestCancel Infos since last update, for persistence update
		deleteRequestCancelInfo     *int64                                      // Deleted RequestCancel Info since last update, for persistence update

		pendingSignalInfoIDs map[int64]*persistence.SignalInfo    // Initiated Event ID -> SignalInfo
		updateSignalInfos    map[*persistence.SignalInfo]struct{} // Modified SignalInfo since last update
		deleteSignalInfo     *int64                               // Deleted SignalInfo since last update

		pendingSignalRequestedIDs map[string]struct{} // Set of signaled requestIds
		updateSignalRequestedIDs  map[string]struct{} // Set of signaled requestIds since last update
		deleteSignalRequestedID   string              // Deleted signaled requestId

		bufferedEvents       []*persistence.SerializedHistoryEventBatch // buffered history events that are already persisted
		updateBufferedEvents *persistence.SerializedHistoryEventBatch   // buffered history events that needs to be persisted
		clearBufferedEvents  bool                                       // delete buffered events from persistence

		bufferedReplicationTasks       map[int64]*persistence.BufferedReplicationTask // Storage for out of order events
		updateBufferedReplicationTasks *persistence.BufferedReplicationTask
		deleteBufferedReplicationEvent *int64

		executionInfo    *persistence.WorkflowExecutionInfo // Workflow mutable state info.
		replicationState *persistence.ReplicationState
		continueAsNew    *persistence.CreateWorkflowExecutionRequest
		hBuilder         *historyBuilder
		eventSerializer  historyEventSerializer
		currentCluster   string
		config           *Config
		logger           bark.Logger
	}

	mutableStateSessionUpdates struct {
		newEventsBuilder                 *historyBuilder
		updateActivityInfos              []*persistence.ActivityInfo
		deleteActivityInfos              []int64
		updateTimerInfos                 []*persistence.TimerInfo
		deleteTimerInfos                 []string
		updateChildExecutionInfos        []*persistence.ChildExecutionInfo
		deleteChildExecutionInfo         *int64
		updateCancelExecutionInfos       []*persistence.RequestCancelInfo
		deleteCancelExecutionInfo        *int64
		updateSignalInfos                []*persistence.SignalInfo
		deleteSignalInfo                 *int64
		updateSignalRequestedIDs         []string
		deleteSignalRequestedID          string
		continueAsNew                    *persistence.CreateWorkflowExecutionRequest
		newBufferedEvents                *persistence.SerializedHistoryEventBatch
		clearBufferedEvents              bool
		newBufferedReplicationEventsInfo *persistence.BufferedReplicationTask
		deleteBufferedReplicationEvent   *int64
	}

	// TODO: This should be part of persistence layer
	decisionInfo struct {
		Version         int64
		ScheduleID      int64
		StartedID       int64
		RequestID       string
		DecisionTimeout int32
		TaskList        string // This is only needed to communicate tasklist used after AddDecisionTaskScheduledEvent
		Attempt         int64
		Timestamp       int64
	}
)

func newMutableStateBuilder(currentCluster string, config *Config, logger bark.Logger) *mutableStateBuilder {
	s := &mutableStateBuilder{
		updateActivityInfos:             make(map[*persistence.ActivityInfo]struct{}),
		pendingActivityInfoIDs:          make(map[int64]*persistence.ActivityInfo),
		pendingActivityInfoByActivityID: make(map[string]int64),
		deleteActivityInfos:             make(map[int64]struct{}),

		pendingTimerInfoIDs: make(map[string]*persistence.TimerInfo),
		updateTimerInfos:    make(map[*persistence.TimerInfo]struct{}),
		deleteTimerInfos:    make(map[string]struct{}),

		updateChildExecutionInfos:    make(map[*persistence.ChildExecutionInfo]struct{}),
		pendingChildExecutionInfoIDs: make(map[int64]*persistence.ChildExecutionInfo),
		deleteChildExecutionInfo:     nil,

		updateRequestCancelInfos:    make(map[*persistence.RequestCancelInfo]struct{}),
		pendingRequestCancelInfoIDs: make(map[int64]*persistence.RequestCancelInfo),
		deleteRequestCancelInfo:     nil,

		updateSignalInfos:    make(map[*persistence.SignalInfo]struct{}),
		pendingSignalInfoIDs: make(map[int64]*persistence.SignalInfo),
		deleteSignalInfo:     nil,

		updateSignalRequestedIDs:  make(map[string]struct{}),
		pendingSignalRequestedIDs: make(map[string]struct{}),
		deleteSignalRequestedID:   "",

		eventSerializer: newJSONHistoryEventSerializer(),
		currentCluster:  currentCluster,
		config:          config,
		logger:          logger,
	}
	s.executionInfo = &persistence.WorkflowExecutionInfo{
		NextEventID:        common.FirstEventID,
		State:              persistence.WorkflowStateCreated,
		CloseStatus:        persistence.WorkflowCloseStatusNone,
		LastProcessedEvent: common.EmptyEventID,
	}
	s.hBuilder = newHistoryBuilder(s, logger)

	return s
}

func newMutableStateBuilderWithReplicationState(currentCluster string, config *Config, logger bark.Logger, version int64) *mutableStateBuilder {
	s := newMutableStateBuilder(currentCluster, config, logger)
	s.replicationState = &persistence.ReplicationState{
		StartVersion:        version,
		CurrentVersion:      version,
		LastWriteVersion:    common.EmptyVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastReplicationInfo: make(map[string]*persistence.ReplicationInfo),
	}
	return s
}

func (e *mutableStateBuilder) CopyToPersistence() *persistence.WorkflowMutableState {
	state := &persistence.WorkflowMutableState{}

	state.ActivitInfos = e.pendingActivityInfoIDs
	state.TimerInfos = e.pendingTimerInfoIDs
	state.ChildExecutionInfos = e.pendingChildExecutionInfoIDs
	state.RequestCancelInfos = e.pendingRequestCancelInfoIDs
	state.SignalInfos = e.pendingSignalInfoIDs
	state.SignalRequestedIDs = e.pendingSignalRequestedIDs
	state.ExecutionInfo = e.executionInfo
	state.ReplicationState = e.replicationState
	state.BufferedEvents = e.bufferedEvents
	state.BufferedReplicationTasks = e.bufferedReplicationTasks
	return state
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
	e.bufferedReplicationTasks = state.BufferedReplicationTasks
	for _, ai := range state.ActivitInfos {
		e.pendingActivityInfoByActivityID[ai.ActivityID] = ai.ScheduleID
	}
}

func (e *mutableStateBuilder) GetHistoryBuilder() *historyBuilder {
	return e.hBuilder
}

func (e *mutableStateBuilder) SetHistoryBuilder(hBuilder *historyBuilder) {
	e.hBuilder = hBuilder
}

func (e *mutableStateBuilder) GetExecutionInfo() *persistence.WorkflowExecutionInfo {
	return e.executionInfo
}

func (e *mutableStateBuilder) GetReplicationState() *persistence.ReplicationState {
	return e.replicationState
}

func (e *mutableStateBuilder) ResetSnapshot(prevRunID string) *persistence.ResetMutableStateRequest {
	insertActivities := make([]*persistence.ActivityInfo, 0, len(e.pendingActivityInfoIDs))
	for _, info := range e.pendingActivityInfoIDs {
		insertActivities = append(insertActivities, info)
	}

	insertTimers := make([]*persistence.TimerInfo, 0, len(e.pendingTimerInfoIDs))
	for _, info := range e.pendingTimerInfoIDs {
		insertTimers = append(insertTimers, info)
	}

	insertChildExecutions := make([]*persistence.ChildExecutionInfo, 0, len(e.pendingChildExecutionInfoIDs))
	for _, info := range e.pendingChildExecutionInfoIDs {
		insertChildExecutions = append(insertChildExecutions, info)
	}

	insertRequestCancels := make([]*persistence.RequestCancelInfo, 0, len(e.pendingRequestCancelInfoIDs))
	for _, info := range e.pendingRequestCancelInfoIDs {
		insertRequestCancels = append(insertRequestCancels, info)
	}

	insertSignals := make([]*persistence.SignalInfo, 0, len(e.pendingSignalInfoIDs))
	for _, info := range e.pendingSignalInfoIDs {
		insertSignals = append(insertSignals, info)
	}

	insertSignalRequested := make([]string, 0, len(e.pendingSignalRequestedIDs))
	for id := range e.pendingSignalRequestedIDs {
		insertSignalRequested = append(insertSignalRequested, id)
	}

	return &persistence.ResetMutableStateRequest{
		PrevRunID:                 prevRunID,
		ExecutionInfo:             e.executionInfo,
		ReplicationState:          e.replicationState,
		InsertActivityInfos:       insertActivities,
		InsertTimerInfos:          insertTimers,
		InsertChildExecutionInfos: insertChildExecutions,
		InsertRequestCancelInfos:  insertRequestCancels,
		InsertSignalInfos:         insertSignals,
		InsertSignalRequestedIDs:  insertSignalRequested,
	}
}

func (e *mutableStateBuilder) FlushBufferedEvents() error {
	// put new events into 2 buckets:
	//  1) if the event was added while there was in-flight decision, then put it in buffered bucket
	//  2) otherwise, put it in committed bucket
	var newBufferedEvents []*workflow.HistoryEvent
	var newCommittedEvents []*workflow.HistoryEvent
	for _, event := range e.hBuilder.history {
		if event.GetEventId() == common.BufferedEventID {
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

func (e *mutableStateBuilder) GetStartVersion() int64 {
	if e.replicationState == nil {
		return common.EmptyVersion
	}
	return e.replicationState.StartVersion
}

func (e *mutableStateBuilder) GetCurrentVersion() int64 {
	if e.replicationState == nil {
		return common.EmptyVersion
	}
	return e.replicationState.CurrentVersion
}

func (e *mutableStateBuilder) GetLastWriteVersion() int64 {
	if e.replicationState == nil {
		return common.EmptyVersion
	}
	return e.replicationState.LastWriteVersion
}

func (e *mutableStateBuilder) UpdateReplicationStateVersion(version int64, forceUpdate bool) {
	if version > e.replicationState.CurrentVersion || forceUpdate {
		e.replicationState.CurrentVersion = version
	}
}

// Assumption: It is expected CurrentVersion on replication state is updated at the start of transaction when
// mutableState is loaded for this workflow execution.
func (e *mutableStateBuilder) UpdateReplicationStateLastEventID(clusterName string, lastWriteVersion, lastEventID int64) {
	e.replicationState.LastWriteVersion = lastWriteVersion
	// TODO: Rename this to NextEventID to stay consistent naming convention with rest of code base
	e.replicationState.LastWriteEventID = lastEventID
	if clusterName != e.currentCluster {
		info, ok := e.replicationState.LastReplicationInfo[clusterName]
		if !ok {
			// ReplicationInfo doesn't exist for this cluster, create one
			info = &persistence.ReplicationInfo{}
			e.replicationState.LastReplicationInfo[clusterName] = info
		}

		info.Version = lastWriteVersion
		info.LastEventID = lastEventID
	}
}

func (e *mutableStateBuilder) CloseUpdateSession() (*mutableStateSessionUpdates, error) {
	if err := e.FlushBufferedEvents(); err != nil {
		return nil, err
	}

	updates := &mutableStateSessionUpdates{
		newEventsBuilder:                 e.hBuilder,
		updateActivityInfos:              convertUpdateActivityInfos(e.updateActivityInfos),
		deleteActivityInfos:              convertDeleteActivityInfos(e.deleteActivityInfos),
		updateTimerInfos:                 convertUpdateTimerInfos(e.updateTimerInfos),
		deleteTimerInfos:                 convertDeleteTimerInfos(e.deleteTimerInfos),
		updateChildExecutionInfos:        convertUpdateChildExecutionInfos(e.updateChildExecutionInfos),
		deleteChildExecutionInfo:         e.deleteChildExecutionInfo,
		updateCancelExecutionInfos:       convertUpdateRequestCancelInfos(e.updateRequestCancelInfos),
		deleteCancelExecutionInfo:        e.deleteRequestCancelInfo,
		updateSignalInfos:                convertUpdateSignalInfos(e.updateSignalInfos),
		deleteSignalInfo:                 e.deleteSignalInfo,
		updateSignalRequestedIDs:         convertSignalRequestedIDs(e.updateSignalRequestedIDs),
		deleteSignalRequestedID:          e.deleteSignalRequestedID,
		continueAsNew:                    e.continueAsNew,
		newBufferedEvents:                e.updateBufferedEvents,
		clearBufferedEvents:              e.clearBufferedEvents,
		newBufferedReplicationEventsInfo: e.updateBufferedReplicationTasks,
		deleteBufferedReplicationEvent:   e.deleteBufferedReplicationEvent,
	}

	// Clear all updates to prepare for the next session
	e.hBuilder = newHistoryBuilder(e, e.logger)
	e.updateActivityInfos = make(map[*persistence.ActivityInfo]struct{})
	e.deleteActivityInfos = make(map[int64]struct{})
	e.updateTimerInfos = make(map[*persistence.TimerInfo]struct{})
	e.deleteTimerInfos = make(map[string]struct{})
	e.updateChildExecutionInfos = make(map[*persistence.ChildExecutionInfo]struct{})
	e.deleteChildExecutionInfo = nil
	e.updateRequestCancelInfos = make(map[*persistence.RequestCancelInfo]struct{})
	e.deleteRequestCancelInfo = nil
	e.updateSignalInfos = make(map[*persistence.SignalInfo]struct{})
	e.deleteSignalInfo = nil
	e.updateSignalRequestedIDs = make(map[string]struct{})
	e.deleteSignalRequestedID = ""
	e.continueAsNew = nil
	e.clearBufferedEvents = false
	if e.updateBufferedEvents != nil {
		e.bufferedEvents = append(e.bufferedEvents, e.updateBufferedEvents)
		e.updateBufferedEvents = nil
	}
	if len(e.bufferedEvents) > e.config.MaximumBufferedEventsBatch() {
		return nil, ErrBufferedEventsLimitExceeded
	}
	e.updateBufferedReplicationTasks = nil
	e.deleteBufferedReplicationEvent = nil

	return updates, nil
}

func (e *mutableStateBuilder) BufferReplicationTask(
	request *h.ReplicateEventsRequest) error {
	var err error
	var serializedHistoryBatch, serializedNewRunHistoryBatch *persistence.SerializedHistoryEventBatch
	if request.History != nil {
		historyBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), request.History.Events)
		serializedHistoryBatch, err = e.hBuilder.serializer.Serialize(historyBatch)
		if err != nil {
			return err
		}
	}

	if request.NewRunHistory != nil {
		newRunHistoryBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(),
			request.NewRunHistory.Events)
		serializedNewRunHistoryBatch, err = e.hBuilder.serializer.Serialize(newRunHistoryBatch)
		if err != nil {
			return err
		}
	}

	bt := &persistence.BufferedReplicationTask{
		FirstEventID:  request.GetFirstEventId(),
		NextEventID:   request.GetNextEventId(),
		Version:       request.GetVersion(),
		History:       serializedHistoryBatch,
		NewRunHistory: serializedNewRunHistoryBatch,
	}

	e.bufferedReplicationTasks[request.GetFirstEventId()] = bt
	e.updateBufferedReplicationTasks = bt

	return nil
}

func (e *mutableStateBuilder) GetBufferedReplicationTask(firstEventID int64) (*persistence.BufferedReplicationTask,
	bool) {
	bt, ok := e.bufferedReplicationTasks[firstEventID]
	return bt, ok
}

func (e *mutableStateBuilder) DeleteBufferedReplicationTask(firstEventID int64) {
	delete(e.bufferedReplicationTasks, firstEventID)
	e.deleteBufferedReplicationEvent = common.Int64Ptr(firstEventID)
}

func (e *mutableStateBuilder) GetBufferedHistory(
	serializedHistory *persistence.SerializedHistoryEventBatch) *workflow.History {

	if serializedHistory != nil {
		var history []*workflow.HistoryEvent
		batch, err := e.hBuilder.serializer.Deserialize(serializedHistory)
		if err != nil {
			// TODO: return proper error here
			return nil
		}

		for _, event := range batch.Events {
			history = append(history, event)
		}

		return &workflow.History{
			Events: history,
		}
	}

	return nil
}

func (e *mutableStateBuilder) CreateReplicationTask() *persistence.HistoryReplicationTask {
	return &persistence.HistoryReplicationTask{
		FirstEventID:        e.GetLastFirstEventID(),
		NextEventID:         e.GetNextEventID(),
		Version:             e.replicationState.CurrentVersion,
		LastReplicationInfo: e.replicationState.LastReplicationInfo,
	}
}

func convertUpdateActivityInfos(inputs map[*persistence.ActivityInfo]struct{}) []*persistence.ActivityInfo {
	outputs := []*persistence.ActivityInfo{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertDeleteActivityInfos(inputs map[int64]struct{}) []int64 {
	outputs := []int64{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateTimerInfos(inputs map[*persistence.TimerInfo]struct{}) []*persistence.TimerInfo {
	outputs := []*persistence.TimerInfo{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertDeleteTimerInfos(inputs map[string]struct{}) []string {
	outputs := []string{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateChildExecutionInfos(inputs map[*persistence.ChildExecutionInfo]struct{}) []*persistence.ChildExecutionInfo {
	outputs := []*persistence.ChildExecutionInfo{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateRequestCancelInfos(inputs map[*persistence.RequestCancelInfo]struct{}) []*persistence.RequestCancelInfo {
	outputs := []*persistence.RequestCancelInfo{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateSignalInfos(inputs map[*persistence.SignalInfo]struct{}) []*persistence.SignalInfo {
	outputs := []*persistence.SignalInfo{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertSignalRequestedIDs(inputs map[string]struct{}) []string {
	outputs := []string{}
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func (e *mutableStateBuilder) assignEventIDToBufferedEvents() {
	newCommittedEvents := e.hBuilder.history

	scheduledIDToStartedID := make(map[int64]int64)
	for _, event := range newCommittedEvents {
		if event.GetEventId() != common.BufferedEventID {
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
				e.updateActivityInfos[ai] = struct{}{}
			}
		case workflow.EventTypeChildWorkflowExecutionStarted:
			attributes := event.ChildWorkflowExecutionStartedEventAttributes
			initiatedID := attributes.GetInitiatedEventId()
			scheduledIDToStartedID[initiatedID] = eventID
			if ci, ok := e.GetChildExecutionInfo(initiatedID); ok {
				ci.StartedID = eventID
				e.updateChildExecutionInfos[ci] = struct{}{}
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

func (e *mutableStateBuilder) IsStickyTaskListEnabled() bool {
	return len(e.executionInfo.StickyTaskList) > 0
}

func (e *mutableStateBuilder) CreateNewHistoryEvent(eventType workflow.EventType) *workflow.HistoryEvent {
	return e.CreateNewHistoryEventWithTimestamp(eventType, time.Now().UnixNano())
}

func (e *mutableStateBuilder) CreateNewHistoryEventWithTimestamp(eventType workflow.EventType,
	timestamp int64) *workflow.HistoryEvent {
	eventID := e.executionInfo.NextEventID
	if e.shouldBufferEvent(eventType) {
		eventID = common.BufferedEventID
	} else {
		// only increase NextEventID if event is not buffered
		e.executionInfo.NextEventID++
	}

	ts := common.Int64Ptr(timestamp)
	historyEvent := &workflow.HistoryEvent{}
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = ts
	historyEvent.EventType = common.EventTypePtr(eventType)
	if e.replicationState != nil {
		historyEvent.Version = common.Int64Ptr(e.replicationState.CurrentVersion)
	}

	return historyEvent
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

func (e *mutableStateBuilder) GetWorkflowType() *workflow.WorkflowType {
	wType := &workflow.WorkflowType{}
	wType.Name = common.StringPtr(e.executionInfo.WorkflowTypeName)

	return wType
}

func (e *mutableStateBuilder) GetLastUpdatedTimestamp() int64 {
	lastUpdated := e.executionInfo.LastUpdatedTimestamp.UnixNano()
	if e.executionInfo.StartTimestamp.UnixNano() >= lastUpdated {
		// This could happen due to clock skews
		// ensure that the lastUpdatedTimestamp is always greater than the StartTimestamp
		lastUpdated = e.executionInfo.StartTimestamp.UnixNano() + 1
	}

	return lastUpdated
}

func (e *mutableStateBuilder) GetActivityScheduledEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	return e.GetHistoryEvent(ai.ScheduledEvent)
}

func (e *mutableStateBuilder) GetActivityStartedEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	return e.GetHistoryEvent(ai.StartedEvent)
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

	return e.GetHistoryEvent(ci.InitiatedEvent)
}

// GetChildExecutionStartedEvent reads out the ChildExecutionStartedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionStartedEvent(initiatedEventID int64) (*workflow.HistoryEvent, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, false
	}

	return e.GetHistoryEvent(ci.StartedEvent)
}

// GetRequestCancelInfo gives details about a request cancellation that is currently in progress.
func (e *mutableStateBuilder) GetRequestCancelInfo(initiatedEventID int64) (*persistence.RequestCancelInfo, bool) {
	ri, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	return ri, ok
}

func (e *mutableStateBuilder) GetRetryBackoffDuration(errReason string) time.Duration {
	info := e.executionInfo
	if !info.HasRetryPolicy {
		return common.NoRetryBackoff
	}

	return getBackoffInterval(info.Attempt, info.MaximumAttempts, info.InitialInterval, info.MaximumInterval, info.BackoffCoefficient, time.Now(), info.ExpirationTime, errReason, info.NonRetriableErrors)
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

	return e.GetHistoryEvent(serializedEvent)
}

// DeletePendingChildExecution deletes details about a ChildExecutionInfo.
func (e *mutableStateBuilder) DeletePendingChildExecution(initiatedEventID int64) {
	delete(e.pendingChildExecutionInfoIDs, initiatedEventID)
	e.deleteChildExecutionInfo = common.Int64Ptr(initiatedEventID)
}

// DeletePendingRequestCancel deletes details about a RequestCancelInfo.
func (e *mutableStateBuilder) DeletePendingRequestCancel(initiatedEventID int64) {
	delete(e.pendingRequestCancelInfoIDs, initiatedEventID)
	e.deleteRequestCancelInfo = common.Int64Ptr(initiatedEventID)
}

// DeletePendingSignal deletes details about a SignalInfo
func (e *mutableStateBuilder) DeletePendingSignal(initiatedEventID int64) {
	delete(e.pendingSignalInfoIDs, initiatedEventID)
	e.deleteSignalInfo = common.Int64Ptr(initiatedEventID)
}

func (e *mutableStateBuilder) writeCompletionEventToMutableState(completionEvent *workflow.HistoryEvent) error {
	// First check to see if this is a Child Workflow
	if e.HasParentExecution() {
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

func (e *mutableStateBuilder) HasParentExecution() bool {
	return e.executionInfo.ParentDomainID != "" && e.executionInfo.ParentWorkflowID != ""
}

func (e *mutableStateBuilder) UpdateActivityProgress(ai *persistence.ActivityInfo,
	request *workflow.RecordActivityTaskHeartbeatRequest) {
	ai.Details = request.Details
	ai.LastHeartBeatUpdatedTime = time.Now()
	e.updateActivityInfos[ai] = struct{}{}
}

// UpdateActivity updates an activity
func (e *mutableStateBuilder) UpdateActivity(ai *persistence.ActivityInfo) error {
	_, ok := e.pendingActivityInfoIDs[ai.ScheduleID]
	if !ok {
		return fmt.Errorf("Unable to find activity with schedule event id: %v in mutable state", ai.ScheduleID)
	}
	e.updateActivityInfos[ai] = struct{}{}
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

	e.deleteActivityInfos[scheduleEventID] = struct{}{}
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
	e.updateTimerInfos[ti] = struct{}{}
}

// DeleteUserTimer deletes an user timer.
func (e *mutableStateBuilder) DeleteUserTimer(timerID string) {
	delete(e.pendingTimerInfoIDs, timerID)
	e.deleteTimerInfos[timerID] = struct{}{}
}

func (e *mutableStateBuilder) getDecisionInfo() *decisionInfo {
	return &decisionInfo{
		Version:         e.executionInfo.DecisionVersion,
		ScheduleID:      e.executionInfo.DecisionScheduleID,
		StartedID:       e.executionInfo.DecisionStartedID,
		RequestID:       e.executionInfo.DecisionRequestID,
		DecisionTimeout: e.executionInfo.DecisionTimeout,
		Attempt:         e.executionInfo.DecisionAttempt,
		Timestamp:       e.executionInfo.DecisionTimestamp,
	}
}

// GetPendingDecision returns details about the in-progress decision task
func (e *mutableStateBuilder) GetPendingDecision(scheduleEventID int64) (*decisionInfo, bool) {
	di := e.getDecisionInfo()
	if scheduleEventID == di.ScheduleID {
		return di, true
	}
	return nil, false
}

func (e *mutableStateBuilder) GetPendingActivityInfos() map[int64]*persistence.ActivityInfo {
	return e.pendingActivityInfoIDs
}

func (e *mutableStateBuilder) GetPendingTimerInfos() map[string]*persistence.TimerInfo {
	return e.pendingTimerInfoIDs
}

func (e *mutableStateBuilder) GetPendingChildExecutionInfos() map[int64]*persistence.ChildExecutionInfo {
	return e.pendingChildExecutionInfoIDs
}

func (e *mutableStateBuilder) HasPendingDecisionTask() bool {
	return e.executionInfo.DecisionScheduleID != common.EmptyEventID
}

func (e *mutableStateBuilder) HasInFlightDecisionTask() bool {
	return e.executionInfo.DecisionStartedID > 0
}

func (e *mutableStateBuilder) GetInFlightDecisionTask() (*decisionInfo, bool) {
	if e.executionInfo.DecisionScheduleID == common.EmptyEventID ||
		e.executionInfo.DecisionStartedID == common.EmptyEventID {
		return nil, false
	}

	di := e.getDecisionInfo()
	return di, true
}

func (e *mutableStateBuilder) HasBufferedEvents() bool {
	if len(e.bufferedEvents) > 0 || e.updateBufferedEvents != nil {
		return true
	}

	for _, event := range e.hBuilder.history {
		if event.GetEventId() == common.BufferedEventID {
			return true
		}
	}

	return false
}

func (e *mutableStateBuilder) HasBufferedReplicationTasks() bool {
	if len(e.bufferedReplicationTasks) > 0 || e.updateBufferedReplicationTasks != nil {
		return true
	}

	return false
}

// UpdateDecision updates a decision task.
func (e *mutableStateBuilder) UpdateDecision(di *decisionInfo) {
	e.executionInfo.DecisionVersion = di.Version
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
		Version:         common.EmptyVersion,
		ScheduleID:      common.EmptyEventID,
		StartedID:       common.EmptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: 0,
		Attempt:         0,
		Timestamp:       0,
	}
	e.UpdateDecision(emptyDecisionInfo)
}

func (e *mutableStateBuilder) FailDecision() {
	// Clear stickiness whenever decision fails
	e.ClearStickyness()

	failDecisionInfo := &decisionInfo{
		Version:         common.EmptyVersion,
		ScheduleID:      common.EmptyEventID,
		StartedID:       common.EmptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: 0,
		Attempt:         e.executionInfo.DecisionAttempt + 1,
	}
	e.UpdateDecision(failDecisionInfo)
}

func (e *mutableStateBuilder) ClearStickyness() {
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

func (e *mutableStateBuilder) IsWorkflowExecutionRunning() bool {
	return e.executionInfo.State != persistence.WorkflowStateCompleted
}

func (e *mutableStateBuilder) IsCancelRequested() (bool, string) {
	if e.executionInfo.CancelRequested {
		return e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID
	}

	return false, ""
}

func (e *mutableStateBuilder) IsSignalRequested(requestID string) bool {
	if _, ok := e.pendingSignalRequestedIDs[requestID]; ok {
		return true
	}
	return false
}

func (e *mutableStateBuilder) AddSignalRequested(requestID string) {
	if e.pendingSignalRequestedIDs == nil {
		e.pendingSignalRequestedIDs = make(map[string]struct{})
	}
	if e.updateSignalRequestedIDs == nil {
		e.updateSignalRequestedIDs = make(map[string]struct{})
	}
	e.pendingSignalRequestedIDs[requestID] = struct{}{} // add requestID to set
	e.updateSignalRequestedIDs[requestID] = struct{}{}
}

func (e *mutableStateBuilder) DeleteSignalRequested(requestID string) {
	delete(e.pendingSignalRequestedIDs, requestID)
	e.deleteSignalRequestedID = requestID
}

func (e *mutableStateBuilder) GetHistoryEvent(serializedEvent []byte) (*workflow.HistoryEvent, bool) {
	event, err := e.eventSerializer.Deserialize(serializedEvent)
	if err != nil {
		return nil, false
	}

	return event, true
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEventForContinueAsNew(domainID string,
	parentExecutionInfo *h.ParentExecutionInfo, execution workflow.WorkflowExecution, previousExecutionState mutableState,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	previousExecutionInfo := previousExecutionState.GetExecutionInfo()
	taskList := previousExecutionInfo.TaskList
	if attributes.TaskList != nil {
		taskList = attributes.TaskList.GetName()
	}
	tl := &workflow.TaskList{}
	tl.Name = common.StringPtr(taskList)

	workflowType := previousExecutionInfo.WorkflowTypeName
	if attributes.WorkflowType != nil {
		workflowType = attributes.WorkflowType.GetName()
	}
	wType := &workflow.WorkflowType{}
	wType.Name = common.StringPtr(workflowType)

	decisionTimeout := previousExecutionInfo.DecisionTimeoutValue
	if attributes.TaskStartToCloseTimeoutSeconds != nil {
		decisionTimeout = attributes.GetTaskStartToCloseTimeoutSeconds()
	}

	createRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainID),
		WorkflowId:                          execution.WorkflowId,
		TaskList:                            tl,
		WorkflowType:                        wType,
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(decisionTimeout),
		ExecutionStartToCloseTimeoutSeconds: attributes.ExecutionStartToCloseTimeoutSeconds,
		Input:       attributes.Input,
		RetryPolicy: attributes.RetryPolicy,
	}

	req := &h.StartWorkflowExecutionRequest{
		DomainUUID:          common.StringPtr(domainID),
		StartRequest:        createRequest,
		ParentExecutionInfo: parentExecutionInfo,
	}
	if attributes.GetBackoffStartIntervalInSeconds() > 0 {
		req.Attempt = common.Int32Ptr(previousExecutionState.GetExecutionInfo().Attempt + 1)
		expirationTime := previousExecutionState.GetExecutionInfo().ExpirationTime
		if !expirationTime.IsZero() {
			req.ExpirationTimestamp = common.Int64Ptr(expirationTime.UnixNano())
		}
	}

	// History event only has domainName so domainID has to be passed in explicitly to update the mutable state
	var parentDomainID *string
	if parentExecutionInfo != nil {
		parentDomainID = parentExecutionInfo.DomainUUID
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(req, &previousExecutionInfo.RunID)
	e.ReplicateWorkflowExecutionStartedEvent(domainID, parentDomainID, execution, createRequest.GetRequestId(),
		event.WorkflowExecutionStartedEventAttributes)

	return event
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEvent(execution workflow.WorkflowExecution,
	startRequest *h.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	request := startRequest.StartRequest
	eventID := e.GetNextEventID()
	if eventID != common.FirstEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowStarted, eventID, "")
		return nil
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(startRequest, nil)

	var parentDomainID *string
	if startRequest.ParentExecutionInfo != nil {
		parentDomainID = startRequest.ParentExecutionInfo.DomainUUID
	}
	e.ReplicateWorkflowExecutionStartedEvent(startRequest.GetDomainUUID(), parentDomainID,
		execution, request.GetRequestId(), event.WorkflowExecutionStartedEventAttributes)

	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionStartedEvent(domainID string, parentDomainID *string,
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
	e.executionInfo.LastProcessedEvent = common.EmptyEventID
	e.executionInfo.CreateRequestID = requestID
	e.executionInfo.DecisionVersion = common.EmptyVersion
	e.executionInfo.DecisionScheduleID = common.EmptyEventID
	e.executionInfo.DecisionStartedID = common.EmptyEventID
	e.executionInfo.DecisionRequestID = emptyUUID
	e.executionInfo.DecisionTimeout = 0

	if parentDomainID != nil {
		e.executionInfo.ParentDomainID = *parentDomainID
	}
	if event.ParentWorkflowExecution != nil {
		e.executionInfo.ParentWorkflowID = event.ParentWorkflowExecution.GetWorkflowId()
		e.executionInfo.ParentRunID = event.ParentWorkflowExecution.GetRunId()
	}
	if event.ParentInitiatedEventId != nil {
		e.executionInfo.InitiatedID = event.GetParentInitiatedEventId()
	}
}

func (e *mutableStateBuilder) AddDecisionTaskScheduledEvent() *decisionInfo {
	if e.HasPendingDecisionTask() {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskScheduled, e.GetNextEventID(),
			fmt.Sprintf("{Pending Decision ScheduleID: %v}", e.executionInfo.DecisionScheduleID))
		return nil
	}

	// Tasklist and decision timeout should already be set from workflow execution started event
	taskList := e.executionInfo.TaskList
	if e.IsStickyTaskListEnabled() {
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

	return e.ReplicateDecisionTaskScheduledEvent(
		e.GetCurrentVersion(),
		scheduleID,
		taskList,
		startToCloseTimeoutSeconds,
		e.executionInfo.DecisionAttempt,
	)
}

func (e *mutableStateBuilder) ReplicateDecisionTaskScheduledEvent(version, scheduleID int64, taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *decisionInfo {
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      scheduleID,
		StartedID:       common.EmptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: startToCloseTimeoutSeconds,
		TaskList:        taskList,
		Attempt:         attempt,
	}

	e.UpdateDecision(di)
	return di
}

func (e *mutableStateBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	request *workflow.PollForDecisionTaskRequest) (*workflow.HistoryEvent, *decisionInfo) {
	hasPendingDecision := e.HasPendingDecisionTask()
	di, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || di.StartedID != common.EmptyEventID {
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

	di = e.ReplicateDecisionTaskStartedEvent(di, e.GetCurrentVersion(), scheduleID, startedID, requestID, timestamp)
	return event, di
}

func (e *mutableStateBuilder) ReplicateDecisionTaskStartedEvent(di *decisionInfo, version, scheduleID, startedID int64,
	requestID string, timestamp int64) *decisionInfo {
	// Replicator calls it with a nil decision info, and it is safe to always lookup the decision in this case as it
	// does not have to deal with transient decision case.
	if di == nil {
		di, _ = e.GetPendingDecision(scheduleID)
	}

	e.executionInfo.State = persistence.WorkflowStateRunning
	// Update mutable decision state
	di = &decisionInfo{
		Version:         version,
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

func (e *mutableStateBuilder) CreateTransientDecisionEvents(di *decisionInfo, identity string) (*workflow.HistoryEvent,
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
		scheduledEvent := e.hBuilder.AddTransientDecisionTaskScheduledEvent(e.executionInfo.TaskList, di.DecisionTimeout,
			di.Attempt, di.Timestamp)
		startedEvent := e.hBuilder.AddTransientDecisionTaskStartedEvent(scheduledEvent.GetEventId(), di.RequestID,
			request.GetIdentity(), di.Timestamp)
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
	e.ClearStickyness()

	event := e.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, 0, workflow.TimeoutTypeScheduleToStart)

	e.ReplicateDecisionTaskTimedOutEvent(scheduleEventID, common.EmptyEventID)
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

	scheduleEventID := event.GetEventId()
	scheduleToCloseTimeout := attributes.GetScheduleToCloseTimeoutSeconds()

	ai := &persistence.ActivityInfo{
		Version:                  event.GetVersion(),
		ScheduleID:               scheduleEventID,
		ScheduledEvent:           scheduleEvent,
		ScheduledTime:            time.Unix(0, *event.Timestamp),
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               common.StringDefault(attributes.ActivityId),
		ScheduleToStartTimeout:   attributes.GetScheduleToStartTimeoutSeconds(),
		ScheduleToCloseTimeout:   scheduleToCloseTimeout,
		StartToCloseTimeout:      attributes.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeout:         attributes.GetHeartbeatTimeoutSeconds(),
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 attributes.TaskList.GetName(),
		HasRetryPolicy:           attributes.RetryPolicy != nil,
	}
	ai.ExpirationTime = ai.ScheduledTime.Add(time.Duration(scheduleToCloseTimeout) * time.Second)
	if ai.HasRetryPolicy {
		ai.InitialInterval = attributes.RetryPolicy.GetInitialIntervalInSeconds()
		ai.BackoffCoefficient = attributes.RetryPolicy.GetBackoffCoefficient()
		ai.MaximumInterval = attributes.RetryPolicy.GetMaximumIntervalInSeconds()
		ai.MaximumAttempts = attributes.RetryPolicy.GetMaximumAttempts()
		ai.NonRetriableErrors = attributes.RetryPolicy.NonRetriableErrorReasons
		if attributes.RetryPolicy.GetExpirationIntervalInSeconds() > scheduleToCloseTimeout {
			ai.ExpirationTime = ai.ScheduledTime.Add(time.Duration(attributes.RetryPolicy.GetExpirationIntervalInSeconds()) * time.Second)
		}
	}

	e.pendingActivityInfoIDs[scheduleEventID] = ai
	e.pendingActivityInfoByActivityID[ai.ActivityID] = scheduleEventID
	e.updateActivityInfos[ai] = struct{}{}

	return ai
}

func (e *mutableStateBuilder) addTransientActivityStartedEvent(scheduleEventID int64) {
	if ai, ok := e.GetActivityInfo(scheduleEventID); ok && ai.StartedID == common.TransientEventID {
		// activity task was started (as transient event), we need to add it now.
		event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, ai.Attempt, ai.RequestID, ai.StartedIdentity)
		if !ai.StartedTime.IsZero() {
			// overwrite started event time to the one recorded in ActivityInfo
			event.Timestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
		}
		e.ReplicateActivityTaskStartedEvent(event)
	}
}

func (e *mutableStateBuilder) AddActivityTaskStartedEvent(ai *persistence.ActivityInfo, scheduleEventID int64,
	requestID string, identity string) *workflow.HistoryEvent {

	if !ai.HasRetryPolicy {
		event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, ai.Attempt, requestID, identity)
		e.ReplicateActivityTaskStartedEvent(event)
		return event
	}

	// we might need to retry, so do not append started event just yet,
	// instead update mutable state and will record started event when activity task is closed
	ai.StartedID = common.TransientEventID
	ai.RequestID = requestID
	ai.StartedTime = time.Now()
	ai.StartedIdentity = identity
	e.UpdateActivity(ai)
	return nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskStartedEvent(event *workflow.HistoryEvent) {
	attributes := event.ActivityTaskStartedEventAttributes
	scheduleID := attributes.GetScheduledEventId()
	ai, _ := e.GetActivityInfo(scheduleID)

	ai.StartedID = event.GetEventId()
	ai.RequestID = attributes.GetRequestId()
	ai.StartedTime = time.Unix(0, event.GetTimestamp())
	e.updateActivityInfos[ai] = struct{}{}
}

func (e *mutableStateBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	e.addTransientActivityStartedEvent(scheduleEventID)
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

	e.addTransientActivityStartedEvent(scheduleEventID)
	event := e.hBuilder.AddActivityTaskFailedEvent(scheduleEventID, startedEventID, request)
	if err := e.ReplicateActivityTaskFailedEvent(event); err != nil {
		return nil
	}

	return event
}

func (e *mutableStateBuilder) ReplicateActivityTaskFailedEvent(event *workflow.HistoryEvent) error {
	attributes := event.ActivityTaskFailedEventAttributes
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID int64,
	timeoutType workflow.TimeoutType, lastHeartBeatDetails []byte) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID ||
		((timeoutType == workflow.TimeoutTypeStartToClose || timeoutType == workflow.TimeoutTypeHeartbeat) &&
			ai.StartedID == common.EmptyEventID) {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskTimedOut, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, TimeOutType: %v, Exist: %v}", scheduleEventID, startedEventID,
			timeoutType, ok))
		return nil
	}

	e.addTransientActivityStartedEvent(scheduleEventID)
	event := e.hBuilder.AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails)
	if err := e.ReplicateActivityTaskTimedOutEvent(event); err != nil {
		return nil
	}

	return event
}

func (e *mutableStateBuilder) ReplicateActivityTaskTimedOutEvent(event *workflow.HistoryEvent) error {
	attributes := event.ActivityTaskTimedOutEventAttributes
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
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

	e.ReplicateActivityTaskCancelRequestedEvent(actCancelReqEvent)

	return actCancelReqEvent, ai, isRunning
}

func (e *mutableStateBuilder) ReplicateActivityTaskCancelRequestedEvent(event *workflow.HistoryEvent) {
	attributes := event.ActivityTaskCancelRequestedEventAttributes
	activityID := attributes.GetActivityId()
	ai, _ := e.GetActivityByActivityID(activityID)

	// - We have the activity dispatched to worker.
	// - The activity might not be heartbeat'ing, but the activity can still call RecordActivityHeartBeat()
	//   to see cancellation while reporting progress of the activity.
	ai.CancelRequested = true

	ai.CancelRequestID = event.GetEventId()
	e.updateActivityInfos[ai] = struct{}{}
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

	e.addTransientActivityStartedEvent(scheduleEventID)
	event := e.hBuilder.AddActivityTaskCanceledEvent(scheduleEventID, startedEventID, latestCancelRequestedEventID,
		details, identity)
	if err := e.ReplicateActivityTaskCanceledEvent(event); err != nil {
		return nil
	}

	return event
}

func (e *mutableStateBuilder) ReplicateActivityTaskCanceledEvent(event *workflow.HistoryEvent) error {
	attributes := event.ActivityTaskCanceledEventAttributes
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
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

	event := e.hBuilder.AddFailWorkflowEvent(decisionCompletedEventID, attributes)
	e.ReplicateWorkflowExecutionFailedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionFailedEvent(event *workflow.HistoryEvent) {
	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusFailed
	e.writeCompletionEventToMutableState(event)
}

func (e *mutableStateBuilder) AddTimeoutWorkflowEvent() *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimeoutWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	event := e.hBuilder.AddTimeoutWorkflowEvent()
	e.ReplicateWorkflowExecutionTimedoutEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionTimedoutEvent(event *workflow.HistoryEvent) {
	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusTimedOut
	e.writeCompletionEventToMutableState(event)
}

func (e *mutableStateBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted || e.executionInfo.CancelRequested {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionRequestCancelWorkflow, e.GetNextEventID(),
			fmt.Sprintf("{State: %v, CancelRequested: %v, RequestID: %v}", e.executionInfo.State,
				e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID))

		return nil
	}

	event := e.hBuilder.AddWorkflowExecutionCancelRequestedEvent(cause, request)
	e.ReplicateWorkflowExecutionCancelRequestedEvent(event)

	// Set the CancelRequestID on the active cluster.  This information is not part of the history event.
	e.executionInfo.CancelRequestID = request.CancelRequest.GetRequestId()
	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCancelRequestedEvent(event *workflow.HistoryEvent) {
	e.executionInfo.CancelRequested = true
}

func (e *mutableStateBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
	}

	event := e.hBuilder.AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)
	e.ReplicateWorkflowExecutionCanceledEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCanceledEvent(event *workflow.HistoryEvent) {
	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusCanceled
	e.writeCompletionEventToMutableState(event)
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	decisionCompletedEventID int64, cancelRequestID string,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.RequestCancelInfo) {
	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	if event == nil {
		return nil, nil
	}

	rci := e.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(event, cancelRequestID)
	return event, rci
}

func (e *mutableStateBuilder) ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	event *workflow.HistoryEvent, cancelRequestID string) *persistence.RequestCancelInfo {
	// TODO: Evaluate if we need cancelRequestID also part of history event
	initiatedEventID := event.GetEventId()
	rci := &persistence.RequestCancelInfo{
		Version:         event.GetVersion(),
		InitiatedID:     initiatedEventID,
		CancelRequestID: cancelRequestID,
	}

	e.pendingRequestCancelInfoIDs[initiatedEventID] = rci
	e.updateRequestCancelInfos[rci] = struct{}{}

	return rci
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCancelRequested, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))

		return nil
	}

	event := e.hBuilder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	e.ReplicateExternalWorkflowExecutionCancelRequested(event)

	return event
}

func (e *mutableStateBuilder) ReplicateExternalWorkflowExecutionCancelRequested(event *workflow.HistoryEvent) {
	initiatedID := event.ExternalWorkflowExecutionCancelRequestedEventAttributes.GetInitiatedEventId()
	e.DeletePendingRequestCancel(initiatedID)
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

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID,
		domain, workflowID, runID, cause)
	e.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event *workflow.HistoryEvent) {
	initiatedID := event.RequestCancelExternalWorkflowExecutionFailedEventAttributes.GetInitiatedEventId()
	e.DeletePendingRequestCancel(initiatedID)
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	signalRequestID string, request *workflow.SignalExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.SignalInfo) {

	event := e.hBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	if event == nil {
		return nil, nil
	}

	si := e.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(event, signalRequestID)

	return event, si
}

func (e *mutableStateBuilder) ReplicateSignalExternalWorkflowExecutionInitiatedEvent(event *workflow.HistoryEvent,
	signalRequestID string) *persistence.SignalInfo {
	// TODO: Consider also writing signalRequestID to history event
	initiatedEventID := event.GetEventId()
	attributes := event.SignalExternalWorkflowExecutionInitiatedEventAttributes
	si := &persistence.SignalInfo{
		Version:         event.GetVersion(),
		InitiatedID:     initiatedEventID,
		SignalRequestID: signalRequestID,
		SignalName:      attributes.GetSignalName(),
		Input:           attributes.Input,
		Control:         attributes.Control,
	}

	e.pendingSignalInfoIDs[initiatedEventID] = si
	e.updateSignalInfos[si] = struct{}{}
	return si
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionSignaled(initiatedID int64,
	domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	_, ok := e.GetSignalInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignalRequested, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	event := e.hBuilder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	e.ReplicateExternalWorkflowExecutionSignaled(event)

	return event
}

func (e *mutableStateBuilder) ReplicateExternalWorkflowExecutionSignaled(event *workflow.HistoryEvent) {
	initiatedID := event.ExternalWorkflowExecutionSignaledEventAttributes.GetInitiatedEventId()
	e.DeletePendingSignal(initiatedID)
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

	event := e.hBuilder.AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID, domain,
		workflowID, runID, control, cause)
	e.ReplicateSignalExternalWorkflowExecutionFailedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateSignalExternalWorkflowExecutionFailedEvent(event *workflow.HistoryEvent) {
	initiatedID := event.SignalExternalWorkflowExecutionFailedEventAttributes.GetInitiatedEventId()
	e.DeletePendingSignal(initiatedID)
}

func (e *mutableStateBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	timerID := request.GetTimerId()
	isTimerRunning, ti := e.GetUserTimer(timerID)
	if isTimerRunning {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerStarted, e.GetNextEventID(), fmt.Sprintf(
			"{IsTimerRunning: %v, TimerID: %v, StartedID: %v}", isTimerRunning, timerID, ti.StartedID))
		return nil, nil
	}

	event := e.hBuilder.AddTimerStartedEvent(decisionCompletedEventID, request)
	ti = e.ReplicateTimerStartedEvent(event)

	return event, ti
}

func (e *mutableStateBuilder) ReplicateTimerStartedEvent(event *workflow.HistoryEvent) *persistence.TimerInfo {
	attributes := event.TimerStartedEventAttributes
	timerID := attributes.GetTimerId()

	startToFireTimeout := attributes.GetStartToFireTimeoutSeconds()
	fireTimeout := time.Duration(startToFireTimeout) * time.Second
	// TODO: Time skew need to be taken in to account.
	expiryTime := time.Unix(0, event.GetTimestamp()).Add(fireTimeout) // should use the event time, not now
	ti := &persistence.TimerInfo{
		Version:    event.GetVersion(),
		TimerID:    timerID,
		ExpiryTime: expiryTime,
		StartedID:  event.GetEventId(),
		TaskID:     TimerTaskStatusNone,
	}

	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos[ti] = struct{}{}

	return ti
}

func (e *mutableStateBuilder) AddTimerFiredEvent(startedEventID int64, timerID string) *workflow.HistoryEvent {
	isTimerRunning, _ := e.GetUserTimer(timerID)
	if !isTimerRunning {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerFired, e.GetNextEventID(), fmt.Sprintf(
			"{startedEventID: %v, Exist: %v, TimerID: %v}", startedEventID, isTimerRunning, timerID))
		return nil
	}

	// Timer is running.
	event := e.hBuilder.AddTimerFiredEvent(startedEventID, timerID)
	e.ReplicateTimerFiredEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateTimerFiredEvent(event *workflow.HistoryEvent) {
	attributes := event.TimerFiredEventAttributes
	timerID := attributes.GetTimerId()

	e.DeleteUserTimer(timerID)
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
	event := e.hBuilder.AddTimerCanceledEvent(ti.StartedID, decisionCompletedEventID, timerID, identity)
	e.ReplicateTimerCanceledEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateTimerCanceledEvent(event *workflow.HistoryEvent) {
	attributes := event.TimerCanceledEventAttributes
	timerID := attributes.GetTimerId()

	e.DeleteUserTimer(timerID)
}

func (e *mutableStateBuilder) AddCancelTimerFailedEvent(decisionCompletedEventID int64,
	attributes *workflow.CancelTimerDecisionAttributes, identity string) *workflow.HistoryEvent {
	// No Operation: We couldn't cancel it probably TIMER_ID_UNKNOWN
	timerID := attributes.GetTimerId()
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

	event := e.hBuilder.AddWorkflowExecutionTerminatedEvent(request)
	e.ReplicateWorkflowExecutionTerminatedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionTerminatedEvent(event *workflow.HistoryEvent) {
	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusTerminated
	e.writeCompletionEventToMutableState(event)
}

func (e *mutableStateBuilder) AddWorkflowExecutionSignaled(
	request *workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignaled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	// No MutableState operation needed for signal
	return e.hBuilder.AddWorkflowExecutionSignaledEvent(request)
}

func (e *mutableStateBuilder) AddContinueAsNewEvent(decisionCompletedEventID int64, domainEntry *cache.DomainCacheEntry,
	parentDomainName string, attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, mutableState,
	error) {

	newRunID := uuid.New()
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(e.executionInfo.WorkflowID),
		RunId:      common.StringPtr(newRunID),
	}

	// Extract ParentExecutionInfo from current run so it can be passed down to the next
	var parentInfo *h.ParentExecutionInfo
	if e.HasParentExecution() {
		parentInfo = &h.ParentExecutionInfo{
			DomainUUID: common.StringPtr(e.executionInfo.ParentDomainID),
			Domain:     common.StringPtr(domainEntry.GetInfo().Name),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(e.executionInfo.ParentWorkflowID),
				RunId:      common.StringPtr(e.executionInfo.ParentRunID),
			},
			InitiatedId: common.Int64Ptr(e.executionInfo.InitiatedID),
		}
	}

	continueAsNewEvent := e.hBuilder.AddContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	var newStateBuilder *mutableStateBuilder
	if domainEntry.IsGlobalDomain() {
		// all workflows within a global domain should have replication state, no matter whether it will be replicated to multiple
		// target clusters or not
		newStateBuilder = newMutableStateBuilderWithReplicationState(e.currentCluster, e.config, e.logger, domainEntry.GetFailoverVersion())
	} else {
		newStateBuilder = newMutableStateBuilder(e.currentCluster, e.config, e.logger)
	}
	domainID := domainEntry.GetInfo().ID
	startedEvent := newStateBuilder.AddWorkflowExecutionStartedEventForContinueAsNew(domainID, parentInfo, newExecution, e, attributes)
	if startedEvent == nil {
		return nil, nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}

	var di *decisionInfo
	// First decision for retry will be created by a backoff timer
	if attributes.GetBackoffStartIntervalInSeconds() == 0 {
		di = newStateBuilder.AddDecisionTaskScheduledEvent()
		if di == nil {
			return nil, nil, &workflow.InternalServiceError{Message: "Failed to add decision started event."}
		}
	}

	e.ReplicateWorkflowExecutionContinuedAsNewEvent("", domainID, continueAsNewEvent, startedEvent, di, newStateBuilder)
	return continueAsNewEvent, newStateBuilder, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionContinuedAsNewEvent(sourceClusterName string, domainID string,
	continueAsNewEvent *workflow.HistoryEvent, startedEvent *workflow.HistoryEvent, di *decisionInfo,
	newStateBuilder mutableState) {
	continueAsNewAttributes := continueAsNewEvent.WorkflowExecutionContinuedAsNewEventAttributes
	startedAttributes := startedEvent.WorkflowExecutionStartedEventAttributes
	newRunID := continueAsNewAttributes.GetNewExecutionRunId()
	prevRunID := startedAttributes.GetContinuedExecutionRunId()
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(e.executionInfo.WorkflowID),
		RunId:      common.StringPtr(newRunID),
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusContinuedAsNew

	parentDomainID := ""
	var parentExecution *workflow.WorkflowExecution
	initiatedID := common.EmptyEventID

	newExecutionInfo := newStateBuilder.GetExecutionInfo()
	if newStateBuilder.HasParentExecution() {
		parentDomainID = newExecutionInfo.ParentDomainID
		parentExecution = &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(newExecutionInfo.ParentWorkflowID),
			RunId:      common.StringPtr(newExecutionInfo.ParentRunID),
		}
		initiatedID = newExecutionInfo.InitiatedID
	}

	continueAsNew := &persistence.CreateWorkflowExecutionRequest{
		// NOTE: there is no replication task for the start / decision scheduled event,
		// the above 2 events will be replicated along with previous continue as new event.
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            newExecution,
		ParentDomainID:       parentDomainID,
		ParentExecution:      parentExecution,
		InitiatedID:          initiatedID,
		TaskList:             newExecutionInfo.TaskList,
		WorkflowTypeName:     newExecutionInfo.WorkflowTypeName,
		WorkflowTimeout:      newExecutionInfo.WorkflowTimeout,
		DecisionTimeoutValue: newExecutionInfo.DecisionTimeoutValue,
		ExecutionContext:     nil,
		NextEventID:          newStateBuilder.GetNextEventID(),
		LastProcessedEvent:   common.EmptyEventID,
		ContinueAsNew:        true,
		PreviousRunID:        prevRunID,
		ReplicationState:     newStateBuilder.GetReplicationState(),
		HasRetryPolicy:       startedAttributes.RetryPolicy != nil,
		Attempt:              e.executionInfo.Attempt,
		InitialInterval:      e.executionInfo.InitialInterval,
		BackoffCoefficient:   e.executionInfo.BackoffCoefficient,
		MaximumInterval:      e.executionInfo.MaximumInterval,
		ExpirationTime:       e.executionInfo.ExpirationTime,
		MaximumAttempts:      e.executionInfo.MaximumAttempts,
		NonRetriableErrors:   e.executionInfo.NonRetriableErrors,
	}
	if continueAsNewAttributes.GetBackoffStartIntervalInSeconds() > 0 {
		// this is a retry
		continueAsNew.Attempt++
	}

	// timeout includes workflow_timeout + backoff_interval
	timeoutInSeconds := continueAsNewAttributes.GetExecutionStartToCloseTimeoutSeconds() + continueAsNewAttributes.GetBackoffStartIntervalInSeconds()
	timeoutDuration := time.Duration(timeoutInSeconds) * time.Second
	startedTime := time.Unix(0, startedEvent.GetTimestamp())
	timeoutDeadline := startedTime.Add(timeoutDuration)
	if !e.executionInfo.ExpirationTime.IsZero() && timeoutDeadline.After(e.executionInfo.ExpirationTime) {
		// expire before timeout
		timeoutDeadline = e.executionInfo.ExpirationTime
	}
	continueAsNew.TimerTasks = []persistence.Task{&persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: timeoutDeadline,
	}}

	if di != nil {
		if newStateBuilder.GetReplicationState() != nil {
			newStateBuilder.UpdateReplicationStateLastEventID(sourceClusterName, startedEvent.GetVersion(), di.ScheduleID)
		}

		continueAsNew.DecisionVersion = di.Version
		continueAsNew.DecisionScheduleID = di.ScheduleID
		continueAsNew.DecisionStartedID = di.StartedID
		continueAsNew.DecisionStartToCloseTimeout = di.DecisionTimeout

		if newStateBuilder.GetReplicationState() != nil {
			newStateBuilder.UpdateReplicationStateLastEventID(sourceClusterName, startedEvent.GetVersion(), di.ScheduleID)
		}

		newTransferTasks := []persistence.Task{&persistence.DecisionTask{
			DomainID:   domainID,
			TaskList:   newExecutionInfo.TaskList,
			ScheduleID: di.ScheduleID,
		}}
		continueAsNew.TransferTasks = newTransferTasks
	} else {
		// this is for retry
		continueAsNew.DecisionVersion = newStateBuilder.GetCurrentVersion()
		continueAsNew.DecisionScheduleID = common.EmptyEventID
		continueAsNew.DecisionStartedID = common.EmptyEventID
		if newStateBuilder.GetReplicationState() != nil {
			newStateBuilder.UpdateReplicationStateLastEventID(sourceClusterName, startedEvent.GetVersion(), startedEvent.GetEventId())
		}
		backoffTimer := &persistence.WorkflowRetryTimerTask{
			VisibilityTimestamp: time.Now().Add(time.Second * time.Duration(continueAsNewAttributes.GetBackoffStartIntervalInSeconds())),
		}
		continueAsNew.TimerTasks = append(continueAsNew.TimerTasks, backoffTimer)
	}
	setTaskInfo(
		newStateBuilder.GetCurrentVersion(),
		startedTime,
		continueAsNew.TransferTasks,
		continueAsNew.TimerTasks,
	)
	e.continueAsNew = continueAsNew
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	createRequestID string, attributes *workflow.StartChildWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent,
	*persistence.ChildExecutionInfo) {
	event := e.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)
	ci := e.ReplicateStartChildWorkflowExecutionInitiatedEvent(event, createRequestID)
	if ci == nil {
		return nil, nil
	}

	return event, ci
}

func (e *mutableStateBuilder) ReplicateStartChildWorkflowExecutionInitiatedEvent(event *workflow.HistoryEvent,
	createRequestID string) *persistence.ChildExecutionInfo {
	initiatedEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil
	}

	initiatedEventID := event.GetEventId()
	ci := &persistence.ChildExecutionInfo{
		Version:         event.GetVersion(),
		InitiatedID:     initiatedEventID,
		InitiatedEvent:  initiatedEvent,
		StartedID:       common.EmptyEventID,
		CreateRequestID: createRequestID,
	}

	e.pendingChildExecutionInfoIDs[initiatedEventID] = ci
	e.updateChildExecutionInfos[ci] = struct{}{}

	return ci
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionStartedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID int64) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionStarted, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	event := e.hBuilder.AddChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID)
	if err := e.ReplicateChildWorkflowExecutionStartedEvent(event); err != nil {
		return nil
	}

	return event
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionStartedEvent(event *workflow.HistoryEvent) error {
	attributes := event.ChildWorkflowExecutionStartedEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	ci, _ := e.GetChildExecutionInfo(initiatedID)
	startedEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return err
	}

	ci.StartedID = event.GetEventId()
	ci.StartedEvent = startedEvent
	e.updateChildExecutionInfos[ci] = struct{}{}

	return nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause workflow.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionStartChildExecutionFailed, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	event := e.hBuilder.AddStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)
	e.ReplicateStartChildWorkflowExecutionFailedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateStartChildWorkflowExecutionFailedEvent(event *workflow.HistoryEvent) {
	attributes := event.StartChildWorkflowExecutionFailedEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCompletedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.GetHistoryEvent(ci.StartedEvent)
	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	event := e.hBuilder.AddChildWorkflowExecutionCompletedEvent(domain, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	e.ReplicateChildWorkflowExecutionCompletedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionCompletedEvent(event *workflow.HistoryEvent) {
	attributes := event.ChildWorkflowExecutionCompletedEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionFailedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionFailed, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.GetHistoryEvent(ci.StartedEvent)
	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	event := e.hBuilder.AddChildWorkflowExecutionFailedEvent(domain, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	e.ReplicateChildWorkflowExecutionFailedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionFailedEvent(event *workflow.HistoryEvent) {
	attributes := event.ChildWorkflowExecutionFailedEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCanceledEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.GetHistoryEvent(ci.StartedEvent)
	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	event := e.hBuilder.AddChildWorkflowExecutionCanceledEvent(domain, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	e.ReplicateChildWorkflowExecutionCanceledEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionCanceledEvent(event *workflow.HistoryEvent) {
	attributes := event.ChildWorkflowExecutionCanceledEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTerminatedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionTerminated, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.GetHistoryEvent(ci.StartedEvent)
	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	event := e.hBuilder.AddChildWorkflowExecutionTerminatedEvent(domain, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	e.ReplicateChildWorkflowExecutionTerminatedEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionTerminatedEvent(event *workflow.HistoryEvent) {
	attributes := event.ChildWorkflowExecutionTerminatedEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTimedOutEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionTimedOut, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.GetHistoryEvent(ci.StartedEvent)
	domain := startedEvent.ChildWorkflowExecutionStartedEventAttributes.Domain
	workflowType := startedEvent.ChildWorkflowExecutionStartedEventAttributes.WorkflowType

	event := e.hBuilder.AddChildWorkflowExecutionTimedOutEvent(domain, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	e.ReplicateChildWorkflowExecutionTimedOutEvent(event)

	return event
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionTimedOutEvent(event *workflow.HistoryEvent) {
	attributes := event.ChildWorkflowExecutionTimedOutEventAttributes
	initiatedID := attributes.GetInitiatedEventId()

	e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) CreateRetryTimer(ai *persistence.ActivityInfo, failureReason string) persistence.Task {
	retryTask := prepareNextRetry(ai, failureReason)
	if retryTask != nil {
		e.updateActivityInfos[ai] = struct{}{}
	}

	return retryTask
}

func (e *mutableStateBuilder) GetContinueAsNew() *persistence.CreateWorkflowExecutionRequest {
	return e.continueAsNew
}
