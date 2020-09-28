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

package history

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/checksum"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	emptyUUID = "emptyUuid"

	mutableStateInvalidHistoryActionMsg         = "invalid history builder state for action"
	mutableStateInvalidHistoryActionMsgTemplate = mutableStateInvalidHistoryActionMsg + ": %v"
)

var (
	// ErrWorkflowFinished indicates trying to mutate mutable state after workflow finished
	ErrWorkflowFinished = serviceerror.NewInternal("invalid mutable state action: mutation after finish")
	// ErrMissingTimerInfo indicates missing timer info
	ErrMissingTimerInfo = serviceerror.NewInternal("unable to get timer info")
	// ErrMissingActivityInfo indicates missing activity info
	ErrMissingActivityInfo = serviceerror.NewInternal("unable to get activity info")
	// ErrMissingChildWorkflowInfo indicates missing child workflow info
	ErrMissingChildWorkflowInfo = serviceerror.NewInternal("unable to get child workflow info")
	// ErrMissingRequestCancelInfo indicates missing request cancel info
	ErrMissingRequestCancelInfo = serviceerror.NewInternal("unable to get request cancel info")
	// ErrMissingSignalInfo indicates missing signal external
	ErrMissingSignalInfo = serviceerror.NewInternal("unable to get signal info")
	// ErrMissingWorkflowStartEvent indicates missing workflow start event
	ErrMissingWorkflowStartEvent = serviceerror.NewInternal("unable to get workflow start event")
	// ErrMissingWorkflowCompletionEvent indicates missing workflow completion event
	ErrMissingWorkflowCompletionEvent = serviceerror.NewInternal("unable to get workflow completion event")
	// ErrMissingActivityScheduledEvent indicates missing workflow activity scheduled event
	ErrMissingActivityScheduledEvent = serviceerror.NewInternal("unable to get activity scheduled event")
	// ErrMissingChildWorkflowInitiatedEvent indicates missing child workflow initiated event
	ErrMissingChildWorkflowInitiatedEvent = serviceerror.NewInternal("unable to get child workflow initiated event")
)

type (
	mutableStateBuilder struct {
		pendingActivityTimerHeartbeats map[int64]time.Time                         // Schedule Event ID -> LastHeartbeatTimeoutVisibilityInSeconds.
		pendingActivityInfoIDs         map[int64]*persistenceblobs.ActivityInfo    // Schedule Event ID -> Activity Info.
		pendingActivityIDToEventID     map[string]int64                            // Activity ID -> Schedule Event ID of the activity.
		updateActivityInfos            map[*persistenceblobs.ActivityInfo]struct{} // Modified activities from last update.
		deleteActivityInfos            map[int64]struct{}                          // Deleted activities from last update.
		syncActivityTasks              map[int64]struct{}                          // Activity to be sync to remote

		pendingTimerInfoIDs     map[string]*persistenceblobs.TimerInfo   // User Timer ID -> Timer Info.
		pendingTimerEventIDToID map[int64]string                         // User Timer Start Event ID -> User Timer ID.
		updateTimerInfos        map[*persistenceblobs.TimerInfo]struct{} // Modified timers from last update.
		deleteTimerInfos        map[string]struct{}                      // Deleted timers from last update.

		pendingChildExecutionInfoIDs map[int64]*persistenceblobs.ChildExecutionInfo    // Initiated Event ID -> Child Execution Info
		updateChildExecutionInfos    map[*persistenceblobs.ChildExecutionInfo]struct{} // Modified ChildExecution Infos since last update
		deleteChildExecutionInfo     *int64                                            // Deleted ChildExecution Info since last update

		pendingRequestCancelInfoIDs map[int64]*persistenceblobs.RequestCancelInfo    // Initiated Event ID -> RequestCancelInfo
		updateRequestCancelInfos    map[*persistenceblobs.RequestCancelInfo]struct{} // Modified RequestCancel Infos since last update, for persistence update
		deleteRequestCancelInfo     *int64                                           // Deleted RequestCancel Info since last update, for persistence update

		pendingSignalInfoIDs map[int64]*persistenceblobs.SignalInfo    // Initiated Event ID -> SignalInfo
		updateSignalInfos    map[*persistenceblobs.SignalInfo]struct{} // Modified SignalInfo since last update
		deleteSignalInfo     *int64                                    // Deleted SignalInfo since last update

		pendingSignalRequestedIDs map[string]struct{} // Set of signaled requestIds
		updateSignalRequestedIDs  map[string]struct{} // Set of signaled requestIds since last update
		deleteSignalRequestedID   string              // Deleted signaled requestId

		bufferedEvents       []*historypb.HistoryEvent // buffered history events that are already persisted
		updateBufferedEvents []*historypb.HistoryEvent // buffered history events that needs to be persisted
		clearBufferedEvents  bool                      // delete buffered events from persistence

		executionInfo    *persistence.WorkflowExecutionInfo // Workflow mutable state info.
		versionHistories *persistence.VersionHistories
		hBuilder         *historyBuilder

		// in memory only attributes
		// indicate the current version
		currentVersion int64
		// indicates whether there are buffered events in persistence
		hasBufferedEventsInDB bool
		// indicates the workflow state in DB, can be used to calculate
		// whether this workflow is pointed by current workflow record
		stateInDB enumsspb.WorkflowExecutionState
		// indicates the next event ID in DB, for conditional update
		nextEventIDInDB int64
		// namespace entry contains a snapshot of namespace
		// NOTE: do not use the failover version inside, use currentVersion above
		namespaceEntry *cache.NamespaceCacheEntry
		// record if a event has been applied to mutable state
		// TODO: persist this to db
		appliedEvents map[string]struct{}

		insertTransferTasks    []persistence.Task
		insertReplicationTasks []persistence.Task
		insertTimerTasks       []persistence.Task

		// do not rely on this, this is only updated on
		// Load() and closeTransactionXXX methods. So when
		// a transaction is in progress, this value will be
		// wrong. This exist primarily for visibility via CLI
		checksum checksum.Checksum

		taskGenerator       mutableStateTaskGenerator
		workflowTaskManager mutableStateWorkflowTaskManager
		queryRegistry       queryRegistry

		shard           ShardContext
		clusterMetadata cluster.Metadata
		eventsCache     eventsCache
		config          *Config
		timeSource      clock.TimeSource
		logger          log.Logger
		metricsClient   metrics.Client
	}
)

var _ mutableState = (*mutableStateBuilder)(nil)

func newMutableStateBuilder(
	shard ShardContext,
	eventsCache eventsCache,
	logger log.Logger,
	namespaceEntry *cache.NamespaceCacheEntry,
) *mutableStateBuilder {
	s := &mutableStateBuilder{
		updateActivityInfos:            make(map[*persistenceblobs.ActivityInfo]struct{}),
		pendingActivityTimerHeartbeats: make(map[int64]time.Time),
		pendingActivityInfoIDs:         make(map[int64]*persistenceblobs.ActivityInfo),
		pendingActivityIDToEventID:     make(map[string]int64),
		deleteActivityInfos:            make(map[int64]struct{}),
		syncActivityTasks:              make(map[int64]struct{}),

		pendingTimerInfoIDs:     make(map[string]*persistenceblobs.TimerInfo),
		pendingTimerEventIDToID: make(map[int64]string),
		updateTimerInfos:        make(map[*persistenceblobs.TimerInfo]struct{}),
		deleteTimerInfos:        make(map[string]struct{}),

		updateChildExecutionInfos:    make(map[*persistenceblobs.ChildExecutionInfo]struct{}),
		pendingChildExecutionInfoIDs: make(map[int64]*persistenceblobs.ChildExecutionInfo),
		deleteChildExecutionInfo:     nil,

		updateRequestCancelInfos:    make(map[*persistenceblobs.RequestCancelInfo]struct{}),
		pendingRequestCancelInfoIDs: make(map[int64]*persistenceblobs.RequestCancelInfo),
		deleteRequestCancelInfo:     nil,

		updateSignalInfos:    make(map[*persistenceblobs.SignalInfo]struct{}),
		pendingSignalInfoIDs: make(map[int64]*persistenceblobs.SignalInfo),
		deleteSignalInfo:     nil,

		updateSignalRequestedIDs:  make(map[string]struct{}),
		pendingSignalRequestedIDs: make(map[string]struct{}),
		deleteSignalRequestedID:   "",

		currentVersion:        namespaceEntry.GetFailoverVersion(),
		hasBufferedEventsInDB: false,
		stateInDB:             enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
		nextEventIDInDB:       0,
		namespaceEntry:        namespaceEntry,
		appliedEvents:         make(map[string]struct{}),

		queryRegistry: newQueryRegistry(),

		shard:           shard,
		clusterMetadata: shard.GetClusterMetadata(),
		eventsCache:     eventsCache,
		config:          shard.GetConfig(),
		timeSource:      shard.GetTimeSource(),
		logger:          logger,
		metricsClient:   shard.GetMetricsClient(),
	}
	s.executionInfo = &persistence.WorkflowExecutionInfo{
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleId: common.EmptyEventID,
		WorkflowTaskStartedId:  common.EmptyEventID,
		WorkflowTaskRequestId:  emptyUUID,
		WorkflowTaskTimeout:    timestamp.DurationFromSeconds(0),
		WorkflowTaskAttempt:    1,

		NextEventId: common.FirstEventID,
		ExecutionState: &persistenceblobs.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		LastProcessedEvent: common.EmptyEventID,
	}
	s.hBuilder = newHistoryBuilder(s, logger)
	s.taskGenerator = newMutableStateTaskGenerator(shard.GetNamespaceCache(), s.logger, s)
	s.workflowTaskManager = newMutableStateWorkflowTaskManager(s)

	return s
}

func newMutableStateBuilderWithVersionHistories(
	shard ShardContext,
	eventsCache eventsCache,
	logger log.Logger,
	namespaceEntry *cache.NamespaceCacheEntry,
) *mutableStateBuilder {

	s := newMutableStateBuilder(shard, eventsCache, logger, namespaceEntry)
	s.versionHistories = persistence.NewVersionHistories(&persistence.VersionHistory{})
	return s
}

func (e *mutableStateBuilder) CopyToPersistence() *persistence.WorkflowMutableState {
	state := &persistence.WorkflowMutableState{}

	state.ActivityInfos = e.pendingActivityInfoIDs
	state.TimerInfos = e.pendingTimerInfoIDs
	state.ChildExecutionInfos = e.pendingChildExecutionInfoIDs
	state.RequestCancelInfos = e.pendingRequestCancelInfoIDs
	state.SignalInfos = e.pendingSignalInfoIDs
	state.SignalRequestedIDs = e.pendingSignalRequestedIDs
	state.ExecutionInfo = e.executionInfo
	state.BufferedEvents = e.bufferedEvents
	state.VersionHistories = e.versionHistories
	state.Checksum = e.checksum

	return state
}

func (e *mutableStateBuilder) Load(
	state *persistence.WorkflowMutableState,
) {

	e.pendingActivityInfoIDs = state.ActivityInfos
	for _, activityInfo := range state.ActivityInfos {
		e.pendingActivityIDToEventID[activityInfo.ActivityId] = activityInfo.ScheduleId
		if (activityInfo.TimerTaskStatus & timerTaskStatusCreatedHeartbeat) > 0 {
			// Sets last pending timer heartbeat to year 2000.
			// This ensures at least one heartbeat task will be processed for the pending activity.
			e.pendingActivityTimerHeartbeats[activityInfo.ScheduleId] = time.Unix(946684800, 0)
		}
	}
	e.pendingTimerInfoIDs = state.TimerInfos
	for _, timerInfo := range state.TimerInfos {
		e.pendingTimerEventIDToID[timerInfo.GetStartedId()] = timerInfo.GetTimerId()
	}
	e.pendingChildExecutionInfoIDs = state.ChildExecutionInfos
	e.pendingRequestCancelInfoIDs = state.RequestCancelInfos
	e.pendingSignalInfoIDs = state.SignalInfos
	e.pendingSignalRequestedIDs = state.SignalRequestedIDs
	e.executionInfo = state.ExecutionInfo

	e.bufferedEvents = state.BufferedEvents

	e.currentVersion = common.EmptyVersion
	e.hasBufferedEventsInDB = len(e.bufferedEvents) > 0
	e.stateInDB = state.ExecutionInfo.ExecutionState.State
	e.nextEventIDInDB = state.ExecutionInfo.NextEventId
	e.versionHistories = state.VersionHistories
	e.checksum = state.Checksum

	if len(state.Checksum.Value) > 0 {
		switch {
		case e.shouldInvalidateCheckum():
			e.checksum = checksum.Checksum{}
			e.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.MutableStateChecksumInvalidated)
		case e.shouldVerifyChecksum():
			if err := verifyMutableStateChecksum(e, state.Checksum); err != nil {
				// we ignore checksum verification errors for now until this
				// feature is tested and/or we have mechanisms in place to deal
				// with these types of errors
				e.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.MutableStateChecksumMismatch)
				e.logError("mutable state checksum mismatch", tag.Error(err))
			}
		}
	}
}

func (e *mutableStateBuilder) GetCurrentBranchToken() ([]byte, error) {
	if e.versionHistories != nil {
		currentVersionHistory, err := e.versionHistories.GetCurrentVersionHistory()
		if err != nil {
			return nil, err
		}
		return currentVersionHistory.GetBranchToken(), nil
	}
	return e.executionInfo.EventBranchToken, nil
}

func (e *mutableStateBuilder) GetVersionHistories() *persistence.VersionHistories {
	return e.versionHistories
}

// set treeID/historyBranches
func (e *mutableStateBuilder) SetHistoryTree(
	treeID string,
) error {

	initialBranchToken, err := persistence.NewHistoryBranchToken(treeID)
	if err != nil {
		return err
	}
	return e.SetCurrentBranchToken(initialBranchToken)
}

func (e *mutableStateBuilder) SetCurrentBranchToken(
	branchToken []byte,
) error {

	exeInfo := e.GetExecutionInfo()
	if e.versionHistories == nil {
		exeInfo.EventBranchToken = branchToken
		return nil
	}

	currentVersionHistory, err := e.versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return err
	}
	return currentVersionHistory.SetBranchToken(branchToken)
}

func (e *mutableStateBuilder) SetVersionHistories(
	versionHistories *persistence.VersionHistories,
) error {

	e.versionHistories = versionHistories
	return nil
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

func (e *mutableStateBuilder) FlushBufferedEvents() error {
	// put new events into 2 buckets:
	//  1) if the event was added while there was in-flight workflow task, then put it in buffered bucket
	//  2) otherwise, put it in committed bucket
	var newBufferedEvents []*historypb.HistoryEvent
	var newCommittedEvents []*historypb.HistoryEvent
	for _, event := range e.hBuilder.history {
		if event.GetEventId() == common.BufferedEventID {
			newBufferedEvents = append(newBufferedEvents, event)
		} else {
			newCommittedEvents = append(newCommittedEvents, event)
		}
	}

	// Sometimes we see buffered events are out of order when read back from database.  This is mostly not an issue
	// except in the Activity case where ActivityStarted and ActivityCompleted gets out of order.  The following code
	// is added to reorder buffered events to guarantee all activity completion events will always be processed at the end.
	var reorderedEvents []*historypb.HistoryEvent
	reorderFunc := func(bufferedEvents []*historypb.HistoryEvent) {
		for _, event := range bufferedEvents {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
				enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
				enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
				enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
				reorderedEvents = append(reorderedEvents, event)
			case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
				enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
				reorderedEvents = append(reorderedEvents, event)
			default:
				newCommittedEvents = append(newCommittedEvents, event)
			}
		}
	}

	// no workflow task in-flight, flush all buffered events to committed bucket
	if !e.HasInFlightWorkflowTask() {
		// flush persisted buffered events
		if len(e.bufferedEvents) > 0 {
			reorderFunc(e.bufferedEvents)
			e.bufferedEvents = nil
		}
		if e.hasBufferedEventsInDB {
			e.clearBufferedEvents = true
		}

		// flush pending buffered events
		reorderFunc(e.updateBufferedEvents)
		// clear pending buffered events
		e.updateBufferedEvents = nil

		// Put back all the reordered buffer events at the end
		if len(reorderedEvents) > 0 {
			newCommittedEvents = append(newCommittedEvents, reorderedEvents...)
		}

		// flush new buffered events that were not saved to persistence yet
		newCommittedEvents = append(newCommittedEvents, newBufferedEvents...)
		newBufferedEvents = nil
	}

	newCommittedEvents = e.trimEventsAfterWorkflowClose(newCommittedEvents)
	e.hBuilder.history = newCommittedEvents
	// make sure all new committed events have correct EventID
	e.assignEventIDToBufferedEvents()
	if err := e.assignTaskIDToEvents(); err != nil {
		return err
	}

	// if workflow task is not closed yet, and there are new buffered events, then put those to the pending buffer
	if e.HasInFlightWorkflowTask() && len(newBufferedEvents) > 0 {
		e.updateBufferedEvents = newBufferedEvents
	}

	return nil
}

func (e *mutableStateBuilder) UpdateCurrentVersion(
	version int64,
	forceUpdate bool,
) error {

	if state, _ := e.GetWorkflowStateStatus(); state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// do not update current version only when workflow is completed
		return nil
	}

	if e.versionHistories != nil {
		versionHistory, err := e.versionHistories.GetCurrentVersionHistory()
		if err != nil {
			return err
		}

		if !versionHistory.IsEmpty() {
			// this make sure current version >= last write version
			versionHistoryItem, err := versionHistory.GetLastItem()
			if err != nil {
				return err
			}
			e.currentVersion = versionHistoryItem.GetVersion()
		}

		if version > e.currentVersion || forceUpdate {
			e.currentVersion = version
		}

		return nil
	}

	// TODO: All mutable state should have versioned histories.  Even local namespace should have it to allow for
	// re-replication of local namespaces to other clusters.
	// We probably need an error if mutableState does not have versioned history.
	e.currentVersion = common.EmptyVersion
	return nil
}

func (e *mutableStateBuilder) GetCurrentVersion() int64 {

	if e.versionHistories != nil {
		return e.currentVersion
	}

	return common.EmptyVersion
}

func (e *mutableStateBuilder) GetStartVersion() (int64, error) {

	if e.versionHistories != nil {
		versionHistory, err := e.versionHistories.GetCurrentVersionHistory()
		if err != nil {
			return 0, err
		}
		firstItem, err := versionHistory.GetFirstItem()
		if err != nil {
			return 0, err
		}
		return firstItem.GetVersion(), nil
	}

	return common.EmptyVersion, nil
}

func (e *mutableStateBuilder) GetLastWriteVersion() (int64, error) {

	if e.versionHistories != nil {
		versionHistory, err := e.versionHistories.GetCurrentVersionHistory()
		if err != nil {
			return 0, err
		}
		lastItem, err := versionHistory.GetLastItem()
		if err != nil {
			return 0, err
		}
		return lastItem.GetVersion(), nil
	}

	return common.EmptyVersion, nil
}

func (e *mutableStateBuilder) scanForBufferedActivityCompletion(
	scheduleID int64,
) *historypb.HistoryEvent {
	var completionEvent *historypb.HistoryEvent

	completionEvent = scanForBufferedActivityCompletion(scheduleID, e.bufferedEvents)
	if completionEvent != nil {
		return completionEvent
	}

	return scanForBufferedActivityCompletion(scheduleID, e.updateBufferedEvents)
}

func scanForBufferedActivityCompletion(
	scheduleID int64,
	events []*historypb.HistoryEvent,
) *historypb.HistoryEvent {
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId() == scheduleID {
				return event
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			if event.GetActivityTaskFailedEventAttributes().GetScheduledEventId() == scheduleID {
				return event
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			if event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId() == scheduleID {
				return event
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			if event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId() == scheduleID {
				return event
			}
		}
	}

	// Completion event not found
	return nil
}

func (e *mutableStateBuilder) checkAndClearTimerFiredEvent(
	timerID string,
) *historypb.HistoryEvent {

	var timerEvent *historypb.HistoryEvent

	e.bufferedEvents, timerEvent = checkAndClearTimerFiredEvent(e.bufferedEvents, timerID)
	if timerEvent != nil {
		return timerEvent
	}
	e.updateBufferedEvents, timerEvent = checkAndClearTimerFiredEvent(e.updateBufferedEvents, timerID)
	if timerEvent != nil {
		return timerEvent
	}
	e.hBuilder.history, timerEvent = checkAndClearTimerFiredEvent(e.hBuilder.history, timerID)
	return timerEvent
}

func checkAndClearTimerFiredEvent(
	events []*historypb.HistoryEvent,
	timerID string,
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

func (e *mutableStateBuilder) trimEventsAfterWorkflowClose(
	input []*historypb.HistoryEvent,
) []*historypb.HistoryEvent {

	if len(input) == 0 {
		return input
	}

	nextIndex := 0

loop:
	for _, event := range input {
		nextIndex++

		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:

			break loop
		}
	}

	return input[0:nextIndex]
}

func (e *mutableStateBuilder) assignEventIDToBufferedEvents() {
	newCommittedEvents := e.hBuilder.history

	scheduledIDToStartedID := make(map[int64]int64)
	for _, event := range newCommittedEvents {
		if event.GetEventId() != common.BufferedEventID {
			continue
		}

		eventID := e.executionInfo.NextEventId
		event.EventId = eventID
		e.executionInfo.IncreaseNextEventID()

		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attributes := event.GetActivityTaskStartedEventAttributes()
			scheduledID := attributes.GetScheduledEventId()
			scheduledIDToStartedID[scheduledID] = eventID
			if ai, ok := e.GetActivityInfo(scheduledID); ok {
				ai.StartedId = eventID
				e.updateActivityInfos[ai] = struct{}{}
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
			initiatedID := attributes.GetInitiatedEventId()
			scheduledIDToStartedID[initiatedID] = eventID
			if ci, ok := e.GetChildExecutionInfo(initiatedID); ok {
				ci.StartedId = eventID
				e.updateChildExecutionInfos[ci] = struct{}{}
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			attributes := event.GetActivityTaskFailedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			attributes := event.GetActivityTaskTimedOutEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			attributes := event.GetActivityTaskCanceledEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		}
	}
}

func (e *mutableStateBuilder) assignTaskIDToEvents() error {

	// assign task IDs to all history events
	// first transient events
	numTaskIDs := len(e.hBuilder.transientHistory)
	if numTaskIDs > 0 {
		taskIDs, err := e.shard.GenerateTransferTaskIDs(numTaskIDs)
		if err != nil {
			return err
		}

		for index, event := range e.hBuilder.transientHistory {
			if event.GetTaskId() == common.EmptyEventTaskID {
				taskID := taskIDs[index]
				event.TaskId = taskID
				e.executionInfo.LastEventTaskId = taskID
			}
		}
	}

	// then normal events
	numTaskIDs = len(e.hBuilder.history)
	if numTaskIDs > 0 {
		taskIDs, err := e.shard.GenerateTransferTaskIDs(numTaskIDs)
		if err != nil {
			return err
		}

		for index, event := range e.hBuilder.history {
			if event.GetTaskId() == common.EmptyEventTaskID {
				taskID := taskIDs[index]
				event.TaskId = taskID
				e.executionInfo.LastEventTaskId = taskID
			}
		}
	}

	return nil
}

func (e *mutableStateBuilder) IsCurrentWorkflowGuaranteed() bool {
	// stateInDB is used like a bloom filter:
	//
	// 1. stateInDB being created / running meaning that this workflow must be the current
	//  workflow (assuming there is no rebuild of mutable state).
	// 2. stateInDB being completed does not guarantee this workflow being the current workflow
	// 3. stateInDB being zombie guarantees this workflow not being the current workflow
	// 4. stateInDB cannot be void, void is only possible when mutable state is just initialized

	switch e.stateInDB {
	case enumsspb.WORKFLOW_EXECUTION_STATE_VOID:
		return false
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
		return true
	case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return true
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return false
	case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
		return false
	case enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED:
		return false
	default:
		panic(fmt.Sprintf("unknown workflow state: %v", e.executionInfo.ExecutionState.State))
	}
}

func (e *mutableStateBuilder) GetNamespaceEntry() *cache.NamespaceCacheEntry {
	return e.namespaceEntry
}

func (e *mutableStateBuilder) IsStickyTaskQueueEnabled() bool {
	if e.executionInfo.StickyTaskQueue == "" {
		return false
	}
	ttl := e.config.StickyTTL(e.GetNamespaceEntry().GetInfo().Name)
	if e.timeSource.Now().After(timestamp.TimeValue(e.executionInfo.LastUpdatedTime).Add(ttl)) {
		return false
	}
	return true
}

func (e *mutableStateBuilder) CreateNewHistoryEvent(
	eventType enumspb.EventType,
) *historypb.HistoryEvent {

	return e.CreateNewHistoryEventWithTime(eventType, e.timeSource.Now())
}

func (e *mutableStateBuilder) CreateNewHistoryEventWithTime(
	eventType enumspb.EventType,
	time time.Time,
) *historypb.HistoryEvent {
	eventID := e.executionInfo.NextEventId
	if e.shouldBufferEvent(eventType) {
		eventID = common.BufferedEventID
	} else {
		// only increase NextEventID if event is not buffered
		e.executionInfo.IncreaseNextEventID()
	}

	historyEvent := &historypb.HistoryEvent{}
	historyEvent.EventId = eventID
	historyEvent.EventTime = &time
	historyEvent.EventType = eventType
	historyEvent.Version = e.GetCurrentVersion()
	historyEvent.TaskId = common.EmptyEventTaskID

	return historyEvent
}

func (e *mutableStateBuilder) shouldBufferEvent(
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
		// workflow complete, failed, cancelled and continue-as-new events are duplication of above
		// just put is here for reference
		// workflow.EventTypeWorkflowExecutionCompleted,
		// workflow.EventTypeWorkflowExecutionFailed,
		// workflow.EventTypeWorkflowExecutionCanceled,
		// workflow.EventTypeWorkflowExecutionContinuedAsNew,
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
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		// do not buffer event if event is directly generated from a corresponding command

		// sanity check there is no workflow task on the fly
		if e.HasInFlightWorkflowTask() {
			msg := fmt.Sprintf("history mutable state is processing event: %v while there is workflow task pending. "+
				"namespaceID: %v, workflow ID: %v, run ID: %v.", eventType, e.executionInfo.NamespaceId, e.executionInfo.WorkflowId, e.executionInfo.ExecutionState.RunId)
			panic(msg)
		}
		return false
	default:
		return true
	}
}

func (e *mutableStateBuilder) GetWorkflowType() *commonpb.WorkflowType {
	wType := &commonpb.WorkflowType{}
	wType.Name = e.executionInfo.WorkflowTypeName

	return wType
}

func (e *mutableStateBuilder) GetQueryRegistry() queryRegistry {
	return e.queryRegistry
}

func (e *mutableStateBuilder) GetActivityScheduledEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {

	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, ErrMissingActivityInfo
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	scheduledEvent, err := e.eventsCache.getEvent(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionInfo.ExecutionState.RunId,
		ai.ScheduledEventBatchId,
		ai.ScheduleId,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingActivityScheduledEvent
	}
	return scheduledEvent, nil
}

// GetActivityInfo gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityInfo(
	scheduleEventID int64,
) (*persistenceblobs.ActivityInfo, bool) {

	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	return ai, ok
}

// GetActivityInfo gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityInfoWithTimerHeartbeat(
	scheduleEventID int64,
) (*persistenceblobs.ActivityInfo, time.Time, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	timerVis, ok := e.pendingActivityTimerHeartbeats[scheduleEventID]

	return ai, timerVis, ok
}

// GetActivityByActivityID gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityByActivityID(
	activityID string,
) (*persistenceblobs.ActivityInfo, bool) {

	eventID, ok := e.pendingActivityIDToEventID[activityID]
	if !ok {
		return nil, false
	}
	return e.GetActivityInfo(eventID)
}

// GetChildExecutionInfo gives details about a child execution that is currently in progress.
func (e *mutableStateBuilder) GetChildExecutionInfo(
	initiatedEventID int64,
) (*persistenceblobs.ChildExecutionInfo, bool) {

	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	return ci, ok
}

// GetChildExecutionInitiatedEvent reads out the ChildExecutionInitiatedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionInitiatedEvent(
	initiatedEventID int64,
) (*historypb.HistoryEvent, error) {

	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingChildWorkflowInfo
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	initiatedEvent, err := e.eventsCache.getEvent(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionInfo.ExecutionState.RunId,
		ci.InitiatedEventBatchId,
		ci.InitiatedId,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingChildWorkflowInitiatedEvent
	}
	return initiatedEvent, nil
}

// GetRequestCancelInfo gives details about a request cancellation that is currently in progress.
func (e *mutableStateBuilder) GetRequestCancelInfo(
	initiatedEventID int64,
) (*persistenceblobs.RequestCancelInfo, bool) {

	ri, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	return ri, ok
}

func (e *mutableStateBuilder) GetRetryBackoffDuration(
	failure *failurepb.Failure,
) (time.Duration, enumspb.RetryState) {

	info := e.executionInfo
	if !info.HasRetryPolicy {
		return backoff.NoBackoff, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
	}

	return getBackoffInterval(
		e.timeSource.Now(),
		timestamp.TimeValue(info.WorkflowExpirationTime),
		info.Attempt,
		info.RetryMaximumAttempts,
		info.RetryInitialInterval,
		info.RetryMaximumInterval,
		info.RetryBackoffCoefficient,
		failure,
		info.RetryNonRetryableErrorTypes,
	)
}

func (e *mutableStateBuilder) GetCronBackoffDuration() (time.Duration, error) {
	info := e.executionInfo
	if len(info.CronSchedule) == 0 {
		return backoff.NoBackoff, nil
	}
	// TODO: decide if we can add execution time in execution info.
	executionTime := timestamp.TimeValue(e.executionInfo.StartTime)
	// This only call when doing ContinueAsNew. At this point, the workflow should have a start event
	workflowStartEvent, err := e.GetStartEvent()
	if err != nil {
		e.logError("unable to find workflow start event", tag.ErrorTypeInvalidHistoryAction)
		return backoff.NoBackoff, err
	}
	firstWorkflowTaskBackoff := timestamp.DurationValue(workflowStartEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstWorkflowTaskBackoff())
	executionTime = executionTime.Add(firstWorkflowTaskBackoff)
	return backoff.GetBackoffForNextSchedule(info.CronSchedule, executionTime, e.timeSource.Now()), nil
}

// GetSignalInfo get details about a signal request that is currently in progress.
func (e *mutableStateBuilder) GetSignalInfo(
	initiatedEventID int64,
) (*persistenceblobs.SignalInfo, bool) {

	ri, ok := e.pendingSignalInfoIDs[initiatedEventID]
	return ri, ok
}

// GetCompletionEvent retrieves the workflow completion event from mutable state
func (e *mutableStateBuilder) GetCompletionEvent() (*historypb.HistoryEvent, error) {
	if e.executionInfo.ExecutionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil, ErrMissingWorkflowCompletionEvent
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	// Completion EventID is always one less than NextEventID after workflow is completed
	completionEventID := e.executionInfo.NextEventId - 1
	firstEventID := e.executionInfo.CompletionEventBatchId
	completionEvent, err := e.eventsCache.getEvent(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionInfo.ExecutionState.RunId,
		firstEventID,
		completionEventID,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingWorkflowCompletionEvent
	}

	return completionEvent, nil
}

// GetStartEvent retrieves the workflow start event from mutable state
func (e *mutableStateBuilder) GetStartEvent() (*historypb.HistoryEvent, error) {

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	startEvent, err := e.eventsCache.getEvent(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionInfo.ExecutionState.RunId,
		common.FirstEventID,
		common.FirstEventID,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingWorkflowStartEvent
	}
	return startEvent, nil
}

// DeletePendingChildExecution deletes details about a ChildExecutionInfo.
func (e *mutableStateBuilder) DeletePendingChildExecution(
	initiatedEventID int64,
) error {

	if _, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]; ok {
		delete(e.pendingChildExecutionInfoIDs, initiatedEventID)
	} else {
		e.logError(
			fmt.Sprintf("unable to find child workflow event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		e.logDataInconsistency()
	}

	e.deleteChildExecutionInfo = &initiatedEventID
	return nil
}

// DeletePendingRequestCancel deletes details about a RequestCancelInfo.
func (e *mutableStateBuilder) DeletePendingRequestCancel(
	initiatedEventID int64,
) error {

	if _, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]; ok {
		delete(e.pendingRequestCancelInfoIDs, initiatedEventID)
	} else {
		e.logError(
			fmt.Sprintf("unable to find request cancel external workflow event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		e.logDataInconsistency()
	}

	e.deleteRequestCancelInfo = &initiatedEventID
	return nil
}

// DeletePendingSignal deletes details about a SignalInfo
func (e *mutableStateBuilder) DeletePendingSignal(
	initiatedEventID int64,
) error {

	if _, ok := e.pendingSignalInfoIDs[initiatedEventID]; ok {
		delete(e.pendingSignalInfoIDs, initiatedEventID)
	} else {
		e.logError(
			fmt.Sprintf("unable to find signal external workflow event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		e.logDataInconsistency()
	}

	e.deleteSignalInfo = &initiatedEventID
	return nil
}

func (e *mutableStateBuilder) writeEventToCache(
	event *historypb.HistoryEvent,
) {

	// For start event: store it within events cache so the recordWorkflowStarted transfer task doesn't need to
	// load it from database
	// For completion event: store it within events cache so we can communicate the result to parent execution
	// during the processing of DeleteTransferTask without loading this event from database
	e.eventsCache.putEvent(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionInfo.ExecutionState.RunId,
		event.GetEventId(),
		event,
	)
}

func (e *mutableStateBuilder) HasParentExecution() bool {
	return e.executionInfo.ParentNamespaceId != "" && e.executionInfo.ParentWorkflowId != ""
}

func (e *mutableStateBuilder) UpdateActivityProgress(
	ai *persistenceblobs.ActivityInfo,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) {
	ai.Version = e.GetCurrentVersion()
	ai.LastHeartbeatDetails = request.Details
	now := e.timeSource.Now()
	ai.LastHeartbeatUpdateTime = &now
	e.updateActivityInfos[ai] = struct{}{}
	e.syncActivityTasks[ai.ScheduleId] = struct{}{}
}

// ReplicateActivityInfo replicate the necessary activity information
func (e *mutableStateBuilder) ReplicateActivityInfo(
	request *historyservice.SyncActivityRequest,
	resetActivityTimerTaskStatus bool,
) error {
	ai, ok := e.pendingActivityInfoIDs[request.GetScheduledId()]
	if !ok {
		e.logError(
			fmt.Sprintf("unable to find activity event ID: %v in mutable state", request.GetScheduledId()),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ai.Version = request.GetVersion()
	ai.ScheduledTime = request.GetScheduledTime()
	ai.StartedId = request.GetStartedId()
	ai.LastHeartbeatUpdateTime = request.GetLastHeartbeatTime()
	if ai.StartedId == common.EmptyEventID {
		ai.StartedTime = timestamp.TimePtr(time.Time{})
	} else {
		ai.StartedTime = request.GetStartedTime()
	}
	ai.LastHeartbeatDetails = request.GetDetails()
	ai.Attempt = request.GetAttempt()
	ai.RetryLastWorkerIdentity = request.GetLastWorkerIdentity()
	ai.RetryLastFailure = request.GetLastFailure()

	if resetActivityTimerTaskStatus {
		ai.TimerTaskStatus = timerTaskStatusNone
	}

	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

// UpdateActivity updates an activity
func (e *mutableStateBuilder) UpdateActivity(
	ai *persistenceblobs.ActivityInfo,
) error {

	if _, ok := e.pendingActivityInfoIDs[ai.ScheduleId]; !ok {
		e.logError(
			fmt.Sprintf("unable to find activity ID: %v in mutable state", ai.ActivityId),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	e.pendingActivityInfoIDs[ai.ScheduleId] = ai
	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

// UpdateActivity updates an activity
func (e *mutableStateBuilder) UpdateActivityWithTimerHeartbeat(
	ai *persistenceblobs.ActivityInfo,
	timerTimeoutVisibility time.Time,
) error {

	err := e.UpdateActivity(ai)
	if err != nil {
		return err
	}

	e.pendingActivityTimerHeartbeats[ai.ScheduleId] = timerTimeoutVisibility
	return nil
}

// DeleteActivity deletes details about an activity.
func (e *mutableStateBuilder) DeleteActivity(
	scheduleEventID int64,
) error {

	if activityInfo, ok := e.pendingActivityInfoIDs[scheduleEventID]; ok {
		delete(e.pendingActivityInfoIDs, scheduleEventID)
		delete(e.pendingActivityTimerHeartbeats, scheduleEventID)

		if _, ok = e.pendingActivityIDToEventID[activityInfo.ActivityId]; ok {
			delete(e.pendingActivityIDToEventID, activityInfo.ActivityId)
		} else {
			e.logError(
				fmt.Sprintf("unable to find activity ID: %v in mutable state", activityInfo.ActivityId),
				tag.ErrorTypeInvalidMutableStateAction,
			)
			// log data inconsistency instead of returning an error
			e.logDataInconsistency()
		}
	} else {
		e.logError(
			fmt.Sprintf("unable to find activity event id: %v in mutable state", scheduleEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		e.logDataInconsistency()
	}

	e.deleteActivityInfos[scheduleEventID] = struct{}{}
	return nil
}

// GetUserTimerInfo gives details about a user timer.
func (e *mutableStateBuilder) GetUserTimerInfo(
	timerID string,
) (*persistenceblobs.TimerInfo, bool) {

	timerInfo, ok := e.pendingTimerInfoIDs[timerID]
	return timerInfo, ok
}

// GetUserTimerInfoByEventID gives details about a user timer.
func (e *mutableStateBuilder) GetUserTimerInfoByEventID(
	startEventID int64,
) (*persistenceblobs.TimerInfo, bool) {

	timerID, ok := e.pendingTimerEventIDToID[startEventID]
	if !ok {
		return nil, false
	}
	return e.GetUserTimerInfo(timerID)
}

// UpdateUserTimer updates the user timer in progress.
func (e *mutableStateBuilder) UpdateUserTimer(
	ti *persistenceblobs.TimerInfo,
) error {

	timerID, ok := e.pendingTimerEventIDToID[ti.GetStartedId()]
	if !ok {
		e.logError(
			fmt.Sprintf("unable to find timer event ID: %v in mutable state", ti.GetStartedId()),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingTimerInfo
	}

	if _, ok := e.pendingTimerInfoIDs[timerID]; !ok {
		e.logError(
			fmt.Sprintf("unable to find timer ID: %v in mutable state", timerID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingTimerInfo
	}

	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos[ti] = struct{}{}
	return nil
}

// DeleteUserTimer deletes an user timer.
func (e *mutableStateBuilder) DeleteUserTimer(
	timerID string,
) error {

	if timerInfo, ok := e.pendingTimerInfoIDs[timerID]; ok {
		delete(e.pendingTimerInfoIDs, timerID)

		if _, ok = e.pendingTimerEventIDToID[timerInfo.GetStartedId()]; ok {
			delete(e.pendingTimerEventIDToID, timerInfo.GetStartedId())
		} else {
			e.logError(
				fmt.Sprintf("unable to find timer event ID: %v in mutable state", timerID),
				tag.ErrorTypeInvalidMutableStateAction,
			)
			// log data inconsistency instead of returning an error
			e.logDataInconsistency()
		}
	} else {
		e.logError(
			fmt.Sprintf("unable to find timer ID: %v in mutable state", timerID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		e.logDataInconsistency()
	}

	e.deleteTimerInfos[timerID] = struct{}{}
	return nil
}

// nolint:unused
func (e *mutableStateBuilder) getWorkflowTaskInfo() *workflowTaskInfo {

	taskQueue := &taskqueuepb.TaskQueue{}
	if e.IsStickyTaskQueueEnabled() {
		taskQueue.Name = e.executionInfo.StickyTaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		taskQueue.Name = e.executionInfo.TaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_NORMAL
	}

	return &workflowTaskInfo{
		Version:                    e.executionInfo.WorkflowTaskVersion,
		ScheduleID:                 e.executionInfo.WorkflowTaskScheduleId,
		StartedID:                  e.executionInfo.WorkflowTaskStartedId,
		RequestID:                  e.executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:        e.executionInfo.WorkflowTaskTimeout,
		Attempt:                    e.executionInfo.WorkflowTaskAttempt,
		StartedTimestamp:           e.executionInfo.WorkflowTaskStartedTimestamp,
		ScheduledTimestamp:         e.executionInfo.WorkflowTaskScheduledTimestamp,
		TaskQueue:                  taskQueue,
		OriginalScheduledTimestamp: e.executionInfo.WorkflowTaskOriginalScheduledTimestamp,
	}
}

// GetWorkflowTaskInfo returns details about the in-progress workflow task
func (e *mutableStateBuilder) GetWorkflowTaskInfo(
	scheduleEventID int64,
) (*workflowTaskInfo, bool) {
	return e.workflowTaskManager.GetWorkflowTaskInfo(scheduleEventID)
}

func (e *mutableStateBuilder) GetPendingActivityInfos() map[int64]*persistenceblobs.ActivityInfo {
	return e.pendingActivityInfoIDs
}

func (e *mutableStateBuilder) GetPendingTimerInfos() map[string]*persistenceblobs.TimerInfo {
	return e.pendingTimerInfoIDs
}

func (e *mutableStateBuilder) GetPendingChildExecutionInfos() map[int64]*persistenceblobs.ChildExecutionInfo {
	return e.pendingChildExecutionInfoIDs
}

func (e *mutableStateBuilder) GetPendingRequestCancelExternalInfos() map[int64]*persistenceblobs.RequestCancelInfo {
	return e.pendingRequestCancelInfoIDs
}

func (e *mutableStateBuilder) GetPendingSignalExternalInfos() map[int64]*persistenceblobs.SignalInfo {
	return e.pendingSignalInfoIDs
}

func (e *mutableStateBuilder) HasProcessedOrPendingWorkflowTask() bool {
	return e.workflowTaskManager.HasProcessedOrPendingWorkflowTask()
}

func (e *mutableStateBuilder) HasPendingWorkflowTask() bool {
	return e.workflowTaskManager.HasPendingWorkflowTask()
}

func (e *mutableStateBuilder) GetPendingWorkflowTask() (*workflowTaskInfo, bool) {
	return e.workflowTaskManager.GetPendingWorkflowTask()
}

func (e *mutableStateBuilder) HasInFlightWorkflowTask() bool {
	return e.workflowTaskManager.HasInFlightWorkflowTask()
}

func (e *mutableStateBuilder) GetInFlightWorkflowTask() (*workflowTaskInfo, bool) {
	return e.workflowTaskManager.GetInFlightWorkflowTask()
}

func (e *mutableStateBuilder) HasBufferedEvents() bool {
	if len(e.bufferedEvents) > 0 || len(e.updateBufferedEvents) > 0 {
		return true
	}

	for _, event := range e.hBuilder.history {
		if event.GetEventId() == common.BufferedEventID {
			return true
		}
	}

	return false
}

// UpdateWorkflowTask updates a workflow task.
func (e *mutableStateBuilder) UpdateWorkflowTask(
	workflowTask *workflowTaskInfo,
) {
	e.workflowTaskManager.UpdateWorkflowTask(workflowTask)
}

// DeleteWorkflowTask deletes a workflow task.
func (e *mutableStateBuilder) DeleteWorkflowTask() {
	e.workflowTaskManager.DeleteWorkflowTask()
}

func (e *mutableStateBuilder) FailWorkflowTask(
	incrementAttempt bool,
) {
	e.workflowTaskManager.FailWorkflowTask(incrementAttempt)
}

func (e *mutableStateBuilder) ClearStickyness() {
	e.executionInfo.StickyTaskQueue = ""
	e.executionInfo.StickyScheduleToStartTimeout = timestamp.DurationFromSeconds(0)
}

// GetLastFirstEventID returns last first event ID
// first event ID is the ID of a batch of events in a single history events record
func (e *mutableStateBuilder) GetLastFirstEventID() int64 {
	return e.executionInfo.LastFirstEventId
}

// GetNextEventID returns next event ID
func (e *mutableStateBuilder) GetNextEventID() int64 {
	return e.executionInfo.NextEventId
}

// GetPreviousStartedEventID returns last started workflow task event ID
func (e *mutableStateBuilder) GetPreviousStartedEventID() int64 {
	return e.executionInfo.LastProcessedEvent
}

func (e *mutableStateBuilder) IsWorkflowExecutionRunning() bool {
	switch e.executionInfo.ExecutionState.State {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
		return true
	case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return true
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return false
	case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
		return false
	case enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED:
		return false
	default:
		panic(fmt.Sprintf("unknown workflow state: %v", e.executionInfo.ExecutionState.State))
	}
}

func (e *mutableStateBuilder) IsCancelRequested() (bool, string) {
	if e.executionInfo.CancelRequested {
		return e.executionInfo.CancelRequested, e.executionInfo.GetExecutionState().CreateRequestId
	}

	return false, ""
}

func (e *mutableStateBuilder) IsSignalRequested(
	requestID string,
) bool {

	if _, ok := e.pendingSignalRequestedIDs[requestID]; ok {
		return true
	}
	return false
}

func (e *mutableStateBuilder) AddSignalRequested(
	requestID string,
) {

	if e.pendingSignalRequestedIDs == nil {
		e.pendingSignalRequestedIDs = make(map[string]struct{})
	}
	if e.updateSignalRequestedIDs == nil {
		e.updateSignalRequestedIDs = make(map[string]struct{})
	}
	e.pendingSignalRequestedIDs[requestID] = struct{}{} // add requestID to set
	e.updateSignalRequestedIDs[requestID] = struct{}{}
}

func (e *mutableStateBuilder) DeleteSignalRequested(
	requestID string,
) {

	delete(e.pendingSignalRequestedIDs, requestID)
	e.deleteSignalRequestedID = requestID
}

func (e *mutableStateBuilder) addWorkflowExecutionStartedEventForContinueAsNew(
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	execution commonpb.WorkflowExecution,
	previousExecutionState mutableState,
	attributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
	firstRunID string,
) (*historypb.HistoryEvent, error) {

	previousExecutionInfo := previousExecutionState.GetExecutionInfo()
	taskQueue := previousExecutionInfo.TaskQueue
	if attributes.TaskQueue != nil {
		taskQueue = attributes.TaskQueue.GetName()
	}
	tq := &taskqueuepb.TaskQueue{
		Name: taskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	workflowType := previousExecutionInfo.WorkflowTypeName
	if attributes.WorkflowType != nil {
		workflowType = attributes.WorkflowType.GetName()
	}
	wType := &commonpb.WorkflowType{}
	wType.Name = workflowType

	var taskTimeout *time.Duration
	if timestamp.DurationValue(attributes.GetWorkflowTaskTimeout()) == 0 {
		taskTimeout = previousExecutionInfo.DefaultWorkflowTaskTimeout
	} else {
		taskTimeout = attributes.GetWorkflowTaskTimeout()
	}

	// Workflow runTimeout is already set to the correct value in
	// validateContinueAsNewWorkflowExecutionAttributes
	runTimeout := attributes.GetWorkflowRunTimeout()

	createRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.New(),
		Namespace:                e.namespaceEntry.GetInfo().Name,
		WorkflowId:               execution.WorkflowId,
		TaskQueue:                tq,
		WorkflowType:             wType,
		WorkflowExecutionTimeout: previousExecutionState.GetExecutionInfo().WorkflowExecutionTimeout,
		WorkflowRunTimeout:       runTimeout,
		WorkflowTaskTimeout:      taskTimeout,
		Input:                    attributes.Input,
		Header:                   attributes.Header,
		RetryPolicy:              attributes.RetryPolicy,
		CronSchedule:             attributes.CronSchedule,
		Memo:                     attributes.Memo,
		SearchAttributes:         attributes.SearchAttributes,
	}

	enums.SetDefaultContinueAsNewInitiator(&attributes.Initiator)

	req := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:              e.namespaceEntry.GetInfo().Id,
		StartRequest:             createRequest,
		ParentExecutionInfo:      parentExecutionInfo,
		LastCompletionResult:     attributes.LastCompletionResult,
		ContinuedFailure:         attributes.GetFailure(),
		ContinueAsNewInitiator:   attributes.Initiator,
		FirstWorkflowTaskBackoff: attributes.BackoffStartInterval,
	}
	if attributes.GetInitiator() == enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY {
		req.Attempt = previousExecutionState.GetExecutionInfo().Attempt + 1
	} else {
		req.Attempt = 1
	}
	workflowTimeoutTime := timestamp.TimeValue(previousExecutionState.GetExecutionInfo().WorkflowExpirationTime)
	if !workflowTimeoutTime.IsZero() {
		req.WorkflowExecutionExpirationTime = &workflowTimeoutTime
	}

	// History event only has namespace so namespaceID has to be passed in explicitly to update the mutable state
	var parentNamespaceID string
	if parentExecutionInfo != nil {
		parentNamespaceID = parentExecutionInfo.GetNamespaceId()
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(req, previousExecutionInfo, firstRunID, execution.GetRunId())
	if err := e.ReplicateWorkflowExecutionStartedEvent(
		parentNamespaceID,
		execution,
		createRequest.GetRequestId(),
		event,
	); err != nil {
		return nil, err
	}

	if err := e.SetHistoryTree(e.GetExecutionInfo().GetRunId()); err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowStartTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, err
	}
	if err := e.taskGenerator.generateRecordWorkflowStartedTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, err
	}

	if err := e.AddFirstWorkflowTaskScheduled(
		event,
	); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEvent(
	execution commonpb.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	request := startRequest.StartRequest
	eventID := e.GetNextEventID()
	if eventID != common.FirstEventID {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(eventID),
			tag.ErrorTypeInvalidHistoryAction)
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(startRequest, nil, execution.GetRunId(), execution.GetRunId())

	var parentNamespaceID string
	if startRequest.ParentExecutionInfo != nil {
		parentNamespaceID = startRequest.ParentExecutionInfo.GetNamespaceId()
	}
	if err := e.ReplicateWorkflowExecutionStartedEvent(
		parentNamespaceID,
		execution,
		request.GetRequestId(),
		event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowStartTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, err
	}
	if err := e.taskGenerator.generateRecordWorkflowStartedTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionStartedEvent(
	parentNamespaceID string,
	execution commonpb.WorkflowExecution,
	requestID string,
	startEvent *historypb.HistoryEvent,
) error {

	event := startEvent.GetWorkflowExecutionStartedEventAttributes()
	e.executionInfo.GetExecutionState().CreateRequestId = requestID
	e.executionInfo.NamespaceId = e.namespaceEntry.GetInfo().Id
	e.executionInfo.WorkflowId = execution.GetWorkflowId()
	e.executionInfo.ExecutionState.RunId = execution.GetRunId()
	e.executionInfo.FirstExecutionRunId = event.GetFirstExecutionRunId()
	e.executionInfo.TaskQueue = event.TaskQueue.GetName()
	e.executionInfo.WorkflowTypeName = event.WorkflowType.GetName()
	e.executionInfo.WorkflowRunTimeout = event.GetWorkflowRunTimeout()
	e.executionInfo.WorkflowExecutionTimeout = event.GetWorkflowExecutionTimeout()
	e.executionInfo.DefaultWorkflowTaskTimeout = event.GetWorkflowTaskTimeout()

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	); err != nil {
		return err
	}
	e.executionInfo.LastProcessedEvent = common.EmptyEventID
	e.executionInfo.LastFirstEventId = startEvent.GetEventId()

	e.executionInfo.WorkflowTaskVersion = common.EmptyVersion
	e.executionInfo.WorkflowTaskScheduleId = common.EmptyEventID
	e.executionInfo.WorkflowTaskStartedId = common.EmptyEventID
	e.executionInfo.WorkflowTaskRequestId = emptyUUID
	e.executionInfo.WorkflowTaskTimeout = timestamp.DurationFromSeconds(0)

	e.executionInfo.CronSchedule = event.GetCronSchedule()
	e.executionInfo.ParentNamespaceId = parentNamespaceID

	if event.ParentWorkflowExecution != nil {
		e.executionInfo.ParentWorkflowId = event.ParentWorkflowExecution.GetWorkflowId()
		e.executionInfo.ParentRunId = event.ParentWorkflowExecution.GetRunId()
	}

	if event.ParentInitiatedEventId != 0 {
		e.executionInfo.InitiatedId = event.GetParentInitiatedEventId()
	} else {
		e.executionInfo.InitiatedId = common.EmptyEventID
	}

	e.executionInfo.Attempt = event.GetAttempt()
	if !timestamp.TimeValue(event.GetWorkflowExecutionExpirationTime()).IsZero() {
		e.executionInfo.WorkflowExpirationTime = event.GetWorkflowExecutionExpirationTime()
	}
	if event.RetryPolicy != nil {
		e.executionInfo.HasRetryPolicy = true
		e.executionInfo.RetryBackoffCoefficient = event.RetryPolicy.GetBackoffCoefficient()
		e.executionInfo.RetryInitialInterval = event.RetryPolicy.GetInitialInterval()
		e.executionInfo.RetryMaximumAttempts = event.RetryPolicy.GetMaximumAttempts()
		e.executionInfo.RetryMaximumInterval = event.RetryPolicy.GetMaximumInterval()
		e.executionInfo.RetryNonRetryableErrorTypes = event.RetryPolicy.GetNonRetryableErrorTypes()
	}

	e.executionInfo.AutoResetPoints = rolloverAutoResetPointsWithExpiringTime(
		event.GetPrevAutoResetPoints(),
		event.GetContinuedExecutionRunId(),
		timestamp.TimeValue(startEvent.GetEventTime()),
		e.namespaceEntry.GetRetentionDays(e.executionInfo.WorkflowId),
	)

	if event.Memo != nil {
		e.executionInfo.Memo = event.Memo.GetFields()
	}
	if event.SearchAttributes != nil {
		e.executionInfo.SearchAttributes = event.SearchAttributes.GetIndexedFields()
	}

	e.writeEventToCache(startEvent)
	return nil
}

func (e *mutableStateBuilder) AddFirstWorkflowTaskScheduled(
	startEvent *historypb.HistoryEvent,
) error {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return err
	}
	return e.workflowTaskManager.AddFirstWorkflowTaskScheduled(startEvent)
}

func (e *mutableStateBuilder) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*workflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskScheduledEvent(bypassTaskGeneration)
}

// originalScheduledTimestamp is to record the first WorkflowTaskScheduledEvent during workflow task heartbeat.
func (e *mutableStateBuilder) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp *time.Time,
) (*workflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp)
}

func (e *mutableStateBuilder) ReplicateTransientWorkflowTaskScheduled() (*workflowTaskInfo, error) {
	return e.workflowTaskManager.ReplicateTransientWorkflowTaskScheduled()
}

func (e *mutableStateBuilder) ReplicateWorkflowTaskScheduledEvent(
	version int64,
	scheduleID int64,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
	scheduleTimestamp *time.Time,
	originalScheduledTimestamp *time.Time,
) (*workflowTaskInfo, error) {
	return e.workflowTaskManager.ReplicateWorkflowTaskScheduledEvent(version, scheduleID, taskQueue, startToCloseTimeoutSeconds, attempt, scheduleTimestamp, originalScheduledTimestamp)
}

func (e *mutableStateBuilder) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	request *workflowservice.PollWorkflowTaskQueueRequest,
) (*historypb.HistoryEvent, *workflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskStartedEvent(scheduleEventID, requestID, request)
}

func (e *mutableStateBuilder) ReplicateWorkflowTaskStartedEvent(
	workflowTask *workflowTaskInfo,
	version int64,
	scheduleID int64,
	startedID int64,
	requestID string,
	timestamp time.Time,
) (*workflowTaskInfo, error) {

	return e.workflowTaskManager.ReplicateWorkflowTaskStartedEvent(workflowTask, version, scheduleID, startedID, requestID, timestamp)
}

func (e *mutableStateBuilder) CreateTransientWorkflowTaskEvents(
	workflowTask *workflowTaskInfo,
	identity string,
) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	return e.workflowTaskManager.CreateTransientWorkflowTaskEvents(workflowTask, identity)
}

// add BinaryCheckSum for the first workflowTaskCompletedID for auto-reset
func (e *mutableStateBuilder) addBinaryCheckSumIfNotExists(
	event *historypb.HistoryEvent,
	maxResetPoints int,
) error {
	binChecksum := event.GetWorkflowTaskCompletedEventAttributes().GetBinaryChecksum()
	if len(binChecksum) == 0 {
		return nil
	}
	exeInfo := e.executionInfo
	var currResetPoints []*workflowpb.ResetPointInfo
	if exeInfo.AutoResetPoints != nil && exeInfo.AutoResetPoints.Points != nil {
		currResetPoints = e.executionInfo.AutoResetPoints.Points
	} else {
		currResetPoints = make([]*workflowpb.ResetPointInfo, 0, 1)
	}

	// List of all recent binary checksums associated with the workflow.
	var recentBinaryChecksums []string

	for _, rp := range currResetPoints {
		recentBinaryChecksums = append(recentBinaryChecksums, rp.GetBinaryChecksum())
		if rp.GetBinaryChecksum() == binChecksum {
			// this checksum already exists
			return nil
		}
	}

	if len(currResetPoints) == maxResetPoints {
		// If exceeding the max limit, do rotation by taking the oldest one out.
		currResetPoints = currResetPoints[1:]
		recentBinaryChecksums = recentBinaryChecksums[1:]
	}
	// Adding current version of the binary checksum.
	recentBinaryChecksums = append(recentBinaryChecksums, binChecksum)

	resettable := true
	err := e.CheckResettable()
	if err != nil {
		resettable = false
	}
	info := &workflowpb.ResetPointInfo{
		BinaryChecksum:               binChecksum,
		RunId:                        exeInfo.GetRunId(),
		FirstWorkflowTaskCompletedId: event.GetEventId(),
		CreateTime:                   timestamp.TimePtr(e.timeSource.Now()),
		Resettable:                   resettable,
	}
	currResetPoints = append(currResetPoints, info)
	exeInfo.AutoResetPoints = &workflowpb.ResetPoints{
		Points: currResetPoints,
	}
	bytes, err := payload.Encode(recentBinaryChecksums)
	if err != nil {
		return err
	}
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload)
	}
	exeInfo.SearchAttributes[definition.BinaryChecksums] = bytes
	if e.shard.GetConfig().AdvancedVisibilityWritingMode() != common.AdvancedVisibilityWritingModeOff {
		return e.taskGenerator.generateWorkflowSearchAttrTasks(timestamp.TimeValue(event.GetEventTime()))
	}
	return nil
}

// TODO: we will release the restriction when reset API allow those pending
func (e *mutableStateBuilder) CheckResettable() error {
	if len(e.GetPendingChildExecutionInfos()) > 0 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("it is not allowed resetting to a point that workflow has pending child workflow."))
	}
	if len(e.GetPendingRequestCancelExternalInfos()) > 0 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("it is not allowed resetting to a point that workflow has pending request cancel."))
	}
	if len(e.GetPendingSignalExternalInfos()) > 0 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("it is not allowed resetting to a point that workflow has pending signals to send."))
	}
	return nil
}

func (e *mutableStateBuilder) AddWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	maxResetPoints int,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskCompletedEvent(scheduleEventID, startedEventID, request, maxResetPoints)
}

func (e *mutableStateBuilder) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	return e.workflowTaskManager.ReplicateWorkflowTaskCompletedEvent(event)
}

func (e *mutableStateBuilder) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID)
}

func (e *mutableStateBuilder) ReplicateWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	return e.workflowTaskManager.ReplicateWorkflowTaskTimedOutEvent(timeoutType)
}

func (e *mutableStateBuilder) AddWorkflowTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleEventID)
}

func (e *mutableStateBuilder) AddWorkflowTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	binChecksum string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskFailedEvent(
		scheduleEventID,
		startedEventID,
		cause,
		failure,
		identity,
		binChecksum,
		baseRunID,
		newRunID,
		forkEventVersion,
	)
}

func (e *mutableStateBuilder) ReplicateWorkflowTaskFailedEvent() error {
	return e.workflowTaskManager.ReplicateWorkflowTaskFailedEvent()
}

func (e *mutableStateBuilder) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
) (*historypb.HistoryEvent, *persistenceblobs.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	_, ok := e.GetActivityByActivityID(attributes.GetActivityId())
	if ok {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction)
		return nil, nil, e.createCallerError(opTag)
	}

	event := e.hBuilder.AddActivityTaskScheduledEvent(workflowTaskCompletedEventID, attributes)

	// Write the event to cache only on active cluster for processing on activity started or retried
	e.eventsCache.putEvent(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionInfo.ExecutionState.RunId,
		event.GetEventId(),
		event,
	)

	ai, err := e.ReplicateActivityTaskScheduledEvent(workflowTaskCompletedEventID, event)
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateActivityTransferTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ai, err
}

func (e *mutableStateBuilder) ReplicateActivityTaskScheduledEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) (*persistenceblobs.ActivityInfo, error) {

	attributes := event.GetActivityTaskScheduledEventAttributes()
	targetNamespaceID := e.executionInfo.NamespaceId
	if attributes.GetNamespace() != "" {
		targetNamespaceEntry, err := e.shard.GetNamespaceCache().GetNamespace(attributes.GetNamespace())
		if err != nil {
			return nil, err
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	scheduleEventID := event.GetEventId()
	scheduleToCloseTimeout := attributes.GetScheduleToCloseTimeout()

	ai := &persistenceblobs.ActivityInfo{
		Version:                 event.GetVersion(),
		ScheduleId:              scheduleEventID,
		ScheduledEventBatchId:   firstEventID,
		ScheduledTime:           event.GetEventTime(),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              attributes.ActivityId,
		NamespaceId:             targetNamespaceID,
		ScheduleToStartTimeout:  attributes.GetScheduleToStartTimeout(),
		ScheduleToCloseTimeout:  scheduleToCloseTimeout,
		StartToCloseTimeout:     attributes.GetStartToCloseTimeout(),
		HeartbeatTimeout:        attributes.GetHeartbeatTimeout(),
		CancelRequested:         false,
		CancelRequestId:         common.EmptyEventID,
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		TaskQueue:               attributes.TaskQueue.GetName(),
		HasRetryPolicy:          attributes.RetryPolicy != nil,
		Attempt:                 1,
	}
	if ai.HasRetryPolicy {
		ai.RetryInitialInterval = attributes.RetryPolicy.GetInitialInterval()
		ai.RetryBackoffCoefficient = attributes.RetryPolicy.GetBackoffCoefficient()
		ai.RetryMaximumInterval = attributes.RetryPolicy.GetMaximumInterval()
		ai.RetryMaximumAttempts = attributes.RetryPolicy.GetMaximumAttempts()
		ai.RetryNonRetryableErrorTypes = attributes.RetryPolicy.NonRetryableErrorTypes
		ai.RetryExpirationTime = timestamp.TimePtr(timestamp.TimeValue(ai.ScheduledTime).Add(timestamp.DurationValue(scheduleToCloseTimeout)))
	}

	e.pendingActivityInfoIDs[scheduleEventID] = ai
	e.pendingActivityIDToEventID[ai.ActivityId] = scheduleEventID
	e.updateActivityInfos[ai] = struct{}{}

	return ai, nil
}

func (e *mutableStateBuilder) addTransientActivityStartedEvent(
	scheduleEventID int64,
) error {

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedId != common.TransientEventID {
		return nil
	}

	// activity task was started (as transient event), we need to add it now.
	event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, int32(ai.Attempt), ai.RequestId, ai.StartedIdentity,
		ai.RetryLastFailure)
	if !ai.StartedTime.IsZero() {
		// overwrite started event time to the one recorded in ActivityInfo
		event.EventTime = ai.StartedTime
	}
	return e.ReplicateActivityTaskStartedEvent(event)
}

func (e *mutableStateBuilder) AddActivityTaskStartedEvent(
	ai *persistenceblobs.ActivityInfo,
	scheduleEventID int64,
	requestID string,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if !ai.HasRetryPolicy {
		event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, int32(ai.Attempt), requestID, identity, ai.RetryLastFailure)
		if err := e.ReplicateActivityTaskStartedEvent(event); err != nil {
			return nil, err
		}
		return event, nil
	}

	// we might need to retry, so do not append started event just yet,
	// instead update mutable state and will record started event when activity task is closed
	ai.Version = e.GetCurrentVersion()
	ai.StartedId = common.TransientEventID
	ai.RequestId = requestID
	ai.StartedTime = timestamp.TimePtr(e.timeSource.Now())
	ai.LastHeartbeatUpdateTime = ai.StartedTime
	ai.StartedIdentity = identity
	if err := e.UpdateActivity(ai); err != nil {
		return nil, err
	}
	e.syncActivityTasks[ai.ScheduleId] = struct{}{}
	return nil, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskStartedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskStartedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()
	ai, ok := e.GetActivityInfo(scheduleID)
	if !ok {
		e.logError(
			fmt.Sprintf("unable to find activity event id: %v in mutable state", scheduleID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ai.Version = event.GetVersion()
	ai.StartedId = event.GetEventId()
	ai.RequestId = attributes.GetRequestId()
	ai.StartedTime = event.GetEventTime()
	ai.LastHeartbeatUpdateTime = ai.StartedTime
	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

func (e *mutableStateBuilder) AddActivityTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedId != startedEventID {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, e.createInternalServerError(opTag)
	}

	if err := e.addTransientActivityStartedEvent(scheduleEventID); err != nil {
		return nil, err
	}
	event := e.hBuilder.AddActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)
	if err := e.ReplicateActivityTaskCompletedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCompletedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedId != startedEventID {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, e.createInternalServerError(opTag)
	}

	if err := e.addTransientActivityStartedEvent(scheduleEventID); err != nil {
		return nil, err
	}
	event := e.hBuilder.AddActivityTaskFailedEvent(scheduleEventID, startedEventID, failure, retryState, identity)
	if err := e.ReplicateActivityTaskFailedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskFailedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	timeoutType := timeoutFailure.GetTimeoutFailureInfo().GetTimeoutType()

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedId != startedEventID || ((timeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE ||
		timeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT) && ai.StartedId == common.EmptyEventID) {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduleID(ai.ScheduleId),
			tag.WorkflowStartedID(ai.StartedId),
			tag.WorkflowTimeoutType(timeoutType))
		return nil, e.createInternalServerError(opTag)
	}

	timeoutFailure.Cause = ai.RetryLastFailure

	if err := e.addTransientActivityStartedEvent(scheduleEventID); err != nil {
		return nil, err
	}
	event := e.hBuilder.AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutFailure, retryState)
	if err := e.ReplicateActivityTaskTimedOutEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskTimedOutEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskTimedOutEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduleID int64,
	identity string,
) (*historypb.HistoryEvent, *persistenceblobs.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskCancelRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	ai, ok := e.GetActivityInfo(scheduleID)
	if !ok {
		// It is possible both started and completed events are buffered for this activity
		completedEvent := e.scanForBufferedActivityCompletion(scheduleID)
		if completedEvent == nil {
			e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
				tag.WorkflowEventID(e.GetNextEventID()),
				tag.ErrorTypeInvalidHistoryAction,
				tag.Bool(ok),
				tag.WorkflowScheduleID(scheduleID))

			return nil, nil, e.createCallerError(opTag)
		}
	}

	// Check for duplicate cancellation
	if ok && ai.CancelRequested {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduleID(scheduleID))

		return nil, nil, e.createCallerError(opTag)
	}

	// At this point we know this is a valid activity cancellation request
	actCancelReqEvent := e.hBuilder.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEventID, scheduleID)

	if err := e.ReplicateActivityTaskCancelRequestedEvent(actCancelReqEvent); err != nil {
		return nil, nil, err
	}

	return actCancelReqEvent, ai, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskCancelRequestedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCancelRequestedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()
	ai, ok := e.GetActivityInfo(scheduleID)
	if !ok {
		// This will only be called on active cluster if activity info is found in mutable state
		// Passive side logic should always have activity info in mutable state if this is called, as the only
		// scenario where active side logic could have this event without activity info in mutable state is when
		// activity start and complete events are buffered.
		return nil
	}

	ai.Version = event.GetVersion()

	// - We have the activity dispatched to worker.
	// - The activity might not be heartbeat'ing, but the activity can still call RecordActivityHeartBeat()
	//   to see cancellation while reporting progress of the activity.
	ai.CancelRequested = true

	ai.CancelRequestId = event.GetEventId()
	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

func (e *mutableStateBuilder) AddActivityTaskCanceledEvent(
	scheduleEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedId != startedEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID))
		return nil, e.createInternalServerError(opTag)
	}

	// Verify cancel request as well.
	if !ai.CancelRequested {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowActivityID(ai.ActivityId),
			tag.WorkflowStartedID(ai.StartedId))
		return nil, e.createInternalServerError(opTag)
	}

	if err := e.addTransientActivityStartedEvent(scheduleEventID); err != nil {
		return nil, err
	}
	event := e.hBuilder.AddActivityTaskCanceledEvent(scheduleEventID, startedEventID, latestCancelRequestedEventID,
		details, identity)
	if err := e.ReplicateActivityTaskCanceledEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCanceledEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.CompleteWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddCompletedWorkflowEvent(workflowTaskCompletedEventID, attributes)
	if err := e.ReplicateWorkflowExecutionCompletedEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCompletedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	attributes *commandpb.FailWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddFailWorkflowEvent(workflowTaskCompletedEventID, retryState, attributes)
	if err := e.ReplicateWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionFailedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddTimeoutWorkflowEvent(
	firstEventID int64,
	retryState enumspb.RetryState,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowTimeout
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddTimeoutWorkflowEvent(retryState)
	if err := e.ReplicateWorkflowExecutionTimedoutEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionTimedoutEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionCancelRequestedEvent(
	cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCancelRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if e.executionInfo.CancelRequested {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowState(e.executionInfo.ExecutionState.State),
			tag.Bool(e.executionInfo.CancelRequested),
			tag.Key(e.executionInfo.CancelRequestId),
		)
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddWorkflowExecutionCancelRequestedEvent(cause, request)
	if err := e.ReplicateWorkflowExecutionCancelRequestedEvent(event); err != nil {
		return nil, err
	}

	// Set the CancelRequestID on the active cluster.  This information is not part of the history event.
	e.executionInfo.CancelRequestId = request.CancelRequest.GetRequestId()
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCancelRequestedEvent(
	event *historypb.HistoryEvent,
) error {

	e.executionInfo.CancelRequested = true
	return nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.CancelWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, attributes)
	if err := e.ReplicateWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCanceledEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	cancelRequestID string,
	request *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, *persistenceblobs.RequestCancelInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, request)
	rci, err := e.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, cancelRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateRequestCancelExternalTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, rci, nil
}

func (e *mutableStateBuilder) ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	cancelRequestID string,
) (*persistenceblobs.RequestCancelInfo, error) {

	// TODO: Evaluate if we need cancelRequestID also part of history event
	initiatedEventID := event.GetEventId()
	rci := &persistenceblobs.RequestCancelInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchId: firstEventID,
		InitiatedId:           initiatedEventID,
		CancelRequestId:       cancelRequestID,
	}

	e.pendingRequestCancelInfoIDs[initiatedEventID] = rci
	e.updateRequestCancelInfos[rci] = struct{}{}

	return rci, nil
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionCancelRequested(
	initiatedID int64,
	namespace string,
	workflowID string,
	runID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddExternalWorkflowExecutionCancelRequested(initiatedID, namespace, workflowID, runID)
	if err := e.ReplicateExternalWorkflowExecutionCancelRequested(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateExternalWorkflowExecutionCancelRequested(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionCancelRequestedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingRequestCancel(initiatedID)
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	namespace string,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedID,
		namespace, workflowID, runID, cause)
	if err := e.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingRequestCancel(initiatedID)
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	signalRequestID string,
	request *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, *persistenceblobs.SignalInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, request)
	si, err := e.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, signalRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateSignalExternalTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, si, nil
}

func (e *mutableStateBuilder) ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	signalRequestID string,
) (*persistenceblobs.SignalInfo, error) {

	// TODO: Consider also writing signalRequestID to history event
	initiatedEventID := event.GetEventId()
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
	si := &persistenceblobs.SignalInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchId: firstEventID,
		InitiatedId:           initiatedEventID,
		RequestId:             signalRequestID,
		Name:                  attributes.GetSignalName(),
		Input:                 attributes.Input,
		Control:               attributes.Control,
	}

	e.pendingSignalInfoIDs[initiatedEventID] = si
	e.updateSignalInfos[si] = struct{}{}
	return si, nil
}

func (e *mutableStateBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionUpsertWorkflowSearchAttributes
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, request)
	e.ReplicateUpsertWorkflowSearchAttributesEvent(event)
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowSearchAttrTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateUpsertWorkflowSearchAttributesEvent(
	event *historypb.HistoryEvent,
) {

	upsertSearchAttr := event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields()
	currentSearchAttr := e.GetExecutionInfo().SearchAttributes

	e.executionInfo.SearchAttributes = mergeMapOfPayload(currentSearchAttr, upsertSearchAttr)
}

func mergeMapOfPayload(
	current map[string]*commonpb.Payload,
	upsert map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {

	if current == nil {
		current = make(map[string]*commonpb.Payload)
	}
	for k, v := range upsert {
		current[k] = v
	}
	return current
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionSignaled(
	initiatedID int64,
	namespace string,
	workflowID string,
	runID string,
	control string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := e.GetSignalInfo(initiatedID)
	if !ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddExternalWorkflowExecutionSignaled(initiatedID, namespace, workflowID, runID, control)
	if err := e.ReplicateExternalWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateExternalWorkflowExecutionSignaled(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionSignaledEventAttributes().GetInitiatedEventId()

	return e.DeletePendingSignal(initiatedID)
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	namespace string,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := e.GetSignalInfo(initiatedID)
	if !ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddSignalExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedID, namespace,
		workflowID, runID, control, cause)
	if err := e.ReplicateSignalExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateSignalExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetSignalExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingSignal(initiatedID)
}

func (e *mutableStateBuilder) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.StartTimerCommandAttributes,
) (*historypb.HistoryEvent, *persistenceblobs.TimerInfo, error) {

	opTag := tag.WorkflowActionTimerStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	timerID := request.GetTimerId()
	_, ok := e.GetUserTimerInfo(timerID)
	if ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowTimerID(timerID))
		return nil, nil, e.createCallerError(opTag)
	}

	event := e.hBuilder.AddTimerStartedEvent(workflowTaskCompletedEventID, request)
	ti, err := e.ReplicateTimerStartedEvent(event)
	if err != nil {
		return nil, nil, err
	}
	return event, ti, err
}

func (e *mutableStateBuilder) ReplicateTimerStartedEvent(
	event *historypb.HistoryEvent,
) (*persistenceblobs.TimerInfo, error) {

	attributes := event.GetTimerStartedEventAttributes()
	timerID := attributes.GetTimerId()

	startToFireTimeout := timestamp.DurationValue(attributes.GetStartToFireTimeout())
	// TODO: Time skew need to be taken in to account.
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(startToFireTimeout) // should use the event time, not now

	ti := &persistenceblobs.TimerInfo{
		Version:    event.GetVersion(),
		TimerId:    timerID,
		ExpiryTime: &expiryTime,
		StartedId:  event.GetEventId(),
		TaskStatus: timerTaskStatusNone,
	}

	e.pendingTimerInfoIDs[timerID] = ti
	e.pendingTimerEventIDToID[event.GetEventId()] = timerID
	e.updateTimerInfos[ti] = struct{}{}

	return ti, nil
}

func (e *mutableStateBuilder) AddTimerFiredEvent(
	timerID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionTimerFired
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	timerInfo, ok := e.GetUserTimerInfo(timerID)
	if !ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowTimerID(timerID))
		return nil, e.createInternalServerError(opTag)
	}

	// Timer is running.
	event := e.hBuilder.AddTimerFiredEvent(timerInfo.GetStartedId(), timerID)
	if err := e.ReplicateTimerFiredEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateTimerFiredEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetTimerFiredEventAttributes()
	timerID := attributes.GetTimerId()

	return e.DeleteUserTimer(timerID)
}

func (e *mutableStateBuilder) AddTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.CancelTimerCommandAttributes,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionTimerCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	var timerStartedID int64
	timerID := attributes.GetTimerId()
	ti, ok := e.GetUserTimerInfo(timerID)
	if !ok {
		// if timer is not running then check if it has fired in the mutable state.
		// If so clear the timer from the mutable state. We need to check both the
		// bufferedEvents and the history builder
		timerFiredEvent := e.checkAndClearTimerFiredEvent(timerID)
		if timerFiredEvent == nil {
			e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
				tag.WorkflowEventID(e.GetNextEventID()),
				tag.ErrorTypeInvalidHistoryAction,
				tag.WorkflowTimerID(timerID))
			return nil, e.createCallerError(opTag)
		}
		timerStartedID = timerFiredEvent.GetTimerFiredEventAttributes().GetStartedEventId()
	} else {
		timerStartedID = ti.GetStartedId()
	}

	// Timer is running.
	event := e.hBuilder.AddTimerCanceledEvent(timerStartedID, workflowTaskCompletedEventID, timerID, identity)
	if ok {
		if err := e.ReplicateTimerCanceledEvent(event); err != nil {
			return nil, err
		}
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateTimerCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetTimerCanceledEventAttributes()
	timerID := attributes.GetTimerId()

	return e.DeleteUserTimer(timerID)
}

func (e *mutableStateBuilder) AddRecordMarkerEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.RecordMarkerCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowRecordMarker
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	return e.hBuilder.AddMarkerRecordedEvent(workflowTaskCompletedEventID, attributes), nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	reason string,
	details *commonpb.Payloads,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowTerminated
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddWorkflowExecutionTerminatedEvent(reason, details, identity)
	if err := e.ReplicateWorkflowExecutionTerminatedEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionSignaled(
	signalName string,
	input *commonpb.Payloads,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowSignaled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddWorkflowExecutionSignaledEvent(signalName, input, identity)
	if err := e.ReplicateWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionSignaled(
	event *historypb.HistoryEvent,
) error {

	// Increment signal count in mutable state for this workflow execution
	e.executionInfo.SignalCount++
	return nil
}

func (e *mutableStateBuilder) AddContinueAsNewEvent(
	firstEventID int64,
	workflowTaskCompletedEventID int64,
	parentNamespace string,
	attributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, mutableState, error) {

	opTag := tag.WorkflowActionWorkflowContinueAsNew
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	var err error
	newRunID := uuid.New()
	newExecution := commonpb.WorkflowExecution{
		WorkflowId: e.executionInfo.WorkflowId,
		RunId:      newRunID,
	}

	// Extract ParentExecutionInfo from current run so it can be passed down to the next
	var parentInfo *workflowspb.ParentExecutionInfo
	if e.HasParentExecution() {
		parentInfo = &workflowspb.ParentExecutionInfo{
			NamespaceId: e.executionInfo.ParentNamespaceId,
			Namespace:   parentNamespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: e.executionInfo.ParentWorkflowId,
				RunId:      e.executionInfo.ParentRunId,
			},
			InitiatedId: e.executionInfo.InitiatedId,
		}
	}

	continueAsNewEvent := e.hBuilder.AddContinuedAsNewEvent(workflowTaskCompletedEventID, newRunID, attributes)
	firstRunID := e.executionInfo.FirstExecutionRunId
	// This is needed for backwards compatibility.  Workflow execution create with Temporal release v0.28.0 or earlier
	// does not have FirstExecutionRunID stored as part of mutable state.  If this is not set then load it from
	// workflow execution started event.
	if len(firstRunID) == 0 {
		currentStartEvent, err := e.GetStartEvent()
		if err != nil {
			return nil, nil, err
		}
		firstRunID = currentStartEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstExecutionRunId()
	}

	namespaceID := e.namespaceEntry.GetInfo().Id
	var newStateBuilder *mutableStateBuilder

	newStateBuilder = newMutableStateBuilderWithVersionHistories(
		e.shard,
		e.shard.GetEventsCache(),
		e.logger,
		e.namespaceEntry,
	)

	if _, err = newStateBuilder.addWorkflowExecutionStartedEventForContinueAsNew(
		parentInfo,
		newExecution,
		e,
		attributes,
		firstRunID,
	); err != nil {
		return nil, nil, serviceerror.NewInternal("Failed to add workflow execution started event.")
	}

	if err = e.ReplicateWorkflowExecutionContinuedAsNewEvent(
		firstEventID,
		namespaceID,
		continueAsNewEvent,
	); err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		timestamp.TimeValue(continueAsNewEvent.GetEventTime()),
	); err != nil {
		return nil, nil, err
	}

	return continueAsNewEvent, newStateBuilder, nil
}

func rolloverAutoResetPointsWithExpiringTime(
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	now time.Time,
	namespaceRetentionDays int32,
) *workflowpb.ResetPoints {

	if resetPoints == nil || resetPoints.Points == nil {
		return resetPoints
	}
	newPoints := make([]*workflowpb.ResetPointInfo, 0, len(resetPoints.Points))
	expireTime := now.Add(time.Duration(namespaceRetentionDays) * time.Hour * 24)
	for _, rp := range resetPoints.Points {
		if rp.GetRunId() == prevRunID {
			rp.ExpireTime = &expireTime
		}
		newPoints = append(newPoints, rp)
	}
	return &workflowpb.ResetPoints{
		Points: newPoints,
	}
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionContinuedAsNewEvent(
	firstEventID int64,
	namespaceID string,
	continueAsNewEvent *historypb.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(continueAsNewEvent)
	return nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	createRequestID string,
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, *persistenceblobs.ChildExecutionInfo, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, attributes)
	// Write the event to cache only on active cluster
	e.eventsCache.putEvent(e.executionInfo.NamespaceId, e.executionInfo.WorkflowId, e.executionInfo.ExecutionState.RunId,
		event.GetEventId(), event)

	ci, err := e.ReplicateStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, createRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateChildWorkflowTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ci, nil
}

func (e *mutableStateBuilder) ReplicateStartChildWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	createRequestID string,
) (*persistenceblobs.ChildExecutionInfo, error) {

	initiatedEventID := event.GetEventId()
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	ci := &persistenceblobs.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedId:           initiatedEventID,
		InitiatedEventBatchId: firstEventID,
		StartedId:             common.EmptyEventID,
		StartedWorkflowId:     attributes.GetWorkflowId(),
		CreateRequestId:       createRequestID,
		Namespace:             attributes.GetNamespace(),
		WorkflowTypeName:      attributes.GetWorkflowType().GetName(),
		ParentClosePolicy:     attributes.GetParentClosePolicy(),
	}

	e.pendingChildExecutionInfoIDs[initiatedEventID] = ci
	e.updateChildExecutionInfos[ci] = struct{}{}

	return ci, nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId != common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddChildWorkflowExecutionStartedEvent(namespace, execution, workflowType, initiatedID, header)
	if err := e.ReplicateChildWorkflowExecutionStartedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionStartedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	ci, _ := e.GetChildExecutionInfo(initiatedID)
	ci.StartedId = event.GetEventId()
	ci.StartedRunId = attributes.GetWorkflowExecution().GetRunId()
	e.updateChildExecutionInfos[ci] = struct{}{}

	return nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiationFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId != common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)
	if err := e.ReplicateStartChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateStartChildWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetStartChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionCompletedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionCompletedEvent(ci.Namespace, childExecution, workflowType, ci.InitiatedId,
		ci.StartedId, attributes)
	if err := e.ReplicateChildWorkflowExecutionCompletedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionCompletedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionFailedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(!ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionFailedEvent(ci.Namespace, childExecution, workflowType, ci.InitiatedId,
		ci.StartedId, attributes)
	if err := e.ReplicateChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionCanceledEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionCanceledEvent(ci.Namespace, childExecution, workflowType, ci.InitiatedId,
		ci.StartedId, attributes)
	if err := e.ReplicateChildWorkflowExecutionCanceledEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionTerminatedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowTerminated
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionTerminatedEvent(ci.Namespace, childExecution, workflowType, ci.InitiatedId,
		ci.StartedId, attributes)
	if err := e.ReplicateChildWorkflowExecutionTerminatedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionTerminatedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionTimedOutEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedId == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionTimedOutEvent(ci.Namespace, childExecution, workflowType, ci.InitiatedId,
		ci.StartedId, attributes)
	if err := e.ReplicateChildWorkflowExecutionTimedOutEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionTimedOutEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) RetryActivity(
	ai *persistenceblobs.ActivityInfo,
	failure *failurepb.Failure,
) (enumspb.RetryState, error) {

	opTag := tag.WorkflowActionActivityTaskRetry
	if err := e.checkMutability(opTag); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}

	if !ai.HasRetryPolicy {
		return enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, nil
	}

	if ai.CancelRequested {
		return enumspb.RETRY_STATE_CANCEL_REQUESTED, nil
	}

	now := e.timeSource.Now()

	backoffInterval, retryState := getBackoffInterval(
		now,
		timestamp.TimeValue(ai.RetryExpirationTime),
		ai.Attempt,
		ai.RetryMaximumAttempts,
		ai.RetryInitialInterval,
		ai.RetryMaximumInterval,
		ai.RetryBackoffCoefficient,
		failure,
		ai.RetryNonRetryableErrorTypes,
	)
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return retryState, nil
	}

	// a retry is needed, update activity info for next retry
	ai.Version = e.GetCurrentVersion()
	ai.Attempt++
	ai.ScheduledTime = timestamp.TimePtr(now.Add(backoffInterval)) // update to next schedule time
	ai.StartedId = common.EmptyEventID
	ai.RequestId = ""
	ai.StartedTime = timestamp.TimePtr(time.Time{})
	ai.TimerTaskStatus = timerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = failure

	if err := e.taskGenerator.generateActivityRetryTasks(
		ai.ScheduleId,
	); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}

	e.updateActivityInfos[ai] = struct{}{}
	e.syncActivityTasks[ai.ScheduleId] = struct{}{}
	return enumspb.RETRY_STATE_IN_PROGRESS, nil
}

// TODO mutable state should generate corresponding transfer / timer tasks according to
//  updates accumulated, while currently all transfer / timer tasks are managed manually

// TODO convert AddTransferTasks to prepareTransferTasks
func (e *mutableStateBuilder) AddTransferTasks(
	transferTasks ...persistence.Task,
) {

	e.insertTransferTasks = append(e.insertTransferTasks, transferTasks...)
}

// TODO convert AddTransferTasks to prepareTimerTasks
func (e *mutableStateBuilder) AddTimerTasks(
	timerTasks ...persistence.Task,
) {

	e.insertTimerTasks = append(e.insertTimerTasks, timerTasks...)
}

func (e *mutableStateBuilder) SetUpdateCondition(
	nextEventIDInDB int64,
) {

	e.nextEventIDInDB = nextEventIDInDB
}

func (e *mutableStateBuilder) GetUpdateCondition() int64 {
	return e.nextEventIDInDB
}

func (e *mutableStateBuilder) GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus) {

	executionInfo := e.executionInfo
	return executionInfo.ExecutionState.State, executionInfo.ExecutionState.Status
}

func (e *mutableStateBuilder) UpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {

	return e.executionInfo.UpdateWorkflowStateStatus(state, status)
}

func (e *mutableStateBuilder) StartTransaction(
	namespaceEntry *cache.NamespaceCacheEntry,
) (bool, error) {

	e.namespaceEntry = namespaceEntry
	if err := e.UpdateCurrentVersion(namespaceEntry.GetFailoverVersion(), false); err != nil {
		return false, err
	}

	flushBeforeReady, err := e.startTransactionHandleWorkflowTaskFailover(false)
	if err != nil {
		return false, err
	}

	return flushBeforeReady, nil
}

func (e *mutableStateBuilder) StartTransactionSkipWorkflowTaskFail(
	namespaceEntry *cache.NamespaceCacheEntry,
) error {

	e.namespaceEntry = namespaceEntry
	if err := e.UpdateCurrentVersion(namespaceEntry.GetFailoverVersion(), false); err != nil {
		return err
	}

	_, err := e.startTransactionHandleWorkflowTaskFailover(true)
	return err
}

func (e *mutableStateBuilder) CloseTransactionAsMutation(
	now time.Time,
	transactionPolicy transactionPolicy,
) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error) {

	if err := e.prepareCloseTransaction(
		now,
		transactionPolicy,
	); err != nil {
		return nil, nil, err
	}

	workflowEventsSeq, err := e.prepareEventsAndReplicationTasks(transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		firstEvent := lastEvents[0]
		lastEvent := lastEvents[len(lastEvents)-1]
		e.updateWithLastFirstEvent(firstEvent)
		if err := e.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, err
		}
	}

	setTaskInfo(e.GetCurrentVersion(), now, e.insertTransferTasks, e.insertTimerTasks)

	// update last update time
	e.executionInfo.LastUpdatedTime = &now

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside workflowExecutionContext.resetWorkflowExecution
	// currently, the updates done inside workflowExecutionContext.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := e.generateChecksum()

	workflowMutation := &persistence.WorkflowMutation{
		ExecutionInfo:    e.executionInfo,
		VersionHistories: e.versionHistories,

		UpsertActivityInfos:       convertUpdateActivityInfos(e.updateActivityInfos),
		DeleteActivityInfos:       convertDeleteActivityInfos(e.deleteActivityInfos),
		UpsertTimerInfos:          convertUpdateTimerInfos(e.updateTimerInfos),
		DeleteTimerInfos:          convertDeleteTimerInfos(e.deleteTimerInfos),
		UpsertChildExecutionInfos: convertUpdateChildExecutionInfos(e.updateChildExecutionInfos),
		DeleteChildExecutionInfo:  e.deleteChildExecutionInfo,
		UpsertRequestCancelInfos:  convertUpdateRequestCancelInfos(e.updateRequestCancelInfos),
		DeleteRequestCancelInfo:   e.deleteRequestCancelInfo,
		UpsertSignalInfos:         convertUpdateSignalInfos(e.updateSignalInfos),
		DeleteSignalInfo:          e.deleteSignalInfo,
		UpsertSignalRequestedIDs:  convertSignalRequestedIDs(e.updateSignalRequestedIDs),
		DeleteSignalRequestedID:   e.deleteSignalRequestedID,
		NewBufferedEvents:         e.updateBufferedEvents,
		ClearBufferedEvents:       e.clearBufferedEvents,

		TransferTasks:    e.insertTransferTasks,
		ReplicationTasks: e.insertReplicationTasks,
		TimerTasks:       e.insertTimerTasks,

		Condition: e.nextEventIDInDB,
		Checksum:  checksum,
	}

	e.checksum = checksum
	if err := e.cleanupTransaction(transactionPolicy); err != nil {
		return nil, nil, err
	}
	return workflowMutation, workflowEventsSeq, nil
}

func (e *mutableStateBuilder) CloseTransactionAsSnapshot(
	now time.Time,
	transactionPolicy transactionPolicy,
) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {

	if err := e.prepareCloseTransaction(
		now,
		transactionPolicy,
	); err != nil {
		return nil, nil, err
	}

	workflowEventsSeq, err := e.prepareEventsAndReplicationTasks(transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(workflowEventsSeq) > 1 {
		return nil, nil, serviceerror.NewInternal("cannot generate workflow snapshot with transient events")
	}
	if len(e.bufferedEvents) > 0 {
		// TODO do we need the functionality to generate snapshot with buffered events?
		return nil, nil, serviceerror.NewInternal("cannot generate workflow snapshot with buffered events")
	}

	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		firstEvent := lastEvents[0]
		lastEvent := lastEvents[len(lastEvents)-1]
		e.updateWithLastFirstEvent(firstEvent)
		if err := e.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, err
		}
	}

	setTaskInfo(e.GetCurrentVersion(), now, e.insertTransferTasks, e.insertTimerTasks)

	// update last update time
	e.executionInfo.LastUpdatedTime = &now

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside workflowExecutionContext.resetWorkflowExecution
	// currently, the updates done inside workflowExecutionContext.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := e.generateChecksum()

	workflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    e.executionInfo,
		VersionHistories: e.versionHistories,

		ActivityInfos:       convertPendingActivityInfos(e.pendingActivityInfoIDs),
		TimerInfos:          convertPendingTimerInfos(e.pendingTimerInfoIDs),
		ChildExecutionInfos: convertPendingChildExecutionInfos(e.pendingChildExecutionInfoIDs),
		RequestCancelInfos:  convertPendingRequestCancelInfos(e.pendingRequestCancelInfoIDs),
		SignalInfos:         convertPendingSignalInfos(e.pendingSignalInfoIDs),
		SignalRequestedIDs:  convertSignalRequestedIDs(e.pendingSignalRequestedIDs),

		TransferTasks:    e.insertTransferTasks,
		ReplicationTasks: e.insertReplicationTasks,
		TimerTasks:       e.insertTimerTasks,

		Condition: e.nextEventIDInDB,
		Checksum:  checksum,
	}

	e.checksum = checksum
	if err := e.cleanupTransaction(transactionPolicy); err != nil {
		return nil, nil, err
	}
	return workflowSnapshot, workflowEventsSeq, nil
}

func (e *mutableStateBuilder) IsResourceDuplicated(
	resourceDedupKey definition.DeduplicationID,
) bool {
	id := definition.GenerateDeduplicationKey(resourceDedupKey)
	_, duplicated := e.appliedEvents[id]
	return duplicated
}

func (e *mutableStateBuilder) UpdateDuplicatedResource(
	resourceDedupKey definition.DeduplicationID,
) {
	id := definition.GenerateDeduplicationKey(resourceDedupKey)
	e.appliedEvents[id] = struct{}{}
}

func (e *mutableStateBuilder) prepareCloseTransaction(
	now time.Time,
	transactionPolicy transactionPolicy,
) error {

	if err := e.closeTransactionWithPolicyCheck(
		transactionPolicy,
	); err != nil {
		return err
	}

	if err := e.closeTransactionHandleBufferedEventsLimit(
		transactionPolicy,
	); err != nil {
		return err
	}

	if err := e.closeTransactionHandleWorkflowReset(
		now,
		transactionPolicy,
	); err != nil {
		return err
	}

	// flushing buffered events should happen at very last
	if transactionPolicy == transactionPolicyActive {
		if err := e.FlushBufferedEvents(); err != nil {
			return err
		}
	}

	// TODO merge active & passive task generation
	// NOTE: this function must be the last call
	//  since we only generate at most one activity & user timer,
	//  regardless of how many activity & user timer created
	//  so the calculation must be at the very end
	return e.closeTransactionHandleActivityUserTimerTasks(
		now,
		transactionPolicy,
	)
}

func (e *mutableStateBuilder) cleanupTransaction(
	transactionPolicy transactionPolicy,
) error {

	// Clear all updates to prepare for the next session
	e.hBuilder = newHistoryBuilder(e, e.logger)

	e.updateActivityInfos = make(map[*persistenceblobs.ActivityInfo]struct{})
	e.deleteActivityInfos = make(map[int64]struct{})
	e.syncActivityTasks = make(map[int64]struct{})

	e.updateTimerInfos = make(map[*persistenceblobs.TimerInfo]struct{})
	e.deleteTimerInfos = make(map[string]struct{})

	e.updateChildExecutionInfos = make(map[*persistenceblobs.ChildExecutionInfo]struct{})
	e.deleteChildExecutionInfo = nil

	e.updateRequestCancelInfos = make(map[*persistenceblobs.RequestCancelInfo]struct{})
	e.deleteRequestCancelInfo = nil

	e.updateSignalInfos = make(map[*persistenceblobs.SignalInfo]struct{})
	e.deleteSignalInfo = nil

	e.updateSignalRequestedIDs = make(map[string]struct{})
	e.deleteSignalRequestedID = ""

	e.clearBufferedEvents = false
	if e.updateBufferedEvents != nil {
		e.bufferedEvents = append(e.bufferedEvents, e.updateBufferedEvents...)
		e.updateBufferedEvents = nil
	}

	e.hasBufferedEventsInDB = len(e.bufferedEvents) > 0
	e.stateInDB = e.executionInfo.ExecutionState.State
	e.nextEventIDInDB = e.GetNextEventID()

	e.insertTransferTasks = nil
	e.insertReplicationTasks = nil
	e.insertTimerTasks = nil

	return nil
}

func (e *mutableStateBuilder) prepareEventsAndReplicationTasks(
	transactionPolicy transactionPolicy,
) ([]*persistence.WorkflowEvents, error) {

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	var workflowEventsSeq []*persistence.WorkflowEvents
	if len(e.hBuilder.transientHistory) != 0 {
		workflowEventsSeq = append(workflowEventsSeq, &persistence.WorkflowEvents{
			NamespaceID: e.executionInfo.NamespaceId,
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionInfo.ExecutionState.RunId,
			BranchToken: currentBranchToken,
			Events:      e.hBuilder.transientHistory,
		})
	}
	if len(e.hBuilder.history) != 0 {
		workflowEventsSeq = append(workflowEventsSeq, &persistence.WorkflowEvents{
			NamespaceID: e.executionInfo.NamespaceId,
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionInfo.ExecutionState.RunId,
			BranchToken: currentBranchToken,
			Events:      e.hBuilder.history,
		})
	}

	if err := e.validateNoEventsAfterWorkflowFinish(
		transactionPolicy,
		e.hBuilder.history,
	); err != nil {
		return nil, err
	}

	for _, workflowEvents := range workflowEventsSeq {
		replicationTasks, err := e.eventsToReplicationTask(transactionPolicy, workflowEvents.Events)
		if err != nil {
			return nil, err
		}
		e.insertReplicationTasks = append(
			e.insertReplicationTasks,
			replicationTasks...,
		)
	}

	e.insertReplicationTasks = append(
		e.insertReplicationTasks,
		e.syncActivityToReplicationTask(transactionPolicy)...,
	)

	if transactionPolicy == transactionPolicyPassive && len(e.insertReplicationTasks) > 0 {
		return nil, serviceerror.NewInternal("should not generate replication task when close transaction as passive")
	}

	return workflowEventsSeq, nil
}

func (e *mutableStateBuilder) eventsToReplicationTask(
	transactionPolicy transactionPolicy,
	events []*historypb.HistoryEvent,
) ([]persistence.Task, error) {

	if transactionPolicy == transactionPolicyPassive ||
		!e.canReplicateEvents() ||
		len(events) == 0 {
		return emptyTasks, nil
	}

	firstEvent := events[0]
	lastEvent := events[len(events)-1]
	version := firstEvent.GetVersion()

	sourceCluster := e.clusterMetadata.ClusterNameForFailoverVersion(version)
	currentCluster := e.clusterMetadata.GetCurrentClusterName()

	if currentCluster != sourceCluster {
		return nil, serviceerror.NewInternal("mutableStateBuilder encounter contradicting version & transaction policy")
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	replicationTask := &persistence.HistoryReplicationTask{
		FirstEventID:      firstEvent.GetEventId(),
		NextEventID:       lastEvent.GetEventId() + 1,
		Version:           firstEvent.GetVersion(),
		BranchToken:       currentBranchToken,
		NewRunBranchToken: nil,
	}

	if e.GetVersionHistories() == nil {
		return nil, serviceerror.NewInternal("should not generate replication task when missing replication state & version history")
	}

	return []persistence.Task{replicationTask}, nil
}

func (e *mutableStateBuilder) syncActivityToReplicationTask(
	transactionPolicy transactionPolicy,
) []persistence.Task {

	if transactionPolicy == transactionPolicyPassive ||
		!e.canReplicateEvents() {
		return emptyTasks
	}

	return convertSyncActivityInfos(
		e.pendingActivityInfoIDs,
		e.syncActivityTasks,
	)
}

func (e *mutableStateBuilder) updateWithLastWriteEvent(
	lastEvent *historypb.HistoryEvent,
	transactionPolicy transactionPolicy,
) error {

	if transactionPolicy == transactionPolicyPassive {
		// already handled in state builder
		return nil
	}

	e.GetExecutionInfo().LastEventTaskId = lastEvent.GetTaskId()

	if e.versionHistories != nil {
		currentVersionHistory, err := e.versionHistories.GetCurrentVersionHistory()
		if err != nil {
			return err
		}
		if err := currentVersionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
			lastEvent.GetEventId(), lastEvent.GetVersion(),
		)); err != nil {
			return err
		}
	}
	return nil
}

func (e *mutableStateBuilder) updateWithLastFirstEvent(
	lastFirstEvent *historypb.HistoryEvent,
) {
	e.GetExecutionInfo().SetLastFirstEventID(lastFirstEvent.GetEventId())
}

func (e *mutableStateBuilder) canReplicateEvents() bool {
	return e.namespaceEntry.GetReplicationPolicy() == cache.ReplicationPolicyMultiCluster
}

// validateNoEventsAfterWorkflowFinish perform check on history event batch
// NOTE: do not apply this check on every batch, since transient
// workflow task && workflow finish will be broken (the first batch)
func (e *mutableStateBuilder) validateNoEventsAfterWorkflowFinish(
	transactionPolicy transactionPolicy,
	events []*historypb.HistoryEvent,
) error {

	if transactionPolicy == transactionPolicyPassive ||
		len(events) == 0 {
		return nil
	}

	// only do check if workflow is finished
	if e.GetExecutionInfo().GetExecutionState().State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil
	}

	// workflow close
	// this will perform check on the last event of last batch
	// NOTE: do not apply this check on every batch, since transient
	// workflow task && workflow finish will be broken (the first batch)
	lastEvent := events[len(events)-1]
	switch lastEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return nil

	default:
		executionInfo := e.GetExecutionInfo()
		e.logError(
			"encounter case where events appears after workflow finish.",
			tag.WorkflowNamespaceID(executionInfo.NamespaceId),
			tag.WorkflowID(executionInfo.WorkflowId),
			tag.WorkflowRunID(executionInfo.ExecutionState.RunId),
		)
		return ErrEventsAterWorkflowFinish
	}
}

func (e *mutableStateBuilder) startTransactionHandleWorkflowTaskFailover(
	skipWorkflowTaskFailed bool,
) (bool, error) {

	if !e.IsWorkflowExecutionRunning() ||
		!e.canReplicateEvents() {
		return false, nil
	}

	// NOTE:
	// the main idea here is to guarantee that once there is a workflow task started
	// all events ending in the buffer should have the same version

	// Handling mutable state turn from standby to active, while having a workflow task on the fly
	workflowTask, ok := e.GetInFlightWorkflowTask()
	if !ok || workflowTask.Version >= e.GetCurrentVersion() {
		// no pending workflow tasks, no buffered events
		// or workflow task has higher / equal version
		return false, nil
	}

	currentVersion := e.GetCurrentVersion()
	lastWriteVersion, err := e.GetLastWriteVersion()
	if err != nil {
		return false, err
	}
	if lastWriteVersion != workflowTask.Version {
		return false, serviceerror.NewInternal(fmt.Sprintf("mutableStateBuilder encounter mismatch version, workflow task: %v, last write version %v", workflowTask.Version, lastWriteVersion))
	}

	lastWriteSourceCluster := e.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentVersionCluster := e.clusterMetadata.ClusterNameForFailoverVersion(currentVersion)
	currentCluster := e.clusterMetadata.GetCurrentClusterName()

	// there are 4 cases for version changes (based on version from namespace cache)
	// NOTE: namespace cache version change may occur after seeing events with higher version
	//  meaning that the flush buffer logic in NDC branch manager should be kept.
	//
	// 1. active -> passive => fail workflow task & flush buffer using last write version
	// 2. active -> active => fail workflow task & flush buffer using last write version
	// 3. passive -> active => fail workflow task using current version, no buffered events
	// 4. passive -> passive => no buffered events, since always passive, nothing to be done

	// handle case 4
	if lastWriteSourceCluster != currentCluster && currentVersionCluster != currentCluster {
		// do a sanity check on buffered events
		if e.HasBufferedEvents() {
			return false, serviceerror.NewInternal("mutableStateBuilder encounter previous passive workflow with buffered events")
		}
		return false, nil
	}

	// handle case 1 & 2
	var flushBufferVersion = lastWriteVersion

	// handle case 3
	if lastWriteSourceCluster != currentCluster && currentVersionCluster == currentCluster {
		// do a sanity check on buffered events
		if e.HasBufferedEvents() {
			return false, serviceerror.NewInternal("mutableStateBuilder encounter previous passive workflow with buffered events")
		}
		flushBufferVersion = currentVersion
	}

	// this workflow was previous active (whether it has buffered events or not),
	// the in flight workflow task must be failed to guarantee all events within same
	// event batch shard the same version
	if err := e.UpdateCurrentVersion(flushBufferVersion, true); err != nil {
		return false, err
	}

	if skipWorkflowTaskFailed {
		return false, nil
	}

	// we have a workflow task with buffered events on the fly with a lower version, fail it
	if err := failWorkflowTask(
		e,
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
	); err != nil {
		return false, err
	}

	err = scheduleWorkflowTask(e)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *mutableStateBuilder) closeTransactionWithPolicyCheck(
	transactionPolicy transactionPolicy,
) error {

	if transactionPolicy == transactionPolicyPassive ||
		!e.canReplicateEvents() {
		return nil
	}

	activeCluster := e.clusterMetadata.ClusterNameForFailoverVersion(e.GetCurrentVersion())
	currentCluster := e.clusterMetadata.GetCurrentClusterName()

	if activeCluster != currentCluster {
		namespaceID := e.GetExecutionInfo().NamespaceId
		return serviceerror.NewNamespaceNotActive(namespaceID, currentCluster, activeCluster)
	}
	return nil
}

func (e *mutableStateBuilder) closeTransactionHandleBufferedEventsLimit(
	transactionPolicy transactionPolicy,
) error {

	if transactionPolicy == transactionPolicyPassive ||
		!e.IsWorkflowExecutionRunning() {
		return nil
	}

	if len(e.bufferedEvents) < e.config.MaximumBufferedEventsBatch() {
		return nil
	}

	// Handling buffered events size issue
	if workflowTask, ok := e.GetInFlightWorkflowTask(); ok {
		// we have a workflow task on the fly with a lower version, fail it
		if err := failWorkflowTask(
			e,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		); err != nil {
			return err
		}

		err := scheduleWorkflowTask(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *mutableStateBuilder) closeTransactionHandleWorkflowReset(
	now time.Time,
	transactionPolicy transactionPolicy,
) error {

	if transactionPolicy == transactionPolicyPassive ||
		!e.IsWorkflowExecutionRunning() {
		return nil
	}

	// compare with bad client binary checksum and schedule a reset task

	// only schedule reset task if current doesn't have childWFs.
	// TODO: This will be removed once our reset allows childWFs
	if len(e.GetPendingChildExecutionInfos()) != 0 {
		return nil
	}

	executionInfo := e.GetExecutionInfo()
	namespaceEntry, err := e.shard.GetNamespaceCache().GetNamespaceByID(executionInfo.NamespaceId)
	if err != nil {
		return err
	}
	if _, pt := FindAutoResetPoint(
		e.timeSource,
		namespaceEntry.GetConfig().BadBinaries,
		e.GetExecutionInfo().AutoResetPoints,
	); pt != nil {
		if err := e.taskGenerator.generateWorkflowResetTasks(
			e.unixNanoToTime(now.UnixNano()),
		); err != nil {
			return err
		}
		e.logInfo("Auto-Reset task is scheduled",
			tag.WorkflowNamespace(namespaceEntry.GetInfo().Name),
			tag.WorkflowID(executionInfo.WorkflowId),
			tag.WorkflowRunID(executionInfo.ExecutionState.RunId),
			tag.WorkflowResetBaseRunID(pt.GetRunId()),
			tag.WorkflowEventID(pt.GetFirstWorkflowTaskCompletedId()),
			tag.WorkflowBinaryChecksum(pt.GetBinaryChecksum()),
		)
	}
	return nil
}

func (e *mutableStateBuilder) closeTransactionHandleActivityUserTimerTasks(
	now time.Time,
	transactionPolicy transactionPolicy,
) error {

	if transactionPolicy == transactionPolicyPassive ||
		!e.IsWorkflowExecutionRunning() {
		return nil
	}

	if err := e.taskGenerator.generateActivityTimerTasks(
		e.unixNanoToTime(now.UnixNano()),
	); err != nil {
		return err
	}

	return e.taskGenerator.generateUserTimerTasks(
		e.unixNanoToTime(now.UnixNano()),
	)
}

func (e *mutableStateBuilder) checkMutability(
	actionTag tag.Tag,
) error {

	if !e.IsWorkflowExecutionRunning() {
		e.logWarn(
			mutableStateInvalidHistoryActionMsg,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowState(e.executionInfo.ExecutionState.State),
			actionTag,
		)
		return ErrWorkflowFinished
	}
	return nil
}

func (e *mutableStateBuilder) generateChecksum() checksum.Checksum {
	if !e.shouldGenerateChecksum() {
		return checksum.Checksum{}
	}
	csum, err := generateMutableStateChecksum(e)
	if err != nil {
		e.logWarn("error generating mutableState checksum", tag.Error(err))
		return checksum.Checksum{}
	}
	return csum
}

func (e *mutableStateBuilder) shouldGenerateChecksum() bool {
	if e.namespaceEntry == nil {
		return false
	}
	return rand.Intn(100) < e.config.MutableStateChecksumGenProbability(e.namespaceEntry.GetInfo().Name)
}

func (e *mutableStateBuilder) shouldVerifyChecksum() bool {
	if e.namespaceEntry == nil {
		return false
	}
	return rand.Intn(100) < e.config.MutableStateChecksumVerifyProbability(e.namespaceEntry.GetInfo().Name)
}

func (e *mutableStateBuilder) shouldInvalidateCheckum() bool {
	invalidateBeforeEpochSecs := int64(e.config.MutableStateChecksumInvalidateBefore())
	if invalidateBeforeEpochSecs > 0 {
		invalidateBefore := time.Unix(invalidateBeforeEpochSecs, 0).UTC()
		return e.executionInfo.LastUpdatedTime.Before(invalidateBefore)
	}
	return false
}

func (e *mutableStateBuilder) createInternalServerError(
	actionTag tag.Tag,
) error {

	return serviceerror.NewInternal(actionTag.Field().String + " operation failed")
}

func (e *mutableStateBuilder) createCallerError(
	actionTag tag.Tag,
) error {

	return serviceerror.NewInvalidArgument(fmt.Sprintf(mutableStateInvalidHistoryActionMsgTemplate, actionTag.Field().String))
}

func (_ *mutableStateBuilder) unixNanoToTime(
	timestampNanos int64,
) time.Time {

	return time.Unix(0, timestampNanos).UTC()
}

func (e *mutableStateBuilder) logInfo(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(e.executionInfo.ExecutionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(e.executionInfo.NamespaceId))
	e.logger.Info(msg, tags...)
}

func (e *mutableStateBuilder) logWarn(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(e.executionInfo.ExecutionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(e.executionInfo.NamespaceId))
	e.logger.Warn(msg, tags...)
}

func (e *mutableStateBuilder) logError(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(e.executionInfo.ExecutionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(e.executionInfo.NamespaceId))
	e.logger.Error(msg, tags...)
}

func (e *mutableStateBuilder) logDataInconsistency() {
	namespaceID := e.executionInfo.NamespaceId
	workflowID := e.executionInfo.WorkflowId
	runID := e.executionInfo.ExecutionState.RunId

	e.logger.Error("encounter cassandra data inconsistency",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
	)
}
