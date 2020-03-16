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
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/checksum"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
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
		pendingActivityInfoIDs     map[int64]*persistence.ActivityInfo    // Schedule Event ID -> Activity Info.
		pendingActivityIDToEventID map[string]int64                       // Activity ID -> Schedule Event ID of the activity.
		updateActivityInfos        map[*persistence.ActivityInfo]struct{} // Modified activities from last update.
		deleteActivityInfos        map[int64]struct{}                     // Deleted activities from last update.
		syncActivityTasks          map[int64]struct{}                     // Activity to be sync to remote

		pendingTimerInfoIDs     map[string]*persistenceblobs.TimerInfo   // User Timer ID -> Timer Info.
		pendingTimerEventIDToID map[int64]string                         // User Timer Start Event ID -> User Timer ID.
		updateTimerInfos        map[*persistenceblobs.TimerInfo]struct{} // Modified timers from last update.
		deleteTimerInfos        map[string]struct{}                      // Deleted timers from last update.

		pendingChildExecutionInfoIDs map[int64]*persistence.ChildExecutionInfo    // Initiated Event ID -> Child Execution Info
		updateChildExecutionInfos    map[*persistence.ChildExecutionInfo]struct{} // Modified ChildExecution Infos since last update
		deleteChildExecutionInfo     *int64                                       // Deleted ChildExecution Info since last update

		pendingRequestCancelInfoIDs map[int64]*persistenceblobs.RequestCancelInfo    // Initiated Event ID -> RequestCancelInfo
		updateRequestCancelInfos    map[*persistenceblobs.RequestCancelInfo]struct{} // Modified RequestCancel Infos since last update, for persistence update
		deleteRequestCancelInfo     *int64                                           // Deleted RequestCancel Info since last update, for persistence update

		pendingSignalInfoIDs map[int64]*persistenceblobs.SignalInfo    // Initiated Event ID -> SignalInfo
		updateSignalInfos    map[*persistenceblobs.SignalInfo]struct{} // Modified SignalInfo since last update
		deleteSignalInfo     *int64                                    // Deleted SignalInfo since last update

		pendingSignalRequestedIDs map[string]struct{} // Set of signaled requestIds
		updateSignalRequestedIDs  map[string]struct{} // Set of signaled requestIds since last update
		deleteSignalRequestedID   string              // Deleted signaled requestId

		bufferedEvents       []*commonproto.HistoryEvent // buffered history events that are already persisted
		updateBufferedEvents []*commonproto.HistoryEvent // buffered history events that needs to be persisted
		clearBufferedEvents  bool                        // delete buffered events from persistence

		executionInfo    *persistence.WorkflowExecutionInfo // Workflow mutable state info.
		versionHistories *persistence.VersionHistories
		replicationState *persistence.ReplicationState
		hBuilder         *historyBuilder

		// in memory only attributes
		// indicate the current version
		currentVersion int64
		// indicates whether there are buffered events in persistence
		hasBufferedEventsInDB bool
		// indicates the workflow state in DB, can be used to calculate
		// whether this workflow is pointed by current workflow record
		stateInDB int
		// indicates the next event ID in DB, for conditional update
		nextEventIDInDB int64
		// domain entry contains a snapshot of domain
		// NOTE: do not use the failover version inside, use currentVersion above
		domainEntry *cache.DomainCacheEntry
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
		decisionTaskManager mutableStateDecisionTaskManager
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
	domainEntry *cache.DomainCacheEntry,
) *mutableStateBuilder {
	s := &mutableStateBuilder{
		updateActivityInfos:        make(map[*persistence.ActivityInfo]struct{}),
		pendingActivityInfoIDs:     make(map[int64]*persistence.ActivityInfo),
		pendingActivityIDToEventID: make(map[string]int64),
		deleteActivityInfos:        make(map[int64]struct{}),
		syncActivityTasks:          make(map[int64]struct{}),

		pendingTimerInfoIDs:     make(map[string]*persistenceblobs.TimerInfo),
		pendingTimerEventIDToID: make(map[int64]string),
		updateTimerInfos:        make(map[*persistenceblobs.TimerInfo]struct{}),
		deleteTimerInfos:        make(map[string]struct{}),

		updateChildExecutionInfos:    make(map[*persistence.ChildExecutionInfo]struct{}),
		pendingChildExecutionInfoIDs: make(map[int64]*persistence.ChildExecutionInfo),
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

		currentVersion:        domainEntry.GetFailoverVersion(),
		hasBufferedEventsInDB: false,
		stateInDB:             persistence.WorkflowStateVoid,
		nextEventIDInDB:       0,
		domainEntry:           domainEntry,
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
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		DecisionRequestID:  emptyUUID,
		DecisionTimeout:    0,

		NextEventID:        common.FirstEventID,
		State:              persistence.WorkflowStateCreated,
		CloseStatus:        persistence.WorkflowCloseStatusRunning,
		LastProcessedEvent: common.EmptyEventID,
	}
	s.hBuilder = newHistoryBuilder(s, logger)
	s.taskGenerator = newMutableStateTaskGenerator(shard.GetDomainCache(), s.logger, s)
	s.decisionTaskManager = newMutableStateDecisionTaskManager(s)

	return s
}

func newMutableStateBuilderWithReplicationState(
	shard ShardContext,
	eventsCache eventsCache,
	logger log.Logger,
	domainEntry *cache.DomainCacheEntry,
) *mutableStateBuilder {
	s := newMutableStateBuilder(shard, eventsCache, logger, domainEntry)
	s.replicationState = &persistence.ReplicationState{
		StartVersion:        s.currentVersion,
		CurrentVersion:      s.currentVersion,
		LastWriteVersion:    common.EmptyVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastReplicationInfo: make(map[string]*replication.ReplicationInfo),
	}
	return s
}

func newMutableStateBuilderWithVersionHistories(
	shard ShardContext,
	eventsCache eventsCache,
	logger log.Logger,
	domainEntry *cache.DomainCacheEntry,
) *mutableStateBuilder {

	s := newMutableStateBuilder(shard, eventsCache, logger, domainEntry)
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

	// TODO when 2DC is deprecated, remove this
	state.ReplicationState = e.replicationState

	return state
}

func (e *mutableStateBuilder) Load(
	state *persistence.WorkflowMutableState,
) {

	e.pendingActivityInfoIDs = state.ActivityInfos
	for _, activityInfo := range state.ActivityInfos {
		e.pendingActivityIDToEventID[activityInfo.ActivityID] = activityInfo.ScheduleID
	}
	e.pendingTimerInfoIDs = state.TimerInfos
	for _, timerInfo := range state.TimerInfos {
		e.pendingTimerEventIDToID[timerInfo.StartedID] = timerInfo.TimerID
	}
	e.pendingChildExecutionInfoIDs = state.ChildExecutionInfos
	e.pendingRequestCancelInfoIDs = state.RequestCancelInfos
	e.pendingSignalInfoIDs = state.SignalInfos
	e.pendingSignalRequestedIDs = state.SignalRequestedIDs
	e.executionInfo = state.ExecutionInfo

	e.replicationState = state.ReplicationState
	e.bufferedEvents = state.BufferedEvents

	e.currentVersion = common.EmptyVersion
	e.hasBufferedEventsInDB = len(e.bufferedEvents) > 0
	e.stateInDB = state.ExecutionInfo.State
	e.nextEventIDInDB = state.ExecutionInfo.NextEventID
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
	return e.executionInfo.BranchToken, nil
}

func (e *mutableStateBuilder) GetVersionHistories() *persistence.VersionHistories {
	return e.versionHistories
}

// set treeID/historyBranches
func (e *mutableStateBuilder) SetHistoryTree(
	treeID []byte,
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
		exeInfo.BranchToken = branchToken
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

func (e *mutableStateBuilder) GetReplicationState() *persistence.ReplicationState {
	return e.replicationState
}

func (e *mutableStateBuilder) FlushBufferedEvents() error {
	// put new events into 2 buckets:
	//  1) if the event was added while there was in-flight decision, then put it in buffered bucket
	//  2) otherwise, put it in committed bucket
	var newBufferedEvents []*commonproto.HistoryEvent
	var newCommittedEvents []*commonproto.HistoryEvent
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
	var reorderedEvents []*commonproto.HistoryEvent
	reorderFunc := func(bufferedEvents []*commonproto.HistoryEvent) {
		for _, event := range bufferedEvents {
			switch event.GetEventType() {
			case enums.EventTypeActivityTaskCompleted,
				enums.EventTypeActivityTaskFailed,
				enums.EventTypeActivityTaskCanceled,
				enums.EventTypeActivityTaskTimedOut:
				reorderedEvents = append(reorderedEvents, event)
			case enums.EventTypeChildWorkflowExecutionCompleted,
				enums.EventTypeChildWorkflowExecutionFailed,
				enums.EventTypeChildWorkflowExecutionCanceled,
				enums.EventTypeChildWorkflowExecutionTimedOut,
				enums.EventTypeChildWorkflowExecutionTerminated:
				reorderedEvents = append(reorderedEvents, event)
			default:
				newCommittedEvents = append(newCommittedEvents, event)
			}
		}
	}

	// no decision in-flight, flush all buffered events to committed bucket
	if !e.HasInFlightDecision() {
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

	// if decision is not closed yet, and there are new buffered events, then put those to the pending buffer
	if e.HasInFlightDecision() && len(newBufferedEvents) > 0 {
		e.updateBufferedEvents = newBufferedEvents
	}

	return nil
}

func (e *mutableStateBuilder) UpdateCurrentVersion(
	version int64,
	forceUpdate bool,
) error {

	if state, _ := e.GetWorkflowStateCloseStatus(); state == persistence.WorkflowStateCompleted {
		// do not update current version only when workflow is completed
		return nil
	}

	// TODO when 2DC is deprecated, remove this block
	if e.replicationState != nil {
		e.UpdateReplicationStateVersion(version, forceUpdate)
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

	// TODO when NDC is fully rolled out remove this block
	//  since event local domain workflow will have version history
	if version != common.EmptyVersion {
		err := serviceerror.NewInternal("cannot update current version of local domain workflow to version other than empty version")
		e.logError(err.Error())
		return err
	}
	e.currentVersion = common.EmptyVersion
	return nil
}

func (e *mutableStateBuilder) GetCurrentVersion() int64 {

	if e.replicationState != nil {
		return e.replicationState.CurrentVersion
	}

	if e.versionHistories != nil {
		return e.currentVersion
	}

	return common.EmptyVersion
}

func (e *mutableStateBuilder) GetStartVersion() (int64, error) {

	if e.replicationState != nil {
		return e.replicationState.StartVersion, nil

	}

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

	if e.replicationState != nil {
		return e.replicationState.LastWriteVersion, nil
	}

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

// TODO nDC deprecate once replication state is deprecated
func (e *mutableStateBuilder) UpdateReplicationStateVersion(
	version int64,
	forceUpdate bool,
) {

	if version > e.replicationState.CurrentVersion || forceUpdate {
		e.replicationState.CurrentVersion = version
	}
}

// TODO nDC deprecate once replication state is deprecated
// Assumption: It is expected CurrentVersion on replication state is updated at the start of transaction when
// mutableState is loaded for this workflow execution.
func (e *mutableStateBuilder) UpdateReplicationStateLastEventID(
	lastWriteVersion,
	lastEventID int64,
) {
	e.replicationState.LastWriteVersion = lastWriteVersion
	e.replicationState.LastWriteEventID = lastEventID

	lastEventSourceCluster := e.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentCluster := e.clusterMetadata.GetCurrentClusterName()
	if lastEventSourceCluster != currentCluster {
		info, ok := e.replicationState.LastReplicationInfo[lastEventSourceCluster]
		if !ok {
			// replication.ReplicationInfo doesn't exist for this cluster, create one
			info = &replication.ReplicationInfo{}
			e.replicationState.LastReplicationInfo[lastEventSourceCluster] = info
		}

		info.Version = lastWriteVersion
		info.LastEventId = lastEventID
	}
}

func (e *mutableStateBuilder) checkAndClearTimerFiredEvent(
	timerID string,
) *commonproto.HistoryEvent {

	var timerEvent *commonproto.HistoryEvent

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
	events []*commonproto.HistoryEvent,
	timerID string,
) ([]*commonproto.HistoryEvent, *commonproto.HistoryEvent) {
	// go over all history events. if we find a timer fired event for the given
	// timerID, clear it
	timerFiredIdx := -1
	for idx, event := range events {
		if event.GetEventType() == enums.EventTypeTimerFired &&
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
	input []*commonproto.HistoryEvent,
) []*commonproto.HistoryEvent {

	if len(input) == 0 {
		return input
	}

	nextIndex := 0

loop:
	for _, event := range input {
		nextIndex++

		switch event.GetEventType() {
		case enums.EventTypeWorkflowExecutionCompleted,
			enums.EventTypeWorkflowExecutionFailed,
			enums.EventTypeWorkflowExecutionTimedOut,
			enums.EventTypeWorkflowExecutionTerminated,
			enums.EventTypeWorkflowExecutionContinuedAsNew,
			enums.EventTypeWorkflowExecutionCanceled:

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

		eventID := e.executionInfo.NextEventID
		event.EventId = eventID
		e.executionInfo.IncreaseNextEventID()

		switch event.GetEventType() {
		case enums.EventTypeActivityTaskStarted:
			attributes := event.GetActivityTaskStartedEventAttributes()
			scheduledID := attributes.GetScheduledEventId()
			scheduledIDToStartedID[scheduledID] = eventID
			if ai, ok := e.GetActivityInfo(scheduledID); ok {
				ai.StartedID = eventID
				e.updateActivityInfos[ai] = struct{}{}
			}
		case enums.EventTypeChildWorkflowExecutionStarted:
			attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
			initiatedID := attributes.GetInitiatedEventId()
			scheduledIDToStartedID[initiatedID] = eventID
			if ci, ok := e.GetChildExecutionInfo(initiatedID); ok {
				ci.StartedID = eventID
				e.updateChildExecutionInfos[ci] = struct{}{}
			}
		case enums.EventTypeActivityTaskCompleted:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeActivityTaskFailed:
			attributes := event.GetActivityTaskFailedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeActivityTaskTimedOut:
			attributes := event.GetActivityTaskTimedOutEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeActivityTaskCanceled:
			attributes := event.GetActivityTaskCanceledEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeChildWorkflowExecutionCompleted:
			attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeChildWorkflowExecutionFailed:
			attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeChildWorkflowExecutionTimedOut:
			attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeChildWorkflowExecutionCanceled:
			attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
			if startedID, ok := scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedID
			}
		case enums.EventTypeChildWorkflowExecutionTerminated:
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
				e.executionInfo.LastEventTaskID = taskID
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
				e.executionInfo.LastEventTaskID = taskID
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
	case persistence.WorkflowStateVoid:
		return false
	case persistence.WorkflowStateCreated:
		return true
	case persistence.WorkflowStateRunning:
		return true
	case persistence.WorkflowStateCompleted:
		return false
	case persistence.WorkflowStateZombie:
		return false
	case persistence.WorkflowStateCorrupted:
		return false
	default:
		panic(fmt.Sprintf("unknown workflow state: %v", e.executionInfo.State))
	}
}

func (e *mutableStateBuilder) GetDomainEntry() *cache.DomainCacheEntry {
	return e.domainEntry
}

func (e *mutableStateBuilder) IsStickyTaskListEnabled() bool {
	if e.executionInfo.StickyTaskList == "" {
		return false
	}
	ttl := e.config.StickyTTL(e.GetDomainEntry().GetInfo().Name)
	if e.timeSource.Now().After(e.executionInfo.LastUpdatedTimestamp.Add(ttl)) {
		return false
	}
	return true
}

func (e *mutableStateBuilder) CreateNewHistoryEvent(
	eventType enums.EventType,
) *commonproto.HistoryEvent {

	return e.CreateNewHistoryEventWithTimestamp(eventType, e.timeSource.Now().UnixNano())
}

func (e *mutableStateBuilder) CreateNewHistoryEventWithTimestamp(
	eventType enums.EventType,
	timestamp int64,
) *commonproto.HistoryEvent {
	eventID := e.executionInfo.NextEventID
	if e.shouldBufferEvent(eventType) {
		eventID = common.BufferedEventID
	} else {
		// only increase NextEventID if event is not buffered
		e.executionInfo.IncreaseNextEventID()
	}

	ts := timestamp
	historyEvent := &commonproto.HistoryEvent{}
	historyEvent.EventId = eventID
	historyEvent.Timestamp = ts
	historyEvent.EventType = eventType
	historyEvent.Version = e.GetCurrentVersion()
	historyEvent.TaskId = common.EmptyEventTaskID

	return historyEvent
}

func (e *mutableStateBuilder) shouldBufferEvent(
	eventType enums.EventType,
) bool {

	switch eventType {
	case // do not buffer for workflow state change
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeWorkflowExecutionCompleted,
		enums.EventTypeWorkflowExecutionFailed,
		enums.EventTypeWorkflowExecutionTimedOut,
		enums.EventTypeWorkflowExecutionTerminated,
		enums.EventTypeWorkflowExecutionContinuedAsNew,
		enums.EventTypeWorkflowExecutionCanceled:
		return false
	case // decision event should not be buffered
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskCompleted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeDecisionTaskTimedOut:
		return false
	case // events generated directly from decisions should not be buffered
		// workflow complete, failed, cancelled and continue-as-new events are duplication of above
		// just put is here for reference
		// workflow.EventTypeWorkflowExecutionCompleted,
		// workflow.EventTypeWorkflowExecutionFailed,
		// workflow.EventTypeWorkflowExecutionCanceled,
		// workflow.EventTypeWorkflowExecutionContinuedAsNew,
		enums.EventTypeActivityTaskScheduled,
		enums.EventTypeActivityTaskCancelRequested,
		enums.EventTypeTimerStarted,
		// DecisionTypeCancelTimer is an exception. This decision will be mapped
		// to either workflow.EventTypeTimerCanceled, or workflow.EventTypeCancelTimerFailed.
		// So both should not be buffered. Ref: historyEngine, search for "workflow.DecisionTypeCancelTimer"
		enums.EventTypeTimerCanceled,
		enums.EventTypeCancelTimerFailed,
		enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated,
		enums.EventTypeMarkerRecorded,
		enums.EventTypeStartChildWorkflowExecutionInitiated,
		enums.EventTypeSignalExternalWorkflowExecutionInitiated,
		enums.EventTypeUpsertWorkflowSearchAttributes:
		// do not buffer event if event is directly generated from a corresponding decision

		// sanity check there is no decision on the fly
		if e.HasInFlightDecision() {
			msg := fmt.Sprintf("history mutable state is processing event: %v while there is decision pending. "+
				"domainID: %v, workflow ID: %v, run ID: %v.", eventType, e.executionInfo.DomainID, e.executionInfo.WorkflowID, e.executionInfo.RunID)
			panic(msg)
		}
		return false
	default:
		return true
	}
}

func (e *mutableStateBuilder) GetWorkflowType() *commonproto.WorkflowType {
	wType := &commonproto.WorkflowType{}
	wType.Name = e.executionInfo.WorkflowTypeName

	return wType
}

func (e *mutableStateBuilder) GetQueryRegistry() queryRegistry {
	return e.queryRegistry
}

func (e *mutableStateBuilder) GetActivityScheduledEvent(
	scheduleEventID int64,
) (*commonproto.HistoryEvent, error) {

	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, ErrMissingActivityInfo
	}

	// Needed for backward compatibility reason
	if ai.ScheduledEvent != nil {
		return ai.ScheduledEvent, nil
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	scheduledEvent, err := e.eventsCache.getEvent(
		e.executionInfo.DomainID,
		e.executionInfo.WorkflowID,
		e.executionInfo.RunID,
		ai.ScheduledEventBatchID,
		ai.ScheduleID,
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
) (*persistence.ActivityInfo, bool) {

	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	return ai, ok
}

// GetActivityByActivityID gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityByActivityID(
	activityID string,
) (*persistence.ActivityInfo, bool) {

	eventID, ok := e.pendingActivityIDToEventID[activityID]
	if !ok {
		return nil, false
	}
	return e.GetActivityInfo(eventID)
}

// GetChildExecutionInfo gives details about a child execution that is currently in progress.
func (e *mutableStateBuilder) GetChildExecutionInfo(
	initiatedEventID int64,
) (*persistence.ChildExecutionInfo, bool) {

	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	return ci, ok
}

// GetChildExecutionInitiatedEvent reads out the ChildExecutionInitiatedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionInitiatedEvent(
	initiatedEventID int64,
) (*commonproto.HistoryEvent, error) {

	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingChildWorkflowInfo
	}

	// Needed for backward compatibility reason
	if ci.InitiatedEvent != nil {
		return ci.InitiatedEvent, nil
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	initiatedEvent, err := e.eventsCache.getEvent(
		e.executionInfo.DomainID,
		e.executionInfo.WorkflowID,
		e.executionInfo.RunID,
		ci.InitiatedEventBatchID,
		ci.InitiatedID,
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
	errReason string,
) time.Duration {

	info := e.executionInfo
	if !info.HasRetryPolicy {
		return backoff.NoBackoff
	}

	return getBackoffInterval(
		e.timeSource.Now(),
		info.ExpirationTime,
		info.Attempt,
		info.MaximumAttempts,
		info.InitialInterval,
		info.MaximumInterval,
		info.BackoffCoefficient,
		errReason,
		info.NonRetriableErrors,
	)
}

func (e *mutableStateBuilder) GetCronBackoffDuration() (time.Duration, error) {
	info := e.executionInfo
	if len(info.CronSchedule) == 0 {
		return backoff.NoBackoff, nil
	}
	// TODO: decide if we can add execution time in execution info.
	executionTime := e.executionInfo.StartTimestamp
	// This only call when doing ContinueAsNew. At this point, the workflow should have a start event
	workflowStartEvent, err := e.GetStartEvent()
	if err != nil {
		e.logError("unable to find workflow start event", tag.ErrorTypeInvalidHistoryAction)
		return backoff.NoBackoff, err
	}
	firstDecisionTaskBackoff :=
		time.Duration(workflowStartEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstDecisionTaskBackoffSeconds()) * time.Second
	executionTime = executionTime.Add(firstDecisionTaskBackoff)
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
func (e *mutableStateBuilder) GetCompletionEvent() (*commonproto.HistoryEvent, error) {
	if e.executionInfo.State != persistence.WorkflowStateCompleted {
		return nil, ErrMissingWorkflowCompletionEvent
	}

	// Needed for backward compatibility reason
	if e.executionInfo.CompletionEvent != nil {
		return e.executionInfo.CompletionEvent, nil
	}

	// Needed for backward compatibility reason
	if e.executionInfo.CompletionEventBatchID == common.EmptyEventID {
		return nil, ErrMissingWorkflowCompletionEvent
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	// Completion EventID is always one less than NextEventID after workflow is completed
	completionEventID := e.executionInfo.NextEventID - 1
	firstEventID := e.executionInfo.CompletionEventBatchID
	completionEvent, err := e.eventsCache.getEvent(
		e.executionInfo.DomainID,
		e.executionInfo.WorkflowID,
		e.executionInfo.RunID,
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
func (e *mutableStateBuilder) GetStartEvent() (*commonproto.HistoryEvent, error) {

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	startEvent, err := e.eventsCache.getEvent(
		e.executionInfo.DomainID,
		e.executionInfo.WorkflowID,
		e.executionInfo.RunID,
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
	event *commonproto.HistoryEvent,
) {

	// For start event: store it within events cache so the recordWorkflowStarted transfer task doesn't need to
	// load it from database
	// For completion event: store it within events cache so we can communicate the result to parent execution
	// during the processing of DeleteTransferTask without loading this event from database
	e.eventsCache.putEvent(
		e.executionInfo.DomainID,
		e.executionInfo.WorkflowID,
		e.executionInfo.RunID,
		event.GetEventId(),
		event,
	)
}

func (e *mutableStateBuilder) HasParentExecution() bool {
	return e.executionInfo.ParentDomainID != "" && e.executionInfo.ParentWorkflowID != ""
}

func (e *mutableStateBuilder) UpdateActivityProgress(
	ai *persistence.ActivityInfo,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) {
	ai.Version = e.GetCurrentVersion()
	ai.Details = request.Details
	ai.LastHeartBeatUpdatedTime = e.timeSource.Now()
	e.updateActivityInfos[ai] = struct{}{}
	e.syncActivityTasks[ai.ScheduleID] = struct{}{}
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
	ai.ScheduledTime = time.Unix(0, request.GetScheduledTime())
	ai.StartedID = request.GetStartedId()
	ai.LastHeartBeatUpdatedTime = time.Unix(0, request.GetLastHeartbeatTime())
	if ai.StartedID == common.EmptyEventID {
		ai.StartedTime = time.Time{}
	} else {
		ai.StartedTime = time.Unix(0, request.GetStartedTime())
	}
	ai.Details = request.GetDetails()
	ai.Attempt = request.GetAttempt()
	ai.LastFailureReason = request.GetLastFailureReason()
	ai.LastWorkerIdentity = request.GetLastWorkerIdentity()
	ai.LastFailureDetails = request.GetLastFailureDetails()

	if resetActivityTimerTaskStatus {
		ai.TimerTaskStatus = timerTaskStatusNone
	}

	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

// UpdateActivity updates an activity
func (e *mutableStateBuilder) UpdateActivity(
	ai *persistence.ActivityInfo,
) error {

	if _, ok := e.pendingActivityInfoIDs[ai.ScheduleID]; !ok {
		e.logError(
			fmt.Sprintf("unable to find activity ID: %v in mutable state", ai.ActivityID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	e.pendingActivityInfoIDs[ai.ScheduleID] = ai
	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

// DeleteActivity deletes details about an activity.
func (e *mutableStateBuilder) DeleteActivity(
	scheduleEventID int64,
) error {

	if activityInfo, ok := e.pendingActivityInfoIDs[scheduleEventID]; ok {
		delete(e.pendingActivityInfoIDs, scheduleEventID)

		if _, ok = e.pendingActivityIDToEventID[activityInfo.ActivityID]; ok {
			delete(e.pendingActivityIDToEventID, activityInfo.ActivityID)
		} else {
			e.logError(
				fmt.Sprintf("unable to find activity ID: %v in mutable state", activityInfo.ActivityID),
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

	timerID, ok := e.pendingTimerEventIDToID[ti.StartedID]
	if !ok {
		e.logError(
			fmt.Sprintf("unable to find timer event ID: %v in mutable state", ti.StartedID),
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

		if _, ok = e.pendingTimerEventIDToID[timerInfo.StartedID]; ok {
			delete(e.pendingTimerEventIDToID, timerInfo.StartedID)
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

//nolint:unused
func (e *mutableStateBuilder) getDecisionInfo() *decisionInfo {

	taskList := e.executionInfo.TaskList
	if e.IsStickyTaskListEnabled() {
		taskList = e.executionInfo.StickyTaskList
	}
	return &decisionInfo{
		Version:                    e.executionInfo.DecisionVersion,
		ScheduleID:                 e.executionInfo.DecisionScheduleID,
		StartedID:                  e.executionInfo.DecisionStartedID,
		RequestID:                  e.executionInfo.DecisionRequestID,
		DecisionTimeout:            e.executionInfo.DecisionTimeout,
		Attempt:                    e.executionInfo.DecisionAttempt,
		StartedTimestamp:           e.executionInfo.DecisionStartedTimestamp,
		ScheduledTimestamp:         e.executionInfo.DecisionScheduledTimestamp,
		TaskList:                   taskList,
		OriginalScheduledTimestamp: e.executionInfo.DecisionOriginalScheduledTimestamp,
	}
}

// GetDecisionInfo returns details about the in-progress decision task
func (e *mutableStateBuilder) GetDecisionInfo(
	scheduleEventID int64,
) (*decisionInfo, bool) {
	return e.decisionTaskManager.GetDecisionInfo(scheduleEventID)
}

func (e *mutableStateBuilder) GetPendingActivityInfos() map[int64]*persistence.ActivityInfo {
	return e.pendingActivityInfoIDs
}

func (e *mutableStateBuilder) GetPendingTimerInfos() map[string]*persistenceblobs.TimerInfo {
	return e.pendingTimerInfoIDs
}

func (e *mutableStateBuilder) GetPendingChildExecutionInfos() map[int64]*persistence.ChildExecutionInfo {
	return e.pendingChildExecutionInfoIDs
}

func (e *mutableStateBuilder) GetPendingRequestCancelExternalInfos() map[int64]*persistenceblobs.RequestCancelInfo {
	return e.pendingRequestCancelInfoIDs
}

func (e *mutableStateBuilder) GetPendingSignalExternalInfos() map[int64]*persistenceblobs.SignalInfo {
	return e.pendingSignalInfoIDs
}

func (e *mutableStateBuilder) HasProcessedOrPendingDecision() bool {
	return e.decisionTaskManager.HasProcessedOrPendingDecision()
}

func (e *mutableStateBuilder) HasPendingDecision() bool {
	return e.decisionTaskManager.HasPendingDecision()
}

func (e *mutableStateBuilder) GetPendingDecision() (*decisionInfo, bool) {
	return e.decisionTaskManager.GetPendingDecision()
}

func (e *mutableStateBuilder) HasInFlightDecision() bool {
	return e.decisionTaskManager.HasInFlightDecision()
}

func (e *mutableStateBuilder) GetInFlightDecision() (*decisionInfo, bool) {
	return e.decisionTaskManager.GetInFlightDecision()
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

// UpdateDecision updates a decision task.
func (e *mutableStateBuilder) UpdateDecision(
	decision *decisionInfo,
) {
	e.decisionTaskManager.UpdateDecision(decision)
}

// DeleteDecision deletes a decision task.
func (e *mutableStateBuilder) DeleteDecision() {
	e.decisionTaskManager.DeleteDecision()
}

func (e *mutableStateBuilder) FailDecision(
	incrementAttempt bool,
) {
	e.decisionTaskManager.FailDecision(incrementAttempt)
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

// GetPreviousStartedEventID returns last started decision task event ID
func (e *mutableStateBuilder) GetPreviousStartedEventID() int64 {
	return e.executionInfo.LastProcessedEvent
}

func (e *mutableStateBuilder) IsWorkflowExecutionRunning() bool {
	switch e.executionInfo.State {
	case persistence.WorkflowStateCreated:
		return true
	case persistence.WorkflowStateRunning:
		return true
	case persistence.WorkflowStateCompleted:
		return false
	case persistence.WorkflowStateZombie:
		return false
	case persistence.WorkflowStateCorrupted:
		return false
	default:
		panic(fmt.Sprintf("unknown workflow state: %v", e.executionInfo.State))
	}
}

func (e *mutableStateBuilder) IsCancelRequested() (bool, string) {
	if e.executionInfo.CancelRequested {
		return e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID
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
	parentExecutionInfo *commonproto.ParentExecutionInfo,
	execution commonproto.WorkflowExecution,
	previousExecutionState mutableState,
	attributes *commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes,
	firstRunID string,
) (*commonproto.HistoryEvent, error) {

	previousExecutionInfo := previousExecutionState.GetExecutionInfo()
	taskList := previousExecutionInfo.TaskList
	if attributes.TaskList != nil {
		taskList = attributes.TaskList.GetName()
	}
	tl := &commonproto.TaskList{}
	tl.Name = taskList

	workflowType := previousExecutionInfo.WorkflowTypeName
	if attributes.WorkflowType != nil {
		workflowType = attributes.WorkflowType.GetName()
	}
	wType := &commonproto.WorkflowType{}
	wType.Name = workflowType

	decisionTimeout := previousExecutionInfo.DecisionStartToCloseTimeout
	if attributes.TaskStartToCloseTimeoutSeconds != 0 {
		decisionTimeout = attributes.GetTaskStartToCloseTimeoutSeconds()
	}

	createRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              e.domainEntry.GetInfo().Name,
		WorkflowId:                          execution.WorkflowId,
		TaskList:                            tl,
		WorkflowType:                        wType,
		TaskStartToCloseTimeoutSeconds:      decisionTimeout,
		ExecutionStartToCloseTimeoutSeconds: attributes.ExecutionStartToCloseTimeoutSeconds,
		Input:                               attributes.Input,
		Header:                              attributes.Header,
		RetryPolicy:                         attributes.RetryPolicy,
		CronSchedule:                        attributes.CronSchedule,
		Memo:                                attributes.Memo,
		SearchAttributes:                    attributes.SearchAttributes,
	}

	req := &historyservice.StartWorkflowExecutionRequest{
		DomainUUID:                      e.domainEntry.GetInfo().ID,
		StartRequest:                    createRequest,
		ParentExecutionInfo:             parentExecutionInfo,
		LastCompletionResult:            attributes.LastCompletionResult,
		ContinuedFailureReason:          attributes.FailureReason,
		ContinuedFailureDetails:         attributes.FailureDetails,
		ContinueAsNewInitiator:          attributes.Initiator,
		FirstDecisionTaskBackoffSeconds: attributes.BackoffStartIntervalInSeconds,
	}
	if attributes.GetInitiator() == enums.ContinueAsNewInitiatorRetryPolicy {
		req.Attempt = previousExecutionState.GetExecutionInfo().Attempt + 1
		expirationTime := previousExecutionState.GetExecutionInfo().ExpirationTime
		if !expirationTime.IsZero() {
			req.ExpirationTimestamp = expirationTime.UnixNano()
		}
	} else {
		// ContinueAsNew by decider or cron
		req.Attempt = 0
		if attributes.RetryPolicy != nil && attributes.RetryPolicy.GetExpirationIntervalInSeconds() > 0 {
			// has retry policy and expiration time.
			expirationSeconds := attributes.RetryPolicy.GetExpirationIntervalInSeconds() + req.GetFirstDecisionTaskBackoffSeconds()
			expirationTime := e.timeSource.Now().Add(time.Second * time.Duration(expirationSeconds))
			req.ExpirationTimestamp = expirationTime.UnixNano()
		}
	}

	// History event only has domainName so domainID has to be passed in explicitly to update the mutable state
	var parentDomainID string
	if parentExecutionInfo != nil {
		parentDomainID = parentExecutionInfo.DomainUUID
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(req, previousExecutionInfo, firstRunID, execution.GetRunId())
	if err := e.ReplicateWorkflowExecutionStartedEvent(
		parentDomainID,
		execution,
		createRequest.GetRequestId(),
		event,
	); err != nil {
		return nil, err
	}

	if err := e.SetHistoryTree(primitives.MustParseUUID(e.GetExecutionInfo().RunID)); err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowStartTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, err
	}
	if err := e.taskGenerator.generateRecordWorkflowStartedTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, err
	}

	if err := e.AddFirstDecisionTaskScheduled(
		event,
	); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEvent(
	execution commonproto.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (*commonproto.HistoryEvent, error) {

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

	var parentDomainID string
	if startRequest.ParentExecutionInfo != nil {
		parentDomainID = startRequest.ParentExecutionInfo.GetDomainUUID()
	}
	if err := e.ReplicateWorkflowExecutionStartedEvent(
		parentDomainID,
		execution,
		request.GetRequestId(),
		event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowStartTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, err
	}
	if err := e.taskGenerator.generateRecordWorkflowStartedTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionStartedEvent(
	parentDomainID string,
	execution commonproto.WorkflowExecution,
	requestID string,
	startEvent *commonproto.HistoryEvent,
) error {

	event := startEvent.GetWorkflowExecutionStartedEventAttributes()
	e.executionInfo.CreateRequestID = requestID
	e.executionInfo.DomainID = e.domainEntry.GetInfo().ID
	e.executionInfo.WorkflowID = execution.GetWorkflowId()
	e.executionInfo.RunID = execution.GetRunId()
	e.executionInfo.TaskList = event.TaskList.GetName()
	e.executionInfo.WorkflowTypeName = event.WorkflowType.GetName()
	e.executionInfo.WorkflowTimeout = event.GetExecutionStartToCloseTimeoutSeconds()
	e.executionInfo.DecisionStartToCloseTimeout = event.GetTaskStartToCloseTimeoutSeconds()

	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCreated,
		persistence.WorkflowCloseStatusRunning,
	); err != nil {
		return err
	}
	e.executionInfo.LastProcessedEvent = common.EmptyEventID
	e.executionInfo.LastFirstEventID = startEvent.GetEventId()

	e.executionInfo.DecisionVersion = common.EmptyVersion
	e.executionInfo.DecisionScheduleID = common.EmptyEventID
	e.executionInfo.DecisionStartedID = common.EmptyEventID
	e.executionInfo.DecisionRequestID = emptyUUID
	e.executionInfo.DecisionTimeout = 0

	e.executionInfo.CronSchedule = event.GetCronSchedule()
	e.executionInfo.ParentDomainID = parentDomainID

	if event.ParentWorkflowExecution != nil {
		e.executionInfo.ParentWorkflowID = event.ParentWorkflowExecution.GetWorkflowId()
		e.executionInfo.ParentRunID = event.ParentWorkflowExecution.GetRunId()
	}

	if event.ParentInitiatedEventId != 0 {
		e.executionInfo.InitiatedID = event.GetParentInitiatedEventId()
	} else {
		e.executionInfo.InitiatedID = common.EmptyEventID
	}

	e.executionInfo.Attempt = event.GetAttempt()
	if event.GetExpirationTimestamp() != 0 {
		e.executionInfo.ExpirationTime = time.Unix(0, event.GetExpirationTimestamp())
	}
	if event.RetryPolicy != nil {
		e.executionInfo.HasRetryPolicy = true
		e.executionInfo.BackoffCoefficient = event.RetryPolicy.GetBackoffCoefficient()
		e.executionInfo.ExpirationSeconds = event.RetryPolicy.GetExpirationIntervalInSeconds()
		e.executionInfo.InitialInterval = event.RetryPolicy.GetInitialIntervalInSeconds()
		e.executionInfo.MaximumAttempts = event.RetryPolicy.GetMaximumAttempts()
		e.executionInfo.MaximumInterval = event.RetryPolicy.GetMaximumIntervalInSeconds()
		e.executionInfo.NonRetriableErrors = event.RetryPolicy.NonRetriableErrorReasons
	}

	e.executionInfo.AutoResetPoints = rolloverAutoResetPointsWithExpiringTime(
		event.GetPrevAutoResetPoints(),
		event.GetContinuedExecutionRunId(),
		startEvent.GetTimestamp(),
		e.domainEntry.GetRetentionDays(e.executionInfo.WorkflowID),
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

func (e *mutableStateBuilder) AddFirstDecisionTaskScheduled(
	startEvent *commonproto.HistoryEvent,
) error {
	opTag := tag.WorkflowActionDecisionTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return err
	}
	return e.decisionTaskManager.AddFirstDecisionTaskScheduled(startEvent)
}

func (e *mutableStateBuilder) AddDecisionTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*decisionInfo, error) {
	opTag := tag.WorkflowActionDecisionTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskScheduledEvent(bypassTaskGeneration)
}

// originalScheduledTimestamp is to record the first scheduled decision during decision heartbeat.
func (e *mutableStateBuilder) AddDecisionTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp int64,
) (*decisionInfo, error) {
	opTag := tag.WorkflowActionDecisionTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp)
}

func (e *mutableStateBuilder) ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error) {
	return e.decisionTaskManager.ReplicateTransientDecisionTaskScheduled()
}

func (e *mutableStateBuilder) ReplicateDecisionTaskScheduledEvent(
	version int64,
	scheduleID int64,
	taskList string,
	startToCloseTimeoutSeconds int32,
	attempt int64,
	scheduleTimestamp int64,
	originalScheduledTimestamp int64,
) (*decisionInfo, error) {
	return e.decisionTaskManager.ReplicateDecisionTaskScheduledEvent(version, scheduleID, taskList, startToCloseTimeoutSeconds, attempt, scheduleTimestamp, originalScheduledTimestamp)
}

func (e *mutableStateBuilder) AddDecisionTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	request *workflowservice.PollForDecisionTaskRequest,
) (*commonproto.HistoryEvent, *decisionInfo, error) {
	opTag := tag.WorkflowActionDecisionTaskStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskStartedEvent(scheduleEventID, requestID, request)
}

func (e *mutableStateBuilder) ReplicateDecisionTaskStartedEvent(
	decision *decisionInfo,
	version int64,
	scheduleID int64,
	startedID int64,
	requestID string,
	timestamp int64,
) (*decisionInfo, error) {

	return e.decisionTaskManager.ReplicateDecisionTaskStartedEvent(decision, version, scheduleID, startedID, requestID, timestamp)
}

func (e *mutableStateBuilder) CreateTransientDecisionEvents(
	decision *decisionInfo,
	identity string,
) (*commonproto.HistoryEvent, *commonproto.HistoryEvent) {
	return e.decisionTaskManager.CreateTransientDecisionEvents(decision, identity)
}

// add BinaryCheckSum for the first decisionTaskCompletedID for auto-reset
func (e *mutableStateBuilder) addBinaryCheckSumIfNotExists(
	event *commonproto.HistoryEvent,
	maxResetPoints int,
) error {
	binChecksum := event.GetDecisionTaskCompletedEventAttributes().GetBinaryChecksum()
	if len(binChecksum) == 0 {
		return nil
	}
	exeInfo := e.executionInfo
	var currResetPoints []*commonproto.ResetPointInfo
	if exeInfo.AutoResetPoints != nil && exeInfo.AutoResetPoints.Points != nil {
		currResetPoints = e.executionInfo.AutoResetPoints.Points
	} else {
		currResetPoints = make([]*commonproto.ResetPointInfo, 0, 1)
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
	info := &commonproto.ResetPointInfo{
		BinaryChecksum:           binChecksum,
		RunId:                    exeInfo.RunID,
		FirstDecisionCompletedId: event.GetEventId(),
		CreatedTimeNano:          e.timeSource.Now().UnixNano(),
		Resettable:               resettable,
	}
	currResetPoints = append(currResetPoints, info)
	exeInfo.AutoResetPoints = &commonproto.ResetPoints{
		Points: currResetPoints,
	}
	bytes, err := json.Marshal(recentBinaryChecksums)
	if err != nil {
		return err
	}
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string][]byte)
	}
	exeInfo.SearchAttributes[definition.BinaryChecksums] = bytes
	if e.shard.GetConfig().AdvancedVisibilityWritingMode() != common.AdvancedVisibilityWritingModeOff {
		return e.taskGenerator.generateWorkflowSearchAttrTasks(e.unixNanoToTime(event.GetTimestamp()))
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

func (e *mutableStateBuilder) AddDecisionTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
	maxResetPoints int,
) (*commonproto.HistoryEvent, error) {
	opTag := tag.WorkflowActionDecisionTaskCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request, maxResetPoints)
}

func (e *mutableStateBuilder) ReplicateDecisionTaskCompletedEvent(
	event *commonproto.HistoryEvent,
) error {
	return e.decisionTaskManager.ReplicateDecisionTaskCompletedEvent(event)
}

func (e *mutableStateBuilder) AddDecisionTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*commonproto.HistoryEvent, error) {
	opTag := tag.WorkflowActionDecisionTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskTimedOutEvent(scheduleEventID, startedEventID)
}

func (e *mutableStateBuilder) ReplicateDecisionTaskTimedOutEvent(
	timeoutType enums.TimeoutType,
) error {
	return e.decisionTaskManager.ReplicateDecisionTaskTimedOutEvent(timeoutType)
}

func (e *mutableStateBuilder) AddDecisionTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*commonproto.HistoryEvent, error) {
	opTag := tag.WorkflowActionDecisionTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskScheduleToStartTimeoutEvent(scheduleEventID)
}

func (e *mutableStateBuilder) AddDecisionTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	cause enums.DecisionTaskFailedCause,
	details []byte,
	identity string,
	reason string,
	binChecksum string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
) (*commonproto.HistoryEvent, error) {
	opTag := tag.WorkflowActionDecisionTaskFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.decisionTaskManager.AddDecisionTaskFailedEvent(
		scheduleEventID,
		startedEventID,
		cause,
		details,
		identity,
		reason,
		binChecksum,
		baseRunID,
		newRunID,
		forkEventVersion,
	)
}

func (e *mutableStateBuilder) ReplicateDecisionTaskFailedEvent() error {
	return e.decisionTaskManager.ReplicateDecisionTaskFailedEvent()
}

func (e *mutableStateBuilder) AddActivityTaskScheduledEvent(
	decisionCompletedEventID int64,
	attributes *commonproto.ScheduleActivityTaskDecisionAttributes,
) (*commonproto.HistoryEvent, *persistence.ActivityInfo, error) {

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

	event := e.hBuilder.AddActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	// Write the event to cache only on active cluster for processing on activity started or retried
	e.eventsCache.putEvent(
		e.executionInfo.DomainID,
		e.executionInfo.WorkflowID,
		e.executionInfo.RunID,
		event.GetEventId(),
		event,
	)

	ai, err := e.ReplicateActivityTaskScheduledEvent(decisionCompletedEventID, event)
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateActivityTransferTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ai, err
}

func (e *mutableStateBuilder) ReplicateActivityTaskScheduledEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
) (*persistence.ActivityInfo, error) {

	attributes := event.GetActivityTaskScheduledEventAttributes()
	targetDomainID := e.executionInfo.DomainID
	if attributes.GetDomain() != "" {
		targetDomainEntry, err := e.shard.GetDomainCache().GetDomain(attributes.GetDomain())
		if err != nil {
			return nil, err
		}
		targetDomainID = targetDomainEntry.GetInfo().ID
	}

	scheduleEventID := event.GetEventId()
	scheduleToCloseTimeout := attributes.GetScheduleToCloseTimeoutSeconds()

	ai := &persistence.ActivityInfo{
		Version:                  event.GetVersion(),
		ScheduleID:               scheduleEventID,
		ScheduledEventBatchID:    firstEventID,
		ScheduledTime:            time.Unix(0, event.GetTimestamp()),
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               attributes.ActivityId,
		DomainID:                 targetDomainID,
		ScheduleToStartTimeout:   attributes.GetScheduleToStartTimeoutSeconds(),
		ScheduleToCloseTimeout:   scheduleToCloseTimeout,
		StartToCloseTimeout:      attributes.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeout:         attributes.GetHeartbeatTimeoutSeconds(),
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusNone,
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
	e.pendingActivityIDToEventID[ai.ActivityID] = scheduleEventID
	e.updateActivityInfos[ai] = struct{}{}

	return ai, nil
}

func (e *mutableStateBuilder) addTransientActivityStartedEvent(
	scheduleEventID int64,
) error {

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedID != common.TransientEventID {
		return nil
	}

	// activity task was started (as transient event), we need to add it now.
	event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, ai.Attempt, ai.RequestID, ai.StartedIdentity)
	if !ai.StartedTime.IsZero() {
		// overwrite started event time to the one recorded in ActivityInfo
		event.Timestamp = ai.StartedTime.UnixNano()
	}
	return e.ReplicateActivityTaskStartedEvent(event)
}

func (e *mutableStateBuilder) AddActivityTaskStartedEvent(
	ai *persistence.ActivityInfo,
	scheduleEventID int64,
	requestID string,
	identity string,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if !ai.HasRetryPolicy {
		event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, ai.Attempt, requestID, identity)
		if err := e.ReplicateActivityTaskStartedEvent(event); err != nil {
			return nil, err
		}
		return event, nil
	}

	// we might need to retry, so do not append started event just yet,
	// instead update mutable state and will record started event when activity task is closed
	ai.Version = e.GetCurrentVersion()
	ai.StartedID = common.TransientEventID
	ai.RequestID = requestID
	ai.StartedTime = e.timeSource.Now()
	ai.LastHeartBeatUpdatedTime = ai.StartedTime
	ai.StartedIdentity = identity
	if err := e.UpdateActivity(ai); err != nil {
		return nil, err
	}
	e.syncActivityTasks[ai.ScheduleID] = struct{}{}
	return nil, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskStartedEvent(
	event *commonproto.HistoryEvent,
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
	ai.StartedID = event.GetEventId()
	ai.RequestID = attributes.GetRequestId()
	ai.StartedTime = time.Unix(0, event.GetTimestamp())
	ai.LastHeartBeatUpdatedTime = ai.StartedTime
	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

func (e *mutableStateBuilder) AddActivityTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
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
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCompletedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondActivityTaskFailedRequest,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
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
	event := e.hBuilder.AddActivityTaskFailedEvent(scheduleEventID, startedEventID, request)
	if err := e.ReplicateActivityTaskFailedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskFailedEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetActivityTaskFailedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
	timeoutType enums.TimeoutType,
	lastHeartBeatDetails []byte,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedID != startedEventID || ((timeoutType == enums.TimeoutTypeStartToClose ||
		timeoutType == enums.TimeoutTypeHeartbeat) && ai.StartedID == common.EmptyEventID) {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduleID(ai.ScheduleID),
			tag.WorkflowStartedID(ai.StartedID),
			tag.WorkflowTimeoutType(int64(timeoutType)))
		return nil, e.createInternalServerError(opTag)
	}

	if err := e.addTransientActivityStartedEvent(scheduleEventID); err != nil {
		return nil, err
	}
	event := e.hBuilder.AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails, ai.LastFailureReason, ai.LastFailureDetails)
	if err := e.ReplicateActivityTaskTimedOutEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskTimedOutEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetActivityTaskTimedOutEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddActivityTaskCancelRequestedEvent(
	decisionCompletedEventID int64,
	activityID string,
	identity string,
) (*commonproto.HistoryEvent, *persistence.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskCancelRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	// we need to add the cancel request event even if activity not in mutable state
	// if activity not in mutable state or already cancel requested,
	// we do not need to call the replication function
	actCancelReqEvent := e.hBuilder.AddActivityTaskCancelRequestedEvent(decisionCompletedEventID, activityID)

	ai, ok := e.GetActivityByActivityID(activityID)
	if !ok || ai.CancelRequested {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowActivityID(activityID))

		return nil, nil, e.createCallerError(opTag)
	}

	if err := e.ReplicateActivityTaskCancelRequestedEvent(actCancelReqEvent); err != nil {
		return nil, nil, err
	}

	return actCancelReqEvent, ai, nil
}

func (e *mutableStateBuilder) ReplicateActivityTaskCancelRequestedEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCancelRequestedEventAttributes()
	activityID := attributes.GetActivityId()
	ai, ok := e.GetActivityByActivityID(activityID)
	if !ok {
		// On active side, if the ActivityTaskCancelRequested is invalid, it will created a RequestCancelActivityTaskFailed
		// Passive will rely on active side logic
		return nil
	}

	ai.Version = event.GetVersion()

	// - We have the activity dispatched to worker.
	// - The activity might not be heartbeat'ing, but the activity can still call RecordActivityHeartBeat()
	//   to see cancellation while reporting progress of the activity.
	ai.CancelRequested = true

	ai.CancelRequestID = event.GetEventId()
	e.updateActivityInfos[ai] = struct{}{}
	return nil
}

func (e *mutableStateBuilder) AddRequestCancelActivityTaskFailedEvent(
	decisionCompletedEventID int64,
	activityID string,
	cause string,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCancelFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	return e.hBuilder.AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID, activityID, cause), nil
}

func (e *mutableStateBuilder) AddActivityTaskCanceledEvent(
	scheduleEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details []byte,
	identity string,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedID != startedEventID {
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
			tag.WorkflowActivityID(ai.ActivityID),
			tag.WorkflowStartedID(ai.StartedID))
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
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCanceledEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *mutableStateBuilder) AddCompletedWorkflowEvent(
	decisionCompletedEventID int64,
	attributes *commonproto.CompleteWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddCompletedWorkflowEvent(decisionCompletedEventID, attributes)
	if err := e.ReplicateWorkflowExecutionCompletedEvent(decisionCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		e.unixNanoToTime(event.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCompletedEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCompleted,
		persistence.WorkflowCloseStatusCompleted,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchID = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddFailWorkflowEvent(
	decisionCompletedEventID int64,
	attributes *commonproto.FailWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddFailWorkflowEvent(decisionCompletedEventID, attributes)
	if err := e.ReplicateWorkflowExecutionFailedEvent(decisionCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		e.unixNanoToTime(event.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionFailedEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCompleted,
		persistence.WorkflowCloseStatusFailed,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchID = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddTimeoutWorkflowEvent(
	firstEventID int64,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowTimeout
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddTimeoutWorkflowEvent()
	if err := e.ReplicateWorkflowExecutionTimedoutEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		e.unixNanoToTime(event.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionTimedoutEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCompleted,
		persistence.WorkflowCloseStatusTimedOut,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchID = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionCancelRequestedEvent(
	cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCancelRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if e.executionInfo.CancelRequested {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowState(e.executionInfo.State),
			tag.Bool(e.executionInfo.CancelRequested),
			tag.Key(e.executionInfo.CancelRequestID),
		)
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddWorkflowExecutionCancelRequestedEvent(cause, request)
	if err := e.ReplicateWorkflowExecutionCancelRequestedEvent(event); err != nil {
		return nil, err
	}

	// Set the CancelRequestID on the active cluster.  This information is not part of the history event.
	e.executionInfo.CancelRequestID = request.CancelRequest.GetRequestId()
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCancelRequestedEvent(
	event *commonproto.HistoryEvent,
) error {

	e.executionInfo.CancelRequested = true
	return nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionCanceledEvent(
	decisionTaskCompletedEventID int64,
	attributes *commonproto.CancelWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)
	if err := e.ReplicateWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		e.unixNanoToTime(event.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionCanceledEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
) error {
	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCompleted,
		persistence.WorkflowCloseStatusCanceled,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchID = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	decisionCompletedEventID int64,
	cancelRequestID string,
	request *commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, *persistenceblobs.RequestCancelInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	rci, err := e.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, event, cancelRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateRequestCancelExternalTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, rci, nil
}

func (e *mutableStateBuilder) ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
	cancelRequestID string,
) (*persistenceblobs.RequestCancelInfo, error) {

	// TODO: Evaluate if we need cancelRequestID also part of history event
	initiatedEventID := event.GetEventId()
	rci := &persistenceblobs.RequestCancelInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchID: firstEventID,
		InitiatedID:           initiatedEventID,
		CancelRequestID:       cancelRequestID,
	}

	e.pendingRequestCancelInfoIDs[initiatedEventID] = rci
	e.updateRequestCancelInfos[rci] = struct{}{}

	return rci, nil
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionCancelRequested(
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
) (*commonproto.HistoryEvent, error) {

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

	event := e.hBuilder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	if err := e.ReplicateExternalWorkflowExecutionCancelRequested(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateExternalWorkflowExecutionCancelRequested(
	event *commonproto.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionCancelRequestedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingRequestCancel(initiatedID)
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID int64,
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
	cause enums.CancelExternalWorkflowExecutionFailedCause,
) (*commonproto.HistoryEvent, error) {

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

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID,
		domain, workflowID, runID, cause)
	if err := e.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *commonproto.HistoryEvent,
) error {

	initiatedID := event.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingRequestCancel(initiatedID)
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(
	decisionCompletedEventID int64,
	signalRequestID string,
	request *commonproto.SignalExternalWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, *persistenceblobs.SignalInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	si, err := e.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, event, signalRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateSignalExternalTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, si, nil
}

func (e *mutableStateBuilder) ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
	signalRequestID string,
) (*persistenceblobs.SignalInfo, error) {

	// TODO: Consider also writing signalRequestID to history event
	initiatedEventID := event.GetEventId()
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
	si := &persistenceblobs.SignalInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchID: firstEventID,
		InitiatedID:           initiatedEventID,
		RequestID:             signalRequestID,
		Name:                  attributes.GetSignalName(),
		Input:                 attributes.Input,
		Control:               attributes.Control,
	}

	e.pendingSignalInfoIDs[initiatedEventID] = si
	e.updateSignalInfos[si] = struct{}{}
	return si, nil
}

func (e *mutableStateBuilder) AddUpsertWorkflowSearchAttributesEvent(
	decisionCompletedEventID int64,
	request *commonproto.UpsertWorkflowSearchAttributesDecisionAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionUpsertWorkflowSearchAttributes
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddUpsertWorkflowSearchAttributesEvent(decisionCompletedEventID, request)
	e.ReplicateUpsertWorkflowSearchAttributesEvent(event)
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowSearchAttrTasks(
		e.unixNanoToTime(event.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateUpsertWorkflowSearchAttributesEvent(
	event *commonproto.HistoryEvent,
) {

	upsertSearchAttr := event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields()
	currentSearchAttr := e.GetExecutionInfo().SearchAttributes

	e.executionInfo.SearchAttributes = mergeMapOfByteArray(currentSearchAttr, upsertSearchAttr)
}

func mergeMapOfByteArray(
	current map[string][]byte,
	upsert map[string][]byte,
) map[string][]byte {

	if current == nil {
		current = make(map[string][]byte)
	}
	for k, v := range upsert {
		current[k] = v
	}
	return current
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionSignaled(
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
	control []byte,
) (*commonproto.HistoryEvent, error) {

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

	event := e.hBuilder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	if err := e.ReplicateExternalWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateExternalWorkflowExecutionSignaled(
	event *commonproto.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionSignaledEventAttributes().GetInitiatedEventId()

	return e.DeletePendingSignal(initiatedID)
}

func (e *mutableStateBuilder) AddSignalExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID int64,
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
	control []byte,
	cause enums.SignalExternalWorkflowExecutionFailedCause,
) (*commonproto.HistoryEvent, error) {

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

	event := e.hBuilder.AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID, domain,
		workflowID, runID, control, cause)
	if err := e.ReplicateSignalExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateSignalExternalWorkflowExecutionFailedEvent(
	event *commonproto.HistoryEvent,
) error {

	initiatedID := event.GetSignalExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingSignal(initiatedID)
}

func (e *mutableStateBuilder) AddTimerStartedEvent(
	decisionCompletedEventID int64,
	request *commonproto.StartTimerDecisionAttributes,
) (*commonproto.HistoryEvent, *persistenceblobs.TimerInfo, error) {

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

	event := e.hBuilder.AddTimerStartedEvent(decisionCompletedEventID, request)
	ti, err := e.ReplicateTimerStartedEvent(event)
	if err != nil {
		return nil, nil, err
	}
	return event, ti, err
}

func (e *mutableStateBuilder) ReplicateTimerStartedEvent(
	event *commonproto.HistoryEvent,
) (*persistenceblobs.TimerInfo, error) {

	attributes := event.GetTimerStartedEventAttributes()
	timerID := attributes.GetTimerId()

	startToFireTimeout := attributes.GetStartToFireTimeoutSeconds()
	fireTimeout := time.Duration(startToFireTimeout) * time.Second
	// TODO: Time skew need to be taken in to account.
	expiryTime, err := types.TimestampProto(time.Unix(0, event.GetTimestamp()).Add(fireTimeout)) // should use the event time, not now

	if err != nil {
		return nil, err
	}

	ti := &persistenceblobs.TimerInfo{
		Version:    event.GetVersion(),
		TimerID:    timerID,
		ExpiryTime: expiryTime,
		StartedID:  event.GetEventId(),
		TaskStatus: timerTaskStatusNone,
	}

	e.pendingTimerInfoIDs[timerID] = ti
	e.pendingTimerEventIDToID[event.GetEventId()] = timerID
	e.updateTimerInfos[ti] = struct{}{}

	return ti, nil
}

func (e *mutableStateBuilder) AddTimerFiredEvent(
	timerID string,
) (*commonproto.HistoryEvent, error) {

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
	event := e.hBuilder.AddTimerFiredEvent(timerInfo.StartedID, timerID)
	if err := e.ReplicateTimerFiredEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateTimerFiredEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetTimerFiredEventAttributes()
	timerID := attributes.GetTimerId()

	return e.DeleteUserTimer(timerID)
}

func (e *mutableStateBuilder) AddTimerCanceledEvent(
	decisionCompletedEventID int64,
	attributes *commonproto.CancelTimerDecisionAttributes,
	identity string,
) (*commonproto.HistoryEvent, error) {

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
		timerStartedID = ti.StartedID
	}

	// Timer is running.
	event := e.hBuilder.AddTimerCanceledEvent(timerStartedID, decisionCompletedEventID, timerID, identity)
	if ok {
		if err := e.ReplicateTimerCanceledEvent(event); err != nil {
			return nil, err
		}
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateTimerCanceledEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetTimerCanceledEventAttributes()
	timerID := attributes.GetTimerId()

	return e.DeleteUserTimer(timerID)
}

func (e *mutableStateBuilder) AddCancelTimerFailedEvent(
	decisionCompletedEventID int64,
	attributes *commonproto.CancelTimerDecisionAttributes,
	identity string,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionTimerCancelFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	// No Operation: We couldn't cancel it probably TIMER_ID_UNKNOWN
	timerID := attributes.GetTimerId()
	return e.hBuilder.AddCancelTimerFailedEvent(timerID, decisionCompletedEventID,
		timerCancellationMsgTimerIDUnknown, identity), nil
}

func (e *mutableStateBuilder) AddRecordMarkerEvent(
	decisionCompletedEventID int64,
	attributes *commonproto.RecordMarkerDecisionAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowRecordMarker
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	return e.hBuilder.AddMarkerRecordedEvent(decisionCompletedEventID, attributes), nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	reason string,
	details []byte,
	identity string,
) (*commonproto.HistoryEvent, error) {

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
		e.unixNanoToTime(event.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCompleted,
		persistence.WorkflowCloseStatusTerminated,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchID = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *mutableStateBuilder) AddWorkflowExecutionSignaled(
	signalName string,
	input []byte,
	identity string,
) (*commonproto.HistoryEvent, error) {

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
	event *commonproto.HistoryEvent,
) error {

	// Increment signal count in mutable state for this workflow execution
	e.executionInfo.SignalCount++
	return nil
}

func (e *mutableStateBuilder) AddContinueAsNewEvent(
	firstEventID int64,
	decisionCompletedEventID int64,
	parentDomainName string,
	attributes *commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, mutableState, error) {

	opTag := tag.WorkflowActionWorkflowContinueAsNew
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	var err error
	newRunID := uuid.New()
	newExecution := commonproto.WorkflowExecution{
		WorkflowId: e.executionInfo.WorkflowID,
		RunId:      newRunID,
	}

	// Extract ParentExecutionInfo from current run so it can be passed down to the next
	var parentInfo *commonproto.ParentExecutionInfo
	if e.HasParentExecution() {
		parentInfo = &commonproto.ParentExecutionInfo{
			DomainUUID: e.executionInfo.ParentDomainID,
			Domain:     parentDomainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: e.executionInfo.ParentWorkflowID,
				RunId:      e.executionInfo.ParentRunID,
			},
			InitiatedId: e.executionInfo.InitiatedID,
		}
	}

	continueAsNewEvent := e.hBuilder.AddContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)
	currentStartEvent, err := e.GetStartEvent()
	if err != nil {
		return nil, nil, err
	}
	firstRunID := currentStartEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstExecutionRunId()

	domainName := e.domainEntry.GetInfo().Name
	domainID := e.domainEntry.GetInfo().ID
	var newStateBuilder *mutableStateBuilder
	// If a workflow is ndc enabled, the continue as new should be ndc enabled.
	if e.config.EnableNDC(domainName) || e.GetVersionHistories() != nil {
		newStateBuilder = newMutableStateBuilderWithVersionHistories(
			e.shard,
			e.shard.GetEventsCache(),
			e.logger,
			e.domainEntry,
		)
	} else {
		if e.domainEntry.IsGlobalDomain() {
			// all workflows within a global domain should have replication state,
			// no matter whether it will be replicated to multiple
			// target clusters or not, for 2DC case
			newStateBuilder = newMutableStateBuilderWithReplicationState(
				e.shard,
				e.eventsCache,
				e.logger,
				e.domainEntry,
			)
		} else {
			newStateBuilder = newMutableStateBuilder(e.shard, e.eventsCache, e.logger, e.domainEntry)
		}
	}

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
		domainID,
		continueAsNewEvent,
	); err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateWorkflowCloseTasks(
		e.unixNanoToTime(continueAsNewEvent.GetTimestamp()),
	); err != nil {
		return nil, nil, err
	}

	return continueAsNewEvent, newStateBuilder, nil
}

func rolloverAutoResetPointsWithExpiringTime(
	resetPoints *commonproto.ResetPoints,
	prevRunID string,
	nowNano int64,
	domainRetentionDays int32,
) *commonproto.ResetPoints {

	if resetPoints == nil || resetPoints.Points == nil {
		return resetPoints
	}
	newPoints := make([]*commonproto.ResetPointInfo, 0, len(resetPoints.Points))
	expiringTimeNano := nowNano + int64(time.Duration(domainRetentionDays)*time.Hour*24)
	for _, rp := range resetPoints.Points {
		if rp.GetRunId() == prevRunID {
			rp.ExpiringTimeNano = expiringTimeNano
		}
		newPoints = append(newPoints, rp)
	}
	return &commonproto.ResetPoints{
		Points: newPoints,
	}
}

func (e *mutableStateBuilder) ReplicateWorkflowExecutionContinuedAsNewEvent(
	firstEventID int64,
	domainID string,
	continueAsNewEvent *commonproto.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateCompleted,
		persistence.WorkflowCloseStatusContinuedAsNew,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchID = firstEventID // Used when completion event needs to be loaded from database
	e.ClearStickyness()
	e.writeEventToCache(continueAsNewEvent)
	return nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	decisionCompletedEventID int64,
	createRequestID string,
	attributes *commonproto.StartChildWorkflowExecutionDecisionAttributes,
) (*commonproto.HistoryEvent, *persistence.ChildExecutionInfo, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)
	// Write the event to cache only on active cluster
	e.eventsCache.putEvent(e.executionInfo.DomainID, e.executionInfo.WorkflowID, e.executionInfo.RunID,
		event.GetEventId(), event)

	ci, err := e.ReplicateStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, event, createRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.generateChildWorkflowTasks(
		e.unixNanoToTime(event.GetTimestamp()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ci, nil
}

func (e *mutableStateBuilder) ReplicateStartChildWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *commonproto.HistoryEvent,
	createRequestID string,
) (*persistence.ChildExecutionInfo, error) {

	initiatedEventID := event.GetEventId()
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	ci := &persistence.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedID:           initiatedEventID,
		InitiatedEventBatchID: firstEventID,
		StartedID:             common.EmptyEventID,
		StartedWorkflowID:     attributes.GetWorkflowId(),
		CreateRequestID:       createRequestID,
		DomainName:            attributes.GetDomain(),
		WorkflowTypeName:      attributes.GetWorkflowType().GetName(),
		ParentClosePolicy:     attributes.GetParentClosePolicy(),
	}

	e.pendingChildExecutionInfoIDs[initiatedEventID] = ci
	e.updateChildExecutionInfos[ci] = struct{}{}

	return ci, nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionStartedEvent(
	domain string,
	execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType,
	initiatedID int64,
	header *commonproto.Header,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID, header)
	if err := e.ReplicateChildWorkflowExecutionStartedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionStartedEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	ci, _ := e.GetChildExecutionInfo(initiatedID)
	ci.StartedID = event.GetEventId()
	ci.StartedRunID = attributes.GetWorkflowExecution().GetRunId()
	e.updateChildExecutionInfos[ci] = struct{}{}

	return nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	cause enums.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiationFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != common.EmptyEventID {
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
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetStartChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	childExecution *commonproto.WorkflowExecution,
	attributes *commonproto.WorkflowExecutionCompletedEventAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonproto.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionCompletedEvent(ci.DomainName, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	if err := e.ReplicateChildWorkflowExecutionCompletedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionCompletedEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	childExecution *commonproto.WorkflowExecution,
	attributes *commonproto.WorkflowExecutionFailedEventAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(!ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonproto.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionFailedEvent(ci.DomainName, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	if err := e.ReplicateChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionFailedEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	childExecution *commonproto.WorkflowExecution,
	attributes *commonproto.WorkflowExecutionCanceledEventAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonproto.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionCanceledEvent(ci.DomainName, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	if err := e.ReplicateChildWorkflowExecutionCanceledEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionCanceledEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	childExecution *commonproto.WorkflowExecution,
	attributes *commonproto.WorkflowExecutionTerminatedEventAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowTerminated
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonproto.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionTerminatedEvent(ci.DomainName, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	if err := e.ReplicateChildWorkflowExecutionTerminatedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionTerminatedEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	childExecution *commonproto.WorkflowExecution,
	attributes *commonproto.WorkflowExecutionTimedOutEventAttributes,
) (*commonproto.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == common.EmptyEventID {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, e.createInternalServerError(opTag)
	}

	workflowType := &commonproto.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := e.hBuilder.AddChildWorkflowExecutionTimedOutEvent(ci.DomainName, childExecution, workflowType, ci.InitiatedID,
		ci.StartedID, attributes)
	if err := e.ReplicateChildWorkflowExecutionTimedOutEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *mutableStateBuilder) ReplicateChildWorkflowExecutionTimedOutEvent(
	event *commonproto.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *mutableStateBuilder) RetryActivity(
	ai *persistence.ActivityInfo,
	failureReason string,
	failureDetails []byte,
) (bool, error) {

	opTag := tag.WorkflowActionActivityTaskRetry
	if err := e.checkMutability(opTag); err != nil {
		return false, err
	}

	if !ai.HasRetryPolicy || ai.CancelRequested {
		return false, nil
	}

	now := e.timeSource.Now()

	backoffInterval := getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		failureReason,
		ai.NonRetriableErrors,
	)
	if backoffInterval == backoff.NoBackoff {
		return false, nil
	}

	// a retry is needed, update activity info for next retry
	ai.Version = e.GetCurrentVersion()
	ai.Attempt++
	ai.ScheduledTime = now.Add(backoffInterval) // update to next schedule time
	ai.StartedID = common.EmptyEventID
	ai.RequestID = ""
	ai.StartedTime = time.Time{}
	ai.TimerTaskStatus = timerTaskStatusNone
	ai.LastFailureReason = failureReason
	ai.LastWorkerIdentity = ai.StartedIdentity
	ai.LastFailureDetails = failureDetails

	if err := e.taskGenerator.generateActivityRetryTasks(
		ai.ScheduleID,
	); err != nil {
		return false, err
	}

	e.updateActivityInfos[ai] = struct{}{}
	e.syncActivityTasks[ai.ScheduleID] = struct{}{}
	return true, nil
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

func (e *mutableStateBuilder) GetWorkflowStateCloseStatus() (int, int) {

	executionInfo := e.executionInfo
	return executionInfo.State, int(executionInfo.CloseStatus)
}

func (e *mutableStateBuilder) UpdateWorkflowStateCloseStatus(
	state int,
	closeStatus int,
) error {

	return e.executionInfo.UpdateWorkflowStateCloseStatus(state, enums.WorkflowExecutionCloseStatus(closeStatus))
}

func (e *mutableStateBuilder) StartTransaction(
	domainEntry *cache.DomainCacheEntry,
) (bool, error) {

	e.domainEntry = domainEntry
	if err := e.UpdateCurrentVersion(domainEntry.GetFailoverVersion(), false); err != nil {
		return false, err
	}

	flushBeforeReady, err := e.startTransactionHandleDecisionFailover()
	if err != nil {
		return false, err
	}

	return flushBeforeReady, nil
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
	e.executionInfo.LastUpdatedTimestamp = now

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside workflowExecutionContext.resetWorkflowExecution
	// currently, the updates done inside workflowExecutionContext.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := e.generateChecksum()

	workflowMutation := &persistence.WorkflowMutation{
		ExecutionInfo:    e.executionInfo,
		ReplicationState: e.replicationState,
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
	e.executionInfo.LastUpdatedTimestamp = now

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside workflowExecutionContext.resetWorkflowExecution
	// currently, the updates done inside workflowExecutionContext.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := e.generateChecksum()

	workflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    e.executionInfo,
		ReplicationState: e.replicationState,
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

	e.updateActivityInfos = make(map[*persistence.ActivityInfo]struct{})
	e.deleteActivityInfos = make(map[int64]struct{})
	e.syncActivityTasks = make(map[int64]struct{})

	e.updateTimerInfos = make(map[*persistenceblobs.TimerInfo]struct{})
	e.deleteTimerInfos = make(map[string]struct{})

	e.updateChildExecutionInfos = make(map[*persistence.ChildExecutionInfo]struct{})
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
	e.stateInDB = e.executionInfo.State
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
			DomainID:    e.executionInfo.DomainID,
			WorkflowID:  e.executionInfo.WorkflowID,
			RunID:       e.executionInfo.RunID,
			BranchToken: currentBranchToken,
			Events:      e.hBuilder.transientHistory,
		})
	}
	if len(e.hBuilder.history) != 0 {
		workflowEventsSeq = append(workflowEventsSeq, &persistence.WorkflowEvents{
			DomainID:    e.executionInfo.DomainID,
			WorkflowID:  e.executionInfo.WorkflowID,
			RunID:       e.executionInfo.RunID,
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
	events []*commonproto.HistoryEvent,
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

	// TODO after NDC release and migration is done, remove this check
	if e.GetReplicationState() != nil {
		replicationTask.LastReplicationInfo = e.GetReplicationState().LastReplicationInfo
	} else if e.GetVersionHistories() != nil {
		replicationTask.LastReplicationInfo = nil
	} else {
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
	lastEvent *commonproto.HistoryEvent,
	transactionPolicy transactionPolicy,
) error {

	if transactionPolicy == transactionPolicyPassive {
		// already handled in state builder
		return nil
	}

	e.GetExecutionInfo().LastEventTaskID = lastEvent.GetTaskId()

	if e.replicationState != nil {
		e.UpdateReplicationStateLastEventID(lastEvent.GetVersion(), lastEvent.GetEventId())
	}

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
	lastFirstEvent *commonproto.HistoryEvent,
) {
	e.GetExecutionInfo().SetLastFirstEventID(lastFirstEvent.GetEventId())
}

func (e *mutableStateBuilder) canReplicateEvents() bool {
	return (e.GetReplicationState() != nil || e.GetVersionHistories() != nil) &&
		e.domainEntry.GetReplicationPolicy() == cache.ReplicationPolicyMultiCluster
}

// validateNoEventsAfterWorkflowFinish perform check on history event batch
// NOTE: do not apply this check on every batch, since transient
// decision && workflow finish will be broken (the first batch)
func (e *mutableStateBuilder) validateNoEventsAfterWorkflowFinish(
	transactionPolicy transactionPolicy,
	events []*commonproto.HistoryEvent,
) error {

	if transactionPolicy == transactionPolicyPassive ||
		len(events) == 0 {
		return nil
	}

	// only do check if workflow is finished
	if e.GetExecutionInfo().State != persistence.WorkflowStateCompleted {
		return nil
	}

	// workflow close
	// this will perform check on the last event of last batch
	// NOTE: do not apply this check on every batch, since transient
	// decision && workflow finish will be broken (the first batch)
	lastEvent := events[len(events)-1]
	switch lastEvent.GetEventType() {
	case enums.EventTypeWorkflowExecutionCompleted,
		enums.EventTypeWorkflowExecutionFailed,
		enums.EventTypeWorkflowExecutionTimedOut,
		enums.EventTypeWorkflowExecutionTerminated,
		enums.EventTypeWorkflowExecutionContinuedAsNew,
		enums.EventTypeWorkflowExecutionCanceled:
		return nil

	default:
		executionInfo := e.GetExecutionInfo()
		e.logError(
			"encounter case where events appears after workflow finish.",
			tag.WorkflowDomainID(executionInfo.DomainID),
			tag.WorkflowID(executionInfo.WorkflowID),
			tag.WorkflowRunID(executionInfo.RunID),
		)
		return ErrEventsAterWorkflowFinish
	}
}

func (e *mutableStateBuilder) startTransactionHandleDecisionFailover() (bool, error) {

	if !e.IsWorkflowExecutionRunning() ||
		!e.canReplicateEvents() {
		return false, nil
	}

	// NOTE:
	// the main idea here is to guarantee that once there is a decision task started
	// all events ending in the buffer should have the same version

	// Handling mutable state turn from standby to active, while having a decision on the fly
	decision, ok := e.GetInFlightDecision()
	if !ok || decision.Version >= e.GetCurrentVersion() {
		// no pending decision, no buffered events
		// or decision has higher / equal version
		return false, nil
	}

	currentVersion := e.GetCurrentVersion()
	lastWriteVersion, err := e.GetLastWriteVersion()
	if err != nil {
		return false, err
	}
	if lastWriteVersion != decision.Version {
		return false, serviceerror.NewInternal(fmt.Sprintf("mutableStateBuilder encounter mismatch version, decision: %v, last write version %v", decision.Version, lastWriteVersion))
	}

	lastWriteSourceCluster := e.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentVersionCluster := e.clusterMetadata.ClusterNameForFailoverVersion(currentVersion)
	currentCluster := e.clusterMetadata.GetCurrentClusterName()

	// there are 4 cases for version changes (based on version from domain cache)
	// NOTE: domain cache version change may occur after seeing events with higher version
	//  meaning that the flush buffer logic in NDC branch manager should be kept.
	//
	// 1. active -> passive => fail decision & flush buffer using last write version
	// 2. active -> active => fail decision & flush buffer using last write version
	// 3. passive -> active => fail decision using current version, no buffered events
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
	// the in flight decision must be failed to guarantee all events within same
	// event batch shard the same version
	if err := e.UpdateCurrentVersion(flushBufferVersion, true); err != nil {
		return false, err
	}

	// we have a decision on the fly with a lower version, fail it
	if err := failDecision(
		e,
		decision,
		enums.DecisionTaskFailedCauseFailoverCloseDecision,
	); err != nil {
		return false, err
	}

	err = scheduleDecision(e)
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
		domainID := e.GetExecutionInfo().DomainID
		return serviceerror.NewDomainNotActive(domainID, currentCluster, activeCluster)
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
	if decision, ok := e.GetInFlightDecision(); ok {
		// we have a decision on the fly with a lower version, fail it
		if err := failDecision(
			e,
			decision,
			enums.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return err
		}

		err := scheduleDecision(e)
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
	domainEntry, err := e.shard.GetDomainCache().GetDomainByID(executionInfo.DomainID)
	if err != nil {
		return err
	}
	if _, pt := FindAutoResetPoint(
		e.timeSource,
		&domainEntry.GetConfig().BadBinaries,
		e.GetExecutionInfo().AutoResetPoints,
	); pt != nil {
		if err := e.taskGenerator.generateWorkflowResetTasks(
			e.unixNanoToTime(now.UnixNano()),
		); err != nil {
			return err
		}
		e.logInfo("Auto-Reset task is scheduled",
			tag.WorkflowDomainName(domainEntry.GetInfo().Name),
			tag.WorkflowID(executionInfo.WorkflowID),
			tag.WorkflowRunID(executionInfo.RunID),
			tag.WorkflowResetBaseRunID(pt.GetRunId()),
			tag.WorkflowEventID(pt.GetFirstDecisionCompletedId()),
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
			tag.WorkflowState(e.executionInfo.State),
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
	if e.domainEntry == nil {
		return false
	}
	return rand.Intn(100) < e.config.MutableStateChecksumGenProbability(e.domainEntry.GetInfo().Name)
}

func (e *mutableStateBuilder) shouldVerifyChecksum() bool {
	if e.domainEntry == nil {
		return false
	}
	return rand.Intn(100) < e.config.MutableStateChecksumVerifyProbability(e.domainEntry.GetInfo().Name)
}

func (e *mutableStateBuilder) shouldInvalidateCheckum() bool {
	invalidateBeforeEpochSecs := int64(e.config.MutableStateChecksumInvalidateBefore())
	if invalidateBeforeEpochSecs > 0 {
		invalidateBefore := time.Unix(invalidateBeforeEpochSecs, 0)
		return e.executionInfo.LastUpdatedTimestamp.Before(invalidateBefore)
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

func (e *mutableStateBuilder) unixNanoToTime(
	timestampNanos int64,
) time.Time {

	return time.Unix(0, timestampNanos)
}

func (e *mutableStateBuilder) logInfo(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowID))
	tags = append(tags, tag.WorkflowRunID(e.executionInfo.RunID))
	tags = append(tags, tag.WorkflowDomainID(e.executionInfo.DomainID))
	e.logger.Info(msg, tags...)
}

func (e *mutableStateBuilder) logWarn(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowID))
	tags = append(tags, tag.WorkflowRunID(e.executionInfo.RunID))
	tags = append(tags, tag.WorkflowDomainID(e.executionInfo.DomainID))
	e.logger.Warn(msg, tags...)
}

func (e *mutableStateBuilder) logError(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowID))
	tags = append(tags, tag.WorkflowRunID(e.executionInfo.RunID))
	tags = append(tags, tag.WorkflowDomainID(e.executionInfo.DomainID))
	e.logger.Error(msg, tags...)
}

func (e *mutableStateBuilder) logDataInconsistency() {
	domainID := e.executionInfo.DomainID
	workflowID := e.executionInfo.WorkflowID
	runID := e.executionInfo.RunID

	e.logger.Error("encounter cassandra data inconsistency",
		tag.WorkflowDomainID(domainID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
	)
}
