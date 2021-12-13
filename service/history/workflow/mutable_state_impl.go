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

package workflow

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/proto"
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
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	emptyUUID = "emptyUuid"

	mutableStateInvalidHistoryActionMsg         = "invalid history builder state for action"
	mutableStateInvalidHistoryActionMsgTemplate = mutableStateInvalidHistoryActionMsg + ": %v, %v"
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
	// ErrMissingSignalInitiatedEvent indicates missing workflow signal initiated event
	ErrMissingSignalInitiatedEvent = serviceerror.NewInternal("unable to get signal initiated event")
)

type (
	MutableStateImpl struct {
		pendingActivityTimerHeartbeats map[int64]time.Time                    // Schedule Event ID -> LastHeartbeatTimeoutVisibilityInSeconds.
		pendingActivityInfoIDs         map[int64]*persistencespb.ActivityInfo // Schedule Event ID -> Activity Info.
		pendingActivityIDToEventID     map[string]int64                       // Activity ID -> Schedule Event ID of the activity.
		updateActivityInfos            map[int64]*persistencespb.ActivityInfo // Modified activities from last update.
		deleteActivityInfos            map[int64]struct{}                     // Deleted activities from last update.
		syncActivityTasks              map[int64]struct{}                     // Activity to be sync to remote

		pendingTimerInfoIDs     map[string]*persistencespb.TimerInfo // User Timer ID -> Timer Info.
		pendingTimerEventIDToID map[int64]string                     // User Timer Start Event ID -> User Timer ID.
		updateTimerInfos        map[string]*persistencespb.TimerInfo // Modified timers from last update.
		deleteTimerInfos        map[string]struct{}                  // Deleted timers from last update.

		pendingChildExecutionInfoIDs map[int64]*persistencespb.ChildExecutionInfo // Initiated Event ID -> Child Execution Info
		updateChildExecutionInfos    map[int64]*persistencespb.ChildExecutionInfo // Modified ChildExecution Infos since last update
		deleteChildExecutionInfos    map[int64]struct{}                           // Deleted ChildExecution Info since last update

		pendingRequestCancelInfoIDs map[int64]*persistencespb.RequestCancelInfo // Initiated Event ID -> RequestCancelInfo
		updateRequestCancelInfos    map[int64]*persistencespb.RequestCancelInfo // Modified RequestCancel Infos since last update, for persistence update
		deleteRequestCancelInfos    map[int64]struct{}                          // Deleted RequestCancel Info since last update, for persistence update

		pendingSignalInfoIDs map[int64]*persistencespb.SignalInfo // Initiated Event ID -> SignalInfo
		updateSignalInfos    map[int64]*persistencespb.SignalInfo // Modified SignalInfo since last update
		deleteSignalInfos    map[int64]struct{}                   // Deleted SignalInfo since last update

		pendingSignalRequestedIDs map[string]struct{} // Set of signaled requestIds
		updateSignalRequestedIDs  map[string]struct{} // Set of signaled requestIds since last update
		deleteSignalRequestedIDs  map[string]struct{} // Deleted signaled requestId

		executionInfo  *persistencespb.WorkflowExecutionInfo // Workflow mutable state info.
		executionState *persistencespb.WorkflowExecutionState

		hBuilder *HistoryBuilder

		// in memory only attributes
		// indicate the current version
		currentVersion int64
		// buffer events from DB
		bufferEventsInDB []*historypb.HistoryEvent
		// indicates the workflow state in DB, can be used to calculate
		// whether this workflow is pointed by current workflow record
		stateInDB enumsspb.WorkflowExecutionState
		// TODO deprecate nextEventIDInDB in favor of dbRecordVersion
		// indicates the next event ID in DB, for conditional update
		nextEventIDInDB int64
		// indicates the DB record version, for conditional update
		dbRecordVersion int64
		// namespace entry contains a snapshot of namespace
		// NOTE: do not use the failover version inside, use currentVersion above
		namespaceEntry *namespace.Namespace
		// record if a event has been applied to mutable state
		// TODO: persist this to db
		appliedEvents map[string]struct{}

		InsertTransferTasks    []tasks.Task
		InsertTimerTasks       []tasks.Task
		InsertReplicationTasks []tasks.Task
		InsertVisibilityTasks  []tasks.Task

		// do not rely on this, this is only updated on
		// Load() and closeTransactionXXX methods. So when
		// a transaction is in progress, this value will be
		// wrong. This exist primarily for visibility via CLI
		checksum *persistencespb.Checksum

		taskGenerator       TaskGenerator
		workflowTaskManager *workflowTaskStateMachine
		QueryRegistry       QueryRegistry

		shard           shard.Context
		clusterMetadata cluster.Metadata
		eventsCache     events.Cache
		config          *configs.Config
		timeSource      clock.TimeSource
		logger          log.Logger
		metricsClient   metrics.Client
	}
)

var _ MutableState = (*MutableStateImpl)(nil)

func NewMutableState(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	startTime time.Time,
) *MutableStateImpl {
	s := &MutableStateImpl{
		updateActivityInfos:            make(map[int64]*persistencespb.ActivityInfo),
		pendingActivityTimerHeartbeats: make(map[int64]time.Time),
		pendingActivityInfoIDs:         make(map[int64]*persistencespb.ActivityInfo),
		pendingActivityIDToEventID:     make(map[string]int64),
		deleteActivityInfos:            make(map[int64]struct{}),
		syncActivityTasks:              make(map[int64]struct{}),

		pendingTimerInfoIDs:     make(map[string]*persistencespb.TimerInfo),
		pendingTimerEventIDToID: make(map[int64]string),
		updateTimerInfos:        make(map[string]*persistencespb.TimerInfo),
		deleteTimerInfos:        make(map[string]struct{}),

		updateChildExecutionInfos:    make(map[int64]*persistencespb.ChildExecutionInfo),
		pendingChildExecutionInfoIDs: make(map[int64]*persistencespb.ChildExecutionInfo),
		deleteChildExecutionInfos:    make(map[int64]struct{}),

		updateRequestCancelInfos:    make(map[int64]*persistencespb.RequestCancelInfo),
		pendingRequestCancelInfoIDs: make(map[int64]*persistencespb.RequestCancelInfo),
		deleteRequestCancelInfos:    make(map[int64]struct{}),

		updateSignalInfos:    make(map[int64]*persistencespb.SignalInfo),
		pendingSignalInfoIDs: make(map[int64]*persistencespb.SignalInfo),
		deleteSignalInfos:    make(map[int64]struct{}),

		updateSignalRequestedIDs:  make(map[string]struct{}),
		pendingSignalRequestedIDs: make(map[string]struct{}),
		deleteSignalRequestedIDs:  make(map[string]struct{}),

		currentVersion:   namespaceEntry.FailoverVersion(),
		bufferEventsInDB: nil,
		stateInDB:        enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
		nextEventIDInDB:  common.FirstEventID,
		dbRecordVersion:  1,
		namespaceEntry:   namespaceEntry,
		appliedEvents:    make(map[string]struct{}),

		QueryRegistry: NewQueryRegistry(),

		shard:           shard,
		clusterMetadata: shard.GetClusterMetadata(),
		eventsCache:     eventsCache,
		config:          shard.GetConfig(),
		timeSource:      shard.GetTimeSource(),
		logger:          logger,
		metricsClient:   shard.GetMetricsClient(),
	}

	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleId: common.EmptyEventID,
		WorkflowTaskStartedId:  common.EmptyEventID,
		WorkflowTaskRequestId:  emptyUUID,
		WorkflowTaskTimeout:    timestamp.DurationFromSeconds(0),
		WorkflowTaskAttempt:    1,

		LastWorkflowTaskStartId: common.EmptyEventID,

		StartTime:        timestamp.TimePtr(startTime),
		VersionHistories: versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		ExecutionStats:   &persistencespb.ExecutionStats{HistorySize: 0},
	}
	s.executionState = &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}

	s.hBuilder = NewMutableHistoryBuilder(
		s.timeSource,
		s.shard.GenerateTransferTaskIDs,
		s.currentVersion,
		common.FirstEventID,
		s.bufferEventsInDB,
	)
	s.taskGenerator = NewTaskGenerator(shard.GetNamespaceRegistry(), s.logger, s)
	s.workflowTaskManager = newWorkflowTaskStateMachine(s)

	return s
}

func newMutableStateBuilderFromDB(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	dbRecord *persistencespb.WorkflowMutableState,
	dbRecordVersion int64,
) (*MutableStateImpl, error) {

	// startTime will be overridden by DB record
	startTime := time.Time{}
	mutableState := NewMutableState(shard, eventsCache, logger, namespaceEntry, startTime)

	if dbRecord.ActivityInfos != nil {
		mutableState.pendingActivityInfoIDs = dbRecord.ActivityInfos
	}
	for _, activityInfo := range dbRecord.ActivityInfos {
		mutableState.pendingActivityIDToEventID[activityInfo.ActivityId] = activityInfo.ScheduleId
		if (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedHeartbeat) > 0 {
			// Sets last pending timer heartbeat to year 2000.
			// This ensures at least one heartbeat task will be processed for the pending activity.
			mutableState.pendingActivityTimerHeartbeats[activityInfo.ScheduleId] = time.Unix(946684800, 0)
		}
	}

	if dbRecord.TimerInfos != nil {
		mutableState.pendingTimerInfoIDs = dbRecord.TimerInfos
	}
	for _, timerInfo := range dbRecord.TimerInfos {
		mutableState.pendingTimerEventIDToID[timerInfo.GetStartedId()] = timerInfo.GetTimerId()
	}

	if dbRecord.ChildExecutionInfos != nil {
		mutableState.pendingChildExecutionInfoIDs = dbRecord.ChildExecutionInfos
	}

	if dbRecord.RequestCancelInfos != nil {
		mutableState.pendingRequestCancelInfoIDs = dbRecord.RequestCancelInfos
	}

	if dbRecord.SignalInfos != nil {
		mutableState.pendingSignalInfoIDs = dbRecord.SignalInfos
	}

	mutableState.pendingSignalRequestedIDs = convert.StringSliceToSet(dbRecord.SignalRequestedIds)
	mutableState.executionState = dbRecord.ExecutionState
	mutableState.executionInfo = dbRecord.ExecutionInfo

	// Workflows created before 1.11 doesn't have ExecutionTime and it must be computed for backwards compatibility.
	// Remove this "if" block when it is ok to rely on executionInfo.ExecutionTime only (added 6/9/21).
	if mutableState.executionInfo.ExecutionTime == nil {
		startEvent, err := mutableState.GetStartEvent()
		if err != nil {
			return nil, err
		}
		backoffDuration := timestamp.DurationValue(startEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstWorkflowTaskBackoff())
		mutableState.executionInfo.ExecutionTime = timestamp.TimePtr(timestamp.TimeValue(mutableState.executionInfo.GetStartTime()).Add(backoffDuration))
	}

	mutableState.hBuilder = NewMutableHistoryBuilder(
		mutableState.timeSource,
		mutableState.shard.GenerateTransferTaskIDs,
		common.EmptyVersion,
		dbRecord.NextEventId,
		dbRecord.BufferedEvents,
	)

	mutableState.currentVersion = common.EmptyVersion
	mutableState.bufferEventsInDB = dbRecord.BufferedEvents
	mutableState.stateInDB = dbRecord.ExecutionState.State
	mutableState.nextEventIDInDB = dbRecord.NextEventId
	mutableState.dbRecordVersion = dbRecordVersion
	mutableState.checksum = dbRecord.Checksum

	if len(dbRecord.Checksum.GetValue()) > 0 {
		switch {
		case mutableState.shouldInvalidateCheckum():
			mutableState.checksum = nil
			mutableState.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.MutableStateChecksumInvalidated)
		case mutableState.shouldVerifyChecksum():
			if err := verifyMutableStateChecksum(mutableState, dbRecord.Checksum); err != nil {
				// we ignore checksum verification errors for now until this
				// feature is tested and/or we have mechanisms in place to deal
				// with these types of errors
				mutableState.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.MutableStateChecksumMismatch)
				mutableState.logError("mutable state checksum mismatch", tag.Error(err))
			}
		}
	}

	return mutableState, nil
}

func (e *MutableStateImpl) CloneToProto() *persistencespb.WorkflowMutableState {
	ms := &persistencespb.WorkflowMutableState{
		ActivityInfos:       e.pendingActivityInfoIDs,
		TimerInfos:          e.pendingTimerInfoIDs,
		ChildExecutionInfos: e.pendingChildExecutionInfoIDs,
		RequestCancelInfos:  e.pendingRequestCancelInfoIDs,
		SignalInfos:         e.pendingSignalInfoIDs,
		SignalRequestedIds:  convert.StringSetToSlice(e.pendingSignalRequestedIDs),
		ExecutionInfo:       e.executionInfo,
		ExecutionState:      e.executionState,
		NextEventId:         e.hBuilder.NextEventID(),
		BufferedEvents:      e.bufferEventsInDB,
		Checksum:            e.checksum,
	}

	return proto.Clone(ms).(*persistencespb.WorkflowMutableState)
}

func (e *MutableStateImpl) GetWorkflowKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(
		e.executionInfo.NamespaceId,
		e.executionInfo.WorkflowId,
		e.executionState.RunId,
	)
}

func (e *MutableStateImpl) GetCurrentBranchToken() ([]byte, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}
	return currentVersionHistory.GetBranchToken(), nil
}

func (e *MutableStateImpl) getCurrentBranchTokenAndEventVersion(eventID int64) ([]byte, int64, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
	if err != nil {
		return nil, 0, err
	}
	version, err := versionhistory.GetVersionHistoryEventVersion(currentVersionHistory, eventID)
	if err != nil {
		return nil, 0, err
	}
	return currentVersionHistory.GetBranchToken(), version, nil
}

// SetHistoryTree set treeID/historyBranches
func (e *MutableStateImpl) SetHistoryTree(
	treeID string,
) error {

	initialBranchToken, err := persistence.NewHistoryBranchToken(treeID)
	if err != nil {
		return err
	}
	return e.SetCurrentBranchToken(initialBranchToken)
}

func (e *MutableStateImpl) SetCurrentBranchToken(
	branchToken []byte,
) error {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
	if err != nil {
		return err
	}
	versionhistory.SetVersionHistoryBranchToken(currentVersionHistory, branchToken)
	return nil
}

func (e *MutableStateImpl) SetHistoryBuilder(hBuilder *HistoryBuilder) {
	e.hBuilder = hBuilder
}

func (e *MutableStateImpl) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	return e.executionInfo
}

func (e *MutableStateImpl) GetExecutionState() *persistencespb.WorkflowExecutionState {
	return e.executionState
}

func (e *MutableStateImpl) FlushBufferedEvents() {
	if e.HasInFlightWorkflowTask() {
		return
	}
	e.updatePendingEventIDs(e.hBuilder.FlushBufferToCurrentBatch())
}

func (e *MutableStateImpl) UpdateCurrentVersion(
	version int64,
	forceUpdate bool,
) error {

	if state, _ := e.GetWorkflowStateStatus(); state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// always set current version to last write version when workflow is completed
		lastWriteVersion, err := e.GetLastWriteVersion()
		if err != nil {
			return err
		}
		e.currentVersion = lastWriteVersion
		return nil
	}

	versionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
	if err != nil {
		return err
	}

	if !versionhistory.IsEmptyVersionHistory(versionHistory) {
		// this make sure current version >= last write version
		versionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
		if err != nil {
			return err
		}
		e.currentVersion = versionHistoryItem.GetVersion()
	}

	if version > e.currentVersion || forceUpdate {
		e.currentVersion = version
	}

	e.hBuilder = NewMutableHistoryBuilder(
		e.timeSource,
		e.shard.GenerateTransferTaskIDs,
		e.currentVersion,
		e.nextEventIDInDB,
		e.bufferEventsInDB,
	)

	return nil
}

func (e *MutableStateImpl) GetCurrentVersion() int64 {

	if e.executionInfo.VersionHistories != nil {
		return e.currentVersion
	}

	return common.EmptyVersion
}

func (e *MutableStateImpl) GetStartVersion() (int64, error) {

	if e.executionInfo.VersionHistories != nil {
		versionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
		if err != nil {
			return 0, err
		}
		firstItem, err := versionhistory.GetFirstVersionHistoryItem(versionHistory)
		if err != nil {
			return 0, err
		}
		return firstItem.GetVersion(), nil
	}

	return common.EmptyVersion, nil
}

func (e *MutableStateImpl) GetLastWriteVersion() (int64, error) {

	if e.executionInfo.VersionHistories != nil {
		versionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
		if err != nil {
			return 0, err
		}
		lastItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
		if err != nil {
			return 0, err
		}
		return lastItem.GetVersion(), nil
	}

	return common.EmptyVersion, nil
}

func (e *MutableStateImpl) IsCurrentWorkflowGuaranteed() bool {
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
		panic(fmt.Sprintf("unknown workflow state: %v", e.executionState.State))
	}
}

func (e *MutableStateImpl) GetNamespaceEntry() *namespace.Namespace {
	return e.namespaceEntry
}

func (e *MutableStateImpl) IsStickyTaskQueueEnabled() bool {
	return e.executionInfo.StickyTaskQueue != ""
}

func (e *MutableStateImpl) GetWorkflowType() *commonpb.WorkflowType {
	wType := &commonpb.WorkflowType{}
	wType.Name = e.executionInfo.WorkflowTypeName

	return wType
}

func (e *MutableStateImpl) GetQueryRegistry() QueryRegistry {
	return e.QueryRegistry
}

func (e *MutableStateImpl) GetActivityScheduledEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {

	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, ErrMissingActivityInfo
	}

	currentBranchToken, version, err := e.getCurrentBranchTokenAndEventVersion(ai.ScheduleId)
	if err != nil {
		return nil, err
	}
	event, err := e.eventsCache.GetEvent(
		events.EventKey{
			NamespaceID: namespace.ID(e.executionInfo.NamespaceId),
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			EventID:     ai.ScheduleId,
			Version:     version,
		},
		ai.ScheduledEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingActivityScheduledEvent
	}
	return event, nil
}

// GetActivityInfo gives details about an activity that is currently in progress.
func (e *MutableStateImpl) GetActivityInfo(
	scheduleEventID int64,
) (*persistencespb.ActivityInfo, bool) {

	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	return ai, ok
}

// GetActivityInfoWithTimerHeartbeat gives details about an activity that is currently in progress.
func (e *MutableStateImpl) GetActivityInfoWithTimerHeartbeat(
	scheduleEventID int64,
) (*persistencespb.ActivityInfo, time.Time, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	timerVis, ok := e.pendingActivityTimerHeartbeats[scheduleEventID]

	return ai, timerVis, ok
}

// GetActivityByActivityID gives details about an activity that is currently in progress.
func (e *MutableStateImpl) GetActivityByActivityID(
	activityID string,
) (*persistencespb.ActivityInfo, bool) {

	eventID, ok := e.pendingActivityIDToEventID[activityID]
	if !ok {
		return nil, false
	}
	return e.GetActivityInfo(eventID)
}

// GetChildExecutionInfo gives details about a child execution that is currently in progress.
func (e *MutableStateImpl) GetChildExecutionInfo(
	initiatedEventID int64,
) (*persistencespb.ChildExecutionInfo, bool) {

	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	return ci, ok
}

// GetChildExecutionInitiatedEvent reads out the ChildExecutionInitiatedEvent from mutable state for in-progress child
// executions
func (e *MutableStateImpl) GetChildExecutionInitiatedEvent(
	initiatedEventID int64,
) (*historypb.HistoryEvent, error) {

	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingChildWorkflowInfo
	}

	currentBranchToken, version, err := e.getCurrentBranchTokenAndEventVersion(ci.InitiatedId)
	if err != nil {
		return nil, err
	}
	event, err := e.eventsCache.GetEvent(
		events.EventKey{
			NamespaceID: namespace.ID(e.executionInfo.NamespaceId),
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			EventID:     ci.InitiatedId,
			Version:     version,
		},
		ci.InitiatedEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingChildWorkflowInitiatedEvent
	}
	return event, nil
}

// GetRequestCancelInfo gives details about a request cancellation that is currently in progress.
func (e *MutableStateImpl) GetRequestCancelInfo(
	initiatedEventID int64,
) (*persistencespb.RequestCancelInfo, bool) {

	ri, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	return ri, ok
}

func (e *MutableStateImpl) GetRetryBackoffDuration(
	failure *failurepb.Failure,
) (time.Duration, enumspb.RetryState) {

	info := e.executionInfo
	if !info.HasRetryPolicy {
		return backoff.NoBackoff, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
	}

	return getBackoffInterval(
		e.timeSource.Now(),
		info.Attempt,
		info.RetryMaximumAttempts,
		info.RetryInitialInterval,
		info.RetryMaximumInterval,
		info.WorkflowExecutionExpirationTime,
		info.RetryBackoffCoefficient,
		failure,
		info.RetryNonRetryableErrorTypes,
	)
}

func (e *MutableStateImpl) GetCronBackoffDuration() time.Duration {
	if e.executionInfo.CronSchedule == "" {
		return backoff.NoBackoff
	}
	executionTime := timestamp.TimeValue(e.GetExecutionInfo().GetExecutionTime())
	return backoff.GetBackoffForNextSchedule(e.executionInfo.CronSchedule, executionTime, e.timeSource.Now())
}

// GetSignalInfo get the details about a signal request that is currently in progress.
func (e *MutableStateImpl) GetSignalInfo(
	initiatedEventID int64,
) (*persistencespb.SignalInfo, bool) {

	ri, ok := e.pendingSignalInfoIDs[initiatedEventID]
	return ri, ok
}

// GetSignalExternalInitiatedEvent get the details about signal external workflow
func (e *MutableStateImpl) GetSignalExternalInitiatedEvent(
	initiatedEventID int64,
) (*historypb.HistoryEvent, error) {
	si, ok := e.pendingSignalInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingSignalInfo
	}

	currentBranchToken, version, err := e.getCurrentBranchTokenAndEventVersion(si.InitiatedId)
	if err != nil {
		return nil, err
	}
	event, err := e.eventsCache.GetEvent(
		events.EventKey{
			NamespaceID: namespace.ID(e.executionInfo.NamespaceId),
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			EventID:     si.InitiatedId,
			Version:     version,
		},
		si.InitiatedEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingSignalInitiatedEvent
	}
	return event, nil
}

// GetCompletionEvent retrieves the workflow completion event from mutable state
func (e *MutableStateImpl) GetCompletionEvent() (*historypb.HistoryEvent, error) {
	if e.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil, ErrMissingWorkflowCompletionEvent
	}

	// Completion EventID is always one less than NextEventID after workflow is completed
	completionEventID := e.hBuilder.NextEventID() - 1
	firstEventID := e.executionInfo.CompletionEventBatchId

	currentBranchToken, version, err := e.getCurrentBranchTokenAndEventVersion(completionEventID)
	if err != nil {
		return nil, err
	}

	event, err := e.eventsCache.GetEvent(
		events.EventKey{
			NamespaceID: namespace.ID(e.executionInfo.NamespaceId),
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			EventID:     completionEventID,
			Version:     version,
		},
		firstEventID,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingWorkflowCompletionEvent
	}
	return event, nil
}

// GetStartEvent retrieves the workflow start event from mutable state
func (e *MutableStateImpl) GetStartEvent() (*historypb.HistoryEvent, error) {

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	startVersion, err := e.GetStartVersion()
	if err != nil {
		return nil, err
	}

	event, err := e.eventsCache.GetEvent(
		events.EventKey{
			NamespaceID: namespace.ID(e.executionInfo.NamespaceId),
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			EventID:     common.FirstEventID,
			Version:     startVersion,
		},
		common.FirstEventID,
		currentBranchToken,
	)
	if err != nil {
		// do not return the original error
		// since original error can be of type entity not exists
		// which can cause task processing side to fail silently
		return nil, ErrMissingWorkflowStartEvent
	}
	return event, nil
}

func (e *MutableStateImpl) GetFirstRunID() (string, error) {
	firstRunID := e.executionInfo.FirstExecutionRunId
	// This is needed for backwards compatibility.  Workflow execution create with Temporal release v0.28.0 or earlier
	// does not have FirstExecutionRunID stored as part of mutable state.  If this is not set then load it from
	// workflow execution started event.
	if len(firstRunID) != 0 {
		return firstRunID, nil
	}
	currentStartEvent, err := e.GetStartEvent()
	if err != nil {
		return "", err
	}
	return currentStartEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstExecutionRunId(), nil
}

// DeletePendingChildExecution deletes details about a ChildExecutionInfo.
func (e *MutableStateImpl) DeletePendingChildExecution(
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

	delete(e.updateChildExecutionInfos, initiatedEventID)
	e.deleteChildExecutionInfos[initiatedEventID] = struct{}{}
	return nil
}

// DeletePendingRequestCancel deletes details about a RequestCancelInfo.
func (e *MutableStateImpl) DeletePendingRequestCancel(
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

	delete(e.updateRequestCancelInfos, initiatedEventID)
	e.deleteRequestCancelInfos[initiatedEventID] = struct{}{}
	return nil
}

// DeletePendingSignal deletes details about a SignalInfo
func (e *MutableStateImpl) DeletePendingSignal(
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

	delete(e.updateSignalInfos, initiatedEventID)
	e.deleteSignalInfos[initiatedEventID] = struct{}{}
	return nil
}

func (e *MutableStateImpl) writeEventToCache(
	event *historypb.HistoryEvent,
) {
	// For start event: store it within events cache so the recordWorkflowStarted transfer task doesn't need to
	// load it from database
	// For completion event: store it within events cache so we can communicate the result to parent execution
	// during the processing of DeleteTransferTask without loading this event from database
	e.eventsCache.PutEvent(
		events.EventKey{
			NamespaceID: namespace.ID(e.executionInfo.NamespaceId),
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			EventID:     event.GetEventId(),
			Version:     event.GetVersion(),
		},
		event,
	)
}

func (e *MutableStateImpl) HasParentExecution() bool {
	return e.executionInfo.ParentNamespaceId != "" && e.executionInfo.ParentWorkflowId != ""
}

func (e *MutableStateImpl) UpdateActivityProgress(
	ai *persistencespb.ActivityInfo,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) {
	ai.Version = e.GetCurrentVersion()
	ai.LastHeartbeatDetails = request.Details
	now := e.timeSource.Now()
	ai.LastHeartbeatUpdateTime = &now
	e.updateActivityInfos[ai.ScheduleId] = ai
	e.syncActivityTasks[ai.ScheduleId] = struct{}{}
}

// ReplicateActivityInfo replicate the necessary activity information
func (e *MutableStateImpl) ReplicateActivityInfo(
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
		ai.TimerTaskStatus = TimerTaskStatusNone
	}

	e.updateActivityInfos[ai.ScheduleId] = ai
	return nil
}

// UpdateActivity updates an activity
func (e *MutableStateImpl) UpdateActivity(
	ai *persistencespb.ActivityInfo,
) error {

	if _, ok := e.pendingActivityInfoIDs[ai.ScheduleId]; !ok {
		e.logError(
			fmt.Sprintf("unable to find activity ID: %v in mutable state", ai.ActivityId),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	e.pendingActivityInfoIDs[ai.ScheduleId] = ai
	e.updateActivityInfos[ai.ScheduleId] = ai
	return nil
}

// UpdateActivityWithTimerHeartbeat updates an activity
func (e *MutableStateImpl) UpdateActivityWithTimerHeartbeat(
	ai *persistencespb.ActivityInfo,
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
func (e *MutableStateImpl) DeleteActivity(
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

	delete(e.updateActivityInfos, scheduleEventID)
	e.deleteActivityInfos[scheduleEventID] = struct{}{}
	return nil
}

// GetUserTimerInfo gives details about a user timer.
func (e *MutableStateImpl) GetUserTimerInfo(
	timerID string,
) (*persistencespb.TimerInfo, bool) {

	timerInfo, ok := e.pendingTimerInfoIDs[timerID]
	return timerInfo, ok
}

// GetUserTimerInfoByEventID gives details about a user timer.
func (e *MutableStateImpl) GetUserTimerInfoByEventID(
	startEventID int64,
) (*persistencespb.TimerInfo, bool) {

	timerID, ok := e.pendingTimerEventIDToID[startEventID]
	if !ok {
		return nil, false
	}
	return e.GetUserTimerInfo(timerID)
}

// UpdateUserTimer updates the user timer in progress.
func (e *MutableStateImpl) UpdateUserTimer(
	ti *persistencespb.TimerInfo,
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

	e.pendingTimerInfoIDs[ti.TimerId] = ti
	e.updateTimerInfos[ti.TimerId] = ti
	return nil
}

// DeleteUserTimer deletes an user timer.
func (e *MutableStateImpl) DeleteUserTimer(
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

	delete(e.updateTimerInfos, timerID)
	e.deleteTimerInfos[timerID] = struct{}{}
	return nil
}

// nolint:unused
func (e *MutableStateImpl) getWorkflowTaskInfo() *WorkflowTaskInfo {

	taskQueue := &taskqueuepb.TaskQueue{}
	if e.IsStickyTaskQueueEnabled() {
		taskQueue.Name = e.executionInfo.StickyTaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		taskQueue.Name = e.executionInfo.TaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_NORMAL
	}

	return &WorkflowTaskInfo{
		Version:               e.executionInfo.WorkflowTaskVersion,
		ScheduleID:            e.executionInfo.WorkflowTaskScheduleId,
		StartedID:             e.executionInfo.WorkflowTaskStartedId,
		RequestID:             e.executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:   e.executionInfo.WorkflowTaskTimeout,
		Attempt:               e.executionInfo.WorkflowTaskAttempt,
		StartedTime:           e.executionInfo.WorkflowTaskStartedTime,
		ScheduledTime:         e.executionInfo.WorkflowTaskScheduledTime,
		TaskQueue:             taskQueue,
		OriginalScheduledTime: e.executionInfo.WorkflowTaskOriginalScheduledTime,
	}
}

// GetWorkflowTaskInfo returns details about the in-progress workflow task
func (e *MutableStateImpl) GetWorkflowTaskInfo(
	scheduleEventID int64,
) (*WorkflowTaskInfo, bool) {
	return e.workflowTaskManager.GetWorkflowTaskInfo(scheduleEventID)
}

func (e *MutableStateImpl) GetPendingActivityInfos() map[int64]*persistencespb.ActivityInfo {
	return e.pendingActivityInfoIDs
}

func (e *MutableStateImpl) GetPendingTimerInfos() map[string]*persistencespb.TimerInfo {
	return e.pendingTimerInfoIDs
}

func (e *MutableStateImpl) GetPendingChildExecutionInfos() map[int64]*persistencespb.ChildExecutionInfo {
	return e.pendingChildExecutionInfoIDs
}

func (e *MutableStateImpl) GetPendingRequestCancelExternalInfos() map[int64]*persistencespb.RequestCancelInfo {
	return e.pendingRequestCancelInfoIDs
}

func (e *MutableStateImpl) GetPendingSignalExternalInfos() map[int64]*persistencespb.SignalInfo {
	return e.pendingSignalInfoIDs
}

func (e *MutableStateImpl) HasProcessedOrPendingWorkflowTask() bool {
	return e.workflowTaskManager.HasProcessedOrPendingWorkflowTask()
}

func (e *MutableStateImpl) HasPendingWorkflowTask() bool {
	return e.workflowTaskManager.HasPendingWorkflowTask()
}

func (e *MutableStateImpl) GetPendingWorkflowTask() (*WorkflowTaskInfo, bool) {
	return e.workflowTaskManager.GetPendingWorkflowTask()
}

func (e *MutableStateImpl) HasInFlightWorkflowTask() bool {
	return e.workflowTaskManager.HasInFlightWorkflowTask()
}

func (e *MutableStateImpl) GetInFlightWorkflowTask() (*WorkflowTaskInfo, bool) {
	return e.workflowTaskManager.GetInFlightWorkflowTask()
}

func (e *MutableStateImpl) HasBufferedEvents() bool {
	return e.hBuilder.HasBufferEvents()
}

// UpdateWorkflowTask updates a workflow task.
func (e *MutableStateImpl) UpdateWorkflowTask(
	workflowTask *WorkflowTaskInfo,
) {
	e.workflowTaskManager.UpdateWorkflowTask(workflowTask)
}

// DeleteWorkflowTask deletes a workflow task.
func (e *MutableStateImpl) DeleteWorkflowTask() {
	e.workflowTaskManager.DeleteWorkflowTask()
}

func (e *MutableStateImpl) ClearStickyness() {
	e.executionInfo.StickyTaskQueue = ""
	e.executionInfo.StickyScheduleToStartTimeout = timestamp.DurationFromSeconds(0)
}

// GetLastFirstEventIDTxnID returns last first event ID and corresponding transaction ID
// first event ID is the ID of a batch of events in a single history events record
func (e *MutableStateImpl) GetLastFirstEventIDTxnID() (int64, int64) {
	return e.executionInfo.LastFirstEventId, e.executionInfo.LastFirstEventTxnId
}

// GetNextEventID returns next event ID
func (e *MutableStateImpl) GetNextEventID() int64 {
	return e.hBuilder.NextEventID()
}

// GetPreviousStartedEventID returns last started workflow task event ID
func (e *MutableStateImpl) GetPreviousStartedEventID() int64 {
	return e.executionInfo.LastWorkflowTaskStartId
}

func (e *MutableStateImpl) IsWorkflowExecutionRunning() bool {
	switch e.executionState.State {
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
		panic(fmt.Sprintf("unknown workflow state: %v", e.executionState.State))
	}
}

func (e *MutableStateImpl) IsCancelRequested() bool {
	return e.executionInfo.CancelRequested
}

func (e *MutableStateImpl) IsSignalRequested(
	requestID string,
) bool {

	if _, ok := e.pendingSignalRequestedIDs[requestID]; ok {
		return true
	}
	return false
}

func (e *MutableStateImpl) AddSignalRequested(
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

func (e *MutableStateImpl) DeleteSignalRequested(
	requestID string,
) {

	delete(e.pendingSignalRequestedIDs, requestID)
	delete(e.updateSignalRequestedIDs, requestID)
	e.deleteSignalRequestedIDs[requestID] = struct{}{}
}

func (e *MutableStateImpl) addWorkflowExecutionStartedEventForContinueAsNew(
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	execution commonpb.WorkflowExecution,
	previousExecutionState MutableState,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
	firstRunID string,
) (*historypb.HistoryEvent, error) {

	previousExecutionInfo := previousExecutionState.GetExecutionInfo()
	taskQueue := previousExecutionInfo.TaskQueue
	if command.TaskQueue != nil {
		taskQueue = command.TaskQueue.GetName()
	}
	tq := &taskqueuepb.TaskQueue{
		Name: taskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	workflowType := previousExecutionInfo.WorkflowTypeName
	if command.WorkflowType != nil {
		workflowType = command.WorkflowType.GetName()
	}
	wType := &commonpb.WorkflowType{}
	wType.Name = workflowType

	var taskTimeout *time.Duration
	if timestamp.DurationValue(command.GetWorkflowTaskTimeout()) == 0 {
		taskTimeout = previousExecutionInfo.DefaultWorkflowTaskTimeout
	} else {
		taskTimeout = command.GetWorkflowTaskTimeout()
	}

	// Workflow runTimeout is already set to the correct value in
	// validateContinueAsNewWorkflowExecutionAttributes
	runTimeout := command.GetWorkflowRunTimeout()

	createRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.New(),
		Namespace:                e.namespaceEntry.Name().String(),
		WorkflowId:               execution.WorkflowId,
		TaskQueue:                tq,
		WorkflowType:             wType,
		WorkflowExecutionTimeout: previousExecutionState.GetExecutionInfo().WorkflowExecutionTimeout,
		WorkflowRunTimeout:       runTimeout,
		WorkflowTaskTimeout:      taskTimeout,
		Input:                    command.Input,
		Header:                   command.Header,
		RetryPolicy:              command.RetryPolicy,
		CronSchedule:             command.CronSchedule,
		Memo:                     command.Memo,
		SearchAttributes:         command.SearchAttributes,
	}

	enums.SetDefaultContinueAsNewInitiator(&command.Initiator)

	req := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:              e.namespaceEntry.ID().String(),
		StartRequest:             createRequest,
		ParentExecutionInfo:      parentExecutionInfo,
		LastCompletionResult:     command.LastCompletionResult,
		ContinuedFailure:         command.GetFailure(),
		ContinueAsNewInitiator:   command.Initiator,
		FirstWorkflowTaskBackoff: command.BackoffStartInterval,
	}
	if command.GetInitiator() == enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY {
		req.Attempt = previousExecutionState.GetExecutionInfo().Attempt + 1
	} else {
		req.Attempt = 1
	}
	workflowTimeoutTime := timestamp.TimeValue(previousExecutionState.GetExecutionInfo().WorkflowExecutionExpirationTime)
	if !workflowTimeoutTime.IsZero() {
		req.WorkflowExecutionExpirationTime = &workflowTimeoutTime
	}

	event, err := e.AddWorkflowExecutionStartedEventWithOptions(
		execution,
		req,
		namespace.ID(parentExecutionInfo.GetNamespaceId()),
		previousExecutionInfo.AutoResetPoints,
		previousExecutionState.GetExecutionState().GetRunId(),
		firstRunID,
	)
	if err != nil {
		return nil, err
	}
	if err = e.AddFirstWorkflowTaskScheduled(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *MutableStateImpl) AddWorkflowExecutionStartedEvent(
	execution commonpb.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (*historypb.HistoryEvent, error) {

	return e.AddWorkflowExecutionStartedEventWithOptions(
		execution,
		startRequest,
		namespace.ID(startRequest.ParentExecutionInfo.GetNamespaceId()),
		nil, // resetPoints
		"",  // prevRunID
		execution.GetRunId(),
	)
}

func (e *MutableStateImpl) AddWorkflowExecutionStartedEventWithOptions(
	execution commonpb.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	parentNamespaceID namespace.ID,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	eventID := e.GetNextEventID()
	if eventID != common.FirstEventID {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(eventID),
			tag.ErrorTypeInvalidHistoryAction)
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddWorkflowExecutionStartedEvent(
		*e.executionInfo.StartTime,
		startRequest,
		resetPoints,
		prevRunID,
		firstRunID,
		execution.GetRunId(),
	)
	if err := e.ReplicateWorkflowExecutionStartedEvent(
		parentNamespaceID,
		execution,
		startRequest.StartRequest.GetRequestId(),
		event,
	); err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowStartTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, err
	}
	if err := e.taskGenerator.GenerateRecordWorkflowStartedTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionStartedEvent(
	parentNamespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
	requestID string,
	startEvent *historypb.HistoryEvent,
) error {

	event := startEvent.GetWorkflowExecutionStartedEventAttributes()
	e.executionState.CreateRequestId = requestID
	e.executionState.RunId = execution.GetRunId()
	e.executionInfo.NamespaceId = e.namespaceEntry.ID().String()
	e.executionInfo.WorkflowId = execution.GetWorkflowId()
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
	e.executionInfo.LastWorkflowTaskStartId = common.EmptyEventID
	e.executionInfo.LastFirstEventId = startEvent.GetEventId()

	e.executionInfo.WorkflowTaskVersion = common.EmptyVersion
	e.executionInfo.WorkflowTaskScheduleId = common.EmptyEventID
	e.executionInfo.WorkflowTaskStartedId = common.EmptyEventID
	e.executionInfo.WorkflowTaskRequestId = emptyUUID
	e.executionInfo.WorkflowTaskTimeout = timestamp.DurationFromSeconds(0)

	e.executionInfo.CronSchedule = event.GetCronSchedule()
	e.executionInfo.ParentNamespaceId = parentNamespaceID.String()

	if event.ParentWorkflowExecution != nil {
		e.executionInfo.ParentWorkflowId = event.ParentWorkflowExecution.GetWorkflowId()
		e.executionInfo.ParentRunId = event.ParentWorkflowExecution.GetRunId()
	}

	if event.ParentInitiatedEventId != 0 {
		e.executionInfo.InitiatedId = event.GetParentInitiatedEventId()
	} else {
		e.executionInfo.InitiatedId = common.EmptyEventID
	}

	e.executionInfo.ExecutionTime = timestamp.TimePtr(
		e.executionInfo.StartTime.Add(timestamp.DurationValue(event.GetFirstWorkflowTaskBackoff())),
	)

	e.executionInfo.Attempt = event.GetAttempt()
	if !timestamp.TimeValue(event.GetWorkflowExecutionExpirationTime()).IsZero() {
		e.executionInfo.WorkflowExecutionExpirationTime = event.GetWorkflowExecutionExpirationTime()
	}

	var workflowRunTimeoutTime time.Time
	workflowRunTimeoutDuration := timestamp.DurationValue(e.executionInfo.WorkflowRunTimeout)
	// if workflowRunTimeoutDuration == 0 then the workflowRunTimeoutTime will be 0
	// meaning that there is not workflow run timeout
	if workflowRunTimeoutDuration != 0 {
		firstWorkflowTaskDelayDuration := timestamp.DurationValue(event.GetFirstWorkflowTaskBackoff())
		workflowRunTimeoutDuration = workflowRunTimeoutDuration + firstWorkflowTaskDelayDuration
		workflowRunTimeoutTime = e.executionInfo.StartTime.Add(workflowRunTimeoutDuration)

		workflowExecutionTimeoutTime := timestamp.TimeValue(e.executionInfo.WorkflowExecutionExpirationTime)
		if !workflowExecutionTimeoutTime.IsZero() && workflowRunTimeoutTime.After(workflowExecutionTimeoutTime) {
			workflowRunTimeoutTime = workflowExecutionTimeoutTime
		}
	}
	e.executionInfo.WorkflowRunExpirationTime = timestamp.TimePtr(workflowRunTimeoutTime)

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
		e.namespaceEntry.Retention(),
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

func (e *MutableStateImpl) AddFirstWorkflowTaskScheduled(
	startEvent *historypb.HistoryEvent,
) error {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return err
	}
	return e.workflowTaskManager.AddFirstWorkflowTaskScheduled(startEvent)
}

func (e *MutableStateImpl) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskScheduledEvent(bypassTaskGeneration)
}

// AddWorkflowTaskScheduledEventAsHeartbeat is to record the first WorkflowTaskScheduledEvent during workflow task heartbeat.
func (e *MutableStateImpl) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp *time.Time,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp)
}

func (e *MutableStateImpl) ReplicateTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error) {
	return e.workflowTaskManager.ReplicateTransientWorkflowTaskScheduled()
}

func (e *MutableStateImpl) ReplicateWorkflowTaskScheduledEvent(
	version int64,
	scheduleID int64,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
	scheduleTimestamp *time.Time,
	originalScheduledTimestamp *time.Time,
) (*WorkflowTaskInfo, error) {
	return e.workflowTaskManager.ReplicateWorkflowTaskScheduledEvent(version, scheduleID, taskQueue, startToCloseTimeoutSeconds, attempt, scheduleTimestamp, originalScheduledTimestamp)
}

func (e *MutableStateImpl) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	taskQueue *taskqueuepb.TaskQueue,
	identity string,
) (*historypb.HistoryEvent, *WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskStartedEvent(scheduleEventID, requestID, taskQueue, identity)
}

func (e *MutableStateImpl) ReplicateWorkflowTaskStartedEvent(
	workflowTask *WorkflowTaskInfo,
	version int64,
	scheduleID int64,
	startedID int64,
	requestID string,
	timestamp time.Time,
) (*WorkflowTaskInfo, error) {

	return e.workflowTaskManager.ReplicateWorkflowTaskStartedEvent(workflowTask, version, scheduleID, startedID, requestID, timestamp)
}

func (e *MutableStateImpl) CreateTransientWorkflowTaskEvents(
	workflowTask *WorkflowTaskInfo,
	identity string,
) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	return e.workflowTaskManager.CreateTransientWorkflowTaskEvents(workflowTask, identity)
}

// add BinaryCheckSum for the first workflowTaskCompletedID for auto-reset
func (e *MutableStateImpl) addBinaryCheckSumIfNotExists(
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
		RunId:                        e.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: event.GetEventId(),
		CreateTime:                   timestamp.TimePtr(e.timeSource.Now()),
		Resettable:                   resettable,
	}
	currResetPoints = append(currResetPoints, info)
	exeInfo.AutoResetPoints = &workflowpb.ResetPoints{
		Points: currResetPoints,
	}
	checksumsPayload, err := searchattribute.EncodeValue(recentBinaryChecksums, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	if err != nil {
		return err
	}
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload, 1)
	}
	exeInfo.SearchAttributes[searchattribute.BinaryChecksums] = checksumsPayload
	if e.shard.GetConfig().AdvancedVisibilityWritingMode() != visibility.AdvancedVisibilityWritingModeOff {
		return e.taskGenerator.GenerateWorkflowSearchAttrTasks(timestamp.TimeValue(event.GetEventTime()))
	}
	return nil
}

// TODO: we will release the restriction when reset API allow those pending

// CheckResettable check if workflow can be reset
func (e *MutableStateImpl) CheckResettable() error {
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

func (e *MutableStateImpl) AddWorkflowTaskCompletedEvent(
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

func (e *MutableStateImpl) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	return e.workflowTaskManager.ReplicateWorkflowTaskCompletedEvent(event)
}

func (e *MutableStateImpl) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID)
}

func (e *MutableStateImpl) ReplicateWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	return e.workflowTaskManager.ReplicateWorkflowTaskTimedOutEvent(timeoutType)
}

func (e *MutableStateImpl) AddWorkflowTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}
	return e.workflowTaskManager.AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleEventID)
}

func (e *MutableStateImpl) AddWorkflowTaskFailedEvent(
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

func (e *MutableStateImpl) ReplicateWorkflowTaskFailedEvent() error {
	return e.workflowTaskManager.ReplicateWorkflowTaskFailedEvent()
}

func (e *MutableStateImpl) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskScheduled
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	_, ok := e.GetActivityByActivityID(command.GetActivityId())
	if ok {
		e.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction)
		return nil, nil, e.createCallerError(opTag, "ActivityID: "+command.GetActivityId())
	}

	event := e.hBuilder.AddActivityTaskScheduledEvent(workflowTaskCompletedEventID, command)
	ai, err := e.ReplicateActivityTaskScheduledEvent(workflowTaskCompletedEventID, event)
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateActivityTransferTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ai, err
}

func (e *MutableStateImpl) ReplicateActivityTaskScheduledEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) (*persistencespb.ActivityInfo, error) {

	attributes := event.GetActivityTaskScheduledEventAttributes()
	targetNamespaceID := namespace.ID(e.executionInfo.NamespaceId)
	if attributes.GetNamespace() != "" {
		targetNamespaceEntry, err := e.shard.GetNamespaceRegistry().GetNamespace(namespace.Name(attributes.GetNamespace()))
		if err != nil {
			return nil, err
		}
		targetNamespaceID = targetNamespaceEntry.ID()
	}

	scheduleEventID := event.GetEventId()
	scheduleToCloseTimeout := attributes.GetScheduleToCloseTimeout()

	ai := &persistencespb.ActivityInfo{
		Version:                 event.GetVersion(),
		ScheduleId:              scheduleEventID,
		ScheduledEventBatchId:   firstEventID,
		ScheduledTime:           event.GetEventTime(),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              attributes.ActivityId,
		NamespaceId:             targetNamespaceID.String(),
		ScheduleToStartTimeout:  attributes.GetScheduleToStartTimeout(),
		ScheduleToCloseTimeout:  scheduleToCloseTimeout,
		StartToCloseTimeout:     attributes.GetStartToCloseTimeout(),
		HeartbeatTimeout:        attributes.GetHeartbeatTimeout(),
		CancelRequested:         false,
		CancelRequestId:         common.EmptyEventID,
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         TimerTaskStatusNone,
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
		if timestamp.DurationValue(scheduleToCloseTimeout) > 0 {
			ai.RetryExpirationTime = timestamp.TimePtr(
				timestamp.TimeValue(ai.ScheduledTime).Add(timestamp.DurationValue(scheduleToCloseTimeout)),
			)
		} else {
			ai.RetryExpirationTime = timestamp.TimePtr(time.Time{})
		}
	}

	e.pendingActivityInfoIDs[ai.ScheduleId] = ai
	e.pendingActivityIDToEventID[ai.ActivityId] = ai.ScheduleId
	e.updateActivityInfos[ai.ScheduleId] = ai

	e.writeEventToCache(event)
	return ai, nil
}

func (e *MutableStateImpl) addTransientActivityStartedEvent(
	scheduleEventID int64,
) error {

	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedId != common.TransientEventID {
		return nil
	}

	// activity task was started (as transient event), we need to add it now.
	event := e.hBuilder.AddActivityTaskStartedEvent(
		scheduleEventID,
		ai.Attempt,
		ai.RequestId,
		ai.StartedIdentity,
		ai.RetryLastFailure,
	)
	if !ai.StartedTime.IsZero() {
		// overwrite started event time to the one recorded in ActivityInfo
		event.EventTime = ai.StartedTime
	}
	return e.ReplicateActivityTaskStartedEvent(event)
}

func (e *MutableStateImpl) AddActivityTaskStartedEvent(
	ai *persistencespb.ActivityInfo,
	scheduleEventID int64,
	requestID string,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	if !ai.HasRetryPolicy {
		event := e.hBuilder.AddActivityTaskStartedEvent(
			scheduleEventID,
			ai.Attempt,
			requestID,
			identity,
			ai.RetryLastFailure,
		)
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

func (e *MutableStateImpl) ReplicateActivityTaskStartedEvent(
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
	e.updateActivityInfos[ai.ScheduleId] = ai
	return nil
}

func (e *MutableStateImpl) AddActivityTaskCompletedEvent(
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
	event := e.hBuilder.AddActivityTaskCompletedEvent(
		scheduleEventID,
		startedEventID,
		request.Identity,
		request.Result,
	)
	if err := e.ReplicateActivityTaskCompletedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *MutableStateImpl) ReplicateActivityTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCompletedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *MutableStateImpl) AddActivityTaskFailedEvent(
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
	event := e.hBuilder.AddActivityTaskFailedEvent(
		scheduleEventID,
		startedEventID,
		failure,
		retryState,
		identity,
	)
	if err := e.ReplicateActivityTaskFailedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *MutableStateImpl) ReplicateActivityTaskFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskFailedEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *MutableStateImpl) AddActivityTaskTimedOutEvent(
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
	event := e.hBuilder.AddActivityTaskTimedOutEvent(
		scheduleEventID,
		startedEventID,
		timeoutFailure,
		retryState,
	)
	if err := e.ReplicateActivityTaskTimedOutEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *MutableStateImpl) ReplicateActivityTaskTimedOutEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskTimedOutEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *MutableStateImpl) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduleID int64,
	_ string,
) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskCancelRequested
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	ai, ok := e.GetActivityInfo(scheduleID)
	if !ok {
		// It is possible both started and completed events are buffered for this activity
		if !e.hBuilder.HasActivityFinishEvent(scheduleID) {
			e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
				tag.WorkflowEventID(e.GetNextEventID()),
				tag.ErrorTypeInvalidHistoryAction,
				tag.Bool(ok),
				tag.WorkflowScheduleID(scheduleID))

			return nil, nil, e.createCallerError(opTag, fmt.Sprintf("ScheduledID: %d", scheduleID))
		}
	}

	// Check for duplicate cancellation
	if ok && ai.CancelRequested {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduleID(scheduleID))

		return nil, nil, e.createCallerError(opTag, fmt.Sprintf("ScheduledID: %d", scheduleID))
	}

	// At this point we know this is a valid activity cancellation request
	actCancelReqEvent := e.hBuilder.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEventID, scheduleID)

	if err := e.ReplicateActivityTaskCancelRequestedEvent(actCancelReqEvent); err != nil {
		return nil, nil, err
	}

	return actCancelReqEvent, ai, nil
}

func (e *MutableStateImpl) ReplicateActivityTaskCancelRequestedEvent(
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
	e.updateActivityInfos[ai.ScheduleId] = ai
	return nil
}

func (e *MutableStateImpl) AddActivityTaskCanceledEvent(
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
	event := e.hBuilder.AddActivityTaskCanceledEvent(
		scheduleEventID,
		startedEventID,
		latestCancelRequestedEventID,
		details,
		identity,
	)
	if err := e.ReplicateActivityTaskCanceledEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (e *MutableStateImpl) ReplicateActivityTaskCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCanceledEventAttributes()
	scheduleID := attributes.GetScheduledEventId()

	return e.DeleteActivity(scheduleID)
}

func (e *MutableStateImpl) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCompleted
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddCompletedWorkflowEvent(workflowTaskCompletedEventID, command, newExecutionRunID)
	if err := e.ReplicateWorkflowExecutionCompletedEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionCompletedEvent(
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
	e.executionInfo.NewExecutionRunId = event.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId()
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *MutableStateImpl) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowFailed
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddFailWorkflowEvent(workflowTaskCompletedEventID, retryState, command, newExecutionRunID)
	if err := e.ReplicateWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionFailedEvent(
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
	e.executionInfo.NewExecutionRunId = event.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId()
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *MutableStateImpl) AddTimeoutWorkflowEvent(
	firstEventID int64,
	retryState enumspb.RetryState,
	newExecutionRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowTimeout
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddTimeoutWorkflowEvent(retryState, newExecutionRunID)
	if err := e.ReplicateWorkflowExecutionTimedoutEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionTimedoutEvent(
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
	e.executionInfo.NewExecutionRunId = event.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId()
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *MutableStateImpl) AddWorkflowExecutionCancelRequestedEvent(
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
			tag.WorkflowState(e.executionState.State),
			tag.Bool(e.executionInfo.CancelRequested),
			tag.Key(e.executionInfo.CancelRequestId),
		)
		return nil, e.createInternalServerError(opTag)
	}

	event := e.hBuilder.AddWorkflowExecutionCancelRequestedEvent(request)
	if err := e.ReplicateWorkflowExecutionCancelRequestedEvent(event); err != nil {
		return nil, err
	}

	// Set the CancelRequestID on the active cluster.  This information is not part of the history event.
	e.executionInfo.CancelRequestId = request.CancelRequest.GetRequestId()
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionCancelRequestedEvent(
	_ *historypb.HistoryEvent,
) error {

	e.executionInfo.CancelRequested = true
	return nil
}

func (e *MutableStateImpl) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, command)
	if err := e.ReplicateWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionCanceledEvent(
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
	e.executionInfo.NewExecutionRunId = ""
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *MutableStateImpl) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	cancelRequestID string,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, *persistencespb.RequestCancelInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, command)
	rci, err := e.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, cancelRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateRequestCancelExternalTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, rci, nil
}

func (e *MutableStateImpl) ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	cancelRequestID string,
) (*persistencespb.RequestCancelInfo, error) {

	// TODO: Evaluate if we need cancelRequestID also part of history event
	initiatedEventID := event.GetEventId()
	rci := &persistencespb.RequestCancelInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchId: firstEventID,
		InitiatedId:           initiatedEventID,
		CancelRequestId:       cancelRequestID,
	}

	e.pendingRequestCancelInfoIDs[rci.InitiatedId] = rci
	e.updateRequestCancelInfos[rci.InitiatedId] = rci

	return rci, nil
}

func (e *MutableStateImpl) AddExternalWorkflowExecutionCancelRequested(
	initiatedID int64,
	namespace namespace.Name,
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

	event := e.hBuilder.AddExternalWorkflowExecutionCancelRequested(
		initiatedID,
		namespace,
		workflowID,
		runID,
	)
	if err := e.ReplicateExternalWorkflowExecutionCancelRequested(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateExternalWorkflowExecutionCancelRequested(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionCancelRequestedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingRequestCancel(initiatedID)
}

func (e *MutableStateImpl) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	initiatedID int64,
	namespace namespace.Name,
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

	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		common.EmptyEventID, // TODO this field is not used at all
		initiatedID,
		namespace,
		workflowID,
		runID,
		cause,
	)
	if err := e.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingRequestCancel(initiatedID)
}

func (e *MutableStateImpl) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	signalRequestID string,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, *persistencespb.SignalInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, command)
	si, err := e.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, signalRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateSignalExternalTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, si, nil
}

func (e *MutableStateImpl) ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	signalRequestID string,
) (*persistencespb.SignalInfo, error) {

	// TODO: Consider also writing signalRequestID to history event
	initiatedEventID := event.GetEventId()
	si := &persistencespb.SignalInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchId: firstEventID,
		InitiatedId:           initiatedEventID,
		RequestId:             signalRequestID,
	}

	e.pendingSignalInfoIDs[si.InitiatedId] = si
	e.updateSignalInfos[si.InitiatedId] = si

	e.writeEventToCache(event)
	return si, nil
}

func (e *MutableStateImpl) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionUpsertWorkflowSearchAttributes
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, command)
	e.ReplicateUpsertWorkflowSearchAttributesEvent(event)
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowSearchAttrTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateUpsertWorkflowSearchAttributesEvent(
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

func (e *MutableStateImpl) AddExternalWorkflowExecutionSignaled(
	initiatedID int64,
	namespace namespace.Name,
	workflowID string,
	runID string,
	control string, // TODO this field is probably deprecated
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

	event := e.hBuilder.AddExternalWorkflowExecutionSignaled(
		initiatedID,
		namespace,
		workflowID,
		runID,
		control, // TODO this field is probably deprecated
	)
	if err := e.ReplicateExternalWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateExternalWorkflowExecutionSignaled(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionSignaledEventAttributes().GetInitiatedEventId()

	return e.DeletePendingSignal(initiatedID)
}

func (e *MutableStateImpl) AddSignalExternalWorkflowExecutionFailedEvent(
	initiatedID int64,
	namespace namespace.Name,
	workflowID string,
	runID string,
	control string, // TODO this field is probably deprecated
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

	event := e.hBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
		common.EmptyEventID, // TODO this field is not used at all
		initiatedID,
		namespace,
		workflowID,
		runID,
		control, // TODO this field is probably deprecated
		cause,
	)
	if err := e.ReplicateSignalExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateSignalExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetSignalExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return e.DeletePendingSignal(initiatedID)
}

func (e *MutableStateImpl) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) (*historypb.HistoryEvent, *persistencespb.TimerInfo, error) {

	opTag := tag.WorkflowActionTimerStarted
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	timerID := command.GetTimerId()
	_, ok := e.GetUserTimerInfo(timerID)
	if ok {
		e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowTimerID(timerID))
		return nil, nil, e.createCallerError(opTag, "TimerID: "+command.GetTimerId())
	}

	event := e.hBuilder.AddTimerStartedEvent(workflowTaskCompletedEventID, command)
	ti, err := e.ReplicateTimerStartedEvent(event)
	if err != nil {
		return nil, nil, err
	}
	return event, ti, err
}

func (e *MutableStateImpl) ReplicateTimerStartedEvent(
	event *historypb.HistoryEvent,
) (*persistencespb.TimerInfo, error) {

	attributes := event.GetTimerStartedEventAttributes()
	timerID := attributes.GetTimerId()

	startToFireTimeout := timestamp.DurationValue(attributes.GetStartToFireTimeout())
	// TODO: Time skew need to be taken in to account.
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(startToFireTimeout) // should use the event time, not now

	ti := &persistencespb.TimerInfo{
		Version:    event.GetVersion(),
		TimerId:    timerID,
		ExpiryTime: &expiryTime,
		StartedId:  event.GetEventId(),
		TaskStatus: TimerTaskStatusNone,
	}

	e.pendingTimerInfoIDs[ti.TimerId] = ti
	e.pendingTimerEventIDToID[ti.StartedId] = ti.TimerId
	e.updateTimerInfos[ti.TimerId] = ti

	return ti, nil
}

func (e *MutableStateImpl) AddTimerFiredEvent(
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
	event := e.hBuilder.AddTimerFiredEvent(timerInfo.GetStartedId(), timerInfo.TimerId)
	if err := e.ReplicateTimerFiredEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateTimerFiredEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetTimerFiredEventAttributes()
	timerID := attributes.GetTimerId()

	return e.DeleteUserTimer(timerID)
}

func (e *MutableStateImpl) AddTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelTimerCommandAttributes,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionTimerCanceled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	var timerStartedID int64
	timerID := command.GetTimerId()
	ti, ok := e.GetUserTimerInfo(timerID)
	if !ok {
		// if timer is not running then check if it has fired in the mutable state.
		// If so clear the timer from the mutable state. We need to check both the
		// bufferedEvents and the history builder
		timerFiredEvent := e.hBuilder.GetAndRemoveTimerFireEvent(timerID)
		if timerFiredEvent == nil {
			e.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
				tag.WorkflowEventID(e.GetNextEventID()),
				tag.ErrorTypeInvalidHistoryAction,
				tag.WorkflowTimerID(timerID))
			return nil, e.createCallerError(opTag, "TimerID: "+command.GetTimerId())
		}
		timerStartedID = timerFiredEvent.GetTimerFiredEventAttributes().GetStartedEventId()
	} else {
		timerStartedID = ti.GetStartedId()
	}

	// Timer is running.
	event := e.hBuilder.AddTimerCanceledEvent(
		workflowTaskCompletedEventID,
		timerStartedID,
		timerID,
		identity,
	)
	if ok {
		if err := e.ReplicateTimerCanceledEvent(event); err != nil {
			return nil, err
		}
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateTimerCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetTimerCanceledEventAttributes()
	timerID := attributes.GetTimerId()

	return e.DeleteUserTimer(timerID)
}

func (e *MutableStateImpl) AddRecordMarkerEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowRecordMarker
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	return e.hBuilder.AddMarkerRecordedEvent(workflowTaskCompletedEventID, command), nil
}

func (e *MutableStateImpl) AddWorkflowExecutionTerminatedEvent(
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
	if err := e.taskGenerator.GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionTerminatedEvent(
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
	e.executionInfo.NewExecutionRunId = ""
	e.ClearStickyness()
	e.writeEventToCache(event)
	return nil
}

func (e *MutableStateImpl) AddWorkflowExecutionSignaled(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowSignaled
	if err := e.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := e.hBuilder.AddWorkflowExecutionSignaledEvent(signalName, input, identity, header)
	if err := e.ReplicateWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateWorkflowExecutionSignaled(
	_ *historypb.HistoryEvent,
) error {

	// Increment signal count in mutable state for this workflow execution
	e.executionInfo.SignalCount++
	return nil
}

func (e *MutableStateImpl) AddContinueAsNewEvent(
	firstEventID int64,
	workflowTaskCompletedEventID int64,
	parentNamespace namespace.Name,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, MutableState, error) {

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
			Namespace:   parentNamespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: e.executionInfo.ParentWorkflowId,
				RunId:      e.executionInfo.ParentRunId,
			},
			InitiatedId: e.executionInfo.InitiatedId,
		}
	}

	continueAsNewEvent := e.hBuilder.AddContinuedAsNewEvent(
		workflowTaskCompletedEventID,
		newRunID,
		command,
	)

	firstRunID, err := e.GetFirstRunID()
	if err != nil {
		return nil, nil, err
	}

	newStateBuilder := NewMutableState(
		e.shard,
		e.shard.GetEventsCache(),
		e.logger,
		e.namespaceEntry,
		timestamp.TimeValue(continueAsNewEvent.GetEventTime()),
	)

	if err = newStateBuilder.SetHistoryTree(newRunID); err != nil {
		return nil, nil, err
	}

	if _, err = newStateBuilder.addWorkflowExecutionStartedEventForContinueAsNew(
		parentInfo,
		newExecution,
		e,
		command,
		firstRunID,
	); err != nil {
		return nil, nil, err
	}

	if err = e.ReplicateWorkflowExecutionContinuedAsNewEvent(
		firstEventID,
		continueAsNewEvent,
	); err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateWorkflowCloseTasks(
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
	namespaceRetention time.Duration,
) *workflowpb.ResetPoints {

	if resetPoints == nil || resetPoints.Points == nil {
		return resetPoints
	}
	newPoints := make([]*workflowpb.ResetPointInfo, 0, len(resetPoints.Points))
	expireTime := now.Add(namespaceRetention)
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

func (e *MutableStateImpl) ReplicateWorkflowExecutionContinuedAsNewEvent(
	firstEventID int64,
	continueAsNewEvent *historypb.HistoryEvent,
) error {

	if err := e.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	); err != nil {
		return err
	}
	e.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	e.executionInfo.NewExecutionRunId = continueAsNewEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
	e.ClearStickyness()
	e.writeEventToCache(continueAsNewEvent)
	return nil
}

func (e *MutableStateImpl) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	createRequestID string,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiated
	if err := e.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := e.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, command)
	ci, err := e.ReplicateStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, createRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := e.taskGenerator.GenerateChildWorkflowTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ci, nil
}

func (e *MutableStateImpl) ReplicateStartChildWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	createRequestID string,
) (*persistencespb.ChildExecutionInfo, error) {

	initiatedEventID := event.GetEventId()
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	ci := &persistencespb.ChildExecutionInfo{
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

	e.pendingChildExecutionInfoIDs[ci.InitiatedId] = ci
	e.updateChildExecutionInfos[ci.InitiatedId] = ci

	e.writeEventToCache(event)
	return ci, nil
}

func (e *MutableStateImpl) AddChildWorkflowExecutionStartedEvent(
	namespace namespace.Name,
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

	event := e.hBuilder.AddChildWorkflowExecutionStartedEvent(
		initiatedID,
		namespace,
		execution,
		workflowType,
		header,
	)
	if err := e.ReplicateChildWorkflowExecutionStartedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateChildWorkflowExecutionStartedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	ci, _ := e.GetChildExecutionInfo(initiatedID)
	ci.StartedId = event.GetEventId()
	ci.StartedRunId = attributes.GetWorkflowExecution().GetRunId()
	e.updateChildExecutionInfos[ci.InitiatedId] = ci

	return nil
}

func (e *MutableStateImpl) AddStartChildWorkflowExecutionFailedEvent(
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

	event := e.hBuilder.AddStartChildWorkflowExecutionFailedEvent(
		common.EmptyEventID, // TODO this field is not used at all
		initiatedID,
		cause,
		namespace.Name(initiatedEventAttributes.Namespace),
		initiatedEventAttributes.WorkflowId,
		initiatedEventAttributes.WorkflowType,
		initiatedEventAttributes.Control, // TODO this field is probably deprecated
	)
	if err := e.ReplicateStartChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateStartChildWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetStartChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *MutableStateImpl) AddChildWorkflowExecutionCompletedEvent(
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

	event := e.hBuilder.AddChildWorkflowExecutionCompletedEvent(
		ci.InitiatedId,
		ci.StartedId,
		namespace.Name(ci.Namespace),
		childExecution,
		workflowType,
		attributes.Result,
	)
	if err := e.ReplicateChildWorkflowExecutionCompletedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateChildWorkflowExecutionCompletedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *MutableStateImpl) AddChildWorkflowExecutionFailedEvent(
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

	event := e.hBuilder.AddChildWorkflowExecutionFailedEvent(
		ci.InitiatedId,
		ci.StartedId,
		namespace.Name(ci.Namespace),
		childExecution,
		workflowType,
		attributes.Failure,
		attributes.RetryState,
	)
	if err := e.ReplicateChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateChildWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *MutableStateImpl) AddChildWorkflowExecutionCanceledEvent(
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

	event := e.hBuilder.AddChildWorkflowExecutionCanceledEvent(
		ci.InitiatedId,
		ci.StartedId,
		namespace.Name(ci.Namespace),
		childExecution,
		workflowType,
		attributes.Details,
	)
	if err := e.ReplicateChildWorkflowExecutionCanceledEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateChildWorkflowExecutionCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *MutableStateImpl) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	_ *historypb.WorkflowExecutionTerminatedEventAttributes, // TODO this field is not used at all
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

	event := e.hBuilder.AddChildWorkflowExecutionTerminatedEvent(
		ci.InitiatedId,
		ci.StartedId,
		namespace.Name(ci.Namespace),
		childExecution,
		workflowType,
	)
	if err := e.ReplicateChildWorkflowExecutionTerminatedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateChildWorkflowExecutionTerminatedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *MutableStateImpl) AddChildWorkflowExecutionTimedOutEvent(
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

	event := e.hBuilder.AddChildWorkflowExecutionTimedOutEvent(
		ci.InitiatedId,
		ci.StartedId,
		namespace.Name(ci.Namespace),
		childExecution,
		workflowType,
		attributes.RetryState,
	)
	if err := e.ReplicateChildWorkflowExecutionTimedOutEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *MutableStateImpl) ReplicateChildWorkflowExecutionTimedOutEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return e.DeletePendingChildExecution(initiatedID)
}

func (e *MutableStateImpl) RetryActivity(
	ai *persistencespb.ActivityInfo,
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
		ai.Attempt,
		ai.RetryMaximumAttempts,
		ai.RetryInitialInterval,
		ai.RetryMaximumInterval,
		ai.RetryExpirationTime,
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
	ai.TimerTaskStatus = TimerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = failure

	if err := e.taskGenerator.GenerateActivityRetryTasks(
		ai.ScheduleId,
	); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}

	e.updateActivityInfos[ai.ScheduleId] = ai
	e.syncActivityTasks[ai.ScheduleId] = struct{}{}
	return enumspb.RETRY_STATE_IN_PROGRESS, nil
}

// TODO mutable state should generate corresponding transfer / timer tasks according to
//  updates accumulated, while currently all transfer / timer tasks are managed manually

// TODO convert AddTransferTasks to prepareTransferTasks

// AddTransferTasks append transfer tasks
func (e *MutableStateImpl) AddTransferTasks(
	transferTasks ...tasks.Task,
) {

	e.InsertTransferTasks = append(e.InsertTransferTasks, transferTasks...)
}

// TODO convert AddTransferTasks to prepareTimerTasks

// AddTimerTasks append timer tasks
func (e *MutableStateImpl) AddTimerTasks(
	timerTasks ...tasks.Task,
) {

	e.InsertTimerTasks = append(e.InsertTimerTasks, timerTasks...)
}

// AddReplicationTasks append visibility tasks
func (e *MutableStateImpl) AddReplicationTasks(
	replicationTasks ...tasks.Task,
) {

	e.InsertReplicationTasks = append(e.InsertReplicationTasks, replicationTasks...)
}

// AddVisibilityTasks append visibility tasks
func (e *MutableStateImpl) AddVisibilityTasks(
	visibilityTasks ...tasks.Task,
) {

	e.InsertVisibilityTasks = append(e.InsertVisibilityTasks, visibilityTasks...)
}

func (e *MutableStateImpl) SetUpdateCondition(
	nextEventIDInDB int64,
	dbRecordVersion int64,
) {

	e.nextEventIDInDB = nextEventIDInDB
	e.dbRecordVersion = dbRecordVersion
}

func (e *MutableStateImpl) GetUpdateCondition() (int64, int64) {
	return e.nextEventIDInDB, e.dbRecordVersion
}

func (e *MutableStateImpl) GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus) {
	return e.executionState.State, e.executionState.Status
}

func (e *MutableStateImpl) UpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {

	return setStateStatus(e.executionState, state, status)
}

func (e *MutableStateImpl) StartTransaction(
	namespaceEntry *namespace.Namespace,
) (bool, error) {

	e.namespaceEntry = namespaceEntry
	if err := e.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false); err != nil {
		return false, err
	}

	flushBeforeReady, err := e.startTransactionHandleWorkflowTaskFailover(false)
	if err != nil {
		return false, err
	}

	e.startTransactionHandleWorkflowTaskTTL()

	return flushBeforeReady, nil
}

func (e *MutableStateImpl) StartTransactionSkipWorkflowTaskFail(
	namespaceEntry *namespace.Namespace,
) error {

	e.namespaceEntry = namespaceEntry
	if err := e.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false); err != nil {
		return err
	}

	_, err := e.startTransactionHandleWorkflowTaskFailover(true)
	return err
}

func (e *MutableStateImpl) CloseTransactionAsMutation(
	now time.Time,
	transactionPolicy TransactionPolicy,
) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error) {

	if err := e.prepareCloseTransaction(
		now,
		transactionPolicy,
	); err != nil {
		return nil, nil, err
	}

	workflowEventsSeq, bufferEvents, clearBuffer, err := e.prepareEventsAndReplicationTasks(now, transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		lastEvent := lastEvents[len(lastEvents)-1]
		if err := e.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, err
		}
	}

	setTaskInfo(e.GetCurrentVersion(), now, e.InsertTransferTasks, e.InsertTimerTasks, e.InsertVisibilityTasks)

	// update last update time
	e.executionInfo.LastUpdateTime = &now
	e.executionInfo.StateTransitionCount += 1

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside Context.resetWorkflowExecution
	// currently, the updates done inside Context.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := e.generateChecksum()

	if e.dbRecordVersion == 0 {
		// noop, existing behavior
	} else {
		e.dbRecordVersion += 1
	}

	workflowMutation := &persistence.WorkflowMutation{
		ExecutionInfo:  e.executionInfo,
		ExecutionState: e.executionState,
		NextEventID:    e.hBuilder.NextEventID(),

		UpsertActivityInfos:       e.updateActivityInfos,
		DeleteActivityInfos:       e.deleteActivityInfos,
		UpsertTimerInfos:          e.updateTimerInfos,
		DeleteTimerInfos:          e.deleteTimerInfos,
		UpsertChildExecutionInfos: e.updateChildExecutionInfos,
		DeleteChildExecutionInfos: e.deleteChildExecutionInfos,
		UpsertRequestCancelInfos:  e.updateRequestCancelInfos,
		DeleteRequestCancelInfos:  e.deleteRequestCancelInfos,
		UpsertSignalInfos:         e.updateSignalInfos,
		DeleteSignalInfos:         e.deleteSignalInfos,
		UpsertSignalRequestedIDs:  e.updateSignalRequestedIDs,
		DeleteSignalRequestedIDs:  e.deleteSignalRequestedIDs,
		NewBufferedEvents:         bufferEvents,
		ClearBufferedEvents:       clearBuffer,

		TransferTasks:    e.InsertTransferTasks,
		ReplicationTasks: e.InsertReplicationTasks,
		TimerTasks:       e.InsertTimerTasks,
		VisibilityTasks:  e.InsertVisibilityTasks,

		Condition:       e.nextEventIDInDB,
		DBRecordVersion: e.dbRecordVersion,
		Checksum:        checksum,
	}

	e.checksum = checksum
	if err := e.cleanupTransaction(transactionPolicy); err != nil {
		return nil, nil, err
	}
	return workflowMutation, workflowEventsSeq, nil
}

func (e *MutableStateImpl) CloseTransactionAsSnapshot(
	now time.Time,
	transactionPolicy TransactionPolicy,
) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {

	if err := e.prepareCloseTransaction(
		now,
		transactionPolicy,
	); err != nil {
		return nil, nil, err
	}

	workflowEventsSeq, bufferEvents, _, err := e.prepareEventsAndReplicationTasks(now, transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(bufferEvents) > 0 {
		// TODO do we need the functionality to generate snapshot with buffered events?
		return nil, nil, serviceerror.NewInternal("cannot generate workflow snapshot with buffered events")
	}

	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		lastEvent := lastEvents[len(lastEvents)-1]
		if err := e.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, err
		}
	}

	setTaskInfo(e.GetCurrentVersion(), now, e.InsertTransferTasks, e.InsertTimerTasks, e.InsertVisibilityTasks)

	// update last update time
	e.executionInfo.LastUpdateTime = &now
	e.executionInfo.StateTransitionCount += 1

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside Context.resetWorkflowExecution
	// currently, the updates done inside Context.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := e.generateChecksum()

	if e.dbRecordVersion == 0 {
		// noop, existing behavior
	} else {
		e.dbRecordVersion += 1
	}

	workflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:  e.executionInfo,
		ExecutionState: e.executionState,
		NextEventID:    e.hBuilder.NextEventID(),

		ActivityInfos:       e.pendingActivityInfoIDs,
		TimerInfos:          e.pendingTimerInfoIDs,
		ChildExecutionInfos: e.pendingChildExecutionInfoIDs,
		RequestCancelInfos:  e.pendingRequestCancelInfoIDs,
		SignalInfos:         e.pendingSignalInfoIDs,
		SignalRequestedIDs:  e.pendingSignalRequestedIDs,

		TransferTasks:    e.InsertTransferTasks,
		ReplicationTasks: e.InsertReplicationTasks,
		TimerTasks:       e.InsertTimerTasks,
		VisibilityTasks:  e.InsertVisibilityTasks,

		Condition:       e.nextEventIDInDB,
		DBRecordVersion: e.dbRecordVersion,
		Checksum:        checksum,
	}

	e.checksum = checksum
	if err := e.cleanupTransaction(transactionPolicy); err != nil {
		return nil, nil, err
	}
	return workflowSnapshot, workflowEventsSeq, nil
}

func (e *MutableStateImpl) IsResourceDuplicated(
	resourceDedupKey definition.DeduplicationID,
) bool {
	id := definition.GenerateDeduplicationKey(resourceDedupKey)
	_, duplicated := e.appliedEvents[id]
	return duplicated
}

func (e *MutableStateImpl) UpdateDuplicatedResource(
	resourceDedupKey definition.DeduplicationID,
) {
	id := definition.GenerateDeduplicationKey(resourceDedupKey)
	e.appliedEvents[id] = struct{}{}
}

func (e *MutableStateImpl) GenerateLastHistoryReplicationTasks(now time.Time) (*tasks.HistoryReplicationTask, error) {
	return e.taskGenerator.GenerateLastHistoryReplicationTasks(now)
}

func (e *MutableStateImpl) prepareCloseTransaction(
	now time.Time,
	transactionPolicy TransactionPolicy,
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

func (e *MutableStateImpl) cleanupTransaction(
	_ TransactionPolicy,
) error {

	e.updateActivityInfos = make(map[int64]*persistencespb.ActivityInfo)
	e.deleteActivityInfos = make(map[int64]struct{})
	e.syncActivityTasks = make(map[int64]struct{})

	e.updateTimerInfos = make(map[string]*persistencespb.TimerInfo)
	e.deleteTimerInfos = make(map[string]struct{})

	e.updateChildExecutionInfos = make(map[int64]*persistencespb.ChildExecutionInfo)
	e.deleteChildExecutionInfos = make(map[int64]struct{})

	e.updateRequestCancelInfos = make(map[int64]*persistencespb.RequestCancelInfo)
	e.deleteRequestCancelInfos = make(map[int64]struct{})

	e.updateSignalInfos = make(map[int64]*persistencespb.SignalInfo)
	e.deleteSignalInfos = make(map[int64]struct{})

	e.updateSignalRequestedIDs = make(map[string]struct{})
	e.deleteSignalRequestedIDs = make(map[string]struct{})

	e.stateInDB = e.executionState.State
	e.nextEventIDInDB = e.GetNextEventID()
	// e.dbRecordVersion remains the same

	e.hBuilder = NewMutableHistoryBuilder(
		e.timeSource,
		e.shard.GenerateTransferTaskIDs,
		e.GetCurrentVersion(),
		e.nextEventIDInDB,
		e.bufferEventsInDB,
	)

	e.InsertTransferTasks = nil
	e.InsertReplicationTasks = nil
	e.InsertTimerTasks = nil
	e.InsertVisibilityTasks = nil

	return nil
}

func (e *MutableStateImpl) prepareEventsAndReplicationTasks(
	now time.Time,
	transactionPolicy TransactionPolicy,
) ([]*persistence.WorkflowEvents, []*historypb.HistoryEvent, bool, error) {

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return nil, nil, false, err
	}

	historyMutation, err := e.hBuilder.Finish(!e.HasInFlightWorkflowTask())
	if err != nil {
		return nil, nil, false, err
	}

	// TODO @wxing1292 need more refactoring to make the logic clean
	e.bufferEventsInDB = historyMutation.MemBufferBatch
	newBufferBatch := historyMutation.DBBufferBatch
	clearBuffer := historyMutation.DBClearBuffer
	newEventsBatches := historyMutation.DBEventsBatches
	e.updatePendingEventIDs(historyMutation.ScheduleIDToStartID)

	workflowEventsSeq := make([]*persistence.WorkflowEvents, len(newEventsBatches))
	historyNodeTxnIDs, err := e.shard.GenerateTransferTaskIDs(len(newEventsBatches))
	if err != nil {
		return nil, nil, false, err
	}
	for index, eventBatch := range newEventsBatches {
		workflowEventsSeq[index] = &persistence.WorkflowEvents{
			NamespaceID: e.executionInfo.NamespaceId,
			WorkflowID:  e.executionInfo.WorkflowId,
			RunID:       e.executionState.RunId,
			BranchToken: currentBranchToken,
			PrevTxnID:   e.executionInfo.LastFirstEventTxnId,
			TxnID:       historyNodeTxnIDs[index],
			Events:      eventBatch,
		}
		e.executionInfo.LastFirstEventId = eventBatch[0].GetEventId()
		e.executionInfo.LastFirstEventTxnId = historyNodeTxnIDs[index]
	}

	if err := e.validateNoEventsAfterWorkflowFinish(
		transactionPolicy,
		workflowEventsSeq,
	); err != nil {
		return nil, nil, false, err
	}

	for _, workflowEvents := range workflowEventsSeq {
		if err := e.eventsToReplicationTask(transactionPolicy, workflowEvents.Events); err != nil {
			return nil, nil, false, err
		}
	}

	e.InsertReplicationTasks = append(
		e.InsertReplicationTasks,
		e.syncActivityToReplicationTask(now, transactionPolicy)...,
	)

	if transactionPolicy == TransactionPolicyPassive && len(e.InsertReplicationTasks) > 0 {
		return nil, nil, false, serviceerror.NewInternal("should not generate replication task when close transaction as passive")
	}

	return workflowEventsSeq, newBufferBatch, clearBuffer, nil
}

func (e *MutableStateImpl) eventsToReplicationTask(
	transactionPolicy TransactionPolicy,
	events []*historypb.HistoryEvent,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!e.canReplicateEvents() ||
		len(events) == 0 {
		return nil
	}

	currentBranchToken, err := e.GetCurrentBranchToken()
	if err != nil {
		return err
	}
	return e.taskGenerator.GenerateHistoryReplicationTasks(
		e.timeSource.Now(),
		currentBranchToken,
		events,
	)
}

func (e *MutableStateImpl) syncActivityToReplicationTask(
	now time.Time,
	transactionPolicy TransactionPolicy,
) []tasks.Task {

	if transactionPolicy == TransactionPolicyPassive ||
		!e.canReplicateEvents() {
		return emptyTasks
	}

	return convertSyncActivityInfos(
		now,
		definition.NewWorkflowKey(
			e.executionInfo.NamespaceId,
			e.executionInfo.WorkflowId,
			e.executionState.RunId,
		),
		e.pendingActivityInfoIDs,
		e.syncActivityTasks,
	)
}

func (e *MutableStateImpl) updatePendingEventIDs(
	scheduleIDToStartID map[int64]int64,
) {
Loop:
	for scheduleID, startID := range scheduleIDToStartID {
		if activityInfo, ok := e.GetActivityInfo(scheduleID); ok {
			activityInfo.StartedId = startID
			e.updateActivityInfos[activityInfo.ScheduleId] = activityInfo
			continue Loop
		}
		if childInfo, ok := e.GetChildExecutionInfo(scheduleID); ok {
			childInfo.StartedId = startID
			e.updateChildExecutionInfos[childInfo.InitiatedId] = childInfo
			continue Loop
		}
	}
}

func (e *MutableStateImpl) updateWithLastWriteEvent(
	lastEvent *historypb.HistoryEvent,
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive {
		// already handled in state builder
		return nil
	}

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(e.executionInfo.VersionHistories)
	if err != nil {
		return err
	}
	if err := versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEvent.GetEventId(), lastEvent.GetVersion(),
	)); err != nil {
		return err
	}
	e.executionInfo.LastEventTaskId = lastEvent.GetTaskId()

	return nil
}

func (e *MutableStateImpl) canReplicateEvents() bool {
	return e.namespaceEntry.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster
}

// validateNoEventsAfterWorkflowFinish perform check on history event batch
// NOTE: do not apply this check on every batch, since transient
// workflow task && workflow finish will be broken (the first batch)
func (e *MutableStateImpl) validateNoEventsAfterWorkflowFinish(
	transactionPolicy TransactionPolicy,
	workflowEventSeq []*persistence.WorkflowEvents,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		len(workflowEventSeq) == 0 {
		return nil
	}

	// only do check if workflow is finished
	if e.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil
	}

	// workflow close
	// this will perform check on the last event of last batch
	// NOTE: do not apply this check on every batch, since transient
	// workflow task && workflow finish will be broken (the first batch)
	eventBatch := workflowEventSeq[len(workflowEventSeq)-1].Events
	lastEvent := eventBatch[len(eventBatch)-1]
	switch lastEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return nil

	default:
		e.logError(
			"encountered case where events appears after workflow finish.",
			tag.WorkflowNamespaceID(e.executionInfo.NamespaceId),
			tag.WorkflowID(e.executionInfo.WorkflowId),
			tag.WorkflowRunID(e.executionState.RunId),
		)
		return consts.ErrEventsAterWorkflowFinish
	}
}

func (e *MutableStateImpl) startTransactionHandleWorkflowTaskTTL() {
	if e.executionInfo.StickyTaskQueue == "" {
		return
	}

	ttl := e.config.StickyTTL(e.GetNamespaceEntry().Name().String())
	expired := e.timeSource.Now().After(timestamp.TimeValue(e.executionInfo.LastUpdateTime).Add(ttl))
	if expired && !e.HasPendingWorkflowTask() {
		e.ClearStickyness()
	}
}

func (e *MutableStateImpl) startTransactionHandleWorkflowTaskFailover(
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
		return false, serviceerror.NewInternal(fmt.Sprintf("MutableStateImpl encountered mismatch version, workflow task: %v, last write version %v", workflowTask.Version, lastWriteVersion))
	}

	lastWriteSourceCluster := e.clusterMetadata.ClusterNameForFailoverVersion(e.namespaceEntry.IsGlobalNamespace(), lastWriteVersion)
	currentVersionCluster := e.clusterMetadata.ClusterNameForFailoverVersion(e.namespaceEntry.IsGlobalNamespace(), currentVersion)
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
			return false, serviceerror.NewInternal("MutableStateImpl encountered previous passive workflow with buffered events")
		}
		return false, nil
	}

	// handle case 1 & 2
	var flushBufferVersion = lastWriteVersion

	// handle case 3
	if lastWriteSourceCluster != currentCluster && currentVersionCluster == currentCluster {
		// do a sanity check on buffered events
		if e.HasBufferedEvents() {
			return false, serviceerror.NewInternal("MutableStateImpl encounter previous passive workflow with buffered events")
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

	err = ScheduleWorkflowTask(e)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *MutableStateImpl) closeTransactionWithPolicyCheck(
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!e.canReplicateEvents() {
		return nil
	}

	activeCluster := e.clusterMetadata.ClusterNameForFailoverVersion(e.namespaceEntry.IsGlobalNamespace(), e.GetCurrentVersion())
	currentCluster := e.clusterMetadata.GetCurrentClusterName()

	if activeCluster != currentCluster {
		namespaceID := e.GetExecutionInfo().NamespaceId
		return serviceerror.NewNamespaceNotActive(namespaceID, currentCluster, activeCluster)
	}
	return nil
}

func (e *MutableStateImpl) closeTransactionHandleBufferedEventsLimit(
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!e.IsWorkflowExecutionRunning() {
		return nil
	}

	if e.hBuilder.BufferEventSize() < e.config.MaximumBufferedEventsBatch() {
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

		err := ScheduleWorkflowTask(e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *MutableStateImpl) closeTransactionHandleWorkflowReset(
	now time.Time,
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!e.IsWorkflowExecutionRunning() {
		return nil
	}

	// compare with bad client binary checksum and schedule a reset task

	// only schedule reset task if current doesn't have childWFs.
	// TODO: This will be removed once our reset allows childWFs
	if len(e.GetPendingChildExecutionInfos()) != 0 {
		return nil
	}

	namespaceEntry, err := e.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(e.executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	if _, pt := FindAutoResetPoint(
		e.timeSource,
		namespaceEntry.VerifyBinaryChecksum,
		e.GetExecutionInfo().AutoResetPoints,
	); pt != nil {
		if err := e.taskGenerator.GenerateWorkflowResetTasks(
			e.unixNanoToTime(now.UnixNano()),
		); err != nil {
			return err
		}
		e.logInfo("Auto-Reset task is scheduled",
			tag.WorkflowNamespace(namespaceEntry.Name().String()),
			tag.WorkflowID(e.executionInfo.WorkflowId),
			tag.WorkflowRunID(e.executionState.RunId),
			tag.WorkflowResetBaseRunID(pt.GetRunId()),
			tag.WorkflowEventID(pt.GetFirstWorkflowTaskCompletedId()),
			tag.WorkflowBinaryChecksum(pt.GetBinaryChecksum()),
		)
	}
	return nil
}

func (e *MutableStateImpl) closeTransactionHandleActivityUserTimerTasks(
	now time.Time,
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!e.IsWorkflowExecutionRunning() {
		return nil
	}

	if err := e.taskGenerator.GenerateActivityTimerTasks(
		e.unixNanoToTime(now.UnixNano()),
	); err != nil {
		return err
	}

	return e.taskGenerator.GenerateUserTimerTasks(
		e.unixNanoToTime(now.UnixNano()),
	)
}

func (e *MutableStateImpl) checkMutability(
	actionTag tag.ZapTag,
) error {

	if !e.IsWorkflowExecutionRunning() {
		e.logWarn(
			mutableStateInvalidHistoryActionMsg,
			tag.WorkflowEventID(e.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowState(e.executionState.State),
			actionTag,
		)
		return ErrWorkflowFinished
	}
	return nil
}

func (e *MutableStateImpl) generateChecksum() *persistencespb.Checksum {
	if !e.shouldGenerateChecksum() {
		return nil
	}
	csum, err := generateMutableStateChecksum(e)
	if err != nil {
		e.logWarn("error generating MutableState checksum", tag.Error(err))
		return nil
	}
	return csum
}

func (e *MutableStateImpl) shouldGenerateChecksum() bool {
	if e.namespaceEntry == nil {
		return false
	}
	return rand.Intn(100) < e.config.MutableStateChecksumGenProbability(e.namespaceEntry.Name().String())
}

func (e *MutableStateImpl) shouldVerifyChecksum() bool {
	if e.namespaceEntry == nil {
		return false
	}
	return rand.Intn(100) < e.config.MutableStateChecksumVerifyProbability(e.namespaceEntry.Name().String())
}

func (e *MutableStateImpl) shouldInvalidateCheckum() bool {
	invalidateBeforeEpochSecs := int64(e.config.MutableStateChecksumInvalidateBefore())
	if invalidateBeforeEpochSecs > 0 {
		invalidateBefore := time.Unix(invalidateBeforeEpochSecs, 0).UTC()
		return e.executionInfo.LastUpdateTime.Before(invalidateBefore)
	}
	return false
}

func (e *MutableStateImpl) createInternalServerError(
	actionTag tag.ZapTag,
) error {

	return serviceerror.NewInternal(actionTag.Field().String + " operation failed")
}

func (e *MutableStateImpl) createCallerError(
	actionTag tag.ZapTag,
	details string,
) error {
	msg := fmt.Sprintf(mutableStateInvalidHistoryActionMsgTemplate, actionTag.Field().String, details)
	return serviceerror.NewInvalidArgument(msg)
}

func (_ *MutableStateImpl) unixNanoToTime(
	timestampNanos int64,
) time.Time {

	return time.Unix(0, timestampNanos).UTC()
}

func (e *MutableStateImpl) logInfo(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(e.executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(e.executionInfo.NamespaceId))
	e.logger.Info(msg, tags...)
}

func (e *MutableStateImpl) logWarn(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(e.executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(e.executionInfo.NamespaceId))
	e.logger.Warn(msg, tags...)
}

func (e *MutableStateImpl) logError(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(e.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(e.executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(e.executionInfo.NamespaceId))
	e.logger.Error(msg, tags...)
}

func (e *MutableStateImpl) logDataInconsistency() {
	namespaceID := e.executionInfo.NamespaceId
	workflowID := e.executionInfo.WorkflowId
	runID := e.executionState.RunId

	e.logger.Error("encounter cassandra data inconsistency",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
	)
}
