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
	"context"
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
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	updatespb "go.temporal.io/server/api/update/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
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

	int64SizeBytes = 8
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
		pendingActivityTimerHeartbeats map[int64]time.Time                    // Scheduled Event ID -> LastHeartbeatTimeoutVisibilityInSeconds.
		pendingActivityInfoIDs         map[int64]*persistencespb.ActivityInfo // Scheduled Event ID -> Activity Info.
		pendingActivityIDToEventID     map[string]int64                       // Activity ID -> Scheduled Event ID of the activity.
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
		// running approximate total size of mutable state fields (except buffered events) when written to DB in bytes
		// buffered events are added to this value when calling GetApproximatePersistedSize
		approximateSize int
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
		// a flag indicating if workflow has attempted to close (complete/cancel/continue as new)
		// but failed due to undelievered buffered events
		// the flag will be unset whenever workflow task successfully completed, timedout or failed
		// due to cause other than UnhandledCommand
		workflowCloseAttempted bool

		InsertTasks map[tasks.Category][]tasks.Task

		speculativeWorkflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask

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
		metricsHandler  metrics.Handler
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

		approximateSize:  0,
		currentVersion:   namespaceEntry.FailoverVersion(),
		bufferEventsInDB: nil,
		stateInDB:        enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
		nextEventIDInDB:  common.FirstEventID,
		dbRecordVersion:  1,
		namespaceEntry:   namespaceEntry,
		appliedEvents:    make(map[string]struct{}),
		InsertTasks:      make(map[tasks.Category][]tasks.Task),

		QueryRegistry: NewQueryRegistry(),

		shard:           shard,
		clusterMetadata: shard.GetClusterMetadata(),
		eventsCache:     eventsCache,
		config:          shard.GetConfig(),
		timeSource:      shard.GetTimeSource(),
		logger:          logger,
		metricsHandler:  shard.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
	}

	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		WorkflowTaskVersion:          common.EmptyVersion,
		WorkflowTaskScheduledEventId: common.EmptyEventID,
		WorkflowTaskStartedEventId:   common.EmptyEventID,
		WorkflowTaskRequestId:        emptyUUID,
		WorkflowTaskTimeout:          timestamp.DurationFromSeconds(0),
		WorkflowTaskAttempt:          1,

		LastWorkflowTaskStartedEventId: common.EmptyEventID,

		StartTime:        timestamp.TimePtr(startTime),
		VersionHistories: versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		ExecutionStats:   &persistencespb.ExecutionStats{HistorySize: 0},
	}
	s.approximateSize += s.executionInfo.Size()
	s.executionState = &persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	s.approximateSize += s.executionState.Size()

	s.hBuilder = NewMutableHistoryBuilder(
		s.timeSource,
		s.shard.GenerateTaskIDs,
		s.currentVersion,
		common.FirstEventID,
		s.bufferEventsInDB,
		s.metricsHandler,
	)
	s.taskGenerator = taskGeneratorProvider.NewTaskGenerator(shard, s)
	s.workflowTaskManager = newWorkflowTaskStateMachine(s)

	return s
}

func newMutableStateFromDB(
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
		mutableState.approximateSize += int64SizeBytes * len(mutableState.pendingActivityInfoIDs)
	}
	for _, activityInfo := range dbRecord.ActivityInfos {
		mutableState.pendingActivityIDToEventID[activityInfo.ActivityId] = activityInfo.ScheduledEventId
		mutableState.approximateSize += activityInfo.Size()
		if (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedHeartbeat) > 0 {
			// Sets last pending timer heartbeat to year 2000.
			// This ensures at least one heartbeat task will be processed for the pending activity.
			mutableState.pendingActivityTimerHeartbeats[activityInfo.ScheduledEventId] = time.Unix(946684800, 0)
		}
	}

	if dbRecord.TimerInfos != nil {
		mutableState.pendingTimerInfoIDs = dbRecord.TimerInfos
	}
	for timerID, timerInfo := range dbRecord.TimerInfos {
		mutableState.pendingTimerEventIDToID[timerInfo.GetStartedEventId()] = timerInfo.GetTimerId()
		mutableState.approximateSize += timerInfo.Size()
		mutableState.approximateSize += len(timerID)
	}

	if dbRecord.ChildExecutionInfos != nil {
		mutableState.pendingChildExecutionInfoIDs = dbRecord.ChildExecutionInfos
		mutableState.approximateSize += int64SizeBytes * len(mutableState.pendingChildExecutionInfoIDs)
	}
	for _, childInfo := range dbRecord.ChildExecutionInfos {
		mutableState.approximateSize += childInfo.Size()
	}

	if dbRecord.RequestCancelInfos != nil {
		mutableState.pendingRequestCancelInfoIDs = dbRecord.RequestCancelInfos
		mutableState.approximateSize += int64SizeBytes * len(mutableState.pendingRequestCancelInfoIDs)
	}
	for _, cancelInfo := range dbRecord.RequestCancelInfos {
		mutableState.approximateSize += cancelInfo.Size()
	}

	if dbRecord.SignalInfos != nil {
		mutableState.pendingSignalInfoIDs = dbRecord.SignalInfos
		mutableState.approximateSize += int64SizeBytes * len(mutableState.pendingSignalInfoIDs)
	}
	for _, signalInfo := range dbRecord.SignalInfos {
		mutableState.approximateSize += signalInfo.Size()
	}

	mutableState.pendingSignalRequestedIDs = convert.StringSliceToSet(dbRecord.SignalRequestedIds)
	for requestID := range mutableState.pendingSignalRequestedIDs {
		mutableState.approximateSize += len(requestID)
	}

	mutableState.approximateSize += dbRecord.ExecutionState.Size() - mutableState.executionState.Size()
	mutableState.executionState = dbRecord.ExecutionState
	mutableState.approximateSize += dbRecord.ExecutionInfo.Size() - mutableState.executionInfo.Size()
	mutableState.executionInfo = dbRecord.ExecutionInfo

	mutableState.hBuilder = NewMutableHistoryBuilder(
		mutableState.timeSource,
		mutableState.shard.GenerateTaskIDs,
		common.EmptyVersion,
		dbRecord.NextEventId,
		dbRecord.BufferedEvents,
		mutableState.metricsHandler,
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
			mutableState.metricsHandler.Counter(metrics.MutableStateChecksumInvalidated.GetMetricName()).Record(1)
		case mutableState.shouldVerifyChecksum():
			if err := verifyMutableStateChecksum(mutableState, dbRecord.Checksum); err != nil {
				// we ignore checksum verification errors for now until this
				// feature is tested and/or we have mechanisms in place to deal
				// with these types of errors
				mutableState.metricsHandler.Counter(metrics.MutableStateChecksumMismatch.GetMetricName()).Record(1)
				mutableState.logError("mutable state checksum mismatch", tag.Error(err))
			}
		}
	}

	return mutableState, nil
}

func NewSanitizedMutableState(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	mutableStateRecord *persistencespb.WorkflowMutableState,
	lastFirstEventTxnID int64,
	lastWriteVersion int64,
) (*MutableStateImpl, error) {

	mutableState, err := newMutableStateFromDB(shard, eventsCache, logger, namespaceEntry, mutableStateRecord, 1)
	if err != nil {
		return nil, err
	}

	// sanitize data
	mutableState.executionInfo.LastFirstEventTxnId = lastFirstEventTxnID
	mutableState.executionInfo.CloseVisibilityTaskId = common.EmptyVersion
	mutableState.executionInfo.CloseTransferTaskId = common.EmptyVersion
	// TODO: after adding cluster to clock info, no need to reset clock here
	mutableState.executionInfo.ParentClock = nil
	for _, childExecutionInfo := range mutableState.pendingChildExecutionInfoIDs {
		childExecutionInfo.Clock = nil
	}
	mutableState.currentVersion = lastWriteVersion
	return mutableState, nil
}

func (ms *MutableStateImpl) CloneToProto() *persistencespb.WorkflowMutableState {
	msProto := &persistencespb.WorkflowMutableState{
		ActivityInfos:       ms.pendingActivityInfoIDs,
		TimerInfos:          ms.pendingTimerInfoIDs,
		ChildExecutionInfos: ms.pendingChildExecutionInfoIDs,
		RequestCancelInfos:  ms.pendingRequestCancelInfoIDs,
		SignalInfos:         ms.pendingSignalInfoIDs,
		SignalRequestedIds:  convert.StringSetToSlice(ms.pendingSignalRequestedIDs),
		ExecutionInfo:       ms.executionInfo,
		ExecutionState:      ms.executionState,
		NextEventId:         ms.hBuilder.NextEventID(),
		BufferedEvents:      ms.bufferEventsInDB,
		Checksum:            ms.checksum,
	}

	return common.CloneProto(msProto)
}

func (ms *MutableStateImpl) GetWorkflowKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(
		ms.executionInfo.NamespaceId,
		ms.executionInfo.WorkflowId,
		ms.executionState.RunId,
	)
}

func (ms *MutableStateImpl) GetCurrentBranchToken() ([]byte, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}
	return currentVersionHistory.GetBranchToken(), nil
}

func (ms *MutableStateImpl) getCurrentBranchTokenAndEventVersion(eventID int64) ([]byte, int64, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
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
func (ms *MutableStateImpl) SetHistoryTree(
	ctx context.Context,
	executionTimeout *time.Duration,
	runTimeout *time.Duration,
	treeID string,
) error {
	// NOTE: Unfortunately execution timeout and run timeout are not yet initialized into ms.executionInfo at this point.
	// TODO: Consider explicitly initializing mutable state with these timeout parameters instead of passing them in.

	var retentionDuration *time.Duration
	if duration := ms.namespaceEntry.Retention(); duration > 0 {
		retentionDuration = &duration
	}
	initialBranchToken, err := ms.shard.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
		ms.namespaceEntry.ID().String(),
		treeID,
		nil,
		[]*persistencespb.HistoryBranchRange{},
		runTimeout,
		executionTimeout,
		retentionDuration,
	)
	if err != nil {
		return err
	}
	return ms.SetCurrentBranchToken(initialBranchToken)
}

func (ms *MutableStateImpl) SetCurrentBranchToken(
	branchToken []byte,
) error {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
	if err != nil {
		return err
	}
	versionhistory.SetVersionHistoryBranchToken(currentVersionHistory, branchToken)
	return nil
}

func (ms *MutableStateImpl) SetHistoryBuilder(hBuilder *HistoryBuilder) {
	ms.hBuilder = hBuilder
}

func (ms *MutableStateImpl) SetBaseWorkflow(
	baseRunID string,
	baseRunLowestCommonAncestorEventID int64,
	baseRunLowestCommonAncestorEventVersion int64,
) {
	ms.executionInfo.BaseExecutionInfo = &workflowspb.BaseExecutionInfo{
		RunId:                            baseRunID,
		LowestCommonAncestorEventId:      baseRunLowestCommonAncestorEventID,
		LowestCommonAncestorEventVersion: baseRunLowestCommonAncestorEventVersion,
	}
}

func (ms *MutableStateImpl) GetBaseWorkflowInfo() *workflowspb.BaseExecutionInfo {
	return ms.executionInfo.BaseExecutionInfo
}

func (ms *MutableStateImpl) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	return ms.executionInfo
}

func (ms *MutableStateImpl) GetExecutionState() *persistencespb.WorkflowExecutionState {
	return ms.executionState
}

func (ms *MutableStateImpl) FlushBufferedEvents() {
	if ms.HasStartedWorkflowTask() {
		return
	}
	ms.updatePendingEventIDs(ms.hBuilder.FlushBufferToCurrentBatch())
}

func (ms *MutableStateImpl) UpdateCurrentVersion(
	version int64,
	forceUpdate bool,
) error {

	if state, _ := ms.GetWorkflowStateStatus(); state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// always set current version to last write version when workflow is completed
		lastWriteVersion, err := ms.GetLastWriteVersion()
		if err != nil {
			return err
		}
		ms.currentVersion = lastWriteVersion
		return nil
	}

	versionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
	if err != nil {
		return err
	}

	if !versionhistory.IsEmptyVersionHistory(versionHistory) {
		// this make sure current version >= last write version
		versionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
		if err != nil {
			return err
		}
		ms.currentVersion = versionHistoryItem.GetVersion()
	}

	if version > ms.currentVersion || forceUpdate {
		ms.currentVersion = version
	}

	ms.hBuilder = NewMutableHistoryBuilder(
		ms.timeSource,
		ms.shard.GenerateTaskIDs,
		ms.currentVersion,
		ms.nextEventIDInDB,
		ms.bufferEventsInDB,
		ms.metricsHandler,
	)

	return nil
}

func (ms *MutableStateImpl) GetCurrentVersion() int64 {

	if ms.executionInfo.VersionHistories != nil {
		return ms.currentVersion
	}

	return common.EmptyVersion
}

func (ms *MutableStateImpl) GetStartVersion() (int64, error) {

	if ms.executionInfo.VersionHistories != nil {
		versionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
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

func (ms *MutableStateImpl) GetLastWriteVersion() (int64, error) {

	if ms.executionInfo.VersionHistories != nil {
		versionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
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

func (ms *MutableStateImpl) IsCurrentWorkflowGuaranteed() bool {
	// stateInDB is used like a bloom filter:
	//
	// 1. stateInDB being created / running meaning that this workflow must be the current
	//  workflow (assuming there is no rebuild of mutable state).
	// 2. stateInDB being completed does not guarantee this workflow being the current workflow
	// 3. stateInDB being zombie guarantees this workflow not being the current workflow
	// 4. stateInDB cannot be void, void is only possible when mutable state is just initialized

	switch ms.stateInDB {
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
		panic(fmt.Sprintf("unknown workflow state: %v", ms.executionState.State))
	}
}

func (ms *MutableStateImpl) GetNamespaceEntry() *namespace.Namespace {
	return ms.namespaceEntry
}

func (ms *MutableStateImpl) CurrentTaskQueue() *taskqueuepb.TaskQueue {
	if ms.IsStickyTaskQueueSet() {
		return &taskqueuepb.TaskQueue{
			Name:       ms.executionInfo.StickyTaskQueue,
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: ms.executionInfo.TaskQueue,
		}
	}
	return &taskqueuepb.TaskQueue{
		Name: ms.executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
}

func (ms *MutableStateImpl) SetStickyTaskQueue(name string, scheduleToStartTimeout *time.Duration) {
	ms.executionInfo.StickyTaskQueue = name
	ms.executionInfo.StickyScheduleToStartTimeout = scheduleToStartTimeout
}

func (ms *MutableStateImpl) ClearStickyTaskQueue() {
	ms.executionInfo.StickyTaskQueue = ""
	ms.executionInfo.StickyScheduleToStartTimeout = nil
}

func (ms *MutableStateImpl) IsStickyTaskQueueSet() bool {
	return ms.executionInfo.StickyTaskQueue != ""
}

// TaskQueueScheduleToStartTimeout returns TaskQueue struct and corresponding StartToClose timeout.
// Task queue kind (sticky or normal) and timeout are set based on comparison of normal task queue name
// in mutable state and provided name.
func (ms *MutableStateImpl) TaskQueueScheduleToStartTimeout(name string) (*taskqueuepb.TaskQueue, *time.Duration) {
	if ms.executionInfo.TaskQueue != name {
		return &taskqueuepb.TaskQueue{
			Name:       ms.executionInfo.StickyTaskQueue,
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: ms.executionInfo.TaskQueue,
		}, ms.executionInfo.StickyScheduleToStartTimeout
	}
	return &taskqueuepb.TaskQueue{
		Name: ms.executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}, ms.executionInfo.WorkflowRunTimeout // No WT ScheduleToStart timeout for normal task queue.
}

func (ms *MutableStateImpl) GetWorkflowType() *commonpb.WorkflowType {
	wType := &commonpb.WorkflowType{}
	wType.Name = ms.executionInfo.WorkflowTypeName

	return wType
}

func (ms *MutableStateImpl) GetQueryRegistry() QueryRegistry {
	return ms.QueryRegistry
}

func (ms *MutableStateImpl) VisitUpdates(visitor func(updID string, updInfo *updatespb.UpdateInfo)) {
	for updID, updInfo := range ms.executionInfo.GetUpdateInfos() {
		visitor(updID, updInfo)
	}
}

func (ms *MutableStateImpl) GetUpdateOutcome(
	ctx context.Context,
	updateID string,
) (*updatepb.Outcome, error) {
	if ms.executionInfo.UpdateInfos == nil {
		return nil, serviceerror.NewNotFound("update not found")
	}
	rec, ok := ms.executionInfo.UpdateInfos[updateID]
	if !ok {
		return nil, serviceerror.NewNotFound("update not found")
	}
	completion := rec.GetCompletion()
	if completion == nil {
		return nil, serviceerror.NewInternal("update has not completed")
	}
	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(completion.EventId)
	if err != nil {
		return nil, err
	}
	eventKey := events.EventKey{
		NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
		WorkflowID:  ms.executionInfo.WorkflowId,
		RunID:       ms.executionState.RunId,
		EventID:     completion.EventId,
		Version:     version,
	}
	event, err := ms.eventsCache.GetEvent(ctx, eventKey, completion.EventBatchId, currentBranchToken)
	if err != nil {
		return nil, err
	}
	attrs := event.GetWorkflowExecutionUpdateCompletedEventAttributes()
	if attrs == nil {
		return nil, serviceerror.NewInternal("event pointer does not reference an update completed event")
	}
	return attrs.GetOutcome(), nil
}

func (ms *MutableStateImpl) GetActivityScheduledEvent(
	ctx context.Context,
	scheduledEventID int64,
) (*historypb.HistoryEvent, error) {

	ai, ok := ms.pendingActivityInfoIDs[scheduledEventID]
	if !ok {
		return nil, ErrMissingActivityInfo
	}

	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(ai.ScheduledEventId)
	if err != nil {
		return nil, err
	}
	event, err := ms.eventsCache.GetEvent(
		ctx,
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     ai.ScheduledEventId,
			Version:     version,
		},
		ai.ScheduledEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			return nil, ErrMissingActivityScheduledEvent
		}
		return nil, err
	}
	return event, nil
}

// GetActivityInfo gives details about an activity that is currently in progress.
func (ms *MutableStateImpl) GetActivityInfo(
	scheduledEventID int64,
) (*persistencespb.ActivityInfo, bool) {

	ai, ok := ms.pendingActivityInfoIDs[scheduledEventID]
	return ai, ok
}

// GetActivityInfoWithTimerHeartbeat gives details about an activity that is currently in progress.
func (ms *MutableStateImpl) GetActivityInfoWithTimerHeartbeat(
	scheduledEventID int64,
) (*persistencespb.ActivityInfo, time.Time, bool) {
	ai, ok := ms.pendingActivityInfoIDs[scheduledEventID]
	if !ok {
		return nil, time.Time{}, false
	}
	timerVis, ok := ms.pendingActivityTimerHeartbeats[scheduledEventID]

	return ai, timerVis, ok
}

// GetActivityByActivityID gives details about an activity that is currently in progress.
func (ms *MutableStateImpl) GetActivityByActivityID(
	activityID string,
) (*persistencespb.ActivityInfo, bool) {

	eventID, ok := ms.pendingActivityIDToEventID[activityID]
	if !ok {
		return nil, false
	}
	return ms.GetActivityInfo(eventID)
}

// GetActivityType gets the ActivityType from ActivityInfo if set,
// or from the events history otherwise for backwards compatibility.
func (ms *MutableStateImpl) GetActivityType(
	ctx context.Context,
	ai *persistencespb.ActivityInfo,
) (*commonpb.ActivityType, error) {
	if ai.GetActivityType() != nil {
		return ai.GetActivityType(), nil
	}
	// For backwards compatibility in case ActivityType is not set in ActivityInfo.
	scheduledEvent, err := ms.GetActivityScheduledEvent(ctx, ai.ScheduledEventId)
	if err != nil {
		return nil, err
	}
	return scheduledEvent.GetActivityTaskScheduledEventAttributes().ActivityType, nil
}

// GetChildExecutionInfo gives details about a child execution that is currently in progress.
func (ms *MutableStateImpl) GetChildExecutionInfo(
	initiatedEventID int64,
) (*persistencespb.ChildExecutionInfo, bool) {

	ci, ok := ms.pendingChildExecutionInfoIDs[initiatedEventID]
	return ci, ok
}

// GetChildExecutionInitiatedEvent reads out the ChildExecutionInitiatedEvent from mutable state for in-progress child
// executions
func (ms *MutableStateImpl) GetChildExecutionInitiatedEvent(
	ctx context.Context,
	initiatedEventID int64,
) (*historypb.HistoryEvent, error) {

	ci, ok := ms.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingChildWorkflowInfo
	}

	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(ci.InitiatedEventId)
	if err != nil {
		return nil, err
	}
	event, err := ms.eventsCache.GetEvent(
		ctx,
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     ci.InitiatedEventId,
			Version:     version,
		},
		ci.InitiatedEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			return nil, ErrMissingChildWorkflowInitiatedEvent
		}
		return nil, err
	}
	return event, nil
}

// GetRequestCancelInfo gives details about a request cancellation that is currently in progress.
func (ms *MutableStateImpl) GetRequestCancelInfo(
	initiatedEventID int64,
) (*persistencespb.RequestCancelInfo, bool) {

	ri, ok := ms.pendingRequestCancelInfoIDs[initiatedEventID]
	return ri, ok
}

func (ms *MutableStateImpl) GetRequesteCancelExternalInitiatedEvent(
	ctx context.Context,
	initiatedEventID int64,
) (*historypb.HistoryEvent, error) {
	ri, ok := ms.pendingRequestCancelInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingRequestCancelInfo
	}

	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(ri.InitiatedEventId)
	if err != nil {
		return nil, err
	}
	event, err := ms.eventsCache.GetEvent(
		ctx,
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     ri.InitiatedEventId,
			Version:     version,
		},
		ri.InitiatedEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			return nil, ErrMissingRequestCancelInfo
		}
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) GetRetryBackoffDuration(
	failure *failurepb.Failure,
) (time.Duration, enumspb.RetryState) {

	info := ms.executionInfo
	if !info.HasRetryPolicy {
		return backoff.NoBackoff, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
	}

	return getBackoffInterval(
		ms.timeSource.Now(),
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

func (ms *MutableStateImpl) GetCronBackoffDuration() time.Duration {
	if ms.executionInfo.CronSchedule == "" {
		return backoff.NoBackoff
	}
	executionTime := timestamp.TimeValue(ms.GetExecutionInfo().GetExecutionTime())
	return backoff.GetBackoffForNextSchedule(ms.executionInfo.CronSchedule, executionTime, ms.timeSource.Now())
}

// GetSignalInfo get the details about a signal request that is currently in progress.
func (ms *MutableStateImpl) GetSignalInfo(
	initiatedEventID int64,
) (*persistencespb.SignalInfo, bool) {

	ri, ok := ms.pendingSignalInfoIDs[initiatedEventID]
	return ri, ok
}

// GetSignalExternalInitiatedEvent get the details about signal external workflow
func (ms *MutableStateImpl) GetSignalExternalInitiatedEvent(
	ctx context.Context,
	initiatedEventID int64,
) (*historypb.HistoryEvent, error) {
	si, ok := ms.pendingSignalInfoIDs[initiatedEventID]
	if !ok {
		return nil, ErrMissingSignalInfo
	}

	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(si.InitiatedEventId)
	if err != nil {
		return nil, err
	}
	event, err := ms.eventsCache.GetEvent(
		ctx,
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     si.InitiatedEventId,
			Version:     version,
		},
		si.InitiatedEventBatchId,
		currentBranchToken,
	)
	if err != nil {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			return nil, ErrMissingSignalInitiatedEvent
		}
		return nil, err
	}
	return event, nil
}

// GetCompletionEvent retrieves the workflow completion event from mutable state
func (ms *MutableStateImpl) GetCompletionEvent(
	ctx context.Context,
) (*historypb.HistoryEvent, error) {
	if ms.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil, ErrMissingWorkflowCompletionEvent
	}

	// Completion EventID is always one less than NextEventID after workflow is completed
	completionEventID := ms.hBuilder.NextEventID() - 1
	firstEventID := ms.executionInfo.CompletionEventBatchId

	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(completionEventID)
	if err != nil {
		return nil, err
	}

	event, err := ms.eventsCache.GetEvent(
		ctx,
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     completionEventID,
			Version:     version,
		},
		firstEventID,
		currentBranchToken,
	)
	if err != nil {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			return nil, ErrMissingWorkflowCompletionEvent
		}
		return nil, err
	}
	return event, nil
}

// GetWorkflowCloseTime returns workflow closed time, returns nil for open workflow
func (ms *MutableStateImpl) GetWorkflowCloseTime(ctx context.Context) (*time.Time, error) {
	if ms.executionState.GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED && ms.executionInfo.CloseTime == nil {
		// This is for backward compatible. Prior to v1.16 does not have close time in mutable state (Added by 05/21/2022).
		// TODO: remove this logic when all mutable state contains close time.
		completionEvent, err := ms.GetCompletionEvent(ctx)
		if err != nil {
			return nil, err
		}
		return completionEvent.GetEventTime(), nil
	}

	return ms.executionInfo.CloseTime, nil
}

// GetStartEvent retrieves the workflow start event from mutable state
func (ms *MutableStateImpl) GetStartEvent(
	ctx context.Context,
) (*historypb.HistoryEvent, error) {

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	startVersion, err := ms.GetStartVersion()
	if err != nil {
		return nil, err
	}

	event, err := ms.eventsCache.GetEvent(
		ctx,
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     common.FirstEventID,
			Version:     startVersion,
		},
		common.FirstEventID,
		currentBranchToken,
	)
	if err != nil {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			return nil, ErrMissingWorkflowStartEvent
		}
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) GetFirstRunID(
	ctx context.Context,
) (string, error) {
	firstRunID := ms.executionInfo.FirstExecutionRunId
	// This is needed for backwards compatibility.  Workflow execution create with Temporal release v0.28.0 or earlier
	// does not have FirstExecutionRunID stored as part of mutable state.  If this is not set then load it from
	// workflow execution started event.
	if len(firstRunID) != 0 {
		return firstRunID, nil
	}
	currentStartEvent, err := ms.GetStartEvent(ctx)
	if err != nil {
		return "", err
	}
	return currentStartEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstExecutionRunId(), nil
}

// DeletePendingChildExecution deletes details about a ChildExecutionInfo.
func (ms *MutableStateImpl) DeletePendingChildExecution(
	initiatedEventID int64,
) error {

	if prev, ok := ms.pendingChildExecutionInfoIDs[initiatedEventID]; ok {
		ms.approximateSize -= prev.Size() + int64SizeBytes
		delete(ms.pendingChildExecutionInfoIDs, initiatedEventID)
	} else {
		ms.logError(
			fmt.Sprintf("unable to find child workflow event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		ms.logDataInconsistency()
	}

	delete(ms.updateChildExecutionInfos, initiatedEventID)
	ms.deleteChildExecutionInfos[initiatedEventID] = struct{}{}
	return nil
}

// DeletePendingRequestCancel deletes details about a RequestCancelInfo.
func (ms *MutableStateImpl) DeletePendingRequestCancel(
	initiatedEventID int64,
) error {

	if prev, ok := ms.pendingRequestCancelInfoIDs[initiatedEventID]; ok {
		ms.approximateSize -= prev.Size() + int64SizeBytes
		delete(ms.pendingRequestCancelInfoIDs, initiatedEventID)
	} else {
		ms.logError(
			fmt.Sprintf("unable to find request cancel external workflow event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		ms.logDataInconsistency()
	}

	delete(ms.updateRequestCancelInfos, initiatedEventID)
	ms.deleteRequestCancelInfos[initiatedEventID] = struct{}{}
	return nil
}

// DeletePendingSignal deletes details about a SignalInfo
func (ms *MutableStateImpl) DeletePendingSignal(
	initiatedEventID int64,
) error {

	if prev, ok := ms.pendingSignalInfoIDs[initiatedEventID]; ok {
		ms.approximateSize -= prev.Size() + int64SizeBytes
		delete(ms.pendingSignalInfoIDs, initiatedEventID)
	} else {
		ms.logError(
			fmt.Sprintf("unable to find signal external workflow event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		ms.logDataInconsistency()
	}

	delete(ms.updateSignalInfos, initiatedEventID)
	ms.deleteSignalInfos[initiatedEventID] = struct{}{}
	return nil
}

func (ms *MutableStateImpl) writeEventToCache(
	event *historypb.HistoryEvent,
) {
	// For start event: store it within events cache so the recordWorkflowStarted transfer task doesn't need to
	// load it from database
	// For completion event: store it within events cache so we can communicate the result to parent execution
	// during the processing of DeleteTransferTask without loading this event from database
	// For Update Accepted/Completed event: store it in here so that Update
	// disposition lookups can be fast
	ms.eventsCache.PutEvent(
		events.EventKey{
			NamespaceID: namespace.ID(ms.executionInfo.NamespaceId),
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			EventID:     event.GetEventId(),
			Version:     event.GetVersion(),
		},
		event,
	)
}

func (ms *MutableStateImpl) HasParentExecution() bool {
	return ms.executionInfo.ParentNamespaceId != "" && ms.executionInfo.ParentWorkflowId != ""
}

func (ms *MutableStateImpl) UpdateActivityProgress(
	ai *persistencespb.ActivityInfo,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) {
	if prev, existed := ms.pendingActivityInfoIDs[ai.ScheduledEventId]; existed {
		ms.approximateSize -= prev.Size()
	}
	ai.Version = ms.GetCurrentVersion()
	ai.LastHeartbeatDetails = request.Details
	now := ms.timeSource.Now()
	ai.LastHeartbeatUpdateTime = &now
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.approximateSize += ai.Size()
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
}

// ReplicateActivityInfo replicate the necessary activity information
func (ms *MutableStateImpl) ReplicateActivityInfo(
	request *historyservice.SyncActivityRequest,
	resetActivityTimerTaskStatus bool,
) error {
	ai, ok := ms.pendingActivityInfoIDs[request.GetScheduledEventId()]
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find activity event ID: %v in mutable state", request.GetScheduledEventId()),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ms.approximateSize -= ai.Size()

	ai.Version = request.GetVersion()
	ai.ScheduledTime = request.GetScheduledTime()
	ai.StartedEventId = request.GetStartedEventId()
	ai.LastHeartbeatUpdateTime = request.GetLastHeartbeatTime()
	if ai.StartedEventId == common.EmptyEventID {
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

	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.approximateSize += ai.Size()
	return nil
}

// UpdateActivity updates an activity
func (ms *MutableStateImpl) UpdateActivity(
	ai *persistencespb.ActivityInfo,
) error {

	prev, ok := ms.pendingActivityInfoIDs[ai.ScheduledEventId]
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find activity ID: %v in mutable state", ai.ActivityId),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ms.pendingActivityInfoIDs[ai.ScheduledEventId] = ai
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.approximateSize += ai.Size() - prev.Size()
	return nil
}

// UpdateActivityWithTimerHeartbeat updates an activity
func (ms *MutableStateImpl) UpdateActivityWithTimerHeartbeat(
	ai *persistencespb.ActivityInfo,
	timerTimeoutVisibility time.Time,
) error {

	err := ms.UpdateActivity(ai)
	if err != nil {
		return err
	}

	ms.pendingActivityTimerHeartbeats[ai.ScheduledEventId] = timerTimeoutVisibility
	return nil
}

// DeleteActivity deletes details about an activity.
func (ms *MutableStateImpl) DeleteActivity(
	scheduledEventID int64,
) error {

	if activityInfo, ok := ms.pendingActivityInfoIDs[scheduledEventID]; ok {
		delete(ms.pendingActivityInfoIDs, scheduledEventID)
		delete(ms.pendingActivityTimerHeartbeats, scheduledEventID)
		ms.approximateSize -= activityInfo.Size() + int64SizeBytes

		if _, ok = ms.pendingActivityIDToEventID[activityInfo.ActivityId]; ok {
			delete(ms.pendingActivityIDToEventID, activityInfo.ActivityId)
		} else {
			ms.logError(
				fmt.Sprintf("unable to find activity ID: %v in mutable state", activityInfo.ActivityId),
				tag.ErrorTypeInvalidMutableStateAction,
			)
			// log data inconsistency instead of returning an error
			ms.logDataInconsistency()
		}
	} else {
		ms.logError(
			fmt.Sprintf("unable to find activity event id: %v in mutable state", scheduledEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		ms.logDataInconsistency()
	}

	delete(ms.updateActivityInfos, scheduledEventID)
	delete(ms.syncActivityTasks, scheduledEventID)
	ms.deleteActivityInfos[scheduledEventID] = struct{}{}
	return nil
}

// GetUserTimerInfo gives details about a user timer.
func (ms *MutableStateImpl) GetUserTimerInfo(
	timerID string,
) (*persistencespb.TimerInfo, bool) {

	timerInfo, ok := ms.pendingTimerInfoIDs[timerID]
	return timerInfo, ok
}

// GetUserTimerInfoByEventID gives details about a user timer.
func (ms *MutableStateImpl) GetUserTimerInfoByEventID(
	startEventID int64,
) (*persistencespb.TimerInfo, bool) {

	timerID, ok := ms.pendingTimerEventIDToID[startEventID]
	if !ok {
		return nil, false
	}
	return ms.GetUserTimerInfo(timerID)
}

// UpdateUserTimer updates the user timer in progress.
func (ms *MutableStateImpl) UpdateUserTimer(
	ti *persistencespb.TimerInfo,
) error {

	timerID, ok := ms.pendingTimerEventIDToID[ti.GetStartedEventId()]
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find timer event ID: %v in mutable state", ti.GetStartedEventId()),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingTimerInfo
	}

	if _, ok := ms.pendingTimerInfoIDs[timerID]; !ok {
		ms.logError(
			fmt.Sprintf("unable to find timer ID: %v in mutable state", timerID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingTimerInfo
	}

	ms.pendingTimerInfoIDs[ti.TimerId] = ti
	ms.updateTimerInfos[ti.TimerId] = ti
	return nil
}

// DeleteUserTimer deletes an user timer.
func (ms *MutableStateImpl) DeleteUserTimer(
	timerID string,
) error {

	if timerInfo, ok := ms.pendingTimerInfoIDs[timerID]; ok {
		delete(ms.pendingTimerInfoIDs, timerID)
		ms.approximateSize -= timerInfo.Size() + len(timerID)

		if _, ok = ms.pendingTimerEventIDToID[timerInfo.GetStartedEventId()]; ok {
			delete(ms.pendingTimerEventIDToID, timerInfo.GetStartedEventId())
		} else {
			ms.logError(
				fmt.Sprintf("unable to find timer event ID: %v in mutable state", timerID),
				tag.ErrorTypeInvalidMutableStateAction,
			)
			// log data inconsistency instead of returning an error
			ms.logDataInconsistency()
		}
	} else {
		ms.logError(
			fmt.Sprintf("unable to find timer ID: %v in mutable state", timerID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		ms.logDataInconsistency()
	}

	delete(ms.updateTimerInfos, timerID)
	ms.deleteTimerInfos[timerID] = struct{}{}
	return nil
}

// GetWorkflowTaskByID returns details about the current workflow task by scheduled event ID.
func (ms *MutableStateImpl) GetWorkflowTaskByID(scheduledEventID int64) *WorkflowTaskInfo {
	return ms.workflowTaskManager.GetWorkflowTaskByID(scheduledEventID)
}

func (ms *MutableStateImpl) GetPendingActivityInfos() map[int64]*persistencespb.ActivityInfo {
	return ms.pendingActivityInfoIDs
}

func (ms *MutableStateImpl) GetPendingTimerInfos() map[string]*persistencespb.TimerInfo {
	return ms.pendingTimerInfoIDs
}

func (ms *MutableStateImpl) GetPendingChildExecutionInfos() map[int64]*persistencespb.ChildExecutionInfo {
	return ms.pendingChildExecutionInfoIDs
}

func (ms *MutableStateImpl) GetPendingRequestCancelExternalInfos() map[int64]*persistencespb.RequestCancelInfo {
	return ms.pendingRequestCancelInfoIDs
}

func (ms *MutableStateImpl) GetPendingSignalExternalInfos() map[int64]*persistencespb.SignalInfo {
	return ms.pendingSignalInfoIDs
}

func (ms *MutableStateImpl) HadOrHasWorkflowTask() bool {
	return ms.workflowTaskManager.HadOrHasWorkflowTask()
}

func (ms *MutableStateImpl) HasPendingWorkflowTask() bool {
	return ms.workflowTaskManager.HasPendingWorkflowTask()
}

func (ms *MutableStateImpl) GetPendingWorkflowTask() *WorkflowTaskInfo {
	return ms.workflowTaskManager.GetPendingWorkflowTask()
}

func (ms *MutableStateImpl) HasStartedWorkflowTask() bool {
	return ms.workflowTaskManager.HasStartedWorkflowTask()
}

func (ms *MutableStateImpl) GetStartedWorkflowTask() *WorkflowTaskInfo {
	return ms.workflowTaskManager.GetStartedWorkflowTask()
}

func (ms *MutableStateImpl) IsTransientWorkflowTask() bool {
	return ms.executionInfo.WorkflowTaskAttempt > 1
}

func (ms *MutableStateImpl) ClearTransientWorkflowTask() error {
	if !ms.HasStartedWorkflowTask() {
		return serviceerror.NewInternal("cannot clear transient workflow task when task is missing")
	}
	if !ms.IsTransientWorkflowTask() {
		return serviceerror.NewInternal("cannot clear transient workflow task when task is not transient")
	}
	// this is transient workflow task
	if ms.HasBufferedEvents() {
		return serviceerror.NewInternal("cannot clear transient workflow task when there are buffered events")
	}
	// no buffered event
	emptyWorkflowTaskInfo := &WorkflowTaskInfo{
		Version:             common.EmptyVersion,
		ScheduledEventID:    common.EmptyEventID,
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: timestamp.DurationFromSeconds(0),
		Attempt:             1,
		StartedTime:         timestamp.UnixOrZeroTimePtr(0),
		ScheduledTime:       timestamp.UnixOrZeroTimePtr(0),

		TaskQueue:             nil,
		OriginalScheduledTime: timestamp.UnixOrZeroTimePtr(0),
		Type:                  enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED,

		SuggestContinueAsNew: false,
		HistorySizeBytes:     0,
	}
	ms.workflowTaskManager.UpdateWorkflowTask(emptyWorkflowTaskInfo)
	return nil
}

func (ms *MutableStateImpl) GetWorkerVersionStamp() *commonpb.WorkerVersionStamp {
	return ms.executionInfo.WorkerVersionStamp
}

func (ms *MutableStateImpl) HasBufferedEvents() bool {
	return ms.hBuilder.HasBufferEvents()
}

// HasAnyBufferedEvent returns true if there is at least one buffered event that matches the provided filter.
func (ms *MutableStateImpl) HasAnyBufferedEvent(filter BufferedEventFilter) bool {
	return ms.hBuilder.HasAnyBufferedEvent(filter)
}

// GetLastFirstEventIDTxnID returns last first event ID and corresponding transaction ID
// first event ID is the ID of a batch of events in a single history events record
func (ms *MutableStateImpl) GetLastFirstEventIDTxnID() (int64, int64) {
	return ms.executionInfo.LastFirstEventId, ms.executionInfo.LastFirstEventTxnId
}

// GetNextEventID returns next event ID
func (ms *MutableStateImpl) GetNextEventID() int64 {
	return ms.hBuilder.NextEventID()
}

// GetLastWorkflowTaskStartedEventID returns last started workflow task event ID
func (ms *MutableStateImpl) GetLastWorkflowTaskStartedEventID() int64 {
	return ms.executionInfo.LastWorkflowTaskStartedEventId
}

func (ms *MutableStateImpl) IsWorkflowExecutionRunning() bool {
	switch ms.executionState.State {
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
		panic(fmt.Sprintf("unknown workflow state: %v", ms.executionState.State))
	}
}

func (ms *MutableStateImpl) IsCancelRequested() bool {
	return ms.executionInfo.CancelRequested
}

func (ms *MutableStateImpl) IsWorkflowCloseAttempted() bool {
	return ms.workflowCloseAttempted
}

func (ms *MutableStateImpl) IsSignalRequested(
	requestID string,
) bool {

	if _, ok := ms.pendingSignalRequestedIDs[requestID]; ok {
		return true
	}
	return false
}

func (ms *MutableStateImpl) IsWorkflowPendingOnWorkflowTaskBackoff() bool {

	workflowTaskBackoff := timestamp.TimeValue(ms.executionInfo.GetExecutionTime()).After(timestamp.TimeValue(ms.executionInfo.GetStartTime()))
	if workflowTaskBackoff && !ms.HadOrHasWorkflowTask() {
		return true
	}
	return false
}

// GetApproximatePersistedSize returns approximate size of in-memory objects that will be written to
// persistence + size of buffered events in history builder if they will not be flushed
func (ms *MutableStateImpl) GetApproximatePersistedSize() int {
	// include buffered events in the size if they will not be flushed
	if ms.BufferSizeAcceptable() && ms.HasStartedWorkflowTask() {
		return ms.approximateSize + ms.hBuilder.SizeInBytesOfBufferedEvents()
	}
	return ms.approximateSize
}

func (ms *MutableStateImpl) AddSignalRequested(
	requestID string,
) {

	if ms.pendingSignalRequestedIDs == nil {
		ms.pendingSignalRequestedIDs = make(map[string]struct{})
	}
	if ms.updateSignalRequestedIDs == nil {
		ms.updateSignalRequestedIDs = make(map[string]struct{})
	}
	ms.pendingSignalRequestedIDs[requestID] = struct{}{} // add requestID to set
	ms.updateSignalRequestedIDs[requestID] = struct{}{}
	ms.approximateSize += len(requestID)
}

func (ms *MutableStateImpl) DeleteSignalRequested(
	requestID string,
) {

	delete(ms.pendingSignalRequestedIDs, requestID)
	delete(ms.updateSignalRequestedIDs, requestID)
	ms.deleteSignalRequestedIDs[requestID] = struct{}{}
	ms.approximateSize -= len(requestID)
}

func (ms *MutableStateImpl) addWorkflowExecutionStartedEventForContinueAsNew(
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
		Namespace:                ms.namespaceEntry.Name().String(),
		WorkflowId:               execution.WorkflowId,
		TaskQueue:                tq,
		WorkflowType:             wType,
		WorkflowExecutionTimeout: previousExecutionInfo.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       runTimeout,
		WorkflowTaskTimeout:      taskTimeout,
		Input:                    command.Input,
		Header:                   command.Header,
		RetryPolicy:              command.RetryPolicy,
		CronSchedule:             command.CronSchedule,
		Memo:                     command.Memo,
		SearchAttributes:         command.SearchAttributes,
		// No need to request eager execution here (for now)
		RequestEagerExecution: false,
	}

	enums.SetDefaultContinueAsNewInitiator(&command.Initiator)

	// Copy version stamp to new workflow only if:
	// - command says to use compatible version
	// - using versioning
	var sourceVersionStamp *commonpb.WorkerVersionStamp
	if command.UseCompatibleVersion {
		sourceVersionStamp = worker_versioning.StampIfUsingVersioning(previousExecutionInfo.WorkerVersionStamp)
	}

	req := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:            ms.namespaceEntry.ID().String(),
		StartRequest:           createRequest,
		ParentExecutionInfo:    parentExecutionInfo,
		LastCompletionResult:   command.LastCompletionResult,
		ContinuedFailure:       command.GetFailure(),
		ContinueAsNewInitiator: command.Initiator,
		// enforce minimal interval between runs to prevent tight loop continue as new spin.
		FirstWorkflowTaskBackoff: previousExecutionState.ContinueAsNewMinBackoff(command.BackoffStartInterval),
		SourceVersionStamp:       sourceVersionStamp,
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

	event, err := ms.AddWorkflowExecutionStartedEventWithOptions(
		execution,
		req,
		previousExecutionInfo.AutoResetPoints,
		previousExecutionState.GetExecutionState().GetRunId(),
		firstRunID,
	)
	if err != nil {
		return nil, err
	}
	var parentClock *clockspb.VectorClock
	if parentExecutionInfo != nil {
		parentClock = parentExecutionInfo.Clock
	}
	if _, err = ms.AddFirstWorkflowTaskScheduled(parentClock, event, false); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ContinueAsNewMinBackoff(backoffDuration *time.Duration) *time.Duration {
	// lifetime of previous execution
	lifetime := ms.timeSource.Now().Sub(ms.executionInfo.StartTime.UTC())
	if ms.executionInfo.ExecutionTime != nil {
		lifetime = ms.timeSource.Now().Sub(ms.executionInfo.ExecutionTime.UTC())
	}

	interval := lifetime
	if backoffDuration != nil {
		// already has a backoff, add it to interval
		interval += *backoffDuration
	}
	// minimal interval for continue as new to prevent tight continue as new loop
	minInterval := ms.config.ContinueAsNewMinInterval(ms.namespaceEntry.Name().String())
	if interval < minInterval {
		// enforce a minimal backoff
		return timestamp.DurationPtr(minInterval - lifetime)
	}

	return backoffDuration
}

func (ms *MutableStateImpl) AddWorkflowExecutionStartedEvent(
	execution commonpb.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (*historypb.HistoryEvent, error) {

	return ms.AddWorkflowExecutionStartedEventWithOptions(
		execution,
		startRequest,
		nil, // resetPoints
		"",  // prevRunID
		execution.GetRunId(),
	)
}

func (ms *MutableStateImpl) AddWorkflowExecutionStartedEventWithOptions(
	execution commonpb.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	eventID := ms.GetNextEventID()
	if eventID != common.FirstEventID {
		ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(eventID),
			tag.ErrorTypeInvalidHistoryAction)
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddWorkflowExecutionStartedEvent(
		*ms.executionInfo.StartTime,
		startRequest,
		resetPoints,
		prevRunID,
		firstRunID,
		execution.GetRunId(),
	)
	if err := ms.ReplicateWorkflowExecutionStartedEvent(
		startRequest.GetParentExecutionInfo().GetClock(),
		execution,
		startRequest.StartRequest.GetRequestId(),
		event,
	); err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowStartTasks(
		event,
	); err != nil {
		return nil, err
	}
	if err := ms.taskGenerator.GenerateRecordWorkflowStartedTasks(
		event,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionStartedEvent(
	parentClock *clockspb.VectorClock,
	execution commonpb.WorkflowExecution,
	requestID string,
	startEvent *historypb.HistoryEvent,
) error {

	ms.approximateSize -= ms.executionInfo.Size()
	event := startEvent.GetWorkflowExecutionStartedEventAttributes()
	ms.executionState.CreateRequestId = requestID
	ms.executionState.RunId = execution.GetRunId()
	ms.executionInfo.NamespaceId = ms.namespaceEntry.ID().String()
	ms.executionInfo.WorkflowId = execution.GetWorkflowId()
	ms.executionInfo.FirstExecutionRunId = event.GetFirstExecutionRunId()
	ms.executionInfo.TaskQueue = event.TaskQueue.GetName()
	ms.executionInfo.WorkflowTypeName = event.WorkflowType.GetName()
	ms.executionInfo.WorkflowRunTimeout = event.GetWorkflowRunTimeout()
	ms.executionInfo.WorkflowExecutionTimeout = event.GetWorkflowExecutionTimeout()
	ms.executionInfo.DefaultWorkflowTaskTimeout = event.GetWorkflowTaskTimeout()

	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	); err != nil {
		return err
	}
	ms.executionInfo.LastWorkflowTaskStartedEventId = common.EmptyEventID
	ms.executionInfo.LastFirstEventId = startEvent.GetEventId()

	ms.executionInfo.WorkflowTaskVersion = common.EmptyVersion
	ms.executionInfo.WorkflowTaskScheduledEventId = common.EmptyEventID
	ms.executionInfo.WorkflowTaskStartedEventId = common.EmptyEventID
	ms.executionInfo.WorkflowTaskRequestId = emptyUUID
	ms.executionInfo.WorkflowTaskTimeout = timestamp.DurationFromSeconds(0)

	ms.executionInfo.CronSchedule = event.GetCronSchedule()

	if event.ParentWorkflowExecution != nil {
		ms.executionInfo.ParentNamespaceId = event.GetParentWorkflowNamespaceId()
		ms.executionInfo.ParentWorkflowId = event.ParentWorkflowExecution.GetWorkflowId()
		ms.executionInfo.ParentRunId = event.ParentWorkflowExecution.GetRunId()
		ms.executionInfo.ParentClock = parentClock
	}

	if event.ParentInitiatedEventId != 0 {
		ms.executionInfo.ParentInitiatedId = event.GetParentInitiatedEventId()
	} else {
		ms.executionInfo.ParentInitiatedId = common.EmptyEventID
	}

	if event.ParentInitiatedEventVersion != 0 {
		ms.executionInfo.ParentInitiatedVersion = event.GetParentInitiatedEventVersion()
	} else {
		ms.executionInfo.ParentInitiatedVersion = common.EmptyVersion
	}

	ms.executionInfo.ExecutionTime = timestamp.TimePtr(
		ms.executionInfo.StartTime.Add(timestamp.DurationValue(event.GetFirstWorkflowTaskBackoff())),
	)

	ms.executionInfo.Attempt = event.GetAttempt()
	if !timestamp.TimeValue(event.GetWorkflowExecutionExpirationTime()).IsZero() {
		ms.executionInfo.WorkflowExecutionExpirationTime = event.GetWorkflowExecutionExpirationTime()
	}

	var workflowRunTimeoutTime time.Time
	workflowRunTimeoutDuration := timestamp.DurationValue(ms.executionInfo.WorkflowRunTimeout)
	// if workflowRunTimeoutDuration == 0 then the workflowRunTimeoutTime will be 0
	// meaning that there is not workflow run timeout
	if workflowRunTimeoutDuration != 0 {
		firstWorkflowTaskDelayDuration := timestamp.DurationValue(event.GetFirstWorkflowTaskBackoff())
		workflowRunTimeoutDuration = workflowRunTimeoutDuration + firstWorkflowTaskDelayDuration
		workflowRunTimeoutTime = ms.executionInfo.StartTime.Add(workflowRunTimeoutDuration)

		workflowExecutionTimeoutTime := timestamp.TimeValue(ms.executionInfo.WorkflowExecutionExpirationTime)
		if !workflowExecutionTimeoutTime.IsZero() && workflowRunTimeoutTime.After(workflowExecutionTimeoutTime) {
			workflowRunTimeoutTime = workflowExecutionTimeoutTime
		}
	}
	ms.executionInfo.WorkflowRunExpirationTime = timestamp.TimePtr(workflowRunTimeoutTime)

	if event.RetryPolicy != nil {
		ms.executionInfo.HasRetryPolicy = true
		ms.executionInfo.RetryBackoffCoefficient = event.RetryPolicy.GetBackoffCoefficient()
		ms.executionInfo.RetryInitialInterval = event.RetryPolicy.GetInitialInterval()
		ms.executionInfo.RetryMaximumAttempts = event.RetryPolicy.GetMaximumAttempts()
		ms.executionInfo.RetryMaximumInterval = event.RetryPolicy.GetMaximumInterval()
		ms.executionInfo.RetryNonRetryableErrorTypes = event.RetryPolicy.GetNonRetryableErrorTypes()
	}

	ms.executionInfo.AutoResetPoints = rolloverAutoResetPointsWithExpiringTime(
		event.GetPrevAutoResetPoints(),
		event.GetContinuedExecutionRunId(),
		timestamp.TimeValue(startEvent.GetEventTime()),
		ms.namespaceEntry.Retention(),
	)

	if event.Memo != nil {
		ms.executionInfo.Memo = event.Memo.GetFields()
	}
	if event.SearchAttributes != nil {
		ms.executionInfo.SearchAttributes = event.SearchAttributes.GetIndexedFields()
	}
	if event.SourceVersionStamp.GetUseVersioning() && event.SourceVersionStamp.GetBuildId() != "" {
		limit := ms.config.SearchAttributesSizeOfValueLimit(string(ms.namespaceEntry.Name()))
		if _, err := ms.addBuildIdsWithNoVisibilityTask([]string{worker_versioning.VersionedBuildIdSearchAttribute(event.SourceVersionStamp.BuildId)}, limit); err != nil {
			return err
		}
	}

	ms.executionInfo.WorkerVersionStamp = event.SourceVersionStamp

	ms.approximateSize += ms.executionInfo.Size()

	ms.writeEventToCache(startEvent)
	return nil
}

// AddFirstWorkflowTaskScheduled adds the first workflow task scehduled event unless it should be delayed as indicated
// by the startEvent's FirstWorkflowTaskBackoff.
// Returns the workflow task's scheduled event ID if a task was scheduled, 0 otherwise.
func (ms *MutableStateImpl) AddFirstWorkflowTaskScheduled(
	parentClock *clockspb.VectorClock,
	startEvent *historypb.HistoryEvent,
	bypassTaskGeneration bool,
) (int64, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := ms.checkMutability(opTag); err != nil {
		return common.EmptyEventID, err
	}
	scheduleEventID, err := ms.workflowTaskManager.AddFirstWorkflowTaskScheduled(startEvent, bypassTaskGeneration)
	if err != nil {
		return 0, err
	}
	if parentClock != nil {
		ms.executionInfo.ParentClock = parentClock
	}
	return scheduleEventID, nil
}

func (ms *MutableStateImpl) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskScheduledEvent(bypassTaskGeneration, workflowTaskType)
}

// AddWorkflowTaskScheduledEventAsHeartbeat is to record the first WorkflowTaskScheduledEvent during workflow task heartbeat.
func (ms *MutableStateImpl) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp *time.Time,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp, workflowTaskType)
}

func (ms *MutableStateImpl) ReplicateTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ReplicateTransientWorkflowTaskScheduled()
}

func (ms *MutableStateImpl) ReplicateWorkflowTaskScheduledEvent(
	version int64,
	scheduledEventID int64,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *time.Duration,
	attempt int32,
	scheduleTimestamp *time.Time,
	originalScheduledTimestamp *time.Time,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ReplicateWorkflowTaskScheduledEvent(version, scheduledEventID, taskQueue, startToCloseTimeout, attempt, scheduleTimestamp, originalScheduledTimestamp, workflowTaskType)
}

func (ms *MutableStateImpl) AddWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	taskQueue *taskqueuepb.TaskQueue,
	identity string,
) (*historypb.HistoryEvent, *WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskStartedEvent(scheduledEventID, requestID, taskQueue, identity)
}

func (ms *MutableStateImpl) ReplicateWorkflowTaskStartedEvent(
	workflowTask *WorkflowTaskInfo,
	version int64,
	scheduledEventID int64,
	startedEventID int64,
	requestID string,
	timestamp time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
) (*WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ReplicateWorkflowTaskStartedEvent(workflowTask, version, scheduledEventID,
		startedEventID, requestID, timestamp, suggestContinueAsNew, historySizeBytes)
}

// TODO (alex-update): 	Transient needs to be renamed to "TransientOrSpeculative"
func (ms *MutableStateImpl) GetTransientWorkflowTaskInfo(
	workflowTask *WorkflowTaskInfo,
	identity string,
) *historyspb.TransientWorkflowTaskInfo {
	if !ms.IsTransientWorkflowTask() && workflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return nil
	}
	return ms.workflowTaskManager.GetTransientWorkflowTaskInfo(workflowTask, identity)
}

// add BinaryCheckSum for the first workflowTaskCompletedID for auto-reset
func (ms *MutableStateImpl) addBinaryCheckSumIfNotExists(
	event *historypb.HistoryEvent,
	maxResetPoints int,
) error {
	binChecksum := event.GetWorkflowTaskCompletedEventAttributes().GetBinaryChecksum()
	if len(binChecksum) == 0 {
		return nil
	}
	if !ms.addResetPointFromCompletion(binChecksum, event.GetEventId(), maxResetPoints) {
		return nil
	}
	exeInfo := ms.executionInfo
	resetPoints := exeInfo.AutoResetPoints.Points
	// List of all recent binary checksums associated with the workflow.
	recentBinaryChecksums := make([]string, len(resetPoints))

	for i, rp := range resetPoints {
		recentBinaryChecksums[i] = rp.GetBinaryChecksum()
	}

	checksumsPayload, err := searchattribute.EncodeValue(recentBinaryChecksums, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	if err != nil {
		return err
	}
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload, 1)
	}
	exeInfo.SearchAttributes[searchattribute.BinaryChecksums] = checksumsPayload
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
}

// Add a reset point for current task completion if needed.
// Returns true if the reset point was added or false if there was no need or no ability to add.
func (ms *MutableStateImpl) addResetPointFromCompletion(
	binaryChecksum string,
	eventID int64,
	maxResetPoints int,
) bool {
	if maxResetPoints < 1 {
		// Nothing to do here
		return false
	}
	exeInfo := ms.executionInfo
	var resetPoints []*workflowpb.ResetPointInfo
	if exeInfo.AutoResetPoints != nil && exeInfo.AutoResetPoints.Points != nil {
		resetPoints = ms.executionInfo.AutoResetPoints.Points
		if len(resetPoints) >= maxResetPoints {
			// If limit is exceeded, drop the oldest ones.
			resetPoints = resetPoints[len(resetPoints)-maxResetPoints+1:]
		}
	} else {
		resetPoints = make([]*workflowpb.ResetPointInfo, 0, 1)
	}

	for _, rp := range resetPoints {
		if rp.GetBinaryChecksum() == binaryChecksum {
			return false
		}
	}

	info := &workflowpb.ResetPointInfo{
		BinaryChecksum:               binaryChecksum,
		RunId:                        ms.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: eventID,
		CreateTime:                   timestamp.TimePtr(ms.timeSource.Now()),
		Resettable:                   ms.CheckResettable() == nil,
	}
	exeInfo.AutoResetPoints = &workflowpb.ResetPoints{
		Points: append(resetPoints, info),
	}
	return true
}

// Similar to (the to-be-deprecated) addBinaryCheckSumIfNotExists but works on build IDs.
func (ms *MutableStateImpl) trackBuildIdFromCompletion(
	version *commonpb.WorkerVersionStamp,
	eventID int64,
	limits WorkflowTaskCompletionLimits,
) error {
	var toAdd []string
	if !version.GetUseVersioning() {
		toAdd = append(toAdd, worker_versioning.UnversionedSearchAttribute)
	}
	if version.GetBuildId() != "" {
		ms.addResetPointFromCompletion(version.GetBuildId(), eventID, limits.MaxResetPoints)
		toAdd = append(toAdd, worker_versioning.VersionStampToBuildIdSearchAttribute(version))
	}
	if len(toAdd) == 0 {
		return nil
	}
	if changed, err := ms.addBuildIdsWithNoVisibilityTask(toAdd, limits.MaxSearchAttributeValueSize); err != nil {
		return err
	} else if !changed {
		return nil
	}
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
}

func (ms *MutableStateImpl) loadBuildIds() ([]string, error) {
	searchAttributes := ms.executionInfo.SearchAttributes
	if searchAttributes == nil {
		return []string{}, nil
	}
	saPayload, found := searchAttributes[searchattribute.BuildIds]
	if !found {
		return []string{}, nil
	}
	decoded, err := searchattribute.DecodeValue(saPayload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	if err != nil {
		return nil, err
	}
	searchAttributeValues, ok := decoded.([]string)
	if !ok {
		return nil, serviceerror.NewInternal("invalid search attribute value stored for BuildIds")
	}
	return searchAttributeValues, nil
}

// Takes a list of loaded build IDs from a search attribute and adds new build IDs to it. Returns a potentially modified
// list and a flag indicating whether it was modified.
func (ms *MutableStateImpl) addBuildIdToLoadedSearchAttribute(existingValues []string, newValues []string) ([]string, bool) {
	var added []string
	for _, newValue := range newValues {
		found := false
		for _, exisitingValue := range existingValues {
			if exisitingValue == newValue {
				found = true
				break
			}
		}
		if !found {
			added = append(added, newValue)
		}
	}
	if len(added) == 0 {
		return existingValues, false
	}
	return append(existingValues, added...), true
}

func (ms *MutableStateImpl) saveBuildIds(buildIds []string, maxSearchAttributeValueSize int) error {
	searchAttributes := ms.executionInfo.SearchAttributes
	if searchAttributes == nil {
		searchAttributes = make(map[string]*commonpb.Payload, 1)
		ms.executionInfo.SearchAttributes = searchAttributes
	}

	hasUnversioned := buildIds[0] == worker_versioning.UnversionedSearchAttribute
	for {
		saPayload, err := searchattribute.EncodeValue(buildIds, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
		if err != nil {
			return err
		}
		if len(buildIds) == 0 || len(saPayload.GetData()) <= maxSearchAttributeValueSize {
			searchAttributes[searchattribute.BuildIds] = saPayload
			break
		}
		if len(buildIds) == 1 {
			buildIds = make([]string, 0)
		} else if hasUnversioned {
			// Make sure to maintain the unversioned sentinel, it's required for the reachability API
			buildIds = append(buildIds[:1], buildIds[2:]...)
		} else {
			buildIds = buildIds[1:]
		}
	}
	return nil
}

func (ms *MutableStateImpl) addBuildIdsWithNoVisibilityTask(buildIds []string, maxSearchAttributeValueSize int) (bool, error) {
	existingBuildIds, err := ms.loadBuildIds()
	if err != nil {
		return false, err
	}
	modifiedBuildIds, added := ms.addBuildIdToLoadedSearchAttribute(existingBuildIds, buildIds)
	if !added {
		return false, nil
	}
	return true, ms.saveBuildIds(modifiedBuildIds, maxSearchAttributeValueSize)
}

// TODO: we will release the restriction when reset API allow those pending

// CheckResettable check if workflow can be reset
func (ms *MutableStateImpl) CheckResettable() error {
	if len(ms.GetPendingChildExecutionInfos()) > 0 {
		return serviceerror.NewInvalidArgument("it is not allowed resetting to a point that workflow has pending child workflow.")
	}
	if len(ms.GetPendingRequestCancelExternalInfos()) > 0 {
		return serviceerror.NewInvalidArgument("it is not allowed resetting to a point that workflow has pending request cancel.")
	}
	if len(ms.GetPendingSignalExternalInfos()) > 0 {
		return serviceerror.NewInvalidArgument("it is not allowed resetting to a point that workflow has pending signals to send.")
	}
	return nil
}

func (ms *MutableStateImpl) AddWorkflowTaskCompletedEvent(
	workflowTask *WorkflowTaskInfo,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	limits WorkflowTaskCompletionLimits,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskCompleted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskCompletedEvent(workflowTask, request, limits)
}

func (ms *MutableStateImpl) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	return ms.workflowTaskManager.ReplicateWorkflowTaskCompletedEvent(event)
}

func (ms *MutableStateImpl) AddWorkflowTaskTimedOutEvent(
	workflowTask *WorkflowTaskInfo,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskTimedOutEvent(workflowTask)
}

func (ms *MutableStateImpl) ReplicateWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	return ms.workflowTaskManager.ReplicateWorkflowTaskTimedOutEvent(timeoutType)
}

func (ms *MutableStateImpl) AddWorkflowTaskScheduleToStartTimeoutEvent(
	workflowTask *WorkflowTaskInfo,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask)
}

func (ms *MutableStateImpl) AddWorkflowTaskFailedEvent(
	workflowTask *WorkflowTaskInfo,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	binChecksum string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskFailedEvent(
		workflowTask,
		cause,
		failure,
		identity,
		binChecksum,
		baseRunID,
		newRunID,
		forkEventVersion,
	)
}

func (ms *MutableStateImpl) ReplicateWorkflowTaskFailedEvent() error {
	return ms.workflowTaskManager.ReplicateWorkflowTaskFailedEvent()
}

func (ms *MutableStateImpl) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
	bypassTaskGeneration bool,
) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskScheduled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	_, ok := ms.GetActivityByActivityID(command.GetActivityId())
	if ok {
		ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction)
		return nil, nil, ms.createCallerError(opTag, "ActivityID: "+command.GetActivityId())
	}

	event := ms.hBuilder.AddActivityTaskScheduledEvent(workflowTaskCompletedEventID, command)
	ai, err := ms.ReplicateActivityTaskScheduledEvent(workflowTaskCompletedEventID, event)
	// TODO merge active & passive task generation
	if !bypassTaskGeneration {
		if err := ms.taskGenerator.GenerateActivityTasks(
			event,
		); err != nil {
			return nil, nil, err
		}
	}

	return event, ai, err
}

func (ms *MutableStateImpl) ReplicateActivityTaskScheduledEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) (*persistencespb.ActivityInfo, error) {

	attributes := event.GetActivityTaskScheduledEventAttributes()

	scheduledEventID := event.GetEventId()
	scheduleToCloseTimeout := attributes.GetScheduleToCloseTimeout()

	ai := &persistencespb.ActivityInfo{
		Version:                 event.GetVersion(),
		ScheduledEventId:        scheduledEventID,
		ScheduledEventBatchId:   firstEventID,
		ScheduledTime:           event.GetEventTime(),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              attributes.ActivityId,
		ScheduleToStartTimeout:  attributes.GetScheduleToStartTimeout(),
		ScheduleToCloseTimeout:  scheduleToCloseTimeout,
		StartToCloseTimeout:     attributes.GetStartToCloseTimeout(),
		HeartbeatTimeout:        attributes.GetHeartbeatTimeout(),
		CancelRequested:         false,
		CancelRequestId:         common.EmptyEventID,
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		TaskQueue:               attributes.TaskQueue.GetName(),
		HasRetryPolicy:          attributes.RetryPolicy != nil,
		Attempt:                 1,
		UseCompatibleVersion:    attributes.UseCompatibleVersion,
		ActivityType:            attributes.GetActivityType(),
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

	ms.pendingActivityInfoIDs[ai.ScheduledEventId] = ai
	ms.pendingActivityIDToEventID[ai.ActivityId] = ai.ScheduledEventId
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.approximateSize += ai.Size() + int64SizeBytes
	ms.executionInfo.ActivityCount++

	ms.writeEventToCache(event)
	return ai, nil
}

func (ms *MutableStateImpl) addTransientActivityStartedEvent(
	scheduledEventID int64,
) error {

	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok || ai.StartedEventId != common.TransientEventID {
		return nil
	}

	// activity task was started (as transient event), we need to add it now.
	event := ms.hBuilder.AddActivityTaskStartedEvent(
		scheduledEventID,
		ai.Attempt,
		ai.RequestId,
		ai.StartedIdentity,
		ai.RetryLastFailure,
	)
	if !ai.StartedTime.IsZero() {
		// overwrite started event time to the one recorded in ActivityInfo
		event.EventTime = ai.StartedTime
	}
	return ms.ReplicateActivityTaskStartedEvent(event)
}

func (ms *MutableStateImpl) AddActivityTaskStartedEvent(
	ai *persistencespb.ActivityInfo,
	scheduledEventID int64,
	requestID string,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	if !ai.HasRetryPolicy {
		event := ms.hBuilder.AddActivityTaskStartedEvent(
			scheduledEventID,
			ai.Attempt,
			requestID,
			identity,
			ai.RetryLastFailure,
		)
		if err := ms.ReplicateActivityTaskStartedEvent(event); err != nil {
			return nil, err
		}
		return event, nil
	}

	// we might need to retry, so do not append started event just yet,
	// instead update mutable state and will record started event when activity task is closed
	ai.Version = ms.GetCurrentVersion()
	ai.StartedEventId = common.TransientEventID
	ai.RequestId = requestID
	ai.StartedTime = timestamp.TimePtr(ms.timeSource.Now())
	ai.StartedIdentity = identity
	if err := ms.UpdateActivity(ai); err != nil {
		return nil, err
	}
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
	return nil, nil
}

func (ms *MutableStateImpl) ReplicateActivityTaskStartedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskStartedEventAttributes()
	scheduledEventID := attributes.GetScheduledEventId()
	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find activity event id: %v in mutable state", scheduledEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ms.approximateSize -= ai.Size()

	ai.Version = event.GetVersion()
	ai.StartedEventId = event.GetEventId()
	ai.RequestId = attributes.GetRequestId()
	ai.StartedTime = event.GetEventTime()
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.approximateSize += ai.Size()
	return nil
}

func (ms *MutableStateImpl) AddActivityTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCompleted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ai, ok := ms.GetActivityInfo(scheduledEventID); !ok || ai.StartedEventId != startedEventID {
		ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduledEventID(scheduledEventID),
			tag.WorkflowStartedEventID(startedEventID))
		return nil, ms.createInternalServerError(opTag)
	}

	if err := ms.addTransientActivityStartedEvent(scheduledEventID); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		request.Identity,
		request.Result,
	)
	if err := ms.ReplicateActivityTaskCompletedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ReplicateActivityTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCompletedEventAttributes()
	scheduledEventID := attributes.GetScheduledEventId()

	return ms.DeleteActivity(scheduledEventID)
}

func (ms *MutableStateImpl) AddActivityTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ai, ok := ms.GetActivityInfo(scheduledEventID); !ok || ai.StartedEventId != startedEventID {
		ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduledEventID(scheduledEventID),
			tag.WorkflowStartedEventID(startedEventID))
		return nil, ms.createInternalServerError(opTag)
	}

	if err := ms.addTransientActivityStartedEvent(scheduledEventID); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		failure,
		retryState,
		identity,
	)
	if err := ms.ReplicateActivityTaskFailedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ReplicateActivityTaskFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskFailedEventAttributes()
	scheduledEventID := attributes.GetScheduledEventId()

	return ms.DeleteActivity(scheduledEventID)
}

func (ms *MutableStateImpl) AddActivityTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskTimedOut
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	timeoutType := timeoutFailure.GetTimeoutFailureInfo().GetTimeoutType()

	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok || ai.StartedEventId != startedEventID || ((timeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE ||
		timeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT) && ai.StartedEventId == common.EmptyEventID) {
		ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduledEventID(ai.ScheduledEventId),
			tag.WorkflowStartedEventID(ai.StartedEventId),
			tag.WorkflowTimeoutType(timeoutType))
		return nil, ms.createInternalServerError(opTag)
	}

	timeoutFailure.Cause = ai.RetryLastFailure

	if err := ms.addTransientActivityStartedEvent(scheduledEventID); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		timeoutFailure,
		retryState,
	)
	if err := ms.ReplicateActivityTaskTimedOutEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ReplicateActivityTaskTimedOutEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskTimedOutEventAttributes()
	scheduledEventID := attributes.GetScheduledEventId()

	return ms.DeleteActivity(scheduledEventID)
}

func (ms *MutableStateImpl) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduledEventID int64,
	_ string,
) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error) {

	opTag := tag.WorkflowActionActivityTaskCancelRequested
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok {
		// It is possible both started and completed events are buffered for this activity
		if !ms.hBuilder.HasActivityFinishEvent(scheduledEventID) {
			ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
				tag.WorkflowEventID(ms.GetNextEventID()),
				tag.ErrorTypeInvalidHistoryAction,
				tag.Bool(ok),
				tag.WorkflowScheduledEventID(scheduledEventID))

			return nil, nil, ms.createCallerError(opTag, fmt.Sprintf("ScheduledEventID: %d", scheduledEventID))
		}
	}

	// Check for duplicate cancellation
	if ok && ai.CancelRequested {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowScheduledEventID(scheduledEventID))

		return nil, nil, ms.createCallerError(opTag, fmt.Sprintf("ScheduledEventID: %d", scheduledEventID))
	}

	// At this point we know this is a valid activity cancellation request
	actCancelReqEvent := ms.hBuilder.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEventID, scheduledEventID)

	if err := ms.ReplicateActivityTaskCancelRequestedEvent(actCancelReqEvent); err != nil {
		return nil, nil, err
	}

	return actCancelReqEvent, ai, nil
}

func (ms *MutableStateImpl) ReplicateActivityTaskCancelRequestedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCancelRequestedEventAttributes()
	scheduledEventID := attributes.GetScheduledEventId()
	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok {
		// This will only be called on active cluster if activity info is found in mutable state
		// Passive side logic should always have activity info in mutable state if this is called, as the only
		// scenario where active side logic could have this event without activity info in mutable state is when
		// activity start and complete events are buffered.
		return nil
	}

	ms.approximateSize -= ai.Size()

	ai.Version = event.GetVersion()

	// - We have the activity dispatched to worker.
	// - The activity might not be heartbeat'ing, but the activity can still call RecordActivityHeartBeat()
	//   to see cancellation while reporting progress of the activity.
	ai.CancelRequested = true

	ai.CancelRequestId = event.GetEventId()
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.approximateSize += ai.Size()
	return nil
}

func (ms *MutableStateImpl) AddActivityTaskCanceledEvent(
	scheduledEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionActivityTaskCanceled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok || ai.StartedEventId != startedEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduledEventID(scheduledEventID))
		return nil, ms.createInternalServerError(opTag)
	}

	// Verify cancel request as well.
	if !ai.CancelRequested {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduledEventID(scheduledEventID),
			tag.WorkflowActivityID(ai.ActivityId),
			tag.WorkflowStartedEventID(ai.StartedEventId))
		return nil, ms.createInternalServerError(opTag)
	}

	if err := ms.addTransientActivityStartedEvent(scheduledEventID); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskCanceledEvent(
		scheduledEventID,
		startedEventID,
		latestCancelRequestedEventID,
		details,
		identity,
	)
	if err := ms.ReplicateActivityTaskCanceledEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ReplicateActivityTaskCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetActivityTaskCanceledEventAttributes()
	scheduledEventID := attributes.GetScheduledEventId()

	return ms.DeleteActivity(scheduledEventID)
}

func (ms *MutableStateImpl) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCompleted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddCompletedWorkflowEvent(workflowTaskCompletedEventID, command, newExecutionRunID)
	if err := ms.ReplicateWorkflowExecutionCompletedEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event,
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionCompletedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	); err != nil {
		return err
	}
	ms.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	ms.executionInfo.NewExecutionRunId = event.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId()
	ms.executionInfo.CloseTime = event.GetEventTime()
	ms.ClearStickyTaskQueue()
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event, batchID := ms.hBuilder.AddFailWorkflowEvent(workflowTaskCompletedEventID, retryState, command, newExecutionRunID)
	if err := ms.ReplicateWorkflowExecutionFailedEvent(batchID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event,
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionFailedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	); err != nil {
		return err
	}
	ms.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	ms.executionInfo.NewExecutionRunId = event.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId()
	ms.executionInfo.CloseTime = event.GetEventTime()
	ms.ClearStickyTaskQueue()
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) AddTimeoutWorkflowEvent(
	firstEventID int64,
	retryState enumspb.RetryState,
	newExecutionRunID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowTimeout
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddTimeoutWorkflowEvent(retryState, newExecutionRunID)
	if err := ms.ReplicateWorkflowExecutionTimedoutEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event,
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionTimedoutEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	); err != nil {
		return err
	}
	ms.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	ms.executionInfo.NewExecutionRunId = event.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId()
	ms.executionInfo.CloseTime = event.GetEventTime()
	ms.ClearStickyTaskQueue()
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionCancelRequestedEvent(
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCancelRequested
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	if ms.executionInfo.CancelRequested {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowState(ms.executionState.State),
			tag.Bool(ms.executionInfo.CancelRequested),
			tag.Key(ms.executionInfo.CancelRequestId),
		)
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddWorkflowExecutionCancelRequestedEvent(request)
	if err := ms.ReplicateWorkflowExecutionCancelRequestedEvent(event); err != nil {
		return nil, err
	}

	// Set the CancelRequestID on the active cluster.  This information is not part of the history event.
	ms.executionInfo.CancelRequestId = request.CancelRequest.GetRequestId()
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionCancelRequestedEvent(
	_ *historypb.HistoryEvent,
) error {

	ms.executionInfo.CancelRequested = true
	return nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowCanceled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, command)
	if err := ms.ReplicateWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event,
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionCanceledEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	); err != nil {
		return err
	}
	ms.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	ms.executionInfo.NewExecutionRunId = ""
	ms.executionInfo.CloseTime = event.GetEventTime()
	ms.ClearStickyTaskQueue()
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	cancelRequestID string,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) (*historypb.HistoryEvent, *persistencespb.RequestCancelInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelInitiated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := ms.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, command, targetNamespaceID)
	rci, err := ms.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, cancelRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateRequestCancelExternalTasks(
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, rci, nil
}

func (ms *MutableStateImpl) ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	cancelRequestID string,
) (*persistencespb.RequestCancelInfo, error) {

	// TODO: Evaluate if we need cancelRequestID also part of history event
	initiatedEventID := event.GetEventId()
	rci := &persistencespb.RequestCancelInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchId: firstEventID,
		InitiatedEventId:      initiatedEventID,
		CancelRequestId:       cancelRequestID,
	}

	ms.pendingRequestCancelInfoIDs[rci.InitiatedEventId] = rci
	ms.updateRequestCancelInfos[rci.InitiatedEventId] = rci
	ms.approximateSize += rci.Size() + int64SizeBytes
	ms.executionInfo.RequestCancelExternalCount++

	ms.writeEventToCache(event)
	return rci, nil
}

func (ms *MutableStateImpl) AddExternalWorkflowExecutionCancelRequested(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelRequested
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := ms.GetRequestCancelInfo(initiatedID)
	if !ok {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddExternalWorkflowExecutionCancelRequested(
		initiatedID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
	)
	if err := ms.ReplicateExternalWorkflowExecutionCancelRequested(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateExternalWorkflowExecutionCancelRequested(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionCancelRequestedEventAttributes().GetInitiatedEventId()

	return ms.DeletePendingRequestCancel(initiatedID)
}

func (ms *MutableStateImpl) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowCancelFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := ms.GetRequestCancelInfo(initiatedID)
	if !ok {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		common.EmptyEventID, // TODO this field is not used at all
		initiatedID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		cause,
	)
	if err := ms.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return ms.DeletePendingRequestCancel(initiatedID)
}

func (ms *MutableStateImpl) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	signalRequestID string,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) (*historypb.HistoryEvent, *persistencespb.SignalInfo, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalInitiated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := ms.hBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, command, targetNamespaceID)
	si, err := ms.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, signalRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateSignalExternalTasks(
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, si, nil
}

func (ms *MutableStateImpl) ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	signalRequestID string,
) (*persistencespb.SignalInfo, error) {

	// TODO: Consider also writing signalRequestID to history event
	initiatedEventID := event.GetEventId()
	si := &persistencespb.SignalInfo{
		Version:               event.GetVersion(),
		InitiatedEventBatchId: firstEventID,
		InitiatedEventId:      initiatedEventID,
		RequestId:             signalRequestID,
	}

	ms.pendingSignalInfoIDs[si.InitiatedEventId] = si
	ms.updateSignalInfos[si.InitiatedEventId] = si
	ms.approximateSize += si.Size() + int64SizeBytes
	ms.executionInfo.SignalExternalCount++

	ms.writeEventToCache(event)
	return si, nil
}

func (ms *MutableStateImpl) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionUpsertWorkflowSearchAttributes
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, command)
	ms.ReplicateUpsertWorkflowSearchAttributesEvent(event)
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateUpsertVisibilityTask(); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateUpsertWorkflowSearchAttributesEvent(
	event *historypb.HistoryEvent,
) {
	upsertSearchAttr := event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields()
	ms.approximateSize -= ms.executionInfo.Size()
	ms.executionInfo.SearchAttributes = payload.MergeMapOfPayload(ms.executionInfo.SearchAttributes, upsertSearchAttr)
	ms.approximateSize += ms.executionInfo.Size()
}

func (ms *MutableStateImpl) AddWorkflowPropertiesModifiedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowPropertiesModified
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddWorkflowPropertiesModifiedEvent(workflowTaskCompletedEventID, command)
	ms.ReplicateWorkflowPropertiesModifiedEvent(event)
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateUpsertVisibilityTask(); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowPropertiesModifiedEvent(
	event *historypb.HistoryEvent,
) {
	attr := event.GetWorkflowPropertiesModifiedEventAttributes()
	if attr.UpsertedMemo != nil {
		upsertMemo := attr.GetUpsertedMemo().GetFields()
		ms.approximateSize -= ms.executionInfo.Size()
		ms.executionInfo.Memo = payload.MergeMapOfPayload(ms.executionInfo.Memo, upsertMemo)
		ms.approximateSize += ms.executionInfo.Size()
	}
}

func (ms *MutableStateImpl) AddExternalWorkflowExecutionSignaled(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string, // TODO this field is probably deprecated
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalRequested
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := ms.GetSignalInfo(initiatedID)
	if !ok {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddExternalWorkflowExecutionSignaled(
		initiatedID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		control, // TODO this field is probably deprecated
	)
	if err := ms.ReplicateExternalWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateExternalWorkflowExecutionSignaled(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetExternalWorkflowExecutionSignaledEventAttributes().GetInitiatedEventId()

	return ms.DeletePendingSignal(initiatedID)
}

func (ms *MutableStateImpl) AddSignalExternalWorkflowExecutionFailedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string, // TODO this field is probably deprecated
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionExternalWorkflowSignalFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	_, ok := ms.GetSignalInfo(initiatedID)
	if !ok {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
		common.EmptyEventID, // TODO this field is not used at all
		initiatedID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		control, // TODO this field is probably deprecated
		cause,
	)
	if err := ms.ReplicateSignalExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateSignalExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	initiatedID := event.GetSignalExternalWorkflowExecutionFailedEventAttributes().GetInitiatedEventId()

	return ms.DeletePendingSignal(initiatedID)
}

func (ms *MutableStateImpl) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) (*historypb.HistoryEvent, *persistencespb.TimerInfo, error) {

	opTag := tag.WorkflowActionTimerStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	timerID := command.GetTimerId()
	_, ok := ms.GetUserTimerInfo(timerID)
	if ok {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowTimerID(timerID))
		return nil, nil, ms.createCallerError(opTag, "TimerID: "+command.GetTimerId())
	}

	event := ms.hBuilder.AddTimerStartedEvent(workflowTaskCompletedEventID, command)
	ti, err := ms.ReplicateTimerStartedEvent(event)
	if err != nil {
		return nil, nil, err
	}
	return event, ti, err
}

func (ms *MutableStateImpl) ReplicateTimerStartedEvent(
	event *historypb.HistoryEvent,
) (*persistencespb.TimerInfo, error) {

	attributes := event.GetTimerStartedEventAttributes()
	timerID := attributes.GetTimerId()

	startToFireTimeout := timestamp.DurationValue(attributes.GetStartToFireTimeout())
	// TODO: Time skew need to be taken in to account.
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(startToFireTimeout) // should use the event time, not now

	ti := &persistencespb.TimerInfo{
		Version:        event.GetVersion(),
		TimerId:        timerID,
		ExpiryTime:     &expiryTime,
		StartedEventId: event.GetEventId(),
		TaskStatus:     TimerTaskStatusNone,
	}

	ms.pendingTimerInfoIDs[ti.TimerId] = ti
	ms.pendingTimerEventIDToID[ti.StartedEventId] = ti.TimerId
	ms.updateTimerInfos[ti.TimerId] = ti
	ms.approximateSize += ti.Size() + len(ti.TimerId)
	ms.executionInfo.UserTimerCount++

	return ti, nil
}

func (ms *MutableStateImpl) AddTimerFiredEvent(
	timerID string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionTimerFired
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	timerInfo, ok := ms.GetUserTimerInfo(timerID)
	if !ok {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowTimerID(timerID))
		return nil, ms.createInternalServerError(opTag)
	}

	// Timer is running.
	event := ms.hBuilder.AddTimerFiredEvent(timerInfo.GetStartedEventId(), timerInfo.TimerId)
	if err := ms.ReplicateTimerFiredEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateTimerFiredEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetTimerFiredEventAttributes()
	timerID := attributes.GetTimerId()

	return ms.DeleteUserTimer(timerID)
}

func (ms *MutableStateImpl) AddTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelTimerCommandAttributes,
	identity string,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionTimerCanceled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	var timerStartedEventID int64
	timerID := command.GetTimerId()
	ti, ok := ms.GetUserTimerInfo(timerID)
	if !ok {
		// if timer is not running then check if it has fired in the mutable state.
		// If so clear the timer from the mutable state. We need to check both the
		// bufferedEvents and the history builder
		timerFiredEvent := ms.hBuilder.GetAndRemoveTimerFireEvent(timerID)
		if timerFiredEvent == nil {
			ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
				tag.WorkflowEventID(ms.GetNextEventID()),
				tag.ErrorTypeInvalidHistoryAction,
				tag.WorkflowTimerID(timerID))
			return nil, ms.createCallerError(opTag, "TimerID: "+command.GetTimerId())
		}
		timerStartedEventID = timerFiredEvent.GetTimerFiredEventAttributes().GetStartedEventId()
	} else {
		timerStartedEventID = ti.GetStartedEventId()
	}

	// Timer is running.
	event := ms.hBuilder.AddTimerCanceledEvent(
		workflowTaskCompletedEventID,
		timerStartedEventID,
		timerID,
		identity,
	)
	if ok {
		if err := ms.ReplicateTimerCanceledEvent(event); err != nil {
			return nil, err
		}
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateTimerCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetTimerCanceledEventAttributes()
	timerID := attributes.GetTimerId()

	return ms.DeleteUserTimer(timerID)
}

func (ms *MutableStateImpl) AddRecordMarkerEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowRecordMarker
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	return ms.hBuilder.AddMarkerRecordedEvent(workflowTaskCompletedEventID, command), nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	reason string,
	details *commonpb.Payloads,
	identity string,
	deleteAfterTerminate bool,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowTerminated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddWorkflowExecutionTerminatedEvent(reason, details, identity)
	if err := ms.ReplicateWorkflowExecutionTerminatedEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event,
		deleteAfterTerminate,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionUpdateAcceptedEvent(
	protocolInstanceID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) (*historypb.HistoryEvent, error) {
	if err := ms.checkMutability(tag.WorkflowActionUpdateAccepted); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddWorkflowExecutionUpdateAcceptedEvent(protocolInstanceID, acceptedRequestMessageId, acceptedRequestSequencingEventId, acceptedRequest)
	if err := ms.ReplicateWorkflowExecutionUpdateAcceptedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionUpdateAcceptedEvent(
	event *historypb.HistoryEvent,
) error {
	attrs := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
	if attrs == nil {
		return serviceerror.NewInternal("wrong event type in call to ReplicateWorkflowExecutionUpdateAcceptedEvent")
	}
	if ms.executionInfo.UpdateInfos == nil {
		ms.executionInfo.UpdateInfos = make(map[string]*updatespb.UpdateInfo, 1)
	}
	updateID := attrs.GetAcceptedRequest().GetMeta().GetUpdateId()
	var sizeDelta int
	if ui, ok := ms.executionInfo.UpdateInfos[updateID]; ok {
		sizeBefore := ui.Value.Size()
		ui.Value = &updatespb.UpdateInfo_Acceptance{
			Acceptance: &updatespb.AcceptanceInfo{EventId: event.EventId},
		}
		sizeDelta = ui.Value.Size() - sizeBefore
	} else {
		ui := updatespb.UpdateInfo{
			Value: &updatespb.UpdateInfo_Acceptance{
				Acceptance: &updatespb.AcceptanceInfo{EventId: event.EventId},
			},
		}
		ms.executionInfo.UpdateInfos[updateID] = &ui
		ms.executionInfo.UpdateCount++
		sizeDelta = ui.Size() + len(updateID)
	}
	ms.approximateSize += sizeDelta
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	updResp *updatepb.Response,
) (*historypb.HistoryEvent, error) {
	if err := ms.checkMutability(tag.WorkflowActionUpdateCompleted); err != nil {
		return nil, err
	}
	event, batchID := ms.hBuilder.AddWorkflowExecutionUpdateCompletedEvent(acceptedEventID, updResp)
	if err := ms.ReplicateWorkflowExecutionUpdateCompletedEvent(event, batchID); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionUpdateCompletedEvent(
	event *historypb.HistoryEvent,
	batchID int64,
) error {
	attrs := event.GetWorkflowExecutionUpdateCompletedEventAttributes()
	if attrs == nil {
		return serviceerror.NewInternal("wrong event type in call to ReplicateWorkflowExecutionUpdateCompletedEvent")
	}
	if ms.executionInfo.UpdateInfos == nil {
		ms.executionInfo.UpdateInfos = make(map[string]*updatespb.UpdateInfo, 1)
	}
	updateID := attrs.GetMeta().GetUpdateId()
	var sizeDelta int
	if ui, ok := ms.executionInfo.UpdateInfos[updateID]; ok {
		sizeBefore := ui.Value.Size()
		ui.Value = &updatespb.UpdateInfo_Completion{
			Completion: &updatespb.CompletionInfo{
				EventId:      event.EventId,
				EventBatchId: batchID,
			},
		}
		sizeDelta = ui.Value.Size() - sizeBefore
	} else {
		ui := updatespb.UpdateInfo{
			Value: &updatespb.UpdateInfo_Completion{
				Completion: &updatespb.CompletionInfo{EventId: event.EventId},
			},
		}
		ms.executionInfo.UpdateInfos[updateID] = &ui
		ms.executionInfo.UpdateCount++
		sizeDelta = ui.Size() + len(updateID)
	}
	ms.approximateSize += sizeDelta
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) RejectWorkflowExecutionUpdate(_ string, _ *updatepb.Rejection) error {
	// TODO (alex-update): This method is noop because we don't currently write rejections to the history.
	return nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {

	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	); err != nil {
		return err
	}
	ms.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	ms.executionInfo.NewExecutionRunId = ""
	ms.executionInfo.CloseTime = event.GetEventTime()
	ms.ClearStickyTaskQueue()
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionSignaled(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	skipGenerateWorkflowTask bool,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionWorkflowSignaled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddWorkflowExecutionSignaledEvent(signalName, input, identity, header, skipGenerateWorkflowTask)
	if err := ms.ReplicateWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateWorkflowExecutionSignaled(
	_ *historypb.HistoryEvent,
) error {

	// Increment signal count in mutable state for this workflow execution
	ms.executionInfo.SignalCount++
	return nil
}

func (ms *MutableStateImpl) AddContinueAsNewEvent(
	ctx context.Context,
	firstEventID int64,
	workflowTaskCompletedEventID int64,
	parentNamespace namespace.Name,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, MutableState, error) {

	opTag := tag.WorkflowActionWorkflowContinueAsNew
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	var err error
	newRunID := uuid.New()
	newExecution := commonpb.WorkflowExecution{
		WorkflowId: ms.executionInfo.WorkflowId,
		RunId:      newRunID,
	}

	// Extract ParentExecutionInfo from current run so it can be passed down to the next
	var parentInfo *workflowspb.ParentExecutionInfo
	if ms.HasParentExecution() {
		parentInfo = &workflowspb.ParentExecutionInfo{
			NamespaceId: ms.executionInfo.ParentNamespaceId,
			Namespace:   parentNamespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: ms.executionInfo.ParentWorkflowId,
				RunId:      ms.executionInfo.ParentRunId,
			},
			InitiatedId:      ms.executionInfo.ParentInitiatedId,
			InitiatedVersion: ms.executionInfo.ParentInitiatedVersion,
			Clock:            ms.executionInfo.ParentClock,
		}
	}

	continueAsNewEvent := ms.hBuilder.AddContinuedAsNewEvent(
		workflowTaskCompletedEventID,
		newRunID,
		command,
	)

	firstRunID, err := ms.GetFirstRunID(ctx)
	if err != nil {
		return nil, nil, err
	}

	newMutableState := NewMutableState(
		ms.shard,
		ms.shard.GetEventsCache(),
		ms.logger,
		ms.namespaceEntry,
		timestamp.TimeValue(continueAsNewEvent.GetEventTime()),
	)

	if _, err = newMutableState.addWorkflowExecutionStartedEventForContinueAsNew(
		parentInfo,
		newExecution,
		ms,
		command,
		firstRunID,
	); err != nil {
		return nil, nil, err
	}

	if err = newMutableState.SetHistoryTree(
		ctx,
		newMutableState.executionInfo.WorkflowExecutionTimeout,
		newMutableState.executionInfo.WorkflowRunTimeout,
		newRunID,
	); err != nil {
		return nil, nil, err
	}

	if err = ms.ReplicateWorkflowExecutionContinuedAsNewEvent(
		firstEventID,
		continueAsNewEvent,
	); err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		continueAsNewEvent,
		false,
	); err != nil {
		return nil, nil, err
	}

	return continueAsNewEvent, newMutableState, nil
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

func (ms *MutableStateImpl) ReplicateWorkflowExecutionContinuedAsNewEvent(
	firstEventID int64,
	continueAsNewEvent *historypb.HistoryEvent,
) error {

	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	); err != nil {
		return err
	}
	ms.executionInfo.CompletionEventBatchId = firstEventID // Used when completion event needs to be loaded from database
	ms.executionInfo.NewExecutionRunId = continueAsNewEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
	ms.executionInfo.CloseTime = continueAsNewEvent.GetEventTime()
	ms.ClearStickyTaskQueue()
	ms.writeEventToCache(continueAsNewEvent)
	return nil
}

func (ms *MutableStateImpl) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	createRequestID string,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := ms.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, command, targetNamespaceID)
	ci, err := ms.ReplicateStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, createRequestID)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateChildWorkflowTasks(
		event,
	); err != nil {
		return nil, nil, err
	}
	return event, ci, nil
}

func (ms *MutableStateImpl) ReplicateStartChildWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
	createRequestID string,
) (*persistencespb.ChildExecutionInfo, error) {

	initiatedEventID := event.GetEventId()
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	ci := &persistencespb.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedEventId:      initiatedEventID,
		InitiatedEventBatchId: firstEventID,
		StartedEventId:        common.EmptyEventID,
		StartedWorkflowId:     attributes.GetWorkflowId(),
		CreateRequestId:       createRequestID,
		Namespace:             attributes.GetNamespace(),
		NamespaceId:           attributes.GetNamespaceId(),
		WorkflowTypeName:      attributes.GetWorkflowType().GetName(),
		ParentClosePolicy:     attributes.GetParentClosePolicy(),
	}

	ms.pendingChildExecutionInfoIDs[ci.InitiatedEventId] = ci
	ms.updateChildExecutionInfos[ci.InitiatedEventId] = ci
	ms.approximateSize += ci.Size() + int64SizeBytes
	ms.executionInfo.ChildExecutionCount++

	ms.writeEventToCache(event)
	return ci, nil
}

func (ms *MutableStateImpl) AddChildWorkflowExecutionStartedEvent(
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
	clock *clockspb.VectorClock,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId != common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddChildWorkflowExecutionStartedEvent(
		initiatedID,
		namespace.Name(ci.GetNamespace()),
		namespace.ID(ci.GetNamespaceId()),
		execution,
		workflowType,
		header,
	)
	if err := ms.ReplicateChildWorkflowExecutionStartedEvent(event, clock); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateChildWorkflowExecutionStartedEvent(
	event *historypb.HistoryEvent,
	clock *clockspb.VectorClock,
) error {

	attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find child workflow event ID: %v in mutable state", initiatedID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingChildWorkflowInfo
	}

	ms.approximateSize -= ci.Size()

	ci.StartedEventId = event.GetEventId()
	ci.StartedRunId = attributes.GetWorkflowExecution().GetRunId()
	ci.Clock = clock
	ms.updateChildExecutionInfos[ci.InitiatedEventId] = ci
	ms.approximateSize += ci.Size()

	return nil
}

func (ms *MutableStateImpl) AddStartChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowInitiationFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId != common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	event := ms.hBuilder.AddStartChildWorkflowExecutionFailedEvent(
		common.EmptyEventID, // TODO this field is not used at all
		initiatedID,
		cause,
		namespace.Name(ci.Namespace),
		namespace.ID(ci.NamespaceId),
		initiatedEventAttributes.WorkflowId,
		initiatedEventAttributes.WorkflowType,
		initiatedEventAttributes.Control, // TODO this field is probably deprecated
	)
	if err := ms.ReplicateStartChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateStartChildWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetStartChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) AddChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionCompletedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowCompleted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId == common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := ms.hBuilder.AddChildWorkflowExecutionCompletedEvent(
		ci.InitiatedEventId,
		ci.StartedEventId,
		namespace.Name(ci.Namespace),
		namespace.ID(ci.NamespaceId),
		childExecution,
		workflowType,
		attributes.Result,
	)
	if err := ms.ReplicateChildWorkflowExecutionCompletedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateChildWorkflowExecutionCompletedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionFailedEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowFailed
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId == common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(!ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := ms.hBuilder.AddChildWorkflowExecutionFailedEvent(
		ci.InitiatedEventId,
		ci.StartedEventId,
		namespace.Name(ci.Namespace),
		namespace.ID(ci.NamespaceId),
		childExecution,
		workflowType,
		attributes.Failure,
		attributes.RetryState,
	)
	if err := ms.ReplicateChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateChildWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) AddChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionCanceledEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowCanceled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId == common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := ms.hBuilder.AddChildWorkflowExecutionCanceledEvent(
		ci.InitiatedEventId,
		ci.StartedEventId,
		namespace.Name(ci.Namespace),
		namespace.ID(ci.NamespaceId),
		childExecution,
		workflowType,
		attributes.Details,
	)
	if err := ms.ReplicateChildWorkflowExecutionCanceledEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateChildWorkflowExecutionCanceledEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	_ *historypb.WorkflowExecutionTerminatedEventAttributes, // TODO this field is not used at all
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowTerminated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId == common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := ms.hBuilder.AddChildWorkflowExecutionTerminatedEvent(
		ci.InitiatedEventId,
		ci.StartedEventId,
		namespace.Name(ci.Namespace),
		namespace.ID(ci.NamespaceId),
		childExecution,
		workflowType,
	)
	if err := ms.ReplicateChildWorkflowExecutionTerminatedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateChildWorkflowExecutionTerminatedEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionTimedOutEventAttributes,
) (*historypb.HistoryEvent, error) {

	opTag := tag.WorkflowActionChildWorkflowTimedOut
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	ci, ok := ms.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedEventId == common.EmptyEventID {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool(ok),
			tag.WorkflowInitiatedID(initiatedID))
		return nil, ms.createInternalServerError(opTag)
	}

	workflowType := &commonpb.WorkflowType{
		Name: ci.WorkflowTypeName,
	}

	event := ms.hBuilder.AddChildWorkflowExecutionTimedOutEvent(
		ci.InitiatedEventId,
		ci.StartedEventId,
		namespace.Name(ci.Namespace),
		namespace.ID(ci.NamespaceId),
		childExecution,
		workflowType,
		attributes.RetryState,
	)
	if err := ms.ReplicateChildWorkflowExecutionTimedOutEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ReplicateChildWorkflowExecutionTimedOutEvent(
	event *historypb.HistoryEvent,
) error {

	attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) RetryActivity(
	ai *persistencespb.ActivityInfo,
	failure *failurepb.Failure,
) (enumspb.RetryState, error) {

	opTag := tag.WorkflowActionActivityTaskRetry
	if err := ms.checkMutability(opTag); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}

	if !ai.HasRetryPolicy {
		return enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, nil
	}

	if ai.CancelRequested {
		return enumspb.RETRY_STATE_CANCEL_REQUESTED, nil
	}

	if prev, ok := ms.pendingActivityInfoIDs[ai.ScheduledEventId]; ok {
		ms.approximateSize -= prev.Size()
	}

	now := ms.timeSource.Now()

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
	ai.Version = ms.GetCurrentVersion()
	ai.Attempt++
	ai.ScheduledTime = timestamp.TimePtr(now.Add(backoffInterval)) // update to next schedule time
	ai.StartedEventId = common.EmptyEventID
	ai.RequestId = ""
	ai.StartedTime = timestamp.TimePtr(time.Time{})
	ai.TimerTaskStatus = TimerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = ms.truncateRetryableActivityFailure(failure)

	if err := ms.taskGenerator.GenerateActivityRetryTasks(
		ai.ScheduledEventId,
	); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}

	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
	ms.approximateSize += ai.Size()
	return enumspb.RETRY_STATE_IN_PROGRESS, nil
}

func (ms *MutableStateImpl) truncateRetryableActivityFailure(
	activityFailure *failurepb.Failure,
) *failurepb.Failure {
	namespaceName := ms.namespaceEntry.Name().String()
	failureSize := activityFailure.Size()

	if failureSize <= ms.config.MutableStateActivityFailureSizeLimitWarn(namespaceName) {
		return activityFailure
	}

	throttledLogger := log.With(
		ms.shard.GetThrottledLogger(),
		tag.WorkflowNamespace(namespaceName),
		tag.WorkflowID(ms.executionInfo.WorkflowId),
		tag.WorkflowRunID(ms.executionState.RunId),
		tag.BlobSize(int64(failureSize)),
		tag.BlobSizeViolationOperation("RetryActivity"),
	)

	sizeLimitError := ms.config.MutableStateActivityFailureSizeLimitError(namespaceName)
	if failureSize <= sizeLimitError {
		throttledLogger.Warn("Activity failure size exceeds warning limit for mutable state.")
		return activityFailure
	}

	throttledLogger.Warn("Activity failure size exceeds error limit for mutable state, truncated.")

	// nonRetryable is set to false here as only retryable failures are recorded in mutable state.
	// also when this method is called, the check for isRetryable is already done, so the value
	// is only for visibility/debugging purpose.
	serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, false)
	serverFailure.Cause = failure.Truncate(activityFailure, sizeLimitError)
	return serverFailure
}

func (ms *MutableStateImpl) GetHistorySize() int64 {
	return ms.executionInfo.ExecutionStats.HistorySize
}

func (ms *MutableStateImpl) AddHistorySize(size int64) {
	ms.executionInfo.ExecutionStats.HistorySize += size
}

// TODO mutable state should generate corresponding transfer / timer tasks according to
//  updates accumulated, while currently all transfer / timer tasks are managed manually

// TODO convert AddTasks to prepareTasks

// AddTasks append transfer tasks
func (ms *MutableStateImpl) AddTasks(
	tasks ...tasks.Task,
) {

	for _, task := range tasks {
		category := task.GetCategory()
		ms.InsertTasks[category] = append(ms.InsertTasks[category], task)
	}
}

func (ms *MutableStateImpl) PopTasks() map[tasks.Category][]tasks.Task {
	insterTasks := ms.InsertTasks
	ms.InsertTasks = make(map[tasks.Category][]tasks.Task)
	return insterTasks
}

func (ms *MutableStateImpl) SetUpdateCondition(
	nextEventIDInDB int64,
	dbRecordVersion int64,
) {

	ms.nextEventIDInDB = nextEventIDInDB
	ms.dbRecordVersion = dbRecordVersion
}

func (ms *MutableStateImpl) GetUpdateCondition() (int64, int64) {
	return ms.nextEventIDInDB, ms.dbRecordVersion
}

func (ms *MutableStateImpl) SetSpeculativeWorkflowTaskTimeoutTask(
	task *tasks.WorkflowTaskTimeoutTask,
) error {
	ms.speculativeWorkflowTaskTimeoutTask = task
	return ms.shard.AddSpeculativeWorkflowTaskTimeoutTask(task)
}

func (ms *MutableStateImpl) CheckSpeculativeWorkflowTaskTimeoutTask(
	task *tasks.WorkflowTaskTimeoutTask,
) bool {
	return ms.speculativeWorkflowTaskTimeoutTask == task
}

func (ms *MutableStateImpl) RemoveSpeculativeWorkflowTaskTimeoutTask() {
	if ms.speculativeWorkflowTaskTimeoutTask != nil {
		// Cancelling task prevents it from being submitted to scheduler in memoryScheduledQueue.
		ms.speculativeWorkflowTaskTimeoutTask.Cancel()
		ms.speculativeWorkflowTaskTimeoutTask = nil
	}
}

func (ms *MutableStateImpl) GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus) {
	return ms.executionState.State, ms.executionState.Status
}

func (ms *MutableStateImpl) UpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) error {

	return setStateStatus(ms.executionState, state, status)
}

func (ms *MutableStateImpl) StartTransaction(
	namespaceEntry *namespace.Namespace,
) (bool, error) {
	if ms.hBuilder.IsDirty() || len(ms.InsertTasks) > 0 {
		ms.logger.Error("MutableState encountered dirty transaction",
			tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId),
			tag.WorkflowID(ms.executionInfo.WorkflowId),
			tag.WorkflowRunID(ms.executionState.RunId),
			tag.Value(ms.hBuilder),
		)
		ms.metricsHandler.Counter(metrics.MutableStateChecksumInvalidated.GetMetricName()).Record(1)
		return false, serviceerror.NewUnavailable("MutableState encountered dirty transaction")
	}

	namespaceEntry, err := ms.startTransactionHandleNamespaceMigration(namespaceEntry)
	if err != nil {
		return false, err
	}
	ms.namespaceEntry = namespaceEntry
	if err := ms.UpdateCurrentVersion(namespaceEntry.FailoverVersion(), false); err != nil {
		return false, err
	}

	flushBeforeReady, err := ms.startTransactionHandleWorkflowTaskFailover()
	if err != nil {
		return false, err
	}

	return flushBeforeReady, nil
}

func (ms *MutableStateImpl) CloseTransactionAsMutation(
	transactionPolicy TransactionPolicy,
) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error) {

	if err := ms.prepareCloseTransaction(
		transactionPolicy,
	); err != nil {
		return nil, nil, err
	}

	// It is important to convert speculative WT to normal before prepareEventsAndReplicationTasks,
	// because prepareEventsAndReplicationTasks will move internal buffered events to the history,
	// and WT related events (WTScheduled, in particular) need to go first.
	if err := ms.workflowTaskManager.convertSpeculativeWorkflowTaskToNormal(); err != nil {
		return nil, nil, err
	}

	workflowEventsSeq, bufferEvents, clearBuffer, err := ms.prepareEventsAndReplicationTasks(transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		lastEvent := lastEvents[len(lastEvents)-1]
		if err := ms.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, err
		}
	}

	// update last update time
	ms.executionInfo.LastUpdateTime = timestamp.TimePtr(ms.shard.GetTimeSource().Now())
	ms.executionInfo.StateTransitionCount += 1

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside Context.resetWorkflowExecution
	// currently, the updates done inside Context.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := ms.generateChecksum()

	if ms.dbRecordVersion == 0 {
		// noop, existing behavior
	} else {
		ms.dbRecordVersion += 1
	}

	workflowMutation := &persistence.WorkflowMutation{
		ExecutionInfo:  ms.executionInfo,
		ExecutionState: ms.executionState,
		NextEventID:    ms.hBuilder.NextEventID(),

		UpsertActivityInfos:       ms.updateActivityInfos,
		DeleteActivityInfos:       ms.deleteActivityInfos,
		UpsertTimerInfos:          ms.updateTimerInfos,
		DeleteTimerInfos:          ms.deleteTimerInfos,
		UpsertChildExecutionInfos: ms.updateChildExecutionInfos,
		DeleteChildExecutionInfos: ms.deleteChildExecutionInfos,
		UpsertRequestCancelInfos:  ms.updateRequestCancelInfos,
		DeleteRequestCancelInfos:  ms.deleteRequestCancelInfos,
		UpsertSignalInfos:         ms.updateSignalInfos,
		DeleteSignalInfos:         ms.deleteSignalInfos,
		UpsertSignalRequestedIDs:  ms.updateSignalRequestedIDs,
		DeleteSignalRequestedIDs:  ms.deleteSignalRequestedIDs,
		NewBufferedEvents:         bufferEvents,
		ClearBufferedEvents:       clearBuffer,

		Tasks: ms.InsertTasks,

		Condition:       ms.nextEventIDInDB,
		DBRecordVersion: ms.dbRecordVersion,
		Checksum:        checksum,
	}

	ms.checksum = checksum
	if err := ms.cleanupTransaction(transactionPolicy); err != nil {
		return nil, nil, err
	}
	return workflowMutation, workflowEventsSeq, nil
}

func (ms *MutableStateImpl) CloseTransactionAsSnapshot(
	transactionPolicy TransactionPolicy,
) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {

	if err := ms.prepareCloseTransaction(
		transactionPolicy,
	); err != nil {
		return nil, nil, err
	}

	if err := ms.workflowTaskManager.convertSpeculativeWorkflowTaskToNormal(); err != nil {
		return nil, nil, err
	}

	workflowEventsSeq, bufferEvents, _, err := ms.prepareEventsAndReplicationTasks(transactionPolicy)
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
		if err := ms.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, err
		}
	}

	// update last update time
	ms.executionInfo.LastUpdateTime = timestamp.TimePtr(ms.shard.GetTimeSource().Now())
	ms.executionInfo.StateTransitionCount += 1

	// we generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside Context.resetWorkflowExecution
	// currently, the updates done inside Context.resetWorkflowExecution doesn't
	// impact the checksum calculation
	checksum := ms.generateChecksum()

	if ms.dbRecordVersion == 0 {
		// noop, existing behavior
	} else {
		ms.dbRecordVersion += 1
	}

	workflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:  ms.executionInfo,
		ExecutionState: ms.executionState,
		NextEventID:    ms.hBuilder.NextEventID(),

		ActivityInfos:       ms.pendingActivityInfoIDs,
		TimerInfos:          ms.pendingTimerInfoIDs,
		ChildExecutionInfos: ms.pendingChildExecutionInfoIDs,
		RequestCancelInfos:  ms.pendingRequestCancelInfoIDs,
		SignalInfos:         ms.pendingSignalInfoIDs,
		SignalRequestedIDs:  ms.pendingSignalRequestedIDs,

		Tasks: ms.InsertTasks,

		Condition:       ms.nextEventIDInDB,
		DBRecordVersion: ms.dbRecordVersion,
		Checksum:        checksum,
	}

	ms.checksum = checksum
	if err := ms.cleanupTransaction(transactionPolicy); err != nil {
		return nil, nil, err
	}
	return workflowSnapshot, workflowEventsSeq, nil
}

func (ms *MutableStateImpl) IsResourceDuplicated(
	resourceDedupKey definition.DeduplicationID,
) bool {
	id := definition.GenerateDeduplicationKey(resourceDedupKey)
	_, duplicated := ms.appliedEvents[id]
	return duplicated
}

func (ms *MutableStateImpl) UpdateDuplicatedResource(
	resourceDedupKey definition.DeduplicationID,
) {
	id := definition.GenerateDeduplicationKey(resourceDedupKey)
	ms.appliedEvents[id] = struct{}{}
}

func (ms *MutableStateImpl) GenerateMigrationTasks() (tasks.Task, int64, error) {
	return ms.taskGenerator.GenerateMigrationTasks()
}

func (ms *MutableStateImpl) prepareCloseTransaction(
	transactionPolicy TransactionPolicy,
) error {

	if err := ms.closeTransactionWithPolicyCheck(
		transactionPolicy,
	); err != nil {
		return err
	}

	if err := ms.closeTransactionHandleBufferedEventsLimit(
		transactionPolicy,
	); err != nil {
		return err
	}

	if err := ms.closeTransactionHandleWorkflowReset(
		transactionPolicy,
	); err != nil {
		return err
	}

	// TODO merge active & passive task generation
	// NOTE: this function must be the last call
	//  since we only generate at most one activity & user timer,
	//  regardless of how many activity & user timer created
	//  so the calculation must be at the very end
	return ms.closeTransactionHandleActivityUserTimerTasks(
		transactionPolicy,
	)
}

func (ms *MutableStateImpl) cleanupTransaction(
	_ TransactionPolicy,
) error {

	ms.updateActivityInfos = make(map[int64]*persistencespb.ActivityInfo)
	ms.deleteActivityInfos = make(map[int64]struct{})
	ms.syncActivityTasks = make(map[int64]struct{})

	ms.updateTimerInfos = make(map[string]*persistencespb.TimerInfo)
	ms.deleteTimerInfos = make(map[string]struct{})

	ms.updateChildExecutionInfos = make(map[int64]*persistencespb.ChildExecutionInfo)
	ms.deleteChildExecutionInfos = make(map[int64]struct{})

	ms.updateRequestCancelInfos = make(map[int64]*persistencespb.RequestCancelInfo)
	ms.deleteRequestCancelInfos = make(map[int64]struct{})

	ms.updateSignalInfos = make(map[int64]*persistencespb.SignalInfo)
	ms.deleteSignalInfos = make(map[int64]struct{})

	ms.updateSignalRequestedIDs = make(map[string]struct{})
	ms.deleteSignalRequestedIDs = make(map[string]struct{})

	ms.stateInDB = ms.executionState.State
	ms.nextEventIDInDB = ms.GetNextEventID()
	// ms.dbRecordVersion remains the same

	ms.hBuilder = NewMutableHistoryBuilder(
		ms.timeSource,
		ms.shard.GenerateTaskIDs,
		ms.GetCurrentVersion(),
		ms.nextEventIDInDB,
		ms.bufferEventsInDB,
		ms.metricsHandler,
	)

	ms.InsertTasks = make(map[tasks.Category][]tasks.Task)

	return nil
}

func (ms *MutableStateImpl) prepareEventsAndReplicationTasks(
	transactionPolicy TransactionPolicy,
) ([]*persistence.WorkflowEvents, []*historypb.HistoryEvent, bool, error) {

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, nil, false, err
	}

	historyMutation, err := ms.hBuilder.Finish(!ms.HasStartedWorkflowTask())
	if err != nil {
		return nil, nil, false, err
	}

	// TODO @wxing1292 need more refactoring to make the logic clean
	ms.bufferEventsInDB = historyMutation.MemBufferBatch
	newBufferBatch := historyMutation.DBBufferBatch
	clearBuffer := historyMutation.DBClearBuffer
	newEventsBatches := historyMutation.DBEventsBatches
	ms.updatePendingEventIDs(historyMutation.ScheduledIDToStartedID)

	workflowEventsSeq := make([]*persistence.WorkflowEvents, len(newEventsBatches))
	historyNodeTxnIDs, err := ms.shard.GenerateTaskIDs(len(newEventsBatches))
	if err != nil {
		return nil, nil, false, err
	}
	for index, eventBatch := range newEventsBatches {
		workflowEventsSeq[index] = &persistence.WorkflowEvents{
			NamespaceID: ms.executionInfo.NamespaceId,
			WorkflowID:  ms.executionInfo.WorkflowId,
			RunID:       ms.executionState.RunId,
			BranchToken: currentBranchToken,
			PrevTxnID:   ms.executionInfo.LastFirstEventTxnId,
			TxnID:       historyNodeTxnIDs[index],
			Events:      eventBatch,
		}
		ms.executionInfo.LastFirstEventId = eventBatch[0].GetEventId()
		ms.executionInfo.LastFirstEventTxnId = historyNodeTxnIDs[index]
	}

	if err := ms.validateNoEventsAfterWorkflowFinish(
		transactionPolicy,
		workflowEventsSeq,
	); err != nil {
		return nil, nil, false, err
	}

	for _, workflowEvents := range workflowEventsSeq {
		if err := ms.eventsToReplicationTask(transactionPolicy, workflowEvents.Events); err != nil {
			return nil, nil, false, err
		}
	}

	ms.InsertTasks[tasks.CategoryReplication] = append(
		ms.InsertTasks[tasks.CategoryReplication],
		ms.syncActivityToReplicationTask(transactionPolicy)...,
	)

	if transactionPolicy == TransactionPolicyPassive &&
		len(ms.InsertTasks[tasks.CategoryReplication]) > 0 {
		return nil, nil, false, serviceerror.NewInternal("should not generate replication task when close transaction as passive")
	}

	return workflowEventsSeq, newBufferBatch, clearBuffer, nil
}

func (ms *MutableStateImpl) eventsToReplicationTask(
	transactionPolicy TransactionPolicy,
	events []*historypb.HistoryEvent,
) error {
	switch transactionPolicy {
	case TransactionPolicyActive:
		if ms.generateReplicationTask() {
			return ms.taskGenerator.GenerateHistoryReplicationTasks(events)
		}
		return nil
	case TransactionPolicyPassive:
		return nil
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) syncActivityToReplicationTask(
	transactionPolicy TransactionPolicy,
) []tasks.Task {
	now := time.Now().UTC()
	switch transactionPolicy {
	case TransactionPolicyActive:
		if ms.generateReplicationTask() {
			return convertSyncActivityInfos(
				now,
				definition.NewWorkflowKey(
					ms.executionInfo.NamespaceId,
					ms.executionInfo.WorkflowId,
					ms.executionState.RunId,
				),
				ms.pendingActivityInfoIDs,
				ms.syncActivityTasks,
			)
		}
		return nil
	case TransactionPolicyPassive:
		return emptyTasks
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) updatePendingEventIDs(
	scheduledIDToStartedID map[int64]int64,
) {
	for scheduledEventID, startedEventID := range scheduledIDToStartedID {
		if activityInfo, ok := ms.GetActivityInfo(scheduledEventID); ok {
			activityInfo.StartedEventId = startedEventID
			ms.updateActivityInfos[activityInfo.ScheduledEventId] = activityInfo
			continue
		}
		if childInfo, ok := ms.GetChildExecutionInfo(scheduledEventID); ok {
			childInfo.StartedEventId = startedEventID
			ms.updateChildExecutionInfos[childInfo.InitiatedEventId] = childInfo
			continue
		}
	}
}

func (ms *MutableStateImpl) updateWithLastWriteEvent(
	lastEvent *historypb.HistoryEvent,
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive {
		// already handled in mutable state.
		return nil
	}

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
	if err != nil {
		return err
	}
	if err := versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEvent.GetEventId(), lastEvent.GetVersion(),
	)); err != nil {
		return err
	}
	ms.executionInfo.LastEventTaskId = lastEvent.GetTaskId()

	return nil
}

// validateNoEventsAfterWorkflowFinish perform check on history event batch
// NOTE: do not apply this check on every batch, since transient
// workflow task && workflow finish will be broken (the first batch)
func (ms *MutableStateImpl) validateNoEventsAfterWorkflowFinish(
	transactionPolicy TransactionPolicy,
	workflowEventSeq []*persistence.WorkflowEvents,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		len(workflowEventSeq) == 0 {
		return nil
	}

	// only do check if workflow is finished
	if ms.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
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
		ms.logError(
			"encountered case where events appears after workflow finish.",
			tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId),
			tag.WorkflowID(ms.executionInfo.WorkflowId),
			tag.WorkflowRunID(ms.executionState.RunId),
		)
		return consts.ErrEventsAterWorkflowFinish
	}
}

func (ms *MutableStateImpl) startTransactionHandleNamespaceMigration(
	namespaceEntry *namespace.Namespace,
) (*namespace.Namespace, error) {
	// NOTE:
	// the main idea here is to guarantee that buffered events & namespace migration works
	// e.g. handle buffered events during version 0 => version > 0 by postponing namespace migration
	// * flush buffered events as if namespace is still local
	// * use updated namespace for actual call

	lastWriteVersion, err := ms.GetLastWriteVersion()
	if err != nil {
		return nil, err
	}

	// local namespace -> global namespace && with started workflow task
	if lastWriteVersion == common.EmptyVersion && namespaceEntry.FailoverVersion() > common.EmptyVersion && ms.HasStartedWorkflowTask() {
		localNamespaceMutation := namespace.NewPretendAsLocalNamespace(
			ms.clusterMetadata.GetCurrentClusterName(),
		)
		return namespaceEntry.Clone(localNamespaceMutation), nil
	}
	return namespaceEntry, nil
}

func (ms *MutableStateImpl) startTransactionHandleWorkflowTaskFailover() (bool, error) {

	if !ms.IsWorkflowExecutionRunning() {
		return false, nil
	}

	// NOTE:
	// the main idea here is to guarantee that once there is a workflow task started
	// all events ending in the buffer should have the same version

	// Handling mutable state turn from standby to active, while having a workflow task on the fly
	workflowTask := ms.GetStartedWorkflowTask()
	if workflowTask == nil || workflowTask.Version >= ms.GetCurrentVersion() {
		// no pending workflow tasks, no buffered events
		// or workflow task has higher / equal version
		return false, nil
	}

	currentVersion := ms.GetCurrentVersion()
	lastWriteVersion, err := ms.GetLastWriteVersion()
	if err != nil {
		return false, err
	}
	if lastWriteVersion != workflowTask.Version {
		return false, serviceerror.NewInternal(fmt.Sprintf("MutableStateImpl encountered mismatch version, workflow task: %v, last write version %v", workflowTask.Version, lastWriteVersion))
	}

	lastWriteSourceCluster := ms.clusterMetadata.ClusterNameForFailoverVersion(ms.namespaceEntry.IsGlobalNamespace(), lastWriteVersion)
	currentVersionCluster := ms.clusterMetadata.ClusterNameForFailoverVersion(ms.namespaceEntry.IsGlobalNamespace(), currentVersion)
	currentCluster := ms.clusterMetadata.GetCurrentClusterName()

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
		if ms.HasBufferedEvents() {
			return false, serviceerror.NewInternal("MutableStateImpl encountered previous passive workflow with buffered events")
		}
		return false, nil
	}

	// handle case 1 & 2
	var flushBufferVersion = lastWriteVersion

	// handle case 3
	if lastWriteSourceCluster != currentCluster && currentVersionCluster == currentCluster {
		// do a sanity check on buffered events
		if ms.HasBufferedEvents() {
			return false, serviceerror.NewInternal("MutableStateImpl encountered previous passive workflow with buffered events")
		}
		flushBufferVersion = currentVersion
	}

	// this workflow was previous active (whether it has buffered events or not),
	// the in flight workflow task must be failed to guarantee all events within same
	// event batch shard the same version
	if err := ms.UpdateCurrentVersion(flushBufferVersion, true); err != nil {
		return false, err
	}

	// we have a workflow task with buffered events on the fly with a lower version, fail it
	if _, err := failWorkflowTask(
		ms,
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
	); err != nil {
		return false, err
	}

	err = ScheduleWorkflowTask(ms)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (ms *MutableStateImpl) closeTransactionWithPolicyCheck(
	transactionPolicy TransactionPolicy,
) error {
	switch transactionPolicy {
	case TransactionPolicyActive:
		// Cannot use ms.namespaceEntry.ActiveClusterName() because currentVersion may be updated during this transaction in
		// passive cluster. For example: if passive cluster sees conflict and decided to terminate this workflow. The
		// currentVersion on mutable state would be updated to point to last write version which is current (passive) cluster.
		activeCluster := ms.clusterMetadata.ClusterNameForFailoverVersion(ms.namespaceEntry.IsGlobalNamespace(), ms.GetCurrentVersion())
		currentCluster := ms.clusterMetadata.GetCurrentClusterName()

		if activeCluster != currentCluster {
			namespaceID := ms.GetExecutionInfo().NamespaceId
			return serviceerror.NewNamespaceNotActive(namespaceID, currentCluster, activeCluster)
		}
		return nil
	case TransactionPolicyPassive:
		return nil
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) BufferSizeAcceptable() bool {
	if ms.hBuilder.NumBufferedEvents() > ms.config.MaximumBufferedEventsBatch() {
		return false
	}

	if ms.hBuilder.SizeInBytesOfBufferedEvents() > ms.config.MaximumBufferedEventsSizeInBytes() {
		return false
	}
	return true
}

func (ms *MutableStateImpl) closeTransactionHandleBufferedEventsLimit(
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
		return nil
	}

	if ms.BufferSizeAcceptable() {
		return nil
	}

	// Handling buffered events size issue
	if workflowTask := ms.GetStartedWorkflowTask(); workflowTask != nil {
		// we have a workflow task on the fly with a lower version, fail it
		if _, err := failWorkflowTask(
			ms,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		); err != nil {
			return err
		}

		err := ScheduleWorkflowTask(ms)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MutableStateImpl) closeTransactionHandleWorkflowReset(
	transactionPolicy TransactionPolicy,
) error {

	if transactionPolicy == TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
		return nil
	}

	// compare with bad client binary checksum and schedule a reset task

	// only schedule reset task if current doesn't have childWFs.
	// TODO: This will be removed once our reset allows childWFs
	if len(ms.GetPendingChildExecutionInfos()) != 0 {
		return nil
	}

	namespaceEntry, err := ms.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(ms.executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	if _, pt := FindAutoResetPoint(
		ms.timeSource,
		namespaceEntry.VerifyBinaryChecksum,
		ms.GetExecutionInfo().AutoResetPoints,
	); pt != nil {
		if err := ms.taskGenerator.GenerateWorkflowResetTasks(); err != nil {
			return err
		}
		ms.logInfo("Auto-Reset task is scheduled",
			tag.WorkflowNamespace(namespaceEntry.Name().String()),
			tag.WorkflowID(ms.executionInfo.WorkflowId),
			tag.WorkflowRunID(ms.executionState.RunId),
			tag.WorkflowResetBaseRunID(pt.GetRunId()),
			tag.WorkflowEventID(pt.GetFirstWorkflowTaskCompletedId()),
			tag.WorkflowBinaryChecksum(pt.GetBinaryChecksum()),
		)
	}
	return nil
}

func (ms *MutableStateImpl) closeTransactionHandleActivityUserTimerTasks(
	transactionPolicy TransactionPolicy,
) error {
	switch transactionPolicy {
	case TransactionPolicyActive:
		if !ms.IsWorkflowExecutionRunning() {
			return nil
		}
		if err := ms.taskGenerator.GenerateActivityTimerTasks(); err != nil {
			return err
		}
		return ms.taskGenerator.GenerateUserTimerTasks()
	case TransactionPolicyPassive:
		return nil
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) generateReplicationTask() bool {
	return len(ms.namespaceEntry.ClusterNames()) > 1
}

func (ms *MutableStateImpl) checkMutability(
	actionTag tag.ZapTag,
) error {

	if !ms.IsWorkflowExecutionRunning() {
		ms.logWarn(
			mutableStateInvalidHistoryActionMsg,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowState(ms.executionState.State),
			actionTag,
		)
		return ErrWorkflowFinished
	}
	return nil
}

func (ms *MutableStateImpl) generateChecksum() *persistencespb.Checksum {
	if !ms.shouldGenerateChecksum() {
		return nil
	}
	csum, err := generateMutableStateChecksum(ms)
	if err != nil {
		ms.logWarn("error generating MutableState checksum", tag.Error(err))
		return nil
	}
	return csum
}

func (ms *MutableStateImpl) shouldGenerateChecksum() bool {
	if ms.namespaceEntry == nil {
		return false
	}
	return rand.Intn(100) < ms.config.MutableStateChecksumGenProbability(ms.namespaceEntry.Name().String())
}

func (ms *MutableStateImpl) shouldVerifyChecksum() bool {
	if ms.namespaceEntry == nil {
		return false
	}
	return rand.Intn(100) < ms.config.MutableStateChecksumVerifyProbability(ms.namespaceEntry.Name().String())
}

func (ms *MutableStateImpl) shouldInvalidateCheckum() bool {
	invalidateBeforeEpochSecs := int64(ms.config.MutableStateChecksumInvalidateBefore())
	if invalidateBeforeEpochSecs > 0 {
		invalidateBefore := time.Unix(invalidateBeforeEpochSecs, 0).UTC()
		return ms.executionInfo.LastUpdateTime.Before(invalidateBefore)
	}
	return false
}

func (ms *MutableStateImpl) createInternalServerError(
	actionTag tag.ZapTag,
) error {

	return serviceerror.NewInternal(actionTag.Field().String + " operation failed")
}

func (ms *MutableStateImpl) createCallerError(
	actionTag tag.ZapTag,
	details string,
) error {
	msg := fmt.Sprintf(mutableStateInvalidHistoryActionMsgTemplate, actionTag.Field().String, details)
	return serviceerror.NewInvalidArgument(msg)
}

func (ms *MutableStateImpl) logInfo(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(ms.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(ms.executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId))
	ms.logger.Info(msg, tags...)
}

func (ms *MutableStateImpl) logWarn(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(ms.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(ms.executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId))
	ms.logger.Warn(msg, tags...)
}

func (ms *MutableStateImpl) logError(msg string, tags ...tag.Tag) {
	tags = append(tags, tag.WorkflowID(ms.executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(ms.executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId))
	ms.logger.Error(msg, tags...)
}

func (ms *MutableStateImpl) logDataInconsistency() {
	namespaceID := ms.executionInfo.NamespaceId
	workflowID := ms.executionInfo.WorkflowId
	runID := ms.executionState.RunId

	ms.logger.Error("encounter cassandra data inconsistency",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
	)
}
