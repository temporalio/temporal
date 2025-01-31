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
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporalnexus"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
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
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	emptyUUID = "emptyUuid"

	mutableStateInvalidHistoryActionMsg         = "invalid history builder state for action"
	mutableStateInvalidHistoryActionMsgTemplate = mutableStateInvalidHistoryActionMsg + ": %v, %v"

	int64SizeBytes = 8
)

// Scheduled tasks with timestamp after this will not be created.
// Those tasks are too far in the future and practically never fire and just consume storage space.
// NOTE: this value is less than timer.MaxAllowedTimer so that no capped timers will be created.
var maxScheduledTaskDuration = time.Hour * 24 * 365 * 99

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
	// ErrPinnedWorkflowCannotTransition indicates attempt to start a transition on a pinned workflow
	ErrPinnedWorkflowCannotTransition = serviceerror.NewInternal("unable to start transition on pinned workflows")

	timeZeroUTC = time.Unix(0, 0).UTC()
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
		deleteSignalRequestedIDs  map[string]struct{} // Deleted signaled requestId since last update

		executionInfo  *persistencespb.WorkflowExecutionInfo // Workflow mutable state info.
		executionState *persistencespb.WorkflowExecutionState

		hBuilder *historybuilder.HistoryBuilder

		// In-memory only attributes
		currentVersion int64
		// Running approximate total size of mutable state fields (except buffered events) when written to DB in bytes.
		// Buffered events are added to this value when calling GetApproximatePersistedSize.
		approximateSize int
		// Total number of tomestones tracked in mutable state
		totalTombstones int
		// Buffer events from DB
		bufferEventsInDB []*historypb.HistoryEvent
		// Indicates the workflow state in DB, can be used to calculate
		// whether this workflow is pointed by current workflow record.
		stateInDB enumsspb.WorkflowExecutionState
		// TODO deprecate nextEventIDInDB in favor of dbRecordVersion.
		// Indicates the next event ID in DB, for conditional update.
		nextEventIDInDB int64
		// Indicates the versionedTransition in DB, can be used to
		// calculate if the state-based replication is disabling.
		versionedTransitionInDB *persistencespb.VersionedTransition
		// Indicates the DB record version, for conditional update.
		dbRecordVersion int64
		// Namespace entry contains a snapshot of namespace.
		// NOTE: do not use the failover version inside, use currentVersion above.
		namespaceEntry *namespace.Namespace
		// Record if an event has been applied to mutable state.
		// TODO: persist this to db
		appliedEvents map[string]struct{}
		// A flag indicating if workflow has attempted to close (complete/cancel/continue as new)
		// but failed due to undelivered buffered events.
		// The flag will be unset whenever workflow task successfully completed, timedout or failed
		// due to cause other than UnhandledCommand.
		workflowCloseAttempted bool
		// A flag indicating if transition history feature is enabled for the current transaction.
		// We need a consistent view of if transition history is enabled within one transaction in case
		// dynamic configuration value changes in the middle of a transaction
		// TODO: remove this flag once transition history feature is stable.
		transitionHistoryEnabled bool
		// following fields are for tracking if sub state machines are updated
		// in a transaction. They are similar to field like updateActivityInfos
		visibilityUpdated     bool
		executionStateUpdated bool
		workflowTaskUpdated   bool
		updateInfoUpdated     map[string]struct{}
		// following xxxUserDataUpdated fields are for tracking if activity/timer user data updated.
		// This help to determine if we need to update transition history: For
		// user data change, we need to update transition history. No update for
		// non-user data change
		activityInfosUserDataUpdated map[int64]struct{}
		timerInfosUserDataUpdated    map[string]struct{}

		// in memory fields to track potential reapply events that needs to be reapplied during workflow update
		// should only be used in the state based replication as state based replication does not have
		// event inside history builder. This is only for x-run reapply (from zombie wf to current wf)
		reapplyEventsCandidate []*historypb.HistoryEvent

		InsertTasks map[tasks.Category][]tasks.Task

		speculativeWorkflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask

		// Do not rely on this, this is only updated on
		// Load() and closeTransactionXXX methods. So when
		// a transaction is in progress, this value will be
		// wrong. This exists primarily for visibility via CLI.
		checksum *persistencespb.Checksum

		taskGenerator       TaskGenerator
		workflowTaskManager *workflowTaskStateMachine
		QueryRegistry       QueryRegistry

		shard            shard.Context
		clusterMetadata  cluster.Metadata
		eventsCache      events.Cache
		config           *configs.Config
		timeSource       clock.TimeSource
		logger           log.Logger
		metricsHandler   metrics.Handler
		stateMachineNode *hsm.Node

		// Tracks all events added via the AddHistoryEvent method that is used by the state machine framework.
		currentTransactionAddedStateMachineEventTypes []enumspb.EventType
	}

	lastUpdatedStateTransitionGetter interface {
		proto.Message
		GetLastUpdateVersionedTransition() *persistencespb.VersionedTransition
		Size() int
	}
)

var _ MutableState = (*MutableStateImpl)(nil)

func NewMutableState(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
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

		approximateSize:              0,
		totalTombstones:              0,
		currentVersion:               namespaceEntry.FailoverVersion(),
		bufferEventsInDB:             nil,
		stateInDB:                    enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
		nextEventIDInDB:              common.FirstEventID,
		dbRecordVersion:              1,
		namespaceEntry:               namespaceEntry,
		appliedEvents:                make(map[string]struct{}),
		InsertTasks:                  make(map[tasks.Category][]tasks.Task),
		transitionHistoryEnabled:     shard.GetConfig().EnableTransitionHistory(),
		visibilityUpdated:            false,
		executionStateUpdated:        false,
		workflowTaskUpdated:          false,
		updateInfoUpdated:            make(map[string]struct{}),
		timerInfosUserDataUpdated:    make(map[string]struct{}),
		activityInfosUserDataUpdated: make(map[int64]struct{}),
		reapplyEventsCandidate:       []*historypb.HistoryEvent{},

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
		NamespaceId: namespaceEntry.ID().String(),
		WorkflowId:  workflowID,

		WorkflowTaskVersion:          common.EmptyVersion,
		WorkflowTaskScheduledEventId: common.EmptyEventID,
		WorkflowTaskStartedEventId:   common.EmptyEventID,
		WorkflowTaskRequestId:        emptyUUID,
		WorkflowTaskTimeout:          timestamp.DurationFromSeconds(0),
		WorkflowTaskAttempt:          1,

		LastCompletedWorkflowTaskStartedEventId: common.EmptyEventID,

		StartTime:              timestamppb.New(startTime),
		VersionHistories:       versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		ExecutionStats:         &persistencespb.ExecutionStats{HistorySize: 0},
		SubStateMachinesByType: make(map[string]*persistencespb.StateMachineMap),
	}
	if s.config.EnableNexus() {
		s.executionInfo.TaskGenerationShardClockTimestamp = shard.CurrentVectorClock().GetClock()
	}
	s.approximateSize += s.executionInfo.Size()

	s.executionState = &persistencespb.WorkflowExecutionState{
		RunId: runID,

		State:     enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: timestamppb.New(startTime),
	}
	s.approximateSize += s.executionState.Size()

	s.hBuilder = historybuilder.New(
		s.timeSource,
		s.shard.GenerateTaskIDs,
		s.currentVersion,
		common.FirstEventID,
		s.bufferEventsInDB,
		s.metricsHandler,
	)
	s.taskGenerator = taskGeneratorProvider.NewTaskGenerator(shard, s)
	s.workflowTaskManager = newWorkflowTaskStateMachine(s, s.metricsHandler)

	s.mustInitHSM()

	return s
}

func NewMutableStateFromDB(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	dbRecord *persistencespb.WorkflowMutableState,
	dbRecordVersion int64,
) (*MutableStateImpl, error) {
	// startTime will be overridden by DB record
	startTime := time.Time{}

	mutableState := NewMutableState(
		shard,
		eventsCache,
		logger,
		namespaceEntry,
		dbRecord.ExecutionInfo.WorkflowId,
		dbRecord.ExecutionState.RunId,
		startTime,
	)

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

	for _, tombstoneBatch := range dbRecord.ExecutionInfo.SubStateMachineTombstoneBatches {
		mutableState.totalTombstones += len(tombstoneBatch.StateMachineTombstones)
	}

	mutableState.approximateSize += dbRecord.ExecutionState.Size() - mutableState.executionState.Size()
	mutableState.executionState = dbRecord.ExecutionState
	mutableState.approximateSize += dbRecord.ExecutionInfo.Size() - mutableState.executionInfo.Size()
	mutableState.executionInfo = dbRecord.ExecutionInfo

	// StartTime was moved from ExecutionInfo to executionState
	if mutableState.executionState.StartTime == nil && dbRecord.ExecutionInfo.StartTime != nil {
		mutableState.executionState.StartTime = dbRecord.ExecutionInfo.StartTime
	}

	mutableState.hBuilder = historybuilder.New(
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
	mutableState.initVersionedTransitionInDB()

	if len(dbRecord.Checksum.GetValue()) > 0 {
		switch {
		case mutableState.shouldInvalidateCheckum():
			mutableState.checksum = nil
			metrics.MutableStateChecksumInvalidated.With(mutableState.metricsHandler).Record(1)
		case mutableState.shouldVerifyChecksum():
			if err := verifyMutableStateChecksum(mutableState, dbRecord.Checksum); err != nil {
				// we ignore checksum verification errors for now until this
				// feature is tested and/or we have mechanisms in place to deal
				// with these types of errors
				metrics.MutableStateChecksumMismatch.With(mutableState.metricsHandler).Record(1)
				mutableState.logError("mutable state checksum mismatch", tag.Error(err))
			}
		}
	}

	mutableState.mustInitHSM()

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
	// Although new versions of temporal server will perform state sanitization,
	// we have to keep the sanitization logic here as well for backward compatibility in case
	// source cluster is running an old version and doesn't do the sanitization.
	if err := SanitizeMutableState(mutableStateRecord); err != nil {
		return nil, err
	}
	if err := common.DiscardUnknownProto(mutableStateRecord); err != nil {
		return nil, err
	}

	mutableState, err := NewMutableStateFromDB(shard, eventsCache, logger, namespaceEntry, mutableStateRecord, 1)
	if err != nil {
		return nil, err
	}

	mutableState.executionInfo.LastFirstEventTxnId = lastFirstEventTxnID
	mutableState.currentVersion = lastWriteVersion

	return mutableState, nil
}

func NewMutableStateInChain(
	shardContext shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	startTime time.Time,
	currentMutableState MutableState,
) (*MutableStateImpl, error) {
	newMutableState := NewMutableState(
		shardContext,
		eventsCache,
		logger,
		namespaceEntry,
		workflowID,
		runID,
		startTime,
	)

	// carry over necessary fields from current mutable state
	newMutableState.executionInfo.WorkflowExecutionTimerTaskStatus = currentMutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus

	// Copy completion callbacks to new run.
	oldCallbacks := callbacks.MachineCollection(currentMutableState.HSM())
	newCallbacks := callbacks.MachineCollection(newMutableState.HSM())
	for _, node := range oldCallbacks.List() {
		cb, err := oldCallbacks.Data(node.Key.ID)
		if err != nil {
			return nil, err
		}
		if _, ok := cb.GetTrigger().GetVariant().(*persistencespb.CallbackInfo_Trigger_WorkflowClosed); ok {
			if _, err := newCallbacks.Add(node.Key.ID, cb); err != nil {
				return nil, err
			}
		}
	}

	// TODO: Today other information like autoResetPoints, previousRunID, firstRunID, etc.
	// are carried over in AddWorkflowExecutionStartedEventWithOptions. Ideally all information
	// should be carried over here since some information is not part of the startedEvent.
	return newMutableState, nil
}

func (ms *MutableStateImpl) mustInitHSM() {
	// Error only occurs if some initialization path forgets to register the workflow state machine.
	stateMachineNode, err := hsm.NewRoot(ms.shard.StateMachineRegistry(), StateMachineType, ms, ms.executionInfo.SubStateMachinesByType, ms)
	if err != nil {
		panic(err)
	}
	ms.stateMachineNode = stateMachineNode
}

func (ms *MutableStateImpl) HSM() *hsm.Node {
	return ms.stateMachineNode
}

// GetNexusCompletion converts a workflow completion event into a [nexus.OperationCompletion].
// Completions may be sent to arbitrary third parties, we intentionally do not include any termination reasons, and
// expose only failure messages.
func (ms *MutableStateImpl) GetNexusCompletion(ctx context.Context) (nexus.OperationCompletion, error) {
	ce, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}
	// Create the link information about the workflow to be attached to fabricated started event if completion is
	// received before start response.
	startLink := temporalnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  ms.namespaceEntry.Name().String(),
		WorkflowId: ms.executionInfo.WorkflowId,
		RunId:      ms.executionState.RunId,
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	})
	switch ce.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		payloads := ce.GetWorkflowExecutionCompletedEventAttributes().GetResult().GetPayloads()
		var p *commonpb.Payload // default to nil, the payload serializer converts nil to Nexus nil Content.
		if len(payloads) > 0 {
			// All of our SDKs support returning a single value from workflows, we can safely ignore the
			// rest of the payloads. Additionally, even if a workflow could return more than a single value,
			// Nexus does not support it.
			p = payloads[0]
		}
		completion, err := nexus.NewOperationCompletionSuccessful(p, nexus.OperationCompletionSuccessfulOptions{
			Serializer: commonnexus.PayloadSerializer,
			StartTime:  ms.executionState.GetStartTime().AsTime(),
			Links:      []nexus.Link{startLink},
		})
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("failed to construct Nexus completion: %v", err))
		}
		return completion, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		f, err := commonnexus.APIFailureToNexusFailure(ce.GetWorkflowExecutionFailedEventAttributes().GetFailure())
		if err != nil {
			return nil, err
		}
		return nexus.NewOperationCompletionUnsuccessful(nexus.NewFailedOperationError(&nexus.FailureError{Failure: f}), nexus.OperationCompletionUnsuccessfulOptions{
			StartTime: ms.executionState.GetStartTime().AsTime(),
			Links:     []nexus.Link{startLink},
		})
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		f, err := commonnexus.APIFailureToNexusFailure(&failurepb.Failure{
			Message: "operation canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details: ce.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return nexus.NewOperationCompletionUnsuccessful(nexus.NewCanceledOperationError(&nexus.FailureError{Failure: f}), nexus.OperationCompletionUnsuccessfulOptions{
			StartTime: ms.executionState.GetStartTime().AsTime(),
			Links:     []nexus.Link{startLink},
		})
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		f, err := commonnexus.APIFailureToNexusFailure(&failurepb.Failure{
			Message: "operation terminated",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		})
		if err != nil {
			return nil, err
		}
		return nexus.NewOperationCompletionUnsuccessful(nexus.NewFailedOperationError(&nexus.FailureError{Failure: f}), nexus.OperationCompletionUnsuccessfulOptions{
			StartTime: ms.executionState.GetStartTime().AsTime(),
			Links:     []nexus.Link{startLink},
		})
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		f, err := commonnexus.APIFailureToNexusFailure(&failurepb.Failure{
			Message: "operation exceeded internal timeout",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					// Not filling in timeout type and other information, it's not particularly interesting to a Nexus
					// caller.
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return nexus.NewOperationCompletionUnsuccessful(nexus.NewFailedOperationError(&nexus.FailureError{Failure: f}), nexus.OperationCompletionUnsuccessfulOptions{
			StartTime: ms.executionState.GetStartTime().AsTime(),
			Links:     []nexus.Link{startLink},
		})
	}
	return nil, serviceerror.NewInternal(fmt.Sprintf("invalid workflow execution status: %v", ce.GetEventType()))
}

// GetHSMCallbackArg converts a workflow completion event into a [persistencespb.HSMCallbackArg].
func (ms *MutableStateImpl) GetHSMCompletionCallbackArg(ctx context.Context) (*persistencespb.HSMCompletionCallbackArg, error) {
	workflowKey := ms.GetWorkflowKey()
	ce, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}
	return &persistencespb.HSMCompletionCallbackArg{
		NamespaceId: workflowKey.NamespaceID,
		WorkflowId:  workflowKey.WorkflowID,
		RunId:       workflowKey.RunID,
		LastEvent:   ce,
	}, nil
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
	executionTimeout *durationpb.Duration,
	runTimeout *durationpb.Duration,
	treeID string,
) error {
	// NOTE: Unfortunately execution timeout and run timeout are not yet initialized into ms.executionInfo at this point.
	// TODO: Consider explicitly initializing mutable state with these timeout parameters instead of passing them in.

	workflowKey := ms.GetWorkflowKey()
	var retentionDuration *durationpb.Duration
	if duration := ms.namespaceEntry.Retention(); duration > 0 {
		retentionDuration = durationpb.New(duration)
	}
	initialBranchToken, err := ms.shard.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
		workflowKey.NamespaceID,
		workflowKey.WorkflowID,
		workflowKey.RunID,
		treeID,
		nil,
		[]*persistencespb.HistoryBranchRange{},
		runTimeout.AsDuration(),
		executionTimeout.AsDuration(),
		retentionDuration.AsDuration(),
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

func (ms *MutableStateImpl) SetHistoryBuilder(hBuilder *historybuilder.HistoryBuilder) {
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

func (ms *MutableStateImpl) UpdateResetRunID(runID string) {
	ms.executionInfo.ResetRunId = runID
}

// IsResetRun returns true if this run is the result of a reset operation.
// A run is a reset run if OriginalExecutionRunID points to another run.
//
// This method only works for workflows started by server version 1.27.0+.
// Older workflows don't have OriginalExecutionRunID set in mutable state,
// and this method will NOT try to load WorkflowExecutionStarted event to
// get that information.
func (ms *MutableStateImpl) IsResetRun() bool {
	originalExecutionRunID := ms.GetExecutionInfo().GetOriginalExecutionRunId()
	return len(originalExecutionRunID) != 0 && originalExecutionRunID != ms.GetExecutionState().GetRunId()
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
	if ms.transitionHistoryEnabled && len(ms.executionInfo.TransitionHistory) != 0 {
		// this make sure current version >= last write version
		lastVersionedTransition := ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1]
		ms.currentVersion = lastVersionedTransition.NamespaceFailoverVersion
	} else {
		versionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
		if err != nil {
			return err
		}

		if !versionhistory.IsEmptyVersionHistory(versionHistory) {
			// this make sure current version >= last event version
			versionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
			if err != nil {
				return err
			}
			ms.currentVersion = versionHistoryItem.GetVersion()
		}
	}

	if version > ms.currentVersion || forceUpdate {
		ms.currentVersion = version
	}

	ms.hBuilder = historybuilder.New(
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

// NextTransitionCount implements hsm.NodeBackend.
func (ms *MutableStateImpl) NextTransitionCount() int64 {
	if !ms.transitionHistoryEnabled {
		return 0
	}

	hist := ms.executionInfo.TransitionHistory
	if len(hist) == 0 {
		// it is possible that this is the first transition and
		// transition history has not been updated yet.
		return 1
	}
	return hist[len(hist)-1].TransitionCount + 1
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

func (ms *MutableStateImpl) GetCloseVersion() (int64, error) {
	if ms.IsWorkflowExecutionRunning() {
		return common.EmptyVersion, serviceerror.NewInternal("GetCloseVersion: workflow is still running")
	}

	// here we assume that closing a workflow must generate an event

	// if workflow is closing in the current transation,
	// then the last event is closed event and the event version is the close version
	if lastEventVersion, ok := ms.hBuilder.LastEventVersion(); ok {
		return lastEventVersion, nil
	}

	return ms.GetLastEventVersion()
}

func (ms *MutableStateImpl) GetLastWriteVersion() (int64, error) {
	if ms.transitionHistoryEnabled && len(ms.executionInfo.TransitionHistory) != 0 {
		lastVersionedTransition := ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1]
		return lastVersionedTransition.NamespaceFailoverVersion, nil
	}

	return ms.GetLastEventVersion()
}

func (ms *MutableStateImpl) GetLastEventVersion() (int64, error) {
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

// AddHistoryEvent adds any history event to this workflow execution.
// The provided setAttributes function should be used to set the attributes on the event.
func (ms *MutableStateImpl) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	event := ms.hBuilder.AddHistoryEvent(t, setAttributes)
	if event.EventId != common.BufferedEventID {
		ms.writeEventToCache(event)
	}
	ms.currentTransactionAddedStateMachineEventTypes = append(ms.currentTransactionAddedStateMachineEventTypes, t)
	return event
}

func (ms *MutableStateImpl) LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error) {
	ref := &tokenspb.HistoryEventRef{}
	err := proto.Unmarshal(token, ref)
	if err != nil {
		return nil, err
	}
	branchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(ref.EventId)
	if err != nil {
		return nil, err
	}
	wfKey := ms.GetWorkflowKey()
	eventKey := events.EventKey{
		NamespaceID: namespace.ID(wfKey.NamespaceID),
		WorkflowID:  wfKey.WorkflowID,
		RunID:       wfKey.RunID,
		EventID:     ref.EventId,
		Version:     version,
	}

	return ms.eventsCache.GetEvent(ctx, ms.shard.GetShardID(), eventKey, ref.EventBatchId, branchToken)
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

func (ms *MutableStateImpl) SetStickyTaskQueue(name string, scheduleToStartTimeout *durationpb.Duration) {
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
// Task queue kind (sticky or normal) is set based on comparison of normal task queue name
// in mutable state and provided name.
// ScheduleToStartTimeout is set based on queue kind and workflow task type.
func (ms *MutableStateImpl) TaskQueueScheduleToStartTimeout(tqName string) (*taskqueuepb.TaskQueue, *durationpb.Duration) {
	isStickyTq := ms.executionInfo.StickyTaskQueue == tqName
	if isStickyTq {
		return &taskqueuepb.TaskQueue{
			Name:       ms.executionInfo.StickyTaskQueue,
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: ms.executionInfo.TaskQueue,
		}, ms.executionInfo.StickyScheduleToStartTimeout
	}

	// If tqName is normal task queue name.
	normalTq := &taskqueuepb.TaskQueue{
		Name: ms.executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	if ms.executionInfo.WorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// Speculative WFT has ScheduleToStartTimeout even on normal task queue.
		// See comment in GenerateScheduleSpeculativeWorkflowTaskTasks for details.
		return normalTq, durationpb.New(tasks.SpeculativeWorkflowTaskScheduleToStartTimeout)
	}
	// No WFT ScheduleToStart timeout for normal WFT on normal task queue.
	return normalTq, ms.executionInfo.WorkflowRunTimeout
}

func (ms *MutableStateImpl) GetWorkflowType() *commonpb.WorkflowType {
	wType := &commonpb.WorkflowType{}
	wType.Name = ms.executionInfo.WorkflowTypeName

	return wType
}

func (ms *MutableStateImpl) GetQueryRegistry() QueryRegistry {
	return ms.QueryRegistry
}

// VisitUpdates visits mutable state update entries, ordered by the ID of the history event pointed to by the mutable
// state entry. Thus, for example, updates entries in Admitted state will be visited in the order that their Admitted
// events were added to history.
func (ms *MutableStateImpl) VisitUpdates(visitor func(updID string, updInfo *persistencespb.UpdateInfo)) {
	type updateEvent struct {
		updId   string
		updInfo *persistencespb.UpdateInfo
		eventId int64
	}
	var updateEvents []updateEvent
	for updID, updInfo := range ms.executionInfo.GetUpdateInfos() {
		u := updateEvent{
			updId:   updID,
			updInfo: updInfo,
		}
		if adm := updInfo.GetAdmission(); adm != nil {
			u.eventId = adm.GetHistoryPointer().EventId
		} else if acc := updInfo.GetAcceptance(); acc != nil {
			u.eventId = acc.EventId
		} else if com := updInfo.GetCompletion(); com != nil {
			u.eventId = com.EventId
		}
		updateEvents = append(updateEvents, u)
	}
	slices.SortFunc(updateEvents, func(u1, u2 updateEvent) int { return cmp.Compare(u1.eventId, u2.eventId) })

	for _, u := range updateEvents {
		visitor(u.updId, u.updInfo)
	}
}

func (ms *MutableStateImpl) GetUpdateOutcome(
	ctx context.Context,
	updateID string,
) (*updatepb.Outcome, error) {
	if ms.executionInfo.UpdateInfos == nil {
		return nil, serviceerror.NewNotFound("update not found")
	}
	ui, ok := ms.executionInfo.UpdateInfos[updateID]
	if !ok {
		return nil, serviceerror.NewNotFound("update not found")
	}
	completion := ui.GetCompletion()
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
	event, err := ms.eventsCache.GetEvent(ctx, ms.shard.GetShardID(), eventKey, completion.EventBatchId, currentBranchToken)
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
		ms.shard.GetShardID(),
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
		ms.shard.GetShardID(),
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
		ms.shard.GetShardID(),
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
		ms.shard.GetShardID(),
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
) (event *historypb.HistoryEvent, err error) {
	defer func() {
		if common.IsNotFoundError(err) {
			// do not return the original error
			// since original error of type NotFound
			// can cause task processing side to fail silently
			err = ErrMissingWorkflowCompletionEvent
		}
	}()

	if ms.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil, ErrMissingWorkflowCompletionEvent
	}

	// Completion EventID is always one less than NextEventID after workflow is completed
	nextEventID := ms.hBuilder.NextEventID()
	completionEventID := nextEventID - 1
	firstEventID := ms.executionInfo.CompletionEventBatchId

	currentBranchToken, version, err := ms.getCurrentBranchTokenAndEventVersion(completionEventID)
	if err != nil {
		return nil, err
	}

	event, err = ms.eventsCache.GetEvent(
		ctx,
		ms.shard.GetShardID(),
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
		if (common.IsNotFoundError(err) ||
			common.IsInternalError(err)) &&
			ms.executionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			// Certain terminated workflows have an incorrect completionEventBatchId recorded which
			// prevents the completion event from being loaded outside of cache.
			//
			// If we get back an internal error, attempt to search history (most recent
			// events first) to find the completion event. Event will be earlier than
			// the recorded CompletionEventBatchID, and should be a part of the same batch.
			//
			// See also: https://github.com/temporalio/temporal/pull/6180
			//
			// TODO: Remove 90 days after deployment (remove after 10/16/2024)
			_, txID := ms.GetLastFirstEventIDTxnID()
			resp, err := ms.shard.GetExecutionManager().ReadHistoryBranchReverse(ctx, &persistence.ReadHistoryBranchReverseRequest{
				ShardID:                ms.shard.GetShardID(),
				BranchToken:            currentBranchToken,
				MaxEventID:             nextEventID, // looking for an event in the most recent batch
				PageSize:               1,
				LastFirstTransactionID: txID,
				NextPageToken:          []byte{},
			})
			if err != nil {
				return nil, err
			}

			for _, event := range resp.HistoryEvents {
				// this only applies to terminated workflows whose ultimate WFT had been failed
				if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
					return event, nil
				}
			}
		}

		return nil, err
	}

	return event, nil
}

// GetWorkflowCloseTime returns workflow closed time, returns a zero time for open workflow
func (ms *MutableStateImpl) GetWorkflowCloseTime(ctx context.Context) (time.Time, error) {
	if ms.executionState.GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED && ms.executionInfo.CloseTime == nil {
		// This is for backward compatible. Prior to v1.16 does not have close time in mutable state (Added by 05/21/2022).
		// TODO: remove this logic when all mutable state contains close time.
		completionEvent, err := ms.GetCompletionEvent(ctx)
		if err != nil {
			return time.Time{}, err
		}
		return completionEvent.GetEventTime().AsTime(), nil
	}
	return ms.executionInfo.CloseTime.AsTime(), nil
}

// GetWorkflowExecutionDuration returns the workflow execution duration.
// Returns zero for open workflow.
func (ms *MutableStateImpl) GetWorkflowExecutionDuration(ctx context.Context) (time.Duration, error) {
	closeTime, err := ms.GetWorkflowCloseTime(ctx)
	if err != nil {
		return 0, err
	}
	if closeTime.IsZero() || ms.executionInfo.ExecutionTime == nil {
		return 0, nil
	}
	return closeTime.Sub(ms.executionInfo.ExecutionTime.AsTime()), nil
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
		ms.shard.GetShardID(),
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
	// For start event: store it here so the recordWorkflowStarted transfer task doesn't need to
	// load it from database.
	// For completion event: store it here so we can communicate the result to parent execution
	// during the processing of DeleteTransferTask without loading this event from database.
	// For Update events: store it here so that Update disposition lookups can be fast.
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
	ai.LastHeartbeatUpdateTime = timestamppb.New(now)
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}
	ms.approximateSize += ai.Size()
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
}

// UpdateActivityInfo applies the necessary activity information
func (ms *MutableStateImpl) UpdateActivityInfo(
	incomingActivityInfo *historyservice.ActivitySyncInfo,
	resetActivityTimerTaskStatus bool,
) error {
	ai, ok := ms.pendingActivityInfoIDs[incomingActivityInfo.GetScheduledEventId()]
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find activity event ID: %v in mutable state", incomingActivityInfo.GetScheduledEventId()),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ms.approximateSize -= ai.Size()
	oldPaused := ai.Paused

	ai.Version = incomingActivityInfo.GetVersion()
	ai.ScheduledTime = incomingActivityInfo.GetScheduledTime()
	// we don't need to update FirstScheduledTime
	ai.StartedEventId = incomingActivityInfo.GetStartedEventId()
	ai.LastHeartbeatUpdateTime = incomingActivityInfo.GetLastHeartbeatTime()
	if ai.StartedEventId == common.EmptyEventID {
		ai.StartedTime = nil
	} else {
		ai.StartedTime = incomingActivityInfo.GetStartedTime()
	}
	ai.LastHeartbeatDetails = incomingActivityInfo.GetDetails()
	ai.Attempt = incomingActivityInfo.GetAttempt()
	ai.RetryLastWorkerIdentity = incomingActivityInfo.GetLastWorkerIdentity()
	ai.RetryLastFailure = incomingActivityInfo.LastFailure

	if resetActivityTimerTaskStatus {
		ai.TimerTaskStatus = TimerTaskStatusNone
	}

	ai.FirstScheduledTime = incomingActivityInfo.GetFirstScheduledTime()
	ai.LastAttemptCompleteTime = incomingActivityInfo.GetLastAttemptCompleteTime()
	ai.Stamp = incomingActivityInfo.GetStamp()

	ai.Paused = incomingActivityInfo.GetPaused()

	ai.RetryInitialInterval = incomingActivityInfo.GetRetryInitialInterval()
	ai.RetryMaximumInterval = incomingActivityInfo.GetRetryMaximumInterval()
	ai.RetryMaximumAttempts = incomingActivityInfo.GetRetryMaximumAttempts()
	ai.RetryBackoffCoefficient = incomingActivityInfo.GetRetryBackoffCoefficient()

	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}
	ms.approximateSize += ai.Size()

	err := ms.applyActivityBuildIdRedirect(ai, incomingActivityInfo.GetLastStartedBuildId(), incomingActivityInfo.GetLastStartedRedirectCounter())
	if err != nil {
		return err
	}

	if oldPaused != ai.Paused {
		err = ms.updatePauseInfoSearchAttribute()
	}

	return err
}

// UpdateActivityTaskStatusWithTimerHeartbeat updates an activity's timer task status or/and timer heartbeat
func (ms *MutableStateImpl) UpdateActivityTaskStatusWithTimerHeartbeat(scheduleEventID int64, timerTaskStatus int32, heartbeatTimeoutVisibility *time.Time) error {
	ai, ok := ms.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find activity event ID: %v in mutable state", scheduleEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingActivityInfo
	}

	ai.TimerTaskStatus = timerTaskStatus
	ms.updateActivityInfos[ai.ScheduledEventId] = ai

	if heartbeatTimeoutVisibility != nil {
		ms.pendingActivityTimerHeartbeats[scheduleEventID] = *heartbeatTimeoutVisibility
	}
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
	delete(ms.activityInfosUserDataUpdated, scheduledEventID)
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
	ms.timerInfosUserDataUpdated[ti.TimerId] = struct{}{}
	return nil
}

func (ms *MutableStateImpl) UpdateUserTimerTaskStatus(timerID string, status int64) error {
	timerInfo, ok := ms.pendingTimerInfoIDs[timerID]
	if !ok {
		ms.logError(
			fmt.Sprintf("unable to find timer ID: %v in mutable state", timerID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		return ErrMissingTimerInfo
	}
	timerInfo.TaskStatus = status
	ms.updateTimerInfos[timerID] = timerInfo
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
	delete(ms.timerInfosUserDataUpdated, timerID)
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

func (ms *MutableStateImpl) GetPendingSignalRequestedIds() []string {
	return convert.StringSetToSlice(ms.pendingSignalRequestedIDs)
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
		WorkflowTaskTimeout: time.Duration(0),
		Attempt:             1,
		StartedTime:         timeZeroUTC,
		ScheduledTime:       timeZeroUTC,

		TaskQueue:             nil,
		OriginalScheduledTime: timeZeroUTC,
		Type:                  enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED,

		SuggestContinueAsNew: false,
		HistorySizeBytes:     0,
	}
	ms.workflowTaskManager.UpdateWorkflowTask(emptyWorkflowTaskInfo)
	return nil
}

func (ms *MutableStateImpl) GetAssignedBuildId() string {
	return ms.executionInfo.AssignedBuildId
}

func (ms *MutableStateImpl) GetInheritedBuildId() string {
	return ms.executionInfo.InheritedBuildId
}

func (ms *MutableStateImpl) GetMostRecentWorkerVersionStamp() *commonpb.WorkerVersionStamp {
	return ms.executionInfo.MostRecentWorkerVersionStamp
}

func (ms *MutableStateImpl) HasBufferedEvents() bool {
	return ms.hBuilder.HasBufferEvents()
}

// HasAnyBufferedEvent returns true if there is at least one buffered event that matches the provided filter.
func (ms *MutableStateImpl) HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool {
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

// GetStartedEventIdForLastCompletedWorkflowTask returns last started workflow task event ID
func (ms *MutableStateImpl) GetLastCompletedWorkflowTaskStartedEventId() int64 {
	return ms.executionInfo.LastCompletedWorkflowTaskStartedEventId
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
	_, ok := ms.pendingSignalRequestedIDs[requestID]
	return ok
}

func (ms *MutableStateImpl) IsWorkflowPendingOnWorkflowTaskBackoff() bool {
	workflowTaskBackoff := timestamp.TimeValue(ms.executionInfo.GetExecutionTime()).After(timestamp.TimeValue(ms.executionState.GetStartTime()))
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
	ms.pendingSignalRequestedIDs[requestID] = struct{}{}
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

func (ms *MutableStateImpl) attachRequestID(requestID string) {
	if ms.executionState.AttachedRequestIds == nil {
		ms.executionState.AttachedRequestIds = []string{}
	}
	ms.executionState.AttachedRequestIds = append(ms.executionState.AttachedRequestIds, requestID)
}

func (ms *MutableStateImpl) addWorkflowExecutionStartedEventForContinueAsNew(
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	execution *commonpb.WorkflowExecution,
	previousExecutionState MutableState,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
	firstRunID string,
	rootExecutionInfo *workflowspb.RootExecutionInfo,
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

	var taskTimeout *durationpb.Duration
	if command.GetWorkflowTaskTimeout().AsDuration() == 0 {
		taskTimeout = previousExecutionInfo.DefaultWorkflowTaskTimeout
	} else {
		taskTimeout = command.GetWorkflowTaskTimeout()
	}

	// Workflow runTimeout is already set to the correct value in
	// validateContinueAsNewWorkflowExecutionAttributes
	// TODO: make this consistent with other fields, i.e. either always fallback
	// to the value in the previous execution or always use the value in the command
	// for other fields as well.
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

	var sourceVersionStamp *commonpb.WorkerVersionStamp
	var inheritedBuildId string
	if command.InheritBuildId {
		inheritedBuildId = previousExecutionInfo.AssignedBuildId
		if inheritedBuildId == "" {
			// TODO: this is only needed for old versioning. get rid of StartWorkflowExecutionRequest.SourceVersionStamp
			// [cleanup-old-wv]
			// Copy version stamp to new workflow only if:
			// - command says to use compatible version
			// - using versioning
			sourceVersionStamp = worker_versioning.StampIfUsingVersioning(previousExecutionInfo.MostRecentWorkerVersionStamp)
		}
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
		RootExecutionInfo:        rootExecutionInfo,
		InheritedBuildId:         inheritedBuildId,
	}
	if command.GetInitiator() == enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY {
		req.Attempt = previousExecutionState.GetExecutionInfo().Attempt + 1
	} else {
		req.Attempt = 1
	}
	workflowTimeoutTime := timestamp.TimeValue(previousExecutionState.GetExecutionInfo().WorkflowExecutionExpirationTime)
	if !workflowTimeoutTime.IsZero() {
		req.WorkflowExecutionExpirationTime = timestamppb.New(workflowTimeoutTime)
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

func (ms *MutableStateImpl) ContinueAsNewMinBackoff(backoffDuration *durationpb.Duration) *durationpb.Duration {
	// lifetime of previous execution
	lifetime := ms.timeSource.Now().Sub(ms.executionState.StartTime.AsTime().UTC())
	if ms.executionInfo.ExecutionTime != nil {
		lifetime = ms.timeSource.Now().Sub(ms.executionInfo.ExecutionTime.AsTime().UTC())
	}

	interval := lifetime
	if backoffDuration != nil {
		// already has a backoff, add it to interval
		interval += backoffDuration.AsDuration()
	}
	// minimal interval for continue as new to prevent tight continue as new loop
	minInterval := ms.config.WorkflowIdReuseMinimalInterval(ms.namespaceEntry.Name().String())
	if interval < minInterval {
		// enforce a minimal backoff
		return durationpb.New(minInterval - lifetime)
	}

	return backoffDuration
}

func (ms *MutableStateImpl) AddWorkflowExecutionStartedEvent(
	execution *commonpb.WorkflowExecution,
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
	execution *commonpb.WorkflowExecution,
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
		ms.executionState.StartTime.AsTime(),
		startRequest,
		resetPoints,
		prevRunID,
		firstRunID,
		execution.GetRunId(),
	)
	if err := ms.ApplyWorkflowExecutionStartedEvent(
		startRequest.GetParentExecutionInfo().GetClock(),
		execution,
		startRequest.StartRequest.GetRequestId(),
		event,
	); err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	var err error
	ms.executionInfo.WorkflowExecutionTimerTaskStatus, err = ms.taskGenerator.GenerateWorkflowStartTasks(
		event,
	)
	if err != nil {
		return nil, err
	}

	if err := ms.taskGenerator.GenerateRecordWorkflowStartedTasks(
		event,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionStartedEvent(
	parentClock *clockspb.VectorClock,
	execution *commonpb.WorkflowExecution,
	requestID string,
	startEvent *historypb.HistoryEvent,
) error {
	if ms.executionInfo.NamespaceId != ms.namespaceEntry.ID().String() {
		return serviceerror.NewInternal(fmt.Sprintf("applying conflicting namespace ID: %v != %v",
			ms.executionInfo.NamespaceId, ms.namespaceEntry.ID().String()))
	}
	if ms.executionInfo.WorkflowId != execution.GetWorkflowId() {
		return serviceerror.NewInternal(fmt.Sprintf("applying conflicting workflow ID: %v != %v",
			ms.executionInfo.WorkflowId, execution.GetWorkflowId()))
	}
	if ms.executionState.RunId != execution.GetRunId() {
		return serviceerror.NewInternal(fmt.Sprintf("applying conflicting run ID: %v != %v",
			ms.executionState.RunId, execution.GetRunId()))
	}

	ms.approximateSize -= ms.executionInfo.Size()
	event := startEvent.GetWorkflowExecutionStartedEventAttributes()
	ms.executionState.CreateRequestId = requestID
	ms.executionInfo.FirstExecutionRunId = event.GetFirstExecutionRunId()
	ms.executionInfo.TaskQueue = event.TaskQueue.GetName()
	ms.executionInfo.WorkflowTypeName = event.WorkflowType.GetName()
	ms.executionInfo.WorkflowRunTimeout = event.GetWorkflowRunTimeout()
	ms.executionInfo.WorkflowExecutionTimeout = event.GetWorkflowExecutionTimeout()
	ms.executionInfo.DefaultWorkflowTaskTimeout = event.GetWorkflowTaskTimeout()
	ms.executionInfo.OriginalExecutionRunId = event.GetOriginalExecutionRunId()

	if err := ms.addCompletionCallbacks(startEvent, event.GetCompletionCallbacks()); err != nil {
		return err
	}
	if err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	); err != nil {
		return err
	}
	ms.executionInfo.LastCompletedWorkflowTaskStartedEventId = common.EmptyEventID
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

	if event.RootWorkflowExecution != nil {
		ms.executionInfo.RootWorkflowId = event.RootWorkflowExecution.GetWorkflowId()
		ms.executionInfo.RootRunId = event.RootWorkflowExecution.GetRunId()
	} else {
		ms.executionInfo.RootWorkflowId = execution.GetWorkflowId()
		ms.executionInfo.RootRunId = execution.GetRunId()
	}

	ms.executionInfo.ExecutionTime = timestamppb.New(
		ms.executionState.StartTime.AsTime().Add(event.GetFirstWorkflowTaskBackoff().AsDuration()),
	)

	ms.executionInfo.Attempt = event.GetAttempt()
	if !timestamp.TimeValue(event.GetWorkflowExecutionExpirationTime()).IsZero() {
		ms.executionInfo.WorkflowExecutionExpirationTime = event.GetWorkflowExecutionExpirationTime()
	}

	var workflowRunTimeoutTime time.Time
	workflowRunTimeoutDuration := ms.executionInfo.WorkflowRunTimeout.AsDuration()
	// if workflowRunTimeoutDuration == 0 then the workflowRunTimeoutTime will be 0
	// meaning that there is not workflow run timeout
	if workflowRunTimeoutDuration != 0 {
		firstWorkflowTaskDelayDuration := event.GetFirstWorkflowTaskBackoff().AsDuration()
		workflowRunTimeoutDuration = workflowRunTimeoutDuration + firstWorkflowTaskDelayDuration
		workflowRunTimeoutTime = ms.executionState.StartTime.AsTime().Add(workflowRunTimeoutDuration)

		workflowExecutionTimeoutTime := timestamp.TimeValue(ms.executionInfo.WorkflowExecutionExpirationTime)
		if !workflowExecutionTimeoutTime.IsZero() && workflowRunTimeoutTime.After(workflowExecutionTimeoutTime) {
			workflowRunTimeoutTime = workflowExecutionTimeoutTime
		}
	}
	ms.executionInfo.WorkflowRunExpirationTime = timestamppb.New(workflowRunTimeoutTime)

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

	if event.GetVersioningOverride() != nil {
		ms.executionInfo.VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{VersioningOverride: event.GetVersioningOverride()}
	}
	if inheritedBuildId := event.InheritedBuildId; inheritedBuildId != "" {
		ms.executionInfo.InheritedBuildId = inheritedBuildId
		if err := ms.UpdateBuildIdAssignment(inheritedBuildId); err != nil {
			return err
		}
	} else if event.SourceVersionStamp.GetUseVersioning() && event.SourceVersionStamp.GetBuildId() != "" ||
		ms.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED {
		// TODO: [cleanup-old-wv]
		limit := ms.config.SearchAttributesSizeOfValueLimit(string(ms.namespaceEntry.Name()))
		if _, err := ms.addBuildIdToSearchAttributesWithNoVisibilityTask(event.SourceVersionStamp, limit); err != nil {
			return err
		}
	}

	if inheritedBuildId := event.InheritedBuildId; inheritedBuildId != "" {
		ms.executionInfo.InheritedBuildId = inheritedBuildId
		if err := ms.UpdateBuildIdAssignment(inheritedBuildId); err != nil {
			return err
		}
	}

	ms.executionInfo.MostRecentWorkerVersionStamp = event.SourceVersionStamp

	ms.approximateSize += ms.executionInfo.Size()

	ms.writeEventToCache(startEvent)
	return nil
}

func (ms *MutableStateImpl) addCompletionCallbacks(
	event *historypb.HistoryEvent,
	completionCallbaks []*commonpb.Callback,
) error {
	coll := callbacks.MachineCollection(ms.HSM())
	for idx, cb := range completionCallbaks {
		persistenceCB := &persistencespb.Callback{}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			persistenceCB.Variant = &persistencespb.Callback_Nexus_{
				Nexus: &persistencespb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		case *commonpb.Callback_Internal_:
			err := proto.Unmarshal(cb.GetInternal().GetData(), persistenceCB)
			if err != nil {
				return err
			}
		}
		machine := callbacks.NewCallback(event.EventTime, callbacks.NewWorkflowClosedTrigger(), persistenceCB)
		// Use the start event version and ID as part of the callback ID to ensure that callbacks have unique
		// IDs that are deterministically created across clusters.
		// TODO: Replicate the state machine state and allocate a uuid there instead of relying on history event
		// replication.
		id := fmt.Sprintf("%d-%d-%d", event.GetVersion(), event.GetEventId(), idx)
		if _, err := coll.Add(id, machine); err != nil {
			return err
		}
	}
	return nil
}

// AddFirstWorkflowTaskScheduled adds the first workflow task scheduled event unless it should be delayed as indicated
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
	originalScheduledTimestamp *timestamppb.Timestamp,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp, workflowTaskType)
}

func (ms *MutableStateImpl) ApplyTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ApplyTransientWorkflowTaskScheduled()
}

func (ms *MutableStateImpl) ApplyWorkflowTaskScheduledEvent(
	version int64,
	scheduledEventID int64,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *durationpb.Duration,
	attempt int32,
	scheduleTimestamp *timestamppb.Timestamp,
	originalScheduledTimestamp *timestamppb.Timestamp,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ApplyWorkflowTaskScheduledEvent(version, scheduledEventID, taskQueue, startToCloseTimeout, attempt, scheduleTimestamp, originalScheduledTimestamp, workflowTaskType)
}

func (ms *MutableStateImpl) AddWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	taskQueue *taskqueuepb.TaskQueue,
	identity string,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
	skipVersioningCheck bool,
) (*historypb.HistoryEvent, *WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskStartedEvent(scheduledEventID, requestID, taskQueue, identity, versioningStamp, redirectInfo, skipVersioningCheck)
}

func (ms *MutableStateImpl) ApplyWorkflowTaskStartedEvent(
	workflowTask *WorkflowTaskInfo,
	version int64,
	scheduledEventID int64,
	startedEventID int64,
	requestID string,
	timestamp time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
) (*WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ApplyWorkflowTaskStartedEvent(workflowTask, version, scheduledEventID,
		startedEventID, requestID, timestamp, suggestContinueAsNew, historySizeBytes, versioningStamp, redirectCounter)
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

func (ms *MutableStateImpl) updateBinaryChecksumSearchAttribute() error {
	exeInfo := ms.executionInfo
	resetPoints := exeInfo.AutoResetPoints.Points
	// List of all recent binary checksums associated with the workflow.
	recentBinaryChecksums := make([]string, 0, len(resetPoints))
	for _, rp := range resetPoints {
		if rp.BinaryChecksum != "" {
			recentBinaryChecksums = append(recentBinaryChecksums, rp.BinaryChecksum)
		}
	}
	checksumsPayload, err := searchattribute.EncodeValue(recentBinaryChecksums, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	if err != nil {
		return err
	}
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload, 1)
	}
	if proto.Equal(exeInfo.SearchAttributes[searchattribute.BinaryChecksums], checksumsPayload) {
		return nil // unchanged
	}
	ms.updateSearchAttributes(map[string]*commonpb.Payload{searchattribute.BinaryChecksums: checksumsPayload})
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
}

// Add a reset point for current task completion if needed.
// Returns true if the reset point was added or false if there was no need or no ability to add.
// Note that a new reset point is added when the pair <binaryChecksum, buildId> changes.
func (ms *MutableStateImpl) addResetPointFromCompletion(
	binaryChecksum string,
	buildId string,
	eventID int64,
	maxResetPoints int,
) bool {
	resetPoints := ms.executionInfo.AutoResetPoints.GetPoints()
	for _, rp := range resetPoints {
		if rp.GetBinaryChecksum() == binaryChecksum && rp.GetBuildId() == buildId {
			return false
		}
	}

	newPoint := &workflowpb.ResetPointInfo{
		BinaryChecksum:               binaryChecksum,
		BuildId:                      buildId,
		RunId:                        ms.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: eventID,
		CreateTime:                   timestamppb.New(ms.timeSource.Now()),
		Resettable:                   ms.CheckResettable() == nil,
	}
	ms.executionInfo.AutoResetPoints = &workflowpb.ResetPoints{
		Points: util.SliceTail(append(resetPoints, newPoint), maxResetPoints),
	}
	return true
}

// validateBuildIdRedirectInfo validates build ID for the task being dispatched and returned the redirect counter
// that should be used in the task started event.
// If the given versioning stamp and redirect info is not valid based on the WF's assigned build ID
// ObsoleteDispatchBuildId error will be returned.
func (ms *MutableStateImpl) validateBuildIdRedirectInfo(
	startedWorkerStamp *commonpb.WorkerVersionStamp,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
) (int64, error) {
	assignedBuildId := ms.GetAssignedBuildId()
	redirectCounter := ms.GetExecutionInfo().GetBuildIdRedirectCounter()

	if !startedWorkerStamp.GetUseVersioning() && assignedBuildId != "" && ms.HasCompletedAnyWorkflowTask() {
		// We don't allow moving from versioned to unversioned once the wf has completed the first WFT.
		// If this happens, it must be a stale task.
		return 0, serviceerrors.NewObsoleteDispatchBuildId("versioned workflow's task cannot be dispatched to unversioned workers")
	}

	if startedWorkerStamp.GetBuildId() == assignedBuildId {
		// dispatch build ID is the same as wf assigned build ID, hence noop.
		return redirectCounter, nil
	}

	if ms.HasCompletedAnyWorkflowTask() &&
		(redirectInfo == nil || redirectInfo.GetAssignedBuildId() != assignedBuildId) {
		// Workflow hs already completed tasks but no redirect or a redirect based on a wrong assigned build ID is
		// reported. This must be a task backlogged on an old build ID. rejecting this task, there should be another
		// task scheduled on the right build ID.
		return 0, serviceerrors.NewObsoleteDispatchBuildId("dispatch build ID is not the workflow's current build ID")
	}

	if assignedBuildId == "" && !ms.HasCompletedAnyWorkflowTask() {
		// If build ID is being set for the first time, and no progress is made by unversioned workers we don't
		// increment redirect counter. This is to keep the redirect counter zero for verisoned WFs that
		// do not experience any redirects, but only initial build ID assignment.
		return redirectCounter, nil
	}

	// Valid redirect is happening.
	return redirectCounter + 1, nil
}

// ApplyBuildIdRedirect applies possible redirect to mutable state based on versioning stamp of a starting task.
// If a redirect is applicable, assigned build ID of the wf will be updated and all scheduled but not
// started tasks will be rescheduled to be put on the matching queue of the right build ID.
func (ms *MutableStateImpl) ApplyBuildIdRedirect(
	startingTaskScheduledEventId int64,
	buildId string,
	redirectCounter int64,
) error {
	if ms.GetExecutionInfo().GetBuildIdRedirectCounter() >= redirectCounter {
		// Existing redirect counter is more than the given one, so ignore this redirect because
		// this or a more recent one has already been applied. This can happen when replaying
		// history because redirects can be applied at activity started time, but we don't record
		// the actual order of activity started events in history (they're transient tasks).
		return nil
	}
	err := ms.UpdateBuildIdAssignment(buildId)
	if err != nil {
		return err
	}

	// We need to set workflow's redirect counter to the given value. This is needed for events applied in standby
	// cluster and events applied by WF rebuilder.
	ms.GetExecutionInfo().BuildIdRedirectCounter = redirectCounter

	// Re-scheduling pending WF and activity tasks which are not started yet.
	if ms.HasPendingWorkflowTask() && !ms.HasStartedWorkflowTask() &&
		ms.GetPendingWorkflowTask().ScheduledEventID != startingTaskScheduledEventId &&
		// TODO: something special may need to be done for speculative tasks as GenerateScheduleWorkflowTaskTasks
		// does not support them.
		ms.GetPendingWorkflowTask().Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// sticky queue should be cleared by UpdateBuildIdAssignment already, so the following call only generates
		// a WorkflowTask, not a WorkflowTaskTimeoutTask.
		err = ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(ms.GetPendingWorkflowTask().ScheduledEventID)
		if err != nil {
			return err
		}
	}

	for _, ai := range ms.GetPendingActivityInfos() {
		if ai.ScheduledEventId == startingTaskScheduledEventId ||
			// activity already started
			ai.StartedEventId != common.EmptyEventID ||
			// activity does not depend on wf build ID
			ai.GetUseWorkflowBuildIdInfo() == nil {
			// TODO: skip task generation also when activity is in backoff period
			continue
		}
		// we only need to resend the activities to matching, no need to update timer tasks.
		err = ms.taskGenerator.GenerateActivityTasks(ai.ScheduledEventId)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateBuildIdAssignment based on initial assignment or a redirect
func (ms *MutableStateImpl) UpdateBuildIdAssignment(buildId string) error {
	if ms.GetAssignedBuildId() == buildId {
		return nil
	}
	ms.executionInfo.AssignedBuildId = buildId
	// because build ID is changed, we clear sticky queue so to make sure the next wf task does not go to old version.
	ms.ClearStickyTaskQueue()
	limit := ms.config.SearchAttributesSizeOfValueLimit(ms.namespaceEntry.Name().String())
	return ms.updateBuildIdsSearchAttribute(&commonpb.WorkerVersionStamp{UseVersioning: true, BuildId: buildId}, limit)
}

// For pinned workflows (ms.GetEffectiveVersioningBehavior() == PINNED), this will append a tag formed as
// pinned:<deployment_series_name>:<deployment_build_id> to the BuildIds search attribute,
// if it does not already exist there. The deployment will be execution_info.deployment, or the override deployment if
// a pinned override is set.
// For all other workflows (ms.GetEffectiveVersioningBehavior() != PINNED), this will append a tag based on the
// workflow's versioning status.
func (ms *MutableStateImpl) updateBuildIdsSearchAttribute(stamp *commonpb.WorkerVersionStamp, maxSearchAttributeValueSize int) error {
	changed, err := ms.addBuildIdToSearchAttributesWithNoVisibilityTask(stamp, maxSearchAttributeValueSize)
	if err != nil {
		return err
	}

	if !changed {
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
	if decoded == nil {
		return []string{}, nil
	}
	searchAttributeValues, ok := decoded.([]string)
	if !ok {
		return nil, serviceerror.NewInternal("invalid search attribute value stored for BuildIds")
	}
	return searchAttributeValues, nil
}

// getPinnedDeployment returns nil if the workflow is not pinned. If there is a pinned override,
// it returns the override deployment. If there is no override, it returns execution_info.deployment,
// which is the last deployment that this workflow completed a task on.
func (ms *MutableStateImpl) getPinnedDeployment() *deploymentpb.Deployment {
	if ms.GetEffectiveVersioningBehavior() != enumspb.VERSIONING_BEHAVIOR_PINNED {
		return nil
	}
	return ms.GetEffectiveDeployment()
}

// Takes a list of loaded build IDs from a search attribute and adds a new build ID to it. Also makes sure that the
// resulting SA list begins with either "unversioned" or "assigned:<bld>" based on workflow's Build ID assignment status.
// Returns a potentially modified list.
// [cleanup-old-wv] old versioning does not add "assigned:<bld>" value to the SA.
// [cleanup-versioning-2] versioning-2 adds "assigned:<bld>" which is no longer used in versioning-3
func (ms *MutableStateImpl) addBuildIdToLoadedSearchAttribute(
	existingValues []string,
	stamp *commonpb.WorkerVersionStamp,
) []string {
	var newValues []string
	effectiveBehavior := ms.GetEffectiveVersioningBehavior()
	if !stamp.GetUseVersioning() && effectiveBehavior == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED { // unversioned workflows may still have non-nil deployment, so we don't check deployment
		newValues = append(newValues, worker_versioning.UnversionedSearchAttribute)
	} else if effectiveBehavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
		newValues = append(newValues, worker_versioning.PinnedBuildIdSearchAttribute(ms.getPinnedDeployment()))
	} else if ms.GetAssignedBuildId() != "" {
		newValues = append(newValues, worker_versioning.AssignedBuildIdSearchAttribute(ms.GetAssignedBuildId()))
	}

	if effectiveBehavior == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		buildId := worker_versioning.VersionStampToBuildIdSearchAttribute(stamp)
		found := slices.Contains(newValues, buildId)
		for _, existingValue := range existingValues {
			if existingValue == buildId {
				found = true
			}
			if !worker_versioning.IsUnversionedOrAssignedBuildIdSearchAttribute(existingValue) {
				newValues = append(newValues, existingValue)
			}
		}
		if !found {
			newValues = append(newValues, buildId)
		}
	}

	// Remove pinned build id search attribute if it exists and we are not pinned
	if effectiveBehavior != enumspb.VERSIONING_BEHAVIOR_PINNED {
		newValues = slices.DeleteFunc(newValues, func(s string) bool {
			return strings.HasPrefix(s, worker_versioning.BuildIdSearchAttributePrefixPinned)
		})
	}
	return newValues
}

func (ms *MutableStateImpl) saveBuildIds(buildIds []string, maxSearchAttributeValueSize int) error {
	searchAttributes := ms.executionInfo.SearchAttributes
	if searchAttributes == nil {
		searchAttributes = make(map[string]*commonpb.Payload, 1)
		ms.executionInfo.SearchAttributes = searchAttributes
	}

	hasUnversionedOrAssigned := false
	if len(buildIds) > 0 { // len is 0 if we are removing the pinned search attribute and the workflow was never unversioned or assigned
		hasUnversionedOrAssigned = worker_versioning.IsUnversionedOrAssignedBuildIdSearchAttribute(buildIds[0])
	}
	for {
		saPayload, err := searchattribute.EncodeValue(buildIds, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
		if err != nil {
			return err
		}
		if len(buildIds) == 0 || len(saPayload.GetData()) <= maxSearchAttributeValueSize {
			ms.updateSearchAttributes(map[string]*commonpb.Payload{searchattribute.BuildIds: saPayload})
			break
		}
		if len(buildIds) == 1 {
			buildIds = make([]string, 0)
		} else if hasUnversionedOrAssigned {
			// Make sure to maintain the unversioned sentinel, it's required for the reachability API
			buildIds = append(buildIds[:1], buildIds[2:]...)
		} else {
			buildIds = buildIds[1:]
		}
	}
	return nil
}

func (ms *MutableStateImpl) addBuildIdToSearchAttributesWithNoVisibilityTask(stamp *commonpb.WorkerVersionStamp, maxSearchAttributeValueSize int) (bool, error) {
	existingBuildIds, err := ms.loadBuildIds()
	if err != nil {
		return false, err
	}
	modifiedBuildIds := ms.addBuildIdToLoadedSearchAttribute(existingBuildIds, stamp)
	if slices.Equal(existingBuildIds, modifiedBuildIds) {
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

func (ms *MutableStateImpl) ApplyWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	return ms.workflowTaskManager.ApplyWorkflowTaskCompletedEvent(event)
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

func (ms *MutableStateImpl) ApplyWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	return ms.workflowTaskManager.ApplyWorkflowTaskTimedOutEvent(timeoutType)
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
	versioningStamp *commonpb.WorkerVersionStamp,
	binChecksum, baseRunID, newRunID string,
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
		versioningStamp,
		binChecksum,
		baseRunID,
		newRunID,
		forkEventVersion,
	)
}

func (ms *MutableStateImpl) ApplyWorkflowTaskFailedEvent() error {
	return ms.workflowTaskManager.ApplyWorkflowTaskFailedEvent()
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
	ai, err := ms.ApplyActivityTaskScheduledEvent(workflowTaskCompletedEventID, event)
	// TODO merge active & passive task generation
	if !bypassTaskGeneration {
		if err := ms.taskGenerator.GenerateActivityTasks(
			event.GetEventId(),
		); err != nil {
			return nil, nil, err
		}
	}

	return event, ai, err
}

func (ms *MutableStateImpl) ApplyActivityTaskScheduledEvent(
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
		FirstScheduledTime:      event.GetEventTime(),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
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
		ActivityType:            attributes.GetActivityType(),
	}

	if attributes.UseWorkflowBuildId {
		if ms.GetAssignedBuildId() != "" {
			// only set when using new versioning
			ai.BuildIdInfo = &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo_{
				UseWorkflowBuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo{},
			}
		} else {
			// only set when using old versioning
			ai.UseCompatibleVersion = true
		}
	}

	if ai.HasRetryPolicy {
		ai.RetryInitialInterval = attributes.RetryPolicy.GetInitialInterval()
		ai.RetryBackoffCoefficient = attributes.RetryPolicy.GetBackoffCoefficient()
		ai.RetryMaximumInterval = attributes.RetryPolicy.GetMaximumInterval()
		ai.RetryMaximumAttempts = attributes.RetryPolicy.GetMaximumAttempts()
		ai.RetryNonRetryableErrorTypes = attributes.RetryPolicy.NonRetryableErrorTypes
		if scheduleToCloseTimeout.AsDuration() > 0 {
			ai.RetryExpirationTime = timestamppb.New(
				ai.ScheduledTime.AsTime().Add(scheduleToCloseTimeout.AsDuration()),
			)
		} else {
			ai.RetryExpirationTime = nil
		}
	}

	ms.addPendingActivityInfo(ai)
	ms.writeEventToCache(event)
	return ai, nil
}

func (ms *MutableStateImpl) addPendingActivityInfo(ai *persistencespb.ActivityInfo) {
	ms.pendingActivityInfoIDs[ai.ScheduledEventId] = ai
	ms.pendingActivityIDToEventID[ai.ActivityId] = ai.ScheduledEventId
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}
	ms.approximateSize += ai.Size() + int64SizeBytes
	ms.executionInfo.ActivityCount++
}

func (ms *MutableStateImpl) addStartedEventForTransientActivity(
	scheduledEventID int64,
	versioningStamp *commonpb.WorkerVersionStamp,
) error {
	ai, ok := ms.GetActivityInfo(scheduledEventID)
	if !ok || ai.StartedEventId != common.TransientEventID {
		return nil
	}

	if versioningStamp == nil {
		// We want to add version stamp to the started event being added now with delay. The task may be now completed,
		// failed, cancelled, or timed out. For some cases such as timeout, cancellation, and failed by Resetter we do
		// not receive a stamp because worker is not involved, therefore, we reconstruct the stamp base on pending
		// activity data, if versioning is used.
		// Find the build ID of the worker to whom the task was dispatched base on whether it was an independently-
		// assigned activity or not.
		startedBuildId := ai.GetLastIndependentlyAssignedBuildId()
		if useWf := ai.GetUseWorkflowBuildIdInfo(); useWf != nil {
			startedBuildId = useWf.GetLastUsedBuildId()
		}
		if startedBuildId != "" {
			// If a build ID is found, i.e. versioning is used for the activity, set versioning stamp.
			versioningStamp = &commonpb.WorkerVersionStamp{UseVersioning: true, BuildId: startedBuildId}
		}
	}

	// activity task was started (as transient event), we need to add it now.
	event := ms.hBuilder.AddActivityTaskStartedEvent(
		scheduledEventID,
		ai.Attempt,
		ai.RequestId,
		ai.StartedIdentity,
		ai.RetryLastFailure,
		versioningStamp,
		ai.GetUseWorkflowBuildIdInfo().GetLastRedirectCounter(),
	)
	if ai.StartedTime != nil {
		// overwrite started event time to the one recorded in ActivityInfo
		event.EventTime = ai.StartedTime
	}
	return ms.ApplyActivityTaskStartedEvent(event)
}

func (ms *MutableStateImpl) AddActivityTaskStartedEvent(
	ai *persistencespb.ActivityInfo,
	scheduledEventID int64,
	requestID string,
	identity string,
	versioningStamp *commonpb.WorkerVersionStamp,
	deployment *deploymentpb.Deployment,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionActivityTaskStarted
	err := ms.checkMutability(opTag)
	if err != nil {
		return nil, err
	}

	var redirectCounter int64
	buildId := worker_versioning.BuildIdIfUsingVersioning(versioningStamp)
	if buildId != "" {
		// note that if versioningStamp.BuildId is present we know it's not an old versioning worker because matching
		// does not pass build ID for old versioning workers to Record*TaskStart.
		// TODO: cleanup this comment [cleanup-old-wv]
		if useWf := ai.GetUseWorkflowBuildIdInfo(); useWf != nil {
			// When a dependent activity is redirected, we'll update workflow's assigned build ID as well. Therefore,
			// need to validate redirect info and possibly increment WF's redirect counter.
			redirectCounter, err = ms.validateBuildIdRedirectInfo(versioningStamp, redirectInfo)
			if err != nil {
				return nil, err
			}
		}
	}

	ai.LastStartedDeployment = deployment

	if !ai.HasRetryPolicy {
		event := ms.hBuilder.AddActivityTaskStartedEvent(
			scheduledEventID,
			ai.Attempt,
			requestID,
			identity,
			ai.RetryLastFailure,
			versioningStamp,
			redirectCounter,
		)
		if err := ms.ApplyActivityTaskStartedEvent(event); err != nil {
			return nil, err
		}
		return event, nil
	}

	// This is a transient start so no events is being created for it. But we still need to process possible build
	// ID redirect.
	if err := ms.applyActivityBuildIdRedirect(ai, buildId, redirectCounter); err != nil {
		return nil, err
	}

	if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ MutableState) error {
		// we might need to retry, so do not append started event just yet,
		// instead update mutable state and will record started event when activity task is closed
		activityInfo.Version = ms.GetCurrentVersion()
		activityInfo.StartedEventId = common.TransientEventID
		activityInfo.RequestId = requestID
		activityInfo.StartedTime = timestamppb.New(ms.timeSource.Now())
		activityInfo.StartedIdentity = identity
		return nil
	}); err != nil {
		return nil, err
	}
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
	return nil, nil
}

func (ms *MutableStateImpl) ApplyActivityTaskStartedEvent(
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
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}
	ms.approximateSize += ai.Size()

	err := ms.applyActivityBuildIdRedirect(ai, worker_versioning.BuildIdIfUsingVersioning(attributes.GetWorkerVersion()), attributes.GetBuildIdRedirectCounter())
	return err
}

func (ms *MutableStateImpl) applyActivityBuildIdRedirect(activityInfo *persistencespb.ActivityInfo, buildId string, redirectCounter int64) error {
	if buildId == "" {
		return nil // not versioned
	}

	if useWf := activityInfo.GetUseWorkflowBuildIdInfo(); useWf != nil {
		// when a dependent activity is redirected, we update workflow's assigned build ID as well
		err := ms.ApplyBuildIdRedirect(activityInfo.ScheduledEventId, buildId, redirectCounter)
		if err != nil {
			return err
		}
		useWf.LastRedirectCounter = redirectCounter
		useWf.LastUsedBuildId = buildId
	} else {
		// This activity is not attached to the wf build ID so we store its build ID in activity info.
		activityInfo.BuildIdInfo = &persistencespb.ActivityInfo_LastIndependentlyAssignedBuildId{LastIndependentlyAssignedBuildId: buildId}
	}
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

	if err := ms.addStartedEventForTransientActivity(scheduledEventID, request.WorkerVersion); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		request.Identity,
		request.Result,
	)
	if err := ms.ApplyActivityTaskCompletedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ApplyActivityTaskCompletedEvent(
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
	versioningStamp *commonpb.WorkerVersionStamp,
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

	if err := ms.addStartedEventForTransientActivity(scheduledEventID, versioningStamp); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		failure,
		retryState,
		identity,
	)
	if err := ms.ApplyActivityTaskFailedEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ApplyActivityTaskFailedEvent(
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

	if err := ms.addStartedEventForTransientActivity(scheduledEventID, nil); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		timeoutFailure,
		retryState,
	)
	if err := ms.ApplyActivityTaskTimedOutEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ApplyActivityTaskTimedOutEvent(
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

	if err := ms.ApplyActivityTaskCancelRequestedEvent(actCancelReqEvent); err != nil {
		return nil, nil, err
	}

	return actCancelReqEvent, ai, nil
}

func (ms *MutableStateImpl) ApplyActivityTaskCancelRequestedEvent(
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
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}
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

	if err := ms.addStartedEventForTransientActivity(scheduledEventID, nil); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddActivityTaskCanceledEvent(
		scheduledEventID,
		startedEventID,
		latestCancelRequestedEventID,
		details,
		identity,
	)
	if err := ms.ApplyActivityTaskCanceledEvent(event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ms *MutableStateImpl) ApplyActivityTaskCanceledEvent(
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
	if err := ms.ApplyWorkflowExecutionCompletedEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event.GetEventTime().AsTime(),
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionCompletedEvent(
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
	return ms.processCloseCallbacks()
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
	if err := ms.ApplyWorkflowExecutionFailedEvent(batchID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event.GetEventTime().AsTime(),
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionFailedEvent(
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
	return ms.processCloseCallbacks()
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
	if err := ms.ApplyWorkflowExecutionTimedoutEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event.GetEventTime().AsTime(),
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionTimedoutEvent(
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
	return ms.processCloseCallbacks()
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
	if err := ms.ApplyWorkflowExecutionCancelRequestedEvent(event); err != nil {
		return nil, err
	}

	// Set the CancelRequestID on the active cluster.  This information is not part of the history event.
	ms.executionInfo.CancelRequestId = request.CancelRequest.GetRequestId()
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionCancelRequestedEvent(
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
	if err := ms.ApplyWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event.GetEventTime().AsTime(),
		false,
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionCanceledEvent(
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
	return ms.processCloseCallbacks()
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
	rci, err := ms.ApplyRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, cancelRequestID)
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

func (ms *MutableStateImpl) ApplyRequestCancelExternalWorkflowExecutionInitiatedEvent(
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
	if err := ms.ApplyExternalWorkflowExecutionCancelRequested(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyExternalWorkflowExecutionCancelRequested(
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
	if err := ms.ApplyRequestCancelExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyRequestCancelExternalWorkflowExecutionFailedEvent(
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
	si, err := ms.ApplySignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, signalRequestID)
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

func (ms *MutableStateImpl) ApplySignalExternalWorkflowExecutionInitiatedEvent(
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
	ms.ApplyUpsertWorkflowSearchAttributesEvent(event)
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateUpsertVisibilityTask(); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyUpsertWorkflowSearchAttributesEvent(
	event *historypb.HistoryEvent,
) {
	upsertSearchAttr := event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields()
	ms.approximateSize -= ms.executionInfo.Size()
	ms.updateSearchAttributes(upsertSearchAttr)
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
	ms.ApplyWorkflowPropertiesModifiedEvent(event)
	// TODO merge active & passive task generation
	// TODO: only generate visibility task when memo is updated
	if err := ms.taskGenerator.GenerateUpsertVisibilityTask(); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowPropertiesModifiedEvent(
	event *historypb.HistoryEvent,
) {
	attr := event.GetWorkflowPropertiesModifiedEventAttributes()
	if attr.UpsertedMemo != nil {
		upsertMemo := attr.GetUpsertedMemo().GetFields()
		ms.approximateSize -= ms.executionInfo.Size()
		ms.updateMemo(upsertMemo)
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
	if err := ms.ApplyExternalWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyExternalWorkflowExecutionSignaled(
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
	if err := ms.ApplySignalExternalWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplySignalExternalWorkflowExecutionFailedEvent(
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
	ti, err := ms.ApplyTimerStartedEvent(event)
	if err != nil {
		return nil, nil, err
	}
	return event, ti, err
}

func (ms *MutableStateImpl) ApplyTimerStartedEvent(
	event *historypb.HistoryEvent,
) (*persistencespb.TimerInfo, error) {
	attributes := event.GetTimerStartedEventAttributes()
	timerID := attributes.GetTimerId()

	startToFireTimeout := attributes.GetStartToFireTimeout().AsDuration()
	// TODO: Time skew needs to be taken in to account.
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(startToFireTimeout)

	ti := &persistencespb.TimerInfo{
		Version:        event.GetVersion(),
		TimerId:        timerID,
		ExpiryTime:     timestamppb.New(expiryTime),
		StartedEventId: event.GetEventId(),
		TaskStatus:     TimerTaskStatusNone,
	}

	ms.pendingTimerInfoIDs[ti.TimerId] = ti
	ms.pendingTimerEventIDToID[ti.StartedEventId] = ti.TimerId
	ms.updateTimerInfos[ti.TimerId] = ti
	ms.timerInfosUserDataUpdated[ti.TimerId] = struct{}{}
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
	if err := ms.ApplyTimerFiredEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyTimerFiredEvent(
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
		if err := ms.ApplyTimerCanceledEvent(event); err != nil {
			return nil, err
		}
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyTimerCanceledEvent(
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
	links []*commonpb.Link,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTerminated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddWorkflowExecutionTerminatedEvent(reason, details, identity, links)
	if err := ms.ApplyWorkflowExecutionTerminatedEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event.GetEventTime().AsTime(),
		deleteAfterTerminate,
	); err != nil {
		return nil, err
	}
	return event, nil
}

// AddWorkflowExecutionUpdateAdmittedEvent adds a WorkflowExecutionUpdateAdmittedEvent to in-memory history.
func (ms *MutableStateImpl) AddWorkflowExecutionUpdateAdmittedEvent(request *updatepb.Request, origin enumspb.UpdateAdmittedEventOrigin) (*historypb.HistoryEvent, error) {
	if err := ms.checkMutability(tag.WorkflowActionUpdateAdmitted); err != nil {
		return nil, err
	}
	event, batchId := ms.hBuilder.AddWorkflowExecutionUpdateAdmittedEvent(request, origin)
	if err := ms.ApplyWorkflowExecutionUpdateAdmittedEvent(event, batchId); err != nil {
		return nil, err
	}
	return event, nil
}

// ApplyWorkflowExecutionUpdateAdmittedEvent applies a WorkflowExecutionUpdateAdmittedEvent to mutable state.
func (ms *MutableStateImpl) ApplyWorkflowExecutionUpdateAdmittedEvent(event *historypb.HistoryEvent, batchId int64) error {
	attrs := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
	if attrs == nil {
		return serviceerror.NewInternal("wrong event type in call to ApplyWorkflowExecutionUpdateAdmittedEvent")
	}
	if ms.executionInfo.UpdateInfos == nil {
		ms.executionInfo.UpdateInfos = make(map[string]*persistencespb.UpdateInfo, 1)
	}
	updateID := attrs.GetRequest().GetMeta().GetUpdateId()
	admission := &persistencespb.UpdateInfo_Admission{
		Admission: &persistencespb.UpdateAdmissionInfo{
			Location: &persistencespb.UpdateAdmissionInfo_HistoryPointer_{
				HistoryPointer: &persistencespb.UpdateAdmissionInfo_HistoryPointer{
					EventId:      event.EventId,
					EventBatchId: batchId,
				},
			},
		},
	}
	if _, ok := ms.executionInfo.UpdateInfos[updateID]; ok {
		return serviceerror.NewInternal(fmt.Sprintf("Update ID %s is already present in mutable state", updateID))
	}
	ui := persistencespb.UpdateInfo{Value: admission}
	ms.executionInfo.UpdateInfos[updateID] = &ui
	ms.executionInfo.UpdateCount++
	sizeDelta := ui.Size() + len(updateID)
	ms.approximateSize += sizeDelta
	ms.updateInfoUpdated[updateID] = struct{}{}
	ms.writeEventToCache(event)
	return nil
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
	if err := ms.ApplyWorkflowExecutionUpdateAcceptedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionUpdateAcceptedEvent(
	event *historypb.HistoryEvent,
) error {
	attrs := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
	if attrs == nil {
		return serviceerror.NewInternal("wrong event type in call to ApplyWorkflowExecutionUpdateAcceptedEvent")
	}
	if ms.executionInfo.UpdateInfos == nil {
		ms.executionInfo.UpdateInfos = make(map[string]*persistencespb.UpdateInfo, 1)
	}
	// NOTE: `attrs.GetAcceptedRequest().GetMeta().GetUpdateId()` was used here before, but that is problematic
	// in a reset/conflict resolution scenario where there is no `acceptedRequest` since the previously written
	// UpdateAdmitted event already contains the Update payload.
	updateID := attrs.GetProtocolInstanceId()
	var sizeDelta int
	if ui, ok := ms.executionInfo.UpdateInfos[updateID]; ok {
		sizeBefore := ui.Size()
		ui.Value = &persistencespb.UpdateInfo_Acceptance{
			Acceptance: &persistencespb.UpdateAcceptanceInfo{EventId: event.EventId},
		}
		sizeDelta = ui.Size() - sizeBefore
	} else {
		ui := persistencespb.UpdateInfo{
			Value: &persistencespb.UpdateInfo_Acceptance{
				Acceptance: &persistencespb.UpdateAcceptanceInfo{EventId: event.EventId},
			},
		}
		ms.executionInfo.UpdateInfos[updateID] = &ui
		ms.executionInfo.UpdateCount++
		sizeDelta = ui.Size() + len(updateID)
	}
	ms.approximateSize += sizeDelta
	ms.updateInfoUpdated[updateID] = struct{}{}
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
	if err := ms.ApplyWorkflowExecutionUpdateCompletedEvent(event, batchID); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionUpdateCompletedEvent(
	event *historypb.HistoryEvent,
	batchID int64,
) error {
	attrs := event.GetWorkflowExecutionUpdateCompletedEventAttributes()
	if attrs == nil {
		return serviceerror.NewInternal("wrong event type in call to ApplyWorkflowExecutionUpdateCompletedEvent")
	}
	if ms.executionInfo.UpdateInfos == nil {
		// UpdateInfo must be created by preceding UpdateAccepted event.
		return serviceerror.NewInvalidArgument("WorkflowExecutionUpdateCompletedEvent doesn't have preceding WorkflowExecutionUpdateAcceptedEvent")
	}
	updateID := attrs.GetMeta().GetUpdateId()
	var sizeDelta int
	ui, uiExists := ms.executionInfo.UpdateInfos[updateID]
	if !uiExists {
		// UpdateInfo must be created by preceding UpdateAccepted event.
		return serviceerror.NewInvalidArgument("WorkflowExecutionUpdateCompletedEvent doesn't have preceding WorkflowExecutionUpdateAcceptedEvent")
	}
	sizeBefore := ui.Size()
	ui.Value = &persistencespb.UpdateInfo_Completion{
		Completion: &persistencespb.UpdateCompletionInfo{
			EventId:      event.EventId,
			EventBatchId: batchID,
		},
	}
	sizeDelta = ui.Size() - sizeBefore
	ms.approximateSize += sizeDelta
	ms.updateInfoUpdated[updateID] = struct{}{}
	ms.writeEventToCache(event)
	return nil
}

func (ms *MutableStateImpl) RejectWorkflowExecutionUpdate(_ string, _ *updatepb.Rejection) error {
	// TODO (alex-update): This method is noop because we don't currently write rejections to the history.
	return nil
}

func (ms *MutableStateImpl) AddWorkflowExecutionOptionsUpdatedEvent(
	versioningOverride *workflowpb.VersioningOverride,
	unsetVersioningOverride bool,
	attachRequestID string,
	attachCompletionCallbacks []*commonpb.Callback,
	links []*commonpb.Link,
) (*historypb.HistoryEvent, error) {
	if err := ms.checkMutability(tag.WorkflowActionWorkflowOptionsUpdated); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddWorkflowExecutionOptionsUpdatedEvent(
		versioningOverride,
		unsetVersioningOverride,
		attachRequestID,
		attachCompletionCallbacks,
		links,
	)
	if err := ms.ApplyWorkflowExecutionOptionsUpdatedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionOptionsUpdatedEvent(event *historypb.HistoryEvent) error {
	attributes := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()
	var err error
	if attributes.GetUnsetVersioningOverride() {
		err = ms.updateVersioningOverride(nil)
	} else if attributes.GetVersioningOverride() != nil {
		err = ms.updateVersioningOverride(attributes.GetVersioningOverride())
	}
	if err != nil {
		return err
	}
	if attributes.GetAttachedRequestId() != "" {
		ms.attachRequestID(attributes.GetAttachedRequestId())
	}
	if len(attributes.GetAttachedCompletionCallbacks()) > 0 {
		if err := ms.addCompletionCallbacks(event, attributes.GetAttachedCompletionCallbacks()); err != nil {
			return err
		}
	}
	return nil
}

func (ms *MutableStateImpl) updateVersioningOverride(
	versioningOverride *workflowpb.VersioningOverride,
) error {
	previousEffectiveDeployment := ms.GetEffectiveDeployment()
	previousEffectiveVersioningBehavior := ms.GetEffectiveVersioningBehavior()
	if ms.GetExecutionInfo().GetVersioningInfo() == nil {
		ms.GetExecutionInfo().VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
	}
	ms.GetExecutionInfo().VersioningInfo.VersioningOverride = versioningOverride

	if !proto.Equal(ms.GetEffectiveDeployment(), previousEffectiveDeployment) ||
		ms.GetEffectiveVersioningBehavior() != previousEffectiveVersioningBehavior {
		// TODO (carly) part 2: if safe mode, do replay test on new deployment if deployment changed, if fail, revert changes and abort
		// If there is an ongoing transition, we remove it so that tasks from this workflow (including the pending WFT
		// that initiated the transition) can run on our override deployment as soon as possible.
		//
		// We only have to think about the case where the workflow is unpinned, since if the workflow is pinned, no
		// transition will start.
		//
		// If we did NOT remove the transition, we would have to keep the pending WFT scheduled per the transition's
		// deployment, so that when the task is started it can run on the transition's target deployment, complete,
		// and thereby complete the transition. If there is anything wrong with the transition's target deployment,
		// the transition could hang due to the task being stuck, or the transition could fail if the WFT fails.
		// Basically, WF might be stuck in a transition loop, and user wants to pin it to the previous build to move
		// it out of the loop. If we don't remove the transition, it will still be stuck.
		//
		// It is possible for there to be an ongoing transition and an override that both result in the same effective
		// behavior and effective deployment. In that case, we would not hit the code path to remove the transition or
		// reschedule the WFT. For this to happen, the existing behavior and the override would both have to be unpinned.
		// If we don't remove the transition or reschedule pending tasks, the outstanding WFT on the transition's
		// target queue will be started on the transition's target deployment. Most likely this matches user's intention
		// because they add the unpinned override, they want to workflow to do the transition. Even if we removed the
		// transition, the rescheduled task will be redirected by Matching to the old transition's deployment again,
		// and it will start the same transition in the workflow. So removing the transition would not make a difference
		// and would in fact add some extra work for the server.
		ms.executionInfo.GetVersioningInfo().DeploymentTransition = nil

		// If effective deployment or behavior change, we need to reschedule any pending tasks, because History will reject
		// the task's start request if the task is being started by a poller that is not from the workflow's effective
		// deployment according to History. Therefore, it is important for matching to match tasks with the correct pollers.
		// Even if the effective deployment does not change, we still need to reschedule tasks into the appropriate
		// default/unpinned queue or the pinned queue, because the two queues will be handled differently if the task queue's
		// Current Deployment changes between now and when the task is started.
		//
		// We choose to let any started WFT that is running on the old deployment finish running, instead of forcing it to fail.
		ms.ClearStickyTaskQueue()
		if ms.HasPendingWorkflowTask() && !ms.HasStartedWorkflowTask() &&
			// Speculative WFT is directly (without transfer task) added to matching when scheduled.
			// It is protected by timeout on both normal and sticky task queues.
			// If there is no poller for previous deployment, it will time out,
			// and will be rescheduled as normal WFT.
			ms.GetPendingWorkflowTask().Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
			// Sticky queue was just cleared, so the following call only generates a WorkflowTask, not a WorkflowTaskTimeoutTask.
			err := ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(ms.GetPendingWorkflowTask().ScheduledEventID)
			if err != nil {
				return err
			}
		}
		// For v3 versioned workflows (ms.GetEffectiveVersioningBehavior() != UNSPECIFIED), this will update the reachability
		// search attribute based on the execution_info.deployment and/or override deployment if one exists.
		limit := ms.config.SearchAttributesSizeOfValueLimit(ms.namespaceEntry.Name().String())
		if err := ms.updateBuildIdsSearchAttribute(nil, limit); err != nil {
			return err
		}
	}
	return ms.reschedulePendingActivities()
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionTerminatedEvent(
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

	return ms.processCloseCallbacks()
}

func (ms *MutableStateImpl) AddWorkflowExecutionSignaled(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	links []*commonpb.Link,
) (*historypb.HistoryEvent, error) {
	return ms.AddWorkflowExecutionSignaledEvent(
		signalName,
		input,
		identity,
		header,
		nil,
		links,
	)
}

func (ms *MutableStateImpl) AddWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	externalWorkflowExecution *commonpb.WorkflowExecution,
	links []*commonpb.Link,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowSignaled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}

	event := ms.hBuilder.AddWorkflowExecutionSignaledEvent(
		signalName,
		input,
		identity,
		header,
		externalWorkflowExecution,
		links,
	)
	if err := ms.ApplyWorkflowExecutionSignaled(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionSignaled(
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
	var rootInfo *workflowspb.RootExecutionInfo
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
		rootInfo = &workflowspb.RootExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: ms.executionInfo.RootWorkflowId,
				RunId:      ms.executionInfo.RootRunId,
			},
		}
	}

	continueAsNewEvent := ms.hBuilder.AddContinuedAsNewEvent(
		workflowTaskCompletedEventID,
		newRunID,
		command,
	)

	newMutableState, err := NewMutableStateInChain(
		ms.shard,
		ms.shard.GetEventsCache(),
		ms.logger,
		ms.namespaceEntry,
		ms.executionInfo.WorkflowId,
		newRunID,
		timestamp.TimeValue(continueAsNewEvent.GetEventTime()),
		ms,
	)
	if err != nil {
		return nil, nil, err
	}

	firstRunID, err := ms.GetFirstRunID(ctx)
	if err != nil {
		return nil, nil, err
	}

	if _, err = newMutableState.addWorkflowExecutionStartedEventForContinueAsNew(
		parentInfo,
		&newExecution,
		ms,
		command,
		firstRunID,
		rootInfo,
	); err != nil {
		return nil, nil, err
	}

	if err = newMutableState.SetHistoryTree(
		newMutableState.executionInfo.WorkflowExecutionTimeout,
		newMutableState.executionInfo.WorkflowRunTimeout,
		newRunID,
	); err != nil {
		return nil, nil, err
	}

	if err = ms.ApplyWorkflowExecutionContinuedAsNewEvent(
		firstEventID,
		continueAsNewEvent,
	); err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		continueAsNewEvent.GetEventTime().AsTime(),
		false,
	); err != nil {
		return nil, nil, err
	}

	return continueAsNewEvent, newMutableState, nil
}

func rolloverAutoResetPointsWithExpiringTime(
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	newExecutionStartTime time.Time,
	namespaceRetention time.Duration,
) *workflowpb.ResetPoints {
	if resetPoints.GetPoints() == nil {
		return resetPoints
	}
	newPoints := make([]*workflowpb.ResetPointInfo, 0, len(resetPoints.Points))
	// For continue-as-new, new execution start time is the same as previous execution close time,
	// so reset points from the previous run will expire at new execution start time plus retention.
	expireTime := newExecutionStartTime.Add(namespaceRetention)
	for _, rp := range resetPoints.Points {
		if rp.ExpireTime != nil && rp.ExpireTime.AsTime().Before(newExecutionStartTime) {
			continue // run is expired, don't preserve it
		}
		if rp.GetRunId() == prevRunID {
			rp.ExpireTime = timestamppb.New(expireTime)
		}
		newPoints = append(newPoints, rp)
	}
	return &workflowpb.ResetPoints{Points: newPoints}
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionContinuedAsNewEvent(
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
	ci, err := ms.ApplyStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event, createRequestID)
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

func (ms *MutableStateImpl) ApplyStartChildWorkflowExecutionInitiatedEvent(
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
	if err := ms.ApplyChildWorkflowExecutionStartedEvent(event, clock); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyChildWorkflowExecutionStartedEvent(
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
	if err := ms.ApplyStartChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyStartChildWorkflowExecutionFailedEvent(
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
	if err := ms.ApplyChildWorkflowExecutionCompletedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyChildWorkflowExecutionCompletedEvent(
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
	if err := ms.ApplyChildWorkflowExecutionFailedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyChildWorkflowExecutionFailedEvent(
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
	if err := ms.ApplyChildWorkflowExecutionCanceledEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyChildWorkflowExecutionCanceledEvent(
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
	if err := ms.ApplyChildWorkflowExecutionTerminatedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyChildWorkflowExecutionTerminatedEvent(
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
	if err := ms.ApplyChildWorkflowExecutionTimedOutEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyChildWorkflowExecutionTimedOutEvent(
	event *historypb.HistoryEvent,
) error {
	attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
	initiatedID := attributes.GetInitiatedEventId()

	return ms.DeletePendingChildExecution(initiatedID)
}

func (ms *MutableStateImpl) RetryActivity(
	ai *persistencespb.ActivityInfo,
	activityFailure *failurepb.Failure,
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

	if !isRetryable(activityFailure, ai.RetryNonRetryableErrorTypes) {
		return enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, nil
	}

	// if activity is paused
	if ai.Paused {
		// need to update activity
		if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ MutableState) error {
			activityInfo.StartedEventId = common.EmptyEventID
			activityInfo.StartedTime = nil
			activityInfo.RequestId = ""
			return nil
		}); err != nil {
			return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
		}

		// TODO: uncomment once RETRY_STATE_PAUSED is supported
		// return enumspb.RETRY_STATE_PAUSED, nil
		return enumspb.RETRY_STATE_IN_PROGRESS, nil
	}

	retryMaxInterval := ai.RetryMaximumInterval
	// if a delay is specified by the application it should override the maximum interval set by the retry policy.
	delay := nextRetryDelayFrom(activityFailure)
	if delay != nil {
		retryMaxInterval = durationpb.New(*delay)
	}

	now := ms.timeSource.Now().In(time.UTC)
	retryBackoff, retryState := nextBackoffInterval(
		ms.timeSource.Now().In(time.UTC),
		ai.Attempt,
		ai.RetryMaximumAttempts,
		ai.RetryInitialInterval,
		retryMaxInterval,
		ai.RetryExpirationTime,
		ai.RetryBackoffCoefficient,
		makeBackoffAlgorithm(delay),
	)
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return retryState, nil
	}

	ms.updateActivityInfoForRetries(ai,
		now.Add(retryBackoff),
		ai.Attempt+1,
		activityFailure)

	if err := ms.taskGenerator.GenerateActivityRetryTasks(ai); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}
	return enumspb.RETRY_STATE_IN_PROGRESS, nil
}

func (ms *MutableStateImpl) RecordLastActivityCompleteTime(ai *persistencespb.ActivityInfo) {
	_ = ms.UpdateActivity(ai.ScheduledEventId, func(info *persistencespb.ActivityInfo, _ MutableState) error {
		ai.LastAttemptCompleteTime = timestamppb.New(ms.shard.GetTimeSource().Now().UTC())
		return nil
	})
}

func (ms *MutableStateImpl) RegenerateActivityRetryTask(ai *persistencespb.ActivityInfo, nextScheduledTime time.Time) error {

	if nextScheduledTime.IsZero() {
		nextScheduledTime = GetNextScheduledTime(ai)
	}

	ms.updateActivityInfoForRetries(ai,
		nextScheduledTime,
		ai.Attempt,
		nil)

	return ms.taskGenerator.GenerateActivityRetryTasks(ai)
}

func (ms *MutableStateImpl) updateActivityInfoForRetries(
	ai *persistencespb.ActivityInfo,
	nextScheduledTime time.Time,
	nextAttempt int32,
	activityFailure *failurepb.Failure,
) {
	_ = ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, mutableState MutableState) error {
		mutableStateImpl, ok := mutableState.(*MutableStateImpl)
		if ok {
			ai = UpdateActivityInfoForRetries(
				activityInfo,
				mutableStateImpl.GetCurrentVersion(),
				nextAttempt,
				mutableStateImpl.truncateRetryableActivityFailure(activityFailure),
				timestamppb.New(nextScheduledTime),
			)
		}
		return nil
	})
}

/*
UpdateActivity function updates the existing pending activity via the updater callback.
To update an activity, we need to do the following steps:
	* preserve the original size of the activity
	* preserve the original state of the activity
	* call the updater callback to update the activity
	* calculate new size of the activity
	* respond to the changes of the activity state (like changes to pause)
*/

func (ms *MutableStateImpl) UpdateActivity(scheduledEventId int64, updater ActivityUpdater) error {
	ai, activityFound := ms.GetActivityInfo(scheduledEventId)
	if !activityFound {
		return consts.ErrActivityNotFound
	}

	prevPause := ai.Paused
	var originalSize int
	if prev, ok := ms.pendingActivityInfoIDs[ai.ScheduledEventId]; ok {
		originalSize = prev.Size()
		prevPause = prev.Paused
	}

	if err := updater(ai, ms); err != nil {
		return err
	}

	if prevPause != ai.Paused {
		err := ms.updatePauseInfoSearchAttribute()
		if err != nil {
			return err
		}
	}

	ms.approximateSize += ai.Size() - originalSize
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}

	return nil
}

func (ms *MutableStateImpl) updatePauseInfoSearchAttribute() error {
	pausedInfoMap := make(map[string]struct{})

	for _, ai := range ms.GetPendingActivityInfos() {
		if !ai.Paused {
			continue
		}
		pausedInfoMap[ai.ActivityType.Name] = struct{}{}
	}
	pausedInfo := make([]string, 0, len(pausedInfoMap))
	for activityType := range pausedInfoMap {
		pausedInfo = append(pausedInfo, fmt.Sprintf("property:activityType=%s", activityType))
	}

	pauseInfoPayload, err := searchattribute.EncodeValue(pausedInfo, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	if err != nil {
		return err
	}

	exeInfo := ms.executionInfo
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload, 1)
	}

	if proto.Equal(exeInfo.SearchAttributes[searchattribute.TemporalPauseInfo], pauseInfoPayload) {
		return nil // unchanged
	}

	ms.updateSearchAttributes(map[string]*commonpb.Payload{searchattribute.TemporalPauseInfo: pauseInfoPayload})
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
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

// processCloseCallbacks triggers "WorkflowClosed" callbacks, applying the state machine transition that schedules
// callback tasks.
func (ms *MutableStateImpl) processCloseCallbacks() error {
	continuedAsNew := ms.GetExecutionInfo().NewExecutionRunId != ""
	if continuedAsNew || ms.GetExecutionInfo().GetWorkflowWasReset() {
		return nil
	}

	coll := callbacks.MachineCollection(ms.HSM())
	for _, node := range coll.List() {
		cb, err := coll.Data(node.Key.ID)
		if err != nil {
			return err
		}
		// Only try to trigger "WorkflowClosed" callbacks.
		if _, ok := cb.Trigger.Variant.(*persistencespb.CallbackInfo_Trigger_WorkflowClosed); !ok {
			continue
		}
		err = coll.Transition(node.Key.ID, func(cb callbacks.Callback) (hsm.TransitionOutput, error) {
			return callbacks.TransitionScheduled.Apply(cb, callbacks.EventScheduled{})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO mutable state should generate corresponding transfer / timer tasks according to
//  updates accumulated, while currently all transfer / timer tasks are managed manually

func (ms *MutableStateImpl) AddTasks(
	newTasks ...tasks.Task,
) {
	now := ms.timeSource.Now()
	for _, task := range newTasks {
		category := task.GetCategory()
		if category.Type() == tasks.CategoryTypeScheduled &&
			task.GetVisibilityTime().Sub(now) > maxScheduledTaskDuration {
			ms.logger.Info("Dropped long duration scheduled task.", tasks.Tags(task)...)
			continue
		}
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
	taskID, err := ms.shard.GenerateTaskID()
	if err != nil {
		return err
	}
	task.TaskID = taskID
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
	ms.executionStateUpdated = true
	ms.visibilityUpdated = true // workflow status & state change triggers visibility change as well
	return setStateStatus(ms.executionState, state, status)
}

func (ms *MutableStateImpl) IsDirty() bool {
	return ms.hBuilder.IsDirty() || len(ms.InsertTasks) > 0 || (ms.stateMachineNode != nil && ms.stateMachineNode.Dirty())
}

func (ms *MutableStateImpl) isStateDirty() bool {
	// TODO: we need to track more workflow state changes
	// e.g. changes to executionInfo.CancelRequested
	return ms.hBuilder.IsDirty() ||
		len(ms.activityInfosUserDataUpdated) > 0 ||
		len(ms.deleteActivityInfos) > 0 ||
		len(ms.timerInfosUserDataUpdated) > 0 ||
		len(ms.deleteTimerInfos) > 0 ||
		len(ms.updateChildExecutionInfos) > 0 ||
		len(ms.deleteChildExecutionInfos) > 0 ||
		len(ms.updateRequestCancelInfos) > 0 ||
		len(ms.deleteRequestCancelInfos) > 0 ||
		len(ms.updateSignalInfos) > 0 ||
		len(ms.deleteSignalInfos) > 0 ||
		len(ms.updateSignalRequestedIDs) > 0 ||
		len(ms.deleteSignalRequestedIDs) > 0 ||
		len(ms.updateInfoUpdated) > 0 ||
		ms.visibilityUpdated ||
		ms.executionStateUpdated ||
		ms.workflowTaskUpdated ||
		(ms.stateMachineNode != nil && ms.stateMachineNode.Dirty())
}

func (ms *MutableStateImpl) IsTransitionHistoryEnabled() bool {
	return ms.transitionHistoryEnabled
}

func (ms *MutableStateImpl) StartTransaction(
	namespaceEntry *namespace.Namespace,
) (bool, error) {
	if ms.IsDirty() {
		ms.logger.Error("MutableState encountered dirty transaction",
			tag.WorkflowNamespaceID(ms.executionInfo.NamespaceId),
			tag.WorkflowID(ms.executionInfo.WorkflowId),
			tag.WorkflowRunID(ms.executionState.RunId),
			tag.Value(ms.hBuilder),
		)
		metrics.MutableStateChecksumInvalidated.With(ms.metricsHandler).Record(1)
		return false, serviceerror.NewUnavailable("MutableState encountered dirty transaction")
	}

	ms.transitionHistoryEnabled = ms.config.EnableTransitionHistory()

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
	result, err := ms.closeTransaction(transactionPolicy)
	if err != nil {
		return nil, nil, err
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
		NewBufferedEvents:         result.bufferEvents,
		ClearBufferedEvents:       result.clearBuffer,

		Tasks: ms.InsertTasks,

		Condition:       ms.nextEventIDInDB,
		DBRecordVersion: ms.dbRecordVersion,
		Checksum:        result.checksum,
	}

	ms.checksum = result.checksum
	if err := ms.cleanupTransaction(); err != nil {
		return nil, nil, err
	}
	return workflowMutation, result.workflowEventsSeq, nil
}

func (ms *MutableStateImpl) CloseTransactionAsSnapshot(
	transactionPolicy TransactionPolicy,
) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {
	result, err := ms.closeTransaction(transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(result.bufferEvents) > 0 {
		// TODO do we need the functionality to generate snapshot with buffered events?
		return nil, nil, serviceerror.NewInternal("cannot generate workflow snapshot with buffered events")
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
		Checksum:        result.checksum,
	}

	ms.checksum = result.checksum
	if err := ms.cleanupTransaction(); err != nil {
		return nil, nil, err
	}
	return workflowSnapshot, result.workflowEventsSeq, nil
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

func (ms *MutableStateImpl) GenerateMigrationTasks() ([]tasks.Task, int64, error) {
	return ms.taskGenerator.GenerateMigrationTasks()
}

func (ms *MutableStateImpl) updateSearchAttributes(
	updatedPayloadMap map[string]*commonpb.Payload,
) {
	ms.executionInfo.SearchAttributes = payload.MergeMapOfPayload(
		ms.executionInfo.SearchAttributes,
		updatedPayloadMap,
	)
	ms.visibilityUpdated = true
}

func (ms *MutableStateImpl) updateMemo(
	updatedMemo map[string]*commonpb.Payload,
) {
	ms.executionInfo.Memo = payload.MergeMapOfPayload(
		ms.executionInfo.Memo,
		updatedMemo,
	)
	ms.visibilityUpdated = true
}

type closeTransactionResult struct {
	workflowEventsSeq []*persistence.WorkflowEvents
	bufferEvents      []*historypb.HistoryEvent
	clearBuffer       bool
	checksum          *persistencespb.Checksum
}

func (ms *MutableStateImpl) closeTransaction(
	transactionPolicy TransactionPolicy,
) (closeTransactionResult, error) {
	if err := ms.closeTransactionWithPolicyCheck(
		transactionPolicy,
	); err != nil {
		return closeTransactionResult{}, err
	}

	if err := ms.closeTransactionHandleWorkflowTask(
		transactionPolicy,
	); err != nil {
		return closeTransactionResult{}, err
	}

	if err := ms.closeTransactionUpdateTransitionHistory(
		transactionPolicy,
	); err != nil {
		return closeTransactionResult{}, err
	}
	ms.closeTransactionHandleUnknownVersionedTransition()
	ms.closeTransactionTrackLastUpdateVersionedTransition(
		transactionPolicy,
	)

	workflowEventsSeq, eventBatches, bufferEvents, clearBuffer, err := ms.closeTransactionPrepareEvents(transactionPolicy)
	if err != nil {
		return closeTransactionResult{}, err
	}

	ms.closeTransactionTrackTombstones(transactionPolicy)

	if err := ms.closeTransactionPrepareTasks(
		transactionPolicy,
		eventBatches,
		clearBuffer,
	); err != nil {
		return closeTransactionResult{}, err
	}

	ms.executionInfo.StateTransitionCount += 1
	ms.executionInfo.LastUpdateTime = timestamppb.New(ms.shard.GetTimeSource().Now())

	// We generate checksum here based on the assumption that the returned
	// snapshot object is considered immutable. As of this writing, the only
	// code that modifies the returned object lives inside Context.resetWorkflowExecution.
	// Currently, the updates done inside Context.resetWorkflowExecution don't
	// impact the checksum calculation.
	checksum := ms.generateChecksum()

	if ms.dbRecordVersion == 0 {
		// noop, existing behavior
	} else {
		ms.dbRecordVersion += 1
	}

	return closeTransactionResult{
		workflowEventsSeq: workflowEventsSeq,
		bufferEvents:      bufferEvents,
		clearBuffer:       clearBuffer,
		checksum:          checksum,
	}, nil
}

func (ms *MutableStateImpl) closeTransactionHandleWorkflowTask(
	transactionPolicy TransactionPolicy,
) error {
	if err := ms.closeTransactionHandleBufferedEventsLimit(
		transactionPolicy,
	); err != nil {
		return err
	}

	if err := ms.closeTransactionHandleWorkflowTaskScheduling(
		transactionPolicy,
	); err != nil {
		return err
	}

	return ms.closeTransactionHandleSpeculativeWorkflowTask(transactionPolicy)
}

func (ms *MutableStateImpl) closeTransactionHandleWorkflowTaskScheduling(
	transactionPolicy TransactionPolicy,
) error {
	if transactionPolicy == TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
		return nil
	}

	for _, t := range ms.currentTransactionAddedStateMachineEventTypes {
		def, ok := ms.shard.StateMachineRegistry().EventDefinition(t)
		if !ok {
			return serviceerror.NewInternal(fmt.Sprintf("no event definition registered for %v", t))
		}
		if def.IsWorkflowTaskTrigger() {
			if !ms.HasPendingWorkflowTask() {
				if _, err := ms.AddWorkflowTaskScheduledEvent(
					false,
					enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
				); err != nil {
					return err
				}
			}
			break
		}
	}

	return nil
}

func (ms *MutableStateImpl) closeTransactionHandleSpeculativeWorkflowTask(
	transactionPolicy TransactionPolicy,
) error {
	if transactionPolicy == TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
		return nil
	}

	// It is important to convert speculative WT to normal before prepareEventsAndReplicationTasks,
	// because prepareEventsAndReplicationTasks will move internal buffered events to the history,
	// and WT related events (WTScheduled, in particular) need to go first.
	return ms.workflowTaskManager.convertSpeculativeWorkflowTaskToNormal()
}

func (ms *MutableStateImpl) closeTransactionUpdateTransitionHistory(
	transactionPolicy TransactionPolicy,
) error {
	if transactionPolicy != TransactionPolicyActive {
		// TODO: replication/standby logic will need a different way for updating transition history
		// when not syncing mutable state
		return nil
	}

	if !ms.transitionHistoryEnabled {
		return nil
	}

	// TODO: treat changes for transient workflow task or signalRequestID removal as state transition as well.
	// Those changes are not replicated today.
	if !ms.isStateDirty() {
		return nil
	}

	// handle disable then re-enable of transition history
	if len(ms.executionInfo.TransitionHistory) == 0 && len(ms.executionInfo.PreviousTransitionHistory) != 0 {
		ms.executionInfo.TransitionHistory = ms.executionInfo.PreviousTransitionHistory
		ms.executionInfo.PreviousTransitionHistory = nil
	}

	ms.executionInfo.TransitionHistory = UpdatedTransitionHistory(
		ms.executionInfo.TransitionHistory,
		ms.GetCurrentVersion(),
	)

	return nil
}

func (ms *MutableStateImpl) closeTransactionTrackLastUpdateVersionedTransition(
	transactionPolicy TransactionPolicy,
) {
	if transactionPolicy != TransactionPolicyActive {
		// TODO: replication/standby logic will need a different way for updating LastUpdatedVersionedTransition
		// when reapplying history, especially when history replication tasks got batched.
		return
	}

	if !ms.transitionHistoryEnabled {
		// transition history is not enabled
		return
	}
	// transaction closed without any state change
	if len(ms.executionInfo.TransitionHistory) == 0 {
		return
	}

	transitionHistory := ms.executionInfo.TransitionHistory
	currentVersionedTransition := transitionHistory[len(transitionHistory)-1]
	for activityId := range ms.activityInfosUserDataUpdated {
		ms.updateActivityInfos[activityId].LastUpdateVersionedTransition = currentVersionedTransition
	}
	for timerId := range ms.timerInfosUserDataUpdated {
		ms.updateTimerInfos[timerId].LastUpdateVersionedTransition = currentVersionedTransition
	}
	for _, childInfo := range ms.updateChildExecutionInfos {
		childInfo.LastUpdateVersionedTransition = currentVersionedTransition
	}
	for _, cancelInfo := range ms.updateRequestCancelInfos {
		cancelInfo.LastUpdateVersionedTransition = currentVersionedTransition
	}
	for _, signalInfo := range ms.updateSignalInfos {
		signalInfo.LastUpdateVersionedTransition = currentVersionedTransition
	}

	// signalRequestedIDs is a set in DB, we don't have a place to store the lastUpdateVersionedTransition for each
	// signal requestedID.
	// Deletion of signalRequestID is not replicated today, so we can even drop the check on deleteSignalRequestedIDs
	if len(ms.updateSignalRequestedIDs) != 0 || len(ms.deleteSignalRequestedIDs) != 0 {
		ms.executionInfo.SignalRequestIdsLastUpdateVersionedTransition = currentVersionedTransition
	}

	for updateID := range ms.updateInfoUpdated {
		ms.executionInfo.UpdateInfos[updateID].LastUpdateVersionedTransition = currentVersionedTransition
	}

	if ms.workflowTaskUpdated && ms.HasPendingWorkflowTask() {
		ms.executionInfo.WorkflowTaskLastUpdateVersionedTransition = currentVersionedTransition
	}

	if ms.visibilityUpdated {
		ms.executionInfo.VisibilityLastUpdateVersionedTransition = currentVersionedTransition
	}

	if ms.executionStateUpdated {
		ms.executionState.LastUpdateVersionedTransition = currentVersionedTransition
	}

	// LastUpdateVersionTransition for HSM nodes already updated when transitioning the nodes.
}

func (ms *MutableStateImpl) closeTransactionHandleUnknownVersionedTransition() {
	if len(ms.executionInfo.TransitionHistory) != 0 {
		if CompareVersionedTransition(
			ms.versionedTransitionInDB,
			ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1],
		) != 0 {
			// versioned transition updated in the transaction
			return
		}
	}
	// already in unknown state or
	// in an old state that didn't get updated
	if !ms.isStateDirty() {
		// no state change in the transaction
		return
	}
	// State changed but transition history not updated.
	// We are in unknown versioned transition state, clear the transition history.
	if len(ms.executionInfo.TransitionHistory) != 0 {
		ms.executionInfo.PreviousTransitionHistory = ms.executionInfo.TransitionHistory
		ms.executionInfo.LastTransitionHistoryBreakPoint = CopyVersionedTransition(ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1])
	}

	ms.executionInfo.TransitionHistory = nil

	ms.executionInfo.SubStateMachineTombstoneBatches = nil
	ms.totalTombstones = 0

	for _, activityInfo := range ms.updateActivityInfos {
		activityInfo.LastUpdateVersionedTransition = nil
	}
	for _, timerInfo := range ms.updateTimerInfos {
		timerInfo.LastUpdateVersionedTransition = nil
	}
	for _, childInfo := range ms.updateChildExecutionInfos {
		childInfo.LastUpdateVersionedTransition = nil
	}
	for _, requestCancelInfo := range ms.updateRequestCancelInfos {
		requestCancelInfo.LastUpdateVersionedTransition = nil
	}
	for _, signalInfo := range ms.updateSignalInfos {
		signalInfo.LastUpdateVersionedTransition = nil
	}
	for updateID := range ms.updateInfoUpdated {
		ms.executionInfo.UpdateInfos[updateID].LastUpdateVersionedTransition = nil
	}
	if len(ms.updateSignalRequestedIDs) > 0 || len(ms.deleteSignalRequestedIDs) > 0 {
		ms.executionInfo.SignalRequestIdsLastUpdateVersionedTransition = nil
	}
	if ms.visibilityUpdated {
		ms.executionInfo.VisibilityLastUpdateVersionedTransition = nil
	}
	if ms.executionStateUpdated {
		ms.executionState.LastUpdateVersionedTransition = nil
	}
	if ms.workflowTaskUpdated {
		ms.executionInfo.WorkflowTaskLastUpdateVersionedTransition = nil
	}
	if ms.stateMachineNode != nil {
		// the error must be nil here since the fn passed into Walk() always returns nil
		_ = ms.stateMachineNode.Walk(func(node *hsm.Node) error {
			persistenceRepr := node.InternalRepr()
			persistenceRepr.LastUpdateVersionedTransition.TransitionCount = 0
			persistenceRepr.InitialVersionedTransition.TransitionCount = 0
			return nil
		})
	}
}

func (ms *MutableStateImpl) closeTransactionTrackTombstones(
	transactionPolicy TransactionPolicy,
) {
	if transactionPolicy != TransactionPolicyActive {
		// Passive/Replication logic will update tombstone list when applying mutable state
		// snapshot or mutation.
		return
	}

	if !ms.transitionHistoryEnabled {
		// transition history is not enabled
		return
	}

	if len(ms.executionInfo.TransitionHistory) == 0 {
		// in an unknown state
		return
	}

	var tombstones []*persistencespb.StateMachineTombstone
	if ms.stateMachineNode != nil {
		opLog, err := ms.stateMachineNode.OpLog()
		if err != nil {
			panic(fmt.Sprintf("Failed to get HSM operation log: %v", err))
		}

		for _, op := range opLog {
			if deleteOp, ok := op.(hsm.DeleteOperation); ok {
				path := deleteOp.Path()
				if len(path) == 0 {
					continue // Skip root deletion
				}

				tombstone := &persistencespb.StateMachineTombstone{
					StateMachineKey: &persistencespb.StateMachineTombstone_StateMachinePath{
						StateMachinePath: &persistencespb.StateMachinePath{
							Path: make([]*persistencespb.StateMachineKey, len(path)),
						},
					},
				}

				for i, key := range path {
					tombstone.GetStateMachinePath().Path[i] = &persistencespb.StateMachineKey{
						Type: key.Type,
						Id:   key.ID,
					}
				}

				tombstones = append(tombstones, tombstone)
			}
		}
	}

	for scheduledEventID := range ms.deleteActivityInfos {
		tombstones = append(tombstones, &persistencespb.StateMachineTombstone{
			StateMachineKey: &persistencespb.StateMachineTombstone_ActivityScheduledEventId{
				ActivityScheduledEventId: scheduledEventID,
			},
		})
	}
	for timerID := range ms.deleteTimerInfos {
		tombstones = append(tombstones, &persistencespb.StateMachineTombstone{
			StateMachineKey: &persistencespb.StateMachineTombstone_TimerId{
				TimerId: timerID,
			},
		})
	}
	for initiatedEventId := range ms.deleteChildExecutionInfos {
		tombstones = append(tombstones, &persistencespb.StateMachineTombstone{
			StateMachineKey: &persistencespb.StateMachineTombstone_ChildExecutionInitiatedEventId{
				ChildExecutionInitiatedEventId: initiatedEventId,
			},
		})
	}
	for initiatedEventId := range ms.deleteRequestCancelInfos {
		tombstones = append(tombstones, &persistencespb.StateMachineTombstone{
			StateMachineKey: &persistencespb.StateMachineTombstone_RequestCancelInitiatedEventId{
				RequestCancelInitiatedEventId: initiatedEventId,
			},
		})
	}
	for initiatedEventId := range ms.deleteSignalInfos {
		tombstones = append(tombstones, &persistencespb.StateMachineTombstone{
			StateMachineKey: &persistencespb.StateMachineTombstone_SignalExternalInitiatedEventId{
				SignalExternalInitiatedEventId: initiatedEventId,
			},
		})
	}
	// Entire signalRequestedIDs will be synced if updated, so we don't track individual signalRequestedID tombstone.
	// TODO: Track signalRequestedID tombstone when we support syncing partial signalRequestedIDs.
	// This requires tracking the lastUpdateVersionedTransition for each signalRequestedID,
	// which is not supported by today's DB schema.
	// TODO: we don't delete updateInfo and StateMachine today. Track them here when we do.
	transitionHistory := ms.executionInfo.TransitionHistory
	currentVersionedTransition := transitionHistory[len(transitionHistory)-1]

	tombstoneBatch := &persistencespb.StateMachineTombstoneBatch{
		VersionedTransition:    currentVersionedTransition,
		StateMachineTombstones: tombstones,
	}
	// As an optimization, we only track the first empty tombstone batch. So we can know the start point of the tombstone batch
	if len(tombstones) > 0 || len(ms.executionInfo.SubStateMachineTombstoneBatches) == 0 {
		ms.executionInfo.SubStateMachineTombstoneBatches = append(ms.executionInfo.SubStateMachineTombstoneBatches, tombstoneBatch)
	}

	ms.totalTombstones += len(tombstones)
	ms.capTombstoneCount()
}

// capTombstoneCount limits the total number of tombstones stored in the mutable state.
// This method should be called whenever tombstone batch list is updated or synced.
func (ms *MutableStateImpl) capTombstoneCount() {
	tombstoneCountLimit := ms.config.MutableStateTombstoneCountLimit()
	for ms.totalTombstones > tombstoneCountLimit &&
		len(ms.executionInfo.SubStateMachineTombstoneBatches) > 0 {
		ms.totalTombstones -= len(ms.executionInfo.SubStateMachineTombstoneBatches[0].StateMachineTombstones)
		ms.executionInfo.SubStateMachineTombstoneBatches = ms.executionInfo.SubStateMachineTombstoneBatches[1:]
	}
}

func (ms *MutableStateImpl) closeTransactionPrepareTasks(
	transactionPolicy TransactionPolicy,
	eventBatches [][]*historypb.HistoryEvent,
	clearBufferEvents bool,
) error {
	if err := ms.closeTransactionHandleWorkflowResetTask(
		transactionPolicy,
	); err != nil {
		return err
	}

	if err := ms.taskGenerator.GenerateDirtySubStateMachineTasks(ms.shard.StateMachineRegistry()); err != nil {
		return err
	}

	ms.closeTransactionCollapseVisibilityTasks()

	// TODO merge active & passive task generation
	// NOTE: this function must be the last call
	//  since we only generate at most one activity & user timer,
	//  regardless of how many activity & user timer created
	//  so the calculation must be at the very end
	if err := ms.closeTransactionHandleActivityUserTimerTasks(transactionPolicy); err != nil {
		return err
	}

	return ms.closeTransactionPrepareReplicationTasks(transactionPolicy, eventBatches, clearBufferEvents)
}

func (ms *MutableStateImpl) closeTransactionPrepareReplicationTasks(
	transactionPolicy TransactionPolicy,
	eventBatches [][]*historypb.HistoryEvent,
	clearBufferEvents bool,
) error {
	var replicationTasks []tasks.Task
	if ms.config.ReplicationMultipleBatches() {
		task, err := ms.eventsToReplicationTask(transactionPolicy, eventBatches)
		if err != nil {
			return err
		}
		replicationTasks = append(replicationTasks, task...)
	} else {
		for _, historyEvents := range eventBatches {
			task, err := ms.eventsToReplicationTask(transactionPolicy, [][]*historypb.HistoryEvent{historyEvents})
			if err != nil {
				return err
			}
			replicationTasks = append(replicationTasks, task...)
		}
	}

	replicationTasks = append(replicationTasks, ms.syncActivityToReplicationTask(transactionPolicy)...)
	replicationTasks = append(replicationTasks, ms.dirtyHSMToReplicationTask(transactionPolicy, eventBatches, clearBufferEvents)...)
	if ms.transitionHistoryEnabled {
		switch transactionPolicy {
		case TransactionPolicyActive:
			if ms.generateReplicationTask() {
				now := time.Now().UTC()
				workflowKey := definition.NewWorkflowKey(
					ms.executionInfo.NamespaceId,
					ms.executionInfo.WorkflowId,
					ms.executionState.RunId,
				)
				var firstEventID, firstEventVersion, nextEventID int64
				var lastVersionHistoryItem *historyspb.VersionHistoryItem
				if len(eventBatches) > 0 {
					firstEventID = eventBatches[0][0].EventId
					firstEventVersion = eventBatches[0][0].Version
					lastBatch := eventBatches[len(eventBatches)-1]
					nextEventID = lastBatch[len(lastBatch)-1].EventId + 1
				} else {
					currentHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
					if err != nil {
						return err
					}
					item, err := versionhistory.GetLastVersionHistoryItem(currentHistory)
					if err != nil {
						return err
					}
					firstEventID = common.EmptyEventID
					firstEventVersion = common.EmptyVersion
					nextEventID = common.EmptyEventID
					lastVersionHistoryItem = versionhistory.CopyVersionHistoryItem(item)
				}
				transitionHistory := ms.executionInfo.TransitionHistory
				if len(transitionHistory) > 0 && CompareVersionedTransition(
					ms.versionedTransitionInDB,
					ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1],
				) != 0 {
					syncVersionedTransitionTask := &tasks.SyncVersionedTransitionTask{
						WorkflowKey:            workflowKey,
						VisibilityTimestamp:    now,
						Priority:               enumsspb.TASK_PRIORITY_HIGH,
						VersionedTransition:    transitionHistory[len(transitionHistory)-1],
						FirstEventID:           firstEventID,
						FirstEventVersion:      firstEventVersion,
						NextEventID:            nextEventID,
						TaskEquivalents:        replicationTasks,
						LastVersionHistoryItem: lastVersionHistoryItem,
					}

					// versioned transition updated in the transaction
					ms.InsertTasks[tasks.CategoryReplication] = append(
						ms.InsertTasks[tasks.CategoryReplication],
						syncVersionedTransitionTask,
					)
				}
			}
		case TransactionPolicyPassive:
		default:
			panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
		}
	} else {
		ms.InsertTasks[tasks.CategoryReplication] = append(
			ms.InsertTasks[tasks.CategoryReplication],
			replicationTasks...,
		)
	}

	if transactionPolicy == TransactionPolicyPassive &&
		len(ms.InsertTasks[tasks.CategoryReplication]) > 0 {
		return serviceerror.NewInternal("should not generate replication task when close transaction as passive")
	}

	return nil
}

func (ms *MutableStateImpl) cleanupTransaction() error {
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

	ms.visibilityUpdated = false
	ms.executionStateUpdated = false
	ms.workflowTaskUpdated = false
	ms.updateInfoUpdated = make(map[string]struct{})
	ms.timerInfosUserDataUpdated = make(map[string]struct{})
	ms.activityInfosUserDataUpdated = make(map[int64]struct{})
	ms.reapplyEventsCandidate = nil

	ms.stateInDB = ms.executionState.State
	ms.nextEventIDInDB = ms.GetNextEventID()
	if len(ms.executionInfo.TransitionHistory) != 0 {
		ms.versionedTransitionInDB = ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1]
	} else {
		ms.versionedTransitionInDB = nil
	}
	// ms.dbRecordVersion remains the same

	ms.hBuilder = historybuilder.New(
		ms.timeSource,
		ms.shard.GenerateTaskIDs,
		ms.GetCurrentVersion(),
		ms.nextEventIDInDB,
		ms.bufferEventsInDB,
		ms.metricsHandler,
	)

	ms.InsertTasks = make(map[tasks.Category][]tasks.Task)

	// Clear outputs for the next transaction.
	ms.stateMachineNode.ClearTransactionState()
	// Clear out transient state machine state.
	ms.currentTransactionAddedStateMachineEventTypes = nil

	return nil
}

func (ms *MutableStateImpl) closeTransactionPrepareEvents(
	transactionPolicy TransactionPolicy,
) ([]*persistence.WorkflowEvents, [][]*historypb.HistoryEvent, []*historypb.HistoryEvent, bool, error) {
	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, nil, nil, false, err
	}

	historyMutation, err := ms.hBuilder.Finish(!ms.HasStartedWorkflowTask())
	if err != nil {
		return nil, nil, nil, false, err
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
		return nil, nil, nil, false, err
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
		return nil, nil, nil, false, err
	}

	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		lastEvent := lastEvents[len(lastEvents)-1]
		if err := ms.updateWithLastWriteEvent(
			lastEvent,
			transactionPolicy,
		); err != nil {
			return nil, nil, nil, false, err
		}
	}

	return workflowEventsSeq, newEventsBatches, newBufferBatch, clearBuffer, nil
}

func (ms *MutableStateImpl) eventsToReplicationTask(
	transactionPolicy TransactionPolicy,
	eventBatches [][]*historypb.HistoryEvent,
) ([]tasks.Task, error) {
	switch transactionPolicy {
	case TransactionPolicyActive:
		if ms.generateReplicationTask() {
			return ms.taskGenerator.GenerateHistoryReplicationTasks(eventBatches)
		}
		return nil, nil
	case TransactionPolicyPassive:
		return nil, nil
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
			var activityIDs map[int64]struct{}
			if ms.disablingTransitionHistory() {
				activityIDs = make(map[int64]struct{}, len(ms.GetPendingActivityInfos()))
				for activityID := range ms.GetPendingActivityInfos() {
					activityIDs[activityID] = struct{}{}
				}
			} else {
				activityIDs = ms.syncActivityTasks
			}
			return convertSyncActivityInfos(
				now,
				definition.NewWorkflowKey(
					ms.executionInfo.NamespaceId,
					ms.executionInfo.WorkflowId,
					ms.executionState.RunId,
				),
				ms.pendingActivityInfoIDs,
				activityIDs,
			)
		}
		return nil
	case TransactionPolicyPassive:
		return emptyTasks
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) dirtyHSMToReplicationTask(
	transactionPolicy TransactionPolicy,
	eventBatches [][]*historypb.HistoryEvent,
	clearBufferEvents bool,
) []tasks.Task {
	switch transactionPolicy {
	case TransactionPolicyActive:
		if !ms.generateReplicationTask() {
			return emptyTasks
		}

		// HSM() contains children also implies Nexus is enabled
		if len(ms.HSM().InternalRepr().Children) == 0 {
			return emptyTasks
		}

		// We also assume that - for the time being - if events were generated in a transaction,
		// all HSM node updates are a result of applying those events.
		// The passive cluster will transition the HSM when applying the events.
		//
		// When mutable state contains buffered events, SyncHSM task will be dropped as
		// the state for **some** HSM nodes may depend on those buffered events.
		// But there might be some other nodes whose state do not depend on buffered events.
		// For those nodes, we need to make sure their states will be synced as well.
		// So when clearing buffered events, generate another SyncHSM task.
		if clearBufferEvents || (ms.HSM().Dirty() && len(eventBatches) == 0) {
			return []tasks.Task{
				&tasks.SyncHSMTask{
					WorkflowKey: ms.GetWorkflowKey(),
					// TaskID and VisibilityTimestamp are set by shard
				},
			}
		}

		return emptyTasks
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
			ms.activityInfosUserDataUpdated[activityInfo.ScheduledEventId] = struct{}{}
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
		localNamespaceMutation := namespace.WithPretendLocalNamespace(
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
	currentVersion := ms.GetCurrentVersion()
	if workflowTask == nil || workflowTask.Version >= currentVersion {
		// no pending workflow tasks, no buffered events
		// or workflow task has higher / equal version
		return false, nil
	}

	lastEventVersion, err := ms.GetLastEventVersion()
	if err != nil {
		return false, err
	}
	if lastEventVersion != workflowTask.Version {
		return false, serviceerror.NewInternal(fmt.Sprintf("MutableStateImpl encountered mismatch version, workflow task: %v, last event version %v", workflowTask.Version, lastEventVersion))
	}

	// NOTE: if lastEventVersion is used here then the version transition history could decrecase
	//
	// TODO: Today's replication task processing logic won't flush buffered events when applying state only changes.
	// As a result, when using lastWriteVersion, which takes state only change into account, here, we could still
	// have buffered events, but lastWriteSourceCluster will no longer be current cluster and the be treated as case 4
	// and fail on the sanity check.
	// We need to change replication task processing logic to always flush buffered events on all replication task types.
	// State transition history is not enabled today so we are safe and LastWriteVersion == LastEventVersion
	lastWriteVersion, err := ms.GetLastWriteVersion()
	if err != nil {
		return false, err
	}

	lastWriteCluster := ms.clusterMetadata.ClusterNameForFailoverVersion(ms.namespaceEntry.IsGlobalNamespace(), lastWriteVersion)
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
	if lastWriteCluster != currentCluster && currentVersionCluster != currentCluster {
		// do a sanity check on buffered events
		if ms.HasBufferedEvents() {
			return false, serviceerror.NewInternal("MutableStateImpl encountered previous passive workflow with buffered events")
		}
		return false, nil
	}

	// handle case 1 & 2
	flushBufferVersion := lastWriteVersion

	// handle case 3
	if lastWriteCluster != currentCluster && currentVersionCluster == currentCluster {
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

func (ms *MutableStateImpl) closeTransactionHandleWorkflowResetTask(
	transactionPolicy TransactionPolicy,
) error {
	if transactionPolicy == TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
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

// Visibility tasks are collapsed into a single one: START < UPSERT < CLOSE < DELETE
// Their enum values are already in order, so using them to make the code simpler.
// Any other task type is preserved in order.
// Eg: [START, UPSERT, TP1, CLOSE, TP2, TP3] -> [TP1, CLOSE, TP2, TP3]
func (ms *MutableStateImpl) closeTransactionCollapseVisibilityTasks() {
	visTasks := ms.InsertTasks[tasks.CategoryVisibility]
	if len(visTasks) < 2 {
		return
	}
	var visTaskToKeep tasks.Task
	lastIndex := -1
	for i, task := range visTasks {
		switch task.GetType() {
		case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
			enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
			enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
			enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
			if visTaskToKeep == nil || task.GetType() >= visTaskToKeep.GetType() {
				visTaskToKeep = task
			}
			lastIndex = i
		}
	}
	if visTaskToKeep == nil {
		return
	}
	collapsedVisTasks := make([]tasks.Task, 0, len(visTasks))
	for i, task := range visTasks {
		switch task.GetType() {
		case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
			enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
			enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
			enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
			if i == lastIndex {
				collapsedVisTasks = append(collapsedVisTasks, visTaskToKeep)
			}
		default:
			collapsedVisTasks = append(collapsedVisTasks, task)
		}
	}
	ms.InsertTasks[tasks.CategoryVisibility] = collapsedVisTasks
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
		return ms.executionInfo.LastUpdateTime.AsTime().Before(invalidateBefore)
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

func (ms *MutableStateImpl) HasCompletedAnyWorkflowTask() bool {
	return ms.GetLastCompletedWorkflowTaskStartedEventId() != common.EmptyEventID
}

func (ms *MutableStateImpl) RefreshExpirationTimeoutTask(ctx context.Context) error {
	executionInfo := ms.GetExecutionInfo()
	weTimeout := timestamp.DurationValue(executionInfo.WorkflowExecutionTimeout)
	if weTimeout > 0 {
		executionInfo.WorkflowExecutionExpirationTime = timestamp.TimeNowPtrUtcAddDuration(weTimeout)
	}
	wrTimeout := timestamp.DurationValue(executionInfo.WorkflowRunTimeout)
	if wrTimeout != 0 {
		executionInfo.WorkflowRunExpirationTime = timestamp.TimeNowPtrUtcAddDuration(wrTimeout)

		if weTimeout > 0 && executionInfo.WorkflowRunExpirationTime.AsTime().After(executionInfo.WorkflowExecutionExpirationTime.AsTime()) {
			executionInfo.WorkflowRunExpirationTime = executionInfo.WorkflowExecutionExpirationTime
		}
	}

	return RefreshTasksForWorkflowStart(ctx, ms, ms.taskGenerator, EmptyVersionedTransition)
}

func (ms *MutableStateImpl) currentVersionedTransition() *persistencespb.VersionedTransition {
	if len(ms.executionInfo.TransitionHistory) == 0 {
		// transition history is not enabled
		return nil
	}
	transitionHistory := ms.executionInfo.TransitionHistory
	return transitionHistory[len(transitionHistory)-1]
}

func (ms *MutableStateImpl) ApplyMutation(
	mutation *persistencespb.WorkflowMutableStateMutation,
) error {
	prevExecutionInfoSize := ms.executionInfo.Size()
	currentVersionedTransition := ms.currentVersionedTransition()

	ms.applySignalRequestedIds(mutation.SignalRequestedIds, mutation.ExecutionInfo)
	err := ms.applyTombstones(mutation.SubStateMachineTombstoneBatches, currentVersionedTransition)
	if err != nil {
		return err
	}
	err = ms.syncExecutionInfo(ms.executionInfo, mutation.ExecutionInfo, false)
	if err != nil {
		return err
	}
	if mutation.ExecutionInfo.WorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		ms.workflowTaskManager.deleteWorkflowTask()
	}

	ms.applyUpdatesToUpdateInfos(mutation.UpdatedUpdateInfos, false)

	ms.approximateSize += mutation.ExecutionState.Size() - ms.executionState.Size()
	ms.executionState = mutation.ExecutionState

	err = ms.applyUpdatesToSubStateMachines(
		mutation.UpdatedActivityInfos,
		mutation.UpdatedTimerInfos,
		mutation.UpdatedChildExecutionInfos,
		mutation.UpdatedRequestCancelInfos,
		mutation.UpdatedSignalInfos,
		false,
	)
	if err != nil {
		return err
	}

	err = ms.applyUpdatesToStateMachineNodes(mutation.UpdatedSubStateMachines)
	if err != nil {
		return err
	}

	ms.approximateSize += ms.executionInfo.Size() - prevExecutionInfoSize

	return nil
}

func (ms *MutableStateImpl) ApplySnapshot(
	snapshot *persistencespb.WorkflowMutableState,
) error {
	prevExecutionInfoSize := ms.executionInfo.Size()

	ms.applySignalRequestedIds(snapshot.SignalRequestedIds, snapshot.ExecutionInfo)
	err := ms.syncExecutionInfo(ms.executionInfo, snapshot.ExecutionInfo, true)
	if err != nil {
		return err
	}
	if snapshot.ExecutionInfo.WorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		ms.workflowTaskManager.deleteWorkflowTask()
	}

	ms.applyUpdatesToUpdateInfos(snapshot.ExecutionInfo.UpdateInfos, true)

	err = ms.syncSubStateMachinesByType(snapshot.ExecutionInfo.SubStateMachinesByType)
	if err != nil {
		return err
	}

	ms.approximateSize += snapshot.ExecutionState.Size() - ms.executionState.Size()
	ms.executionState = snapshot.ExecutionState

	err = ms.applyUpdatesToSubStateMachines(
		snapshot.ActivityInfos,
		snapshot.TimerInfos,
		snapshot.ChildExecutionInfos,
		snapshot.RequestCancelInfos,
		snapshot.SignalInfos,
		true,
	)
	if err != nil {
		return err
	}

	ms.approximateSize += ms.executionInfo.Size() - prevExecutionInfoSize
	return nil
}

func (ms *MutableStateImpl) ShouldResetActivityTimerTaskMask(current, incoming *persistencespb.ActivityInfo) bool {
	// calculate whether to reset the activity timer task status bits
	// reset timer task status bits if
	// 1. same source cluster & attempt changes
	// 2. same activity stamp
	// 3. different source cluster
	if !ms.clusterMetadata.IsVersionFromSameCluster(current.Version, incoming.Version) {
		return true
	} else if current.Attempt != incoming.Attempt {
		return true
	} else if current.Stamp != incoming.Stamp {
		return true
	}
	return false
}

func (ms *MutableStateImpl) applyUpdatesToSubStateMachines(
	updatedActivityInfos map[int64]*persistencespb.ActivityInfo,
	updatedTimerInfos map[string]*persistencespb.TimerInfo,
	updatedChildExecutionInfos map[int64]*persistencespb.ChildExecutionInfo,
	updatedRequestCancelInfos map[int64]*persistencespb.RequestCancelInfo,
	updatedSignalInfos map[int64]*persistencespb.SignalInfo,
	isSnapshot bool,
) error {
	err := applyUpdatesToSubStateMachine(ms, ms.pendingActivityInfoIDs, ms.updateActivityInfos, updatedActivityInfos, isSnapshot, ms.DeleteActivity, func(current, incoming *persistencespb.ActivityInfo) {
		if current == nil || ms.ShouldResetActivityTimerTaskMask(current, incoming) {
			incoming.TimerTaskStatus = TimerTaskStatusNone
		} else {
			incoming.TimerTaskStatus = current.TimerTaskStatus
		}
	}, func(ai *persistencespb.ActivityInfo) {
		ms.pendingActivityIDToEventID[ai.ActivityId] = ai.ScheduledEventId
		ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}
	})
	if err != nil {
		return err
	}

	err = applyUpdatesToSubStateMachine(ms, ms.pendingTimerInfoIDs, ms.updateTimerInfos, updatedTimerInfos, isSnapshot, ms.DeleteUserTimer, func(current, incoming *persistencespb.TimerInfo) {
		incoming.TaskStatus = TimerTaskStatusNone
	}, func(ti *persistencespb.TimerInfo) {
		ms.pendingTimerEventIDToID[ti.StartedEventId] = ti.TimerId
		ms.timerInfosUserDataUpdated[ti.TimerId] = struct{}{}
	})
	if err != nil {
		return err
	}

	err = applyUpdatesToSubStateMachine(ms, ms.pendingChildExecutionInfoIDs, ms.updateChildExecutionInfos, updatedChildExecutionInfos, isSnapshot, ms.DeletePendingChildExecution, func(current, incoming *persistencespb.ChildExecutionInfo) {
		if current != nil {
			incoming.Clock = current.Clock
		}
	}, nil)
	if err != nil {
		return err
	}

	err = applyUpdatesToSubStateMachine(ms, ms.pendingRequestCancelInfoIDs, ms.updateRequestCancelInfos, updatedRequestCancelInfos, isSnapshot, ms.DeletePendingRequestCancel, nil, nil)
	if err != nil {
		return err
	}

	err = applyUpdatesToSubStateMachine(ms, ms.pendingSignalInfoIDs, ms.updateSignalInfos, updatedSignalInfos, isSnapshot, ms.DeletePendingSignal, nil, nil)
	return err
}

func (ms *MutableStateImpl) applyUpdatesToStateMachineNodes(
	nodeMutations []*persistencespb.WorkflowMutableStateMutation_StateMachineNodeMutation,
) error {
	root := ms.HSM()
	// Source cluster uses Walk() to generate node mutations.
	// Walk() uses pre-order DFS. Updated parent nodes will be added before children.
	for _, nodeMutation := range nodeMutations {
		var internalNode *persistencespb.StateMachineNode
		incomingPath := []hsm.Key{}
		for _, p := range nodeMutation.Path.Path {
			incomingPath = append(incomingPath, hsm.Key{Type: p.Type, ID: p.Id})
		}
		node, err := root.Child(incomingPath)
		if err != nil {
			if !errors.Is(err, hsm.ErrStateMachineNotFound) {
				return err
			}
			parent := incomingPath[:len(incomingPath)-1]
			if len(parent) == 0 {
				parent = root.Path()
			}
			parentNode, err := root.Child(parent)
			if err != nil {
				// we don't have enough information to recreate all parents
				return err
			}

			key := incomingPath[len(incomingPath)-1]
			parentInternalNode := parentNode.InternalRepr()

			internalNode = &persistencespb.StateMachineNode{
				Children: make(map[string]*persistencespb.StateMachineMap),
			}
			children, ok := parentInternalNode.Children[key.Type]
			if !ok {
				children = &persistencespb.StateMachineMap{MachinesById: make(map[string]*persistencespb.StateMachineNode)}
				// Children may be nil if the map was empty and the proto message we serialized and deserialized.
				if parentInternalNode.Children == nil {
					parentInternalNode.Children = make(map[string]*persistencespb.StateMachineMap, 1)
				}
				parentInternalNode.Children[key.Type] = children
			}
			children.MachinesById[key.ID] = internalNode
		} else {
			internalNode = node.InternalRepr()
			if CompareVersionedTransition(nodeMutation.LastUpdateVersionedTransition, internalNode.LastUpdateVersionedTransition) == 0 {
				continue
			}
			node.InvalidateCache()
		}
		internalNode.Data = nodeMutation.Data
		internalNode.InitialVersionedTransition = nodeMutation.InitialVersionedTransition
		internalNode.LastUpdateVersionedTransition = nodeMutation.LastUpdateVersionedTransition
		internalNode.TransitionCount++
	}
	return nil
}

func (ms *MutableStateImpl) applySignalRequestedIds(signalRequestedIds []string, incomingExecutionInfo *persistencespb.WorkflowExecutionInfo) {
	if CompareVersionedTransition(
		incomingExecutionInfo.SignalRequestIdsLastUpdateVersionedTransition,
		ms.executionInfo.SignalRequestIdsLastUpdateVersionedTransition,
	) == 0 {
		return
	}

	ids := make(map[string]struct{}, len(signalRequestedIds))
	for _, id := range signalRequestedIds {
		ids[id] = struct{}{}
	}
	for requestID := range ms.pendingSignalRequestedIDs {
		if _, ok := ids[requestID]; !ok {
			ms.DeleteSignalRequested(requestID)
		}
	}

	for _, requestID := range signalRequestedIds {
		if _, ok := ms.pendingSignalRequestedIDs[requestID]; !ok {
			ms.AddSignalRequested(requestID)
		}
	}
}

func applyUpdatesToSubStateMachine[K comparable, V lastUpdatedStateTransitionGetter](
	ms *MutableStateImpl,
	pendingInfos map[K]V,
	updateInfos map[K]V,
	updatedSubStateMachine map[K]V,
	isSnapshot bool,
	deleteFn func(K) error,
	sanitizeFn func(current, incoming V),
	postUpdateFn func(V),
) error {
	if isSnapshot {
		for key := range pendingInfos {
			if _, ok := updatedSubStateMachine[key]; !ok {
				err := deleteFn(key)
				if err != nil {
					return err
				}
			}
		}
	}

	getSizeOfKey := func(key any) int {
		switch v := key.(type) {
		case string:
			return len(v)
		default:
			return int(reflect.TypeOf(key).Size())
		}
	}

	for key, updated := range updatedSubStateMachine {
		var existing V
		if existing, ok := pendingInfos[key]; ok {
			if CompareVersionedTransition(existing.GetLastUpdateVersionedTransition(), updated.GetLastUpdateVersionedTransition()) == 0 {
				continue
			}
			ms.approximateSize -= existing.Size() + getSizeOfKey(key)
		}
		val := updated
		if sanitizeFn != nil {
			val = common.CloneProto(updated)
			sanitizeFn(existing, val)
		}
		pendingInfos[key] = val
		updateInfos[key] = val
		ms.approximateSize += val.Size() + getSizeOfKey(key)
		if postUpdateFn != nil {
			postUpdateFn(val)
		}
	}
	return nil
}

func (ms *MutableStateImpl) applyUpdatesToUpdateInfos(
	updatedUpdateInfos map[string]*persistencespb.UpdateInfo,
	isSnapshot bool,
) {
	if ms.executionInfo.UpdateInfos == nil {
		ms.executionInfo.UpdateInfos = make(map[string]*persistencespb.UpdateInfo, len(updatedUpdateInfos))
	}
	if isSnapshot {
		for updateID := range ms.executionInfo.UpdateInfos {
			if _, ok := updatedUpdateInfos[updateID]; !ok {
				ms.approximateSize -= ms.executionInfo.UpdateInfos[updateID].Size() + len(updateID)
				delete(ms.executionInfo.UpdateInfos, updateID)
			}
		}
	}

	for updateID, ui := range updatedUpdateInfos {
		if existing, ok := ms.executionInfo.UpdateInfos[updateID]; ok {
			if CompareVersionedTransition(existing.GetLastUpdateVersionedTransition(), ui.GetLastUpdateVersionedTransition()) == 0 {
				continue
			}
			ms.approximateSize -= existing.Size() + len(updateID)
		} else {
			ms.executionInfo.UpdateCount++
		}
		ms.executionInfo.UpdateInfos[updateID] = ui
		ms.approximateSize += ui.Size() + len(updateID)
	}
}

func (ms *MutableStateImpl) syncExecutionInfo(current *persistencespb.WorkflowExecutionInfo, incoming *persistencespb.WorkflowExecutionInfo, isSnapshot bool) error {
	doNotSync := func(v any) []interface{} {
		info, ok := v.(*persistencespb.WorkflowExecutionInfo)
		if !ok || info == nil {
			return nil
		}
		ignoreFields := []interface{}{
			&info.WorkflowTaskVersion,
			&info.WorkflowTaskScheduledEventId,
			&info.WorkflowTaskStartedEventId,
			&info.WorkflowTaskRequestId,
			&info.WorkflowTaskTimeout,
			&info.WorkflowTaskAttempt,
			&info.WorkflowTaskStartedTime,
			&info.WorkflowTaskScheduledTime,
			&info.WorkflowTaskOriginalScheduledTime,
			&info.WorkflowTaskType,
			&info.WorkflowTaskSuggestContinueAsNew,
			&info.WorkflowTaskHistorySizeBytes,
			&info.WorkflowTaskBuildId,
			&info.WorkflowTaskBuildIdRedirectCounter,
			&info.VersionHistories,
			&info.ExecutionStats,
			&info.LastFirstEventTxnId,
			&info.ParentClock,
			&info.CloseTransferTaskId,
			&info.CloseVisibilityTaskId,
			&info.RelocatableAttributesRemoved,
			&info.WorkflowExecutionTimerTaskStatus,
			&info.SubStateMachinesByType,
			&info.StateMachineTimers,
			&info.TaskGenerationShardClockTimestamp,
			&info.UpdateInfos,
		}
		if !isSnapshot {
			ignoreFields = append(ignoreFields, &info.SubStateMachineTombstoneBatches)
		}
		return ignoreFields
	}
	err := common.MergeProtoExcludingFields(current, incoming, doNotSync)
	if err != nil {
		return err
	}

	ms.ClearStickyTaskQueue()

	ms.workflowTaskManager.UpdateWorkflowTask(&WorkflowTaskInfo{
		Version:             incoming.WorkflowTaskVersion,
		ScheduledEventID:    incoming.WorkflowTaskScheduledEventId,
		StartedEventID:      incoming.WorkflowTaskStartedEventId,
		RequestID:           incoming.WorkflowTaskRequestId,
		WorkflowTaskTimeout: incoming.WorkflowTaskTimeout.AsDuration(),
		Attempt:             incoming.WorkflowTaskAttempt,
		StartedTime:         incoming.WorkflowTaskStartedTime.AsTime(),
		ScheduledTime:       incoming.WorkflowTaskScheduledTime.AsTime(),

		OriginalScheduledTime: incoming.WorkflowTaskOriginalScheduledTime.AsTime(),
		Type:                  incoming.WorkflowTaskType,

		SuggestContinueAsNew:   incoming.WorkflowTaskSuggestContinueAsNew,
		HistorySizeBytes:       incoming.WorkflowTaskHistorySizeBytes,
		BuildId:                incoming.WorkflowTaskBuildId,
		BuildIdRedirectCounter: incoming.WorkflowTaskBuildIdRedirectCounter,
	})

	return nil
}

func (ms *MutableStateImpl) syncSubStateMachinesByType(incoming map[string]*persistencespb.StateMachineMap) error {
	currentHSM := ms.HSM()

	// we don't care about the root here which is the entire mutable state
	incomingHSM, err := hsm.NewRoot(
		ms.shard.StateMachineRegistry(),
		StateMachineType,
		ms,
		incoming,
		ms,
	)
	if err != nil {
		return err
	}

	if err := incomingHSM.Walk(func(incomingNode *hsm.Node) error {
		if incomingNode.Parent == nil {
			// skip root which is the entire mutable state
			return nil
		}

		incomingNodePath := incomingNode.Path()
		currentNode, err := currentHSM.Child(incomingNodePath)
		if err != nil {
			// 1. Already done history resend if needed before,
			// and node creation today always associated with an event
			// 2. Node deletion is not supported right now.
			// Based on 1 and 2, node should always be found here.
			return err
		}

		return currentNode.Sync(incomingNode)
	}); err != nil {
		return err
	}

	return nil
}

func (ms *MutableStateImpl) applyTombstones(tombstoneBatches []*persistencespb.StateMachineTombstoneBatch, currentVersionedTransition *persistencespb.VersionedTransition) error {
	var err error
	for _, tombstoneBatch := range tombstoneBatches {
		if CompareVersionedTransition(tombstoneBatch.VersionedTransition, currentVersionedTransition) <= 0 {
			continue
		}
		for _, tombstone := range tombstoneBatch.StateMachineTombstones {
			switch tombstone.StateMachineKey.(type) {
			case *persistencespb.StateMachineTombstone_ActivityScheduledEventId:
				if _, ok := ms.pendingActivityInfoIDs[tombstone.GetActivityScheduledEventId()]; ok {
					err = ms.DeleteActivity(tombstone.GetActivityScheduledEventId())
				}
			case *persistencespb.StateMachineTombstone_TimerId:
				if _, ok := ms.pendingTimerInfoIDs[tombstone.GetTimerId()]; ok {
					err = ms.DeleteUserTimer(tombstone.GetTimerId())
				}
			case *persistencespb.StateMachineTombstone_ChildExecutionInitiatedEventId:
				if _, ok := ms.pendingChildExecutionInfoIDs[tombstone.GetChildExecutionInitiatedEventId()]; ok {
					err = ms.DeletePendingChildExecution(tombstone.GetChildExecutionInitiatedEventId())
				}
			case *persistencespb.StateMachineTombstone_RequestCancelInitiatedEventId:
				if _, ok := ms.pendingRequestCancelInfoIDs[tombstone.GetRequestCancelInitiatedEventId()]; ok {
					err = ms.DeletePendingRequestCancel(tombstone.GetRequestCancelInitiatedEventId())
				}
			case *persistencespb.StateMachineTombstone_SignalExternalInitiatedEventId:
				if _, ok := ms.pendingSignalInfoIDs[tombstone.GetSignalExternalInitiatedEventId()]; ok {
					err = ms.DeletePendingSignal(tombstone.GetSignalExternalInitiatedEventId())
				}
			default:
				// TODO: updateID and stateMachinePath
				err = serviceerror.NewInternal("unknown tombstone type")
			}
			if err != nil {
				return err
			}
		}
		ms.executionInfo.SubStateMachineTombstoneBatches = append(ms.executionInfo.SubStateMachineTombstoneBatches, tombstoneBatch)
		ms.totalTombstones += len(tombstoneBatch.StateMachineTombstones)
	}
	ms.capTombstoneCount()
	return nil
}

func (ms *MutableStateImpl) disablingTransitionHistory() bool {
	return ms.versionedTransitionInDB != nil && len(ms.executionInfo.TransitionHistory) == 0
}

func (ms *MutableStateImpl) InitTransitionHistory() {
	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: ms.GetCurrentVersion(),
		TransitionCount:          1,
	}
	ms.GetExecutionInfo().TransitionHistory = []*persistencespb.VersionedTransition{versionedTransition}

	ms.GetExecutionInfo().VisibilityLastUpdateVersionedTransition = versionedTransition
	ms.GetExecutionState().LastUpdateVersionedTransition = versionedTransition
	if ms.HasPendingWorkflowTask() {
		ms.GetExecutionInfo().WorkflowTaskLastUpdateVersionedTransition = versionedTransition
	}
}

func (ms *MutableStateImpl) initVersionedTransitionInDB() {
	if len(ms.executionInfo.TransitionHistory) != 0 {
		ms.versionedTransitionInDB = ms.executionInfo.TransitionHistory[len(ms.executionInfo.TransitionHistory)-1]
	}
}

// GetEffectiveDeployment returns the effective deployment in the following order:
//  1. DeploymentTransition.Deployment: this is returned when the wf is transitioning to a
//     new deployment
//  2. VersioningOverride.Deployment: this is returned when user has set a PINNED override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  3. Deployment: this is returned when there is no transition and no override (the most
//     common case). Deployment is set based on the worker-sent deployment in the latest WFT
//     completion. Exception: if Deployment is set but the workflow's effective behavior is
//     UNSPECIFIED, it means the workflow is unversioned, so effective deployment will be nil.
//
// Note: Deployment objects are immutable, never change their fields.
func (ms *MutableStateImpl) GetEffectiveDeployment() *deploymentpb.Deployment {
	return GetEffectiveDeployment(ms.GetExecutionInfo().GetVersioningInfo())
}

func (ms *MutableStateImpl) GetDeploymentTransition() *workflowpb.DeploymentTransition {
	return ms.GetExecutionInfo().GetVersioningInfo().GetDeploymentTransition()
}

// GetEffectiveVersioningBehavior returns the effective versioning behavior in the following
// order:
//  1. VersioningOverride.Behavior: this is returned when user has set a behavior override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  2. Behavior: this is returned when there is no override (most common case). Behavior is
//     set based on the worker-sent deployment in the latest WFT completion.
func (ms *MutableStateImpl) GetEffectiveVersioningBehavior() enumspb.VersioningBehavior {
	return GetEffectiveVersioningBehavior(ms.GetExecutionInfo().GetVersioningInfo())
}

// StartDeploymentTransition starts a transition to the given deployment which must be
// different from workflows effective deployment. Will fail if the workflow is pinned.
// Starting a new transition replaces current transition, if present, without rescheduling
// activities.
// If there is a pending workflow task that is not started yet, it'll be rescheduled after
// transition start.
func (ms *MutableStateImpl) StartDeploymentTransition(deployment *deploymentpb.Deployment) error {
	wfBehavior := ms.GetEffectiveVersioningBehavior()
	if wfBehavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
		// WF is pinned so we reject the transition.
		// It's possible that a backlogged task in matching from an earlier time that this wf was
		// unpinned is being dispatched now and wants to redirect the wf. Such task should be dropped.
		return ErrPinnedWorkflowCannotTransition
	}

	versioningInfo := ms.GetExecutionInfo().GetVersioningInfo()
	if versioningInfo == nil {
		versioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
		ms.GetExecutionInfo().VersioningInfo = versioningInfo
	}

	versioningInfo.DeploymentTransition = &workflowpb.DeploymentTransition{
		Deployment: deployment,
	}

	// Because deployment is changed, we clear sticky queue to make sure the next wf task does not
	// go to the old deployment.
	ms.ClearStickyTaskQueue()

	// If there is a pending (unstarted) WFT, we need to reschedule it, so it goes to Matching with
	// the right directive deployment. The current scheduled WFT will be rejected when attempted to
	// start because its directive deployment no longer matches workflows effective deployment.
	// If the WFT is started but not finished, we let it run its course, once it's completed, failed
	// or timed out a new one will be scheduled.
	if ms.HasPendingWorkflowTask() && !ms.HasStartedWorkflowTask() &&
		// Speculative WFT is directly (without transfer task) added to matching when scheduled.
		// It is protected by timeout on both normal and sticky task queues.
		// If there is no poller for previous deployment, it will time out,
		// and will be rescheduled as normal WFT.
		ms.GetPendingWorkflowTask().Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// sticky queue was just cleared, so the following call only generates
		// a WorkflowTask, not a WorkflowTaskTimeoutTask.
		err := ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(ms.GetPendingWorkflowTask().ScheduledEventID)
		if err != nil {
			return err
		}
	}
	return nil
}

// reschedulePendingActivities reschedules all the activities that are not started, so they are
// scheduled against the right queue in matching.
func (ms *MutableStateImpl) reschedulePendingActivities() error {
	for _, ai := range ms.GetPendingActivityInfos() {
		if ai.StartedEventId != common.EmptyEventID {
			// TODO: skip task generation also when activity is in backoff period
			// activity already started
			continue
		}
		// we only need to resend the activities to matching, no need to update timer tasks.
		err := ms.taskGenerator.GenerateActivityTasks(ai.ScheduledEventId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ms *MutableStateImpl) AddReapplyCandidateEvent(event *historypb.HistoryEvent) {
	if shouldReapplyEvent(ms.shard.StateMachineRegistry(), event) {
		ms.reapplyEventsCandidate = append(ms.reapplyEventsCandidate, event)
	}
}

func (ms *MutableStateImpl) GetReapplyCandidateEvents() []*historypb.HistoryEvent {
	return ms.reapplyEventsCandidate
}
