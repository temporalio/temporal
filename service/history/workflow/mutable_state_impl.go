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

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	rulespb "go.temporal.io/api/rules/v1"
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
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/update"
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

var emptyTasks = []tasks.Task{}

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

		chasmTree historyi.ChasmTree

		executionInfo  *persistencespb.WorkflowExecutionInfo // Workflow mutable state info.
		executionState *persistencespb.WorkflowExecutionState

		hBuilder *historybuilder.HistoryBuilder

		// In-memory only attributes
		currentVersion int64
		// Running approximate total size of mutable state fields (except buffered events) when written to DB in bytes.
		// Buffered events are added to this value when calling GetApproximatePersistedSize.
		approximateSize int
		chasmNodeSizes  map[string]int // chasm node path -> key + node size in bytes
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

		// isResetStateUpdated is used to track if resetRunID is updated.
		isResetStateUpdated bool

		// in memory fields to track potential reapply events that needs to be reapplied during workflow update
		// should only be used in the state based replication as state based replication does not have
		// event inside history builder. This is only for x-run reapply (from zombie wf to current wf)
		reapplyEventsCandidate []*historypb.HistoryEvent

		InsertTasks map[tasks.Category][]tasks.Task

		// BestEffortDeleteTasks holds keys of history tasks to be deleted after a successful
		// persistence update. This deletion is done on best effort basis. Persistence layer can ignore it without
		// any errors.
		BestEffortDeleteTasks map[tasks.Category][]tasks.Key

		speculativeWorkflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask

		// In-memory storage for workflow task timeout tasks. These are set when timeout tasks are
		// generated and used to delete them when the workflow task completes. Not persisted to storage.
		wftScheduleToStartTimeoutTask *tasks.WorkflowTaskTimeoutTask
		wftStartToCloseTimeoutTask    *tasks.WorkflowTaskTimeoutTask

		// In-memory storage for CHASM pure tasks. These are set when CHASM pure tasks are generated and used to
		// delete them when then are no longer needed. (i.e. when the task's scheduled time is after that of the
		// earliest valid CHASM pure task's).
		//
		// Those pure tasks are mostly reverse ordered by their scheduled time (the VisibilityTimestamp field).
		// Since a physical pure task is only generated when there's no other pure task with an earlier scheduled time,
		// simply appending new pure tasks to the end of the slice maintains the order.
		//
		// NOTE: shard context may move those tasks' scheduled time to the future if they are earlier than the timer queue's
		// max read level (otherwise those tasks won't be loaded), which may potentially break the reverse order.
		// That is fine, however, as in the worst case we just delete fewer tasks than we could have, but we will never delete
		// tasks that are still needed (all tasks deleted are those having an earlier scheduled time than what's needed).
		// Task deletion is just a best-effort optimization after all, so not complicating the logic to account for that here.
		chasmPureTasks []*tasks.ChasmTaskPure

		// Do not rely on this, this is only updated on
		// Load() and closeTransactionXXX methods. So when
		// a transaction is in progress, this value will be
		// wrong. This exists primarily for visibility via CLI.
		checksum *persistencespb.Checksum

		taskGenerator       TaskGenerator
		workflowTaskManager *workflowTaskStateMachine
		QueryRegistry       historyi.QueryRegistry

		shard                  historyi.ShardContext
		clusterMetadata        cluster.Metadata
		eventsCache            events.Cache
		config                 *configs.Config
		timeSource             clock.TimeSource
		logger                 log.Logger
		metricsHandler         metrics.Handler
		stateMachineNode       *hsm.Node
		subStateMachineDeleted bool

		// Tracks all events added via the AddHistoryEvent method that is used by the state machine framework.
		currentTransactionAddedStateMachineEventTypes []enumspb.EventType
	}

	lastUpdatedStateTransitionGetter interface {
		proto.Message
		GetLastUpdateVersionedTransition() *persistencespb.VersionedTransition
		Size() int
	}
)

var _ historyi.MutableState = (*MutableStateImpl)(nil)

func NewMutableState(
	shard historyi.ShardContext,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	startTime time.Time,
) *MutableStateImpl {

	namespaceName := namespaceEntry.Name().String()
	logger = log.NewLazyLogger(logger, func() []tag.Tag {
		return []tag.Tag{
			tag.WorkflowNamespace(namespaceName),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
		}
	})

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

		// This field will be initialized with a real chasm tree at the end of this function
		// when feature flag is enabled.
		chasmTree: &noopChasmTree{},

		approximateSize:              0,
		chasmNodeSizes:               make(map[string]int),
		totalTombstones:              0,
		currentVersion:               namespaceEntry.FailoverVersion(workflowID),
		bufferEventsInDB:             nil,
		stateInDB:                    enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
		nextEventIDInDB:              common.FirstEventID,
		dbRecordVersion:              1,
		namespaceEntry:               namespaceEntry,
		appliedEvents:                make(map[string]struct{}),
		InsertTasks:                  make(map[tasks.Category][]tasks.Task),
		BestEffortDeleteTasks:        make(map[tasks.Category][]tasks.Key),
		transitionHistoryEnabled:     shard.GetConfig().EnableTransitionHistory(namespaceName),
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
		ExecutionTime:          timestamppb.New(startTime),
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

		State:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime:  timestamppb.New(startTime),
		RequestIds: make(map[string]*persistencespb.RequestIDInfo),
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

	if s.config.EnableChasm(namespaceName) {
		s.chasmTree = chasm.NewEmptyTree(
			shard.ChasmRegistry(),
			shard.GetTimeSource(),
			s,
			chasm.DefaultPathEncoder,
			logger,
			shard.GetMetricsHandler(),
		)
	}

	return s
}

func NewMutableStateFromDB(
	shard historyi.ShardContext,
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

	// Track chasm node size even if chasm is not enabled,
	// because those nodes are still stored in the mutable state,
	// and should be taken into account when deciding if execution
	// should be terminated based on mutable state size.
	for key, node := range dbRecord.ChasmNodes {
		nodeSize := len(key) + node.Size()
		mutableState.approximateSize += nodeSize
		mutableState.chasmNodeSizes[key] = nodeSize
	}

	if shard.GetConfig().EnableChasm(namespaceEntry.Name().String()) {
		var err error
		mutableState.chasmTree, err = chasm.NewTreeFromDB(
			dbRecord.ChasmNodes,
			shard.ChasmRegistry(),
			shard.GetTimeSource(),
			mutableState,
			chasm.DefaultPathEncoder,
			mutableState.logger, // this logger is tagged with execution key.
			shard.GetMetricsHandler(),
		)
		if err != nil {
			return nil, err
		}
	}

	return mutableState, nil
}

func NewSanitizedMutableState(
	shard historyi.ShardContext,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	mutableStateRecord *persistencespb.WorkflowMutableState,
	lastWriteVersion int64,
) (*MutableStateImpl, error) {
	// Although new versions of temporal server will perform state sanitization,
	// we have to keep the sanitization logic here as well for backward compatibility in case
	// source cluster is running an old version and doesn't do the sanitization.
	SanitizeMutableState(mutableStateRecord)
	if err := common.DiscardUnknownProto(mutableStateRecord); err != nil {
		return nil, err
	}

	mutableState, err := NewMutableStateFromDB(shard, eventsCache, logger, namespaceEntry, mutableStateRecord, 1)
	if err != nil {
		return nil, err
	}

	mutableState.currentVersion = lastWriteVersion
	return mutableState, nil
}

func NewMutableStateInChain(
	shardContext historyi.ShardContext,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	startTime time.Time,
	currentMutableState historyi.MutableState,
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
	newMutableState.executionInfo.ChildrenInitializedPostResetPoint = currentMutableState.GetExecutionInfo().ChildrenInitializedPostResetPoint

	// TODO: Today other information like autoResetPoints, previousRunID, firstRunID, etc.
	// are carried over in AddWorkflowExecutionStartedEventWithOptions. Ideally all information
	// should be carried over here since some information is not part of the startedEvent.
	return newMutableState, nil
}

func (ms *MutableStateImpl) mustInitHSM() {
	if ms.executionInfo.SubStateMachinesByType == nil {
		ms.executionInfo.SubStateMachinesByType = make(map[string]*persistencespb.StateMachineMap)
	}

	// Error only occurs if some initialization path forgets to register the workflow state machine.
	stateMachineNode, err := hsm.NewRoot(ms.shard.StateMachineRegistry(), StateMachineType, ms, ms.executionInfo.SubStateMachinesByType, ms)
	if err != nil {
		panic(err)
	}
	ms.stateMachineNode = stateMachineNode
}

func (ms *MutableStateImpl) IsWorkflow() bool {
	return ms.chasmTree.ArchetypeID() == chasm.WorkflowArchetypeID
}

func (ms *MutableStateImpl) HSM() *hsm.Node {
	return ms.stateMachineNode
}

func (ms *MutableStateImpl) ChasmTree() historyi.ChasmTree {
	return ms.chasmTree
}

// ChasmEnabled returns true if the mutable state has a real chasm tree.
// The chasmTree is initialized with a noopChasmTree which is then overwritten with an actual chasm tree if chasm is
// enabled when the mutable state is created. Once the EnableChasm dynamic config is removed and the tree is always
// initialized, this helper can be removed.
func (ms *MutableStateImpl) ChasmEnabled() bool {
	_, isNoop := ms.chasmTree.(*noopChasmTree)
	return !isNoop
}

// chasmCallbacksEnabled returns true if CHASM callbacks are enabled for this workflow.
func (ms *MutableStateImpl) chasmCallbacksEnabled() bool {
	if !ms.ChasmEnabled() {
		return false
	}

	// Check the callback library's EnableCallbacks config via history config
	return ms.shard.GetConfig().EnableCHASMCallbacks(ms.GetNamespaceEntry().Name().String())
}

// ChasmWorkflowComponent gets the root workflow component from the CHASM tree.
// Returns the workflow component (which is *chasmworkflow.Workflow) and the CHASM mutable context.
// This method is for write operations. Callers can type assert to *chasmworkflow.Workflow if needed.
func (ms *MutableStateImpl) ChasmWorkflowComponent(ctx context.Context) (*chasmworkflow.Workflow, chasm.MutableContext, error) {
	chasmCtx := chasm.NewMutableContext(ctx, ms.chasmTree.(*chasm.Node))
	rootComponent, err := ms.chasmTree.ComponentByPath(chasmCtx, nil)
	if err != nil {
		return nil, nil, err
	}
	wf, ok := rootComponent.(*chasmworkflow.Workflow)
	if !ok {
		return nil, nil, serviceerror.NewInternalf("expected workflow component, but got %T", rootComponent)
	}
	return wf, chasmCtx, nil
}

func (ms *MutableStateImpl) ensureChasmWorkflowComponent(ctx context.Context) {
	// Initialize chasm tree once for new workflows.
	// Using context.Background() because this is done outside an actual request context and the
	// chasmworkflow.NewWorkflow does not actually use it currently.
	root, ok := ms.chasmTree.(*chasm.Node)
	softassert.That(ms.logger, ok, "chasmTree cast failed")

	if root.ArchetypeID() == chasm.UnspecifiedArchetypeID {
		mutableContext := chasm.NewMutableContext(ctx, root)
		if err := root.SetRootComponent(chasmworkflow.NewWorkflow(mutableContext, chasm.NewMSPointer(ms))); err != nil {
			softassert.Fail(ms.logger, "SetRootComponent failed", tag.Error(err))
		}
	}
}

// ChasmWorkflowComponentReadOnly gets the root workflow component from the CHASM tree.
// Returns both the workflow component and a read-only CHASM context.
// This method is for read-only operations.
func (ms *MutableStateImpl) ChasmWorkflowComponentReadOnly(ctx context.Context) (*chasmworkflow.Workflow, chasm.Context, error) {
	chasmCtx := chasm.NewContext(ctx, ms.chasmTree.(*chasm.Node))
	rootComponent, err := ms.chasmTree.ComponentByPath(chasmCtx, nil)
	if err != nil {
		return nil, nil, err
	}
	wf, ok := rootComponent.(*chasmworkflow.Workflow)
	if !ok {
		return nil, nil, serviceerror.NewInternalf("expected workflow component, but got %T", rootComponent)
	}
	return wf, chasmCtx, nil
}

// GetNexusCompletion converts a workflow completion event into a [nexus.OperationCompletion].
// Completions may be sent to arbitrary third parties, we intentionally do not include any termination reasons, and
// expose only failure messages.
func (ms *MutableStateImpl) GetNexusCompletion(
	ctx context.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	ce, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return nexusrpc.CompleteOperationOptions{}, err
	}

	// Create the link information about the workflow to be attached to fabricated started event if completion is
	// received before start response.
	link := &commonpb.Link_WorkflowEvent{
		Namespace:  ms.namespaceEntry.Name().String(),
		WorkflowId: ms.executionInfo.WorkflowId,
		RunId:      ms.executionState.RunId,
	}
	requestIDInfo := ms.executionState.RequestIds[requestID]
	// For a callback that is attached to a running workflow, we create a RequestIdReference link since the event may be
	// buffered and not have a final event ID yet.
	if requestIDInfo.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
		// If the callback was attached, then replace with RequestIdReference.
		link.Reference = &commonpb.Link_WorkflowEvent_RequestIdRef{
			RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
				RequestId: requestID,
				EventType: requestIDInfo.GetEventType(),
			},
		}
	} else {
		// For a callback that is attached on workflow start, we create an EventReference link.
		link.Reference = &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   common.FirstEventID,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		}
	}
	startLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(link)

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
		return nexusrpc.CompleteOperationOptions{
			Result:    p,
			StartTime: ms.executionState.GetStartTime().AsTime(),
			CloseTime: ce.GetEventTime().AsTime(),
			Links:     []nexus.Link{startLink},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		f, err := commonnexus.TemporalFailureToNexusFailure(ce.GetWorkflowExecutionFailedEventAttributes().GetFailure())
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opErr := &nexus.OperationError{
			Message: "operation failed",
			State:   nexus.OperationStateFailed,
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		return nexusrpc.CompleteOperationOptions{
			Error:     opErr,
			StartTime: ms.executionState.GetStartTime().AsTime(),
			CloseTime: ce.GetEventTime().AsTime(),
			Links:     []nexus.Link{startLink},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		f, err := commonnexus.TemporalFailureToNexusFailure(&failurepb.Failure{
			Message: "operation canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details: ce.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
				},
			},
		})
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opErr := &nexus.OperationError{
			State:   nexus.OperationStateCanceled,
			Message: "operation canceled",
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		return nexusrpc.CompleteOperationOptions{
			Error:     opErr,
			StartTime: ms.executionState.GetStartTime().AsTime(),
			CloseTime: ce.GetEventTime().AsTime(),
			Links:     []nexus.Link{startLink},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		f, err := commonnexus.TemporalFailureToNexusFailure(&failurepb.Failure{
			Message: "operation terminated",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		})
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opErr := &nexus.OperationError{
			State:   nexus.OperationStateFailed,
			Message: "operation failed",
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		return nexusrpc.CompleteOperationOptions{
			Error:     opErr,
			StartTime: ms.executionState.GetStartTime().AsTime(),
			CloseTime: ce.GetEventTime().AsTime(),
			Links:     []nexus.Link{startLink},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		f, err := commonnexus.TemporalFailureToNexusFailure(&failurepb.Failure{
			Message: "operation exceeded internal timeout",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					// Not filling in timeout type and other information, it's not particularly interesting to a Nexus
					// caller.
				},
			},
		})
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opErr := &nexus.OperationError{
			State:   nexus.OperationStateFailed,
			Message: "operation failed",
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		return nexusrpc.CompleteOperationOptions{
			Error:     opErr,
			StartTime: ms.executionState.GetStartTime().AsTime(),
			CloseTime: ce.GetEventTime().AsTime(),
			Links:     []nexus.Link{startLink},
		}, nil
	}
	return nexusrpc.CompleteOperationOptions{}, serviceerror.NewInternalf("invalid workflow execution status: %v", ce.GetEventType())
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
		ChasmNodes:          ms.chasmTree.Snapshot(nil).Nodes,
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

	archetypeID := ms.ChasmTree().ArchetypeID()
	if archetypeID != chasm.WorkflowArchetypeID {
		return softassert.UnexpectedInternalErr(
			ms.logger,
			"Backfilling history not supported for non-workflow archetype",
			nil,
			tag.ArchetypeID(archetypeID),
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
		)
	}

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
	ms.isResetStateUpdated = true
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

func (ms *MutableStateImpl) SetChildrenInitializedPostResetPoint(children map[string]*persistencespb.ResetChildInfo) {
	ms.executionInfo.ChildrenInitializedPostResetPoint = children
	ms.isResetStateUpdated = true
}

func (ms *MutableStateImpl) GetChildrenInitializedPostResetPoint() map[string]*persistencespb.ResetChildInfo {
	return ms.executionInfo.GetChildrenInitializedPostResetPoint()
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
		lastVersionedTransition := ms.CurrentVersionedTransition()
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
	// TODO: can we always return ms.currentVersion here?
	if ms.executionInfo.VersionHistories != nil {
		return ms.currentVersion
	}

	if ms.transitionHistoryEnabled && len(ms.executionInfo.TransitionHistory) != 0 {
		return ms.currentVersion
	}

	return common.EmptyVersion
}

// NextTransitionCount implements hsm.NodeBackend.
func (ms *MutableStateImpl) NextTransitionCount() int64 {
	if !ms.transitionHistoryEnabled {
		return 0
	}

	currentVersionedTransition := ms.CurrentVersionedTransition()
	if currentVersionedTransition == nil {
		// it is possible that this is the first transition and
		// transition history has not been updated yet.
		return 1
	}
	return currentVersionedTransition.TransitionCount + 1
}

func (ms *MutableStateImpl) GetStartVersion() (int64, error) {
	if ms.executionInfo.VersionHistories != nil {
		versionHistory, err := versionhistory.GetCurrentVersionHistory(ms.executionInfo.VersionHistories)
		if err != nil {
			return 0, err
		}

		if !versionhistory.IsEmptyVersionHistory(versionHistory) {
			firstItem, err := versionhistory.GetFirstVersionHistoryItem(versionHistory)
			if err != nil {
				return 0, err
			}
			return firstItem.GetVersion(), nil
		}
	}

	// We can't check TransitionHistory before VersionHistories because old workflows (mutable state) may
	// not have transition history enabled while they are running, so the first item in the transition history
	// is not the actual start version.
	//
	// However, this assumes that if mutable state has event, it must also generate an event in it's first transition.
	// That assumption is true today, but no necessarily true in the future. We should fix this if we ever
	// have such a case.
	if ms.transitionHistoryEnabled && len(ms.executionInfo.TransitionHistory) != 0 {
		return ms.executionInfo.TransitionHistory[0].GetNamespaceFailoverVersion(), nil
	}

	return common.EmptyVersion, nil
}

func (ms *MutableStateImpl) GetCloseVersion() (int64, error) {
	// TODO: Remove this special handling for zombie workflow.
	// This method should not be called for zombie workflow as it's not considered closed (though it's not running either).
	//
	// However, most callers in this codebase simply check if workflowIsRunning before calling this method, so we cloud reach here
	// even if the workflow is zombie.
	// Most callers are in task executor logic, and we should just prevent any task executor from running when workflow is in zombie state.
	if ms.executionState.State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return ms.GetLastWriteVersion()
	}

	// Do NOT use ms.IsWorkflowExecutionRunning() for the check.
	// Zombie workflow is not considered running but also not closed.
	if ms.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return common.EmptyVersion, serviceerror.NewInternalf("workflow still running, current state: %v", ms.executionState.State.String())
	}

	// if workflow is closing in the current transation,
	// then the last event is closed event and the event version is the close version
	if lastEventVersion, ok := ms.hBuilder.LastEventVersion(); ok {
		return lastEventVersion, nil
	}

	// We check version history first to prevserve the existing behaior of workflow to minimize risk.
	// However, this assumes that if mutable state has event, it must also generate an event upon closing.
	// That assumption is true today, but no necessarily true in the future. We should fix this if we ever
	// have such a case.
	if ms.executionInfo.VersionHistories != nil {
		isEmpty, err := versionhistory.IsCurrentVersionHistoryEmpty(ms.executionInfo.VersionHistories)
		if err != nil {
			return common.EmptyVersion, err
		}
		if !isEmpty {
			return ms.GetLastEventVersion()
		}
	}

	if ms.transitionHistoryEnabled {
		if ms.executionStateUpdated {
			// closing in the current transaction
			return ms.GetCurrentVersion(), nil
		}

		// once the mutable state is closed, the execution state will no longer be updated.
		// so the last update version is the close version.
		if ms.executionState.LastUpdateVersionedTransition != nil {
			return ms.executionState.LastUpdateVersionedTransition.NamespaceFailoverVersion, nil
		}
	}

	return common.EmptyVersion, nil
}

func (ms *MutableStateImpl) GetLastWriteVersion() (int64, error) {
	if ms.transitionHistoryEnabled && len(ms.executionInfo.TransitionHistory) != 0 {
		lastVersionedTransition := ms.CurrentVersionedTransition()
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

func (ms *MutableStateImpl) IsNonCurrentWorkflowGuaranteed() (bool, error) {
	switch ms.stateInDB {
	case enumsspb.WORKFLOW_EXECUTION_STATE_VOID:
		return true, nil
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED:
		return false, nil
	case enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return false, nil
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return false, nil
	case enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE:
		return true, nil
	case enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED:
		return false, nil
	default:
		return false, serviceerror.NewInternalf("unknown workflow state: %v", ms.executionState.State.String())
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

func (ms *MutableStateImpl) GetQueryRegistry() historyi.QueryRegistry {
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

	if payloadSize := request.Details.Size(); payloadSize > 0 {
		ms.metricsHandler.Counter(metrics.ActivityPayloadSize.Name()).Record(
			int64(payloadSize),
			metrics.OperationTag(metrics.HistoryRecordActivityTaskHeartbeatScope),
			metrics.NamespaceTag(ms.namespaceEntry.Name().String()))
	}
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
	ai.StartVersion = incomingActivityInfo.GetStartVersion()
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
	if incomingActivityInfo.RetryInitialInterval != nil {
		ai.RetryInitialInterval = incomingActivityInfo.GetRetryInitialInterval()
		ai.RetryMaximumInterval = incomingActivityInfo.GetRetryMaximumInterval()
		ai.RetryMaximumAttempts = incomingActivityInfo.GetRetryMaximumAttempts()
		ai.RetryBackoffCoefficient = incomingActivityInfo.GetRetryBackoffCoefficient()
	}

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
func (ms *MutableStateImpl) GetWorkflowTaskByID(scheduledEventID int64) *historyi.WorkflowTaskInfo {
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

func (ms *MutableStateImpl) GetPendingChildIds() map[int64]struct{} {
	ids := make(map[int64]struct{})
	for _, child := range ms.GetPendingChildExecutionInfos() {
		ids[child.InitiatedEventId] = struct{}{}
	}
	return ids
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

func (ms *MutableStateImpl) GetPendingWorkflowTask() *historyi.WorkflowTaskInfo {
	return ms.workflowTaskManager.GetPendingWorkflowTask()
}

func (ms *MutableStateImpl) HasStartedWorkflowTask() bool {
	return ms.workflowTaskManager.HasStartedWorkflowTask()
}

func (ms *MutableStateImpl) GetStartedWorkflowTask() *historyi.WorkflowTaskInfo {
	return ms.workflowTaskManager.GetStartedWorkflowTask()
}

func (ms *MutableStateImpl) IsTransientWorkflowTask() bool {
	return ms.executionInfo.WorkflowTaskAttempt > 1
}

func (ms *MutableStateImpl) ClearTransientWorkflowTask() error {
	if !ms.HasStartedWorkflowTask() {
		return softassert.UnexpectedInternalErr(
			ms.logger,
			"cannot clear transient workflow task when task is missing",
			nil,
		)
	}
	if !ms.IsTransientWorkflowTask() {
		return softassert.UnexpectedInternalErr(
			ms.logger,
			"cannot clear transient workflow task when task is not transient",
			nil,
		)
	}
	if ms.HasBufferedEvents() {
		return softassert.UnexpectedInternalErr(
			ms.logger,
			"cannot clear transient workflow task when there are buffered events",
			nil,
		)
	}
	// no buffered event
	emptyWorkflowTaskInfo := &historyi.WorkflowTaskInfo{
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

		SuggestContinueAsNew:        false,
		SuggestContinueAsNewReasons: nil,
		HistorySizeBytes:            0,
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

func (ms *MutableStateImpl) AttachRequestID(
	requestID string,
	eventType enumspb.EventType,
	eventID int64,
) {
	ms.approximateSize -= ms.executionState.Size()
	if ms.executionState.RequestIds == nil {
		ms.executionState.RequestIds = make(map[string]*persistencespb.RequestIDInfo, 1)
	}
	ms.executionState.RequestIds[requestID] = &persistencespb.RequestIDInfo{
		EventType: eventType,
		EventId:   eventID,
	}
	if eventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		ms.executionState.CreateRequestId = requestID
	}
	ms.approximateSize += ms.executionState.Size()
}

func (ms *MutableStateImpl) HasRequestID(
	requestID string,
) bool {
	if ms.executionState.RequestIds == nil {
		return false
	}
	_, ok := ms.executionState.RequestIds[requestID]
	return ok
}

func (ms *MutableStateImpl) addWorkflowExecutionStartedEventForContinueAsNew(
	ctx context.Context,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	execution *commonpb.WorkflowExecution,
	previousExecutionState historyi.MutableState,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
	firstRunID string,
	rootExecutionInfo *workflowspb.RootExecutionInfo,
	links []*commonpb.Link,
	IsWFTaskQueueInVersionDetector worker_versioning.IsWFTaskQueueInVersionDetector,
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

	completionCallbacks, err := getCompletionCallbacksAsProtoSlice(ctx, previousExecutionState)
	if err != nil {
		return nil, err
	}

	// If there is a pinned override, then the effective version will be the same as the pinned override version.
	// If this is a cross-TQ child, we don't want to ask matching the same question twice, so we re-use the result from
	// the first matching task-queue-in-version check.
	newTQInPinnedVersion := false

	// By default, the new run initiated by workflow ContinueAsNew of a Pinned run, will inherit the previous run's
	// version if the new run's Task Queue belongs to that version.
	// If the continue-as-new command says to use InitialVersioningBehavior AutoUpgrade, the new run will start as
	// AutoUpgrade in the first task and then assume the SDK-sent behavior on first workflow task completion.
	var inheritedPinnedVersion *deploymentpb.WorkerDeploymentVersion
	if previousExecutionState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED &&
		command.GetInitialVersioningBehavior() != enumspb.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE {
		inheritedPinnedVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(previousExecutionState.GetEffectiveDeployment())
		newTQ := command.GetTaskQueue().GetName()
		if newTQ != previousExecutionInfo.GetTaskQueue() {
			newTQInPinnedVersion, err = IsWFTaskQueueInVersionDetector(ctx, ms.GetNamespaceEntry().ID().String(), newTQ, inheritedPinnedVersion)
			if err != nil {
				return nil, fmt.Errorf("error determining child task queue presence in inherited version: %w", err)
			}
			if !newTQInPinnedVersion {
				inheritedPinnedVersion = nil
			}
		}
	}

	// Pinned override is inherited if Task Queue of new run is compatible with the override version.
	var pinnedOverride *workflowpb.VersioningOverride
	if o := previousExecutionInfo.GetVersioningInfo().GetVersioningOverride(); worker_versioning.OverrideIsPinned(o) {
		pinnedOverride = o
		newTQ := command.GetTaskQueue().GetName()
		if newTQ != previousExecutionInfo.GetTaskQueue() && !newTQInPinnedVersion {
			pinnedOverride = nil
		}
	}

	// New run initiated by ContinueAsNew of an AUTO_UPGRADE workflow execution will inherit the previous run's
	// deployment version and revision number iff the new run's Task Queue belongs to source deployment version.
	//
	// If the initiating workflow is PINNED and the continue-as-new command says to use InitialVersioningBehavior
	// AutoUpgrade, the new run will start as AutoUpgrade in the first task and then assume the SDK-sent behavior
	// after first workflow task completion.
	var sourceDeploymentVersion *deploymentpb.WorkerDeploymentVersion
	var sourceDeploymentRevisionNumber int64
	if previousExecutionState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE ||
		(previousExecutionState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED &&
			command.GetInitialVersioningBehavior() == enumspb.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE) {
		sourceDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(previousExecutionState.GetEffectiveDeployment())
		sourceDeploymentRevisionNumber = previousExecutionState.GetVersioningRevisionNumber()

		newTQ := command.GetTaskQueue().GetName()
		if newTQ != previousExecutionInfo.GetTaskQueue() {
			// Cross-TQ CAN: check if new TQ is in parent's deployment
			TQInSourceDeploymentVersion, err := IsWFTaskQueueInVersionDetector(
				ctx,
				ms.GetNamespaceEntry().ID().String(),
				newTQ,
				sourceDeploymentVersion,
			)
			if err != nil {
				return nil, fmt.Errorf("error determining CAN task queue presence in auto upgrade deployment: %w", err)
			}
			if !TQInSourceDeploymentVersion {
				sourceDeploymentVersion = nil
				sourceDeploymentRevisionNumber = 0
			}
		}
	}

	createRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
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
		CompletionCallbacks:   completionCallbacks,
		Links:                 links,
		Priority:              previousExecutionInfo.Priority,
	}

	enums.SetDefaultContinueAsNewInitiator(&command.Initiator)

	var sourceVersionStamp *commonpb.WorkerVersionStamp
	var inheritedBuildId string
	if command.InheritBuildId && previousExecutionState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// Do not set inheritedBuildId for v3 wfs
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
		InheritedPinnedVersion:   inheritedPinnedVersion,
		VersioningOverride:       pinnedOverride,
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

	// Add InheritedAutoUpgradeInfo if InheritedPinnedVersion is not set and source deployment version and revision number are set.
	if sourceDeploymentVersion != nil && sourceDeploymentRevisionNumber != 0 && inheritedPinnedVersion == nil {
		req.InheritedAutoUpgradeInfo = &deploymentpb.InheritedAutoUpgradeInfo{
			SourceDeploymentVersion:        sourceDeploymentVersion,
			SourceDeploymentRevisionNumber: sourceDeploymentRevisionNumber,
		}
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

	metrics.WorkflowContinueAsNewCount.With(
		ms.metricsHandler.WithTags(
			metrics.NamespaceTag(ms.namespaceEntry.Name().String()),
			metrics.VersioningBehaviorTag(previousExecutionState.GetEffectiveVersioningBehavior()),
			metrics.ContinueAsNewVersioningBehaviorTag(command.GetInitialVersioningBehavior()),
		),
	).Record(1)
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

	// Versioning Override set on StartWorkflowExecutionRequest
	if startRequest.GetStartRequest().GetVersioningOverride() != nil {
		metrics.WorkerDeploymentVersioningOverrideCounter.With(
			ms.metricsHandler.WithTags(
				metrics.NamespaceTag(ms.namespaceEntry.Name().String()),
				metrics.VersioningBehaviorBeforeOverrideTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
				metrics.VersioningBehaviorAfterOverrideTag(worker_versioning.ExtractVersioningBehaviorFromOverride(startRequest.GetStartRequest().GetVersioningOverride())),
				metrics.RunInitiatorTag(prevRunID, event.GetWorkflowExecutionStartedEventAttributes()),
			),
		).Record(1)
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
		return serviceerror.NewInternalf("applying conflicting namespace ID: %v != %v",
			ms.executionInfo.NamespaceId, ms.namespaceEntry.ID().String())
	}
	if ms.executionInfo.WorkflowId != execution.GetWorkflowId() {
		return serviceerror.NewInternalf("applying conflicting workflow ID: %v != %v",
			ms.executionInfo.WorkflowId, execution.GetWorkflowId())
	}
	if ms.executionState.RunId != execution.GetRunId() {
		return serviceerror.NewInternalf("applying conflicting run ID: %v != %v",
			ms.executionState.RunId, execution.GetRunId())
	}

	event := startEvent.GetWorkflowExecutionStartedEventAttributes()
	ms.AttachRequestID(requestID, startEvent.EventType, startEvent.EventId)

	ms.approximateSize -= ms.executionInfo.Size()
	ms.executionInfo.FirstExecutionRunId = event.GetFirstExecutionRunId()
	ms.executionInfo.TaskQueue = event.TaskQueue.GetName()
	ms.executionInfo.WorkflowTypeName = event.WorkflowType.GetName()
	ms.executionInfo.WorkflowRunTimeout = event.GetWorkflowRunTimeout()
	ms.executionInfo.WorkflowExecutionTimeout = event.GetWorkflowExecutionTimeout()
	ms.executionInfo.DefaultWorkflowTaskTimeout = event.GetWorkflowTaskTimeout()
	ms.executionInfo.OriginalExecutionRunId = event.GetOriginalExecutionRunId()

	ms.approximateSize -= ms.executionState.Size()
	if err := ms.addCompletionCallbacks(
		startEvent,
		requestID,
		event.GetCompletionCallbacks(),
	); err != nil {
		return err
	}
	if _, err := ms.UpdateWorkflowStateStatus(
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
		if ms.executionInfo.VersioningInfo == nil {
			ms.executionInfo.VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
		}
		ms.executionInfo.VersioningInfo.VersioningOverride = event.GetVersioningOverride()
		//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
		if d := event.GetVersioningOverride().GetDeployment(); d != nil { // v0.30 pinned
			// We read from both old and new fields but write in the new fields only.
			ms.executionInfo.VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(d),
				},
			}
			ms.executionInfo.VersioningInfo.VersioningOverride.Deployment = nil
			ms.executionInfo.VersioningInfo.VersioningOverride.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
		}
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if vs := event.GetVersioningOverride().GetPinnedVersion(); vs != "" {
			// We read from both old and new fields but write in the new fields only.
			ms.executionInfo.VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(vs),
				},
			}
			//nolint:staticcheck // SA1019: worker versioning v0.31
			ms.executionInfo.VersioningInfo.VersioningOverride.PinnedVersion = ""
			ms.executionInfo.VersioningInfo.VersioningOverride.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
		}
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if b := event.GetVersioningOverride().GetBehavior(); b == enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE {
			// We read from both old and new fields but write in the new fields only.
			ms.executionInfo.VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true}
			//nolint:staticcheck // SA1019: worker versioning v0.31
			ms.executionInfo.VersioningInfo.VersioningOverride.Behavior = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
		}
	}

	if event.GetInheritedPinnedVersion() != nil {
		if ms.executionInfo.VersioningInfo == nil {
			ms.executionInfo.VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
		}
		ms.executionInfo.VersioningInfo.DeploymentVersion = event.GetInheritedPinnedVersion()
		ms.executionInfo.VersioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED
	}

	// Populate the versioningInfo if the inheritedAutoUpgradeInfo is present.
	if event.GetInheritedAutoUpgradeInfo() != nil {
		ms.SetVersioningRevisionNumber(event.GetInheritedAutoUpgradeInfo().GetSourceDeploymentRevisionNumber())
		// TODO (Shivam): Remove this once you make SetDeploymentVersion and SetVersioningBehavior methods with nil checks
		if ms.executionInfo.VersioningInfo == nil {
			ms.executionInfo.VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
		}
		ms.executionInfo.VersioningInfo.DeploymentVersion = event.GetInheritedAutoUpgradeInfo().GetSourceDeploymentVersion()
		// Assume AutoUpgrade behavior for the first workflow task.
		ms.executionInfo.VersioningInfo.Behavior = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	}

	if inheritedBuildId := event.InheritedBuildId; inheritedBuildId != "" {
		ms.executionInfo.InheritedBuildId = inheritedBuildId
		if err := ms.UpdateBuildIdAssignment(inheritedBuildId); err != nil {
			return err
		}
	} else if event.SourceVersionStamp.GetUseVersioning() && event.SourceVersionStamp.GetBuildId() != "" ||
		ms.GetEffectiveVersioningBehavior() != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// TODO: [cleanup-old-wv]
		limit := ms.config.SearchAttributesSizeOfValueLimit(string(ms.namespaceEntry.Name()))
		// Passing nil for usedVersion because starting with pinned override does not add the version to used versions SA until the version is actaully used.
		//nolint:staticcheck // SA1019
		if _, err := ms.addBuildIDAndDeploymentInfoToSearchAttributesWithNoVisibilityTask(event.SourceVersionStamp, nil, limit); err != nil {
			return err
		}
	}

	// This will include override and inheritance, but not transition, because WF never starts with a transition
	ms.executionInfo.WorkerDeploymentName = ms.GetEffectiveDeployment().GetSeriesName()

	if inheritedBuildId := event.InheritedBuildId; inheritedBuildId != "" {
		ms.executionInfo.InheritedBuildId = inheritedBuildId
		if err := ms.UpdateBuildIdAssignment(inheritedBuildId); err != nil {
			return err
		}
	}

	ms.executionInfo.MostRecentWorkerVersionStamp = event.SourceVersionStamp
	ms.executionInfo.Priority = event.Priority

	ms.approximateSize += ms.executionInfo.Size()
	ms.approximateSize += ms.executionState.Size()

	ms.writeEventToCache(startEvent)
	return nil
}

func (ms *MutableStateImpl) IsWorkflowExecutionStatusPaused() bool {
	return ms.executionState.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
}

func (ms *MutableStateImpl) AddWorkflowExecutionPausedEvent(
	identity string,
	reason string,
	requestID string,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowPaused
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddWorkflowExecutionPausedEvent(identity, reason, requestID)
	if err := ms.ApplyWorkflowExecutionPausedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

// ApplyWorkflowExecutionPausedEvent applies the paused event to the mutable state. It updates the workflow execution status to paused and sets the pause info.
func (ms *MutableStateImpl) ApplyWorkflowExecutionPausedEvent(event *historypb.HistoryEvent) error {
	// Update workflow status.
	if _, err := ms.UpdateWorkflowStateStatus(ms.executionState.GetState(), enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED); err != nil {
		return err
	}
	// Set pause info in mutable state.
	ms.executionInfo.PauseInfo = &persistencespb.WorkflowPauseInfo{
		PauseTime: timestamppb.New(event.GetEventTime().AsTime()),
		Identity:  event.GetWorkflowExecutionPausedEventAttributes().GetIdentity(),
		Reason:    event.GetWorkflowExecutionPausedEventAttributes().GetReason(),
		RequestId: event.GetWorkflowExecutionPausedEventAttributes().GetRequestId(),
	}

	// Update approximate size of the mutable state. This will be decreased when the pause info is removed (when the workflow is unpaused)
	ms.approximateSize += ms.executionInfo.PauseInfo.Size()

	// Invalidate all the pending activities. Do not mark individual activities as paused.
	for _, ai := range ms.GetPendingActivityInfos() {
		if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
			activityInfo.Stamp = activityInfo.Stamp + 1
			return nil
		}); err != nil {
			return err
		}
	}

	// Invalidate pending workflow task by incrementing the persisted stamp.
	// This ensures subsequent task dispatch detects the change.
	if ms.HasPendingWorkflowTask() {
		ms.executionInfo.WorkflowTaskStamp += 1
		ms.workflowTaskManager.UpdateWorkflowTask(ms.GetPendingWorkflowTask())
	}

	return ms.updatePauseInfoSearchAttribute()
}

func (ms *MutableStateImpl) AddWorkflowExecutionUnpausedEvent(
	identity string,
	reason string,
	requestID string,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowUnpaused
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	event := ms.hBuilder.AddWorkflowExecutionUnpausedEvent(identity, reason, requestID)
	if err := ms.ApplyWorkflowExecutionUnpausedEvent(event); err != nil {
		return nil, err
	}
	return event, nil
}

// ApplyWorkflowExecutionUnpausedEvent applies the unpaused event to the mutable state. It updates the workflow execution status to running and clears the pause info.
func (ms *MutableStateImpl) ApplyWorkflowExecutionUnpausedEvent(event *historypb.HistoryEvent) error {
	// Update workflow status.
	if _, err := ms.UpdateWorkflowStateStatus(ms.executionState.GetState(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING); err != nil {
		return err
	}

	// save pauseInfoSize before clearing so that we can adjust approximate size later before returning success
	pauseInfoSize := 0
	if ms.executionInfo.PauseInfo != nil {
		pauseInfoSize = ms.GetExecutionInfo().GetPauseInfo().Size()
		// Clear pause info in mutable state.
		ms.executionInfo.PauseInfo = nil
	}

	// Reschedule any pending activities
	// Note: workflow task is scheduled in the unpause API. So no need to schedule it here.
	for _, ai := range ms.GetPendingActivityInfos() {
		// Bump activity stamp to force replication so that the passive cluster can recreate the activity task.
		if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
			activityInfo.Stamp = activityInfo.Stamp + 1
			return nil
		}); err != nil {
			return err
		}

		// Check activity scheduled time and generate activity retry task if scheduled time is in the future.
		if ai.GetScheduledTime().AsTime().After(ms.timeSource.Now().UTC()) {
			if err := ms.taskGenerator.GenerateActivityRetryTasks(ai); err != nil {
				return err
			}
		} else {
			// Generate activity task to resend the activity to matching immediately.
			if err := ms.taskGenerator.GenerateActivityTasks(ai.ScheduledEventId); err != nil {
				return err
			}
		}
	}

	// Update approximate size of the mutable state.
	ms.approximateSize -= pauseInfoSize

	return ms.updatePauseInfoSearchAttribute()
}

func (ms *MutableStateImpl) addCompletionCallbacks(
	event *historypb.HistoryEvent,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	if len(completionCallbacks) == 0 {
		return nil
	}
	if ms.chasmCallbacksEnabled() {
		// Initialize chasm tree once for new workflows.
		// Using context.Background() because this is done outside an actual request context and the
		// chasmworkflow.NewWorkflow does not actually use it currently.
		ms.ensureChasmWorkflowComponent(context.Background())
		return ms.addCompletionCallbacksChasm(event, requestID, completionCallbacks)
	}

	return ms.addCompletionCallbacksHsm(event, requestID, completionCallbacks)
}

// addCompletionCallbacksHsm creates completion callbacks using the HSM implementation.
func (ms *MutableStateImpl) addCompletionCallbacksHsm(
	event *historypb.HistoryEvent,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	coll := callbacks.MachineCollection(ms.HSM())
	maxCallbacksPerWorkflow := ms.config.MaxCallbacksPerWorkflow(ms.GetNamespaceEntry().Name().String())
	if len(completionCallbacks)+coll.Size() > maxCallbacksPerWorkflow {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to a workflow (%d callbacks already attached)",
			maxCallbacksPerWorkflow,
			coll.Size(),
		)
	}
	for idx, cb := range completionCallbacks {
		persistenceCB := &persistencespb.Callback{
			Links: cb.GetLinks(),
		}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			persistenceCB.Variant = &persistencespb.Callback_Nexus_{
				Nexus: &persistencespb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			return serviceerror.NewInvalidArgumentf("unknown callback variant: %T", cb.Variant)
		}
		machine := callbacks.NewCallback(requestID, event.EventTime, callbacks.NewWorkflowClosedTrigger(), persistenceCB)
		id := ""
		// This is for backwards compatibility: callbacks were initially only attached when the workflow
		// execution started, but now they can be attached while the workflow is running.
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			// Use the start event version and ID as part of the callback ID to ensure that callbacks have unique
			// IDs that are deterministically created across clusters.
			id = fmt.Sprintf("%d-%d-%d", event.GetVersion(), event.GetEventId(), idx)
		} else {
			id = fmt.Sprintf("cb-%s-%d", requestID, idx)
		}
		if _, err := coll.Add(id, machine); err != nil {
			return err
		}
	}
	return nil
}

// addCompletionCallbacksChasm creates completion callbacks using the CHASM implementation.
func (ms *MutableStateImpl) addCompletionCallbacksChasm(
	event *historypb.HistoryEvent,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	wf, ctx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		return err
	}

	maxCallbacksPerWorkflow := ms.config.MaxCHASMCallbacksPerWorkflow(ms.GetNamespaceEntry().Name().String())
	return wf.AddCompletionCallbacks(ctx, event.EventTime, requestID, completionCallbacks, maxCallbacksPerWorkflow)
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
) (*historyi.WorkflowTaskInfo, error) {
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
) (*historyi.WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp, workflowTaskType)
}

func (ms *MutableStateImpl) ApplyTransientWorkflowTaskScheduled() (*historyi.WorkflowTaskInfo, error) {
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
) (*historyi.WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ApplyWorkflowTaskScheduledEvent(version, scheduledEventID, taskQueue, startToCloseTimeout, attempt, scheduleTimestamp, originalScheduledTimestamp, workflowTaskType)
}

func (ms *MutableStateImpl) AddWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	taskQueue *taskqueuepb.TaskQueue,
	identity string,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
	updateReg update.Registry,
	skipVersioningCheck bool,
	targetDeploymentVersion *deploymentpb.WorkerDeploymentVersion,
) (*historypb.HistoryEvent, *historyi.WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskStartedEvent(scheduledEventID, requestID, taskQueue, identity, versioningStamp, redirectInfo, skipVersioningCheck, updateReg, targetDeploymentVersion)
}

func (ms *MutableStateImpl) ApplyWorkflowTaskStartedEvent(
	workflowTask *historyi.WorkflowTaskInfo,
	version int64,
	scheduledEventID int64,
	startedEventID int64,
	requestID string,
	timestamp time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
	suggestContinueAsNewReasons []enumspb.SuggestContinueAsNewReason,
	targetWorkerDeploymentVersionChanged bool,
) (*historyi.WorkflowTaskInfo, error) {
	return ms.workflowTaskManager.ApplyWorkflowTaskStartedEvent(workflowTask, version, scheduledEventID,
		startedEventID, requestID, timestamp, suggestContinueAsNew, historySizeBytes, versioningStamp, redirectCounter,
		suggestContinueAsNewReasons, targetWorkerDeploymentVersionChanged)
}

// TODO (alex-update): 	Transient needs to be renamed to "TransientOrSpeculative"
func (ms *MutableStateImpl) GetTransientWorkflowTaskInfo(
	workflowTask *historyi.WorkflowTaskInfo,
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
	if proto.Equal(exeInfo.SearchAttributes[sadefs.BinaryChecksums], checksumsPayload) {
		return nil // unchanged
	}
	ms.updateSearchAttributes(map[string]*commonpb.Payload{sadefs.BinaryChecksums: checksumsPayload})
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

	// Re-scheduling pending workflow and activity tasks.
	err = ms.reschedulePendingWorkflowTask()
	if err != nil {
		return err
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

		// need to update stamp so the passive side regenerate the task
		err := ms.UpdateActivity(ai.ScheduledEventId, func(info *persistencespb.ActivityInfo, state historyi.MutableState) error {
			info.Stamp++
			return nil
		})
		if err != nil {
			return err
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
	return ms.updateBuildIdsAndDeploymentSearchAttributes(&commonpb.WorkerVersionStamp{UseVersioning: true, BuildId: buildId}, nil, limit)
}

// Sets TemporalWorkerDeployment to the override DeploymentName if present, or`ms.executionInfo.WorkerDeploymentName`.
// Sets TemporalWorkerDeploymentVersion to the override PinnedVersion if present, or `ms.executionInfo.VersioningInfo.Version`.
// Sets TemporalWorkflowVersioningBehavior to the override Behavior if present, or `ms.executionInfo.VersioningInfo.Behavior` if specified.
//
// For pinned workflows using WorkerDeployment APIs (ms.GetEffectiveVersioningBehavior() == PINNED &&
// ms.executionInfo.VersioningInfo.Version != ""), this will append a tag formed as `pinned:<version>`
// to the BuildIds search attribute, if it does not already exist there. The version used will be the
// effective version of the workflow (aka, the override version if override is set).
//
// If deprecated Deployment-based APIs are in use and the workflow is pinned, `pinned:<deployment_series_name>:<deployment_build_id>`
// will be appended to the BuilIds list if it is not already present. The deployment will be
// the effective deployment of the workflow (aka the override deployment_series and build_id if set).
//
// For all other workflows (ms.GetEffectiveVersioningBehavior() != PINNED), this will append a tag  to BuildIds
// based on the workflow's versioning status.
func (ms *MutableStateImpl) updateBuildIdsAndDeploymentSearchAttributes(
	stamp *commonpb.WorkerVersionStamp,
	usedVersion *deploymentpb.WorkerDeploymentVersion,
	maxSearchAttributeValueSize int,
) error {
	changed, err := ms.addBuildIDAndDeploymentInfoToSearchAttributesWithNoVisibilityTask(stamp, usedVersion, maxSearchAttributeValueSize)
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
	saPayload, found := searchAttributes[sadefs.BuildIds]
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

func (ms *MutableStateImpl) loadSearchAttributeString(saName string) (string, error) {
	searchAttributes := ms.executionInfo.SearchAttributes
	if searchAttributes == nil {
		return "", nil
	}
	saPayload, found := searchAttributes[saName]
	if !found {
		return "", nil
	}
	decoded, err := searchattribute.DecodeValue(saPayload, enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
	if err != nil {
		return "", err
	}
	if decoded == "" {
		return "", nil
	}
	val, ok := decoded.(string)
	if !ok {
		return "", serviceerror.NewInternalf("invalid search attribute value stored for %s", saName)
	}
	return val, nil
}

func (ms *MutableStateImpl) loadUsedDeploymentVersions() ([]string, error) {
	searchAttributes := ms.executionInfo.SearchAttributes
	if searchAttributes == nil {
		return []string{}, nil
	}
	saPayload, found := searchAttributes[sadefs.TemporalUsedWorkerDeploymentVersions]
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
	usedDeploymentVersions, ok := decoded.([]string)
	if !ok {
		return nil, serviceerror.NewInternal("invalid search attribute value stored for TemporalUsedWorkerDeploymentVersions")
	}
	return usedDeploymentVersions, nil
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
	var buildId string

	behavior := ms.GetWorkflowVersioningBehaviorSA()

	// set up the unversioned or assigned:x sentinels (versioning v2)
	if !stamp.GetUseVersioning() && behavior == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED { // unversioned workflows may still have non-nil deployment, so we don't check deployment
		newValues = append(newValues, worker_versioning.UnversionedSearchAttribute)
	} else if ms.GetAssignedBuildId() != "" && behavior == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		newValues = append(newValues, worker_versioning.AssignedBuildIdSearchAttribute(ms.GetAssignedBuildId()))
	}

	// get the most up-to-date pinned entry put it at the front (v3 reachability and v3.1 drainage)
	if behavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
		newValues = append(newValues, worker_versioning.PinnedBuildIdSearchAttribute(ms.GetWorkerDeploymentVersionSA()))
	}

	// get the build id entry (all versions of versioning)
	if stamp != nil {
		buildId = worker_versioning.VersionStampToBuildIdSearchAttribute(stamp)
	}

	// add all previous values except for unversioned, assigned, or pinned (there can only be one, and we just added it)
	foundBuildId := false
	for _, existingValue := range existingValues {
		if existingValue == buildId {
			foundBuildId = true
		}
		if !worker_versioning.IsUnversionedOrAssignedBuildIdSearchAttribute(existingValue) &&
			!strings.HasPrefix(existingValue, worker_versioning.BuildIdSearchAttributePrefixPinned) {
			newValues = append(newValues, existingValue)
		}
	}

	// add buildId to the list only if it wasn't there before
	if !foundBuildId && buildId != "" {
		newValues = append(newValues, buildId)
	}
	return newValues
}

func (ms *MutableStateImpl) addUsedDeploymentVersionToLoadedSearchAttribute(existingValues []string, usedVersion *deploymentpb.WorkerDeploymentVersion) []string {
	if usedVersion == nil {
		return existingValues
	}

	// Get the current deployment version string (already formatted via ExternalWorkerDeploymentVersionToString)
	deploymentVersionStr := worker_versioning.ExternalWorkerDeploymentVersionToString(usedVersion)

	// Skip empty deployment versions (unversioned workflows don't get tracked)
	if deploymentVersionStr == "" {
		return existingValues
	}

	// Check if already exists (deduplicate)
	for _, existingValue := range existingValues {
		if existingValue == deploymentVersionStr {
			return existingValues // Already present, no change
		}
	}

	// Add new deployment version to the list
	newValues := make([]string, 0, len(existingValues)+1)
	newValues = append(newValues, existingValues...)
	newValues = append(newValues, deploymentVersionStr)
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
			ms.updateSearchAttributes(map[string]*commonpb.Payload{sadefs.BuildIds: saPayload})
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

func (ms *MutableStateImpl) saveUsedDeploymentVersions(usedDeploymentVersions []string, maxSearchAttributeValueSize int) error {
	searchAttributes := ms.executionInfo.SearchAttributes
	if searchAttributes == nil {
		searchAttributes = make(map[string]*commonpb.Payload, 1)
		ms.executionInfo.SearchAttributes = searchAttributes
	}

	for {
		saPayload, err := searchattribute.EncodeValue(usedDeploymentVersions, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
		if err != nil {
			return err
		}
		if len(usedDeploymentVersions) == 0 || len(saPayload.GetData()) <= maxSearchAttributeValueSize {
			ms.updateSearchAttributes(map[string]*commonpb.Payload{
				sadefs.TemporalUsedWorkerDeploymentVersions: saPayload,
			})
			break
		}
		// If too large, remove oldest entries
		if len(usedDeploymentVersions) == 1 {
			usedDeploymentVersions = make([]string, 0)
		} else {
			usedDeploymentVersions = usedDeploymentVersions[1:] // Remove oldest
		}
	}
	return nil
}

// Note: If the encoding for one of these strings fails, none of them would get saved. But we really
// don't expect any of the strings to be unencodable, so I think the all-or-nothing method is worth it
// so that we can merge the SearchAttributes map only once instead of three times.
func (ms *MutableStateImpl) saveDeploymentSearchAttributes(deployment, version, behavior string, maxSearchAttributeValueSize int) error {
	saPayloads := make(map[string]*commonpb.Payload)
	if deployment == "" {
		saPayloads[sadefs.TemporalWorkerDeployment] = nil
	} else {
		deploymentPayload, err := searchattribute.EncodeValue(deployment, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
		if err != nil {
			return err
		}
		if len(deploymentPayload.GetData()) <= maxSearchAttributeValueSize { // we know the string won't really be over, but still check
			saPayloads[sadefs.TemporalWorkerDeployment] = deploymentPayload
		}
	}
	if version == "" {
		saPayloads[sadefs.TemporalWorkerDeploymentVersion] = nil
	} else {
		saPayloads[sadefs.TemporalWorkerDeploymentVersion] = nil
		versionPayload, err := searchattribute.EncodeValue(version, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
		if err != nil {
			return err
		}
		if len(versionPayload.GetData()) <= maxSearchAttributeValueSize { // we know the string won't really be over, but still check
			saPayloads[sadefs.TemporalWorkerDeploymentVersion] = versionPayload
		}
	}
	if behavior == "" {
		saPayloads[sadefs.TemporalWorkflowVersioningBehavior] = nil
	} else {
		behaviorPayload, err := searchattribute.EncodeValue(behavior, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
		if err != nil {
			return err
		}
		if len(behaviorPayload.GetData()) <= maxSearchAttributeValueSize { // we know the string won't really be over, but still check
			saPayloads[sadefs.TemporalWorkflowVersioningBehavior] = behaviorPayload
		}
	}
	ms.updateSearchAttributes(saPayloads)
	return nil
}

func (ms *MutableStateImpl) addBuildIDAndDeploymentInfoToSearchAttributesWithNoVisibilityTask(
	stamp *commonpb.WorkerVersionStamp,
	usedVersion *deploymentpb.WorkerDeploymentVersion,
	maxSearchAttributeValueSize int,
) (bool, error) {
	// get all the existing SAs
	existingBuildIds, err := ms.loadBuildIds()
	if err != nil {
		return false, err
	}
	existingUsedDeploymentVersions, err := ms.loadUsedDeploymentVersions()
	if err != nil {
		return false, err
	}
	existingDeployment, err := ms.loadSearchAttributeString(sadefs.TemporalWorkerDeployment)
	if err != nil {
		return false, err
	}
	existingVersion, err := ms.loadSearchAttributeString(sadefs.TemporalWorkerDeploymentVersion)
	if err != nil {
		return false, err
	}
	existingBehavior, err := ms.loadSearchAttributeString(sadefs.TemporalWorkflowVersioningBehavior)
	if err != nil {
		return false, err
	}

	// modify them
	modifiedBuildIds := ms.addBuildIdToLoadedSearchAttribute(existingBuildIds, stamp)
	modifiedUsedDeploymentVersions := ms.addUsedDeploymentVersionToLoadedSearchAttribute(existingUsedDeploymentVersions, usedVersion)
	modifiedDeployment := ms.GetWorkerDeploymentSA()
	modifiedVersion := ms.GetWorkerDeploymentVersionSA()
	modifiedBehavior := ""
	if b := ms.GetWorkflowVersioningBehaviorSA(); b != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		modifiedBehavior = b.String()
	}

	// check equality
	if slices.Equal(existingBuildIds, modifiedBuildIds) &&
		slices.Equal(existingUsedDeploymentVersions, modifiedUsedDeploymentVersions) &&
		existingDeployment == modifiedDeployment &&
		existingVersion == modifiedVersion &&
		existingBehavior == modifiedBehavior {
		return false, nil
	}

	// save build ids if changed
	if !slices.Equal(existingBuildIds, modifiedBuildIds) {
		err = ms.saveBuildIds(modifiedBuildIds, maxSearchAttributeValueSize)
		if err != nil {
			return false, err // if err != nil, nothing will be written
		}
	}

	// save used deployment versions if changed
	if !slices.Equal(existingUsedDeploymentVersions, modifiedUsedDeploymentVersions) {
		err = ms.saveUsedDeploymentVersions(modifiedUsedDeploymentVersions, maxSearchAttributeValueSize)
		if err != nil {
			return false, err // if err != nil, nothing will be written
		}
	}

	// save deployment search attributes if changed
	if !(existingDeployment == modifiedDeployment &&
		existingVersion == modifiedVersion &&
		existingBehavior == modifiedBehavior) {
		err = ms.saveDeploymentSearchAttributes(modifiedDeployment, modifiedVersion, modifiedBehavior, maxSearchAttributeValueSize)
		if err != nil {
			return false, err // if err != nil, nothing will be written
		}
	}
	return true, nil
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
	workflowTask *historyi.WorkflowTaskInfo,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	limits historyi.WorkflowTaskCompletionLimits,
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
	workflowTask *historyi.WorkflowTaskInfo,
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
	workflowTask *historyi.WorkflowTaskInfo,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if err := ms.checkMutability(opTag); err != nil {
		return nil, err
	}
	return ms.workflowTaskManager.AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask)
}

func (ms *MutableStateImpl) AddWorkflowTaskFailedEvent(
	workflowTask *historyi.WorkflowTaskInfo,
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

	event := ms.hBuilder.AddActivityTaskScheduledEvent(workflowTaskCompletedEventID, command, ms.namespaceEntry.Name())
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
		StartVersion:            common.EmptyVersion,
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
		Priority:                attributes.Priority,
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

	if deployment != nil {
		ai.LastWorkerDeploymentVersion = worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment))
		ai.LastDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment)
	}

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

	if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		// we might need to retry, so do not append started event just yet,
		// instead update mutable state and will record started event when activity task is closed
		activityInfo.Version = ms.GetCurrentVersion()
		activityInfo.StartedEventId = common.TransientEventID
		activityInfo.StartVersion = ms.GetCurrentVersion()
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
	ai.StartVersion = event.GetVersion()
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
			tag.Bool("hasActivityInfo", ok),
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
		ms.namespaceEntry.Name(),
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
			tag.Bool("hasActivityInfo", ok),
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
		ms.namespaceEntry.Name(),
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
			tag.Bool("hasActivityInfo", ok),
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
				tag.Bool("hasActivityInfo", ok),
				tag.WorkflowScheduledEventID(scheduledEventID))

			return nil, nil, ms.createCallerError(opTag, fmt.Sprintf("ScheduledEventID: %d", scheduledEventID))
		}
	}

	// Check for duplicate cancellation
	if ok && ai.CancelRequested {
		ms.logWarn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.Bool("hasActivityInfo", ok),
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
		false, // skipCloseTransferTask
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionCompletedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if _, err := ms.UpdateWorkflowStateStatus(
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
		false, // skipCloseTransferTask
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionFailedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if _, err := ms.UpdateWorkflowStateStatus(
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

	attrs := event.GetWorkflowExecutionFailedEventAttributes()
	if attrs.RetryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return ms.processCloseCallbacks()
	}
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
	if err := ms.ApplyWorkflowExecutionTimedoutEvent(firstEventID, event); err != nil {
		return nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateWorkflowCloseTasks(
		event.GetEventTime().AsTime(),
		false,
		false, // skipCloseTransferTask
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionTimedoutEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if _, err := ms.UpdateWorkflowStateStatus(
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

	attrs := event.GetWorkflowExecutionTimedOutEventAttributes()
	if attrs.RetryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return ms.processCloseCallbacks()
	}
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
			tag.Bool("cancelRequested", ms.executionInfo.CancelRequested),
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
		false, // skipCloseTransferTask
	); err != nil {
		return nil, err
	}
	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionCanceledEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if _, err := ms.UpdateWorkflowStateStatus(
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
		false, // skipCloseTransferTask
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

func (ms *MutableStateImpl) DeleteSubStateMachine(path *persistencespb.StateMachinePath) error {
	incomingPath := make([]hsm.Key, len(path.Path))
	for i, p := range path.Path {
		incomingPath[i] = hsm.Key{Type: p.Type, ID: p.Id}
	}

	root := ms.HSM()
	node, err := root.Child(incomingPath)
	if err != nil {
		if !errors.Is(err, hsm.ErrStateMachineNotFound) {
			return err
		}
		// node is already deleted.
		return nil
	}
	err = node.Parent.DeleteChild(node.Key)
	if err != nil {
		return err
	}
	ms.subStateMachineDeleted = true
	return nil
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
		return serviceerror.NewInternalf("Update ID %s is already present in mutable state", updateID)
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
	identity string,
	priority *commonpb.Priority,
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
		identity,
		priority,
	)
	prevEffectiveVersioningBehavior := ms.GetEffectiveVersioningBehavior()
	prevEffectiveDeployment := ms.GetEffectiveDeployment()

	if err := ms.ApplyWorkflowExecutionOptionsUpdatedEvent(event); err != nil {
		return nil, err
	}

	if !proto.Equal(ms.GetEffectiveDeployment(), prevEffectiveDeployment) ||
		ms.GetEffectiveVersioningBehavior() != prevEffectiveVersioningBehavior {
		metrics.WorkerDeploymentVersioningOverrideCounter.With(
			ms.metricsHandler.WithTags(
				metrics.NamespaceTag(ms.namespaceEntry.Name().String()),
				metrics.VersioningBehaviorBeforeOverrideTag(prevEffectiveVersioningBehavior),
				metrics.VersioningBehaviorAfterOverrideTag(ms.GetEffectiveVersioningBehavior()),
				metrics.RunInitiatorTag("", event.GetWorkflowExecutionStartedEventAttributes()),
			),
		).Record(1)
	}

	return event, nil
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionOptionsUpdatedEvent(event *historypb.HistoryEvent) error {
	attributes := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()

	// Update versioning.
	var err error
	var requestReschedulePendingWorkflowTask bool
	if attributes.GetUnsetVersioningOverride() {
		requestReschedulePendingWorkflowTask, err = ms.updateVersioningOverride(nil)
	} else if attributes.GetVersioningOverride() != nil {
		requestReschedulePendingWorkflowTask, err = ms.updateVersioningOverride(attributes.GetVersioningOverride())
	}
	if err != nil {
		return err
	}

	// Update attached request ID.
	if attributes.GetAttachedRequestId() != "" {
		ms.AttachRequestID(attributes.GetAttachedRequestId(), event.EventType, event.EventId)
	}

	// Update completion callbacks.
	if err := ms.addCompletionCallbacks(
		event,
		attributes.GetAttachedRequestId(),
		attributes.GetAttachedCompletionCallbacks(),
	); err != nil {
		return err
	}

	// Update priority.
	if attributes.GetPriority() != nil {
		if !proto.Equal(ms.executionInfo.Priority, attributes.GetPriority()) {
			requestReschedulePendingWorkflowTask = true
		}
		ms.executionInfo.Priority = attributes.GetPriority()
	}

	// Finally, reschedule the pending workflow task if so requested.
	if requestReschedulePendingWorkflowTask {
		return ms.reschedulePendingWorkflowTask()
	}
	return nil
}

func (ms *MutableStateImpl) updateVersioningOverride(
	override *workflowpb.VersioningOverride,
) (bool, error) {
	previousEffectiveDeployment := ms.GetEffectiveDeployment()
	previousEffectiveVersioningBehavior := ms.GetEffectiveVersioningBehavior()
	var requestReschedulePendingWorkflowTask bool

	if override != nil {
		if ms.GetExecutionInfo().GetVersioningInfo() == nil {
			ms.GetExecutionInfo().VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
		}
		ms.GetExecutionInfo().VersioningInfo.VersioningOverride = &workflowpb.VersioningOverride{
			Override: override.GetOverride(),
		}
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if override.GetPinnedVersion() != "" {
			// If the old Pinned Version field was populated instead of VersioningOverride_Pinned,
			// we read from both old and new fields but write in the new fields only.
			ms.GetExecutionInfo().VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					//nolint:staticcheck // SA1019: worker versioning v0.31
					Version:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(override.GetPinnedVersion()),
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				},
			}
		}

		//nolint:staticcheck // SA1019: worker versioning v0.31
		if override.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE {
			// If the old behavior field was set to auto upgrade instead of VersioningOverride_AutoUpgrade,
			// we read from both old and new fields but write in the new fields only.
			ms.GetExecutionInfo().VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_AutoUpgrade{
				AutoUpgrade: true,
			}
		}

		//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
		if d := override.GetDeployment(); d != nil { // v0.30 pinned
			// We read from both old and new fields but write in the new fields only.
			ms.GetExecutionInfo().VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(d),
				},
			}
		}

		//nolint:staticcheck // SA1019: worker versioning v0.31
		if vs := override.GetPinnedVersion(); vs != "" { // v0.31 pinned
			// We read from both old and new fields but write in the new fields only.
			ms.GetExecutionInfo().VersioningInfo.VersioningOverride.Override = &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(vs),
				},
			}
		}

		if o := ms.GetExecutionInfo().VersioningInfo.VersioningOverride; worker_versioning.OverrideIsPinned(o) {
			ms.GetExecutionInfo().WorkerDeploymentName = o.GetPinned().GetVersion().GetDeploymentName()
		}
	} else if vi := ms.GetExecutionInfo().GetVersioningInfo(); vi != nil {
		ms.GetExecutionInfo().VersioningInfo.VersioningOverride = nil
		ms.GetExecutionInfo().WorkerDeploymentName = vi.GetDeploymentVersion().GetDeploymentName()
	} else {
		ms.GetExecutionInfo().WorkerDeploymentName = ""
	}

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
		ms.executionInfo.GetVersioningInfo().VersionTransition = nil

		// If effective deployment or behavior change, we need to reschedule any pending tasks, because History will reject
		// the task's start request if the task is being started by a poller that is not from the workflow's effective
		// deployment according to History. Therefore, it is important for matching to match tasks with the correct pollers.
		// Even if the effective deployment does not change, we still need to reschedule tasks into the appropriate
		// default/unpinned queue or the pinned queue, because the two queues will be handled differently if the task queue's
		// Current Deployment changes between now and when the task is started.
		//
		// We choose to let any started WFT that is running on the old deployment finish running, instead of forcing it to fail.
		requestReschedulePendingWorkflowTask = true
		ms.ClearStickyTaskQueue()

		// For v3 versioned workflows (ms.GetEffectiveVersioningBehavior() != UNSPECIFIED), this will update the reachability
		// search attribute based on the execution_info.deployment and/or override deployment if one exists.
		limit := ms.config.SearchAttributesSizeOfValueLimit(ms.namespaceEntry.Name().String())
		// Passing nil useVersion because an override by itself should not add to used versions SA
		if err := ms.updateBuildIdsAndDeploymentSearchAttributes(nil, nil, limit); err != nil {
			return requestReschedulePendingWorkflowTask, err
		}
	}

	return requestReschedulePendingWorkflowTask, ms.reschedulePendingActivities(0)
}

func (ms *MutableStateImpl) ApplyWorkflowExecutionTerminatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) error {
	if _, err := ms.UpdateWorkflowStateStatus(
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
	IsWFTaskQueueInVersionDetector worker_versioning.IsWFTaskQueueInVersionDetector,
) (*historypb.HistoryEvent, historyi.MutableState, error) {
	opTag := tag.WorkflowActionWorkflowContinueAsNew
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	var err error
	newRunID := uuid.NewString()
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

	startEvent, err := ms.GetStartEvent(ctx)
	if err != nil {
		return nil, nil, err
	}

	if _, err = newMutableState.addWorkflowExecutionStartedEventForContinueAsNew(
		ctx,
		parentInfo,
		&newExecution,
		ms,
		command,
		firstRunID,
		rootInfo,
		startEvent.Links,
		IsWFTaskQueueInVersionDetector,
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
		false, // skipCloseTransferTask
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
	if _, err := ms.UpdateWorkflowStateStatus(
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
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo, error) {
	opTag := tag.WorkflowActionChildWorkflowInitiated
	if err := ms.checkMutability(opTag); err != nil {
		return nil, nil, err
	}

	event := ms.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	ci, err := ms.ApplyStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, event)
	if err != nil {
		return nil, nil, err
	}
	// TODO merge active & passive task generation
	if err := ms.taskGenerator.GenerateChildWorkflowTasks(
		event.GetEventId(),
	); err != nil {
		return nil, nil, err
	}
	return event, ci, nil
}

// generateChildWorkflowRequestID generates a unique request ID for child workflow execution
func (ms *MutableStateImpl) generateChildWorkflowRequestID(event *historypb.HistoryEvent) string {
	return fmt.Sprintf("%s:%d:%d", ms.executionState.RunId, event.GetEventId(), event.GetVersion())
}

func (ms *MutableStateImpl) ApplyStartChildWorkflowExecutionInitiatedEvent(
	firstEventID int64,
	event *historypb.HistoryEvent,
) (*persistencespb.ChildExecutionInfo, error) {
	initiatedEventID := event.GetEventId()
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	ci := &persistencespb.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedEventId:      initiatedEventID,
		InitiatedEventBatchId: firstEventID,
		StartedEventId:        common.EmptyEventID,
		StartedWorkflowId:     attributes.GetWorkflowId(),
		CreateRequestId:       ms.generateChildWorkflowRequestID(event),
		Namespace:             attributes.GetNamespace(),
		NamespaceId:           attributes.GetNamespaceId(),
		WorkflowTypeName:      attributes.GetWorkflowType().GetName(),
		ParentClosePolicy:     attributes.GetParentClosePolicy(),
		Priority:              attributes.Priority,
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
			tag.Bool("hasChildInfo", ok),
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
			tag.Bool("hasChildInfo", ok),
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
			tag.Bool("hasChildInfo", ok),
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
			tag.Bool("doesntHaveChildInfo", !ok),
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
			tag.Bool("hasChildInfo", ok),
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
			tag.Bool("hasChildInfo", ok),
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
			tag.Bool("hasChildInfo", ok),
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

	// ScheduleToStart and ScheduleToClose timeouts are server-enforced deadlines,
	// not actual activity execution failures. They should return RETRY_STATE_TIMEOUT,
	// not RETRY_STATE_NON_RETRYABLE_FAILURE.
	//
	// When RETRY_STATE_TIMEOUT is returned, the actual activity failure (if any) that
	// caused retries is available in failure.Cause. SDKs should surface this cause
	// to users since it represents the real failure, while the schedule timeout is
	// just the trigger that finally closed the activity after exhausting retries.
	//
	// See: https://github.com/temporalio/temporal/issues/3667
	if activityFailure.GetTimeoutFailureInfo() != nil {
		timeoutType := activityFailure.GetTimeoutFailureInfo().GetTimeoutType()
		if timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START ||
			timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE {
			return enumspb.RETRY_STATE_TIMEOUT, nil
		}
	}

	if !isRetryable(activityFailure, ai.RetryNonRetryableErrorTypes) {
		return enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, nil
	}

	// check workflow rules
	if !ai.Paused {
		ActivityMatchWorkflowRules(ms, ms.timeSource, ms.logger, ai)
	}

	// if activity is paused
	if ai.Paused {
		// need to update activity
		if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
			activityInfo.StartedEventId = common.EmptyEventID
			activityInfo.StartVersion = common.EmptyVersion
			activityInfo.StartedTime = nil
			activityInfo.RequestId = ""
			activityInfo.RetryLastFailure = ms.truncateRetryableActivityFailure(activityFailure)
			activityInfo.Attempt++
			if ms.config.EnableActivityRetryStampIncrement() {
				activityInfo.Stamp++
			}
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
		backoff.MakeBackoffAlgorithm(delay),
	)
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return retryState, nil
	}

	err := ms.updateActivityInfoForRetries(ai,
		now.Add(retryBackoff),
		ai.Attempt+1,
		activityFailure)
	if err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}

	if err := ms.taskGenerator.GenerateActivityRetryTasks(ai); err != nil {
		return enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR, err
	}
	return enumspb.RETRY_STATE_IN_PROGRESS, nil
}

func (ms *MutableStateImpl) RecordLastActivityCompleteTime(ai *persistencespb.ActivityInfo) {
	_ = ms.UpdateActivity(ai.ScheduledEventId, func(info *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		ai.LastAttemptCompleteTime = timestamppb.New(ms.shard.GetTimeSource().Now().UTC())
		return nil
	})
}

func (ms *MutableStateImpl) RegenerateActivityRetryTask(ai *persistencespb.ActivityInfo, nextScheduledTime time.Time) error {

	if nextScheduledTime.IsZero() {
		nextScheduledTime = GetNextScheduledTime(ai)
	}

	err := ms.updateActivityInfoForRetries(ai,
		nextScheduledTime,
		ai.Attempt,
		nil)
	if err != nil {
		return err
	}

	return ms.taskGenerator.GenerateActivityRetryTasks(ai)
}

func (ms *MutableStateImpl) updateActivityInfoForRetries(
	ai *persistencespb.ActivityInfo,
	nextScheduledTime time.Time,
	nextAttempt int32,
	activityFailure *failurepb.Failure,
) error {
	return ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		UpdateActivityInfoForRetries(
			activityInfo,
			ms.GetCurrentVersion(),
			nextAttempt,
			ms.truncateRetryableActivityFailure(activityFailure),
			timestamppb.New(nextScheduledTime),
			ms.config.EnableActivityRetryStampIncrement(),
		)
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

func (ms *MutableStateImpl) UpdateActivity(scheduledEventId int64, updater historyi.ActivityUpdater) error {
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

		if ai.Paused {
			metrics.PausedActivitiesCounter.With(
				ms.metricsHandler.WithTags(
					metrics.NamespaceTag(ms.namespaceEntry.Name().String()),
					metrics.WorkflowTypeTag(ms.GetExecutionInfo().WorkflowTypeName),
					metrics.ActivityTypeTag(ai.ActivityType.Name),
				),
			).Record(1)
		}
	}

	ms.approximateSize += ai.Size() - originalSize
	ms.updateActivityInfos[ai.ScheduledEventId] = ai
	ms.syncActivityTasks[ai.ScheduledEventId] = struct{}{}
	ms.activityInfosUserDataUpdated[ai.ScheduledEventId] = struct{}{}

	return nil
}

func (ms *MutableStateImpl) buildTemporalPauseInfoEntries() []string {
	var entries []string

	// Add workflow pause entries if workflow is paused
	if ms.executionInfo.PauseInfo != nil {
		entries = append(entries, fmt.Sprintf("Workflow:%s", ms.GetWorkflowKey().WorkflowID))
		if reason := ms.executionInfo.PauseInfo.Reason; reason != "" {
			entries = append(entries, fmt.Sprintf("Reason:%s", reason))
		}
	}

	// Add activity pause entries for paused activities
	pausedActivityTypes := make(map[string]struct{})
	for _, ai := range ms.GetPendingActivityInfos() {
		if ai.Paused {
			pausedActivityTypes[ai.ActivityType.Name] = struct{}{}
		}
	}

	for activityType := range pausedActivityTypes {
		entries = append(entries, fmt.Sprintf("property:activityType=%s", activityType))
	}

	return entries
}

func (ms *MutableStateImpl) updatePauseInfoSearchAttribute() error {
	allEntries := ms.buildTemporalPauseInfoEntries()

	pauseInfoPayload, err := searchattribute.EncodeValue(allEntries, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	if err != nil {
		return err
	}

	exeInfo := ms.executionInfo
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload, 1)
	}

	if proto.Equal(exeInfo.SearchAttributes[sadefs.TemporalPauseInfo], pauseInfoPayload) {
		return nil // unchanged
	}

	ms.updateSearchAttributes(map[string]*commonpb.Payload{sadefs.TemporalPauseInfo: pauseInfoPayload})
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
}

func (ms *MutableStateImpl) UpdateReportedProblemsSearchAttribute() error {
	var reportedProblems []string
	switch wftFailure := ms.executionInfo.LastWorkflowTaskFailure.(type) {
	case *persistencespb.WorkflowExecutionInfo_LastWorkflowTaskFailureCause:
		reportedProblems = []string{
			"category=WorkflowTaskFailed",
			fmt.Sprintf("cause=WorkflowTaskFailedCause%s", wftFailure.LastWorkflowTaskFailureCause.String()),
		}
	case *persistencespb.WorkflowExecutionInfo_LastWorkflowTaskTimedOutType:
		reportedProblems = []string{
			"category=WorkflowTaskTimedOut",
			fmt.Sprintf("cause=WorkflowTaskTimedOutCause%s", wftFailure.LastWorkflowTaskTimedOutType.String()),
		}
	}

	reportedProblemsPayload, err := searchattribute.EncodeValue(reportedProblems, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	if err != nil {
		return err
	}

	exeInfo := ms.executionInfo
	if exeInfo.SearchAttributes == nil {
		exeInfo.SearchAttributes = make(map[string]*commonpb.Payload, 1)
	}

	decodedA, err := searchattribute.DecodeValue(exeInfo.SearchAttributes[sadefs.TemporalReportedProblems], enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
	if err != nil {
		return err
	}

	existingProblems, ok := decodedA.([]string)
	if !ok && decodedA != nil {
		softassert.Fail(ms.logger, "TemporalReportedProblems payload decoded to unexpected type for logging")
		return errors.New("TemporalReportedProblems payload decoded to unexpected type for logging")
	}

	if slices.Equal(existingProblems, reportedProblems) {
		return nil
	}

	// Log the search attribute change
	ms.logReportedProblemsChange(existingProblems, reportedProblems)

	ms.updateSearchAttributes(map[string]*commonpb.Payload{sadefs.TemporalReportedProblems: reportedProblemsPayload})
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
}

func (ms *MutableStateImpl) RemoveReportedProblemsSearchAttribute() error {
	if ms.executionInfo.SearchAttributes == nil {
		return nil
	}

	temporalReportedProblems := ms.executionInfo.SearchAttributes[sadefs.TemporalReportedProblems]
	if temporalReportedProblems == nil {
		return nil
	}

	// Log the removal of the search attribute
	ms.logReportedProblemsChange(ms.decodeReportedProblems(temporalReportedProblems), nil)

	ms.executionInfo.LastWorkflowTaskFailure = nil

	// Just remove the search attribute entirely for now
	ms.updateSearchAttributes(map[string]*commonpb.Payload{sadefs.TemporalReportedProblems: nil})
	return ms.taskGenerator.GenerateUpsertVisibilityTask()
}

// logReportedProblemsChange logs changes to the TemporalReportedProblems search attribute
func (ms *MutableStateImpl) logReportedProblemsChange(oldPayload, newPayload []string) {
	if oldPayload == nil && newPayload != nil {
		// Adding search attribute
		ms.logger.Info("TemporalReportedProblems search attribute added",
			tag.Strings("reported-problems", newPayload))
	} else if oldPayload != nil && newPayload == nil {
		// Removing search attribute
		ms.logger.Info("TemporalReportedProblems search attribute removed",
			tag.Strings("previous-reported-problems", oldPayload))
	} else if oldPayload != nil && newPayload != nil {
		// Updating search attribute
		ms.logger.Info("TemporalReportedProblems search attribute updated",
			tag.Strings("previous-reported-problems", oldPayload),
			tag.Strings("reported-problems", newPayload))
	}
}

// decodeReportedProblems safely decodes a keyword list payload to []string
func (ms *MutableStateImpl) decodeReportedProblems(p *commonpb.Payload) []string {
	if p == nil {
		return nil
	}

	decoded, err := searchattribute.DecodeValue(p, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
	if err != nil {
		ms.logger.Error("Failed to decode TemporalReportedProblems payload for logging",
			tag.Error(err))
		softassert.Fail(ms.logger, "Failed to decode TemporalReportedProblems payload for logging")
		return []string{}
	}

	problems, ok := decoded.([]string)
	if !ok {
		ms.logger.Error("TemporalReportedProblems payload decoded to unexpected type for logging")
		softassert.Fail(ms.logger, "TemporalReportedProblems payload decoded to unexpected type for logging")
		return []string{}
	}

	return problems
}

func (ms *MutableStateImpl) truncateRetryableActivityFailure(
	activityFailure *failurepb.Failure,
) *failurepb.Failure {
	namespaceName := ms.namespaceEntry.Name().String()
	failureSize := activityFailure.Size()

	if failureSize <= ms.config.MutableStateActivityFailureSizeLimitWarn(namespaceName) {
		return activityFailure
	}

	sizeLimitError := ms.config.MutableStateActivityFailureSizeLimitError(namespaceName)
	if failureSize <= sizeLimitError {
		return activityFailure
	}

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

func (ms *MutableStateImpl) GetExternalPayloadSize() int64 {
	return ms.executionInfo.GetExecutionStats().GetExternalPayloadSize()
}

func (ms *MutableStateImpl) AddExternalPayloadSize(size int64) {
	if ms.executionInfo.ExecutionStats == nil {
		ms.executionInfo.ExecutionStats = &persistencespb.ExecutionStats{}
	}
	ms.executionInfo.ExecutionStats.ExternalPayloadSize += size
}

func (ms *MutableStateImpl) GetExternalPayloadCount() int64 {
	return ms.executionInfo.GetExecutionStats().GetExternalPayloadCount()
}

func (ms *MutableStateImpl) AddExternalPayloadCount(count int64) {
	if ms.executionInfo.ExecutionStats == nil {
		ms.executionInfo.ExecutionStats = &persistencespb.ExecutionStats{}
	}
	ms.executionInfo.ExecutionStats.ExternalPayloadCount += count
}

// processCloseCallbacks triggers "WorkflowClosed" callbacks, applying the state machine transition that schedules
// callback tasks.
func (ms *MutableStateImpl) processCloseCallbacks() error {
	resetRunID := ms.GetExecutionInfo().GetResetRunId()
	if ms.GetExecutionInfo().GetWorkflowWasReset() && resetRunID != "" && resetRunID != ms.executionState.RunId {
		return nil
	}

	// Process CHASM callbacks if CHASM is enabled. Note that we check ChasmEnabled() rather than
	// chasmCallbacksEnabled() to ensure that callbacks created when both HSM and CHASM callbacks
	// were enabled can still be triggered even if the EnableCHASMCallbacks dynamic config is later
	// turned off. Once created in CHASM, callbacks should always be processed as long as CHASM is enabled.
	if ms.ChasmEnabled() {
		if err := ms.processCloseCallbacksChasm(); err != nil {
			return err
		}
	}

	// Always process HSM callbacks as well (a workflow can have both)
	return ms.processCloseCallbacksHsm()
}

// processCloseCallbacksHsm triggers "WorkflowClosed" callbacks using the HSM implementation.
func (ms *MutableStateImpl) processCloseCallbacksHsm() error {
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

// processCloseCallbacksChasm triggers "WorkflowClosed" callbacks using the CHASM implementation.
func (ms *MutableStateImpl) processCloseCallbacksChasm() error {
	wf, _, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	if err != nil {
		return err
	}

	// Return early if there are no chasm callbacks to process.
	if len(wf.Callbacks) == 0 {
		return nil
	}

	// If there are callbacks to process, create a writable workflow component.
	wf, ctx, err := ms.ChasmWorkflowComponent(context.Background())
	if err != nil {
		return err
	}

	return wf.ProcessCloseCallbacks(ctx)
}

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

		if chasmPureTask, ok := task.(*tasks.ChasmTaskPure); ok {
			ms.chasmPureTasks = append(ms.chasmPureTasks, chasmPureTask)
			maxPureTasks := ms.config.ChasmMaxInMemoryPureTasks()
			if len(ms.chasmPureTasks) > maxPureTasks {
				// Since tasks are reverse ordered by their scheduled time, tasks in the beginning are those
				// - Generated a long time ago
				// - Scheduled time is far in the future
				// both types of tasks are likely to already be persisted in DB and best-effort deletion won't help,
				// so drop them from the in-memory list first.
				ms.chasmPureTasks = ms.chasmPureTasks[len(ms.chasmPureTasks)-maxPureTasks:]
			}
		}

		ms.InsertTasks[category] = append(ms.InsertTasks[category], task)
	}
}

func (ms *MutableStateImpl) PopTasks() map[tasks.Category][]tasks.Task {
	insertTasks := ms.InsertTasks
	ms.InsertTasks = make(map[tasks.Category][]tasks.Task)
	return insertTasks
}

func (ms *MutableStateImpl) DeleteCHASMPureTasks(maxScheduledTime time.Time) {
	for lastTaskIdx := len(ms.chasmPureTasks) - 1; lastTaskIdx >= 0; lastTaskIdx-- {
		task := ms.chasmPureTasks[lastTaskIdx]
		if !task.GetVisibilityTime().Before(maxScheduledTime) {
			ms.chasmPureTasks = ms.chasmPureTasks[:lastTaskIdx+1]
			return
		}

		ms.BestEffortDeleteTasks[tasks.CategoryTimer] = append(
			ms.BestEffortDeleteTasks[tasks.CategoryTimer],
			task.GetKey(),
		)
	}

	// If we reach here, all tasks have visibility time before maxScheduledTime
	// and need to be deleted.
	ms.chasmPureTasks = ms.chasmPureTasks[:0]
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

func (ms *MutableStateImpl) SetWorkflowTaskScheduleToStartTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) {
	ms.wftScheduleToStartTimeoutTask = task
}

func (ms *MutableStateImpl) SetWorkflowTaskStartToCloseTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) {
	ms.wftStartToCloseTimeoutTask = task
}

func (ms *MutableStateImpl) GetWorkflowTaskScheduleToStartTimeoutTask() *tasks.WorkflowTaskTimeoutTask {
	return ms.wftScheduleToStartTimeoutTask
}

func (ms *MutableStateImpl) GetWorkflowTaskStartToCloseTimeoutTask() *tasks.WorkflowTaskTimeoutTask {
	return ms.wftStartToCloseTimeoutTask
}

func (ms *MutableStateImpl) GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus) {
	return ms.executionState.State, ms.executionState.Status
}

func (ms *MutableStateImpl) UpdateWorkflowStateStatus(
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) (bool, error) {
	if state == ms.executionState.State && status == ms.executionState.Status {
		return false, nil
	}
	if state != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE &&
		ms.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		// Suppress and Revive workflows are cluster local operations.
		ms.executionStateUpdated = true
		ms.visibilityUpdated = true // workflow status & state change triggers visibility change as well
	}
	return true, setStateStatus(ms.executionState, state, status)
}

// IsDirty is used for sanity check that mutable state is "clean" after mutable state lock is released.
// However, certain in-memory changes (e.g. speculative workflow task) won't be cleared before releasing
// the lock and have to be excluded from the check.
func (ms *MutableStateImpl) IsDirty() bool {
	return ms.hBuilder.IsDirty() ||
		len(ms.InsertTasks) > 0 ||
		(ms.stateMachineNode != nil && ms.stateMachineNode.Dirty()) ||
		ms.chasmTree.IsDirty()
}

// isStateDirty is used upon closing transaction to determine if application data has been updated, and
// mutable state should move to a new versioned transition.
func (ms *MutableStateImpl) isStateDirty() bool {
	// TODO: we need to track more workflow state changes
	// e.g. changes to executionInfo.CancelRequested
	// They are mostly covered by history builder check today.
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
		(ms.stateMachineNode != nil && ms.stateMachineNode.Dirty()) ||
		ms.chasmTree.IsStateDirty() ||
		ms.isResetStateUpdated
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

	ms.transitionHistoryEnabled = ms.config.EnableTransitionHistory(namespaceEntry.Name().String())

	namespaceEntry, err := ms.startTransactionHandleNamespaceMigration(namespaceEntry)
	if err != nil {
		return false, err
	}
	ms.namespaceEntry = namespaceEntry
	if err := ms.UpdateCurrentVersion(namespaceEntry.FailoverVersion(ms.executionInfo.WorkflowId), false); err != nil {
		return false, err
	}

	flushBeforeReady, err := ms.startTransactionHandleWorkflowTaskFailover()
	if err != nil {
		return false, err
	}

	return flushBeforeReady, nil
}

func (ms *MutableStateImpl) CloseTransactionAsMutation(
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error) {
	result, err := ms.closeTransaction(ctx, transactionPolicy)
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
		UpsertChasmNodes:          result.chasmNodesMutation.UpdatedNodes,
		DeleteChasmNodes:          result.chasmNodesMutation.DeletedNodes,
		NewBufferedEvents:         result.bufferEvents,
		ClearBufferedEvents:       result.clearBuffer,

		Tasks:                 ms.InsertTasks,
		BestEffortDeleteTasks: ms.BestEffortDeleteTasks,

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
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {
	result, err := ms.closeTransaction(ctx, transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	if len(result.bufferEvents) > 0 {
		// TODO do we need the functionality to generate snapshot with buffered events?
		return nil, nil, softassert.UnexpectedInternalErr(
			ms.logger,
			"cannot generate workflow snapshot with buffered events",
			nil,
		)
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
		ChasmNodes:          ms.chasmTree.Snapshot(nil).Nodes,

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

func (ms *MutableStateImpl) GenerateMigrationTasks(targetClusters []string) ([]tasks.Task, int64, error) {
	return ms.taskGenerator.GenerateMigrationTasks(targetClusters)
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
	workflowEventsSeq  []*persistence.WorkflowEvents
	bufferEvents       []*historypb.HistoryEvent
	clearBuffer        bool
	checksum           *persistencespb.Checksum
	chasmNodesMutation chasm.NodesMutation
}

func (ms *MutableStateImpl) SetContextMetadata(
	ctx context.Context,
) {
	switch ms.chasmTree.ArchetypeID() {
	case chasm.WorkflowArchetypeID, chasm.UnspecifiedArchetypeID:
		// Set workflow type
		if wfType := ms.GetWorkflowType(); wfType != nil && wfType.GetName() != "" {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, wfType.GetName())
		}

		// Set workflow task queue
		if ms.executionInfo != nil && ms.executionInfo.TaskQueue != "" {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, ms.executionInfo.TaskQueue)
		}

		// TODO: To set activity_type/activity_task_queue metadata, the history gRPC handler should
		// set the relevant activity information on the metadata context before calling mutable state.
	default:
		// No metadata to set for other archetype types
	}
}

func (ms *MutableStateImpl) closeTransaction(
	ctx context.Context,
	transactionPolicy historyi.TransactionPolicy,
) (closeTransactionResult, error) {
	ms.SetContextMetadata(ctx)

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

	// Save if the state is dirty before closeTransactionPrepareEvents since it flushes the buffer
	// events, and therefore change the dirty state.
	isStateDirty := ms.isStateDirty()

	// closeTransactionPrepareEvents must be called after closeTransactionHandleWorkflowTask because
	// the latter might fail the workflow task and buffered events must be flushed afterwards.
	// We need to save the value of ms.isStateDirty() before calling closeTransactionPrepareEvents
	// because flushing the buffered events might change the dirty state.
	workflowEventsSeq, eventBatches, bufferEvents, clearBuffer, err := ms.closeTransactionPrepareEvents(transactionPolicy)
	if err != nil {
		return closeTransactionResult{}, err
	}

	// CloseTransaction() on chasmTree may update execution state & status,
	// so must be called before closeTransactionUpdateTransitionHistory().
	chasmNodesMutation, err := ms.chasmTree.CloseTransaction()
	if err != nil {
		return closeTransactionResult{}, err
	}
	for nodePath := range chasmNodesMutation.DeletedNodes {
		ms.approximateSize -= ms.chasmNodeSizes[nodePath]
		delete(ms.chasmNodeSizes, nodePath)
	}
	for nodePath, node := range chasmNodesMutation.UpdatedNodes {
		newSize := len(nodePath) + node.Size()
		ms.approximateSize += newSize - ms.chasmNodeSizes[nodePath]
		ms.chasmNodeSizes[nodePath] = newSize
	}

	if isStateDirty {
		if err := ms.closeTransactionUpdateTransitionHistory(
			transactionPolicy,
		); err != nil {
			return closeTransactionResult{}, err
		}
		ms.closeTransactionHandleUnknownVersionedTransition()
		ms.closeTransactionUpdateLastRunningClock(transactionPolicy, workflowEventsSeq)
	}

	ms.closeTransactionTrackLastUpdateVersionedTransition(
		transactionPolicy,
	)

	ms.closeTransactionTrackTombstones(transactionPolicy, chasmNodesMutation)

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
		workflowEventsSeq:  workflowEventsSeq,
		bufferEvents:       bufferEvents,
		clearBuffer:        clearBuffer,
		checksum:           checksum,
		chasmNodesMutation: chasmNodesMutation,
	}, nil
}

func (ms *MutableStateImpl) closeTransactionHandleWorkflowTask(
	transactionPolicy historyi.TransactionPolicy,
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
	transactionPolicy historyi.TransactionPolicy,
) error {
	if transactionPolicy == historyi.TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
		return nil
	}

	for _, t := range ms.currentTransactionAddedStateMachineEventTypes {
		def, ok := ms.shard.StateMachineRegistry().EventDefinition(t)
		if !ok {
			return serviceerror.NewInternalf("no event definition registered for %v", t)
		}
		if def.IsWorkflowTaskTrigger() {
			if !ms.HasPendingWorkflowTask() && !ms.IsWorkflowExecutionStatusPaused() {
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
	transactionPolicy historyi.TransactionPolicy,
) error {
	if transactionPolicy == historyi.TransactionPolicyPassive ||
		!ms.IsWorkflowExecutionRunning() {
		return nil
	}

	// It is important to convert speculative WT to normal before prepareEventsAndReplicationTasks,
	// because prepareEventsAndReplicationTasks will move internal buffered events to the history,
	// and WT related events (WTScheduled, in particular) need to go first.
	return ms.workflowTaskManager.convertSpeculativeWorkflowTaskToNormal()
}

func (ms *MutableStateImpl) closeTransactionUpdateTransitionHistory(
	transactionPolicy historyi.TransactionPolicy,
) error {
	if transactionPolicy != historyi.TransactionPolicyActive {
		// TODO: replication/standby logic will need a different way for updating transition history
		// when not syncing mutable state
		return nil
	}

	if !ms.transitionHistoryEnabled {
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
	transactionPolicy historyi.TransactionPolicy,
) {
	if transactionPolicy != historyi.TransactionPolicyActive {
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

	currentVersionedTransition := ms.CurrentVersionedTransition()
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

	if ms.workflowTaskUpdated {
		ms.executionInfo.WorkflowTaskLastUpdateVersionedTransition = currentVersionedTransition
	}

	if ms.visibilityUpdated {
		ms.executionInfo.VisibilityLastUpdateVersionedTransition = currentVersionedTransition
	}

	if ms.executionStateUpdated {
		ms.executionState.LastUpdateVersionedTransition = currentVersionedTransition
	}

	// LastUpdateVersionTransition for HSM nodes already updated when transitioning the nodes.
	// LastUpdateVersionTransition for CHASM nodes already updated when closing the chasm tree transaction.
}

func (ms *MutableStateImpl) closeTransactionHandleUnknownVersionedTransition() {
	if len(ms.executionInfo.TransitionHistory) != 0 {
		if transitionhistory.Compare(
			ms.versionedTransitionInDB,
			ms.CurrentVersionedTransition(),
		) != 0 {
			// versioned transition updated in the transaction
			return
		}
	}

	// State changed but transition history not updated.
	// We are in unknown versioned transition state, clear the transition history.
	if len(ms.executionInfo.TransitionHistory) != 0 {
		ms.executionInfo.PreviousTransitionHistory = ms.executionInfo.TransitionHistory
		ms.executionInfo.LastTransitionHistoryBreakPoint = transitionhistory.CopyVersionedTransition(ms.CurrentVersionedTransition())
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

func (ms *MutableStateImpl) closeTransactionUpdateLastRunningClock(
	transactionPolicy historyi.TransactionPolicy,
	workflowEventsSeq []*persistence.WorkflowEvents,
) {
	if transactionPolicy != historyi.TransactionPolicyActive {
		return
	}

	// Events can only be generated while mutable state is running,
	// so we can update LastRunningClock blindly.
	if len(workflowEventsSeq) > 0 {
		lastEvents := workflowEventsSeq[len(workflowEventsSeq)-1].Events
		lastEvent := lastEvents[len(lastEvents)-1]
		ms.executionInfo.LastRunningClock = lastEvent.GetTaskId()
		return
	}

	if !ms.IsWorkflowExecutionRunning() && !ms.IsCurrentWorkflowGuaranteed() {
		// If workflow currently is not running and also not running at the beginning of the transaction,
		// then don't update the lastRunningClock
		// NOTE: running at the beginning of the transaction == it's a current workflow in DB.
		return
	}

	ms.executionInfo.LastRunningClock = ms.shard.CurrentVectorClock().GetClock()
}

func (ms *MutableStateImpl) closeTransactionTrackTombstones(
	transactionPolicy historyi.TransactionPolicy,
	chasmNodesMutation chasm.NodesMutation,
) {
	if transactionPolicy != historyi.TransactionPolicyActive {
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
	for chasmNodePath := range chasmNodesMutation.DeletedNodes {
		tombstones = append(tombstones, &persistencespb.StateMachineTombstone{
			StateMachineKey: &persistencespb.StateMachineTombstone_ChasmNodePath{
				ChasmNodePath: chasmNodePath,
			},
		})
	}
	// Entire signalRequestedIDs will be synced if updated, so we don't track individual signalRequestedID tombstone.
	// TODO: Track signalRequestedID tombstone when we support syncing partial signalRequestedIDs.
	// This requires tracking the lastUpdateVersionedTransition for each signalRequestedID,
	// which is not supported by today's DB schema.
	// TODO: we don't delete updateInfo today. Track them here when we do.
	currentVersionedTransition := ms.CurrentVersionedTransition()

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
	transactionPolicy historyi.TransactionPolicy,
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

	if err := ms.closeTransactionGenerateChasmRetentionTask(transactionPolicy); err != nil {
		return err
	}

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

func (ms *MutableStateImpl) closeTransactionGenerateChasmRetentionTask(
	transactionPolicy historyi.TransactionPolicy,
) error {

	if ms.IsWorkflow() ||
		ms.executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED ||
		ms.stateInDB == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return nil
	}

	// Generate retention timer for chasm executions if it's currentely completed
	// but state in DB is not completed, i.e. completing in this transaction.

	if transactionPolicy == historyi.TransactionPolicyActive {
		// TODO: consider setting CloseTime in ChasmTree closeTransaction() instead of here.
		ms.executionInfo.CloseTime = timestamppb.New(ms.timeSource.Now())

		// If passive cluster, the CloseTime field should already be populated by the replication stack.
	}

	return ms.taskGenerator.GenerateDeleteHistoryEventTask(ms.executionInfo.CloseTime.AsTime())
}

func (ms *MutableStateImpl) closeTransactionPrepareReplicationTasks(
	transactionPolicy historyi.TransactionPolicy,
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

	archetypeID := ms.ChasmTree().ArchetypeID()
	isWorkflow := archetypeID == chasm.WorkflowArchetypeID
	if !isWorkflow && len(replicationTasks) != 0 {
		return softassert.UnexpectedInternalErr(ms.logger, "chasm execution generated workflow replication tasks", nil)
	}

	if ms.transitionHistoryEnabled {
		switch transactionPolicy {
		case historyi.TransactionPolicyActive:
			if ms.generateReplicationTask() {
				now := time.Now().UTC()
				workflowKey := definition.NewWorkflowKey(
					ms.executionInfo.NamespaceId,
					ms.executionInfo.WorkflowId,
					ms.executionState.RunId,
				)
				firstEventID := common.EmptyEventID
				firstEventVersion := common.EmptyVersion
				nextEventID := common.EmptyEventID
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
					if !versionhistory.IsEmptyVersionHistory(currentHistory) {
						item, err := versionhistory.GetLastVersionHistoryItem(currentHistory)
						//nolint:revive // max-control-nesting: control flow nesting exceeds 5
						if err != nil {
							return err
						}
						lastVersionHistoryItem = versionhistory.CopyVersionHistoryItem(item)
					}
				}

				currentVersionedTransition := ms.CurrentVersionedTransition()
				if currentVersionedTransition != nil && transitionhistory.Compare(
					ms.versionedTransitionInDB,
					currentVersionedTransition,
				) != 0 {
					syncVersionedTransitionTask := &tasks.SyncVersionedTransitionTask{
						WorkflowKey:            workflowKey,
						VisibilityTimestamp:    now,
						ArchetypeID:            archetypeID,
						Priority:               enumsspb.TASK_PRIORITY_HIGH,
						VersionedTransition:    currentVersionedTransition,
						FirstEventID:           firstEventID,
						FirstEventVersion:      firstEventVersion,
						NextEventID:            nextEventID,
						TaskEquivalents:        replicationTasks,
						LastVersionHistoryItem: lastVersionHistoryItem,
					}

					if ms.dbRecordVersion == 1 {
						syncVersionedTransitionTask.IsFirstTask = true
					}

					// versioned transition updated in the transaction
					ms.InsertTasks[tasks.CategoryReplication] = append(
						ms.InsertTasks[tasks.CategoryReplication],
						syncVersionedTransitionTask,
					)
				}
			}
		case historyi.TransactionPolicyPassive:
		default:
			panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
		}
	} else if isWorkflow {
		ms.InsertTasks[tasks.CategoryReplication] = append(
			ms.InsertTasks[tasks.CategoryReplication],
			replicationTasks...,
		)
	} else {
		return softassert.UnexpectedInternalErr(ms.logger, "state-based replication not enabled for chasm execution", nil)
	}

	if transactionPolicy == historyi.TransactionPolicyPassive &&
		len(ms.InsertTasks[tasks.CategoryReplication]) > 0 {
		return softassert.UnexpectedInternalErr(
			ms.logger,
			"should not generate replication task when close transaction as passive",
			nil,
		)
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
	ms.isResetStateUpdated = false
	ms.updateInfoUpdated = make(map[string]struct{})
	ms.timerInfosUserDataUpdated = make(map[string]struct{})
	ms.activityInfosUserDataUpdated = make(map[int64]struct{})
	ms.reapplyEventsCandidate = nil
	ms.subStateMachineDeleted = false

	ms.stateInDB = ms.executionState.State
	ms.nextEventIDInDB = ms.GetNextEventID()
	if len(ms.executionInfo.TransitionHistory) != 0 {
		ms.versionedTransitionInDB = ms.CurrentVersionedTransition()
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
	ms.BestEffortDeleteTasks = make(map[tasks.Category][]tasks.Key)

	// Clear outputs for the next transaction.
	ms.stateMachineNode.ClearTransactionState()
	// Clear out transient state machine state.
	ms.currentTransactionAddedStateMachineEventTypes = nil

	return nil
}

func (ms *MutableStateImpl) closeTransactionPrepareEvents(
	transactionPolicy historyi.TransactionPolicy,
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
	ms.updatePendingEventIDs(historyMutation.ScheduledIDToStartedID, historyMutation.RequestIDToEventID)

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

		// Calculate and add the external payload size and count for this batch
		if ms.config.ExternalPayloadsEnabled(ms.GetNamespaceEntry().Name().String()) {
			externalPayloadSize, externalPayloadCount, err := CalculateExternalPayloadSize(eventBatch, ms.metricsHandler)
			if err != nil {
				return nil, nil, nil, false, err
			}
			ms.AddExternalPayloadSize(externalPayloadSize)
			ms.AddExternalPayloadCount(externalPayloadCount)
		}
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
	transactionPolicy historyi.TransactionPolicy,
	eventBatches [][]*historypb.HistoryEvent,
) ([]tasks.Task, error) {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		if ms.generateReplicationTask() {
			return ms.taskGenerator.GenerateHistoryReplicationTasks(eventBatches)
		}
		return nil, nil
	case historyi.TransactionPolicyPassive:
		return nil, nil
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) syncActivityToReplicationTask(
	transactionPolicy historyi.TransactionPolicy,
) []tasks.Task {
	now := time.Now().UTC()
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
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
				nil,
			)
		}
		return nil
	case historyi.TransactionPolicyPassive:
		return emptyTasks
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) dirtyHSMToReplicationTask(
	transactionPolicy historyi.TransactionPolicy,
	eventBatches [][]*historypb.HistoryEvent,
	clearBufferEvents bool,
) []tasks.Task {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
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
	case historyi.TransactionPolicyPassive:
		return emptyTasks
	default:
		panic(fmt.Sprintf("unknown transaction policy: %v", transactionPolicy))
	}
}

func (ms *MutableStateImpl) updatePendingEventIDs(
	scheduledIDToStartedID map[int64]int64,
	requestIDToEventID map[string]int64,
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
	if len(requestIDToEventID) > 0 {
		for requestID, eventID := range requestIDToEventID {
			if requestIDInfo, ok := ms.executionState.RequestIds[requestID]; ok {
				requestIDInfo.EventId = eventID
			}
		}
	}
}

func (ms *MutableStateImpl) updateWithLastWriteEvent(
	lastEvent *historypb.HistoryEvent,
	transactionPolicy historyi.TransactionPolicy,
) error {
	if transactionPolicy == historyi.TransactionPolicyPassive {
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

	return nil
}

// validateNoEventsAfterWorkflowFinish perform check on history event batch
// NOTE: do not apply this check on every batch, since transient
// workflow task && workflow finish will be broken (the first batch)
func (ms *MutableStateImpl) validateNoEventsAfterWorkflowFinish(
	transactionPolicy historyi.TransactionPolicy,
	workflowEventSeq []*persistence.WorkflowEvents,
) error {
	if transactionPolicy == historyi.TransactionPolicyPassive ||
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
	if lastWriteVersion == common.EmptyVersion && namespaceEntry.FailoverVersion(ms.executionInfo.WorkflowId) > common.EmptyVersion && ms.HasStartedWorkflowTask() {
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
		return false, serviceerror.NewInternalf("MutableStateImpl encountered mismatch version, workflow task: %v, last event version %v", workflowTask.Version, lastEventVersion)
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
	transactionPolicy historyi.TransactionPolicy,
) error {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
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
	case historyi.TransactionPolicyPassive:
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
	transactionPolicy historyi.TransactionPolicy,
) error {
	if transactionPolicy == historyi.TransactionPolicyPassive ||
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
	transactionPolicy historyi.TransactionPolicy,
) error {
	if transactionPolicy == historyi.TransactionPolicyPassive ||
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
	transactionPolicy historyi.TransactionPolicy,
) error {
	switch transactionPolicy {
	case historyi.TransactionPolicyActive:
		if !ms.IsWorkflowExecutionRunning() {
			return nil
		}
		if err := ms.taskGenerator.GenerateActivityTimerTasks(); err != nil {
			return err
		}
		return ms.taskGenerator.GenerateUserTimerTasks()
	case historyi.TransactionPolicyPassive:
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
	return len(ms.namespaceEntry.ClusterNames(ms.GetWorkflowKey().WorkflowID)) > 1
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

// TODO: Deprecate following logging methods and use ms.logger directly.
func (ms *MutableStateImpl) logInfo(msg string, tags ...tag.Tag) {
	ms.logger.Info(msg, tags...)
}

func (ms *MutableStateImpl) logWarn(msg string, tags ...tag.Tag) {
	ms.logger.Warn(msg, tags...)
}

func (ms *MutableStateImpl) logError(msg string, tags ...tag.Tag) {
	ms.logger.Error(msg, tags...)
}

func (ms *MutableStateImpl) logDataInconsistency() {
	ms.logger.Error("encounter cassandra data inconsistency")
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

func (ms *MutableStateImpl) CurrentVersionedTransition() *persistencespb.VersionedTransition {
	return transitionhistory.LastVersionedTransition(ms.executionInfo.TransitionHistory)
}

func (ms *MutableStateImpl) ApplyMutation(
	mutation *persistencespb.WorkflowMutableStateMutation,
) error {
	prevExecutionInfoSize := ms.executionInfo.Size()
	currentVersionedTransition := ms.CurrentVersionedTransition()

	ms.applySignalRequestedIds(mutation.SignalRequestedIds, mutation.ExecutionInfo)
	err := ms.applyTombstones(mutation.SubStateMachineTombstoneBatches, currentVersionedTransition)
	if err != nil {
		return err
	}
	err = ms.syncExecutionInfo(ms.executionInfo, mutation.ExecutionInfo, false)
	if err != nil {
		return err
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

	// approximateSize update will be handled upon closing transaction
	return ms.chasmTree.ApplyMutation(chasm.NodesMutation{
		UpdatedNodes: mutation.UpdatedChasmNodes,
	})
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

	// approximateSize update will be handled upon closing transaction
	return ms.chasmTree.ApplySnapshot(chasm.NodesSnapshot{
		Nodes: snapshot.ChasmNodes,
	})
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
			if transitionhistory.Compare(nodeMutation.LastUpdateVersionedTransition, internalNode.LastUpdateVersionedTransition) == 0 {
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
	if transitionhistory.Compare(
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
			if transitionhistory.Compare(existing.GetLastUpdateVersionedTransition(), updated.GetLastUpdateVersionedTransition()) == 0 {
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
			if transitionhistory.Compare(existing.GetLastUpdateVersionedTransition(), ui.GetLastUpdateVersionedTransition()) == 0 {
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
	var workflowTaskVersionUpdated bool
	if transitionhistory.Compare(current.WorkflowTaskLastUpdateVersionedTransition, incoming.WorkflowTaskLastUpdateVersionedTransition) != 0 {
		ms.workflowTaskManager.UpdateWorkflowTask(&historyi.WorkflowTaskInfo{
			Version:                  incoming.WorkflowTaskVersion,
			ScheduledEventID:         incoming.WorkflowTaskScheduledEventId,
			StartedEventID:           incoming.WorkflowTaskStartedEventId,
			RequestID:                incoming.WorkflowTaskRequestId,
			WorkflowTaskTimeout:      incoming.WorkflowTaskTimeout.AsDuration(),
			Attempt:                  incoming.WorkflowTaskAttempt,
			AttemptsSinceLastSuccess: incoming.WorkflowTaskAttemptsSinceLastSuccess,
			Stamp:                    incoming.WorkflowTaskStamp,
			StartedTime:              incoming.WorkflowTaskStartedTime.AsTime(),
			ScheduledTime:            incoming.WorkflowTaskScheduledTime.AsTime(),

			OriginalScheduledTime: incoming.WorkflowTaskOriginalScheduledTime.AsTime(),
			Type:                  incoming.WorkflowTaskType,

			SuggestContinueAsNew:                 incoming.WorkflowTaskSuggestContinueAsNew,
			SuggestContinueAsNewReasons:          incoming.WorkflowTaskSuggestContinueAsNewReasons,
			TargetWorkerDeploymentVersionChanged: incoming.WorkflowTaskTargetWorkerDeploymentVersionChanged,
			HistorySizeBytes:                     incoming.WorkflowTaskHistorySizeBytes,
			BuildId:                              incoming.WorkflowTaskBuildId,
			BuildIdRedirectCounter:               incoming.WorkflowTaskBuildIdRedirectCounter,
		})
		workflowTaskVersionUpdated = true
	}

	if incoming.WorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		ms.workflowTaskManager.deleteWorkflowTask()
		ms.RemoveSpeculativeWorkflowTaskTimeoutTask()
		if !workflowTaskVersionUpdated {
			// Source has speculative workflow task. We need to reset workflowTaskUpdated
			// because speculative workflow task is not a real workflow task and it is not
			// updated in mutation or snapshot.
			ms.workflowTaskUpdated = false
		}
	}

	doNotSync := func(v any) []any {
		info, ok := v.(*persistencespb.WorkflowExecutionInfo)
		if !ok || info == nil {
			return nil
		}
		ignoreFields := []any{
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
			&info.WorkflowTaskSuggestContinueAsNewReasons,
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

	return nil
}

func (ms *MutableStateImpl) syncSubStateMachinesByType(incoming map[string]*persistencespb.StateMachineMap) error {
	// check if there is node been deleted
	currentHSM := ms.HSM()
	incomingHSM, err := hsm.NewRoot(ms.shard.StateMachineRegistry(), StateMachineType, ms, incoming, ms)
	if err != nil {
		return err
	}

	if err := incomingHSM.Walk(func(incomingNode *hsm.Node) error {
		if incomingNode.Parent == nil {
			// skip root which is the entire mutable state
			return nil
		}
		incomingNodePath := incomingNode.Path()
		_, err := currentHSM.Child(incomingNodePath)
		if err != nil && errors.Is(err, hsm.ErrStateMachineNotFound) {
			ms.subStateMachineDeleted = true
			return nil
		}
		return err
	}); err != nil {
		return err
	}

	ms.executionInfo.SubStateMachinesByType = incoming
	ms.mustInitHSM()
	return nil
}

//revive:disable-next-line:cognitive-complexity
func (ms *MutableStateImpl) applyTombstones(
	tombstoneBatches []*persistencespb.StateMachineTombstoneBatch,
	currentVersionedTransition *persistencespb.VersionedTransition,
) error {
	var err error
	deletedChasmNodes := make(map[string]struct{})
	for _, tombstoneBatch := range tombstoneBatches {
		if transitionhistory.Compare(tombstoneBatch.VersionedTransition, currentVersionedTransition) <= 0 {
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
			case *persistencespb.StateMachineTombstone_StateMachinePath:
				err = ms.DeleteSubStateMachine(tombstone.GetStateMachinePath())
			case *persistencespb.StateMachineTombstone_ChasmNodePath:
				deletedChasmNodes[tombstone.GetChasmNodePath()] = struct{}{}
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

	return ms.chasmTree.ApplyMutation(chasm.NodesMutation{
		DeletedNodes: deletedChasmNodes,
	})
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
		ms.versionedTransitionInDB = ms.CurrentVersionedTransition()
	}
}

// GetEffectiveDeployment returns the effective deployment in the following order:
//  1. DeploymentVersionTransition.Deployment: this is returned when the wf is transitioning to a
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

func (ms *MutableStateImpl) GetWorkerDeploymentSA() string {
	versioningInfo := ms.GetExecutionInfo().GetVersioningInfo()
	if override := versioningInfo.GetVersioningOverride(); override != nil &&
		worker_versioning.OverrideIsPinned(override) {
		if v := override.GetPinned().GetVersion(); v != nil {
			return v.GetDeploymentName()
		}
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if vs := override.GetPinnedVersion(); vs != "" {
			v, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(vs)
			return v.GetDeploymentName()
		}
		//nolint:staticcheck // SA1019: worker versioning v0.30
		return override.GetDeployment().GetSeriesName()
	}
	if v := versioningInfo.GetDeploymentVersion(); v != nil {
		return v.GetDeploymentName()
	}
	return ms.GetExecutionInfo().GetWorkerDeploymentName()
}

func (ms *MutableStateImpl) GetWorkerDeploymentVersionSA() string {
	versioningInfo := ms.GetExecutionInfo().GetVersioningInfo()
	if override := versioningInfo.GetVersioningOverride(); override != nil &&
		worker_versioning.OverrideIsPinned(override) {
		if v := override.GetPinned().GetVersion(); v != nil {
			return worker_versioning.ExternalWorkerDeploymentVersionToString(v)
		}
		//nolint:staticcheck // SA1019: worker versioning v0.31
		if vs := override.GetPinnedVersion(); vs != "" {
			return worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(vs))
		}
		//nolint:staticcheck // SA1019: worker versioning v0.30
		return worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(override.GetDeployment()))
	}
	if v := versioningInfo.GetDeploymentVersion(); v != nil {
		return worker_versioning.ExternalWorkerDeploymentVersionToString(v)
	}
	//nolint:staticcheck // SA1019: worker versioning v0.31
	return worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(versioningInfo.GetVersion()))
}

func (ms *MutableStateImpl) GetWorkflowVersioningBehaviorSA() enumspb.VersioningBehavior {
	if override := ms.executionInfo.GetVersioningInfo().GetVersioningOverride(); override != nil {
		if override.GetAutoUpgrade() {
			return enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
		} else if worker_versioning.OverrideIsPinned(override) {
			return enumspb.VERSIONING_BEHAVIOR_PINNED
		}
		//nolint:staticcheck // SA1019: worker versioning v0.31 and v0.30
		return override.GetBehavior()
	}
	return ms.executionInfo.GetVersioningInfo().GetBehavior()
}

func (ms *MutableStateImpl) GetDeploymentTransition() *workflowpb.DeploymentTransition {
	vi := ms.GetExecutionInfo().GetVersioningInfo()
	if t := vi.GetVersionTransition(); t != nil {
		//nolint:staticcheck // SA1019: worker versioning v0.30
		ret := &workflowpb.DeploymentTransition{}
		if dv := t.GetDeploymentVersion(); dv != nil {
			ret.Deployment = worker_versioning.DeploymentFromExternalDeploymentVersion(dv)
		} else {
			//nolint:staticcheck // SA1019: worker versioning v0.31
			v, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(t.GetVersion())
			ret.Deployment = worker_versioning.DeploymentFromDeploymentVersion(v)
		}
		return ret
	}
	//nolint:staticcheck // SA1019: worker versioning v0.30
	return ms.GetExecutionInfo().GetVersioningInfo().GetDeploymentTransition()
}

// GetEffectiveVersioningBehavior returns the effective versioning behavior in the following
// order:
//  1. DeploymentVersionTransition: if there is a transition, then effective behavior is AUTO_UPGRADE.
//  2. VersioningOverride.Behavior: this is returned when user has set a behavior override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  3. Behavior: this is returned when there is no override (most common case). Behavior is
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
// This method must be called with a version different from the effective version.
func (ms *MutableStateImpl) StartDeploymentTransition(deployment *deploymentpb.Deployment, revisionNumber int64) error {
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

	preTransitionEffectiveDeployment := ms.GetEffectiveDeployment()
	if preTransitionEffectiveDeployment.Equal(deployment) {
		return serviceerror.NewInternal("start transition should receive a version different from effective version")
	}

	// Only store transition in VersionTransition but read from both VersionTransition and DeploymentVersionTransition.
	// Within VersionTransition, only store in DeploymentVersion, but read from Version too.
	//nolint:staticcheck // SA1019 deprecated DeploymentTransition will clean up later
	versioningInfo.DeploymentTransition = nil
	versioningInfo.VersionTransition = &workflowpb.DeploymentVersionTransition{
		// [cleanup-wv-3.1]
		DeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment),
	}

	// Because deployment has changed we:
	// - clear sticky queue to make sure the next WFT does not go to the old deployment
	// - reschedule the pending WFT so the old one is invalided
	ms.ClearStickyTaskQueue()

	err := ms.reschedulePendingWorkflowTask()
	if err != nil {
		return err
	}

	// DeploymentTransition has taken place, so we increment the DeploymentTransition metric
	metrics.StartDeploymentTransitionCounter.With(
		ms.metricsHandler.WithTags(
			metrics.NamespaceTag(ms.namespaceEntry.Name().String()),
			metrics.FromUnversionedTag(worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(preTransitionEffectiveDeployment))),
			metrics.ToUnversionedTag(worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment))),
		),
	).Record(1)

	ms.SetVersioningRevisionNumber(revisionNumber)

	return nil
}

func (ms *MutableStateImpl) GetVersioningRevisionNumber() int64 {
	return ms.GetExecutionInfo().GetVersioningInfo().GetRevisionNumber()
}

func (ms *MutableStateImpl) SetVersioningRevisionNumber(revisionNumber int64) {
	if ms.GetExecutionInfo().GetVersioningInfo() == nil {
		ms.GetExecutionInfo().VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
	}
	ms.GetExecutionInfo().GetVersioningInfo().RevisionNumber = revisionNumber
}

// reschedulePendingActivities reschedules all the activities that are not started, so they are
// scheduled against the right queue in matching.
func (ms *MutableStateImpl) reschedulePendingActivities(wftScheduleToClose time.Duration) error {
	for _, ai := range ms.GetPendingActivityInfos() {
		if ai.StartedEventId != common.EmptyEventID {
			// TODO: skip task generation also when activity is in backoff period
			// activity already started
			continue
		}

		// need to update stamp so the passive side regenerate the task
		err := ms.UpdateActivity(ai.ScheduledEventId, func(info *persistencespb.ActivityInfo, state historyi.MutableState) error {
			info.Stamp++
			// Updating scheduled time because we don't want the time spent on the transition wft to
			// be considered in the schedule-to-start latency calculation.
			// This is specifically needed for the cases that the activity is blocked not because of a
			// backlog but because of an ongoing transition. See ActivityStartDuringTransition error usage.

			info.ScheduledTime = timestamppb.New(info.ScheduledTime.AsTime().Add(wftScheduleToClose))
			// This ensures the schedule to start timer task is regenerate so we don't miss the timout.
			info.TimerTaskStatus &^= TimerTaskStatusCreatedScheduleToStart
			return nil
		})
		if err != nil {
			return err
		}
		// we only need to resend the activities to matching, no need to update timer tasks.
		err = ms.taskGenerator.GenerateActivityTasks(ai.ScheduledEventId)
		if err != nil {
			return err
		}
	}

	return nil
}

// reschedulePendingWorkflowTask reschedules the pending WFT if it is not started yet.
// The currently scheduled WFT will be rejected when attempting to start because its stamp changed.
func (ms *MutableStateImpl) reschedulePendingWorkflowTask() error {
	// If the WFT is started but not finished, we let it run its course
	// - once it's completed, failed or timed out a new one will be scheduled.
	if !ms.HasPendingWorkflowTask() || ms.HasStartedWorkflowTask() {
		return nil
	}

	// A speculative WFT cannot be rescheduled since it is added directly (without transfer task)
	// to Matching when being scheduled. It is protected by a timeout on both normal and sticky task queues.
	// If there is no poller for previous deployment, it will time out, and rescheduled as normal WFT.
	pendingTask := ms.GetPendingWorkflowTask()
	if pendingTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		ms.logInfo("start transition did not reschedule pending speculative task")
		return nil
	}

	// TODO (shahab): In case of transient task, consider converting it to normal because we want
	// user to see in the workflow history that we are trying to dispatch workflow tasks
	// to the new Version (say the old version has a failed WFT event, but the new version
	// is also failing the tasks, we want another failed WFT event with the info from
	// the worker in the new version).
	// The other solution is showing the ongoing transition in the UI and CLI so user knows
	// we're retrying on the new version.
	// Note that we cannot simply set attempt to 1 here, because it does not guarantee
	// generating new tasks all the time automatically by itself.

	// Increase the stamp ("version") to invalidate the pending non-speculative WFT.
	// We don't invalidate speculative WFTs because they are very latency sensitive.
	ms.executionInfo.WorkflowTaskStamp += 1
	ms.workflowTaskUpdated = true

	return ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(pendingTask.ScheduledEventID)
}

func (ms *MutableStateImpl) AddReapplyCandidateEvent(event *historypb.HistoryEvent) {
	if shouldReapplyEvent(ms.shard.StateMachineRegistry(), event) {
		ms.reapplyEventsCandidate = append(ms.reapplyEventsCandidate, event)
	}
}

func (ms *MutableStateImpl) GetReapplyCandidateEvents() []*historypb.HistoryEvent {
	return ms.reapplyEventsCandidate
}

func (ms *MutableStateImpl) IsSubStateMachineDeleted() bool {
	return ms.subStateMachineDeleted
}

func (ms *MutableStateImpl) SetSuccessorRunID(runID string) {
	ms.executionInfo.SuccessorRunId = runID
}

// ActivityMatchWorkflowRules checks if the activity matches any of the workflow rules
// and takes action based on the matched rule.
// If activity is changed in the result, it should be updated in the mutable state.
// In this case this function return true.
// If activity was not changed it will return false.
func ActivityMatchWorkflowRules(
	ms historyi.MutableState,
	timeSource clock.TimeSource,
	logger log.Logger,
	ai *persistencespb.ActivityInfo) bool {

	workflowRules := ms.GetNamespaceEntry().GetWorkflowRules()

	activityChanged := false
	now := timeSource.Now()

	for _, rule := range workflowRules {
		expirationTime := rule.GetSpec().GetExpirationTime()
		if expirationTime != nil && expirationTime.AsTime().Before(now) {
			// the rule is expired
			continue
		}
		match, err := MatchWorkflowRule(ms.GetExecutionInfo(), ms.GetExecutionState(), ai, rule.GetSpec())
		if err != nil {
			logError(logger, "error matching workflow rule", ms.GetExecutionInfo(), ms.GetExecutionState(), tag.Error(err))
			continue
		}
		if !match {
			continue
		}

		// activity matched
		for _, action := range rule.GetSpec().Actions {
			switch action.Variant.(type) {
			case *rulespb.WorkflowRuleAction_ActivityPause:
				// pause the activity
				if !ai.Paused {
					pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
						PauseTime: timestamppb.New(timeSource.Now()),
						PausedBy: &persistencespb.ActivityInfo_PauseInfo_RuleId{
							RuleId: rule.GetSpec().GetId(),
						},
					}
					if err = PauseActivity(ms, ai.ActivityId, pauseInfo); err != nil {
						logError(logger, "error pausing activity", ms.GetExecutionInfo(), ms.GetExecutionState(), tag.Error(err))
					}
					activityChanged = true
				}
			}
		}
	}

	return activityChanged
}

func logError(
	logger log.Logger,
	msg string,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	tags ...tag.Tag,
) {
	tags = append(tags, tag.WorkflowID(executionInfo.WorkflowId))
	tags = append(tags, tag.WorkflowRunID(executionState.RunId))
	tags = append(tags, tag.WorkflowNamespaceID(executionInfo.NamespaceId))
	logger.Error(msg, tags...)
}
