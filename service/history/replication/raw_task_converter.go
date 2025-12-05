//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination raw_task_converter_mock.go

package replication

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	SourceTaskConverterImpl struct {
		historyEngine  historyi.Engine
		namespaceCache namespace.Registry
		serializer     serialization.Serializer
		config         *configs.Config
	}
	SourceTaskConverter interface {
		Convert(task tasks.Task, targetClusterID int32, priority enumsspb.TaskPriority) (*replicationspb.ReplicationTask, error)
	}
	SourceTaskConverterProvider func(
		historyEngine historyi.Engine,
		shardContext historyi.ShardContext,
		clientClusterName string, // Some task converter may use the client cluster name.
		serializer serialization.Serializer,
	) SourceTaskConverter

	syncVersionedTransitionTaskConverter struct {
		shardContext       historyi.ShardContext
		shardID            int32
		workflowCache      wcache.Cache
		eventBlobCache     persistence.XDCCache
		replicationCache   ProgressCache
		executionManager   persistence.ExecutionManager
		syncStateRetriever SyncStateRetriever
		logger             log.Logger
	}
)

func NewSourceTaskConverter(
	historyEngine historyi.Engine,
	namespaceCache namespace.Registry,
	serializer serialization.Serializer,
	config *configs.Config,
) *SourceTaskConverterImpl {
	return &SourceTaskConverterImpl{
		historyEngine:  historyEngine,
		namespaceCache: namespaceCache,
		serializer:     serializer,
		config:         config,
	}
}

func (c *SourceTaskConverterImpl) Convert(
	task tasks.Task,
	targetClusterID int32,
	priority enumsspb.TaskPriority,
) (*replicationspb.ReplicationTask, error) {

	var ctx context.Context
	var cancel context.CancelFunc
	var nsName string
	namespaceEntry, err := c.namespaceCache.GetNamespaceByID(
		namespace.ID(task.GetNamespaceID()),
	)
	if err != nil {
		// if there is error, then blindly send the task, better safe than sorry
		nsName = namespace.EmptyName.String()
	}
	if namespaceEntry != nil {
		nsName = namespaceEntry.Name().String()
	}
	callerInfo := getReplicaitonCallerInfo(priority)
	ctx, cancel = newTaskContext(nsName, c.config.ReplicationTaskApplyTimeout(), callerInfo)
	defer cancel()
	replicationTask, err := c.historyEngine.ConvertReplicationTask(ctx, task, targetClusterID)
	if err != nil {
		return nil, err
	}
	if replicationTask != nil {
		rawTaskInfo, err := c.serializer.ParseReplicationTaskInfo(task)
		if err != nil {
			return nil, err
		}
		replicationTask.RawTaskInfo = rawTaskInfo
	}
	return replicationTask, nil
}

func convertActivityStateReplicationTask(
	ctx context.Context,
	shardContext historyi.ShardContext,
	taskInfo *tasks.SyncActivityTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		chasm.WorkflowArchetypeID,
		workflowCache,
		func(mutableState historyi.MutableState, releaseFunc historyi.ReleaseWorkflowContextFunc) (*replicationspb.ReplicationTask, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, nil
			}
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.ScheduledEventID)
			if !ok {
				return nil, nil
			}
			if activityInfo.Version != taskInfo.Version {
				return nil, nil
			}

			// The activity may be in a scheduled state
			var startedTime *timestamppb.Timestamp
			if activityInfo.StartedEventId != common.EmptyEventID {
				startedTime = activityInfo.StartedTime
			}

			// Version history uses when replicate the sync activity task
			versionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
			currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
			if err != nil {
				return nil, err
			}

			lastStartedBuildId := activityInfo.GetLastIndependentlyAssignedBuildId()
			if lastStartedBuildId == "" {
				lastStartedBuildId = activityInfo.GetUseWorkflowBuildIdInfo().GetLastUsedBuildId()
			}
			// We will use field to distinguish between nil and zero value for backward compatibility
			retryInitialInterval := activityInfo.RetryInitialInterval
			if retryInitialInterval == nil {
				retryInitialInterval = &durationpb.Duration{
					Nanos: 0,
				}
			}

			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
				SourceTaskId: taskInfo.TaskID,
				Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
					SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
						NamespaceId:                taskInfo.NamespaceID,
						WorkflowId:                 taskInfo.WorkflowID,
						RunId:                      taskInfo.RunID,
						Version:                    activityInfo.Version,
						ScheduledEventId:           activityInfo.ScheduledEventId,
						ScheduledTime:              activityInfo.ScheduledTime,
						StartedEventId:             activityInfo.StartedEventId,
						StartVersion:               activityInfo.StartVersion,
						StartedTime:                startedTime,
						LastHeartbeatTime:          activityInfo.LastHeartbeatUpdateTime,
						Details:                    activityInfo.LastHeartbeatDetails,
						Attempt:                    activityInfo.Attempt,
						LastFailure:                activityInfo.RetryLastFailure,
						LastWorkerIdentity:         activityInfo.RetryLastWorkerIdentity,
						LastStartedBuildId:         lastStartedBuildId,
						LastStartedRedirectCounter: activityInfo.GetUseWorkflowBuildIdInfo().GetLastRedirectCounter(),
						BaseExecutionInfo:          persistence.CopyBaseWorkflowInfo(mutableState.GetBaseWorkflowInfo()),
						VersionHistory:             versionhistory.CopyVersionHistory(currentVersionHistory),
						FirstScheduledTime:         activityInfo.FirstScheduledTime,
						LastAttemptCompleteTime:    activityInfo.LastAttemptCompleteTime,
						Stamp:                      activityInfo.Stamp,
						Paused:                     activityInfo.Paused,
						RetryInitialInterval:       retryInitialInterval,
						RetryMaximumInterval:       activityInfo.RetryMaximumInterval,
						RetryMaximumAttempts:       activityInfo.RetryMaximumAttempts,
						RetryBackoffCoefficient:    activityInfo.RetryBackoffCoefficient,
					},
				},
				VisibilityTime: timestamppb.New(taskInfo.VisibilityTimestamp),
			}, nil
		},
	)
}

func convertWorkflowStateReplicationTask(
	ctx context.Context,
	shardContext historyi.ShardContext,
	taskInfo *tasks.SyncWorkflowStateTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		chasm.WorkflowArchetypeID,
		workflowCache,
		func(mutableState historyi.MutableState, releaseFunc historyi.ReleaseWorkflowContextFunc) (*replicationspb.ReplicationTask, error) {
			state, _ := mutableState.GetWorkflowStateStatus()
			if state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
				return nil, nil
			}
			workflowMutableState := mutableState.CloneToProto()
			workflow.SanitizeMutableState(workflowMutableState)
			if err := common.DiscardUnknownProto(workflowMutableState); err != nil {
				return nil, err
			}

			isCloseTransferTaskAcked := isCloseTransferTaskAckedForWorkflow(shardContext, &tasks.CloseExecutionTask{
				WorkflowKey: mutableState.GetWorkflowKey(),
				TaskID:      mutableState.GetExecutionInfo().GetCloseTransferTaskId(),
			})

			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
				SourceTaskId: taskInfo.TaskID,
				Priority:     taskInfo.Priority,
				Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
					SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
						WorkflowState:            workflowMutableState,
						IsForceReplication:       taskInfo.IsForceReplication,
						IsCloseTransferTaskAcked: isCloseTransferTaskAcked,
					},
				},
				VisibilityTime: timestamppb.New(taskInfo.VisibilityTimestamp),
			}, nil
		},
	)
}

func convertSyncHSMReplicationTask(
	ctx context.Context,
	shardContext historyi.ShardContext,
	taskInfo *tasks.SyncHSMTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		chasm.WorkflowArchetypeID,
		workflowCache,
		func(mutableState historyi.MutableState, releaseFunc historyi.ReleaseWorkflowContextFunc) (*replicationspb.ReplicationTask, error) {
			// HSM can be updated after workflow is completed
			// so no check on workflow state here.

			if mutableState.HasBufferedEvents() {
				// we can't sync HSM when there's buffered events
				// as current state could depend on those buffered events
				return nil, nil
			}

			versionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
			currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
			if err != nil {
				return nil, err
			}

			stateMachineNode := common.CloneProto(mutableState.HSM().InternalRepr())
			workflow.SanitizeStateMachineNode(stateMachineNode)
			if err := common.DiscardUnknownProto(stateMachineNode); err != nil {
				return nil, err
			}

			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
				SourceTaskId: taskInfo.TaskID,
				Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{
					SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
						NamespaceId:      taskInfo.NamespaceID,
						WorkflowId:       taskInfo.WorkflowID,
						RunId:            taskInfo.RunID,
						VersionHistory:   versionhistory.CopyVersionHistory(currentVersionHistory),
						StateMachineNode: stateMachineNode,
					},
				},
				VisibilityTime: timestamppb.New(taskInfo.VisibilityTimestamp),
			}, nil
		},
	)
}

func convertSyncVersionedTransitionTask(
	ctx context.Context,
	taskInfo *tasks.SyncVersionedTransitionTask,
	targetClusterID int32,
	converter *syncVersionedTransitionTaskConverter,
) (*replicationspb.ReplicationTask, error) {
	if taskInfo.ArchetypeID == chasm.UnspecifiedArchetypeID {
		taskInfo.ArchetypeID = chasm.WorkflowArchetypeID
	}
	return generateStateReplicationTask(
		ctx,
		converter.shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		taskInfo.ArchetypeID,
		converter.workflowCache,
		func(mutableState historyi.MutableState, releaseFunc historyi.ReleaseWorkflowContextFunc) (*replicationspb.ReplicationTask, error) {
			return converter.convert(ctx, taskInfo, targetClusterID, mutableState, releaseFunc)
		},
	)
}

func convertHistoryReplicationTask(
	ctx context.Context,
	shardContext historyi.ShardContext,
	taskInfo *tasks.HistoryReplicationTask,
	shardID int32,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	config *configs.Config,
) (*replicationspb.ReplicationTask, error) {
	currentVersionHistory, currentEvents, newEvents, currentBaseWorkflowInfo, err := getVersionHistoryAndEventsWithNewRun(
		ctx,
		shardContext,
		shardID,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		taskInfo.Version,
		taskInfo.FirstEventID,
		taskInfo.NextEventID,
		taskInfo.NewRunID,
		workflowCache,
		eventBlobCache,
		executionManager,
		logger,
	)
	if err != nil {
		return nil, err
	}
	if currentVersionHistory == nil {
		return nil, nil
	}

	var events *commonpb.DataBlob
	var eventsBatches []*commonpb.DataBlob
	if config.ReplicationMultipleBatches() {
		eventsBatches = currentEvents
	} else {
		if len(currentEvents) != 1 {
			return nil, serviceerror.NewInternal("replicatorQueueProcessor encountered more than 1 NDC raw event batch")
		}
		events = currentEvents[0]
	}

	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		SourceTaskId: taskInfo.TaskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         taskInfo.NamespaceID,
				WorkflowId:          taskInfo.WorkflowID,
				RunId:               taskInfo.RunID,
				BaseExecutionInfo:   currentBaseWorkflowInfo,
				VersionHistoryItems: currentVersionHistory,
				Events:              events,
				EventsBatches:       eventsBatches,
				NewRunEvents:        newEvents,
				NewRunId:            taskInfo.NewRunID,
			},
		},
		VisibilityTime: timestamppb.New(taskInfo.VisibilityTimestamp),
	}, nil
}

func generateStateReplicationTask(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	archetypeID chasm.ArchetypeID,
	workflowCache wcache.Cache,
	action func(mutableState historyi.MutableState, releaseFunc historyi.ReleaseWorkflowContextFunc) (*replicationspb.ReplicationTask, error),
) (retReplicationTask *replicationspb.ReplicationTask, retError error) {
	wfContext, release, err := workflowCache.GetOrCreateChasmExecution(
		ctx,
		shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		archetypeID,
		locks.PriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	ms, err := wfContext.LoadMutableState(ctx, shardContext)
	switch err.(type) {
	case nil:
		return action(ms, release) // do not access mutable state after this point
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func getVersionHistoryAndEvents(
	ctx context.Context,
	shardContext historyi.ShardContext,
	shardID int32,
	workflowKey definition.WorkflowKey,
	eventVersion int64,
	firstEventID int64,
	nextEventID int64,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
) ([]*historyspb.VersionHistoryItem, []*commonpb.DataBlob, *workflowspb.BaseExecutionInfo, error) {
	if eventBlobCache != nil {
		if xdcCacheValue, ok := eventBlobCache.Get(persistence.NewXDCCacheKey(
			workflowKey,
			firstEventID,
			eventVersion,
		)); ok {
			return xdcCacheValue.VersionHistoryItems, xdcCacheValue.EventBlobs, xdcCacheValue.BaseWorkflowInfo, nil
		}
	}
	versionHistory, branchToken, baseWorkflowInfo, err := getBranchToken(
		ctx,
		shardContext,
		workflowKey,
		workflowCache,
		firstEventID,
		eventVersion,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	if versionHistory == nil {
		return nil, nil, nil, nil
	}
	eventBatches, err := getEventsBlob(ctx, shardID, branchToken, firstEventID, nextEventID, executionManager)
	if err != nil {
		return nil, nil, nil, convertGetHistoryError(workflowKey, logger, err)
	}
	return versionHistory, eventBatches, baseWorkflowInfo, nil
}

func getVersionHistoryAndEventsWithNewRun(
	ctx context.Context,
	shardContext historyi.ShardContext,
	shardID int32,
	workflowKey definition.WorkflowKey,
	eventVersion int64,
	firstEventID int64,
	nextEventID int64,
	newRunID string,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
) ([]*historyspb.VersionHistoryItem, []*commonpb.DataBlob, *commonpb.DataBlob, *workflowspb.BaseExecutionInfo, error) {
	versionHistory, eventBatches, baseWorkflowInfo, err := getVersionHistoryAndEvents(
		ctx,
		shardContext,
		shardID,
		definition.NewWorkflowKey(workflowKey.NamespaceID, workflowKey.WorkflowID, workflowKey.RunID),
		eventVersion,
		firstEventID,
		nextEventID,
		workflowCache,
		eventBlobCache,
		executionManager,
		logger,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if versionHistory == nil {
		return nil, nil, nil, nil, nil
	}

	var newEvents *commonpb.DataBlob
	if len(newRunID) != 0 {
		newVersionHistory, newEventBlob, _, err := getVersionHistoryAndEvents(
			ctx,
			shardContext,
			shardID,
			definition.NewWorkflowKey(workflowKey.NamespaceID, workflowKey.WorkflowID, newRunID),
			eventVersion,
			common.FirstEventID,
			// when generating the replication task,
			// we validated that new run contains only 1 replication task (event batch)
			common.FirstEventID+1,
			workflowCache,
			eventBlobCache,
			executionManager,
			logger,
		)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if len(newEventBlob) != 1 {
			return nil, nil, nil, nil, serviceerror.NewInternal("replicatorQueueProcessor encountered more than 1 NDC raw event batch for new run")
		}
		if newVersionHistory != nil {
			newEvents = newEventBlob[0]
		}
	}
	return versionHistory, eventBatches, newEvents, baseWorkflowInfo, nil
}

func getBranchToken(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	workflowCache wcache.Cache,
	eventID int64,
	eventVersion int64,
) (_ []*historyspb.VersionHistoryItem, _ []byte, _ *workflowspb.BaseExecutionInfo, retError error) {
	// CHASM runs don't have events so should never reach here.
	// We can continue to use GetOrCreateWorkflowExecution.
	wfContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		locks.PriorityLow,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() { release(retError) }()

	ms, err := wfContext.LoadMutableState(ctx, shardContext)
	switch err.(type) {
	case nil:
		return persistence.GetXDCCacheValue(ms.GetExecutionInfo(), eventID, eventVersion)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return nil, nil, nil, nil
	default:
		return nil, nil, nil, err
	}
}

func getEventsBlob(
	ctx context.Context,
	shardID int32,
	branchToken []byte,
	firstEventID int64,
	nextEventID int64,
	executionManager persistence.ExecutionManager,
) ([]*commonpb.DataBlob, error) {
	var eventBatchBlobs []*commonpb.DataBlob
	var pageToken []byte
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}

	for {
		resp, err := executionManager.ReadRawHistoryBranch(ctx, req)
		if err != nil {
			return nil, err
		}

		req.NextPageToken = resp.NextPageToken
		eventBatchBlobs = append(eventBatchBlobs, resp.HistoryEventBlobs...)

		if len(req.NextPageToken) == 0 {
			break
		}
	}

	return eventBatchBlobs, nil
}

func convertGetHistoryError(
	workflowKey definition.WorkflowKey,
	logger log.Logger,
	err error,
) error {
	switch err.(type) {
	case *serviceerror.NotFound:
		// bypass this corrupted workflow to unblock the replication queue.
		logger.Error("Cannot get history from missing workflow",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return nil
	case *serviceerror.DataLoss:
		// bypass this corrupted workflow to unblock the replication queue.
		logger.Error("Cannot get history from corrupted workflow",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return nil
	default:
		return err
	}
}

func newSyncVersionedTransitionTaskConverter(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	replicationCache ProgressCache,
	executionManager persistence.ExecutionManager,
	syncStateRetriever SyncStateRetriever,
	logger log.Logger,
) *syncVersionedTransitionTaskConverter {
	return &syncVersionedTransitionTaskConverter{
		shardContext:       shardContext,
		workflowCache:      workflowCache,
		eventBlobCache:     eventBlobCache,
		replicationCache:   replicationCache,
		executionManager:   executionManager,
		syncStateRetriever: syncStateRetriever,
		shardID:            shardContext.GetShardID(),
		logger:             logger,
	}
}

func (c *syncVersionedTransitionTaskConverter) convert(
	ctx context.Context,
	taskInfo *tasks.SyncVersionedTransitionTask,
	targetClusterID int32,
	mutableState historyi.MutableState,
	releaseFunc historyi.ReleaseWorkflowContextFunc,
) (*replicationspb.ReplicationTask, error) {
	executionInfo := mutableState.GetExecutionInfo()

	// If workflow is not on any versionedTransition (in an unknown state from state-based replication perspective),
	// we can't convert this raw task to a replication task, instead we need to rely on its task equivalents.
	if len(executionInfo.TransitionHistory) == 0 {
		isWorkflow := mutableState.IsWorkflow()
		releaseFunc(nil)
		if !isWorkflow {
			return nil, serviceerror.NewInternalf("chasm execution not on any versioned transition, is state-based replication enabled? execution key: %v", taskInfo.WorkflowKey)
		}
		return c.convertTaskEquivalents(ctx, taskInfo, targetClusterID)
	}

	progress := c.replicationCache.Get(taskInfo.RunID, targetClusterID)

	if progress.VersionedTransitionSent(taskInfo.VersionedTransition) {
		return c.generateVerifyVersionedTransitionTask(taskInfo, mutableState)
	}

	if !c.onCurrentBranch(mutableState, taskInfo.VersionedTransition) {
		if taskInfo.FirstEventID == common.EmptyEventID && taskInfo.NextEventID == common.EmptyEventID && len(taskInfo.NewRunID) == 0 {
			return nil, nil
		}
		releaseFunc(nil) // release wf lock before retrieving history events
		return c.generateBackfillHistoryTask(ctx, taskInfo, targetClusterID)
	}

	if mutableState.HasBufferedEvents() {
		// we can't sync state when there's buffered events
		// as current state could depend on those buffered events
		return nil, nil
	}

	var targetHistoryItems [][]*historyspb.VersionHistoryItem
	if progress != nil {
		targetHistoryItems = progress.eventVersionHistoryItems
	}
	currentHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}
	currentHistoryCopy := versionhistory.CopyVersionHistory(currentHistory)

	// Extract data from mutable state before releasing the lock
	closeTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: mutableState.GetWorkflowKey(),
		TaskID:      executionInfo.GetCloseTransferTaskId(),
	}

	var syncStateResult *SyncStateResult
	if taskInfo.IsFirstTask {
		syncStateResult, err = c.syncStateRetriever.GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
			ctx,
			taskInfo.NamespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: taskInfo.WorkflowID,
				RunId:      taskInfo.RunID,
			},
			mutableState,
			releaseFunc,
			taskInfo.VersionedTransition,
		)
	} else {
		syncStateResult, err = c.syncStateRetriever.GetSyncWorkflowStateArtifactFromMutableState(
			ctx,
			taskInfo.NamespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: taskInfo.WorkflowID,
				RunId:      taskInfo.RunID,
			},
			mutableState,
			progress.LastSyncedTransition(),
			targetHistoryItems,
			releaseFunc,
		)
	}

	if err != nil {
		return nil, err
	}

	// WARNING: do not access mutable state after this point. If you are using mutable state in this function, be warned that the
	// releaseFunc that is being passed into this function is what is used to release the lock we are holding on mutable state. If
	// you use mutable state after the releaseFunc has been called, you will be accessing mutable state without holding the lock.
	// Deep copy what you need.

	syncStateResult.VersionedTransitionArtifact.IsCloseTransferTaskAcked = c.isCloseTransferTaskAcked(closeTransferTask)
	syncStateResult.VersionedTransitionArtifact.IsForceReplication = taskInfo.IsForceReplication

	err = c.replicationCache.Update(taskInfo.RunID, targetClusterID, syncStateResult.VersionedTransitionHistory, currentHistoryCopy.Items)
	if err != nil {
		return nil, err
	}
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
		SourceTaskId: taskInfo.TaskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				VersionedTransitionArtifact: syncStateResult.VersionedTransitionArtifact,
				NamespaceId:                 taskInfo.NamespaceID,
				WorkflowId:                  taskInfo.WorkflowID,
				RunId:                       taskInfo.RunID,
				ArchetypeId:                 taskInfo.ArchetypeID,
			},
		},
		VersionedTransition: taskInfo.VersionedTransition,
		VisibilityTime:      timestamppb.New(taskInfo.VisibilityTimestamp),
	}, nil
}

func (c *syncVersionedTransitionTaskConverter) onCurrentBranch(mutableState historyi.MutableState, versionedTransition *persistencespb.VersionedTransition) bool {
	return transitionhistory.StalenessCheck(mutableState.GetExecutionInfo().TransitionHistory, versionedTransition) == nil
}

func (c *syncVersionedTransitionTaskConverter) generateVerifyVersionedTransitionTask(
	taskInfo *tasks.SyncVersionedTransitionTask,
	mutableState historyi.MutableState,
) (*replicationspb.ReplicationTask, error) {
	currentHistory, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().VersionHistories)
	if err != nil {
		return nil, err
	}
	var nextEventId = taskInfo.NextEventID
	if nextEventId == common.EmptyEventID && taskInfo.LastVersionHistoryItem != nil {
		nextEventId = taskInfo.LastVersionHistoryItem.GetEventId() + 1
	}

	var eventVersionHistory []*historyspb.VersionHistoryItem
	if nextEventId != common.EmptyEventID {
		lastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(currentHistory, nextEventId-1)
		if err != nil {
			return nil, err
		}
		capItems, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(currentHistory, &historyspb.VersionHistoryItem{
			EventId: nextEventId - 1,
			Version: lastEventVersion,
		})
		if err != nil {
			return nil, err
		}

		eventVersionHistory = capItems.Items
	}

	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: taskInfo.TaskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId:         taskInfo.NamespaceID,
				WorkflowId:          taskInfo.WorkflowID,
				RunId:               taskInfo.RunID,
				ArchetypeId:         taskInfo.ArchetypeID,
				NewRunId:            taskInfo.NewRunID,
				EventVersionHistory: eventVersionHistory,
				NextEventId:         nextEventId,
			},
		},
		VersionedTransition: taskInfo.VersionedTransition,
		VisibilityTime:      timestamppb.New(taskInfo.VisibilityTimestamp),
	}, nil
}

func (c *syncVersionedTransitionTaskConverter) generateBackfillHistoryTask(
	ctx context.Context,
	taskInfo *tasks.SyncVersionedTransitionTask,
	targetClusterID int32,
) (*replicationspb.ReplicationTask, error) {
	if taskInfo.FirstEventID == common.EmptyEventID &&
		taskInfo.NextEventID == common.EmptyEventID &&
		len(taskInfo.NewRunID) == 0 {
		return nil, nil
	}

	historyItems, taskEvents, taskNewEvents, _, err := getVersionHistoryAndEventsWithNewRun(
		ctx,
		c.shardContext,
		c.shardID,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		taskInfo.FirstEventVersion,
		taskInfo.FirstEventID,
		taskInfo.NextEventID,
		taskInfo.NewRunID,
		c.workflowCache,
		c.eventBlobCache,
		c.executionManager,
		c.logger,
	)
	if err != nil {
		return nil, err
	}

	// truncate historyItems to task's last event
	var taskHistoryItems []*historyspb.VersionHistoryItem
	for _, item := range historyItems {
		if item.GetEventId() >= taskInfo.NextEventID-1 {
			taskHistoryItems = append(taskHistoryItems, versionhistory.NewVersionHistoryItem(taskInfo.NextEventID-1, item.GetVersion()))
			break
		}
		taskHistoryItems = append(taskHistoryItems, item)
	}

	err = c.replicationCache.Update(taskInfo.RunID, targetClusterID, nil, taskHistoryItems)
	if err != nil {
		return nil, err
	}
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK,
		SourceTaskId: taskInfo.TaskID,
		Priority:     taskInfo.Priority,
		Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId:         taskInfo.NamespaceID,
				WorkflowId:          taskInfo.WorkflowID,
				RunId:               taskInfo.RunID,
				EventVersionHistory: taskHistoryItems,
				EventBatches:        taskEvents,
				NewRunInfo: &replicationspb.NewRunInfo{
					RunId:      taskInfo.NewRunID,
					EventBatch: taskNewEvents,
				},
			},
		},
		VersionedTransition: taskInfo.VersionedTransition,
		VisibilityTime:      timestamppb.New(taskInfo.VisibilityTimestamp),
	}, nil
}

func (c *syncVersionedTransitionTaskConverter) isCloseTransferTaskAcked(
	closeTransferTask *tasks.CloseExecutionTask,
) bool {
	return isCloseTransferTaskAckedForWorkflow(c.shardContext, closeTransferTask)
}

func isCloseTransferTaskAckedForWorkflow(
	shardContext historyi.ShardContext,
	closeTransferTask *tasks.CloseExecutionTask,
) bool {
	if closeTransferTask.TaskID == 0 {
		return false
	}

	transferQueueState, ok := shardContext.GetQueueState(tasks.CategoryTransfer)
	if !ok {
		return false
	}

	return queues.IsTaskAcked(closeTransferTask, transferQueueState)
}

func (c *syncVersionedTransitionTaskConverter) convertTaskEquivalents(
	ctx context.Context,
	taskInfo *tasks.SyncVersionedTransitionTask,
	targetClusterID int32,
) (*replicationspb.ReplicationTask, error) {
	if len(taskInfo.TaskEquivalents) == 0 {
		// no task equivalents, nothing to do
		c.logger.Info("No task equivalents for sync versioned transition task, dropping the task.",
			tag.WorkflowNamespaceID(taskInfo.NamespaceID),
			tag.WorkflowID(taskInfo.WorkflowID),
			tag.WorkflowRunID(taskInfo.RunID),
			tag.TaskKey(taskInfo.GetKey()),
			tag.Task(taskInfo))
		return nil, nil
	}
	if len(taskInfo.TaskEquivalents) == 1 {
		// when there is only one task equivalent, we can directly convert it to a replication task
		historyEngine, err := c.shardContext.GetEngine(ctx)
		if err != nil {
			return nil, err
		}
		taskEquivalent := taskInfo.TaskEquivalents[0]
		taskEquivalent.SetTaskID(taskInfo.GetTaskID())
		taskEquivalent.SetVisibilityTime(taskInfo.GetVisibilityTime())
		// TODO: set workflow key as well so we don't have to persist it multiple times
		return historyEngine.ConvertReplicationTask(ctx, taskEquivalent, targetClusterID)
	}
	// When there are multiple task equivalents, we have to write them back to the replication queue,
	// as multiple tasks can't share the same taskID.
	// AddTasks() method will handle:
	// 1. The allocation of taskID (and task visibilityTimestamp)
	// 2. The case where namespace is in handover state in which case, the AddTasks() request will fail
	// and an ErrNamespaceHandover will be returned.
	return nil, c.shardContext.AddTasks(
		ctx,
		&persistence.AddHistoryTasksRequest{
			ShardID:     c.shardID,
			NamespaceID: taskInfo.NamespaceID,
			WorkflowID:  taskInfo.WorkflowID,
			ArchetypeID: chasm.WorkflowArchetypeID, // only workflow has task equivalents
			Tasks: map[tasks.Category][]tasks.Task{
				tasks.CategoryReplication: taskInfo.TaskEquivalents,
			},
		},
	)
}
