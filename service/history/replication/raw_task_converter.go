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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination raw_task_converter_mock.go

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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	SourceTaskConverterImpl struct {
		historyEngine  shard.Engine
		namespaceCache namespace.Registry
		serializer     serialization.Serializer
		config         *configs.Config
	}
	SourceTaskConverter interface {
		Convert(task tasks.Task, targetClusterID int32) (*replicationspb.ReplicationTask, error)
	}
	SourceTaskConverterProvider func(
		historyEngine shard.Engine,
		shardContext shard.Context,
		clientClusterName string, // Some task converter may use the client cluster name.
		serializer serialization.Serializer,
	) SourceTaskConverter

	syncVersionedTransitionTaskConverter struct {
		ctx              context.Context
		shardContext     shard.Context
		taskInfo         *tasks.SyncVersionedTransitionTask
		shardID          int32
		workflowCache    wcache.Cache
		eventBlobCache   persistence.XDCCache
		replicationCache ProgressCache
		targetClusterID  int32
		executionManager persistence.ExecutionManager
		mutableState     workflow.MutableState
		logger           log.Logger
	}
)

func NewSourceTaskConverter(
	historyEngine shard.Engine,
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
	ctx, cancel = newTaskContext(nsName, c.config.ReplicationTaskApplyTimeout())
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
	shardContext shard.Context,
	taskInfo *tasks.SyncActivityTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		workflowCache,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
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
					},
				},
				VisibilityTime: timestamppb.New(taskInfo.VisibilityTimestamp),
			}, nil
		},
	)
}

func convertWorkflowStateReplicationTask(
	ctx context.Context,
	shardContext shard.Context,
	taskInfo *tasks.SyncWorkflowStateTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		workflowCache,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			state, _ := mutableState.GetWorkflowStateStatus()
			if state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
				return nil, nil
			}
			workflowMutableState := mutableState.CloneToProto()
			if err := workflow.SanitizeMutableState(workflowMutableState); err != nil {
				return nil, err
			}
			if err := common.DiscardUnknownProto(workflowMutableState); err != nil {
				return nil, err
			}
			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
				SourceTaskId: taskInfo.TaskID,
				Priority:     taskInfo.Priority,
				Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
					SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
						WorkflowState: workflowMutableState,
					},
				},
				VisibilityTime: timestamppb.New(taskInfo.VisibilityTimestamp),
			}, nil
		},
	)
}

func convertSyncHSMReplicationTask(
	ctx context.Context,
	shardContext shard.Context,
	taskInfo *tasks.SyncHSMTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		workflowCache,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
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
	shardContext shard.Context,
	taskInfo *tasks.SyncVersionedTransitionTask,
	shardID int32,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	replicationCache ProgressCache,
	targetClusterID int32,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		shardContext,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		workflowCache,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			svttConverter := newSyncVersionedTransitionTaskConverter(
				ctx,
				shardContext,
				taskInfo,
				shardID,
				workflowCache,
				eventBlobCache,
				replicationCache,
				targetClusterID,
				executionManager,
				mutableState,
				logger,
			)
			return svttConverter.convert()

		},
	)
}

func convertHistoryReplicationTask(
	ctx context.Context,
	shardContext shard.Context,
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
	shardContext shard.Context,
	workflowKey definition.WorkflowKey,
	workflowCache wcache.Cache,
	action func(workflow.MutableState) (*replicationspb.ReplicationTask, error),
) (retReplicationTask *replicationspb.ReplicationTask, retError error) {
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
		return nil, err
	}
	defer func() { release(retError) }()

	ms, err := wfContext.LoadMutableState(ctx, shardContext)
	switch err.(type) {
	case nil:
		return action(ms)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func getVersionHistoryAndEvents(
	ctx context.Context,
	shardContext shard.Context,
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
			nextEventID,
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
	shardContext shard.Context,
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
	shardContext shard.Context,
	workflowKey definition.WorkflowKey,
	workflowCache wcache.Cache,
	eventID int64,
	eventVersion int64,
) (_ []*historyspb.VersionHistoryItem, _ []byte, _ *workflowspb.BaseExecutionInfo, retError error) {
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
	ctx context.Context,
	shardContext shard.Context,
	taskInfo *tasks.SyncVersionedTransitionTask,
	shardID int32,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	replicationCache ProgressCache,
	targetClusterID int32,
	executionManager persistence.ExecutionManager,
	mutableState workflow.MutableState,
	logger log.Logger,
) *syncVersionedTransitionTaskConverter {
	return &syncVersionedTransitionTaskConverter{
		ctx:              ctx,
		shardContext:     shardContext,
		taskInfo:         taskInfo,
		shardID:          shardID,
		workflowCache:    workflowCache,
		eventBlobCache:   eventBlobCache,
		replicationCache: replicationCache,
		targetClusterID:  targetClusterID,
		executionManager: executionManager,
		mutableState:     mutableState,
		logger:           logger,
	}
}

func (c *syncVersionedTransitionTaskConverter) convert() (*replicationspb.ReplicationTask, error) {
	progress := c.replicationCache.Get(c.taskInfo.RunID, c.targetClusterID)

	if progress.VersionedTransitionSent(c.taskInfo.VersionedTransition) {
		return c.generateVerifyVersionedTransitionTask()
	}

	if !c.onCurrentBranch(c.taskInfo.VersionedTransition) {
		if c.taskInfo.FirstEventID == common.EmptyEventID && c.taskInfo.NextEventID == common.EmptyEventID && len(c.taskInfo.NewRunID) == 0 {
			return nil, nil
		}
		return c.generateBackfillHistoryTask()
	}

	// lastSyncedTransition := progress.LastSyncedTransition()
	// TODO: SyncWorkflowStateMutation & SyncWorkflowStateSnapshot based on lastSyncedTransition
	return nil, nil
}

func (c *syncVersionedTransitionTaskConverter) onCurrentBranch(versionedTransition *persistencespb.VersionedTransition) bool {
	return workflow.TransitionHistoryStalenessCheck(c.mutableState.GetExecutionInfo().TransitionHistory, versionedTransition) == nil
}

func (c *syncVersionedTransitionTaskConverter) generateVerifyVersionedTransitionTask() (*replicationspb.ReplicationTask, error) {
	versionHistoryIndex, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
		c.mutableState.GetExecutionInfo().VersionHistories,
		versionhistory.NewVersionHistoryItem(
			c.taskInfo.FirstEventID,
			c.taskInfo.VersionedTransition.NamespaceFailoverVersion,
		),
	)
	if err != nil {
		return nil, err
	}
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: c.taskInfo.TaskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId:         c.taskInfo.NamespaceID,
				WorkflowId:          c.taskInfo.WorkflowID,
				RunId:               c.taskInfo.RunID,
				NewRunId:            c.taskInfo.NewRunID,
				EventVersionHistory: c.mutableState.GetExecutionInfo().VersionHistories.Histories[versionHistoryIndex].Items,
				NextEventId:         c.taskInfo.NextEventID,
			},
		},
		VersionedTransition: c.taskInfo.VersionedTransition,
		VisibilityTime:      timestamppb.New(c.taskInfo.VisibilityTimestamp),
	}, nil
}

func (c *syncVersionedTransitionTaskConverter) generateBackfillHistoryTask() (*replicationspb.ReplicationTask, error) {
	historyItems, taskEvents, taskNewEvents, _, err := getVersionHistoryAndEventsWithNewRun(
		c.ctx,
		c.shardContext,
		c.shardID,
		definition.NewWorkflowKey(c.taskInfo.NamespaceID, c.taskInfo.WorkflowID, c.taskInfo.RunID),
		c.taskInfo.VersionedTransition.NamespaceFailoverVersion,
		c.taskInfo.FirstEventID,
		c.taskInfo.NextEventID,
		c.taskInfo.NewRunID,
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
		if item.GetEventId() >= c.taskInfo.NextEventID-1 {
			taskHistoryItems = append(taskHistoryItems, versionhistory.NewVersionHistoryItem(c.taskInfo.NextEventID-1, item.GetVersion()))
			break
		}
		taskHistoryItems = append(taskHistoryItems, item)
	}

	err = c.replicationCache.Update(c.taskInfo.RunID, c.targetClusterID, nil, taskHistoryItems)
	if err != nil {
		return nil, err
	}
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK,
		SourceTaskId: c.taskInfo.TaskID,
		Priority:     c.taskInfo.Priority,
		Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId:         c.taskInfo.NamespaceID,
				WorkflowId:          c.taskInfo.WorkflowID,
				RunId:               c.taskInfo.RunID,
				EventVersionHistory: taskHistoryItems,
				EventBatches:        taskEvents,
				NewRunInfo: &replicationspb.NewRunInfo{
					RunId:      c.taskInfo.NewRunID,
					EventBatch: taskNewEvents,
				},
			},
		},
		VersionedTransition: c.taskInfo.VersionedTransition,
		VisibilityTime:      timestamppb.New(c.taskInfo.VisibilityTimestamp),
	}, nil
}
