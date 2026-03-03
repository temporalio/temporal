package replication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/softassert"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination executable_task_mock.go

const (
	taskStatePending = int32(ctasks.TaskStatePending)

	taskStateAborted   = int32(ctasks.TaskStateAborted)
	taskStateCancelled = int32(ctasks.TaskStateCancelled)
	taskStateAcked     = int32(ctasks.TaskStateAcked)
	taskStateNacked    = int32(ctasks.TaskStateNacked)
)

const (
	ResendAttempt = 2
)

var (
	ErrResendAttemptExceeded = serviceerror.NewInternal("resend history attempts exceeded")
)

type (
	ExecutableTask interface {
		TaskID() int64
		TaskCreationTime() time.Time
		SourceClusterName() string
		Ack()
		Nack(err error)
		Abort()
		Cancel()
		Reschedule()
		IsRetryableError(err error) bool
		RetryPolicy() backoff.RetryPolicy
		State() ctasks.State
		TerminalState() bool
		Attempt() int
		Resend(
			ctx context.Context,
			remoteCluster string,
			retryErr *serviceerrors.RetryReplication,
			remainingAttempt int,
		) (bool, error)
		DeleteWorkflow(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
		) (retError error)
		GetNamespaceInfo(
			ctx context.Context,
			namespaceID string,
			businessID string,
		) (string, bool, error)
		SyncState(
			ctx context.Context,
			syncStateErr *serviceerrors.SyncState,
			remainingAttempt int,
		) (bool, error)
		ReplicationTask() *replicationspb.ReplicationTask
		MarkPoisonPill() error
		BackFillEvents(
			ctx context.Context,
			remoteCluster string,
			workflowKey definition.WorkflowKey,
			startEventId int64, // inclusive
			startEventVersion int64,
			endEventId int64, // inclusive
			endEventVersion int64,
			newRunId string,
		) error
		MarkTaskDuplicated()
		MarkExecutionStart()
		GetPriority() enumsspb.TaskPriority
		NamespaceName() string
	}
	ExecutableTaskImpl struct {
		ProcessToolBox

		// immutable data
		taskID            int64
		metricsTag        string
		taskCreationTime  time.Time
		taskReceivedTime  time.Time
		sourceClusterName string
		sourceShardKey    ClusterShardKey
		taskPriority      enumsspb.TaskPriority
		replicationTask   *replicationspb.ReplicationTask

		// mutable data
		taskState              int32
		attempt                int32
		namespace              atomic.Value
		markPoisonPillAttempts int
		isDuplicated           bool
		taskExecuteStartTime   time.Time
	}
)

func NewExecutableTask(
	processToolBox ProcessToolBox,
	taskID int64,
	metricsTag string,
	taskCreationTime time.Time,
	taskReceivedTime time.Time,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableTaskImpl {
	return &ExecutableTaskImpl{
		ProcessToolBox:         processToolBox,
		taskID:                 taskID,
		metricsTag:             metricsTag,
		taskCreationTime:       taskCreationTime,
		taskReceivedTime:       taskReceivedTime,
		sourceClusterName:      sourceClusterName,
		sourceShardKey:         sourceShardKey,
		taskPriority:           replicationTask.GetPriority(),
		replicationTask:        replicationTask,
		taskState:              taskStatePending,
		attempt:                1,
		markPoisonPillAttempts: 0,
	}
}

func (e *ExecutableTaskImpl) TaskID() int64 {
	return e.taskID
}

func (e *ExecutableTaskImpl) TaskCreationTime() time.Time {
	return e.taskCreationTime
}

func (e *ExecutableTaskImpl) SourceClusterName() string {
	return e.sourceClusterName
}

func (e *ExecutableTaskImpl) ReplicationTask() *replicationspb.ReplicationTask {
	return e.replicationTask
}

func (e *ExecutableTaskImpl) Ack() {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		return
	}
	if !atomic.CompareAndSwapInt32(&e.taskState, taskStatePending, taskStateAcked) {
		e.Ack() // retry ack
	}

	now := time.Now().UTC()
	e.emitFinishMetrics(now)
}

func (e *ExecutableTaskImpl) Nack(err error) {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		return
	}
	if !atomic.CompareAndSwapInt32(&e.taskState, taskStatePending, taskStateNacked) {
		e.Nack(err) // retry nack
	}

	e.Logger.Error(fmt.Sprintf(
		"replication task: %v encountered nack event",
		e.taskID,
	), tag.Error(err))
	now := time.Now().UTC()
	e.emitFinishMetrics(now)

	var namespaceName string
	item := e.namespace.Load()
	if item != nil {
		namespaceName = item.(namespace.Name).String()
	}
	metrics.ReplicationTasksFailed.With(e.MetricsHandler).Record(
		1,
		metrics.OperationTag(e.metricsTag),
		metrics.NamespaceTag(namespaceName),
	)
}

func (e *ExecutableTaskImpl) Abort() {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		return
	}
	if !atomic.CompareAndSwapInt32(&e.taskState, taskStatePending, taskStateAborted) {
		e.Abort() // retry abort
	}

	e.ThrottledLogger.Debug(fmt.Sprintf(
		"replication task: %v encountered abort event",
		e.taskID,
	))
	// should not emit metrics since abort means shutdown
}

func (e *ExecutableTaskImpl) Cancel() {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		return
	}
	if !atomic.CompareAndSwapInt32(&e.taskState, taskStatePending, taskStateCancelled) {
		e.Cancel() // retry cancel
	}

	e.ThrottledLogger.Debug(fmt.Sprintf(
		"replication task: %v encountered cancellation event",
		e.taskID,
	))
	now := time.Now().UTC()
	e.emitFinishMetrics(now)
}

func (e *ExecutableTaskImpl) Reschedule() {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		return
	}

	e.ThrottledLogger.Info(fmt.Sprintf(
		"replication task: %v scheduled for retry",
		e.taskID,
	))
	atomic.AddInt32(&e.attempt, 1)
}

func (e *ExecutableTaskImpl) IsRetryableError(err error) bool {
	switch err.(type) {
	case *serviceerror.InvalidArgument, *serviceerror.DataLoss:
		return false
	default:
		return true
	}
}

func (e *ExecutableTaskImpl) RetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(e.Config.ReplicationExecutableTaskErrorRetryWait()).
		WithBackoffCoefficient(e.Config.ReplicationExecutableTaskErrorRetryBackoffCoefficient()).
		WithMaximumInterval(e.Config.ReplicationExecutableTaskErrorRetryMaxInterval()).
		WithMaximumAttempts(e.Config.ReplicationExecutableTaskErrorRetryMaxAttempts()).
		WithExpirationInterval(e.Config.ReplicationExecutableTaskErrorRetryExpiration())
}

func (e *ExecutableTaskImpl) State() ctasks.State {
	return ctasks.State(atomic.LoadInt32(&e.taskState))
}

func (e *ExecutableTaskImpl) TerminalState() bool {
	state := atomic.LoadInt32(&e.taskState)
	return state != taskStatePending
}

func (e *ExecutableTaskImpl) Attempt() int {
	return int(atomic.LoadInt32(&e.attempt))
}

func (e *ExecutableTaskImpl) MarkTaskDuplicated() {
	e.isDuplicated = true
}

func (e *ExecutableTaskImpl) MarkExecutionStart() {
	if e.taskExecuteStartTime.IsZero() {
		e.taskExecuteStartTime = time.Now().UTC()
	}
}

func (e *ExecutableTaskImpl) GetPriority() enumsspb.TaskPriority {
	return e.taskPriority
}

func (e *ExecutableTaskImpl) NamespaceName() string {
	item := e.namespace.Load()
	if item != nil {
		return item.(namespace.Name).String()
	}
	return ""
}

func (e *ExecutableTaskImpl) emitFinishMetrics(
	now time.Time,
) {
	if e.isDuplicated {
		if e.replicationTask.RawTaskInfo != nil {
			metrics.ReplicationDuplicatedTaskCount.With(e.MetricsHandler).Record(1,
				metrics.OperationTag(e.metricsTag),
				metrics.NamespaceTag(e.replicationTask.RawTaskInfo.NamespaceId))
		}
		return
	}
	nsTag := metrics.NamespaceUnknownTag()
	item := e.namespace.Load()
	if item != nil {
		nsTag = metrics.NamespaceTag(item.(namespace.Name).String())
	}

	// Only emit queue/processing latency metrics if execution actually started
	if !e.taskExecuteStartTime.IsZero() {
		// Queue latency: time from task creation to execution start
		queueLatency := e.taskExecuteStartTime.Sub(e.taskReceivedTime)
		metrics.ReplicationTaskQueueLatency.With(e.MetricsHandler).Record(
			queueLatency,
			metrics.OperationTag(e.metricsTag),
			nsTag,
			metrics.SourceClusterTag(e.sourceClusterName),
		)

		// Processing latency: time from execution start to ACK/NACK
		processingLatency := now.Sub(e.taskExecuteStartTime)
		metrics.ReplicationTaskProcessingLatency.With(e.MetricsHandler).Record(
			processingLatency,
			metrics.OperationTag(e.metricsTag),
			nsTag,
		)
		if processingLatency > 10*time.Second && e.replicationTask != nil && e.replicationTask.RawTaskInfo != nil {
			e.ThrottledLogger.Warn(fmt.Sprintf(
				"replication task latency is too long: queue=%.2fs processing=%.2fs",
				queueLatency.Seconds(),
				processingLatency.Seconds(),
			),
				tag.WorkflowNamespace(e.NamespaceName()),
				tag.WorkflowID(e.replicationTask.RawTaskInfo.WorkflowId),
				tag.WorkflowRunID(e.replicationTask.RawTaskInfo.RunId),
				tag.ReplicationTask(e.replicationTask.GetRawTaskInfo()),
				tag.ShardID(e.Config.GetShardID(namespace.ID(e.replicationTask.RawTaskInfo.NamespaceId), e.replicationTask.RawTaskInfo.WorkflowId)),
				tag.AttemptCount(int64(e.Attempt())),
			)
		}
	}

	replicationLatency := now.Sub(e.taskCreationTime)
	metrics.ReplicationLatency.With(e.MetricsHandler).Record(
		replicationLatency,
		metrics.OperationTag(e.metricsTag),
		nsTag,
		metrics.SourceClusterTag(e.sourceClusterName),
	)
	metrics.ReplicationTaskTransmissionLatency.With(e.MetricsHandler).Record(
		e.taskReceivedTime.Sub(e.taskCreationTime),
		metrics.OperationTag(e.metricsTag),
		nsTag,
		metrics.SourceClusterTag(e.sourceClusterName),
	)

	// Emit task attempt count
	metrics.ReplicationTasksAttempt.With(e.MetricsHandler).Record(
		int64(e.Attempt()),
		metrics.OperationTag(e.metricsTag),
		nsTag,
	)
}

func (e *ExecutableTaskImpl) Resend(
	ctx context.Context,
	remoteCluster string,
	retryErr *serviceerrors.RetryReplication,
	remainingAttempt int,
) (bool, error) {
	remainingAttempt--
	if remainingAttempt < 0 {
		e.Logger.Error("resend history attempts exceeded",
			tag.WorkflowNamespaceID(retryErr.NamespaceId),
			tag.WorkflowID(retryErr.WorkflowId),
			tag.WorkflowRunID(retryErr.RunId),
			tag.Value(retryErr),
			tag.Error(ErrResendAttemptExceeded),
		)
		return false, ErrResendAttemptExceeded
	}

	var namespaceName string
	item := e.namespace.Load()
	if item != nil {
		namespaceName = item.(namespace.Name).String()
	}
	metrics.ReplicationTasksBackFill.With(e.MetricsHandler).Record(
		1,
		metrics.OperationTag(e.metricsTag+"Resend"),
		metrics.NamespaceTag(namespaceName),
		metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
	)
	startTime := time.Now().UTC()
	defer func() {
		metrics.ReplicationTasksBackFillLatency.With(e.MetricsHandler).Record(
			time.Since(startTime),
			metrics.OperationTag(e.metricsTag+"Resend"),
			metrics.NamespaceTag(namespaceName),
			metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
		)
	}()
	resendErr := e.ProcessToolBox.ResendHandler.ResendHistoryEvents(
		ctx,
		remoteCluster,
		namespace.ID(retryErr.NamespaceId),
		retryErr.WorkflowId,
		retryErr.RunId,
		retryErr.StartEventId,
		retryErr.StartEventVersion,
		retryErr.EndEventId,
		retryErr.EndEventVersion,
	)
	switch resendErr := resendErr.(type) {
	case nil:
		// no-op
		return true, nil
	case *serviceerror.NotFound:
		e.Logger.Error(
			"workflow not found in source cluster, proceed to cleanup",
			tag.WorkflowNamespaceID(retryErr.NamespaceId),
			tag.WorkflowID(retryErr.WorkflowId),
			tag.WorkflowRunID(retryErr.RunId),
		)
		// workflow is not found in source cluster, cleanup workflow in target cluster
		return false, e.DeleteWorkflow(
			ctx,
			definition.NewWorkflowKey(
				retryErr.NamespaceId,
				retryErr.WorkflowId,
				retryErr.RunId,
			),
		)
	case *serviceerrors.RetryReplication:
		// it is possible that resend will trigger another resend, e.g.
		// 1. replicating a workflow which is a reset workflow (call this workflow `new workflow`)
		// 2. base workflow (call this workflow `old workflow`) of reset workflow is deleted on
		//	src cluster and never replicated to target cluster
		// 3. when any of events of the new workflow arrive at target cluster
		//  a. using base workflow info to resend until branching point between old & new workflow
		//  b. attempting to use old workflow history events to replay for mutable state then apply new workflow events
		//  c. attempt failed due to old workflow does not exist
		//  d. return error to resend new workflow before the branching point

		if resendErr.Equal(retryErr) {
			return false, softassert.UnexpectedDataLoss(e.Logger,
				"failed to get requested data while resending history", nil,
				tag.WorkflowNamespaceID(retryErr.NamespaceId),
				tag.WorkflowID(retryErr.WorkflowId),
				tag.WorkflowRunID(retryErr.RunId),
				tag.String("first-resend-error", retryErr.Error()),
				tag.String("second-resend-error", resendErr.Error()),
			)
		}
		// handle 2nd resend error, then 1st resend error
		_, err := e.Resend(ctx, remoteCluster, resendErr, remainingAttempt)
		if err == nil {
			return e.Resend(ctx, remoteCluster, retryErr, remainingAttempt)
		}
		e.Logger.Error("error resend 2nd workflow history for history event",
			tag.WorkflowNamespaceID(resendErr.NamespaceId),
			tag.WorkflowID(resendErr.WorkflowId),
			tag.WorkflowRunID(resendErr.RunId),
			tag.String("first-resend-error", retryErr.Error()),
			tag.String("second-resend-error", resendErr.Error()),
			tag.Error(err),
		)
		return false, resendErr
	default:
		e.Logger.Error("error resend history for history event",
			tag.WorkflowNamespaceID(retryErr.NamespaceId),
			tag.WorkflowID(retryErr.WorkflowId),
			tag.WorkflowRunID(retryErr.RunId),
			tag.String("first-resend-error", retryErr.Error()),
			tag.String("second-resend-error", resendErr.Error()),
		)
		return false, resendErr
	}
}

//nolint:revive // cognitive complexity 29 (> max enabled 25)
func (e *ExecutableTaskImpl) BackFillEvents(
	ctx context.Context,
	remoteCluster string,
	workflowKey definition.WorkflowKey,
	startEventId int64, // inclusive
	startEventVersion int64,
	endEventId int64, // inclusive
	endEventVersion int64,
	newRunId string, // only verify task should pass this value
) error {
	if len(newRunId) != 0 && e.replicationTask.GetTaskType() != enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK {
		return serviceerror.NewInternal("newRunId should be empty for non verify task")
	}

	var namespaceName string
	item := e.namespace.Load()
	if item != nil {
		namespaceName = item.(namespace.Name).String()
	}
	metrics.ReplicationTasksBackFill.With(e.MetricsHandler).Record(
		1,
		metrics.OperationTag(e.metricsTag+"BackFill"),
		metrics.NamespaceTag(namespaceName),
		metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
	)
	startTime := time.Now().UTC()
	defer func() {
		metrics.ReplicationTasksBackFillLatency.With(e.MetricsHandler).Record(
			time.Since(startTime),
			metrics.OperationTag(e.metricsTag+"BackFill"),
			metrics.NamespaceTag(namespaceName),
			metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
		)
	}()
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	)
	if err != nil {
		return err
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}

	var eventsBatch [][]*historypb.HistoryEvent
	var newRunEvents []*historypb.HistoryEvent
	var versionHistory []*historyspb.VersionHistoryItem
	const EmptyVersion = int64(-1) // 0 is a valid event version when namespace is local
	var eventsVersion = EmptyVersion
	isLastEvent := false
	if len(newRunId) != 0 {
		iterator := e.ProcessToolBox.RemoteHistoryFetcher.GetSingleWorkflowHistoryPaginatedIteratorInclusive(
			ctx,
			remoteCluster,
			namespace.ID(workflowKey.NamespaceID),
			workflowKey.WorkflowID,
			newRunId,
			1,
			endEventVersion, // continue as new run's first event batch should have the same version as the last event of the old run
			1,
			endEventVersion,
		)
		if !iterator.HasNext() {
			return serviceerror.NewInternalf("failed to get new run history when backfill")
		}
		batch, err := iterator.Next()
		if err != nil {
			return serviceerror.NewInternalf("failed to get new run history when backfill: %v", err)
		}
		events, err := e.Serializer.DeserializeEvents(batch.RawEventBatch)
		if err != nil {
			return serviceerror.NewInternalf("failed to deserailize run history events when backfill: %v", err)
		}
		newRunEvents = events
	}

	applyFn := func() error {
		backFillRequest := &historyi.BackfillHistoryEventsRequest{
			WorkflowKey:         workflowKey,
			SourceClusterName:   e.SourceClusterName(),
			VersionedHistory:    e.ReplicationTask().VersionedTransition,
			VersionHistoryItems: versionHistory,
			Events:              eventsBatch,
		}
		if isLastEvent && len(newRunId) > 0 && len(newRunEvents) > 0 {
			backFillRequest.NewEvents = newRunEvents
			backFillRequest.NewRunID = newRunId
		}
		err := engine.BackfillHistoryEvents(ctx, backFillRequest)
		if err != nil {
			return serviceerror.NewInternalf("failed to backfill: %v", err)
		}
		eventsBatch = nil
		versionHistory = nil
		eventsVersion = EmptyVersion
		return nil
	}
	iterator := e.ProcessToolBox.RemoteHistoryFetcher.GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		ctx,
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
	)
	for iterator.HasNext() {
		batch, err := iterator.Next()
		if err != nil {
			return err
		}
		events, err := e.Serializer.DeserializeEvents(batch.RawEventBatch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return serviceerror.NewInvalidArgument("Empty batch received from remote during resend")
		}
		if len(eventsBatch) != 0 && len(versionHistory) != 0 {
			if !versionhistory.IsEqualVersionHistoryItems(versionHistory, batch.VersionHistory.Items) ||
				(eventsVersion != EmptyVersion && eventsVersion != events[0].Version) {
				err := applyFn()
				if err != nil {
					return err
				}
			}
		}
		eventsBatch = append(eventsBatch, events)
		if events[len(events)-1].GetEventId() == endEventId {
			isLastEvent = true
		}
		versionHistory = batch.VersionHistory.Items
		eventsVersion = events[0].Version
		if len(eventsBatch) >= e.Config.ReplicationResendMaxBatchCount() {
			err := applyFn()
			if err != nil {
				return err
			}
		}
	}
	if len(eventsBatch) > 0 {
		err := applyFn()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *ExecutableTaskImpl) SyncState(
	ctx context.Context,
	syncStateErr *serviceerrors.SyncState,
	remainingAttempt int,
) (bool, error) {

	// TODO: check & update remainingAttempt

	remoteAdminClient, err := e.ClientBean.GetRemoteAdminClient(e.sourceClusterName)
	if err != nil {
		return false, err
	}

	targetClusterInfo := e.ClusterMetadata.GetAllClusterInfo()[e.ClusterMetadata.GetCurrentClusterName()]

	// Remove branch tokens from version histories to reduce request size
	versionHistories := syncStateErr.VersionHistories
	if versionHistories != nil {
		versionHistories = versionhistory.CopyVersionHistories(versionHistories)
		for _, history := range versionHistories.Histories {
			history.BranchToken = nil
		}
	}

	req := &adminservice.SyncWorkflowStateRequest{
		NamespaceId: syncStateErr.NamespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: syncStateErr.WorkflowId,
			RunId:      syncStateErr.RunId,
		},
		ArchetypeId:         syncStateErr.ArchetypeId,
		VersionedTransition: syncStateErr.VersionedTransition,
		VersionHistories:    versionHistories,
		TargetClusterId:     int32(targetClusterInfo.InitialFailoverVersion),
	}
	resp, err := remoteAdminClient.SyncWorkflowState(ctx, req)
	if err != nil {
		var resourceExhaustedError *serviceerror.ResourceExhausted
		if errors.As(err, &resourceExhaustedError) {
			return false, serviceerror.NewInvalidArgumentf("sync workflow state failed due to resource exhausted: %v, request payload size: %v", err, req.Size())
		}
		logger := log.With(e.Logger,
			tag.WorkflowNamespaceID(syncStateErr.NamespaceId),
			tag.WorkflowID(syncStateErr.WorkflowId),
			tag.WorkflowRunID(syncStateErr.RunId),
			tag.ReplicationTask(e.replicationTask),
		)

		var workflowNotReady *serviceerror.WorkflowNotReady
		if errors.As(err, &workflowNotReady) {
			logger.Info("Dropped replication task as source mutable state has buffered events.", tag.Error(err))
			return false, nil
		}
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) {
			logger.Error(
				"workflow not found in source cluster, proceed to cleanup")
			// workflow is not found in source cluster, cleanup workflow in target cluster
			return false, e.DeleteWorkflow(
				ctx,
				definition.NewWorkflowKey(
					syncStateErr.NamespaceId,
					syncStateErr.WorkflowId,
					syncStateErr.RunId,
				),
			)
		}
		var failedPreconditionErr *serviceerror.FailedPrecondition
		if !errors.As(err, &failedPreconditionErr) {
			return false, err
		}
		// Unable to perform sync state. Transition history maybe disabled in source cluster.
		// Add task equivalents back to source cluster.
		taskEquivalents := e.replicationTask.GetRawTaskInfo().GetTaskEquivalents()

		if len(taskEquivalents) == 0 {
			// Just drop the task since there's nothing to replicate in event-based stack.
			logger.Info("Dropped replication task as there's no event-based replication task equivalent.")
			return false, nil
		}

		tasksToAdd := make([]*adminservice.AddTasksRequest_Task, 0, len(taskEquivalents))
		for _, taskEquivalent := range taskEquivalents {
			blob, err := e.Serializer.ReplicationTaskInfoToBlob(taskEquivalent)
			if err != nil {
				return false, err
			}

			tasksToAdd = append(tasksToAdd, &adminservice.AddTasksRequest_Task{
				CategoryId: tasks.CategoryIDReplication,
				Blob:       blob,
			})
		}

		_, err := remoteAdminClient.AddTasks(ctx, &adminservice.AddTasksRequest{
			ShardId: e.sourceShardKey.ShardID,
			Tasks:   tasksToAdd,
		})
		return false, err
	}

	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	)
	if err != nil {
		return false, err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return false, err
	}
	err = engine.ReplicateVersionedTransition(ctx, syncStateErr.ArchetypeId, resp.VersionedTransitionArtifact, e.SourceClusterName())
	if err == nil || errors.Is(err, consts.ErrDuplicate) {
		return true, nil
	}
	return false, err
}

func (e *ExecutableTaskImpl) DeleteWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
) (retError error) {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	_, err = engine.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: workflowKey.NamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		ClosedWorkflowOnly: false,
	})
	var notFoundErr *serviceerror.NotFound
	if errors.As(err, &notFoundErr) {
		return nil
	}
	return err
}

func (e *ExecutableTaskImpl) GetNamespaceInfo(
	ctx context.Context,
	namespaceID string,
	businessID string,
) (string, bool, error) {
	namespaceEntry, err := e.NamespaceCache.GetNamespaceByID(namespace.ID(namespaceID))
	switch err.(type) {
	case nil:
		if e.replicationTask.VersionedTransition != nil && e.replicationTask.VersionedTransition.NamespaceFailoverVersion > namespaceEntry.FailoverVersion(businessID) {
			_, err = e.ProcessToolBox.EagerNamespaceRefresher.SyncNamespaceFromSourceCluster(ctx, namespace.ID(namespaceID), e.sourceClusterName)
			if err != nil {
				return "", false, err
			}
		}
	case *serviceerror.NamespaceNotFound:
		_, err = e.ProcessToolBox.EagerNamespaceRefresher.SyncNamespaceFromSourceCluster(ctx, namespace.ID(namespaceID), e.sourceClusterName)
		if err != nil {
			e.ThrottledLogger.Error("Failed to SyncNamespaceFromSourceCluster", tag.Error(err))
			return "", false, nil
		}
	default:
		return "", false, err
	}
	namespaceEntry, err = e.NamespaceCache.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return "", false, err
	}
	// need to make sure ns in cache is up-to-date
	if e.replicationTask.VersionedTransition != nil && namespaceEntry.FailoverVersion(businessID) < e.replicationTask.VersionedTransition.NamespaceFailoverVersion {
		return "", false, serviceerror.NewInternalf("cannot process task because namespace failover version is not up to date after sync, task version: %v, namespace version: %v",
			e.replicationTask.VersionedTransition.NamespaceFailoverVersion, namespaceEntry.FailoverVersion(businessID))
	}

	e.namespace.Store(namespaceEntry.Name())
	if namespaceEntry.State() == enumspb.NAMESPACE_STATE_DELETED {
		return namespaceEntry.Name().String(), false, nil
	}
	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range namespaceEntry.ClusterNames(businessID) {
		if e.ClusterMetadata.GetCurrentClusterName() == targetCluster {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return namespaceEntry.Name().String(), shouldProcessTask, nil
}

func (e *ExecutableTaskImpl) MarkPoisonPill() error {
	taskInfo := e.ReplicationTask().GetRawTaskInfo()

	if e.markPoisonPillAttempts >= MarkPoisonPillMaxAttempts {
		e.Logger.Error("MarkPoisonPill reached max attempts",
			tag.SourceCluster(e.SourceClusterName()),
			tag.ReplicationTask(taskInfo),
		)
		return nil
	}
	e.markPoisonPillAttempts++

	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.replicationTask.RawTaskInfo.NamespaceId),
		e.replicationTask.RawTaskInfo.WorkflowId,
	)
	if err != nil {
		return err
	}

	e.Logger.Error("Enqueued replication task to DLQ",
		tag.TargetShardID(shardContext.GetShardID()),
		tag.SourceShardID(e.sourceShardKey.ShardID),
		tag.WorkflowNamespaceID(e.replicationTask.RawTaskInfo.NamespaceId),
		tag.WorkflowID(e.replicationTask.RawTaskInfo.WorkflowId),
		tag.WorkflowRunID(e.replicationTask.RawTaskInfo.RunId),
		tag.TaskID(e.taskID),
		tag.SourceCluster(e.SourceClusterName()),
		tag.ReplicationTask(taskInfo),
	)

	ctx, cancel := newTaskContext(
		e.replicationTask.RawTaskInfo.NamespaceId,
		e.Config.ReplicationTaskApplyTimeout(),
		headers.SystemPreemptableCallerInfo,
	)
	defer cancel()

	return writeTaskToDLQ(ctx, e.DLQWriter, e.sourceShardKey.ShardID, e.SourceClusterName(), shardContext.GetShardID(), taskInfo)
}

func newTaskContext(
	namespaceName string,
	timeout time.Duration,
	callerInfo headers.CallerInfo,
) (context.Context, context.CancelFunc) {
	ctx := headers.SetCallerInfo(
		context.Background(),
		callerInfo,
	)
	ctx = headers.SetCallerName(ctx, namespaceName)
	return context.WithTimeout(ctx, timeout)
}

func getReplicaitonCallerInfo(priority enumsspb.TaskPriority) headers.CallerInfo {
	switch priority {
	case enumsspb.TASK_PRIORITY_LOW:
		return headers.SystemPreemptableCallerInfo
	default:
		return headers.SystemBackgroundLowCallerInfo
	}
}
