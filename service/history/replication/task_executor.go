//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_executor_mock.go

package replication

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication/eventhandler"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	TaskExecutor interface {
		Execute(ctx context.Context, replicationTask *replicationspb.ReplicationTask, forceApply bool) error
	}

	TaskExecutorParams struct {
		RemoteCluster        string // TODO: Remove this remote cluster from executor then it can use singleton.
		Shard                historyi.ShardContext
		RemoteHistoryFetcher eventhandler.HistoryPaginatedFetcher
		DeleteManager        deletemanager.DeleteManager
		WorkflowCache        wcache.Cache
	}

	TaskExecutorProvider func(params TaskExecutorParams) TaskExecutor

	taskExecutorImpl struct {
		currentCluster       string
		remoteCluster        string
		shardContext         historyi.ShardContext
		namespaceRegistry    namespace.Registry
		remoteHistoryFetcher eventhandler.HistoryPaginatedFetcher
		deleteManager        deletemanager.DeleteManager
		workflowCache        wcache.Cache
		metricsHandler       metrics.Handler
		logger               log.Logger
	}
)

// NewTaskExecutor creates a replication task executor
// The executor uses by 1) DLQ replication task handler 2) history replication task processor
func NewTaskExecutor(
	remoteCluster string,
	shardContext historyi.ShardContext,
	remoteHistoryFetcher eventhandler.HistoryPaginatedFetcher,
	deleteManager deletemanager.DeleteManager,
	workflowCache wcache.Cache,
) TaskExecutor {
	return &taskExecutorImpl{
		currentCluster:       shardContext.GetClusterMetadata().GetCurrentClusterName(),
		remoteCluster:        remoteCluster,
		shardContext:         shardContext,
		namespaceRegistry:    shardContext.GetNamespaceRegistry(),
		remoteHistoryFetcher: remoteHistoryFetcher,
		deleteManager:        deleteManager,
		workflowCache:        workflowCache,
		metricsHandler:       shardContext.GetMetricsHandler(),
		logger:               shardContext.GetLogger(),
	}
}

func (e *taskExecutorImpl) Execute(
	ctx context.Context,
	replicationTask *replicationspb.ReplicationTask,
	forceApply bool,
) error {
	var err error
	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK:
		// Shard status will be sent as part of the Replication message without kafka
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		err = e.handleActivityTask(ctx, replicationTask, forceApply)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		err = e.handleHistoryReplicationTask(ctx, replicationTask, forceApply)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		err = e.handleSyncWorkflowStateTask(ctx, replicationTask, forceApply)
	default:
		// NOTE: not handling SyncHSMTask in this deprecated code path, task will go to DLQ
		e.logger.Error("Unknown replication task type.", tag.ReplicationTask(replicationTask))
		err = ErrUnknownReplicationTask
	}

	return err
}

func (e *taskExecutorImpl) handleActivityTask(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetSyncActivityTaskAttributes()
	doContinue, err := e.filterTask(namespace.ID(attr.GetNamespaceId()), attr.WorkflowId, forceApply)
	if err != nil || !doContinue {
		return err
	}

	startTime := time.Now().UTC()
	defer func() {
		metrics.ServiceLatency.With(e.metricsHandler).Record(
			time.Since(startTime),
			metrics.OperationTag(metrics.SyncActivityTaskScope),
			metrics.NamespaceTag(attr.GetNamespaceId()),
		)
	}()

	request := &historyservice.SyncActivityRequest{
		NamespaceId:                attr.NamespaceId,
		WorkflowId:                 attr.WorkflowId,
		RunId:                      attr.RunId,
		Version:                    attr.Version,
		ScheduledEventId:           attr.ScheduledEventId,
		ScheduledTime:              attr.ScheduledTime,
		StartedEventId:             attr.StartedEventId,
		StartVersion:               attr.StartVersion,
		StartedTime:                attr.StartedTime,
		LastHeartbeatTime:          attr.LastHeartbeatTime,
		Details:                    attr.Details,
		Attempt:                    attr.Attempt,
		LastFailure:                attr.LastFailure,
		LastWorkerIdentity:         attr.LastWorkerIdentity,
		LastStartedBuildId:         attr.LastStartedBuildId,
		LastStartedRedirectCounter: attr.LastStartedRedirectCounter,
		VersionHistory:             attr.GetVersionHistory(),
	}
	namespaceName, _ := e.namespaceRegistry.GetNamespaceName(namespace.ID(attr.NamespaceId))
	ctx, cancel := e.newTaskContext(ctx, namespaceName)
	defer cancel()

	// This might be extra cost if the workflow belongs to local shard.
	// Add a wrapper of the history client to call history engine directly if it becomes an issue.
	_, err = e.shardContext.GetHistoryClient().SyncActivity(ctx, request)
	switch retryErr := err.(type) {
	case nil:
		return nil

	case *serviceerrors.RetryReplication:
		metrics.ClientRequests.With(e.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRereplicationByActivityReplicationScope),
			metrics.NamespaceTag(namespaceName.String()),
			metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
		)
		startTime := time.Now().UTC()
		defer func() {
			metrics.ClientLatency.With(e.metricsHandler).Record(
				time.Since(startTime),
				metrics.OperationTag(metrics.HistoryRereplicationByActivityReplicationScope),
				metrics.NamespaceTag(namespaceName.String()),
				metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
			)
		}()

		resendErr := e.resend(
			ctx,
			e.remoteCluster,
			namespace.ID(retryErr.NamespaceId),
			retryErr.WorkflowId,
			retryErr.RunId,
			retryErr.StartEventId,
			retryErr.StartEventVersion,
			retryErr.EndEventId,
			retryErr.EndEventVersion,
		)
		switch resendErr.(type) {
		case *serviceerror.NotFound:
			// workflow is not found in source cluster, cleanup workflow in target cluster
			return e.cleanupWorkflowExecution(ctx, retryErr.NamespaceId, retryErr.WorkflowId, retryErr.RunId)
		case nil:
			// no-op
		default:
			e.logger.Error("error resend history for history event", tag.Error(resendErr))
			return err
		}
		// This might be extra cost if the workflow belongs to local shard.
		// Add a wrapper of the history client to call history engine directly if it becomes an issue.
		_, err = e.shardContext.GetHistoryClient().SyncActivity(ctx, request)
		return err

	default:
		if errors.Is(err, consts.ErrDuplicate) {
			return nil
		}
		return err
	}
}

func (e *taskExecutorImpl) handleHistoryReplicationTask(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetHistoryTaskAttributes()
	doContinue, err := e.filterTask(namespace.ID(attr.GetNamespaceId()), attr.WorkflowId, forceApply)
	if err != nil || !doContinue {
		return err
	}

	startTime := time.Now().UTC()
	defer func() {
		metrics.ServiceLatency.With(e.metricsHandler).Record(
			time.Since(startTime),
			metrics.OperationTag(metrics.HistoryReplicationTaskScope),
			metrics.NamespaceTag(attr.GetNamespaceId()),
		)
	}()

	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: attr.NamespaceId,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		VersionHistoryItems: attr.VersionHistoryItems,
		Events:              attr.Events,
		// new run events does not need version history since there is no prior events
		NewRunEvents: attr.NewRunEvents,
		NewRunId:     attr.NewRunId,
	}
	namespaceName, _ := e.namespaceRegistry.GetNamespaceName(namespace.ID(attr.NamespaceId))
	ctx, cancel := e.newTaskContext(ctx, namespaceName)
	defer cancel()

	// This might be extra cost if the workflow belongs to local shard.
	// Add a wrapper of the history client to call history engine directly if it becomes an issue.
	_, err = e.shardContext.GetHistoryClient().ReplicateEventsV2(ctx, request)
	switch retryErr := err.(type) {
	case nil:
		return nil

	case *serviceerrors.RetryReplication:
		metrics.ClientRequests.With(e.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRereplicationByHistoryReplicationScope),
			metrics.NamespaceTag(namespaceName.String()),
			metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
		)
		startTime := time.Now().UTC()
		defer func() {
			metrics.ClientLatency.With(e.metricsHandler).Record(
				time.Since(startTime),
				metrics.OperationTag(metrics.HistoryRereplicationByHistoryReplicationScope),
				metrics.NamespaceTag(namespaceName.String()),
				metrics.ServiceRoleTag(metrics.HistoryRoleTagValue),
			)
		}()
		resendErr := e.resend(
			ctx,
			e.remoteCluster,
			namespace.ID(retryErr.NamespaceId),
			retryErr.WorkflowId,
			retryErr.RunId,
			retryErr.StartEventId,
			retryErr.StartEventVersion,
			retryErr.EndEventId,
			retryErr.EndEventVersion,
		)
		switch resendErr.(type) {
		case *serviceerror.NotFound:
			// workflow is not found in source cluster, cleanup workflow in target cluster
			return e.cleanupWorkflowExecution(ctx, retryErr.NamespaceId, retryErr.WorkflowId, retryErr.RunId)
		case nil:
			// no-op
		default:
			e.logger.Error("error resend history for history event", tag.Error(resendErr))
			return err
		}

		// This might be extra cost if the workflow belongs to local shard.
		// Add a wrapper of the history client to call history engine directly if it becomes an issue.
		_, err = e.shardContext.GetHistoryClient().ReplicateEventsV2(ctx, request)
		return err
	default:
		if errors.Is(err, consts.ErrDuplicate) {
			return nil
		}
		return err
	}
}

func (e *taskExecutorImpl) handleSyncWorkflowStateTask(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
	forceApply bool,
) (retErr error) {

	attr := task.GetSyncWorkflowStateTaskAttributes()
	executionInfo := attr.GetWorkflowState().GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.GetNamespaceId())

	doContinue, err := e.filterTask(namespaceID, executionInfo.GetWorkflowId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	namespaceName, _ := e.namespaceRegistry.GetNamespaceName(namespace.ID(executionInfo.NamespaceId))
	ctx, cancel := e.newTaskContext(ctx, namespaceName)
	defer cancel()

	// This might be extra cost if the workflow belongs to local shard.
	// Add a wrapper of the history client to call history engine directly if it becomes an issue.
	request := &historyservice.ReplicateWorkflowStateRequest{
		NamespaceId:   namespaceID.String(),
		WorkflowState: attr.GetWorkflowState(),
		RemoteCluster: e.remoteCluster,
	}
	_, err = e.shardContext.GetHistoryClient().ReplicateWorkflowState(ctx, request)
	switch retryErr := err.(type) {
	case nil:
		return nil
	case *serviceerrors.RetryReplication:
		resendErr := e.resend(
			ctx,
			e.remoteCluster,
			namespace.ID(retryErr.NamespaceId),
			retryErr.WorkflowId,
			retryErr.RunId,
			retryErr.StartEventId,
			retryErr.StartEventVersion,
			retryErr.EndEventId,
			retryErr.EndEventVersion,
		)
		switch resendErr.(type) {
		case *serviceerror.NotFound:
			// workflow is not found in source cluster, cleanup workflow in target cluster
			return e.cleanupWorkflowExecution(ctx, retryErr.NamespaceId, retryErr.WorkflowId, retryErr.RunId)
		case nil:
			_, err = e.shardContext.GetHistoryClient().ReplicateWorkflowState(ctx, request)
			return err
		default:
			e.logger.Error("error resend history for replicate workflow state", tag.Error(resendErr))
			return err
		}
	default:
		if errors.Is(err, consts.ErrDuplicate) {
			return nil
		}
		return err
	}
}

func (e *taskExecutorImpl) filterTask(
	namespaceID namespace.ID,
	workflowID string,
	forceApply bool,
) (bool, error) {

	if forceApply {
		return true, nil
	}

	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceNotFound); ok {
			// Drop the task
			return false, nil
		}
		return false, err
	}

	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range namespaceEntry.ClusterNames(workflowID) {
		if e.currentCluster == targetCluster {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return shouldProcessTask, nil
}

func (e *taskExecutorImpl) cleanupWorkflowExecution(ctx context.Context, namespaceID string, workflowID string, runID string) (retErr error) {
	nsID := namespace.ID(namespaceID)
	ex := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	// CHASM runs only uses state based replication logic and should never reach here.
	// Can continue to use GetOrCreateWorkflowExecution.
	wfCtx, releaseFn, err := e.workflowCache.GetOrCreateWorkflowExecution(ctx, e.shardContext, nsID, &ex, locks.PriorityLow)
	if err != nil {
		return err
	}
	defer func() { releaseFn(retErr) }()
	mutableState, err := wfCtx.LoadMutableState(ctx, e.shardContext)
	if err != nil {
		return err
	}

	return e.deleteManager.DeleteWorkflowExecution(
		ctx,
		nsID,
		&ex,
		wfCtx,
		mutableState,
		nil, // stage is not stored during cleanup process.
	)
}

func (e *taskExecutorImpl) newTaskContext(
	parentCtx context.Context,
	namespaceName namespace.Name,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parentCtx, replicationTimeout)
	ctx = headers.SetCallerName(ctx, namespaceName.String())

	return ctx, cancel
}

func (e *taskExecutorImpl) resend(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64) error {
	iterator := e.remoteHistoryFetcher.GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion,
	)
	for iterator.HasNext() {
		historyBatch, err := iterator.Next()
		if err != nil {
			return err
		}
		replicateRequest := &historyservice.ReplicateEventsV2Request{
			NamespaceId: namespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Events:              historyBatch.RawEventBatch,
			VersionHistoryItems: historyBatch.VersionHistory.GetItems(),
		}
		_, err = e.shardContext.GetHistoryClient().ReplicateEventsV2(ctx, replicateRequest)
		if err != nil {
			return err
		}
	}
	return nil
}
