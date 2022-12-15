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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_executor_mock.go

package replication

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	TaskExecutor interface {
		Execute(ctx context.Context, replicationTask *replicationspb.ReplicationTask, forceApply bool) (string, error)
	}

	TaskExecutorParams struct {
		RemoteCluster   string // TODO: Remove this remote cluster from executor then it can use singleton.
		Shard           shard.Context
		HistoryResender xdc.NDCHistoryResender
		HistoryEngine   shard.Engine
		DeleteManager   deletemanager.DeleteManager
		WorkflowCache   wcache.Cache
	}

	TaskExecutorProvider func(params TaskExecutorParams) TaskExecutor

	taskExecutorImpl struct {
		currentCluster     string
		remoteCluster      string
		shard              shard.Context
		namespaceRegistry  namespace.Registry
		nDCHistoryResender xdc.NDCHistoryResender
		historyEngine      shard.Engine
		deleteManager      deletemanager.DeleteManager
		workflowCache      wcache.Cache
		metricsHandler     metrics.MetricsHandler
		logger             log.Logger
	}
)

// NewTaskExecutor creates a replication task executor
// The executor uses by 1) DLQ replication task handler 2) history replication task processor
func NewTaskExecutor(
	remoteCluster string,
	shard shard.Context,
	nDCHistoryResender xdc.NDCHistoryResender,
	historyEngine shard.Engine,
	deleteManager deletemanager.DeleteManager,
	workflowCache wcache.Cache,
) TaskExecutor {
	return &taskExecutorImpl{
		currentCluster:     shard.GetClusterMetadata().GetCurrentClusterName(),
		remoteCluster:      remoteCluster,
		shard:              shard,
		namespaceRegistry:  shard.GetNamespaceRegistry(),
		nDCHistoryResender: nDCHistoryResender,
		historyEngine:      historyEngine,
		deleteManager:      deleteManager,
		workflowCache:      workflowCache,
		metricsHandler:     shard.GetMetricsHandler(),
		logger:             shard.GetLogger(),
	}
}

func (e *taskExecutorImpl) Execute(
	ctx context.Context,
	replicationTask *replicationspb.ReplicationTask,
	forceApply bool,
) (string, error) {
	var err error
	var operation string
	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK:
		// Shard status will be sent as part of the Replication message without kafka
		operation = metrics.SyncShardTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		operation = metrics.SyncActivityTaskScope
		err = e.handleActivityTask(ctx, replicationTask, forceApply)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
		operation = metrics.HistoryMetadataReplicationTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		operation = metrics.HistoryReplicationTaskScope
		err = e.handleHistoryReplicationTask(ctx, replicationTask, forceApply)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		operation = metrics.SyncWorkflowStateTaskScope
		err = e.handleSyncWorkflowStateTask(ctx, replicationTask, forceApply)
	default:
		e.logger.Error("Unknown task type.")
		operation = metrics.ReplicatorScope
		err = ErrUnknownReplicationTask
	}

	return operation, err
}

func (e *taskExecutorImpl) handleActivityTask(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetSyncActivityTaskAttributes()
	doContinue, err := e.filterTask(namespace.ID(attr.GetNamespaceId()), forceApply)
	if err != nil || !doContinue {
		return err
	}

	startTime := time.Now().UTC()
	defer func() {
		e.metricsHandler.Timer(metrics.ServiceLatency.GetMetricName()).Record(
			time.Since(startTime),
			metrics.OperationTag(metrics.SyncActivityTaskScope),
		)
	}()

	request := &historyservice.SyncActivityRequest{
		NamespaceId:        attr.NamespaceId,
		WorkflowId:         attr.WorkflowId,
		RunId:              attr.RunId,
		Version:            attr.Version,
		ScheduledEventId:   attr.ScheduledEventId,
		ScheduledTime:      attr.ScheduledTime,
		StartedEventId:     attr.StartedEventId,
		StartedTime:        attr.StartedTime,
		LastHeartbeatTime:  attr.LastHeartbeatTime,
		Details:            attr.Details,
		Attempt:            attr.Attempt,
		LastFailure:        attr.LastFailure,
		LastWorkerIdentity: attr.LastWorkerIdentity,
		VersionHistory:     attr.GetVersionHistory(),
	}
	ctx, cancel := e.newTaskContext(ctx, attr.NamespaceId)
	defer cancel()

	err = e.historyEngine.SyncActivity(ctx, request)
	switch retryErr := err.(type) {
	case nil:
		return nil

	case *serviceerrors.RetryReplication:
		e.metricsHandler.Counter(metrics.ClientRequests.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRereplicationByActivityReplicationScope),
		)
		startTime := time.Now().UTC()
		defer func() {
			e.metricsHandler.Timer(metrics.ClientLatency.GetMetricName()).Record(
				time.Since(startTime),
				metrics.OperationTag(metrics.HistoryRereplicationByActivityReplicationScope),
			)
		}()

		resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
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
		return e.historyEngine.SyncActivity(ctx, request)

	default:
		return err
	}
}

func (e *taskExecutorImpl) handleHistoryReplicationTask(
	ctx context.Context,
	task *replicationspb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetHistoryTaskAttributes()
	doContinue, err := e.filterTask(namespace.ID(attr.GetNamespaceId()), forceApply)
	if err != nil || !doContinue {
		return err
	}

	startTime := time.Now().UTC()
	defer func() {
		e.metricsHandler.Timer(metrics.ServiceLatency.GetMetricName()).Record(
			time.Since(startTime),
			metrics.OperationTag(metrics.HistoryReplicationTaskScope),
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
	}
	ctx, cancel := e.newTaskContext(ctx, attr.NamespaceId)
	defer cancel()

	err = e.historyEngine.ReplicateEventsV2(ctx, request)
	switch retryErr := err.(type) {
	case nil:
		return nil

	case *serviceerrors.RetryReplication:
		e.metricsHandler.Counter(metrics.ClientRequests.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRereplicationByHistoryReplicationScope),
		)
		startTime := time.Now().UTC()
		defer func() {
			e.metricsHandler.Timer(metrics.ClientLatency.GetMetricName()).Record(
				time.Since(startTime),
				metrics.OperationTag(metrics.HistoryRereplicationByHistoryReplicationScope),
			)
		}()

		resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
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

		return e.historyEngine.ReplicateEventsV2(ctx, request)

	default:
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

	doContinue, err := e.filterTask(namespaceID, forceApply)
	if err != nil || !doContinue {
		return err
	}

	ctx, cancel := e.newTaskContext(ctx, executionInfo.NamespaceId)
	defer cancel()

	return e.historyEngine.ReplicateWorkflowState(ctx, &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: attr.GetWorkflowState(),
		RemoteCluster: e.remoteCluster,
	})
}

func (e *taskExecutorImpl) filterTask(
	namespaceID namespace.ID,
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
	for _, targetCluster := range namespaceEntry.ClusterNames() {
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
	wfCtx, releaseFn, err := e.workflowCache.GetOrCreateWorkflowExecution(ctx, nsID, ex, workflow.CallerTypeTask)
	if err != nil {
		return err
	}
	defer func() { releaseFn(retErr) }()
	mutableState, err := wfCtx.LoadMutableState(ctx)
	if err != nil {
		return err
	}

	return e.deleteManager.DeleteWorkflowExecution(
		ctx,
		nsID,
		ex,
		wfCtx,
		mutableState,
		false,
		nil, // stage is not stored during cleanup process.
	)
}

func (e *taskExecutorImpl) newTaskContext(
	parentCtx context.Context,
	namespaceID string,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parentCtx, replicationTimeout)

	namespace, _ := e.namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID))
	ctx = headers.SetCallerName(ctx, namespace.String())

	return ctx, cancel
}
