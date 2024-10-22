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

package replication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executable_task_mock.go

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
	TaskRetryPolicy = backoff.NewExponentialRetryPolicy(1 * time.Second).
			WithBackoffCoefficient(1.2).
			WithMaximumInterval(5 * time.Second).
			WithMaximumAttempts(80).
			WithExpirationInterval(5 * time.Minute)
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
		) (string, bool, error)
		SyncState(
			ctx context.Context,
			syncStateErr *serviceerrors.SyncState,
			remainingAttempt int,
		) (bool, error)
		ReplicationTask() *replicationspb.ReplicationTask
		MarkPoisonPill() error
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
	priority enumsspb.TaskPriority,
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
		taskPriority:           priority,
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

	e.Logger.Debug(fmt.Sprintf(
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

	e.Logger.Debug(fmt.Sprintf(
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

	e.Logger.Info(fmt.Sprintf(
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
	return TaskRetryPolicy
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

func (e *ExecutableTaskImpl) emitFinishMetrics(
	now time.Time,
) {
	nsTag := metrics.NamespaceUnknownTag()
	item := e.namespace.Load()
	if item != nil {
		nsTag = metrics.NamespaceTag(item.(namespace.Name).String())
	}
	metrics.ServiceLatency.With(e.MetricsHandler).Record(
		now.Sub(e.taskReceivedTime),
		metrics.OperationTag(e.metricsTag),
		nsTag,
	)
	// replication lag is only meaningful for non-low priority tasks as for low priority task, we may delay processing
	if e.taskPriority != enumsspb.TASK_PRIORITY_LOW {
		metrics.ReplicationLatency.With(e.MetricsHandler).Record(
			now.Sub(e.taskCreationTime),
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
	}
	// TODO consider emit attempt metrics
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

	metrics.ClientRequests.With(e.MetricsHandler).Record(
		1,
		metrics.OperationTag(e.metricsTag+"Resend"),
	)
	startTime := time.Now().UTC()
	defer func() {
		metrics.ClientLatency.With(e.MetricsHandler).Record(
			time.Since(startTime),
			metrics.OperationTag(e.metricsTag+"Resend"),
		)
	}()
	var resendErr error
	if e.Config.EnableReplicateLocalGeneratedEvent() {
		resendErr = e.ProcessToolBox.ResendHandler.ResendHistoryEvents(
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
	} else {
		resendErr = e.ProcessToolBox.NDCHistoryResender.SendSingleWorkflowHistory(
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
	}
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
			e.Logger.Error("error resend history on the same workflow run",
				tag.WorkflowNamespaceID(retryErr.NamespaceId),
				tag.WorkflowID(retryErr.WorkflowId),
				tag.WorkflowRunID(retryErr.RunId),
				tag.NewStringTag("first-resend-error", retryErr.Error()),
				tag.NewStringTag("second-resend-error", resendErr.Error()),
			)
			return false, serviceerror.NewDataLoss("failed to get requested data while resending history")
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
			tag.NewStringTag("first-resend-error", retryErr.Error()),
			tag.NewStringTag("second-resend-error", resendErr.Error()),
			tag.Error(err),
		)
		return false, resendErr
	default:
		e.Logger.Error("error resend history for history event",
			tag.WorkflowNamespaceID(retryErr.NamespaceId),
			tag.WorkflowID(retryErr.WorkflowId),
			tag.WorkflowRunID(retryErr.RunId),
			tag.NewStringTag("first-resend-error", retryErr.Error()),
			tag.NewStringTag("second-resend-error", resendErr.Error()),
		)
		return false, resendErr
	}
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
	resp, err := remoteAdminClient.SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
		NamespaceId: syncStateErr.NamespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: syncStateErr.WorkflowId,
			RunId:      syncStateErr.RunId,
		},
		VersionedTransition: syncStateErr.VersionedTransition,
		VersionHistories:    syncStateErr.VersionHistories,
		TargetClusterId:     int32(targetClusterInfo.InitialFailoverVersion),
	})
	if err != nil {
		var failedPreconditionErr *serviceerror.FailedPrecondition
		if !errors.As(err, &failedPreconditionErr) {
			return false, err
		}
		// Unable to perform sync state. Transition history maybe disabled in source cluster.
		// Add task equivalents back to source cluster.
		taskEquivalents := e.replicationTask.GetRawTaskInfo().GetTaskEquivalents()

		logger := log.With(e.Logger,
			tag.WorkflowNamespaceID(syncStateErr.NamespaceId),
			tag.WorkflowID(syncStateErr.WorkflowId),
			tag.WorkflowRunID(syncStateErr.RunId),
			tag.ReplicationTask(e.replicationTask),
		)

		if len(taskEquivalents) == 0 {
			// Just drop the task since there's nothing to replicate in event-based stack.
			logger.Info("Dropped replication task as there's no event-based replication task equivalent.")
			return false, nil
		}

		tasksToAdd := make([]*adminservice.AddTasksRequest_Task, 0, len(taskEquivalents))
		for _, taskEquivalent := range taskEquivalents {
			blob, err := serialization.ReplicationTaskInfoToBlob(taskEquivalent)
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
	err = engine.ReplicateVersionedTransition(ctx, resp.VersionedTransitionArtifact, e.SourceClusterName())
	if err != nil {
		return false, err
	}
	return true, nil
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
	return err
}

func (e *ExecutableTaskImpl) GetNamespaceInfo(
	ctx context.Context,
	namespaceID string,
) (string, bool, error) {
	namespaceEntry, err := e.NamespaceCache.GetNamespaceByID(namespace.ID(namespaceID))
	switch err.(type) {
	case nil:
		if e.replicationTask.VersionedTransition != nil && e.replicationTask.VersionedTransition.NamespaceFailoverVersion > namespaceEntry.FailoverVersion() {
			if !e.ProcessToolBox.Config.EnableReplicationEagerRefreshNamespace() {
				return "", false, serviceerror.NewInternal(fmt.Sprintf("cannot process task because namespace failover version is not up to date, task version: %v, namespace version: %v", e.replicationTask.VersionedTransition.NamespaceFailoverVersion, namespaceEntry.FailoverVersion()))
			}
			_, err = e.ProcessToolBox.EagerNamespaceRefresher.SyncNamespaceFromSourceCluster(ctx, namespace.ID(namespaceID), e.sourceClusterName)
			if err != nil {
				return "", false, err
			}
		}
	case *serviceerror.NamespaceNotFound:
		if !e.ProcessToolBox.Config.EnableReplicationEagerRefreshNamespace() {
			return "", false, nil
		}
		_, err = e.ProcessToolBox.EagerNamespaceRefresher.SyncNamespaceFromSourceCluster(ctx, namespace.ID(namespaceID), e.sourceClusterName)
		if err != nil {
			e.Logger.Info("Failed to SyncNamespaceFromSourceCluster", tag.Error(err))
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
	if e.replicationTask.VersionedTransition != nil && namespaceEntry.FailoverVersion() < e.replicationTask.VersionedTransition.NamespaceFailoverVersion {
		return "", false, serviceerror.NewInternal(fmt.Sprintf("cannot process task because namespace failover version is not up to date after sync, task version: %v, namespace version: %v", e.replicationTask.VersionedTransition.NamespaceFailoverVersion, namespaceEntry.FailoverVersion()))
	}

	e.namespace.Store(namespaceEntry.Name())
	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range namespaceEntry.ClusterNames() {
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
		tag.WorkflowRunID(e.replicationTask.RawTaskInfo.NamespaceId),
		tag.TaskID(e.taskID),
		tag.SourceCluster(e.SourceClusterName()),
		tag.ReplicationTask(taskInfo),
	)

	ctx, cancel := newTaskContext(e.replicationTask.RawTaskInfo.NamespaceId, e.Config.ReplicationTaskApplyTimeout())
	defer cancel()

	return writeTaskToDLQ(ctx, e.DLQWriter, e.sourceShardKey.ShardID, e.SourceClusterName(), shardContext.GetShardID(), taskInfo)
}

func newTaskContext(
	namespaceName string,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemPreemptableCallerInfo,
	)
	ctx = headers.SetCallerName(ctx, namespaceName)
	return context.WithTimeout(ctx, timeout)
}
