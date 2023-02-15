package replication

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executable_task_mock.go

const (
	taskStatePending = int32(ctasks.TaskStatePending)

	taskStateCancelled = int32(ctasks.TaskStateCancelled)
	taskStateAcked     = int32(ctasks.TaskStateAcked)
	taskStateNacked    = int32(ctasks.TaskStateNacked)
)

var (
	TaskRetryPolicy = backoff.NewExponentialRetryPolicy(1 * time.Second).
		WithBackoffCoefficient(1.2).
		WithMaximumInterval(5 * time.Second).
		WithMaximumAttempts(80).
		WithExpirationInterval(5 * time.Minute)
)

type (
	ExecutableTask interface {
		TaskID() int64
		TaskCreationTime() time.Time
		Ack()
		Nack(err error)
		Cancel()
		Reschedule()
		IsRetryableError(err error) bool
		RetryPolicy() backoff.RetryPolicy
		State() ctasks.State
		Attempt() int
		Resend(
			ctx context.Context,
			remoteCluster string,
			processToolBox ProcessToolBox,
			retryErr *serviceerrors.RetryReplication,
		) error
		DeleteWorkflow(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
		) (retError error)
		GetNamespaceInfo(
			namespaceID string,
		) (string, bool, error)
	}
	ExecutableTaskImpl struct {
		ProcessToolBox

		// immutable data
		taskID           int64
		metricsTag       string
		taskCreationTime time.Time
		taskReceivedTime time.Time

		// mutable data
		taskState    int32
		attempt      int32
		scheduleTime int64
	}
)

func NewExecutableTask(
	processToolBox ProcessToolBox,
	taskID int64,
	metricsTag string,
	taskCreationTime time.Time,
	taskReceivedTime time.Time,
) *ExecutableTaskImpl {
	return &ExecutableTaskImpl{
		ProcessToolBox:   processToolBox,
		taskID:           taskID,
		metricsTag:       metricsTag,
		taskCreationTime: taskCreationTime,
		taskReceivedTime: taskReceivedTime,

		taskState:    taskStatePending,
		attempt:      0,
		scheduleTime: 0,
	}
}

func (e *ExecutableTaskImpl) TaskID() int64 {
	return e.taskID
}

func (e *ExecutableTaskImpl) TaskCreationTime() time.Time {
	return e.taskCreationTime
}

func (e *ExecutableTaskImpl) Ack() {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		e.Logger.Error(fmt.Sprintf(
			"replication task: %v encountered concurrent completion event",
			e.taskID,
		))
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
		e.Logger.Error(fmt.Sprintf(
			"replication task: %v encountered concurrent completion event",
			e.taskID,
		))
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
}

func (e *ExecutableTaskImpl) Cancel() {
	if atomic.LoadInt32(&e.taskState) != taskStatePending {
		e.Logger.Error(fmt.Sprintf(
			"replication task: %v encountered concurrent completion event",
			e.taskID,
		))
		return
	}
	if !atomic.CompareAndSwapInt32(&e.taskState, taskStatePending, taskStateCancelled) {
		e.Cancel() // retry cancel
	}

	e.Logger.Info(fmt.Sprintf(
		"replication task: %v encountered cancellation event",
		e.taskID,
	))
	now := time.Now().UTC()
	e.emitFinishMetrics(now)
}

func (e *ExecutableTaskImpl) Reschedule() {
	taskState := atomic.LoadInt32(&e.taskState)
	if taskState != taskStatePending {
		e.Logger.Error(fmt.Sprintf(
			"replication task: %v encountered concurrent completion event",
			e.taskID,
		))
		return
	}

	atomic.AddInt32(&e.attempt, 1)
}

func (e *ExecutableTaskImpl) IsRetryableError(err error) bool {
	switch err.(type) {
	case *serviceerror.InvalidArgument:
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

func (e *ExecutableTaskImpl) Attempt() int {
	return int(atomic.LoadInt32(&e.attempt))
}

func (e *ExecutableTaskImpl) emitFinishMetrics(
	now time.Time,
) {
	e.MetricsHandler.Timer(metrics.ServiceLatency.GetMetricName()).Record(
		now.Sub(e.taskReceivedTime),
		metrics.OperationTag(e.metricsTag),
	)
	e.MetricsHandler.Timer(metrics.ServiceLatency.GetMetricName()).Record(
		e.taskReceivedTime.Sub(e.taskCreationTime),
		metrics.OperationTag(e.metricsTag),
	)
	// TODO consider emit attempt metrics
}

func (e *ExecutableTaskImpl) Resend(
	ctx context.Context,
	remoteCluster string,
	processToolBox ProcessToolBox,
	retryErr *serviceerrors.RetryReplication,
) error {
	e.MetricsHandler.Counter(metrics.ClientRequests.GetMetricName()).Record(
		1,
		metrics.OperationTag(e.metricsTag+"Resend"),
	)
	startTime := time.Now().UTC()
	defer func() {
		e.MetricsHandler.Timer(metrics.ClientLatency.GetMetricName()).Record(
			time.Since(startTime),
			metrics.OperationTag(e.metricsTag+"Resend"),
		)
	}()

	resendErr := processToolBox.NDCHistoryResender.SendSingleWorkflowHistory(
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
	switch resendErr.(type) {
	case nil:
		// no-op
		return nil
	case *serviceerror.NotFound:
		e.Logger.Error(
			"workflow not found in source cluster, proceed to cleanup",
			tag.WorkflowNamespaceID(retryErr.NamespaceId),
			tag.WorkflowID(retryErr.WorkflowId),
			tag.WorkflowRunID(retryErr.RunId),
		)
		// workflow is not found in source cluster, cleanup workflow in target cluster
		return e.DeleteWorkflow(
			ctx,
			definition.NewWorkflowKey(
				retryErr.NamespaceId,
				retryErr.WorkflowId,
				retryErr.RunId,
			),
		)
	default:
		e.Logger.Error("error resend history for history event", tag.Error(resendErr))
		return resendErr
	}
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
	return engine.ForceDeleteWorkflowExecution(ctx, workflowKey)
}

func (e *ExecutableTaskImpl) GetNamespaceInfo(
	namespaceID string,
) (string, bool, error) {
	namespaceEntry, err := e.NamespaceCache.GetNamespaceByID(namespace.ID(namespaceID))
	switch err.(type) {
	case nil:
		shouldProcessTask := false
	FilterLoop:
		for _, targetCluster := range namespaceEntry.ClusterNames() {
			if e.ClusterMetadata.GetCurrentClusterName() == targetCluster {
				shouldProcessTask = true
				break FilterLoop
			}
		}
		return string(namespaceEntry.Name()), shouldProcessTask, nil
	case *serviceerror.NamespaceNotFound:
		return "", false, nil
	default:
		return "", false, err
	}
}

func newTaskContext(
	namespaceName string,
) (context.Context, context.CancelFunc) {
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemPreemptableCallerInfo,
	)
	ctx = headers.SetCallerName(ctx, namespaceName)
	return context.WithTimeout(ctx, replicationTimeout)
}
