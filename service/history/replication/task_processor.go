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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_processor_mock.go

package replication

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

const (
	dropSyncShardTaskTimeThreshold = 10 * time.Minute
	replicationTimeout             = 30 * time.Second
)

var (
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = serviceerror.NewInvalidArgument("unknown replication task")
)

type (
	// TaskProcessor is the interface for task processor
	TaskProcessor interface {
		common.Daemon
	}

	// taskProcessorImpl is responsible for processing replication tasks for a shard.
	taskProcessorImpl struct {
		currentCluster          string
		sourceCluster           string
		status                  int32
		shard                   shard.Context
		historyEngine           shard.Engine
		historySerializer       serialization.Serializer
		config                  *configs.Config
		metricsHandler          metrics.MetricsHandler
		logger                  log.Logger
		replicationTaskExecutor TaskExecutor

		rateLimiter quotas.RateLimiter

		taskRetryPolicy backoff.RetryPolicy
		dlqRetryPolicy  backoff.RetryPolicy

		// recv side
		maxRxProcessedTaskID    int64
		maxRxProcessedTimestamp time.Time
		maxRxReceivedTaskID     int64
		rxTaskBackoff           time.Duration

		requestChan   chan<- *replicationTaskRequest
		syncShardChan chan *replicationspb.SyncShardStatus
		shutdownChan  chan struct{}
	}

	replicationTaskRequest struct {
		token    *replicationspb.ReplicationToken
		respChan chan<- *replicationspb.ReplicationMessages
	}
)

// NewTaskProcessor creates a new replication task processor.
func NewTaskProcessor(
	shard shard.Context,
	historyEngine shard.Engine,
	config *configs.Config,
	metricsHandler metrics.MetricsHandler,
	replicationTaskFetcher taskFetcher,
	replicationTaskExecutor TaskExecutor,
	eventSerializer serialization.Serializer,
) TaskProcessor {
	shardID := shard.GetShardID()
	taskRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait(shardID)).
		WithBackoffCoefficient(config.ReplicationTaskProcessorErrorRetryBackoffCoefficient(shardID)).
		WithMaximumInterval(config.ReplicationTaskProcessorErrorRetryMaxInterval(shardID)).
		WithMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts(shardID)).
		WithExpirationInterval(config.ReplicationTaskProcessorErrorRetryExpiration(shardID))

	// TODO: define separate set of configs for dlq retry
	dlqRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait(shardID)).
		WithBackoffCoefficient(config.ReplicationTaskProcessorErrorRetryBackoffCoefficient(shardID)).
		WithMaximumInterval(config.ReplicationTaskProcessorErrorRetryMaxInterval(shardID)).
		WithMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts(shardID)).
		WithExpirationInterval(config.ReplicationTaskProcessorErrorRetryExpiration(shardID))

	return &taskProcessorImpl{
		currentCluster:          shard.GetClusterMetadata().GetCurrentClusterName(),
		sourceCluster:           replicationTaskFetcher.getSourceCluster(),
		status:                  common.DaemonStatusInitialized,
		shard:                   shard,
		historyEngine:           historyEngine,
		historySerializer:       eventSerializer,
		config:                  config,
		metricsHandler:          metricsHandler,
		logger:                  shard.GetLogger(),
		replicationTaskExecutor: replicationTaskExecutor,
		rateLimiter: quotas.NewMultiRateLimiter([]quotas.RateLimiter{
			quotas.NewDefaultOutgoingRateLimiter(
				func() float64 { return config.ReplicationTaskProcessorShardQPS() },
			),
			replicationTaskFetcher.getRateLimiter(),
		}),
		taskRetryPolicy:      taskRetryPolicy,
		dlqRetryPolicy:       dlqRetryPolicy,
		requestChan:          replicationTaskFetcher.getRequestChan(),
		syncShardChan:        make(chan *replicationspb.SyncShardStatus, 1),
		shutdownChan:         make(chan struct{}),
		maxRxProcessedTaskID: persistence.EmptyQueueMessageID,
		maxRxReceivedTaskID:  persistence.EmptyQueueMessageID,
	}
}

// Start starts the processor
func (p *taskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go p.eventLoop()

	p.logger.Info("ReplicationTaskProcessor started.")
}

// Stop stops the processor
func (p *taskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(p.shutdownChan)

	p.logger.Info("ReplicationTaskProcessor shutting down.")
}

func (p *taskProcessorImpl) eventLoop() {
	syncShardTimer := time.NewTimer(backoff.JitDuration(
		p.config.ShardSyncMinInterval(),
		p.config.ShardSyncTimerJitterCoefficient(),
	))
	defer syncShardTimer.Stop()

	replicationTimer := time.NewTimer(0)
	defer replicationTimer.Stop()

	var syncShardTask *replicationspb.SyncShardStatus
	for {
		select {
		case syncShardTask = <-p.syncShardChan:

		case <-syncShardTimer.C:
			if err := p.handleSyncShardStatus(syncShardTask); err != nil {
				p.logger.Error("unable to sync shard status", tag.Error(err))
				p.metricsHandler.Counter(metrics.SyncShardFromRemoteFailure.GetMetricName()).Record(
					1,
					metrics.OperationTag(metrics.HistorySyncShardStatusScope))
			}
			syncShardTimer.Reset(backoff.JitDuration(
				p.config.ShardSyncMinInterval(),
				p.config.ShardSyncTimerJitterCoefficient(),
			))

		case <-p.shutdownChan:
			return

		case <-replicationTimer.C:
			if err := p.pollProcessReplicationTasks(); err != nil {
				p.logger.Error("unable to process replication tasks", tag.Error(err))
			}
			replicationTimer.Reset(p.rxTaskBackoff)
		}
	}
}

func (p *taskProcessorImpl) pollProcessReplicationTasks() (retError error) {
	defer func() {
		if retError != nil {
			p.maxRxReceivedTaskID = p.maxRxProcessedTaskID
			p.rxTaskBackoff = p.config.ReplicationTaskFetcherErrorRetryWait()
		}
	}()

	taskIterator := collection.NewPagingIterator(p.paginationFn)
	for taskIterator.HasNext() && !p.isStopped() {
		task, err := taskIterator.Next()
		if err != nil {
			return err
		}

		replicationTask := task.(*replicationspb.ReplicationTask)
		taskCreationTime := replicationTask.GetVisibilityTime()
		if taskCreationTime != nil {
			now := p.shard.GetTimeSource().Now()
			p.metricsHandler.Timer(metrics.ReplicationLatency.GetMetricName()).Record(
				now.Sub(*taskCreationTime),
				metrics.OperationTag(metrics.ReplicationTaskFetcherScope))
		}
		if err = p.applyReplicationTask(replicationTask); err != nil {
			return err
		}
		p.maxRxProcessedTaskID = replicationTask.GetSourceTaskId()
		p.maxRxProcessedTimestamp = timestamp.TimeValue(replicationTask.GetVisibilityTime())
	}

	if !p.isStopped() {
		// all tasks fetched successfully processed
		// setting the receiver side max processed task ID to max received task ID
		// since task ID is not contiguous
		p.maxRxProcessedTaskID = p.maxRxReceivedTaskID
	}

	return nil
}

func (p *taskProcessorImpl) applyReplicationTask(
	replicationTask *replicationspb.ReplicationTask,
) error {
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	err := p.handleReplicationTask(ctx, replicationTask)
	if err == nil || p.isStopped() || shard.IsShardOwnershipLostError(err) {
		return err
	}

	p.logger.Error(
		"failed to apply replication task after retry",
		tag.TaskID(replicationTask.GetSourceTaskId()),
		tag.Error(err),
	)
	request, err := p.convertTaskToDLQTask(replicationTask)
	if err != nil {
		p.logger.Error("failed to generate DLQ replication task", tag.Error(err))
		return nil
	}
	if err := p.handleReplicationDLQTask(ctx, request); err != nil {
		return err
	}
	return nil
}

func (p *taskProcessorImpl) handleSyncShardStatus(
	status *replicationspb.SyncShardStatus,
) error {

	now := p.shard.GetTimeSource().Now()
	if status == nil {
		return nil
	} else if now.Sub(timestamp.TimeValue(status.GetStatusTime())) > dropSyncShardTaskTimeThreshold {
		return nil
	}

	p.metricsHandler.Counter(metrics.SyncShardFromRemoteCounter.GetMetricName()).Record(
		1,
		metrics.OperationTag(metrics.HistorySyncShardStatusScope))
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)

	return p.historyEngine.SyncShardStatus(ctx, &historyservice.SyncShardStatusRequest{
		SourceCluster: p.sourceCluster,
		ShardId:       p.shard.GetShardID(),
		StatusTime:    status.StatusTime,
	})
}

func (p *taskProcessorImpl) handleReplicationTask(
	ctx context.Context,
	replicationTask *replicationspb.ReplicationTask,
) error {
	_ = p.rateLimiter.Wait(ctx)

	operation := func() error {
		operation, err := p.replicationTaskExecutor.Execute(ctx, replicationTask, false)
		p.emitTaskMetrics(operation, err)
		return err
	}
	return backoff.ThrottleRetry(operation, p.taskRetryPolicy, p.isRetryableError)
}

func (p *taskProcessorImpl) handleReplicationDLQTask(
	ctx context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	_ = p.rateLimiter.Wait(ctx)

	p.logger.Info("enqueue replication task to DLQ",
		tag.ShardID(p.shard.GetShardID()),
		tag.WorkflowNamespaceID(request.TaskInfo.GetNamespaceId()),
		tag.WorkflowID(request.TaskInfo.GetWorkflowId()),
		tag.WorkflowRunID(request.TaskInfo.GetRunId()),
		tag.TaskID(request.TaskInfo.GetTaskId()),
	)
	p.metricsHandler.Gauge(metrics.ReplicationDLQMaxLevelGauge.GetMetricName()).Record(
		float64(request.TaskInfo.GetTaskId()),
		metrics.OperationTag(metrics.ReplicationDLQStatsScope),
		metrics.TargetClusterTag(p.sourceCluster),
		metrics.InstanceTag(convert.Int32ToString(p.shard.GetShardID())))
	// The following is guaranteed to success or retry forever until processor is shutdown.
	return backoff.ThrottleRetry(func() error {
		err := p.shard.GetExecutionManager().PutReplicationTaskToDLQ(ctx, request)
		if err != nil {
			p.logger.Error("failed to enqueue replication task to DLQ", tag.Error(err))
			p.metricsHandler.Counter(metrics.ReplicationDLQFailed.GetMetricName()).Record(1, metrics.OperationTag(metrics.ReplicationTaskFetcherScope))
		}
		return err
	}, p.dlqRetryPolicy, p.isRetryableError)
}

func (p *taskProcessorImpl) convertTaskToDLQTask(
	replicationTask *replicationspb.ReplicationTask,
) (*persistence.PutReplicationTaskToDLQRequest, error) {
	switch replicationTask.TaskType {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		taskAttributes := replicationTask.GetSyncActivityTaskAttributes()
		return &persistence.PutReplicationTaskToDLQRequest{
			ShardID:           p.shard.GetShardID(),
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId:      taskAttributes.GetNamespaceId(),
				WorkflowId:       taskAttributes.GetWorkflowId(),
				RunId:            taskAttributes.GetRunId(),
				TaskId:           replicationTask.GetSourceTaskId(),
				TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
				ScheduledEventId: taskAttributes.GetScheduledEventId(),
				Version:          taskAttributes.GetVersion(),
			},
		}, nil

	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		taskAttributes := replicationTask.GetHistoryTaskAttributes()

		events, err := p.historySerializer.DeserializeEvents(taskAttributes.GetEvents())
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			p.logger.Error("Empty events in a batch")
			return nil, fmt.Errorf("corrupted history event batch, empty events")
		}
		firstEvent := events[0]
		lastEvent := events[len(events)-1]
		// NOTE: last event vs next event, next event ID is exclusive
		nextEventID := lastEvent.GetEventId() + 1

		return &persistence.PutReplicationTaskToDLQRequest{
			ShardID:           p.shard.GetShardID(),
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId:  taskAttributes.GetNamespaceId(),
				WorkflowId:   taskAttributes.GetWorkflowId(),
				RunId:        taskAttributes.GetRunId(),
				TaskId:       replicationTask.GetSourceTaskId(),
				TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
				FirstEventId: firstEvent.GetEventId(),
				NextEventId:  nextEventID,
				Version:      firstEvent.GetVersion(),
			},
		}, nil

	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		taskAttributes := replicationTask.GetSyncWorkflowStateTaskAttributes()
		executionInfo := taskAttributes.GetWorkflowState().GetExecutionInfo()
		executionState := taskAttributes.GetWorkflowState().GetExecutionState()
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.GetVersionHistories())
		if err != nil {
			return nil, err
		}
		lastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, err
		}

		return &persistence.PutReplicationTaskToDLQRequest{
			ShardID:           p.shard.GetShardID(),
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: executionInfo.GetNamespaceId(),
				WorkflowId:  executionInfo.GetWorkflowId(),
				RunId:       executionState.GetRunId(),
				TaskId:      replicationTask.GetSourceTaskId(),
				TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
				Version:     lastItem.GetVersion(),
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown replication task type: %v", replicationTask.TaskType)
	}
}

func (p *taskProcessorImpl) paginationFn(_ []byte) ([]interface{}, []byte, error) {
	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	p.requestChan <- &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                     p.shard.GetShardID(),
			LastProcessedMessageId:      p.maxRxProcessedTaskID,
			LastProcessedVisibilityTime: &p.maxRxProcessedTimestamp,
			LastRetrievedMessageId:      p.maxRxReceivedTaskID,
		},
		respChan: respChan,
	}

	select {
	case resp, ok := <-respChan:
		if !ok {
			return nil, nil, nil
		}

		select {
		case p.syncShardChan <- resp.GetSyncShardStatus():

		default:
			// channel full, it is ok to drop the sync shard status
			// since sync shard status are periodically updated
		}

		var tasks []interface{}
		for _, task := range resp.GetReplicationTasks() {
			tasks = append(tasks, task)
		}
		p.maxRxReceivedTaskID = resp.GetLastRetrievedMessageId()
		if len(tasks) == 0 {
			// Update processed timestamp to the source cluster time when there is no replication task
			p.maxRxProcessedTimestamp = timestamp.TimeValue(resp.GetSyncShardStatus().GetStatusTime())
		}

		if resp.GetHasMore() {
			p.rxTaskBackoff = time.Duration(0)
		} else {
			p.rxTaskBackoff = p.config.ReplicationTaskProcessorNoTaskRetryWait(p.shard.GetShardID())
		}
		return tasks, nil, nil

	case <-p.shutdownChan:
		return nil, nil, nil
	}
}

func (p *taskProcessorImpl) emitTaskMetrics(operation string, err error) {
	metricsScope := p.metricsHandler.WithTags(metrics.OperationTag(operation))
	if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
		metricsScope.Counter(metrics.ServiceErrContextTimeoutCounter.GetMetricName()).Record(1)
		return
	}

	// Also update counter to distinguish between type of failures
	switch err := err.(type) {
	case nil:
		metricsScope.Counter(metrics.ReplicationTasksApplied.GetMetricName()).Record(1)
		return
	case *serviceerrors.ShardOwnershipLost:
		metricsScope.Counter(metrics.ServiceErrShardOwnershipLostCounter.GetMetricName()).Record(1)
	case *serviceerror.InvalidArgument:
		metricsScope.Counter(metrics.ServiceErrInvalidArgumentCounter.GetMetricName()).Record(1)
	case *serviceerror.NamespaceNotActive:
		metricsScope.Counter(metrics.ServiceErrNamespaceNotActiveCounter.GetMetricName()).Record(1)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		metricsScope.Counter(metrics.ServiceErrExecutionAlreadyStartedCounter.GetMetricName()).Record(1)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		metricsScope.Counter(metrics.ServiceErrNotFoundCounter.GetMetricName()).Record(1)
	case *serviceerror.ResourceExhausted:
		metricsScope.Counter(metrics.ServiceErrResourceExhaustedCounter.GetMetricName()).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
	case *serviceerrors.RetryReplication:
		metricsScope.Counter(metrics.ServiceErrRetryTaskCounter.GetMetricName()).Record(1)
	default:
	}
	metricsScope.Counter(metrics.ReplicationTasksFailed.GetMetricName()).Record(1)
}

func (p *taskProcessorImpl) isStopped() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStopped
}

func (p *taskProcessorImpl) isRetryableError(
	err error,
) bool {
	if p.isStopped() {
		return false
	}

	switch err.(type) {
	case *serviceerror.InvalidArgument:
		return false
	default:
		return true
	}
}
