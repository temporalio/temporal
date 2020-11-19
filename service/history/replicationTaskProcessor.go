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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskProcessor_mock.go -self_package go.temporal.io/server/service/history

package history

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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

const (
	dropSyncShardTaskTimeThreshold   = 10 * time.Minute
	replicationTimeout               = 30 * time.Second
	taskErrorRetryBackoffCoefficient = 1.2
	taskErrorRetryMaxInterval        = 5 * time.Second
)

var (
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = serviceerror.NewInvalidArgument("unknown replication task")
)

type (
	// ReplicationTaskProcessorImpl is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessorImpl struct {
		currentCluster          string
		sourceCluster           string
		status                  int32
		shard                   shard.Context
		historyEngine           shard.Engine
		historySerializer       persistence.PayloadSerializer
		config                  *configs.Config
		metricsClient           metrics.Client
		logger                  log.Logger
		replicationTaskExecutor replicationTaskExecutor

		hostRateLimiter  *quotas.DynamicRateLimiter
		shardRateLimiter *quotas.DynamicRateLimiter

		taskRetryPolicy backoff.RetryPolicy
		dlqRetryPolicy  backoff.RetryPolicy

		// send side
		minTxAckedTaskID int64
		// recv side
		maxRxProcessedTaskID int64

		requestChan   chan<- *replicationTaskRequest
		syncShardChan chan *replicationspb.SyncShardStatus
		shutdownChan  chan struct{}
	}

	// ReplicationTaskProcessor is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessor interface {
		common.Daemon
	}

	replicationTaskRequest struct {
		token    *replicationspb.ReplicationToken
		respChan chan<- *replicationspb.ReplicationMessages
	}
)

// NewReplicationTaskProcessor creates a new replication task processor.
func NewReplicationTaskProcessor(
	shard shard.Context,
	historyEngine shard.Engine,
	config *configs.Config,
	metricsClient metrics.Client,
	replicationTaskFetcher ReplicationTaskFetcher,
	replicationTaskExecutor replicationTaskExecutor,
) *ReplicationTaskProcessorImpl {
	shardID := shard.GetShardID()
	taskRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait(shardID))
	taskRetryPolicy.SetBackoffCoefficient(taskErrorRetryBackoffCoefficient)
	taskRetryPolicy.SetMaximumInterval(taskErrorRetryMaxInterval)
	taskRetryPolicy.SetMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts(shardID))

	dlqRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait(shardID))
	dlqRetryPolicy.SetBackoffCoefficient(taskErrorRetryBackoffCoefficient)
	dlqRetryPolicy.SetMaximumInterval(taskErrorRetryMaxInterval)
	dlqRetryPolicy.SetMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts(shardID))

	return &ReplicationTaskProcessorImpl{
		currentCluster:          shard.GetClusterMetadata().GetCurrentClusterName(),
		sourceCluster:           replicationTaskFetcher.GetSourceCluster(),
		status:                  common.DaemonStatusInitialized,
		shard:                   shard,
		historyEngine:           historyEngine,
		historySerializer:       persistence.NewPayloadSerializer(),
		config:                  config,
		metricsClient:           metricsClient,
		logger:                  shard.GetLogger(),
		replicationTaskExecutor: replicationTaskExecutor,
		hostRateLimiter:         replicationTaskFetcher.GetRateLimiter(),
		shardRateLimiter: quotas.NewDynamicRateLimiter(func() float64 {
			return config.ReplicationTaskProcessorShardQPS()
		}), taskRetryPolicy: taskRetryPolicy,
		requestChan:          replicationTaskFetcher.GetRequestChan(),
		syncShardChan:        make(chan *replicationspb.SyncShardStatus, 1),
		shutdownChan:         make(chan struct{}),
		minTxAckedTaskID:     persistence.EmptyQueueMessageID,
		maxRxProcessedTaskID: persistence.EmptyQueueMessageID,
	}
}

// Start starts the processor
func (p *ReplicationTaskProcessorImpl) Start() {
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
func (p *ReplicationTaskProcessorImpl) Stop() {
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

func (p *ReplicationTaskProcessorImpl) eventLoop() {
	shardID := p.shard.GetShardID()

	syncShardTimer := time.NewTimer(backoff.JitDuration(
		p.config.ShardSyncMinInterval(),
		p.config.ShardSyncTimerJitterCoefficient(),
	))
	defer syncShardTimer.Stop()

	cleanupTimer := time.NewTimer(backoff.JitDuration(
		p.config.ReplicationTaskProcessorCleanupInterval(shardID),
		p.config.ReplicationTaskProcessorCleanupJitterCoefficient(shardID),
	))
	defer cleanupTimer.Stop()

	var syncShardTask *replicationspb.SyncShardStatus
	for {
		select {
		case syncShardTask = <-p.syncShardChan:

		case <-syncShardTimer.C:
			if err := p.handleSyncShardStatus(syncShardTask); err != nil {
				p.logger.Error("unable to sync shard status", tag.Error(err))
				p.metricsClient.Scope(metrics.HistorySyncShardStatusScope).IncCounter(metrics.SyncShardFromRemoteFailure)
			}
			syncShardTimer.Reset(backoff.JitDuration(
				p.config.ShardSyncMinInterval(),
				p.config.ShardSyncTimerJitterCoefficient(),
			))

		case <-cleanupTimer.C:
			if p.config.EnableCleanupReplicationTask() {
				if err := p.cleanupReplicationTasks(); err != nil {
					p.logger.Error("Failed to clean up replication messages.", tag.Error(err))
					p.metricsClient.Scope(metrics.ReplicationTaskCleanupScope).IncCounter(metrics.ReplicationTaskCleanupFailure)
				}
			}
			cleanupTimer.Reset(backoff.JitDuration(
				p.config.ReplicationTaskProcessorCleanupInterval(shardID),
				p.config.ReplicationTaskProcessorCleanupJitterCoefficient(shardID),
			))

		case <-p.shutdownChan:
			return

		default:
			if err := p.pollProcessReplicationTasks(); err != nil {
				p.logger.Error("unable to process replication tasks", tag.Error(err))
			}
		}
	}
}

func (p *ReplicationTaskProcessorImpl) pollProcessReplicationTasks() error {
	taskIterator := collection.NewPagingIterator(p.paginationFn)

	count := 0
	for taskIterator.HasNext() && !p.isStopped() {
		task, err := taskIterator.Next()
		if err != nil {
			return err
		}

		count++
		replicationTask := task.(*replicationspb.ReplicationTask)
		if err = p.applyReplicationTask(replicationTask); err != nil {
			return err
		}
		p.maxRxProcessedTaskID = replicationTask.GetSourceTaskId()
	}

	// TODO there should be better handling of remote not having replication tasks
	//  & make the application of replication task evenly distributed (in terms of time)
	//  stream / long poll API worth considering
	if count == 0 {
		time.Sleep(p.config.ReplicationTaskProcessorNoTaskRetryWait(p.shard.GetShardID()))
	}

	return nil
}

func (p *ReplicationTaskProcessorImpl) applyReplicationTask(
	replicationTask *replicationspb.ReplicationTask,
) error {
	err := p.handleReplicationTask(replicationTask)
	if err == nil || p.isStopped() {
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
	if err := p.handleReplicationDLQTask(request); err != nil {
		return err
	}
	return nil
}

func (p *ReplicationTaskProcessorImpl) handleSyncShardStatus(
	status *replicationspb.SyncShardStatus,
) error {

	now := p.shard.GetTimeSource().Now()
	if status == nil {
		return nil
	} else if now.Sub(timestamp.TimeValue(status.GetStatusTime())) > dropSyncShardTaskTimeThreshold {
		return nil
	}

	p.metricsClient.Scope(metrics.HistorySyncShardStatusScope).IncCounter(metrics.SyncShardFromRemoteCounter)
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncShardStatus(ctx, &historyservice.SyncShardStatusRequest{
		SourceCluster: p.sourceCluster,
		ShardId:       p.shard.GetShardID(),
		StatusTime:    status.StatusTime,
	})
}

func (p *ReplicationTaskProcessorImpl) handleReplicationTask(
	replicationTask *replicationspb.ReplicationTask,
) error {

	// TODO create a dedicated multi-stage rate limiter
	ctx := context.Background()
	_ = p.shardRateLimiter.Wait(ctx)
	_ = p.hostRateLimiter.Wait(ctx)

	operation := func() error {
		scope, err := p.replicationTaskExecutor.execute(replicationTask, false)
		p.emitTaskMetrics(scope, err)
		return err
	}
	return backoff.Retry(operation, p.taskRetryPolicy, p.isRetryableError)
}

func (p *ReplicationTaskProcessorImpl) handleReplicationDLQTask(
	request *persistence.PutReplicationTaskToDLQRequest,
) error {

	// TODO create a dedicated multi-stage rate limiter
	ctx := context.Background()
	_ = p.shardRateLimiter.Wait(ctx)
	_ = p.hostRateLimiter.Wait(ctx)

	p.logger.Info("enqueue replication task to DLQ",
		tag.WorkflowNamespaceID(request.TaskInfo.GetNamespaceId()),
		tag.WorkflowID(request.TaskInfo.GetWorkflowId()),
		tag.WorkflowRunID(request.TaskInfo.GetRunId()),
		tag.TaskID(request.TaskInfo.GetTaskId()),
	)
	p.metricsClient.Scope(
		metrics.ReplicationDLQStatsScope,
		metrics.TargetClusterTag(p.sourceCluster),
		metrics.InstanceTag(convert.Int32ToString(p.shard.GetShardID())),
	).UpdateGauge(
		metrics.ReplicationDLQMaxLevelGauge,
		float64(request.TaskInfo.GetTaskId()),
	)
	// The following is guaranteed to success or retry forever until processor is shutdown.
	return backoff.Retry(func() error {
		err := p.shard.GetExecutionManager().PutReplicationTaskToDLQ(request)
		if err != nil {
			p.logger.Error("failed to enqueue replication task to DLQ", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ReplicationTaskFetcherScope, metrics.ReplicationDLQFailed)
		}
		return err
	}, p.dlqRetryPolicy, p.isRetryableError)
}

func (p *ReplicationTaskProcessorImpl) convertTaskToDLQTask(
	replicationTask *replicationspb.ReplicationTask,
) (*persistence.PutReplicationTaskToDLQRequest, error) {
	switch replicationTask.TaskType {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		taskAttributes := replicationTask.GetSyncActivityTaskAttributes()
		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: taskAttributes.GetNamespaceId(),
				WorkflowId:  taskAttributes.GetWorkflowId(),
				RunId:       taskAttributes.GetRunId(),
				TaskId:      replicationTask.GetSourceTaskId(),
				TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
				ScheduledId: taskAttributes.GetScheduledId(),
				Version:     taskAttributes.GetVersion(),
			},
		}, nil

	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		taskAttributes := replicationTask.GetHistoryTaskV2Attributes()

		eventsDataBlob := persistence.NewDataBlobFromProto(taskAttributes.GetEvents())
		events, err := p.historySerializer.DeserializeEvents(eventsDataBlob)
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			p.logger.Error("Empty events in a batch")
			return nil, fmt.Errorf("corrupted history event batch, empty events")
		}
		firstEvent := events[0]
		lastEvent := events[len(events)-1]

		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId:  taskAttributes.GetNamespaceId(),
				WorkflowId:   taskAttributes.GetWorkflowId(),
				RunId:        taskAttributes.GetRunId(),
				TaskId:       replicationTask.GetSourceTaskId(),
				TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
				FirstEventId: firstEvent.GetEventId(),
				NextEventId:  lastEvent.GetEventId(),
				Version:      firstEvent.GetVersion(),
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown replication task type")
	}
}

func (p *ReplicationTaskProcessorImpl) paginationFn(_ []byte) ([]interface{}, []byte, error) {
	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	p.requestChan <- &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                p.shard.GetShardID(),
			LastProcessedMessageId: p.maxRxProcessedTaskID,
			LastRetrievedMessageId: p.maxRxProcessedTaskID,
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
		return tasks, nil, nil

	case <-p.shutdownChan:
		return nil, nil, nil
	}
}

func (p *ReplicationTaskProcessorImpl) cleanupReplicationTasks() error {

	clusterMetadata := p.shard.GetClusterMetadata()
	currentCluster := clusterMetadata.GetCurrentClusterName()
	var minAckedTaskID *int64
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if !clusterInfo.Enabled || clusterName == currentCluster {
			continue
		}

		ackLevel := p.shard.GetClusterReplicationLevel(clusterName)
		if minAckedTaskID == nil || ackLevel < *minAckedTaskID {
			minAckedTaskID = &ackLevel
		}
	}
	if minAckedTaskID == nil || *minAckedTaskID <= p.minTxAckedTaskID {
		return nil
	}

	p.logger.Info("cleaning up replication task queue", tag.ReadLevel(*minAckedTaskID))
	p.metricsClient.Scope(metrics.ReplicationTaskCleanupScope).IncCounter(metrics.ReplicationTaskCleanupCount)
	p.metricsClient.Scope(
		metrics.ReplicationTaskFetcherScope,
		metrics.TargetClusterTag(p.currentCluster),
	).RecordTimer(
		metrics.ReplicationTasksLag,
		time.Duration(p.shard.GetTransferMaxReadLevel()-*minAckedTaskID),
	)
	err := p.shard.GetExecutionManager().RangeCompleteReplicationTask(
		&persistence.RangeCompleteReplicationTaskRequest{
			InclusiveEndTaskID: *minAckedTaskID,
		},
	)
	if err == nil {
		p.minTxAckedTaskID = *minAckedTaskID
	}
	return err
}

func (p *ReplicationTaskProcessorImpl) emitTaskMetrics(scope int, err error) {
	if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
		p.metricsClient.IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
		return
	}

	// Also update counter to distinguish between type of failures
	switch err.(type) {
	case nil:
		p.metricsClient.IncCounter(scope, metrics.ReplicationTasksApplied)
	case *serviceerrors.ShardOwnershipLost:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrShardOwnershipLostCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	case *serviceerror.InvalidArgument:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	case *serviceerror.NamespaceNotActive:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrNamespaceNotActiveCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	case *serviceerror.NotFound:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrNotFoundCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	case *serviceerror.ResourceExhausted:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	case *serviceerrors.RetryReplication:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	default:
		p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)
	}
}

func (p *ReplicationTaskProcessorImpl) isStopped() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStopped
}

func (p *ReplicationTaskProcessorImpl) isRetryableError(
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
