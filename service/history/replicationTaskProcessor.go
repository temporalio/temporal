// Copyright (c) 2019 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskProcessor_mock.go -self_package github.com/temporalio/temporal/service/history

package history

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	h "github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

const (
	dropSyncShardTaskTimeThreshold   = 10 * time.Minute
	replicationTimeout               = 30 * time.Second
	taskErrorRetryBackoffCoefficient = 1.2
	dlqErrorRetryWait                = time.Second
	emptyMessageID                   = -1
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
		shard                   ShardContext
		historyEngine           Engine
		historySerializer       persistence.PayloadSerializer
		config                  *Config
		metricsClient           metrics.Client
		logger                  log.Logger
		replicationTaskExecutor replicationTaskExecutor

		taskRetryPolicy backoff.RetryPolicy
		dlqRetryPolicy  backoff.RetryPolicy
		noTaskRetrier   backoff.Retrier

		lastProcessedMessageID int64
		lastRetrievedMessageID int64

		requestChan   chan<- *request
		syncShardChan chan *replication.SyncShardStatus
		done          chan struct{}
	}

	// ReplicationTaskProcessor is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessor interface {
		common.Daemon
	}

	request struct {
		token    *replication.ReplicationToken
		respChan chan<- *replication.ReplicationMessages
	}
)

// NewReplicationTaskProcessor creates a new replication task processor.
func NewReplicationTaskProcessor(
	shard ShardContext,
	historyEngine Engine,
	config *Config,
	metricsClient metrics.Client,
	replicationTaskFetcher ReplicationTaskFetcher,
	replicationTaskExecutor replicationTaskExecutor,
) *ReplicationTaskProcessorImpl {
	taskRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait())
	taskRetryPolicy.SetBackoffCoefficient(taskErrorRetryBackoffCoefficient)
	taskRetryPolicy.SetMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts())

	dlqRetryPolicy := backoff.NewExponentialRetryPolicy(dlqErrorRetryWait)
	dlqRetryPolicy.SetExpirationInterval(backoff.NoInterval)

	noTaskBackoffPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorNoTaskRetryWait())
	noTaskBackoffPolicy.SetBackoffCoefficient(1)
	noTaskBackoffPolicy.SetExpirationInterval(backoff.NoInterval)
	noTaskRetrier := backoff.NewRetrier(noTaskBackoffPolicy, backoff.SystemClock)
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
		taskRetryPolicy:         taskRetryPolicy,
		noTaskRetrier:           noTaskRetrier,
		requestChan:             replicationTaskFetcher.GetRequestChan(),
		syncShardChan:           make(chan *replication.SyncShardStatus),
		done:                    make(chan struct{}),
		lastProcessedMessageID:  emptyMessageID,
		lastRetrievedMessageID:  emptyMessageID,
	}
}

// Start starts the processor
func (p *ReplicationTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
	go p.syncShardStatusLoop()
	go p.cleanupReplicationTaskLoop()
	p.logger.Info("ReplicationTaskProcessor started.")
}

// Stop stops the processor
func (p *ReplicationTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("ReplicationTaskProcessor shutting down.")
	close(p.done)
}

func (p *ReplicationTaskProcessorImpl) processorLoop() {
	p.lastProcessedMessageID = p.shard.GetClusterReplicationLevel(p.sourceCluster)

	defer func() {
		p.logger.Info("Closing replication task processor.", tag.ReadLevel(p.lastRetrievedMessageID))
	}()

Loop:
	for {
		// for each iteration, do close check first
		select {
		case <-p.done:
			p.logger.Info("ReplicationTaskProcessor shutting down.")
			return
		default:
		}

		respChan := p.sendFetchMessageRequest()

		select {
		case response, ok := <-respChan:
			if !ok {
				p.logger.Debug("Fetch replication messages chan closed.")
				continue Loop
			}

			p.logger.Debug("Got fetch replication messages response.",
				tag.ReadLevel(response.GetLastRetrievedMessageId()),
				tag.Bool(response.GetHasMore()),
				tag.Counter(len(response.GetReplicationTasks())),
			)

			p.processResponse(response)
		case <-p.done:
			return
		}
	}
}

func (p *ReplicationTaskProcessorImpl) cleanupReplicationTaskLoop() {

	timer := time.NewTimer(backoff.JitDuration(
		p.config.ShardSyncMinInterval(),
		p.config.ShardSyncTimerJitterCoefficient(),
	))
	for {
		select {
		case <-p.done:
			timer.Stop()
			return
		case <-timer.C:
			err := p.cleanupAckedReplicationTasks()
			if err != nil {
				p.logger.Error("Failed to clean up replication messages.", tag.Error(err))
				p.metricsClient.Scope(metrics.ReplicationTaskCleanupScope).IncCounter(metrics.ReplicationTaskCleanupFailure)
			}
			timer.Reset(backoff.JitDuration(
				p.config.ShardSyncMinInterval(),
				p.config.ShardSyncTimerJitterCoefficient(),
			))
		}
	}
}

func (p *ReplicationTaskProcessorImpl) cleanupAckedReplicationTasks() error {

	clusterMetadata := p.shard.GetClusterMetadata()
	currentCluster := clusterMetadata.GetCurrentClusterName()
	minAckLevel := int64(math.MaxInt64)
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if !clusterInfo.Enabled {
			continue
		}

		if clusterName != currentCluster {
			ackLevel := p.shard.GetClusterReplicationLevel(clusterName)
			if ackLevel < minAckLevel {
				minAckLevel = ackLevel
			}
		}
	}

	p.logger.Info("Cleaning up replication task queue.", tag.ReadLevel(minAckLevel))
	p.metricsClient.Scope(metrics.ReplicationTaskCleanupScope).IncCounter(metrics.ReplicationTaskCleanupCount)
	return p.shard.GetExecutionManager().RangeCompleteReplicationTask(
		&persistence.RangeCompleteReplicationTaskRequest{
			InclusiveEndTaskID: minAckLevel,
		},
	)
}

func (p *ReplicationTaskProcessorImpl) sendFetchMessageRequest() <-chan *replication.ReplicationMessages {
	respChan := make(chan *replication.ReplicationMessages, 1)
	// TODO: when we support prefetching, LastRetrievedMessageId can be different than LastProcessedMessageId
	p.requestChan <- &request{
		token: &replication.ReplicationToken{
			ShardID:                int32(p.shard.GetShardID()),
			LastRetrievedMessageId: p.lastRetrievedMessageID,
			LastProcessedMessageId: p.lastProcessedMessageID,
		},
		respChan: respChan,
	}
	return respChan
}

func (p *ReplicationTaskProcessorImpl) processResponse(response *replication.ReplicationMessages) {

	p.syncShardChan <- response.GetSyncShardStatus()
	// Note here we check replication tasks instead of hasMore. The expectation is that in a steady state
	// we will receive replication tasks but hasMore is false (meaning that we are always catching up).
	// So hasMore might not be a good indicator for additional wait.
	if len(response.ReplicationTasks) == 0 {
		backoffDuration := p.noTaskRetrier.NextBackOff()
		time.Sleep(backoffDuration)
		return
	}

	for _, replicationTask := range response.ReplicationTasks {
		err := p.processSingleTask(replicationTask)
		if err != nil {
			// Processor is shutdown. Exit without updating the checkpoint.
			return
		}
	}

	p.lastProcessedMessageID = response.GetLastRetrievedMessageId()
	p.lastRetrievedMessageID = response.GetLastRetrievedMessageId()
	scope := p.metricsClient.Scope(metrics.ReplicationTaskFetcherScope, metrics.TargetClusterTag(p.sourceCluster))
	scope.UpdateGauge(metrics.LastRetrievedMessageID, float64(p.lastRetrievedMessageID))
	p.noTaskRetrier.Reset()
}

func (p *ReplicationTaskProcessorImpl) syncShardStatusLoop() {

	timer := time.NewTimer(backoff.JitDuration(
		p.config.ShardSyncMinInterval(),
		p.config.ShardSyncTimerJitterCoefficient(),
	))
	var syncShardTask *replication.SyncShardStatus
	for {
		select {
		case syncShardRequest := <-p.syncShardChan:
			syncShardTask = syncShardRequest
		case <-timer.C:
			if err := p.handleSyncShardStatus(
				syncShardTask,
			); err != nil {
				p.logger.Error("failed to sync shard status", tag.Error(err))
				p.metricsClient.Scope(metrics.HistorySyncShardStatusScope).IncCounter(metrics.SyncShardFromRemoteFailure)
			}
			timer.Reset(backoff.JitDuration(
				p.config.ShardSyncMinInterval(),
				p.config.ShardSyncTimerJitterCoefficient(),
			))
		case <-p.done:
			timer.Stop()
			return
		}
	}
}

func (p *ReplicationTaskProcessorImpl) handleSyncShardStatus(
	status *replication.SyncShardStatus,
) error {

	if status == nil ||
		p.shard.GetTimeSource().Now().Sub(
			time.Unix(0, status.GetTimestamp())) > dropSyncShardTaskTimeThreshold {
		return nil
	}
	p.metricsClient.Scope(metrics.HistorySyncShardStatusScope).IncCounter(metrics.SyncShardFromRemoteCounter)
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncShardStatus(ctx, &h.SyncShardStatusRequest{
		SourceCluster: common.StringPtr(p.sourceCluster),
		ShardId:       common.Int64Ptr(int64(p.shard.GetShardID())),
		Timestamp:     &status.Timestamp,
	})
}

func (p *ReplicationTaskProcessorImpl) processSingleTask(replicationTask *replication.ReplicationTask) error {
	err := backoff.Retry(func() error {
		return p.processTaskOnce(replicationTask)
	}, p.taskRetryPolicy, isTransientRetryableError)

	if err != nil {
		p.logger.Error(
			"Failed to apply replication task after retry. Putting task into DLQ.",
			tag.TaskID(replicationTask.GetSourceTaskId()),
			tag.Error(err),
		)

		return p.putReplicationTaskToDLQ(replicationTask)
	}

	return nil
}

func (p *ReplicationTaskProcessorImpl) processTaskOnce(replicationTask *replication.ReplicationTask) error {
	scope, err := p.replicationTaskExecutor.execute(
		p.sourceCluster,
		replicationTask,
		false)

	if err != nil {
		p.updateFailureMetric(scope, err)
	} else {
		p.logger.Debug("Successfully applied replication task.", tag.TaskID(replicationTask.GetSourceTaskId()))
		p.metricsClient.Scope(
			metrics.ReplicationTaskFetcherScope,
			metrics.TargetClusterTag(p.sourceCluster),
		).IncCounter(metrics.ReplicationTasksApplied)
	}

	return err
}

func (p *ReplicationTaskProcessorImpl) putReplicationTaskToDLQ(replicationTask *replication.ReplicationTask) error {
	request, err := p.generateDLQRequest(replicationTask)
	if err != nil {
		p.logger.Error("Failed to generate DLQ replication task.", tag.Error(err))
		// We cannot deserialize the task. Dropping it.
		return nil
	}

	// The following is guaranteed to success or retry forever until processor is shutdown.
	return backoff.Retry(func() error {
		err := p.shard.GetExecutionManager().PutReplicationTaskToDLQ(request)
		if err != nil {
			p.logger.Error("Failed to put replication task to DLQ.", tag.Error(err))
			p.metricsClient.IncCounter(metrics.ReplicationTaskFetcherScope, metrics.ReplicationDLQFailed)
		}
		return err
	}, p.dlqRetryPolicy, p.shouldRetryDLQ)
}

func (p *ReplicationTaskProcessorImpl) generateDLQRequest(
	replicationTask *replication.ReplicationTask,
) (*persistence.PutReplicationTaskToDLQRequest, error) {
	switch replicationTask.TaskType {
	case enums.ReplicationTaskTypeSyncActivity:
		taskAttributes := replicationTask.GetSyncActivityTaskAttributes()
		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistenceblobs.ReplicationTaskInfo{
				DomainID:    primitives.MustParseUUID(taskAttributes.GetDomainId()),
				WorkflowID:  taskAttributes.GetWorkflowId(),
				RunID:       primitives.MustParseUUID(taskAttributes.GetRunId()),
				TaskID:      replicationTask.GetSourceTaskId(),
				TaskType:    persistence.ReplicationTaskTypeSyncActivity,
				ScheduledID: taskAttributes.GetScheduledId(),
			},
		}, nil

	case enums.ReplicationTaskTypeHistory:
		taskAttributes := replicationTask.GetHistoryTaskAttributes()
		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistenceblobs.ReplicationTaskInfo{
				DomainID:            primitives.MustParseUUID(taskAttributes.GetDomainId()),
				WorkflowID:          taskAttributes.GetWorkflowId(),
				RunID:               primitives.MustParseUUID(taskAttributes.GetRunId()),
				TaskID:              replicationTask.GetSourceTaskId(),
				TaskType:            persistence.ReplicationTaskTypeHistory,
				FirstEventID:        taskAttributes.GetFirstEventId(),
				NextEventID:         taskAttributes.GetNextEventId(),
				Version:             taskAttributes.GetVersion(),
				LastReplicationInfo: taskAttributes.GetReplicationInfo(),
				ResetWorkflow:       taskAttributes.GetResetWorkflow(),
			},
		}, nil
	case enums.ReplicationTaskTypeHistoryV2:
		taskAttributes := replicationTask.GetHistoryTaskV2Attributes()

		eventsDataBlob := persistence.NewDataBlobFromProto(taskAttributes.GetEvents())
		events, err := p.historySerializer.DeserializeBatchEvents(eventsDataBlob)
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			p.logger.Error("Empty events in a batch")
			return nil, fmt.Errorf("corrupted history event batch, empty events")
		}

		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistenceblobs.ReplicationTaskInfo{
				DomainID:     primitives.MustParseUUID(taskAttributes.GetDomainId()),
				WorkflowID:   taskAttributes.GetWorkflowId(),
				RunID:        primitives.MustParseUUID(taskAttributes.GetRunId()),
				TaskID:       replicationTask.GetSourceTaskId(),
				TaskType:     persistence.ReplicationTaskTypeHistory,
				FirstEventID: events[0].GetEventId(),
				NextEventID:  events[len(events)-1].GetEventId(),
				Version:      events[0].GetVersion(),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown replication task type")
	}
}

func isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *serviceerror.InvalidArgument:
		return false
	default:
		return true
	}
}

func (p *ReplicationTaskProcessorImpl) shouldRetryDLQ(err error) bool {
	if err == nil {
		return false
	}

	select {
	case <-p.done:
		p.logger.Info("ReplicationTaskProcessor shutting down.")
		return false
	default:
		return true
	}
}

func toPersistenceReplicationInfo(
	info map[string]*replication.ReplicationInfo,
) map[string]*persistence.ReplicationInfo {
	replicationInfoMap := make(map[string]*persistence.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &persistence.ReplicationInfo{
			Version:     v.GetVersion(),
			LastEventID: v.GetLastEventId(),
		}
	}

	return replicationInfoMap
}

func (p *ReplicationTaskProcessorImpl) updateFailureMetric(scope int, err error) {
	// Always update failure counter for all replicator errors
	p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)

	// Also update counter to distinguish between type of failures
	switch err.(type) {
	case *serviceerror.ShardOwnershipLost:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrShardOwnershipLostCounter)
	case *serviceerror.InvalidArgument:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *serviceerror.DomainNotActive:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrDomainNotActiveCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *serviceerror.ResourceExhausted:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrLimitExceededCounter)
	case *serviceerror.RetryTask:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrRetryTaskCounter)
	case *serviceerror.DeadlineExceeded:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
	}
}
