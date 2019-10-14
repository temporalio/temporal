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

package history

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/yarpc/yarpcerrors"

	h "github.com/uber/cadence/.gen/go/history"
	r "github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/service/worker/replicator"
)

const (
	dropSyncShardTaskTimeThreshold            = 10 * time.Minute
	replicationTimeout                        = 30 * time.Second
	taskProcessorErrorRetryWait               = time.Second
	taskProcessorErrorRetryBackoffCoefficient = 1
	taskProcessorErrorRetryMaxAttampts        = 5
)

var (
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = &shared.BadRequestError{Message: "unknown replication task"}
)

type (
	// ReplicationTaskProcessor is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessor struct {
		currentCluster   string
		sourceCluster    string
		status           int32
		shard            ShardContext
		historyEngine    Engine
		domainCache      cache.DomainCache
		metricsClient    metrics.Client
		domainReplicator replicator.DomainReplicator
		logger           log.Logger

		retryPolicy          backoff.RetryPolicy
		noTaskBackoffRetrier backoff.Retrier

		lastProcessedMessageID int64
		lastRetrievedMessageID int64

		requestChan chan<- *request
		done        chan struct{}
	}

	request struct {
		token    *r.ReplicationToken
		respChan chan<- *r.ReplicationMessages
	}
)

// NewReplicationTaskProcessor creates a new replication task processor.
func NewReplicationTaskProcessor(
	shard ShardContext,
	historyEngine Engine,
	domainReplicator replicator.DomainReplicator,
	metricsClient metrics.Client,
	replicationTaskFetcher *ReplicationTaskFetcher,
) *ReplicationTaskProcessor {
	retryPolicy := backoff.NewExponentialRetryPolicy(taskProcessorErrorRetryWait)
	retryPolicy.SetBackoffCoefficient(taskProcessorErrorRetryBackoffCoefficient)
	retryPolicy.SetMaximumAttempts(taskProcessorErrorRetryMaxAttampts)

	var noTaskBackoffRetrier backoff.Retrier
	config := shard.GetClusterMetadata().GetReplicationConsumerConfig().ProcessorConfig
	// TODO: add a default noTaskBackoffRetrier?
	if config != nil {
		noTaskBackoffPolicy := backoff.NewExponentialRetryPolicy(time.Duration(config.NoTaskInitialWaitIntervalSecs) * time.Second)
		noTaskBackoffPolicy.SetBackoffCoefficient(config.NoTaskWaitBackoffCoefficient)
		noTaskBackoffPolicy.SetMaximumInterval(time.Duration(config.NoTaskMaxWaitIntervalSecs) * time.Second)
		noTaskBackoffPolicy.SetExpirationInterval(backoff.NoInterval)
		noTaskBackoffRetrier = backoff.NewRetrier(noTaskBackoffPolicy, backoff.SystemClock)
	}

	return &ReplicationTaskProcessor{
		currentCluster:       shard.GetClusterMetadata().GetCurrentClusterName(),
		sourceCluster:        replicationTaskFetcher.GetSourceCluster(),
		status:               common.DaemonStatusInitialized,
		shard:                shard,
		historyEngine:        historyEngine,
		domainCache:          shard.GetDomainCache(),
		metricsClient:        metricsClient,
		domainReplicator:     domainReplicator,
		logger:               shard.GetLogger(),
		retryPolicy:          retryPolicy,
		noTaskBackoffRetrier: noTaskBackoffRetrier,
		requestChan:          replicationTaskFetcher.GetRequestChan(),
		done:                 make(chan struct{}),
	}
}

// Start starts the processor
func (p *ReplicationTaskProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
	p.logger.Info("ReplicationTaskProcessor started.")
}

// Stop stops the processor
func (p *ReplicationTaskProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(p.done)
}

func (p *ReplicationTaskProcessor) processorLoop() {
	p.lastProcessedMessageID = p.shard.GetClusterReplicationLevel(p.sourceCluster)
	scope := p.metricsClient.Scope(metrics.ReplicationTaskFetcherScope, metrics.TargetClusterTag(p.sourceCluster))

	defer func() {
		p.logger.Info("Closing replication task processor.", tag.ReadLevel(p.lastRetrievedMessageID))
	}()

Loop:
	for {
		// for each iteration, do close check first
		select {
		case <-p.done:
			return
		default:
		}

		respChan := make(chan *r.ReplicationMessages, 1)
		// TODO: when we support prefetching, LastRetrivedMessageId can be different than LastProcessedMessageId
		p.requestChan <- &request{
			token: &r.ReplicationToken{
				ShardID:                common.Int32Ptr(int32(p.shard.GetShardID())),
				LastRetrivedMessageId:  common.Int64Ptr(p.lastRetrievedMessageID),
				LastProcessedMessageId: common.Int64Ptr(p.lastProcessedMessageID),
			},
			respChan: respChan,
		}

		select {
		case response, ok := <-respChan:
			if !ok {
				p.logger.Debug("Fetch replication messages chan closed.")
				continue Loop
			}

			p.logger.Debug("Got fetch replication messages response.",
				tag.ReadLevel(response.GetLastRetrivedMessageId()),
				tag.Bool(response.GetHasMore()),
				tag.Counter(len(response.GetReplicationTasks())),
			)

			// Note here we check replication tasks instead of hasMore. The expectation is that in a steady state
			// we will receive replication tasks but hasMore is false (meaning that we are always catching up).
			// So hasMore might not be a good indicator for additional wait.
			if len(response.ReplicationTasks) == 0 {
				backoffDuration := p.noTaskBackoffRetrier.NextBackOff()
				time.Sleep(backoffDuration)
				continue
			}

			for _, replicationTask := range response.ReplicationTasks {
				p.processTask(replicationTask)
			}

			p.lastProcessedMessageID = response.GetLastRetrivedMessageId()
			p.lastRetrievedMessageID = response.GetLastRetrivedMessageId()
			err := p.shard.UpdateClusterReplicationLevel(p.sourceCluster, p.lastRetrievedMessageID)
			if err != nil {
				p.logger.Error("Error updating replication level for shard", tag.Error(err), tag.OperationFailed)
			}

			scope.UpdateGauge(metrics.LastRetrievedMessageID, float64(p.lastRetrievedMessageID))
			p.noTaskBackoffRetrier.Reset()
		case <-p.done:
			return
		}
	}
}

func (p *ReplicationTaskProcessor) processTask(replicationTask *r.ReplicationTask) {
	var err error

	for execute := true; execute; execute = err != nil {
		err = backoff.Retry(func() error {
			return p.processTaskOnce(replicationTask)
		}, p.retryPolicy, isTransientRetryableError)

		if err != nil {
			// TODO: insert into our own dlq in cadence persistence?
			// p.nackMsg(msg, err, logger)
			p.logger.Error(
				"Failed to apply replication task after retry.",
				tag.TaskID(replicationTask.GetSourceTaskId()),
				tag.Error(err),
			)
		}
	}
}

func (p *ReplicationTaskProcessor) processTaskOnce(replicationTask *r.ReplicationTask) error {
	var err error
	var scope int
	switch replicationTask.GetTaskType() {
	case r.ReplicationTaskTypeDomain:
		scope = metrics.DomainReplicationTaskScope
		err = p.handleDomainReplicationTask(replicationTask)
	case r.ReplicationTaskTypeSyncShardStatus:
		scope = metrics.SyncShardTaskScope
		err = p.handleSyncShardTask(replicationTask)
	case r.ReplicationTaskTypeSyncActivity:
		scope = metrics.SyncActivityTaskScope
		err = p.handleActivityTask(replicationTask)
	case r.ReplicationTaskTypeHistory:
		scope = metrics.HistoryReplicationTaskScope
		err = p.handleHistoryReplicationTask(replicationTask)
	case r.ReplicationTaskTypeHistoryMetadata:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
	case r.ReplicationTaskTypeHistoryV2:
		scope = metrics.HistoryReplicationV2TaskScope
		err = p.handleHistoryReplicationTaskV2(replicationTask)
	default:
		p.logger.Error("Unknown task type.")
		scope = metrics.ReplicatorScope
		err = ErrUnknownReplicationTask
	}

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

func isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *shared.BadRequestError:
		return false
	default:
		return true
	}
}

func (p *ReplicationTaskProcessor) updateFailureMetric(scope int, err error) {
	// Always update failure counter for all replicator errors
	p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)

	// Also update counter to distinguish between type of failures
	switch err := err.(type) {
	case *h.ShardOwnershipLostError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrShardOwnershipLostCounter)
	case *shared.BadRequestError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *shared.DomainNotActiveError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrDomainNotActiveCounter)
	case *shared.WorkflowExecutionAlreadyStartedError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
	case *shared.EntityNotExistsError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *shared.LimitExceededError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrLimitExceededCounter)
	case *shared.RetryTaskError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrRetryTaskCounter)
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			p.metricsClient.IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
		}
	}
}

func (p *ReplicationTaskProcessor) handleActivityTask(
	task *r.ReplicationTask,
) error {

	attr := task.SyncActicvityTaskAttributes
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.SyncActivityRequest{
		DomainId:           attr.DomainId,
		WorkflowId:         attr.WorkflowId,
		RunId:              attr.RunId,
		Version:            attr.Version,
		ScheduledId:        attr.ScheduledId,
		ScheduledTime:      attr.ScheduledTime,
		StartedId:          attr.StartedId,
		StartedTime:        attr.StartedTime,
		LastHeartbeatTime:  attr.LastHeartbeatTime,
		Details:            attr.Details,
		Attempt:            attr.Attempt,
		LastFailureReason:  attr.LastFailureReason,
		LastWorkerIdentity: attr.LastWorkerIdentity,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncActivity(ctx, request)
}

func (p *ReplicationTaskProcessor) handleHistoryReplicationTask(
	task *r.ReplicationTask,
) error {

	attr := task.HistoryTaskAttributes
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(p.sourceCluster),
		DomainUUID:    attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		FirstEventId:      attr.FirstEventId,
		NextEventId:       attr.NextEventId,
		Version:           attr.Version,
		ReplicationInfo:   attr.ReplicationInfo,
		History:           attr.History,
		NewRunHistory:     attr.NewRunHistory,
		ForceBufferEvents: common.BoolPtr(false),
		ResetWorkflow:     attr.ResetWorkflow,
		NewRunNDC:         attr.NewRunNDC,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.ReplicateEvents(ctx, request)
}

func (p *ReplicationTaskProcessor) handleHistoryReplicationTaskV2(
	task *r.ReplicationTask,
) error {

	attr := task.HistoryTaskV2Attributes
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.ReplicateEventsV2Request{
		DomainUUID: attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		VersionHistoryItems: attr.VersionHistoryItems,
		Events:              attr.Events,
		// new run events does not need version history since there is no prior events
		NewRunEvents: attr.NewRunEvents,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.ReplicateEventsV2(ctx, request)
}

func (p *ReplicationTaskProcessor) handleSyncShardTask(
	task *r.ReplicationTask,
) error {

	attr := task.SyncShardStatusTaskAttributes
	if time.Now().Sub(time.Unix(0, attr.GetTimestamp())) > dropSyncShardTaskTimeThreshold {
		return nil
	}

	req := &h.SyncShardStatusRequest{
		SourceCluster: attr.SourceCluster,
		ShardId:       attr.ShardId,
		Timestamp:     attr.Timestamp,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncShardStatus(ctx, req)
}

func (p *ReplicationTaskProcessor) handleDomainReplicationTask(
	task *r.ReplicationTask,
) error {

	p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.DomainReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	return p.domainReplicator.HandleReceivingTask(task.DomainTaskAttributes)
}

func (p *ReplicationTaskProcessor) filterTask(
	domainID string,
) (bool, error) {

	domainEntry, err := p.domainCache.GetDomainByID(domainID)
	if err != nil {
		return false, err
	}

	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range domainEntry.GetReplicationConfig().Clusters {
		if p.currentCluster == targetCluster.ClusterName {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return shouldProcessTask, nil
}
