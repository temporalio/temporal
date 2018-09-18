// Copyright (c) 2017 Uber Technologies, Inc.
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

package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/yarpc/yarpcerrors"
)

type (
	// DomainReplicator is the interface which can replicate the domain
	DomainReplicator interface {
		HandleReceivingTask(task *replicator.DomainTaskAttributes) error
	}

	replicationTaskProcessor struct {
		currentCluster   string
		sourceCluster    string
		topicName        string
		consumerName     string
		client           messaging.Client
		consumer         messaging.Consumer
		isStarted        int32
		isStopped        int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		config           *Config
		logger           bark.Logger
		metricsClient    metrics.Client
		domainReplicator DomainReplicator
		historyClient    history.Client
		msgEncoder       codec.BinaryEncoder
	}
)

const (
	retryErrorWaitMillis = 100
)

const (
	replicationTaskInitialRetryInterval = 100 * time.Millisecond
	replicationTaskMaxRetryInterval     = 2 * time.Second
	replicationTaskExpirationInterval   = 10 * time.Second
)

var (
	// ErrEmptyReplicationTask is the error to indicate empty replication task
	ErrEmptyReplicationTask = &shared.BadRequestError{Message: "empty replication task"}
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = &shared.BadRequestError{Message: "unknown replication task"}
	// ErrDeserializeReplicationTask is the error to indicate failure to deserialize replication task
	ErrDeserializeReplicationTask = &shared.BadRequestError{Message: "Failed to deserialize replication task"}

	replicationTaskRetryPolicy = createReplicatorRetryPolicy()
)

func newReplicationTaskProcessor(currentCluster, sourceCluster, consumer string, client messaging.Client, config *Config,
	logger bark.Logger, metricsClient metrics.Client, domainReplicator DomainReplicator,
	historyClient history.Client) *replicationTaskProcessor {

	retryableHistoryClient := history.NewRetryableClient(historyClient, common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError)

	return &replicationTaskProcessor{
		currentCluster: currentCluster,
		sourceCluster:  sourceCluster,
		consumerName:   consumer,
		client:         client,
		shutdownCh:     make(chan struct{}),
		config:         config,
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueReplicationTaskProcessorComponent,
			logging.TagSourceCluster:     sourceCluster,
			logging.TagConsumerName:      consumer,
		}),
		metricsClient:    metricsClient,
		domainReplicator: domainReplicator,
		historyClient:    retryableHistoryClient,
		msgEncoder:       codec.NewThriftRWEncoder(),
	}
}

func (p *replicationTaskProcessor) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return nil
	}

	logging.LogReplicationTaskProcessorStartingEvent(p.logger)
	consumer, err := p.client.NewConsumer(p.currentCluster, p.sourceCluster, p.consumerName, p.config.ReplicatorConcurrency)
	if err != nil {
		logging.LogReplicationTaskProcessorStartFailedEvent(p.logger, err)
		return err
	}

	if err := consumer.Start(); err != nil {
		logging.LogReplicationTaskProcessorStartFailedEvent(p.logger, err)
		return err
	}

	p.consumer = consumer
	p.shutdownWG.Add(1)
	go p.processorPump()

	logging.LogReplicationTaskProcessorStartedEvent(p.logger)
	return nil
}

func (p *replicationTaskProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.isStopped, 0, 1) {
		return
	}

	logging.LogReplicationTaskProcessorShuttingDownEvent(p.logger)
	defer logging.LogReplicationTaskProcessorShutdownEvent(p.logger)

	if atomic.LoadInt32(&p.isStarted) == 1 {
		close(p.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		logging.LogReplicationTaskProcessorShutdownTimedoutEvent(p.logger)
	}
}

func (p *replicationTaskProcessor) processorPump() {
	defer p.shutdownWG.Done()

	var workerWG sync.WaitGroup
	for workerID := 0; workerID < p.config.ReplicatorConcurrency; workerID++ {
		workerWG.Add(1)
		go p.messageProcessLoop(&workerWG, workerID)
	}

	select {
	case <-p.shutdownCh:
		// Processor is shutting down, close the underlying consumer
		p.consumer.Stop()
	}

	p.logger.Info("Replication task processor pump shutting down.")
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		p.logger.Warn("Replication task processor timed out on worker shutdown.")
	}
}

func (p *replicationTaskProcessor) messageProcessLoop(workerWG *sync.WaitGroup, workerID int) {
	defer workerWG.Done()

	for {
		select {
		case msg, ok := <-p.consumer.Messages():
			if !ok {
				p.logger.Info("Worker for replication task processor shutting down.")
				return // channel closed
			}
			p.processWithRetry(msg, workerID)
		}
	}
}

func (p *replicationTaskProcessor) processWithRetry(msg messaging.Message, workerID int) {
	var err error
	logger := p.logger.WithFields(bark.Fields{
		logging.TagPartitionKey: msg.Partition(),
		logging.TagOffset:       msg.Offset(),
		logging.TagAttemptStart: time.Now(),
	})

	forceBuffer := false
	remainingRetryCount := p.config.ReplicationTaskMaxRetry

	attempt := 0
	op := func() error {
		attempt++
		logger, err = p.process(msg, logger, forceBuffer)
		if err != nil && p.isRetryTaskError(err) {
			// Enable buffering of replication tasks for next attempt
			forceBuffer = true
		}

		return err
	}

ProcessRetryLoop:
	for {
		select {
		case <-p.shutdownCh:
			return
		default:
			// isTransientRetryableError is pretty broad on purpose as we want to retry replication tasks few times before
			// moving them to DLQ.
			err = backoff.Retry(op, replicationTaskRetryPolicy, p.isTransientRetryableError)
			if err != nil && p.isTransientRetryableError(err) {
				// Any whitelisted transient errors should be retried indefinitely
				if common.IsWhitelistServiceTransientError(err) {
					// Emit a warning log on every 100 transient error retries of replication task
					if attempt%100 == 0 {
						logger.WithFields(bark.Fields{
							logging.TagErr:          err,
							logging.TagAttemptCount: attempt,
							logging.TagAttemptEnd:   time.Now(),
						}).Warn("Error (transient) processing replication task.")
					}

					// Keep on retrying transient errors for ever
					continue ProcessRetryLoop
				}

				// Otherwise decrement the remaining retries and check if we have more attempts left.
				// This code path is needed to handle RetryTaskError so we can retry such replication tasks with forceBuffer
				// enabled.  Once all attempts are exhausted then msg will be nack'ed and moved to DLQ
				remainingRetryCount--
				if remainingRetryCount > 0 {
					continue ProcessRetryLoop
				}
			}

		}

		break ProcessRetryLoop
	}

	if err == nil {
		// Successfully processed replication task.  Ack message to move the cursor forward.
		msg.Ack()
	} else {
		// Task still failed after all retries.  This is most probably due to a bug in replication code.
		// Nack the task to move it to DLQ to not block replication for other workflow executions.
		logger.WithFields(bark.Fields{
			logging.TagErr:          err,
			logging.TagAttemptCount: attempt,
			logging.TagAttemptEnd:   time.Now(),
		}).Error("Error processing replication task.")
		msg.Nack()
	}
}

func (p *replicationTaskProcessor) process(msg messaging.Message, logger bark.Logger, inRetry bool) (bark.Logger, error) {
	scope := metrics.ReplicatorScope
	task, err := p.deserialize(msg.Value())
	if err != nil {
		p.updateFailureMetric(scope, err)
		logger.WithFields(bark.Fields{
			logging.TagErr: err,
		}).Error("Failed to deserialize replication task.")

		// return BadRequestError so processWithRetry can nack the message
		return logger, ErrDeserializeReplicationTask
	}

	if task.TaskType == nil {
		p.updateFailureMetric(scope, ErrEmptyReplicationTask)
		logger.WithFields(bark.Fields{
			logging.TagErr: ErrEmptyReplicationTask,
		}).Error("Task type is missing.")
		return logger, ErrEmptyReplicationTask
	}

	switch task.GetTaskType() {
	case replicator.ReplicationTaskTypeDomain:
		attr := task.DomainTaskAttributes
		logger = logger.WithFields(bark.Fields{
			logging.TagDomainID: attr.GetID(),
		})
		scope = metrics.DomainReplicationTaskScope
		err = p.handleDomainReplicationTask(task, logger)
	case replicator.ReplicationTaskTypeSyncShardStatus:
		scope = metrics.SyncShardTaskScope
		err = p.handleSyncShardTask(task, logger)
	case replicator.ReplicationTaskTypeHistory:
		attr := task.HistoryTaskAttributes
		logger = logger.WithFields(bark.Fields{
			logging.TagDomainID:            attr.GetDomainId(),
			logging.TagWorkflowExecutionID: attr.GetWorkflowId(),
			logging.TagWorkflowRunID:       attr.GetRunId(),
			logging.TagFirstEventID:        attr.GetFirstEventId(),
			logging.TagNextEventID:         attr.GetNextEventId(),
			logging.TagVersion:             attr.GetVersion(),
		})
		scope = metrics.HistoryReplicationTaskScope
		err = p.handleHistoryReplicationTask(task, logger, inRetry)
	default:
		logger.Error("Unknown task type.")
		err = ErrUnknownReplicationTask
	}

	if err != nil {
		p.updateFailureMetric(scope, err)
	}

	return logger, err
}

func (p *replicationTaskProcessor) handleDomainReplicationTask(task *replicator.ReplicationTask, logger bark.Logger) error {
	p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.DomainReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	logger.Debugf("Received domain replication task %v.", task.DomainTaskAttributes)
	return p.domainReplicator.HandleReceivingTask(task.DomainTaskAttributes)
}

func (p *replicationTaskProcessor) handleSyncShardTask(task *replicator.ReplicationTask, logger bark.Logger) error {
	p.metricsClient.IncCounter(metrics.SyncShardTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.SyncShardTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	attr := task.SyncShardStatusTaskAttributes
	logger.Debugf("Received sync shard task %v.", attr)

	req := &h.SyncShardStatusRequest{
		SourceCluster: attr.SourceCluster,
		ShardId:       attr.ShardId,
		Timestamp:     attr.Timestamp,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.historyClient.SyncShardStatus(ctx, req)
}

func (p *replicationTaskProcessor) handleHistoryReplicationTask(task *replicator.ReplicationTask, logger bark.Logger, inRetry bool) error {
	p.metricsClient.IncCounter(metrics.HistoryReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.HistoryReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	attr := task.HistoryTaskAttributes
	processTask := false
Loop:
	for _, cluster := range attr.TargetClusters {
		if p.currentCluster == cluster {
			processTask = true
			break Loop
		}
	}
	if !processTask {
		logger.Warn("Dropping non-targeted history task.")
		return nil
	}

	var err error
	req := &h.ReplicateEventsRequest{
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
		ForceBufferEvents: common.BoolPtr(inRetry),
	}

RetryLoop:
	for i := 0; i < p.config.ReplicatorBufferRetryCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = p.historyClient.ReplicateEvents(ctx, req)
		cancel()

		// Replication tasks could be slightly out of order for a particular workflow execution
		// We first try to apply the events without buffering enabled with a small delay to account for such delays
		// Caller should try to apply the event with buffering enabled once we return RetryTaskError after all retries
		if p.isRetryTaskError(err) {
			time.Sleep(retryErrorWaitMillis * time.Millisecond)
			continue RetryLoop
		}
		break RetryLoop
	}
	return err
}

func (p *replicationTaskProcessor) updateFailureMetric(scope int, err error) {
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

func (p *replicationTaskProcessor) isRetryTaskError(err error) bool {
	if _, ok := err.(*shared.RetryTaskError); ok {
		return true
	}

	return false
}

func (p *replicationTaskProcessor) isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *shared.BadRequestError:
		return false
	default:
		return true
	}
}

func (p *replicationTaskProcessor) deserialize(payload []byte) (*replicator.ReplicationTask, error) {
	var task replicator.ReplicationTask
	if err := p.msgEncoder.Decode(payload, &task); err != nil {
		return nil, err
	}

	return &task, nil
}

func createReplicatorRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(replicationTaskInitialRetryInterval)
	policy.SetMaximumInterval(replicationTaskMaxRetryInterval)
	policy.SetExpirationInterval(replicationTaskExpirationInterval)

	return policy
}
