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
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"

	"context"

	"github.com/uber-common/bark"
	"github.com/uber-go/kafka-client/kafka"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/yarpc/yarpcerrors"
)

type workerStatus int

var errMaxAttemptReached = errors.New("Maximum attempts exceeded")

const (
	workerStatusRunning workerStatus = iota
	workerStatusPendingRetry
)

const (
	// below are the max retry count for worker

	// [0, 0.6) percentage max retry count
	retryCountInfinity int64 = math.MaxInt64 // using int64 max as infinity
	// [0.6, 0.7) percentage max retry count
	retryCount70PercentInRetry int64 = 256
	// [0.7, 0.8) percentage max retry count
	retryCount80PercentInRetry int64 = 128
	// [0.8, 0.9) percentage max retry count
	retryCount90PercentInRetry int64 = 64
	// [0.9, 0.95) percentage max retry count
	retryCount95PercentInRetry int64 = 32
	// [0.95, 1] percentage max retry count
	retryCount100PercentInRetry int64 = 8
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
		consumer         kafka.Consumer
		isStarted        int32
		isStopped        int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		config           *Config
		logger           bark.Logger
		metricsClient    metrics.Client
		domainReplicator DomainReplicator
		historyClient    history.Client

		// worker in retry count is used by underlying processor when doing retry on a task
		// this help the replicator / underlying processor understanding the overall
		// situation and giveup retrying
		workerInRetryCount int32
	}
)

const (
	replicationTaskInitialRetryInterval = 50 * time.Millisecond
	replicationTaskMaxRetryInterval     = 10 * time.Second
	replicationTaskExpirationInterval   = 30 * time.Second
)

var (
	// ErrEmptyReplicationTask is the error to indicate empty replication task
	ErrEmptyReplicationTask = &shared.BadRequestError{Message: "empty replication task"}
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask  = &shared.BadRequestError{Message: "unknown replication task"}
	replicationTaskRetryPolicy = createReplicatorRetryPolicy()
)

func newReplicationTaskProcessor(currentCluster, sourceCluster, consumer string, client messaging.Client, config *Config,
	logger bark.Logger, metricsClient metrics.Client, domainReplicator DomainReplicator,
	historyClient history.Client) *replicationTaskProcessor {
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
		metricsClient:      metricsClient,
		domainReplicator:   domainReplicator,
		historyClient:      historyClient,
		workerInRetryCount: 0,
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

func (p *replicationTaskProcessor) updateWorkerRetryStatus(isInRetry bool) {
	if isInRetry {
		atomic.AddInt32(&p.workerInRetryCount, 1)
	} else {
		atomic.AddInt32(&p.workerInRetryCount, -1)
	}
}

// getRemainingRetryCount returns the max retry count at the moment
func (p *replicationTaskProcessor) getRemainingRetryCount(remainingRetryCount int64) int64 {
	workerInRetry := float64(atomic.LoadInt32(&p.workerInRetryCount))
	numWorker := float64(p.config.ReplicatorConcurrency)
	retryPercentage := workerInRetry / numWorker

	min := func(i int64, j int64) int64 {
		if i < j {
			return i
		}
		return j
	}

	if retryPercentage < 0.6 {
		return min(remainingRetryCount, retryCountInfinity)
	}
	if retryPercentage < 0.7 {
		return min(remainingRetryCount, retryCount70PercentInRetry)
	}
	if retryPercentage < 0.8 {
		return min(remainingRetryCount, retryCount80PercentInRetry)
	}
	if retryPercentage < 0.9 {
		return min(remainingRetryCount, retryCount90PercentInRetry)
	}
	if retryPercentage < 0.95 {
		return min(remainingRetryCount, retryCount95PercentInRetry)
	}
	return min(remainingRetryCount, retryCount100PercentInRetry)
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
		case <-p.consumer.Closed():
			p.logger.Info("Consumer closed. Processor shutting down.")
			return
		}
	}
}

func (p *replicationTaskProcessor) processWithRetry(msg kafka.Message, workerID int) {
	var err error

	isInRetry := false
	remainingRetryCount := retryCountInfinity
	defer func() {
		if isInRetry {
			p.updateWorkerRetryStatus(false)
		}
	}()

ProcessRetryLoop:
	for {
		select {
		case <-p.shutdownCh:
			return
		default:
			op := func() error {
				remainingRetryCount--
				if remainingRetryCount <= 0 {
					return errMaxAttemptReached
				}

				errMsg := p.process(msg)
				if errMsg != nil && p.isTransientRetryableError(errMsg) {
					// Keep on retrying transient errors for ever
					if !isInRetry {
						isInRetry = true
						p.updateWorkerRetryStatus(true)
					}
					remainingRetryCount = p.getRemainingRetryCount(remainingRetryCount)
				}
				return errMsg
			}

			err = backoff.Retry(op, replicationTaskRetryPolicy, p.isTransientRetryableError)
			if err != nil && p.isTransientRetryableError(err) {
				// Keep on retrying transient errors for ever
				continue ProcessRetryLoop
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
		p.logger.WithFields(bark.Fields{
			logging.TagErr:          err,
			logging.TagPartitionKey: msg.Partition(),
			logging.TagOffset:       msg.Offset(),
		}).Error("Error processing replication task.")
		msg.Nack()
	}
}

func (p *replicationTaskProcessor) process(msg kafka.Message) error {
	scope := metrics.ReplicatorScope
	task, err := deserialize(msg.Value())
	if err != nil {
		p.updateFailureMetric(scope, err)
		p.logger.WithFields(bark.Fields{
			logging.TagErr:          err,
			logging.TagPartitionKey: msg.Partition(),
			logging.TagOffset:       msg.Offset(),
		}).Error("Failed to deserialize replication task.")
		return err
	}

	if task.TaskType == nil {
		p.updateFailureMetric(scope, ErrEmptyReplicationTask)
		return ErrEmptyReplicationTask
	}

	switch task.GetTaskType() {
	case replicator.ReplicationTaskTypeDomain:
		scope = metrics.DomainReplicationTaskScope
		err = p.handleDomainReplicationTask(task)
	case replicator.ReplicationTaskTypeHistory:
		scope = metrics.HistoryReplicationTaskScope
		err = p.handleHistoryReplicationTask(task)
	default:
		err = ErrUnknownReplicationTask
	}

	if err != nil {
		p.updateFailureMetric(scope, err)
	}

	return err
}

func (p *replicationTaskProcessor) handleDomainReplicationTask(task *replicator.ReplicationTask) error {
	p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.DomainReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	p.logger.Debugf("Received domain replication task %v.", task.DomainTaskAttributes)
	return p.domainReplicator.HandleReceivingTask(task.DomainTaskAttributes)
}

func (p *replicationTaskProcessor) handleHistoryReplicationTask(task *replicator.ReplicationTask) error {
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
		p.logger.Debugf("Dropping non-targeted history task with domainID: %v, workflowID: %v, runID: %v, firstEventID: %v, nextEventID: %v.",
			attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(), attr.GetFirstEventId(), attr.GetNextEventId())
		return nil
	}

	return p.historyClient.ReplicateEvents(context.Background(), &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(p.sourceCluster),
		DomainUUID:    attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		FirstEventId:    attr.FirstEventId,
		NextEventId:     attr.NextEventId,
		Version:         attr.Version,
		ReplicationInfo: attr.ReplicationInfo,
		History:         attr.History,
		NewRunHistory:   attr.NewRunHistory,
	})
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

func (p *replicationTaskProcessor) isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *h.ShardOwnershipLostError:
		return true
	case *shared.ServiceBusyError:
		return true
	case *shared.LimitExceededError:
		return true
	case *shared.InternalServiceError:
		return true
	case *shared.RetryTaskError:
		return true
	default:
		return false
	}
}

func deserialize(payload []byte) (*replicator.ReplicationTask, error) {
	var task replicator.ReplicationTask
	if err := json.Unmarshal(payload, &task); err != nil {
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
