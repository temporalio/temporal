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
	"fmt"
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
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
)

type (
	// DomainReplicator is the interface which can replicate the domain
	DomainReplicator interface {
		HandleReceivingTask(task *replicator.DomainTaskAttributes) error
	}

	replicationTaskProcessor struct {
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
	}
)

var (
	// ErrEmptyReplicationTask is the error to indicate empty replication task
	ErrEmptyReplicationTask = errors.New("empty replication task")
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = errors.New("unknown replication task")
)

func newReplicationTaskProcessor(topic, consumer string, client messaging.Client, config *Config,
	logger bark.Logger, metricsClient metrics.Client, domainReplicator DomainReplicator,
	historyClient history.Client) *replicationTaskProcessor {
	return &replicationTaskProcessor{
		topicName:    topic,
		consumerName: consumer,
		client:       client,
		shutdownCh:   make(chan struct{}),
		config:       config,
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueReplicationTaskProcessorComponent,
			logging.TagTopicName:         topic,
			logging.TagConsumerName:      consumer,
		}),
		metricsClient:    metricsClient,
		domainReplicator: domainReplicator,
		historyClient:    historyClient,
	}
}

func (p *replicationTaskProcessor) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return nil
	}

	logging.LogReplicationTaskProcessorStartingEvent(p.logger)
	consumer, err := p.client.NewConsumer(p.topicName, p.consumerName, p.config.ReplicatorConcurrency)
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
	for i := 0; i < p.config.ReplicatorConcurrency; i++ {
		workerWG.Add(1)
		go p.worker(&workerWG)
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

func (p *replicationTaskProcessor) worker(workerWG *sync.WaitGroup) {
	defer workerWG.Done()

	for {
		select {
		case msg, ok := <-p.consumer.Messages():
			if !ok {
				p.logger.Info("Worker for replication task processor shutting down.")
				return // channel closed
			}

			p.metricsClient.IncCounter(metrics.ReplicatorScope, metrics.ReplicatorMessages)
			sw := p.metricsClient.StartTimer(metrics.ReplicatorScope, metrics.ReplicatorLatency)

			// TODO: We skip over any messages which cannot be deserialized.  Figure out DLQ story for corrupted messages.
			task, err := deserialize(msg.Value())
			if err != nil {
				err = fmt.Errorf("Deserialize Error. Value: %v, Error: %v", string(msg.Value()), err)
			} else {

				// TODO: We need to figure out DLQ story for corrupted payload
				if task.TaskType == nil {
					err = ErrEmptyReplicationTask
				} else {
					switch task.GetTaskType() {
					case replicator.ReplicationTaskTypeDomain:
						p.logger.Debugf("Recieved domain replication task %v.", task.DomainTaskAttributes)
						err = p.domainReplicator.HandleReceivingTask(task.DomainTaskAttributes)
					case replicator.ReplicationTaskTypeHistory:
						err = p.historyClient.ReplicateEvents(context.Background(), &h.ReplicateEventsRequest{
							DomainUUID: task.HistoryTaskAttributes.DomainId,
							WorkflowExecution: &shared.WorkflowExecution{
								WorkflowId: task.HistoryTaskAttributes.WorkflowId,
								RunId:      task.HistoryTaskAttributes.RunId,
							},
							FirstEventId: task.HistoryTaskAttributes.FirstEventId,
							NextEventId:  task.HistoryTaskAttributes.NextEventId,
							Version:      task.HistoryTaskAttributes.Version,
							History:      task.HistoryTaskAttributes.History,
						})

					default:
						err = ErrUnknownReplicationTask
					}
				}
			}

			if err != nil {
				p.logger.WithField(logging.TagErr, err).Error("Error processing replication task.")
				p.metricsClient.IncCounter(metrics.ReplicatorScope, metrics.ReplicatorFailures)
				msg.Nack()
			} else {
				msg.Ack()
			}
			sw.Stop()
		case <-p.consumer.Closed():
			p.logger.Info("Consumer closed. Processor shutting down.")
			return
		}
	}
}

func deserialize(payload []byte) (*replicator.ReplicationTask, error) {
	var task replicator.ReplicationTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return nil, err
	}

	return &task, nil
}
