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

package replicator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/task"
	"go.temporal.io/server/common/xdc"
)

type (
	replicationTaskProcessor struct {
		currentCluster                   string
		sourceCluster                    string
		consumerName                     string
		client                           messaging.Client
		consumer                         messaging.Consumer
		isStarted                        int32
		isStopped                        int32
		shutdownWG                       sync.WaitGroup
		shutdownCh                       chan struct{}
		config                           *Config
		logger                           log.Logger
		metricsClient                    metrics.Client
		namespacereplicationTaskExecutor namespace.ReplicationTaskExecutor
		nDCHistoryResender               xdc.NDCHistoryResender
		historyClient                    history.Client
		namespaceCache                   cache.NamespaceCache
		timeSource                       clock.TimeSource
		sequentialTaskProcessor          task.Processor
	}
)

const (
	dropSyncShardTaskTimeThreshold = 10 * time.Minute
)

var (
	// ErrEmptyReplicationTask is the error to indicate empty replication task
	ErrEmptyReplicationTask = serviceerror.NewInvalidArgument("empty replication task")
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = serviceerror.NewInvalidArgument("unknown replication task")
	// ErrDeserializeReplicationTask is the error to indicate failure to deserialize replication task
	ErrDeserializeReplicationTask = serviceerror.NewInvalidArgument("Failed to deserialize replication task")
)

func newReplicationTaskProcessor(
	currentCluster string,
	sourceCluster string,
	consumer string,
	client messaging.Client,
	config *Config,
	logger log.Logger,
	metricsClient metrics.Client,
	namespacereplicationTaskExecutor namespace.ReplicationTaskExecutor,
	nDCHistoryResender xdc.NDCHistoryResender,
	historyClient history.Client,
	namespaceCache cache.NamespaceCache,
	sequentialTaskProcessor task.Processor,
) *replicationTaskProcessor {

	return &replicationTaskProcessor{
		currentCluster:                   currentCluster,
		sourceCluster:                    sourceCluster,
		consumerName:                     consumer,
		client:                           client,
		shutdownCh:                       make(chan struct{}),
		config:                           config,
		logger:                           logger,
		metricsClient:                    metricsClient,
		namespacereplicationTaskExecutor: namespacereplicationTaskExecutor,
		nDCHistoryResender:               nDCHistoryResender,
		historyClient:                    historyClient,
		timeSource:                       clock.NewRealTimeSource(),
		namespaceCache:                   namespaceCache,
		sequentialTaskProcessor:          sequentialTaskProcessor,
	}
}

func (p *replicationTaskProcessor) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return nil
	}

	p.logger.Info("", tag.LifeCycleStarting, tag.ComponentReplicationTaskProcessor)
	consumer, err := p.client.NewConsumerWithClusterName(p.currentCluster, p.sourceCluster, p.consumerName, p.config.ReplicatorMessageConcurrency())
	if err != nil {
		p.logger.Info("", tag.LifeCycleStartFailed, tag.ComponentReplicationTaskProcessor, tag.Error(err))
		return err
	}

	if err := consumer.Start(); err != nil {
		p.logger.Info("", tag.LifeCycleStartFailed, tag.ComponentReplicationTaskProcessor, tag.Error(err))
		return err
	}

	p.consumer = consumer
	p.shutdownWG.Add(1)
	go p.processorPump()
	p.sequentialTaskProcessor.Start()

	p.logger.Info("", tag.LifeCycleStarted, tag.ComponentReplicationTaskProcessor)
	return nil
}

func (p *replicationTaskProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.isStopped, 0, 1) {
		return
	}

	p.sequentialTaskProcessor.Stop()
	p.logger.Info("", tag.LifeCycleStopping, tag.ComponentReplicationTaskProcessor)
	defer p.logger.Info("", tag.LifeCycleStopped, tag.ComponentReplicationTaskProcessor)

	if atomic.LoadInt32(&p.isStarted) == 1 {
		close(p.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Info("", tag.LifeCycleStopTimedout, tag.ComponentReplicationTaskProcessor)
	}
}

func (p *replicationTaskProcessor) processorPump() {
	defer p.shutdownWG.Done()

	var workerWG sync.WaitGroup
	for workerID := 0; workerID < p.config.ReplicatorMetaTaskConcurrency(); workerID++ {
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
			p.decodeMsgAndSubmit(msg)
		}
	}
}

func (p *replicationTaskProcessor) decodeMsgAndSubmit(msg messaging.Message) {
	logger := p.initLogger(msg)
	replicationTask, err := p.decodeAndValidateMsg(msg, logger)
	if err != nil {
		p.nackMsg(msg, err, logger)
		return
	}

SubmitLoop:
	for {
		var scope int
		switch replicationTask.GetTaskType() {
		case enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK:
			logger = logger.WithTags(tag.WorkflowNamespaceID(replicationTask.GetNamespaceTaskAttributes().GetId()))
			scope = metrics.NamespaceReplicationTaskScope
			err = p.handleNamespaceReplicationTask(replicationTask, msg, logger)
		case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK:
			scope = metrics.SyncShardTaskScope
			err = p.handleSyncShardTask(replicationTask, msg, logger)
		case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
			scope = metrics.SyncActivityTaskScope
			err = p.handleActivityTask(replicationTask, msg, logger)
		case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK:
			scope = metrics.HistoryMetadataReplicationTaskScope
			err = p.handleHistoryMetadataReplicationTask(replicationTask, msg, logger)
		case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
			scope = metrics.HistoryReplicationV2TaskScope
			err = p.handleHistoryReplicationV2Task(replicationTask, msg, logger)
		default:
			logger.Error("Unknown task type.")
			scope = metrics.ReplicatorScope
			err = ErrUnknownReplicationTask
		}

		if err != nil {
			p.updateFailureMetric(scope, err)
			if !isTransientRetryableError(err) {
				break SubmitLoop
			}
		} else {
			break SubmitLoop
		}
	}

	if err != nil {
		p.nackMsg(msg, err, logger)
	}
}

func (p *replicationTaskProcessor) initLogger(msg messaging.Message) log.Logger {
	return p.logger.WithTags(tag.KafkaPartition(msg.Partition()),
		tag.KafkaOffset(msg.Offset()),
		tag.AttemptStart(time.Now().UTC()))
}

func (p *replicationTaskProcessor) ackMsg(msg messaging.Message, logger log.Logger) {
	// the underlying implementation will not return anything other than nil
	// do logging just in case
	if err := msg.Ack(); err != nil {
		logger.Error("unable to ack", tag.Error(err))
	}
}

func (p *replicationTaskProcessor) nackMsg(msg messaging.Message, err error, logger log.Logger) {
	p.updateFailureMetric(metrics.ReplicatorScope, err)
	logger.Error(ErrDeserializeReplicationTask.Error(), tag.Error(err), tag.AttemptEnd(time.Now().UTC()))
	// the underlying implementation will not return anything other than nil
	// do logging just in case
	if err = msg.Nack(); err != nil {
		logger.Error("unable to nack", tag.Error(err))
	}
}

func (p *replicationTaskProcessor) decodeAndValidateMsg(msg messaging.Message, logger log.Logger) (*replicationspb.ReplicationTask, error) {
	var replicationTask replicationspb.ReplicationTask
	err := replicationTask.Unmarshal(msg.Value())
	if err != nil {
		// return InvalidArgument so processWithRetry can nack the message
		return nil, ErrDeserializeReplicationTask
	}

	return &replicationTask, nil
}

func (p *replicationTaskProcessor) handleNamespaceReplicationTask(task *replicationspb.ReplicationTask, msg messaging.Message, logger log.Logger) (retError error) {
	p.metricsClient.IncCounter(metrics.NamespaceReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.NamespaceReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	defer func() {
		if retError == nil {
			p.ackMsg(msg, logger)
		}
	}()

	err := p.namespacereplicationTaskExecutor.Execute(task.GetNamespaceTaskAttributes())
	if err != nil {
		return err
	}
	return nil
}

func (p *replicationTaskProcessor) handleSyncShardTask(task *replicationspb.ReplicationTask, msg messaging.Message, logger log.Logger) (retError error) {
	p.metricsClient.IncCounter(metrics.SyncShardTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.SyncShardTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	defer func() {
		if retError == nil {
			p.ackMsg(msg, logger)
		}
	}()

	attr := task.GetSyncShardStatusTaskAttributes()
	st := timestamp.TimeValue(attr.GetStatusTime())
	if timestamp.TimeNowPtrUtc().Sub(st) > dropSyncShardTaskTimeThreshold {
		return nil
	}

	req := &historyservice.SyncShardStatusRequest{
		SourceCluster: attr.SourceCluster,
		ShardId:       attr.ShardId,
		StatusTime:    &st,
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.config.ReplicationTaskContextTimeout())
	defer cancel()
	_, err := p.historyClient.SyncShardStatus(ctx, req)
	return err
}

func (p *replicationTaskProcessor) handleActivityTask(
	task *replicationspb.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
) error {

	doContinue, err := p.filterTask(task.GetSyncActivityTaskAttributes().GetNamespaceId())
	if err != nil || !doContinue {
		return err
	}

	activityReplicationTask := newActivityReplicationTask(
		task,
		msg,
		logger,
		p.config,
		p.timeSource,
		p.historyClient,
		p.metricsClient,
		p.nDCHistoryResender,
	)
	return p.sequentialTaskProcessor.Submit(activityReplicationTask)
}

func (p *replicationTaskProcessor) handleHistoryMetadataReplicationTask(
	task *replicationspb.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
) error {

	doContinue, err := p.filterTask(task.GetHistoryMetadataTaskAttributes().GetNamespaceId())
	if err != nil || !doContinue {
		return err
	}

	historyMetadataReplicationTask := newHistoryMetadataReplicationTask(
		task,
		msg,
		p.sourceCluster,
		logger,
		p.config,
		p.timeSource,
		p.historyClient,
		p.metricsClient,
		p.nDCHistoryResender,
	)
	return p.sequentialTaskProcessor.Submit(historyMetadataReplicationTask)
}

func (p *replicationTaskProcessor) handleHistoryReplicationV2Task(
	task *replicationspb.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
) error {

	doContinue, err := p.filterTask(task.GetHistoryTaskV2Attributes().GetNamespaceId())
	if err != nil || !doContinue {
		return err
	}

	historyReplicationTask := newHistoryReplicationV2Task(
		task,
		msg,
		logger,
		p.config,
		p.timeSource,
		p.historyClient,
		p.metricsClient,
		p.nDCHistoryResender,
	)
	return p.sequentialTaskProcessor.Submit(historyReplicationTask)
}

func (p *replicationTaskProcessor) filterTask(
	namespaceID string,
) (bool, error) {

	namespaceEntry, err := p.namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		return false, err
	}

	shouldProcessTask := false

FilterLoop:
	for _, targetCluster := range namespaceEntry.GetReplicationConfig().Clusters {
		if p.currentCluster == targetCluster {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return shouldProcessTask, nil
}

func (p *replicationTaskProcessor) updateFailureMetric(scope int, err error) {
	// Always update failure counter for all replicator errors
	p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)

	// Also update counter to distinguish between type of failures
	switch err.(type) {
	case *serviceerrors.ShardOwnershipLost:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrShardOwnershipLostCounter)
	case *serviceerror.InvalidArgument:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.NamespaceNotActive:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrNamespaceNotActiveCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrNotFoundCounter)
	case *serviceerror.ResourceExhausted:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerrors.RetryTask:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.DeadlineExceeded:
		p.metricsClient.IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
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
