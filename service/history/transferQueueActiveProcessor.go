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

package history

import (
	"context"

	"github.com/pborman/uuid"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	transferQueueActiveProcessorImpl struct {
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		currentClusterName string
		shard              shard.Context
		transferTaskFilter taskFilter
		logger             log.Logger
		metricsClient      metrics.Client
		taskExecutor       queueTaskExecutor
	}
)

func newTransferQueueActiveProcessor(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	taskAllocator taskAllocator,
	logger log.Logger,
	registry namespace.Registry,
) *transferQueueActiveProcessorImpl {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                           config.TransferTaskBatchSize,
		WorkerCount:                         config.TransferTaskWorkerCount,
		MaxPollRPS:                          config.TransferProcessorMaxPollRPS,
		MaxPollInterval:                     config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                       config.TransferTaskMaxRetryCount,
		RedispatchInterval:                  config.TransferProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.TransferProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.TransferProcessorMaxRedispatchQueueSize,
		EnablePriorityTaskProcessor:         config.TransferProcessorEnablePriorityTaskProcessor,
		MetricScope:                         metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	logger = log.With(logger, tag.ClusterName(currentClusterName))
	transferTaskFilter := func(task tasks.Task) (bool, error) {
		return taskAllocator.verifyActiveTask(namespace.ID(task.GetNamespaceID()), task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateTransferAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferClusterAckLevel(currentClusterName, ackLevel)
	}

	transferQueueShutdown := func() error {
		return nil
	}

	processor := &transferQueueActiveProcessorImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		logger:             logger,
		metricsClient:      historyEngine.metricsClient,
		transferTaskFilter: transferTaskFilter,
		taskExecutor: newTransferQueueActiveTaskExecutor(
			shard,
			historyEngine,
			logger,
			historyEngine.metricsClient,
			config,
			matchingClient,
			registry,
		),
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateTransferAckLevel,
			transferQueueShutdown,
			logger,
		),
	}

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		processor,
		shard.GetTransferClusterAckLevel(currentClusterName),
		logger,
	)

	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		queueAckMgr,
		historyEngine.historyCache,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferActiveQueueProcessorScope),
	)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func newTransferQueueFailoverProcessor(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	namespaceIDs map[string]struct{},
	standbyClusterName string,
	minLevel int64,
	maxLevel int64,
	taskAllocator taskAllocator,
	logger log.Logger,
	registry namespace.Registry,
) (func(ackLevel int64) error, *transferQueueActiveProcessorImpl) {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                           config.TransferTaskBatchSize,
		WorkerCount:                         config.TransferTaskWorkerCount,
		MaxPollRPS:                          config.TransferProcessorFailoverMaxPollRPS,
		MaxPollInterval:                     config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                       config.TransferTaskMaxRetryCount,
		RedispatchInterval:                  config.TransferProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.TransferProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.TransferProcessorMaxRedispatchQueueSize,
		EnablePriorityTaskProcessor:         config.TransferProcessorEnablePriorityTaskProcessor,
		MetricScope:                         metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	failoverUUID := uuid.New()
	logger = log.With(
		logger,
		tag.ClusterName(currentClusterName),
		tag.WorkflowNamespaceIDs(namespaceIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	transferTaskFilter := func(task tasks.Task) (bool, error) {
		return taskAllocator.verifyFailoverActiveTask(namespaceIDs, namespace.ID(task.GetNamespaceID()), task)
	}
	maxReadAckLevel := func() int64 {
		return maxLevel // this is a const
	}
	failoverStartTime := shard.GetTimeSource().Now()
	updateTransferAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferFailoverLevel(
			failoverUUID,
			persistence.TransferFailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     minLevel,
				CurrentLevel: ackLevel,
				MaxLevel:     maxLevel,
				NamespaceIDs: namespaceIDs,
			},
		)
	}
	transferQueueShutdown := func() error {
		return shard.DeleteTransferFailoverLevel(failoverUUID)
	}

	processor := &transferQueueActiveProcessorImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		logger:             logger,
		metricsClient:      historyEngine.metricsClient,
		transferTaskFilter: transferTaskFilter,
		taskExecutor: newTransferQueueActiveTaskExecutor(
			shard,
			historyEngine,
			logger,
			historyEngine.metricsClient,
			config,
			matchingClient,
			registry,
		),
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateTransferAckLevel,
			transferQueueShutdown,
			logger,
		),
	}

	queueAckMgr := newQueueFailoverAckMgr(
		shard,
		options,
		processor,
		minLevel,
		logger,
	)

	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		queueAckMgr,
		historyEngine.historyCache,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferActiveQueueProcessorScope),
	)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase
	return updateTransferAckLevel, processor
}

func (t *transferQueueActiveProcessorImpl) getTaskFilter() taskFilter {
	return t.transferTaskFilter
}

func (t *transferQueueActiveProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueActiveProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.Task)
}

func (t *transferQueueActiveProcessorImpl) process(
	ctx context.Context,
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := getTransferTaskMetricsScope(taskInfo.Task, true)
	return metricScope, t.taskExecutor.execute(ctx, taskInfo.Task, taskInfo.shouldProcessTask)
}
