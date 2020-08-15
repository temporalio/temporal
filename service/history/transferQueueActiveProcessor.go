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
	"github.com/pborman/uuid"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

const identityHistoryService = "history-service"

type (
	transferQueueActiveProcessorImpl struct {
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		currentClusterName string
		shard              ShardContext
		transferTaskFilter taskFilter
		logger             log.Logger
		metricsClient      metrics.Client
		taskExecutor       queueTaskExecutor
	}
)

func newTransferQueueActiveProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	historyClient history.Client,
	taskAllocator taskAllocator,
	queueTaskProcessor queueTaskProcessor,
	logger log.Logger,
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
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	logger = logger.WithTags(tag.ClusterName(currentClusterName))
	transferTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		task, ok := taskInfo.(*persistenceblobs.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyActiveTask(task.GetNamespaceId(), task)
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
		metricsClient:      historyService.metricsClient,
		transferTaskFilter: transferTaskFilter,
		taskExecutor: newTransferQueueActiveTaskExecutor(
			shard,
			historyService,
			logger,
			historyService.metricsClient,
			config,
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

	redispatchQueue := collection.NewConcurrentQueue()

	transferQueueTaskInitializer := func(taskInfo queueTaskInfo) queueTask {
		return newTransferQueueTask(
			shard,
			taskInfo,
			historyService.metricsClient.Scope(
				getTransferTaskMetricsScope(taskInfo.GetTaskType(), true),
			),
			initializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			transferTaskFilter,
			processor.taskExecutor,
			redispatchQueue,
			shard.GetTimeSource(),
			options.MaxRetryCount,
			queueAckMgr,
		)
	}

	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		queueTaskProcessor,
		queueAckMgr,
		redispatchQueue,
		historyService.historyCache,
		transferQueueTaskInitializer,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferActiveQueueProcessorScope),
	)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func newTransferQueueFailoverProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	historyClient history.Client,
	namespaceIDs map[string]struct{},
	standbyClusterName string,
	minLevel int64,
	maxLevel int64,
	taskAllocator taskAllocator,
	queueTaskProcessor queueTaskProcessor,
	logger log.Logger,
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
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	failoverUUID := uuid.New()
	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowNamespaceIDs(namespaceIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	transferTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		task, ok := taskInfo.(*persistenceblobs.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyFailoverActiveTask(namespaceIDs, task.GetNamespaceId(), task)
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
		metricsClient:      historyService.metricsClient,
		transferTaskFilter: transferTaskFilter,
		taskExecutor: newTransferQueueActiveTaskExecutor(
			shard,
			historyService,
			logger,
			historyService.metricsClient,
			config,
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

	redispatchQueue := collection.NewConcurrentQueue()

	transferQueueTaskInitializer := func(taskInfo queueTaskInfo) queueTask {
		return newTransferQueueTask(
			shard,
			taskInfo,
			historyService.metricsClient.Scope(
				getTransferTaskMetricsScope(taskInfo.GetTaskType(), true),
			),
			initializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			transferTaskFilter,
			processor.taskExecutor,
			redispatchQueue,
			shard.GetTimeSource(),
			options.MaxRetryCount,
			queueAckMgr,
		)
	}

	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		queueTaskProcessor,
		queueAckMgr,
		redispatchQueue,
		historyService.historyCache,
		transferQueueTaskInitializer,
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

	t.queueProcessorBase.complete(taskInfo.task)
}

func (t *transferQueueActiveProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := getTransferTaskMetricsScope(taskInfo.task.GetTaskType(), true)
	return metricScope, t.taskExecutor.execute(taskInfo.task, taskInfo.shouldProcessTask)
}
