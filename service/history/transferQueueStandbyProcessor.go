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

package history

import (
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/queue"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

type (
	transferQueueStandbyProcessorImpl struct {
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		clusterName        string
		shard              shard.Context
		config             *config.Config
		transferTaskFilter task.Filter
		logger             log.Logger
		metricsClient      metrics.Client
		taskExecutor       task.Executor
	}
)

func newTransferQueueStandbyProcessor(
	clusterName string,
	shard shard.Context,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	taskAllocator queue.TaskAllocator,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	queueTaskProcessor task.Processor,
	logger log.Logger,
) *transferQueueStandbyProcessorImpl {

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
		MetricScope:                         metrics.TransferStandbyQueueProcessorScope,
	}
	logger = logger.WithTags(tag.ClusterName(clusterName))

	transferTaskFilter := func(taskInfo task.Info) (bool, error) {
		task, ok := taskInfo.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyStandbyTask(clusterName, task.DomainID, task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateClusterAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferClusterAckLevel(clusterName, ackLevel)
	}
	transferQueueShutdown := func() error {
		return nil
	}

	processor := &transferQueueStandbyProcessorImpl{
		clusterName:        clusterName,
		shard:              shard,
		config:             shard.GetConfig(),
		transferTaskFilter: transferTaskFilter,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		taskExecutor: task.NewTransferStandbyTaskExecutor(
			shard,
			historyService.archivalClient,
			historyService.executionCache,
			historyRereplicator,
			nDCHistoryResender,
			logger,
			historyService.metricsClient,
			clusterName,
			config,
		),
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateClusterAckLevel,
			transferQueueShutdown,
			logger,
		),
	}

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		processor,
		shard.GetTransferClusterAckLevel(clusterName),
		logger,
	)

	redispatchQueue := collection.NewConcurrentQueue()

	transferQueueTaskInitializer := func(taskInfo task.Info) task.Task {
		return task.NewTransferTask(
			shard,
			taskInfo,
			task.QueueTypeStandbyTransfer,
			historyService.metricsClient.Scope(
				task.GetTransferTaskMetricsScope(taskInfo.GetTaskType(), false),
			),
			task.InitializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			transferTaskFilter,
			processor.taskExecutor,
			redispatchQueue,
			shard.GetTimeSource(),
			options.MaxRetryCount,
			queueAckMgr,
		)
	}

	queueProcessorBase := newQueueProcessorBase(
		clusterName,
		shard,
		options,
		processor,
		queueTaskProcessor,
		queueAckMgr,
		redispatchQueue,
		historyService.executionCache,
		transferQueueTaskInitializer,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferStandbyQueueProcessorScope),
	)

	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *transferQueueStandbyProcessorImpl) getTaskFilter() task.Filter {
	return t.transferTaskFilter
}

func (t *transferQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueStandbyProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.task)
}

func (t *transferQueueStandbyProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := task.GetTransferTaskMetricsScope(taskInfo.task.GetTaskType(), false)
	return metricScope, t.taskExecutor.Execute(taskInfo.task, taskInfo.shouldProcessTask)
}
