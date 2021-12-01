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

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	transferQueueStandbyProcessorImpl struct {
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		clusterName        string
		shard              shard.Context
		config             *configs.Config
		transferTaskFilter taskFilter
		logger             log.Logger
		metricsClient      metrics.Client
		taskExecutor       queueTaskExecutor
	}
)

func newTransferQueueStandbyProcessor(
	clusterName string,
	shard shard.Context,
	historyEngine *historyEngineImpl,
	taskAllocator taskAllocator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	clientBean client.Bean,
	matchingClient matchingservice.MatchingServiceClient,
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
	logger = log.With(logger, tag.ClusterName(clusterName))

	transferTaskFilter := func(task tasks.Task) (bool, error) {
		return taskAllocator.verifyStandbyTask(clusterName, namespace.ID(task.GetNamespaceID()), task)
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
		metricsClient:      historyEngine.metricsClient,
		taskExecutor: newTransferQueueStandbyTaskExecutor(
			shard,
			historyEngine,
			nDCHistoryResender,
			logger,
			historyEngine.metricsClient,
			clusterName,
			config,
			clientBean,
			matchingClient,
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

	queueProcessorBase := newQueueProcessorBase(
		clusterName,
		shard,
		options,
		processor,
		queueAckMgr,
		historyEngine.historyCache,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferStandbyQueueProcessorScope),
	)

	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *transferQueueStandbyProcessorImpl) getTaskFilter() taskFilter {
	return t.transferTaskFilter
}

func (t *transferQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueStandbyProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.Task)
}

func (t *transferQueueStandbyProcessorImpl) process(
	ctx context.Context,
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := getTransferTaskMetricsScope(taskInfo.Task, false)
	return metricScope, t.taskExecutor.execute(ctx, taskInfo.Task, taskInfo.shouldProcessTask)
}
