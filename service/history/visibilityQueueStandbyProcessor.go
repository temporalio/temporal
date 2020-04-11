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
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	visibilityQueueStandbyProcessorImpl struct {
		*visibilityQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		clusterName          string
		shard                ShardContext
		config               *Config
		visibilityTaskFilter taskFilter
		logger               log.Logger
		metricsClient        metrics.Client
		taskExecutor         queueTaskExecutor
	}
)

func newVisibilityQueueStandbyProcessor(
	clusterName string,
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	taskAllocator taskAllocator,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
) *visibilityQueueStandbyProcessorImpl {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                          config.VisibilityTaskBatchSize,
		WorkerCount:                        config.VisibilityTaskWorkerCount,
		MaxPollRPS:                         config.VisibilityProcessorMaxPollRPS,
		MaxPollInterval:                    config.VisibilityProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.VisibilityProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.VisibilityTaskMaxRetryCount,
		MetricScope:                        metrics.VisibilityStandbyQueueProcessorScope,
	}
	logger = logger.WithTags(tag.ClusterName(clusterName))

	visibilityTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		task, ok := taskInfo.(*persistenceblobs.VisibilityTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyStandbyTask(clusterName, primitives.UUID(task.DomainID).String(), task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetVisibilityMaxReadLevel()
	}
	updateClusterAckLevel := func(ackLevel int64) error {
		return shard.UpdateVisibilityClusterAckLevel(clusterName, ackLevel)
	}
	visibilityQueueShutdown := func() error {
		return nil
	}

	processor := &visibilityQueueStandbyProcessorImpl{
		clusterName:          clusterName,
		shard:                shard,
		config:               shard.GetConfig(),
		visibilityTaskFilter: visibilityTaskFilter,
		logger:               logger,
		metricsClient:        historyService.metricsClient,
		taskExecutor: newVisibilityQueueStandbyTaskExecutor(
			shard,
			historyService,
			historyRereplicator,
			nDCHistoryResender,
			logger,
			historyService.metricsClient,
			clusterName,
			config,
		),
		visibilityQueueProcessorBase: newVisibilityQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateClusterAckLevel,
			visibilityQueueShutdown,
			logger,
		),
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetVisibilityClusterAckLevel(clusterName), logger)
	queueProcessorBase := newQueueProcessorBase(clusterName, shard, options, processor, queueAckMgr, historyService.historyCache, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *visibilityQueueStandbyProcessorImpl) getTaskFilter() taskFilter {
	return t.visibilityTaskFilter
}

func (t *visibilityQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *visibilityQueueStandbyProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.task)
}

func (t *visibilityQueueStandbyProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := t.getVisibilityTaskMetricsScope(taskInfo.task.GetTaskType(), false)
	return metricScope, t.taskExecutor.execute(taskInfo.task, taskInfo.shouldProcessTask)
}
