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
	"github.com/pborman/uuid"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	visibilityQueueActiveProcessorImpl struct {
		*visibilityQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		currentClusterName   string
		shard                ShardContext
		visibilityTaskFilter taskFilter
		logger               log.Logger
		metricsClient        metrics.Client
		taskExecutor         queueTaskExecutor
	}
)

func newVisibilityQueueActiveProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	historyClient history.Client,
	taskAllocator taskAllocator,
	logger log.Logger,
) *visibilityQueueActiveProcessorImpl {

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
		MetricScope:                        metrics.VisibilityActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	logger = logger.WithTags(tag.ClusterName(currentClusterName))
	visibilityTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		task, ok := taskInfo.(*persistenceblobs.VisibilityTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyActiveTask(primitives.UUID(task.DomainID).String(), task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetVisibilityMaxReadLevel()
	}
	updateVisibilityAckLevel := func(ackLevel int64) error {
		return shard.UpdateVisibilityClusterAckLevel(currentClusterName, ackLevel)
	}

	visibilityQueueShutdown := func() error {
		return nil
	}

	processor := &visibilityQueueActiveProcessorImpl{
		currentClusterName:   currentClusterName,
		shard:                shard,
		logger:               logger,
		metricsClient:        historyService.metricsClient,
		visibilityTaskFilter: visibilityTaskFilter,
		taskExecutor: newVisibilityQueueActiveTaskExecutor(
			shard,
			historyService,
			logger,
			historyService.metricsClient,
			config,
		),
		visibilityQueueProcessorBase: newVisibilityQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateVisibilityAckLevel,
			visibilityQueueShutdown,
			logger,
		),
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetVisibilityClusterAckLevel(currentClusterName), logger)
	queueProcessorBase := newQueueProcessorBase(currentClusterName, shard, options, processor, queueAckMgr, historyService.historyCache, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func newVisibilityQueueFailoverProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	historyClient history.Client,
	domainIDs map[string]struct{},
	standbyClusterName string,
	minLevel int64,
	maxLevel int64,
	taskAllocator taskAllocator,
	logger log.Logger,
) (func(ackLevel int64) error, *visibilityQueueActiveProcessorImpl) {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                          config.VisibilityTaskBatchSize,
		WorkerCount:                        config.VisibilityTaskWorkerCount,
		MaxPollRPS:                         config.VisibilityProcessorFailoverMaxPollRPS,
		MaxPollInterval:                    config.VisibilityProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.VisibilityProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.VisibilityTaskMaxRetryCount,
		MetricScope:                        metrics.VisibilityActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	failoverUUID := uuid.New()
	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowDomainIDs(domainIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	visibilityTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		task, ok := taskInfo.(*persistenceblobs.VisibilityTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyFailoverActiveTask(domainIDs, primitives.UUID(task.DomainID).String(), task)
	}
	maxReadAckLevel := func() int64 {
		return maxLevel // this is a const
	}
	failoverStartTime := shard.GetTimeSource().Now()
	updateVisibilityAckLevel := func(ackLevel int64) error {
		return shard.UpdateVisibilityFailoverLevel(
			failoverUUID,
			persistence.VisibilityFailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     minLevel,
				CurrentLevel: ackLevel,
				MaxLevel:     maxLevel,
				DomainIDs:    domainIDs,
			},
		)
	}
	visibilityQueueShutdown := func() error {
		return shard.DeleteVisibilityFailoverLevel(failoverUUID)
	}

	processor := &visibilityQueueActiveProcessorImpl{
		currentClusterName:   currentClusterName,
		shard:                shard,
		logger:               logger,
		metricsClient:        historyService.metricsClient,
		visibilityTaskFilter: visibilityTaskFilter,
		taskExecutor: newVisibilityQueueActiveTaskExecutor(
			shard,
			historyService,
			logger,
			historyService.metricsClient,
			config,
		),
		visibilityQueueProcessorBase: newVisibilityQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateVisibilityAckLevel,
			visibilityQueueShutdown,
			logger,
		),
	}

	queueAckMgr := newQueueFailoverAckMgr(shard, options, processor, minLevel, logger)
	queueProcessorBase := newQueueProcessorBase(currentClusterName, shard, options, processor, queueAckMgr, historyService.historyCache, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase
	return updateVisibilityAckLevel, processor
}

func (t *visibilityQueueActiveProcessorImpl) getTaskFilter() taskFilter {
	return t.visibilityTaskFilter
}

func (t *visibilityQueueActiveProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *visibilityQueueActiveProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.task)
}

func (t *visibilityQueueActiveProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {
	metricScope := t.getVisibilityTaskMetricsScope(taskInfo.task.GetTaskType(), true)
	return metricScope, t.taskExecutor.execute(taskInfo.task, taskInfo.shouldProcessTask)
}
