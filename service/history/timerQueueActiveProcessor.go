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
	"time"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueActiveProcessorImpl struct {
		shard                   ShardContext
		timerTaskFilter         taskFilter
		now                     timeNow
		logger                  log.Logger
		metricsClient           metrics.Client
		currentClusterName      string
		taskExecutor            queueTaskExecutor
		timerQueueProcessorBase *timerQueueProcessorBase
	}
)

func newTimerQueueActiveProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	matchingClient matching.Client,
	taskAllocator taskAllocator,
	logger log.Logger,
) *timerQueueActiveProcessorImpl {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		return shard.GetCurrentTime(currentClusterName)
	}
	updateShardAckLevel := func(ackLevel timerKey) error {
		return shard.UpdateTimerClusterAckLevel(currentClusterName, ackLevel.VisibilityTimestamp)
	}
	logger = logger.WithTags(tag.ClusterName(currentClusterName))
	timerTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		timer, ok := taskInfo.(*persistence.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyActiveTask(timer.DomainID, timer)
	}

	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerActiveQueueProcessorScope,
		shard,
		historyService.metricsClient,
		shard.GetTimerClusterAckLevel(currentClusterName),
		timeNow,
		updateShardAckLevel,
		logger,
		currentClusterName,
	)

	timerGate := NewLocalTimerGate(shard.GetTimeSource())
	processor := &timerQueueActiveProcessorImpl{
		shard:              shard,
		timerTaskFilter:    timerTaskFilter,
		now:                timeNow,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		currentClusterName: currentClusterName,
		timerQueueProcessorBase: newTimerQueueProcessorBase(
			metrics.TimerActiveQueueProcessorScope,
			shard,
			historyService,
			timerQueueAckMgr,
			timerGate,
			shard.GetConfig().TimerProcessorMaxPollRPS,
			logger,
		),
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	processor.taskExecutor = newTimerQueueActiveTaskExecutor(
		shard,
		historyService,
		processor,
		logger,
		historyService.metricsClient,
		shard.GetConfig(),
	)
	return processor
}

func newTimerQueueFailoverProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	domainIDs map[string]struct{},
	standbyClusterName string,
	minLevel time.Time,
	maxLevel time.Time,
	matchingClient matching.Client,
	taskAllocator taskAllocator,
	logger log.Logger,
) (func(ackLevel timerKey) error, *timerQueueActiveProcessorImpl) {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		// should use current cluster's time when doing domain failover
		return shard.GetCurrentTime(currentClusterName)
	}
	failoverStartTime := shard.GetTimeSource().Now()
	failoverUUID := uuid.New()

	updateShardAckLevel := func(ackLevel timerKey) error {
		return shard.UpdateTimerFailoverLevel(
			failoverUUID,
			persistence.TimerFailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     minLevel,
				CurrentLevel: ackLevel.VisibilityTimestamp,
				MaxLevel:     maxLevel,
				DomainIDs:    domainIDs,
			},
		)
	}
	timerAckMgrShutdown := func() error {
		return shard.DeleteTimerFailoverLevel(failoverUUID)
	}

	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowDomainIDs(domainIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)
	timerTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		timer, ok := taskInfo.(*persistence.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyFailoverActiveTask(domainIDs, timer.DomainID, timer)
	}

	timerQueueAckMgr := newTimerQueueFailoverAckMgr(
		shard,
		historyService.metricsClient,
		minLevel,
		maxLevel,
		timeNow,
		updateShardAckLevel,
		timerAckMgrShutdown,
		logger,
	)

	timerGate := NewLocalTimerGate(shard.GetTimeSource())
	processor := &timerQueueActiveProcessorImpl{
		shard:              shard,
		timerTaskFilter:    timerTaskFilter,
		now:                timeNow,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		currentClusterName: currentClusterName,
		timerQueueProcessorBase: newTimerQueueProcessorBase(
			metrics.TimerActiveQueueProcessorScope,
			shard,
			historyService,
			timerQueueAckMgr,
			timerGate,
			shard.GetConfig().TimerProcessorFailoverMaxPollRPS,
			logger,
		),
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	processor.taskExecutor = newTimerQueueActiveTaskExecutor(
		shard,
		historyService,
		processor,
		logger,
		historyService.metricsClient,
		shard.GetConfig(),
	)
	return updateShardAckLevel, processor
}

func (t *timerQueueActiveProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueActiveProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
}

func (t *timerQueueActiveProcessorImpl) getTaskFilter() taskFilter {
	return t.timerTaskFilter
}

func (t *timerQueueActiveProcessorImpl) getAckLevel() timerKey {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

func (t *timerQueueActiveProcessorImpl) getReadLevel() timerKey {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getReadLevel()
}

// NotifyNewTimers - Notify the processor about the new active timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueActiveProcessorImpl) notifyNewTimers(
	timerTasks []persistence.Task,
) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}

func (t *timerQueueActiveProcessorImpl) complete(
	taskInfo *taskInfo,
) {
	timerTask, ok := taskInfo.task.(*persistence.TimerTaskInfo)
	if !ok {
		return
	}
	t.timerQueueProcessorBase.complete(timerTask)
}

func (t *timerQueueActiveProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := t.timerQueueProcessorBase.getTimerTaskMetricScope(taskInfo.task.GetTaskType(), true)
	return metricScope, t.taskExecutor.execute(taskInfo.task, taskInfo.shouldProcessTask)
}
