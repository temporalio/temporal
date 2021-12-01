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
	"time"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	historyRereplicationTimeout = 30 * time.Second
)

type (
	timerQueueStandbyProcessorImpl struct {
		shard                   shard.Context
		timerTaskFilter         taskFilter
		logger                  log.Logger
		metricsClient           metrics.Client
		timerGate               timer.RemoteGate
		timerQueueProcessorBase *timerQueueProcessorBase
		taskExecutor            queueTaskExecutor
	}
)

func newTimerQueueStandbyProcessor(
	shard shard.Context,
	historyService *historyEngineImpl,
	clusterName string,
	taskAllocator taskAllocator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	clientBean client.Bean,
) *timerQueueStandbyProcessorImpl {

	timeNow := func() time.Time {
		return shard.GetCurrentTime(clusterName)
	}
	updateShardAckLevel := func(ackLevel timerKey) error {
		return shard.UpdateTimerClusterAckLevel(clusterName, ackLevel.VisibilityTimestamp)
	}
	logger = log.With(logger, tag.ClusterName(clusterName))
	timerTaskFilter := func(task tasks.Task) (bool, error) {
		return taskAllocator.verifyStandbyTask(clusterName, namespace.ID(task.GetNamespaceID()), task)
	}

	timerGate := timer.NewRemoteGate()
	timerGate.SetCurrentTime(shard.GetCurrentTime(clusterName))
	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerStandbyQueueProcessorScope,
		shard,
		historyService.metricsClient,
		shard.GetTimerClusterAckLevel(clusterName),
		timeNow,
		updateShardAckLevel,
		logger,
		clusterName,
	)

	processor := &timerQueueStandbyProcessorImpl{
		shard:           shard,
		timerTaskFilter: timerTaskFilter,
		logger:          logger,
		metricsClient:   historyService.metricsClient,
		timerGate:       timerGate,
		taskExecutor: newTimerQueueStandbyTaskExecutor(
			shard,
			historyService,
			nDCHistoryResender,
			logger,
			historyService.metricsClient,
			clusterName,
			shard.GetConfig(),
			clientBean,
		),
	}

	processor.timerQueueProcessorBase = newTimerQueueProcessorBase(
		metrics.TimerStandbyQueueProcessorScope,
		shard,
		historyService,
		processor,
		timerQueueAckMgr,
		timerGate,
		shard.GetConfig().TimerProcessorMaxPollRPS,
		logger,
		shard.GetMetricsClient().Scope(metrics.TimerStandbyQueueProcessorScope),
	)

	return processor
}

func (t *timerQueueStandbyProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueStandbyProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
}

//nolint:unused
func (t *timerQueueStandbyProcessorImpl) getTimerFiredCount() uint64 {
	return t.timerQueueProcessorBase.getTimerFiredCount()
}

func (t *timerQueueStandbyProcessorImpl) setCurrentTime(
	currentTime time.Time,
) {

	t.timerGate.SetCurrentTime(currentTime)
}

func (t *timerQueueStandbyProcessorImpl) retryTasks() {
	t.timerQueueProcessorBase.retryTasks()
}

func (t *timerQueueStandbyProcessorImpl) getTaskFilter() taskFilter {
	return t.timerTaskFilter
}

func (t *timerQueueStandbyProcessorImpl) getAckLevel() timerKey {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

//nolint:unused
func (t *timerQueueStandbyProcessorImpl) getReadLevel() timerKey {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getReadLevel()
}

// NotifyNewTimers - Notify the processor about the new standby timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueStandbyProcessorImpl) notifyNewTimers(
	timerTasks []tasks.Task,
) {

	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}

func (t *timerQueueStandbyProcessorImpl) complete(
	taskInfo *taskInfo,
) {
	t.timerQueueProcessorBase.complete(taskInfo.Task)
}

func (t *timerQueueStandbyProcessorImpl) process(
	ctx context.Context,
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := getTimerTaskMetricScope(taskInfo.Task, false)
	return metricScope, t.taskExecutor.execute(ctx, taskInfo.Task, taskInfo.shouldProcessTask)
}
