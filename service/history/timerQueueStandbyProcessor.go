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

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueStandbyProcessorImpl struct {
		timerGate               timer.RemoteGate
		timerQueueProcessorBase *timerQueueProcessorBase

		// this is the scheduler owned by this standby queue processor
		ownedScheduler queues.Scheduler
	}
)

func newTimerQueueStandbyProcessor(
	shard shard.Context,
	workflowCache workflow.Cache,
	scheduler queues.Scheduler,
	workflowDeleteManager workflow.DeleteManager,
	matchingClient matchingservice.MatchingServiceClient,
	clusterName string,
	taskAllocator taskAllocator,
	clientBean client.Bean,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
) *timerQueueStandbyProcessorImpl {
	timeNow := func() time.Time {
		return shard.GetCurrentTime(clusterName)
	}
	updateShardAckLevel := func(ackLevel tasks.Key) error {
		return shard.UpdateQueueClusterAckLevel(
			tasks.CategoryTimer,
			clusterName,
			ackLevel,
		)
	}
	logger = log.With(logger, tag.ClusterName(clusterName))
	metricsClient := shard.GetMetricsClient()
	timerTaskFilter := func(task tasks.Task) bool {
		switch task.GetType() {
		case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
			enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
			return true
		default:
			return taskAllocator.verifyStandbyTask(clusterName, namespace.ID(task.GetNamespaceID()), task)
		}
	}

	timerGate := timer.NewRemoteGate()
	timerGate.SetCurrentTime(shard.GetCurrentTime(clusterName))

	config := shard.GetConfig()
	taskExecutor := newTimerQueueStandbyTaskExecutor(
		shard,
		workflowCache,
		workflowDeleteManager,
		xdc.NewNDCHistoryResender(
			shard.GetNamespaceRegistry(),
			clientBean,
			func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
				engine, err := shard.GetEngine(ctx)
				if err != nil {
					return err
				}
				return engine.ReplicateEventsV2(ctx, request)
			},
			shard.GetPayloadSerializer(),
			config.StandbyTaskReReplicationContextTimeout,
			logger,
		),
		matchingClient,
		logger,
		metricProvider,
		clusterName,
		config,
	)

	processor := &timerQueueStandbyProcessorImpl{
		timerGate: timerGate,
	}

	if scheduler == nil {
		scheduler = newTimerTaskScheduler(shard, logger, metricProvider)
		processor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(queues.OperationTimerStandbyQueueProcessor)),
	)

	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerStandbyQueueProcessorScope,
		shard,
		shard.GetQueueClusterAckLevel(tasks.CategoryTimer, clusterName).FireTime,
		timeNow,
		updateShardAckLevel,
		logger,
		clusterName,
		func(t tasks.Task) queues.Executable {
			return queues.NewExecutable(
				t,
				timerTaskFilter,
				taskExecutor,
				scheduler,
				rescheduler,
				shard.GetTimeSource(),
				logger,
				config.TimerTaskMaxRetryCount,
				queues.QueueTypeStandbyTimer,
				config.NamespaceCacheRefreshInterval,
			)
		},
	)

	processor.timerQueueProcessorBase = newTimerQueueProcessorBase(
		metrics.TimerStandbyQueueProcessorScope,
		shard,
		workflowCache,
		processor,
		timerQueueAckMgr,
		timerGate,
		scheduler,
		rescheduler,
		rateLimiter,
		logger,
		metricsClient.Scope(metrics.TimerStandbyQueueProcessorScope),
	)

	return processor
}

func (t *timerQueueStandbyProcessorImpl) Start() {
	if t.ownedScheduler != nil {
		t.ownedScheduler.Start()
	}
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueStandbyProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
	if t.ownedScheduler != nil {
		t.ownedScheduler.Stop()
	}
}

func (t *timerQueueStandbyProcessorImpl) setCurrentTime(
	currentTime time.Time,
) {
	t.timerGate.SetCurrentTime(currentTime)
}

func (t *timerQueueStandbyProcessorImpl) getAckLevel() tasks.Key {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

// NotifyNewTimers - Notify the processor about the new standby timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueStandbyProcessorImpl) notifyNewTimers(
	timerTasks []tasks.Task,
) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}
