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

	"github.com/pborman/uuid"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueActiveProcessorImpl struct {
		timerQueueProcessorBase *timerQueueProcessorBase

		// this is the scheduler owned by this active queue processor
		ownedScheduler queues.Scheduler
	}
)

func newTimerQueueActiveProcessor(
	shard shard.Context,
	workflowCache workflow.Cache,
	scheduler queues.Scheduler,
	workflowDeleteManager workflow.DeleteManager,
	matchingClient matchingservice.MatchingServiceClient,
	taskAllocator taskAllocator,
	clientBean client.Bean,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
	singleProcessor bool,
) *timerQueueActiveProcessorImpl {
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		return shard.GetCurrentTime(currentClusterName)
	}
	updateShardAckLevel := func(ackLevel tasks.Key) error {
		// in single cursor mode, continue to update cluster ack level
		// complete task loop will update overall ack level and
		// shard.UpdateQueueAcklevel will then forward it to standby cluster ack level entries
		// so that we can later disable single cursor mode without encountering tombstone issues
		return shard.UpdateQueueClusterAckLevel(
			tasks.CategoryTimer,
			currentClusterName,
			ackLevel,
		)
	}
	logger = log.With(logger, tag.ClusterName(currentClusterName))
	metricsClient := shard.GetMetricsClient()
	config := shard.GetConfig()

	processor := &timerQueueActiveProcessorImpl{}

	if scheduler == nil {
		scheduler = newTimerTaskScheduler(shard, logger, metricProvider)
		processor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(queues.OperationTimerActiveQueueProcessor)),
	)

	timerTaskFilter := func(task tasks.Task) bool {
		return taskAllocator.verifyActiveTask(namespace.ID(task.GetNamespaceID()), task)
	}
	taskExecutor := newTimerQueueActiveTaskExecutor(
		shard,
		workflowCache,
		workflowDeleteManager,
		processor,
		logger,
		metricProvider,
		config,
		matchingClient,
	)
	ackLevel := shard.GetQueueClusterAckLevel(tasks.CategoryTimer, currentClusterName).FireTime
	queueType := queues.QueueTypeActiveTimer

	// if single cursor is enabled, then this processor is responsible for both active and standby tasks
	// and we need to customize some parameters for ack manager and task executable
	if singleProcessor {
		timerTaskFilter = func(task tasks.Task) bool { return true }
		taskExecutor = queues.NewExecutorWrapper(
			currentClusterName,
			shard.GetNamespaceRegistry(),
			taskExecutor,
			newTimerQueueStandbyTaskExecutor(
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
				// note: the cluster name is for calculating time for standby tasks,
				// here we are basically using current cluster time
				// this field will be deprecated soon, currently exists so that
				// we have the option of revert to old behavior
				currentClusterName,
				config,
			),
			logger,
		)
		ackLevel = shard.GetQueueAckLevel(tasks.CategoryTimer).FireTime
		queueType = queues.QueueTypeTimer
	}

	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerActiveQueueProcessorScope,
		shard,
		ackLevel,
		timeNow,
		updateShardAckLevel,
		logger,
		currentClusterName,
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
				queueType,
				config.NamespaceCacheRefreshInterval,
			)
		},
	)

	processor.timerQueueProcessorBase = newTimerQueueProcessorBase(
		metrics.TimerActiveQueueProcessorScope,
		shard,
		workflowCache,
		processor,
		timerQueueAckMgr,
		timer.NewLocalGate(shard.GetTimeSource()),
		scheduler,
		rescheduler,
		rateLimiter,
		logger,
		metricsClient.Scope(metrics.TimerActiveQueueProcessorScope),
	)

	return processor
}

func newTimerQueueFailoverProcessor(
	shard shard.Context,
	workflowCache workflow.Cache,
	scheduler queues.Scheduler,
	workflowDeleteManager workflow.DeleteManager,
	namespaceIDs map[string]struct{},
	standbyClusterName string,
	minLevel time.Time,
	maxLevel time.Time,
	matchingClient matchingservice.MatchingServiceClient,
	taskAllocator taskAllocator,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
) (func(ackLevel tasks.Key) error, *timerQueueActiveProcessorImpl) {
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		// should use current cluster's time when doing namespace failover
		return shard.GetCurrentTime(currentClusterName)
	}
	failoverStartTime := shard.GetTimeSource().Now()
	failoverUUID := uuid.New()

	updateShardAckLevel := func(ackLevel tasks.Key) error {
		return shard.UpdateFailoverLevel(
			tasks.CategoryTimer,
			failoverUUID,
			persistence.FailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     tasks.NewKey(minLevel, 0),
				CurrentLevel: ackLevel,
				MaxLevel:     tasks.NewKey(maxLevel, 0),
				NamespaceIDs: namespaceIDs,
			},
		)
	}
	timerAckMgrShutdown := func() error {
		return shard.DeleteFailoverLevel(tasks.CategoryTimer, failoverUUID)
	}

	logger = log.With(
		logger,
		tag.ClusterName(currentClusterName),
		tag.WorkflowNamespaceIDs(namespaceIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)
	timerTaskFilter := func(task tasks.Task) bool {
		return taskAllocator.verifyFailoverActiveTask(namespaceIDs, namespace.ID(task.GetNamespaceID()), task)
	}

	processor := &timerQueueActiveProcessorImpl{}

	taskExecutor := newTimerQueueActiveTaskExecutor(
		shard,
		workflowCache,
		workflowDeleteManager,
		processor,
		logger,
		metricProvider,
		shard.GetConfig(),
		matchingClient,
	)

	if scheduler == nil {
		scheduler = newTimerTaskScheduler(shard, logger, metricProvider)
		processor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(queues.OperationTimerActiveQueueProcessor)),
	)

	timerQueueAckMgr := newTimerQueueFailoverAckMgr(
		shard,
		minLevel,
		maxLevel,
		timeNow,
		updateShardAckLevel,
		timerAckMgrShutdown,
		logger,
		func(t tasks.Task) queues.Executable {
			return queues.NewExecutable(
				t,
				timerTaskFilter,
				taskExecutor,
				scheduler,
				rescheduler,
				shard.GetTimeSource(),
				logger,
				shard.GetConfig().TimerTaskMaxRetryCount,
				queues.QueueTypeActiveTimer,
				shard.GetConfig().NamespaceCacheRefreshInterval,
			)
		},
	)

	processor.timerQueueProcessorBase = newTimerQueueProcessorBase(
		metrics.TimerActiveQueueProcessorScope,
		shard,
		workflowCache,
		processor,
		timerQueueAckMgr,
		timer.NewLocalGate(shard.GetTimeSource()),
		scheduler,
		rescheduler,
		rateLimiter,
		logger,
		shard.GetMetricsClient().Scope(metrics.TimerActiveQueueProcessorScope),
	)

	return updateShardAckLevel, processor
}

func (t *timerQueueActiveProcessorImpl) Start() {
	if t.ownedScheduler != nil {
		t.ownedScheduler.Start()
	}
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueActiveProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
	if t.ownedScheduler != nil {
		t.ownedScheduler.Stop()
	}
}

func (t *timerQueueActiveProcessorImpl) getAckLevel() tasks.Key {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

func (t *timerQueueActiveProcessorImpl) getReadLevel() tasks.Key {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getReadLevel()
}

// NotifyNewTimers - Notify the processor about the new active timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueActiveProcessorImpl) notifyNewTimers(
	timerTasks []tasks.Task,
) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}
