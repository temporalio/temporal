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

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	transferQueueStandbyProcessorImpl struct {
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		// this is the scheduler owned by this standby queue processor
		ownedScheduler queues.Scheduler
	}
)

func newTransferQueueStandbyProcessor(
	clusterName string,
	shard shard.Context,
	scheduler queues.Scheduler,
	priorityAssigner queues.PriorityAssigner,
	workflowCache wcache.Cache,
	archivalClient archiver.Client,
	taskAllocator taskAllocator,
	clientBean client.Bean,
	rateLimiter quotas.RateLimiter,
	schedulerRateLimiter queues.SchedulerRateLimiter,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
	matchingClient matchingservice.MatchingServiceClient,
) *transferQueueStandbyProcessorImpl {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                          config.TransferTaskBatchSize,
		MaxPollInterval:                    config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxReschdulerSize:                  config.TransferProcessorMaxReschedulerSize,
		PollBackoffInterval:                config.TransferProcessorPollBackoffInterval,
		Operation:                          metrics.TransferStandbyQueueProcessorScope,
	}
	logger = log.With(logger, tag.ClusterName(clusterName))

	transferTaskFilter := func(task tasks.Task) bool {
		switch task.GetType() {
		case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
			// no reset needed for standby
			return false
		case enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION:
			return true
		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
			if shard.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival() {
				return true
			}
			fallthrough
		default:
			return taskAllocator.verifyStandbyTask(clusterName, namespace.ID(task.GetNamespaceID()), task)
		}
	}
	maxReadLevel := func() int64 {
		// we are creating standby processor, so we know we are not in single processor mode
		return shard.GetImmediateQueueExclusiveHighReadWatermark().TaskID
	}
	updateClusterAckLevel := func(ackLevel int64) error {
		return shard.UpdateQueueClusterAckLevel(tasks.CategoryTransfer, clusterName, tasks.NewImmediateKey(ackLevel))
	}
	transferQueueShutdown := func() error {
		return nil
	}

	processor := &transferQueueStandbyProcessorImpl{
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			maxReadLevel,
			updateClusterAckLevel,
			transferQueueShutdown,
			logger,
		),
	}

	taskExecutor := newTransferQueueStandbyTaskExecutor(
		shard,
		workflowCache,
		archivalClient,
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
		logger,
		metricProvider,
		clusterName,
		matchingClient,
	)

	if scheduler == nil {
		scheduler = newTransferTaskShardScheduler(shard, schedulerRateLimiter, logger)
		processor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(metrics.OperationTransferStandbyQueueProcessorScope)),
	)

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		processor,
		shard.GetQueueClusterAckLevel(tasks.CategoryTransfer, clusterName).TaskID,
		logger,
		func(t tasks.Task) queues.Executable {
			return queues.NewExecutable(
				queues.DefaultReaderId,
				t,
				transferTaskFilter,
				taskExecutor,
				scheduler,
				rescheduler,
				priorityAssigner,
				shard.GetTimeSource(),
				shard.GetNamespaceRegistry(),
				logger,
				metricProvider,
				shard.GetConfig().TransferTaskMaxRetryCount,
				shard.GetConfig().NamespaceCacheRefreshInterval,
			)
		},
	)

	queueProcessorBase := newQueueProcessorBase(
		clusterName,
		shard,
		options,
		processor,
		queueAckMgr,
		workflowCache,
		scheduler,
		rescheduler,
		rateLimiter,
		logger,
	)

	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *transferQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueStandbyProcessorImpl) Start() {
	if t.ownedScheduler != nil {
		t.ownedScheduler.Start()
	}
	t.queueProcessorBase.Start()
}

func (t *transferQueueStandbyProcessorImpl) Stop() {
	t.queueProcessorBase.Stop()
	if t.ownedScheduler != nil {
		t.ownedScheduler.Stop()
	}
}
