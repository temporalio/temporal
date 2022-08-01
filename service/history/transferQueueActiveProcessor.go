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
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	transferQueueActiveProcessorImpl struct {
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		// this is the scheduler owned by this active queue processor
		ownedScheduler queues.Scheduler
	}
)

func newTransferQueueActiveProcessor(
	shard shard.Context,
	workflowCache workflow.Cache,
	scheduler queues.Scheduler,
	archivalClient archiver.Client,
	sdkClientFactory sdk.ClientFactory,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	taskAllocator taskAllocator,
	clientBean client.Bean,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
	singleProcessor bool,
) *transferQueueActiveProcessorImpl {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                          config.TransferTaskBatchSize,
		MaxPollInterval:                    config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxReschdulerSize:                  config.TransferProcessorMaxReschedulerSize,
		PollBackoffInterval:                config.TransferProcessorPollBackoffInterval,
		MetricScope:                        metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	logger = log.With(logger, tag.ClusterName(currentClusterName))

	maxReadLevel := func() int64 {
		return shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTransfer, currentClusterName).TaskID
	}
	updateTransferAckLevel := func(ackLevel int64) error {
		// in single cursor mode, continue to update cluster ack level
		// complete task loop will update overall ack level and
		// shard.UpdateQueueAcklevel will then forward it to standby cluster ack level entries
		// so that we can later disable single cursor mode without encountering tombstone issues
		return shard.UpdateQueueClusterAckLevel(
			tasks.CategoryTransfer,
			currentClusterName,
			tasks.NewImmediateKey(ackLevel),
		)
	}

	transferQueueShutdown := func() error {
		return nil
	}

	processor := &transferQueueActiveProcessorImpl{
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			maxReadLevel,
			updateTransferAckLevel,
			transferQueueShutdown,
			logger,
		),
	}

	if scheduler == nil {
		scheduler = newTransferTaskScheduler(shard, logger, metricProvider)
		processor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(queues.OperationTransferActiveQueueProcessor)),
	)

	transferTaskFilter := func(task tasks.Task) bool {
		return taskAllocator.verifyActiveTask(namespace.ID(task.GetNamespaceID()), task)
	}
	taskExecutor := newTransferQueueActiveTaskExecutor(
		shard,
		workflowCache,
		archivalClient,
		sdkClientFactory,
		logger,
		metricProvider,
		config,
		matchingClient,
	)
	ackLevel := shard.GetQueueClusterAckLevel(tasks.CategoryTransfer, currentClusterName).TaskID
	queueType := queues.QueueTypeActiveTransfer

	// if single cursor is enabled, then this processor is responsible for both active and standby tasks
	// and we need to customize some parameters for ack manager and task executable
	if singleProcessor {
		transferTaskFilter = func(task tasks.Task) bool { return true }
		taskExecutor = queues.NewExecutorWrapper(
			currentClusterName,
			shard.GetNamespaceRegistry(),
			taskExecutor,
			newTransferQueueStandbyTaskExecutor(
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
				// note: the cluster name is for calculating time for standby tasks,
				// here we are basically using current cluster time
				// this field will be deprecated soon, currently exists so that
				// we have the option of revert to old behavior
				currentClusterName,
				matchingClient,
			),
			logger,
		)

		ackLevel = shard.GetQueueAckLevel(tasks.CategoryTransfer).TaskID
		queueType = queues.QueueTypeTransfer
	}

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		processor,
		ackLevel,
		logger,
		func(t tasks.Task) queues.Executable {
			return queues.NewExecutable(
				t,
				transferTaskFilter,
				taskExecutor,
				scheduler,
				rescheduler,
				shard.GetTimeSource(),
				logger,
				config.TransferTaskMaxRetryCount,
				queueType,
				shard.GetConfig().NamespaceCacheRefreshInterval,
			)
		},
	)

	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		queueAckMgr,
		workflowCache,
		scheduler,
		rescheduler,
		rateLimiter,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferActiveQueueProcessorScope),
	)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func newTransferQueueFailoverProcessor(
	shard shard.Context,
	workflowCache workflow.Cache,
	scheduler queues.Scheduler,
	archivalClient archiver.Client,
	sdkClientFactory sdk.ClientFactory,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	namespaceIDs map[string]struct{},
	standbyClusterName string,
	minLevel int64,
	maxLevel int64,
	taskAllocator taskAllocator,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
) (func(ackLevel int64) error, *transferQueueActiveProcessorImpl) {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                          config.TransferTaskBatchSize,
		MaxPollInterval:                    config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxReschdulerSize:                  config.TransferProcessorMaxReschedulerSize,
		PollBackoffInterval:                config.TransferProcessorPollBackoffInterval,
		MetricScope:                        metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	failoverUUID := uuid.New()
	logger = log.With(
		logger,
		tag.ClusterName(currentClusterName),
		tag.WorkflowNamespaceIDs(namespaceIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	transferTaskFilter := func(task tasks.Task) bool {
		return taskAllocator.verifyFailoverActiveTask(namespaceIDs, namespace.ID(task.GetNamespaceID()), task)
	}
	maxReadAckLevel := func() int64 {
		return maxLevel // this is a const
	}
	failoverStartTime := shard.GetTimeSource().Now()
	updateTransferAckLevel := func(ackLevel int64) error {
		return shard.UpdateFailoverLevel(
			tasks.CategoryTransfer,
			failoverUUID,
			persistence.FailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     tasks.NewImmediateKey(minLevel),
				CurrentLevel: tasks.NewImmediateKey(ackLevel),
				MaxLevel:     tasks.NewImmediateKey(maxLevel),
				NamespaceIDs: namespaceIDs,
			},
		)
	}
	transferQueueShutdown := func() error {
		return shard.DeleteFailoverLevel(tasks.CategoryTransfer, failoverUUID)
	}

	processor := &transferQueueActiveProcessorImpl{
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			maxReadAckLevel,
			updateTransferAckLevel,
			transferQueueShutdown,
			logger,
		),
	}

	taskExecutor := newTransferQueueActiveTaskExecutor(
		shard,
		workflowCache,
		archivalClient,
		sdkClientFactory,
		logger,
		metricProvider,
		config,
		matchingClient,
	)

	if scheduler == nil {
		scheduler = newTransferTaskScheduler(shard, logger, metricProvider)
		processor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(queues.OperationTransferActiveQueueProcessor)),
	)

	queueAckMgr := newQueueFailoverAckMgr(
		shard,
		options,
		processor,
		minLevel,
		logger,
		func(t tasks.Task) queues.Executable {
			return queues.NewExecutable(
				t,
				transferTaskFilter,
				taskExecutor,
				scheduler,
				rescheduler,
				shard.GetTimeSource(),
				logger,
				shard.GetConfig().TransferTaskMaxRetryCount,
				queues.QueueTypeActiveTransfer,
				shard.GetConfig().NamespaceCacheRefreshInterval,
			)
		},
	)

	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		queueAckMgr,
		workflowCache,
		scheduler,
		rescheduler,
		rateLimiter,
		logger,
		shard.GetMetricsClient().Scope(metrics.TransferActiveQueueProcessorScope),
	)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase
	return updateTransferAckLevel, processor
}

func (t *transferQueueActiveProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueActiveProcessorImpl) Start() {
	if t.ownedScheduler != nil {
		t.ownedScheduler.Start()
	}
	t.queueProcessorBase.Start()
}

func (t *transferQueueActiveProcessorImpl) Stop() {
	t.queueProcessorBase.Stop()
	if t.ownedScheduler != nil {
		t.ownedScheduler.Stop()
	}
}
