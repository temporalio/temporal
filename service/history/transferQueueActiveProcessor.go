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
	"github.com/pborman/uuid"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/sdk"
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
	logger log.Logger,
) *transferQueueActiveProcessorImpl {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                           config.TransferTaskBatchSize,
		MaxPollRPS:                          config.TransferProcessorMaxPollRPS,
		MaxPollInterval:                     config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		RescheduleInterval:                  config.TransferProcessorRescheduleInterval,
		RescheduleIntervalJitterCoefficient: config.TransferProcessorRescheduleIntervalJitterCoefficient,
		MaxReschdulerSize:                   config.TransferProcessorMaxReschedulerSize,
		PollBackoffInterval:                 config.TransferProcessorPollBackoffInterval,
		MetricScope:                         metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	logger = log.With(logger, tag.ClusterName(currentClusterName))
	transferTaskFilter := func(task tasks.Task) bool {
		return taskAllocator.verifyActiveTask(namespace.ID(task.GetNamespaceID()), task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetQueueMaxReadLevel(tasks.CategoryTransfer, currentClusterName).TaskID
	}
	updateTransferAckLevel := func(ackLevel int64) error {
		return shard.UpdateQueueClusterAckLevel(
			tasks.CategoryTransfer,
			currentClusterName,
			tasks.Key{TaskID: ackLevel},
		)
	}

	transferQueueShutdown := func() error {
		return nil
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
		config,
		matchingClient,
	)

	if scheduler == nil {
		scheduler = newTransferTaskScheduler(shard, logger)
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		shard.GetMetricsClient().Scope(metrics.TimerActiveQueueProcessorScope),
	)

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		processor,
		shard.GetQueueClusterAckLevel(tasks.CategoryTransfer, currentClusterName).TaskID,
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
				shard.GetMetricsClient().Scope(
					tasks.GetActiveTransferTaskMetricsScope(t),
				),
				config.TransferTaskMaxRetryCount,
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
	archivalClient archiver.Client,
	sdkClientFactory sdk.ClientFactory,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	namespaceIDs map[string]struct{},
	standbyClusterName string,
	minLevel int64,
	maxLevel int64,
	taskAllocator taskAllocator,
	logger log.Logger,
) (func(ackLevel int64) error, *transferQueueActiveProcessorImpl) {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                           config.TransferTaskBatchSize,
		MaxPollRPS:                          config.TransferProcessorFailoverMaxPollRPS,
		MaxPollInterval:                     config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		RescheduleInterval:                  config.TransferProcessorRescheduleInterval,
		RescheduleIntervalJitterCoefficient: config.TransferProcessorRescheduleIntervalJitterCoefficient,
		MaxReschdulerSize:                   config.TransferProcessorMaxReschedulerSize,
		PollBackoffInterval:                 config.TransferProcessorPollBackoffInterval,
		MetricScope:                         metrics.TransferActiveQueueProcessorScope,
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
				MinLevel:     tasks.Key{TaskID: minLevel},
				CurrentLevel: tasks.Key{TaskID: ackLevel},
				MaxLevel:     tasks.Key{TaskID: maxLevel},
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
		config,
		matchingClient,
	)

	scheduler := newTransferTaskScheduler(shard, logger)

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		shard.GetMetricsClient().Scope(metrics.TimerActiveQueueProcessorScope),
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
				shard.GetMetricsClient().Scope(
					tasks.GetActiveTransferTaskMetricsScope(t),
				),
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
