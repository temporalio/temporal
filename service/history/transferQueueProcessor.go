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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/archiver"
)

var errUnknownTransferTask = serviceerror.NewInternal("Unknown transfer task")

type (
	taskFilter func(task tasks.Task) bool

	transferQueueProcessorImpl struct {
		singleProcessor           bool
		currentClusterName        string
		shard                     shard.Context
		workflowCache             wcache.Cache
		archivalClient            archiver.Client
		sdkClientFactory          sdk.ClientFactory
		taskAllocator             taskAllocator
		config                    *configs.Config
		metricHandler             metrics.MetricsHandler
		clientBean                client.Bean
		matchingClient            matchingservice.MatchingServiceClient
		historyClient             historyservice.HistoryServiceClient
		ackLevel                  int64
		hostRateLimiter           quotas.RateLimiter
		schedulerRateLimiter      queues.SchedulerRateLimiter
		logger                    log.Logger
		isStarted                 int32
		isStopped                 int32
		shutdownChan              chan struct{}
		scheduler                 queues.Scheduler
		priorityAssigner          queues.PriorityAssigner
		activeTaskProcessor       *transferQueueActiveProcessorImpl
		standbyTaskProcessorsLock sync.RWMutex
		standbyTaskProcessors     map[string]*transferQueueStandbyProcessorImpl
	}
)

func newTransferQueueProcessor(
	shard shard.Context,
	workflowCache wcache.Cache,
	scheduler queues.Scheduler,
	priorityAssigner queues.PriorityAssigner,
	clientBean client.Bean,
	archivalClient archiver.Client,
	sdkClientFactory sdk.ClientFactory,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	metricProvider metrics.MetricsHandler,
	hostRateLimiter quotas.RateLimiter,
	schedulerRateLimiter queues.SchedulerRateLimiter,
) queues.Queue {

	singleProcessor := !shard.GetClusterMetadata().IsGlobalNamespaceEnabled() ||
		shard.GetConfig().TransferProcessorEnableSingleProcessor()

	logger := log.With(shard.GetLogger(), tag.ComponentTransferQueue)
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()
	taskAllocator := newTaskAllocator(shard)

	return &transferQueueProcessorImpl{
		singleProcessor:      singleProcessor,
		currentClusterName:   currentClusterName,
		shard:                shard,
		workflowCache:        workflowCache,
		archivalClient:       archivalClient,
		sdkClientFactory:     sdkClientFactory,
		taskAllocator:        taskAllocator,
		config:               config,
		metricHandler:        metricProvider,
		clientBean:           clientBean,
		matchingClient:       matchingClient,
		historyClient:        historyClient,
		ackLevel:             shard.GetQueueAckLevel(tasks.CategoryTransfer).TaskID,
		hostRateLimiter:      hostRateLimiter,
		schedulerRateLimiter: schedulerRateLimiter,
		logger:               logger,
		shutdownChan:         make(chan struct{}),
		scheduler:            scheduler,
		priorityAssigner:     priorityAssigner,
		activeTaskProcessor: newTransferQueueActiveProcessor(
			shard,
			workflowCache,
			scheduler,
			priorityAssigner,
			archivalClient,
			sdkClientFactory,
			matchingClient,
			historyClient,
			taskAllocator,
			clientBean,
			newQueueProcessorRateLimiter(
				hostRateLimiter,
				config.TransferProcessorMaxPollRPS,
			),
			schedulerRateLimiter,
			logger,
			metricProvider,
			singleProcessor,
		),
		standbyTaskProcessors: make(map[string]*transferQueueStandbyProcessorImpl),
	}
}

func (t *transferQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	t.activeTaskProcessor.Start()
	if !t.singleProcessor {
		t.listenToClusterMetadataChange()
	}

	go t.completeTransferLoop()
}

func (t *transferQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.activeTaskProcessor.Stop()
	if !t.singleProcessor {
		t.shard.GetClusterMetadata().UnRegisterMetadataChangeCallback(t)
		t.standbyTaskProcessorsLock.RLock()
		for _, standbyTaskProcessor := range t.standbyTaskProcessors {
			standbyTaskProcessor.Stop()
		}
		t.standbyTaskProcessorsLock.RUnlock()
	}
	close(t.shutdownChan)
}

// NotifyNewTasks - Notify the processor about the new active / standby transfer task arrival.
// This should be called each time new transfer task arrives, otherwise tasks maybe delayed.
func (t *transferQueueProcessorImpl) NotifyNewTasks(
	clusterName string,
	transferTasks []tasks.Task,
) {
	if clusterName == t.currentClusterName || t.singleProcessor {
		// we will ignore the current time passed in, since the active processor process task immediately
		if len(transferTasks) != 0 {
			t.activeTaskProcessor.notifyNewTask()
		}
		return
	}

	t.standbyTaskProcessorsLock.RLock()
	standbyTaskProcessor, ok := t.standbyTaskProcessors[clusterName]
	t.standbyTaskProcessorsLock.RUnlock()
	if !ok {
		t.logger.Warn(fmt.Sprintf("Cannot find transfer processor for %s.", clusterName))
		return
	}
	if len(transferTasks) != 0 {
		standbyTaskProcessor.notifyNewTask()
	}
}

func (t *transferQueueProcessorImpl) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {
	if t.singleProcessor {
		// TODO: we may want to reschedule all tasks for new active namespaces in buffer
		// so that they don't have to keeping waiting on the backoff timer
		return
	}

	minLevel := t.shard.GetQueueClusterAckLevel(tasks.CategoryTransfer, t.currentClusterName).TaskID
	standbyClusterName := t.currentClusterName
	for clusterName, info := range t.shard.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}
		ackLevel := t.shard.GetQueueClusterAckLevel(tasks.CategoryTransfer, clusterName).TaskID
		if ackLevel < minLevel {
			minLevel = ackLevel
			standbyClusterName = clusterName
		}
	}

	// the ack manager is exclusive, so add 1
	maxLevel := t.activeTaskProcessor.getQueueReadLevel() + 1
	t.logger.Info("Transfer Failover Triggered",
		tag.WorkflowNamespaceIDs(namespaceIDs),
		tag.MinLevel(minLevel),
		tag.MaxLevel(maxLevel))
	updateShardAckLevel, failoverTaskProcessor := newTransferQueueFailoverProcessor(
		t.shard,
		t.workflowCache,
		t.scheduler,
		t.priorityAssigner,
		t.archivalClient,
		t.sdkClientFactory,
		t.matchingClient,
		t.historyClient,
		namespaceIDs,
		standbyClusterName,
		minLevel,
		maxLevel,
		t.taskAllocator,
		newQueueProcessorRateLimiter(
			t.hostRateLimiter,
			t.config.TransferProcessorFailoverMaxPollRPS,
		),
		t.schedulerRateLimiter,
		t.logger,
		t.metricHandler,
	)

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerNamespaceFailoverCallback function
	err := updateShardAckLevel(minLevel)
	if err != nil {
		t.logger.Error("Error update shard ack level", tag.Error(err))
	}
	failoverTaskProcessor.Start()
}

func (t *transferQueueProcessorImpl) LockTaskProcessing() {
	if t.singleProcessor {
		return
	}

	t.taskAllocator.lock()
}

func (t *transferQueueProcessorImpl) UnlockTaskProcessing() {
	if t.singleProcessor {
		return
	}

	t.taskAllocator.unlock()
}

func (t *transferQueueProcessorImpl) Category() tasks.Category {
	return tasks.CategoryTransfer
}

func (t *transferQueueProcessorImpl) completeTransferLoop() {
	timer := time.NewTimer(t.config.TransferProcessorCompleteTransferInterval())
	defer timer.Stop()

	completeTaskRetryPolicy := common.CreateCompleteTaskRetryPolicy()

	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			if err := t.completeTransfer(); err != nil {
				t.logger.Error("Failed to complete transfer task", tag.Error(err))
			}
			return
		case <-timer.C:
			// TODO: We should have a better approach to handle shard and its component lifecycle
			_ = backoff.ThrottleRetry(func() error {
				err := t.completeTransfer()
				if err != nil {
					t.logger.Info("Failed to complete transfer task", tag.Error(err))
				}
				return err
			}, completeTaskRetryPolicy, func(err error) bool {
				select {
				case <-t.shutdownChan:
					return false
				default:
				}
				return !shard.IsShardOwnershipLostError(err)
			})

			timer.Reset(t.config.TransferProcessorCompleteTransferInterval())
		}
	}
}

func (t *transferQueueProcessorImpl) completeTransfer() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.activeTaskProcessor.queueAckMgr.getQueueAckLevel()

	if !t.singleProcessor {
		t.standbyTaskProcessorsLock.RLock()
		for _, standbyTaskProcessor := range t.standbyTaskProcessors {
			ackLevel := standbyTaskProcessor.queueAckMgr.getQueueAckLevel()
			if upperAckLevel > ackLevel {
				upperAckLevel = ackLevel
			}
		}
		t.standbyTaskProcessorsLock.RUnlock()

		for _, failoverInfo := range t.shard.GetAllFailoverLevels(tasks.CategoryTransfer) {
			if upperAckLevel > failoverInfo.MinLevel.TaskID {
				upperAckLevel = failoverInfo.MinLevel.TaskID
			}
		}
	}

	t.logger.Debug("Start completing transfer task", tag.AckLevel(lowerAckLevel), tag.AckLevel(upperAckLevel))
	if lowerAckLevel >= upperAckLevel {
		return nil
	}

	t.metricHandler.Counter(metrics.TaskBatchCompleteCounter.GetMetricName()).Record(
		1,
		metrics.OperationTag(metrics.TransferQueueProcessorScope))

	if lowerAckLevel < upperAckLevel {
		ctx, cancel := newQueueIOContext()
		defer cancel()

		err := t.shard.GetExecutionManager().RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             t.shard.GetShardID(),
			TaskCategory:        tasks.CategoryTransfer,
			InclusiveMinTaskKey: tasks.NewImmediateKey(lowerAckLevel + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(upperAckLevel + 1),
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateQueueAckLevel(tasks.CategoryTransfer, tasks.NewImmediateKey(upperAckLevel))
}

func (t *transferQueueProcessorImpl) listenToClusterMetadataChange() {
	t.shard.GetClusterMetadata().RegisterMetadataChangeCallback(
		t,
		t.handleClusterMetadataUpdate,
	)
}

func (t *transferQueueProcessorImpl) handleClusterMetadataUpdate(
	oldClusterMetadata map[string]*cluster.ClusterInformation,
	newClusterMetadata map[string]*cluster.ClusterInformation,
) {
	t.standbyTaskProcessorsLock.Lock()
	defer t.standbyTaskProcessorsLock.Unlock()
	for clusterName := range oldClusterMetadata {
		if clusterName == t.currentClusterName {
			continue
		}
		// The metadata triggers a update when the following fields update: 1. Enabled 2. Initial Failover Version 3. Cluster address
		// The callback covers three cases:
		// Case 1: Remove a cluster Case 2: Add a new cluster Case 3: Refresh cluster metadata.
		if processor, ok := t.standbyTaskProcessors[clusterName]; ok {
			// Case 1 and Case 3
			processor.Stop()
			delete(t.standbyTaskProcessors, clusterName)
		}
		if clusterInfo := newClusterMetadata[clusterName]; clusterInfo != nil && clusterInfo.Enabled {
			// Case 2 and Case 3
			processor := newTransferQueueStandbyProcessor(
				clusterName,
				t.shard,
				t.scheduler,
				t.priorityAssigner,
				t.workflowCache,
				t.archivalClient,
				t.taskAllocator,
				t.clientBean,
				newQueueProcessorRateLimiter(
					t.hostRateLimiter,
					t.config.TransferProcessorMaxPollRPS,
				),
				t.schedulerRateLimiter,
				t.logger,
				t.metricHandler,
				t.matchingClient,
			)
			processor.Start()
			t.standbyTaskProcessors[clusterName] = processor
		}
	}
}
