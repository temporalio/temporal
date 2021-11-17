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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination transferQueueProcessor_mock.go

package history

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/cluster"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

var (
	errUnknownTransferTask = serviceerror.NewInternal("Unknown transfer task")
	transferQueueKey       = "transferQueue"
)

type (
	transferQueueProcessor interface {
		common.Daemon
		FailoverNamespace(namespaceIDs map[string]struct{})
		NotifyNewTask(clusterName string, transferTasks []tasks.Task)
		LockTaskProcessing()
		UnlockTaskProcessing()
	}

	taskFilter func(task tasks.Task) (bool, error)

	transferQueueProcessorImpl struct {
		isGlobalNamespaceEnabled  bool
		currentClusterName        string
		shard                     shard.Context
		taskAllocator             taskAllocator
		config                    *configs.Config
		metricsClient             metrics.Client
		historyService            *historyEngineImpl
		matchingClient            matchingservice.MatchingServiceClient
		historyClient             historyservice.HistoryServiceClient
		ackLevel                  int64
		logger                    log.Logger
		isStarted                 int32
		isStopped                 int32
		shutdownChan              chan struct{}
		activeTaskProcessor       *transferQueueActiveProcessorImpl
		standbyTaskProcessorsLock sync.Mutex
		standbyTaskProcessors     map[string]*transferQueueStandbyProcessorImpl
	}
)

func newTransferQueueProcessor(
	shard shard.Context,
	historyService *historyEngineImpl,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	logger log.Logger,
) *transferQueueProcessorImpl {

	logger = log.With(logger, tag.ComponentTransferQueue)
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()
	taskAllocator := newTaskAllocator(shard)

	return &transferQueueProcessorImpl{
		isGlobalNamespaceEnabled: shard.GetService().GetClusterMetadata().IsGlobalNamespaceEnabled(),
		currentClusterName:       currentClusterName,
		shard:                    shard,
		taskAllocator:            taskAllocator,
		config:                   config,
		metricsClient:            historyService.metricsClient,
		historyService:           historyService,
		matchingClient:           matchingClient,
		historyClient:            historyClient,
		ackLevel:                 shard.GetTransferAckLevel(),
		logger:                   logger,
		shutdownChan:             make(chan struct{}),
		activeTaskProcessor: newTransferQueueActiveProcessor(
			shard,
			historyService,
			matchingClient,
			historyClient,
			taskAllocator,
			logger,
		),
		standbyTaskProcessors: make(map[string]*transferQueueStandbyProcessorImpl),
	}
}

func (t *transferQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	t.activeTaskProcessor.Start()
	if t.isGlobalNamespaceEnabled {
		t.watchClusterMetadataChange()
	}

	go t.completeTransferLoop()
}

func (t *transferQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.activeTaskProcessor.Stop()
	if t.isGlobalNamespaceEnabled {
		t.shard.GetClusterMetadata().UnRegisterMetadataChangeCallback(fmt.Sprintf("%s-%v", transferQueueKey, t.shard.GetShardID()))
		t.standbyTaskProcessorsLock.Lock()
		for _, standbyTaskProcessor := range t.standbyTaskProcessors {
			standbyTaskProcessor.Stop()
		}
		t.standbyTaskProcessorsLock.Unlock()
	}
	close(t.shutdownChan)
}

// NotifyNewTask - Notify the processor about the new active / standby transfer task arrival.
// This should be called each time new transfer task arrives, otherwise tasks maybe delayed.
func (t *transferQueueProcessorImpl) NotifyNewTask(
	clusterName string,
	transferTasks []tasks.Task,
) {

	if clusterName == t.currentClusterName {
		// we will ignore the current time passed in, since the active processor process task immediately
		if len(transferTasks) != 0 {
			t.activeTaskProcessor.notifyNewTask()
		}
		return
	}
	t.standbyTaskProcessorsLock.Lock()
	standbyTaskProcessor, ok := t.standbyTaskProcessors[clusterName]
	t.standbyTaskProcessorsLock.Unlock()
	if !ok {
		panic(fmt.Sprintf("Cannot find transfer processor for %s.", clusterName))
	}
	if len(transferTasks) != 0 {
		standbyTaskProcessor.notifyNewTask()
	}
	standbyTaskProcessor.retryTasks()
}

func (t *transferQueueProcessorImpl) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {

	minLevel := t.shard.GetTransferClusterAckLevel(t.currentClusterName)
	standbyClusterName := t.currentClusterName
	for clusterName, info := range t.shard.GetService().GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}
		ackLevel := t.shard.GetTransferClusterAckLevel(clusterName)
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
		t.historyService,
		t.matchingClient,
		t.historyClient,
		namespaceIDs,
		standbyClusterName,
		minLevel,
		maxLevel,
		t.taskAllocator,
		t.logger,
	)
	t.standbyTaskProcessorsLock.Lock()
	for _, standbyTaskProcessor := range t.standbyTaskProcessors {
		standbyTaskProcessor.retryTasks()
	}
	t.standbyTaskProcessorsLock.Unlock()

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerNamespaceFailoverCallback function
	err := updateShardAckLevel(minLevel)
	if err != nil {
		t.logger.Error("Error update shard ack level", tag.Error(err))
	}
	failoverTaskProcessor.Start()
}

func (t *transferQueueProcessorImpl) LockTaskProcessing() {
	t.taskAllocator.lock()
}

func (t *transferQueueProcessorImpl) UnlockTaskProcessing() {
	t.taskAllocator.unlock()
}

func (t *transferQueueProcessorImpl) completeTransferLoop() {
	timer := time.NewTimer(t.config.TransferProcessorCompleteTransferInterval())
	defer timer.Stop()

	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			err := t.completeTransfer()
			if err != nil {
				t.logger.Error("Error complete transfer task", tag.Error(err))
			}
			return
		case <-timer.C:
		CompleteLoop:
			for attempt := 1; attempt <= t.config.TransferProcessorCompleteTransferFailureRetryCount(); attempt++ {
				err := t.completeTransfer()
				if err != nil {
					t.logger.Info("Failed to complete transfer task", tag.Error(err))
					if err == shard.ErrShardClosed {
						// shard closed, trigger shutdown and bail out
						t.Stop()
						return
					}
					backoff := time.Duration((attempt - 1) * 100)
					time.Sleep(backoff * time.Millisecond)
				} else {
					break CompleteLoop
				}
			}
			timer.Reset(t.config.TransferProcessorCompleteTransferInterval())
		}
	}
}

func (t *transferQueueProcessorImpl) completeTransfer() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.activeTaskProcessor.queueAckMgr.getQueueAckLevel()

	if t.isGlobalNamespaceEnabled {
		for _, standbyTaskProcessor := range t.standbyTaskProcessors {
			ackLevel := standbyTaskProcessor.queueAckMgr.getQueueAckLevel()
			if upperAckLevel > ackLevel {
				upperAckLevel = ackLevel
			}
		}

		for _, failoverInfo := range t.shard.GetAllTransferFailoverLevels() {
			if upperAckLevel > failoverInfo.MinLevel {
				upperAckLevel = failoverInfo.MinLevel
			}
		}
	}

	t.logger.Debug("Start completing transfer task", tag.AckLevel(lowerAckLevel), tag.AckLevel(upperAckLevel))
	if lowerAckLevel >= upperAckLevel {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if lowerAckLevel < upperAckLevel {
		err := t.shard.GetExecutionManager().RangeCompleteTransferTask(&persistence.RangeCompleteTransferTaskRequest{
			ShardID:              t.shard.GetShardID(),
			ExclusiveBeginTaskID: lowerAckLevel,
			InclusiveEndTaskID:   upperAckLevel,
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateTransferAckLevel(upperAckLevel)
}

func (t *transferQueueProcessorImpl) watchClusterMetadataChange() {
	t.shard.GetClusterMetadata().RegisterMetadataChangeCallback(
		fmt.Sprintf("%s-%v", transferQueueKey, t.shard.GetShardID()),
		func(oldClusterMetadata map[string]*cluster.ClusterInformation, newClusterMetadata map[string]*cluster.ClusterInformation) {
			t.standbyTaskProcessorsLock.Lock()
			defer t.standbyTaskProcessorsLock.Unlock()
			for clusterName := range oldClusterMetadata {
				if clusterName == t.currentClusterName {
					continue
				}
				if processor, ok := t.standbyTaskProcessors[clusterName]; ok {
					processor.Stop()
					delete(t.standbyTaskProcessors, clusterName)
				}
				if clusterInfo := newClusterMetadata[clusterName]; clusterInfo != nil && clusterInfo.Enabled {
					nDCHistoryResender := xdc.NewNDCHistoryResender(
						t.shard.GetNamespaceRegistry(),
						t.shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
						func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
							return t.historyService.ReplicateEventsV2(ctx, request)
						},
						t.shard.GetService().GetPayloadSerializer(),
						t.config.StandbyTaskReReplicationContextTimeout,
						t.logger,
					)
					processor := newTransferQueueStandbyProcessor(
						clusterName,
						t.shard,
						t.historyService,
						t.matchingClient,
						t.taskAllocator,
						nDCHistoryResender,
						t.logger,
					)
					processor.Start()
					t.standbyTaskProcessors[clusterName] = processor
				}
			}
		},
	)
}
