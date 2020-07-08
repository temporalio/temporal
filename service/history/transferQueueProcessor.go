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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/xdc"
)

var (
	errUnknownTransferTask = errors.New("Unknown transfer task")
)

type (
	transferQueueProcessor interface {
		common.Daemon
		FailoverNamespace(namespaceIDs map[string]struct{})
		NotifyNewTask(clusterName string, transferTasks []persistence.Task)
		LockTaskProcessing()
		UnlockTaskPrrocessing()
	}

	taskFilter func(task queueTaskInfo) (bool, error)

	transferQueueProcessorImpl struct {
		isGlobalNamespaceEnabled bool
		currentClusterName       string
		shard                    ShardContext
		taskAllocator            taskAllocator
		config                   *Config
		metricsClient            metrics.Client
		historyService           *historyEngineImpl
		visibilityMgr            persistence.VisibilityManager
		matchingClient           matching.Client
		historyClient            history.Client
		ackLevel                 int64
		logger                   log.Logger
		isStarted                int32
		isStopped                int32
		shutdownChan             chan struct{}
		queueTaskProcessor       queueTaskProcessor
		activeTaskProcessor      *transferQueueActiveProcessorImpl
		standbyTaskProcessors    map[string]*transferQueueStandbyProcessorImpl
	}
)

func newTransferQueueProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	historyClient history.Client,
	queueTaskProcessor queueTaskProcessor,
	logger log.Logger,
) *transferQueueProcessorImpl {

	logger = logger.WithTags(tag.ComponentTransferQueue)
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	taskAllocator := newTaskAllocator(shard)
	standbyTaskProcessors := make(map[string]*transferQueueStandbyProcessorImpl)
	for clusterName, info := range shard.GetService().GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if clusterName != currentClusterName {
			historyRereplicator := xdc.NewHistoryRereplicator(
				currentClusterName,
				shard.GetNamespaceCache(),
				shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
				func(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) error {
					return historyService.ReplicateRawEvents(ctx, request)
				},
				persistence.NewPayloadSerializer(),
				historyRereplicationTimeout,
				nil,
				logger,
			)
			nDCHistoryResender := xdc.NewNDCHistoryResender(
				shard.GetNamespaceCache(),
				shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
				func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
					return historyService.ReplicateEventsV2(ctx, request)
				},
				shard.GetService().GetPayloadSerializer(),
				nil,
				logger,
			)
			standbyTaskProcessors[clusterName] = newTransferQueueStandbyProcessor(
				clusterName,
				shard,
				historyService,
				visibilityMgr,
				matchingClient,
				taskAllocator,
				historyRereplicator,
				nDCHistoryResender,
				queueTaskProcessor,
				logger,
			)
		}
	}

	return &transferQueueProcessorImpl{
		isGlobalNamespaceEnabled: shard.GetService().GetClusterMetadata().IsGlobalNamespaceEnabled(),
		currentClusterName:       currentClusterName,
		shard:                    shard,
		taskAllocator:            taskAllocator,
		config:                   shard.GetConfig(),
		metricsClient:            historyService.metricsClient,
		historyService:           historyService,
		visibilityMgr:            visibilityMgr,
		matchingClient:           matchingClient,
		historyClient:            historyClient,
		ackLevel:                 shard.GetTransferAckLevel(),
		logger:                   logger,
		shutdownChan:             make(chan struct{}),
		queueTaskProcessor:       queueTaskProcessor,
		activeTaskProcessor: newTransferQueueActiveProcessor(
			shard,
			historyService,
			visibilityMgr,
			matchingClient,
			historyClient,
			taskAllocator,
			queueTaskProcessor,
			logger,
		),
		standbyTaskProcessors: standbyTaskProcessors,
	}
}

func (t *transferQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	t.activeTaskProcessor.Start()
	if t.isGlobalNamespaceEnabled {
		for _, standbyTaskProcessor := range t.standbyTaskProcessors {
			standbyTaskProcessor.Start()
		}
	}

	go t.completeTransferLoop()
}

func (t *transferQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.activeTaskProcessor.Stop()
	if t.isGlobalNamespaceEnabled {
		for _, standbyTaskProcessor := range t.standbyTaskProcessors {
			standbyTaskProcessor.Stop()
		}
	}
	close(t.shutdownChan)
}

// NotifyNewTask - Notify the processor about the new active / standby transfer task arrival.
// This should be called each time new transfer task arrives, otherwise tasks maybe delayed.
func (t *transferQueueProcessorImpl) NotifyNewTask(
	clusterName string,
	transferTasks []persistence.Task,
) {

	if clusterName == t.currentClusterName {
		// we will ignore the current time passed in, since the active processor process task immediately
		if len(transferTasks) != 0 {
			t.activeTaskProcessor.notifyNewTask()
		}
		return
	}

	standbyTaskProcessor, ok := t.standbyTaskProcessors[clusterName]
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
		t.visibilityMgr,
		t.matchingClient,
		t.historyClient,
		namespaceIDs,
		standbyClusterName,
		minLevel,
		maxLevel,
		t.taskAllocator,
		t.queueTaskProcessor,
		t.logger,
	)

	for _, standbyTaskProcessor := range t.standbyTaskProcessors {
		standbyTaskProcessor.retryTasks()
	}

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

func (t *transferQueueProcessorImpl) UnlockTaskPrrocessing() {
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
			for attempt := 0; attempt < t.config.TransferProcessorCompleteTransferFailureRetryCount(); attempt++ {
				err := t.completeTransfer()
				if err != nil {
					t.logger.Info("Failed to complete transfer task", tag.Error(err))
					if err == ErrShardClosed {
						// shard closed, trigger shutdown and bail out
						t.Stop()
						return
					}
					backoff := time.Duration(attempt * 100)
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
