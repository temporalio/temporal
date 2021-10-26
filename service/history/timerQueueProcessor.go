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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination timerQueueProcessor_mock.go

package history

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	errUnknownTimerTask = errors.New("unknown timer task")
)

type (
	timerQueueProcessor interface {
		common.Daemon
		FailoverNamespace(namespaceIDs map[string]struct{})
		NotifyNewTimers(clusterName string, timerTask []tasks.Task)
		LockTaskProcessing()
		UnlockTaskProcessing()
	}

	timeNow                 func() time.Time
	updateTimerAckLevel     func(timerKey) error
	timerQueueShutdown      func() error
	timerQueueProcessorImpl struct {
		isGlobalNamespaceEnabled bool
		currentClusterName       string
		shard                    shard.Context
		taskAllocator            taskAllocator
		config                   *configs.Config
		metricsClient            metrics.Client
		historyService           *historyEngineImpl
		ackLevel                 timerKey
		logger                   log.Logger
		matchingClient           matchingservice.MatchingServiceClient
		status                   int32
		shutdownChan             chan struct{}
		shutdownWG               sync.WaitGroup
		queueTaskProcessor       queueTaskProcessor
		activeTimerProcessor     *timerQueueActiveProcessorImpl
		standbyTimerProcessors   map[string]*timerQueueStandbyProcessorImpl
	}
)

func newTimerQueueProcessor(
	shard shard.Context,
	historyService *historyEngineImpl,
	matchingClient matchingservice.MatchingServiceClient,
	queueTaskProcessor queueTaskProcessor,
	logger log.Logger,
) timerQueueProcessor {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()
	logger = log.With(logger, tag.ComponentTimerQueue)
	taskAllocator := newTaskAllocator(shard)

	standbyTimerProcessors := make(map[string]*timerQueueStandbyProcessorImpl)
	for clusterName, info := range shard.GetService().GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if clusterName != shard.GetService().GetClusterMetadata().GetCurrentClusterName() {
			nDCHistoryResender := xdc.NewNDCHistoryResender(
				shard.GetNamespaceRegistry(),
				shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
				func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
					return historyService.ReplicateEventsV2(ctx, request)
				},
				shard.GetService().GetPayloadSerializer(),
				config.StandbyTaskReReplicationContextTimeout,
				logger,
			)
			standbyTimerProcessors[clusterName] = newTimerQueueStandbyProcessor(
				shard,
				historyService,
				clusterName,
				taskAllocator,
				nDCHistoryResender,
				queueTaskProcessor,
				logger,
			)
		}
	}

	return &timerQueueProcessorImpl{
		isGlobalNamespaceEnabled: shard.GetService().GetClusterMetadata().IsGlobalNamespaceEnabled(),
		currentClusterName:       currentClusterName,
		shard:                    shard,
		taskAllocator:            taskAllocator,
		config:                   config,
		metricsClient:            historyService.metricsClient,
		historyService:           historyService,
		ackLevel:                 timerKey{VisibilityTimestamp: shard.GetTimerAckLevel()},
		logger:                   logger,
		matchingClient:           matchingClient,
		status:                   common.DaemonStatusInitialized,
		shutdownChan:             make(chan struct{}),
		queueTaskProcessor:       queueTaskProcessor,
		activeTimerProcessor: newTimerQueueActiveProcessor(
			shard,
			historyService,
			matchingClient,
			taskAllocator,
			queueTaskProcessor,
			logger,
		),
		standbyTimerProcessors: standbyTimerProcessors,
	}
}

func (t *timerQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	t.activeTimerProcessor.Start()
	if t.isGlobalNamespaceEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			standbyTimerProcessor.Start()
		}
	}

	t.shutdownWG.Add(1)
	go t.completeTimersLoop()
}

func (t *timerQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	t.activeTimerProcessor.Stop()
	if t.isGlobalNamespaceEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			standbyTimerProcessor.Stop()
		}
	}
	close(t.shutdownChan)
	common.AwaitWaitGroup(&t.shutdownWG, time.Minute)
}

// NotifyNewTimers - Notify the processor about the new active / standby timer arrival.
// This should be called each time new timer arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorImpl) NotifyNewTimers(
	clusterName string,
	timerTasks []tasks.Task,
) {

	if clusterName == t.currentClusterName {
		t.activeTimerProcessor.notifyNewTimers(timerTasks)
		return
	}

	standbyTimerProcessor, ok := t.standbyTimerProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processor for %s.", clusterName))
	}
	standbyTimerProcessor.setCurrentTime(t.shard.GetCurrentTime(clusterName))
	standbyTimerProcessor.notifyNewTimers(timerTasks)
	standbyTimerProcessor.retryTasks()
}

func (t *timerQueueProcessorImpl) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {
	// Failover queue is used to scan all inflight tasks, if queue processor is not
	// started, there's no inflight task and we don't need to create a failover processor.
	// Also the HandleAction will be blocked if queue processor processing loop is not running.
	if atomic.LoadInt32(&t.status) != common.DaemonStatusStarted {
		return
	}

	minLevel := t.shard.GetTimerClusterAckLevel(t.currentClusterName)
	standbyClusterName := t.currentClusterName
	for clusterName, info := range t.shard.GetService().GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		ackLevel := t.shard.GetTimerClusterAckLevel(clusterName)
		if ackLevel.Before(minLevel) {
			minLevel = ackLevel
			standbyClusterName = clusterName
		}
	}
	// the ack manager is exclusive, so just add a cassandra min precision
	maxLevel := t.activeTimerProcessor.getReadLevel().VisibilityTimestamp.Add(1 * time.Millisecond)
	t.logger.Info("Timer Failover Triggered",
		tag.WorkflowNamespaceIDs(namespaceIDs),
		tag.MinLevel(minLevel.UnixNano()),
		tag.MaxLevel(maxLevel.UnixNano()))
	// we should consider make the failover idempotent
	updateShardAckLevel, failoverTimerProcessor := newTimerQueueFailoverProcessor(
		t.shard,
		t.historyService,
		namespaceIDs,
		standbyClusterName,
		minLevel,
		maxLevel,
		t.matchingClient,
		t.taskAllocator,
		t.queueTaskProcessor,
		t.logger,
	)

	for _, standbyTimerProcessor := range t.standbyTimerProcessors {
		standbyTimerProcessor.retryTasks()
	}

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerNamespaceFailoverCallback function
	err := updateShardAckLevel(timerKey{VisibilityTimestamp: minLevel})
	if err != nil {
		t.logger.Error("Error when update shard ack level", tag.Error(err))
	}
	failoverTimerProcessor.Start()
}

func (t *timerQueueProcessorImpl) LockTaskProcessing() {
	t.taskAllocator.lock()
}

func (t *timerQueueProcessorImpl) UnlockTaskProcessing() {
	t.taskAllocator.unlock()
}

func (t *timerQueueProcessorImpl) completeTimersLoop() {
	defer t.shutdownWG.Done()

	timer := time.NewTimer(t.config.TimerProcessorCompleteTimerInterval())
	defer timer.Stop()
	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			t.completeTimers() //nolint:errcheck
			return
		case <-timer.C:
		CompleteLoop:
			for attempt := 1; attempt <= t.config.TimerProcessorCompleteTimerFailureRetryCount(); attempt++ {
				err := t.completeTimers()
				if err != nil {
					t.logger.Info("Failed to complete timers.", tag.Error(err))
					if err == shard.ErrShardClosed {
						// shard is unloaded, timer processor should quit as well
						go t.Stop()
						return
					}
					backoff := time.Duration((attempt - 1) * 100)
					time.Sleep(backoff * time.Millisecond)
				} else {
					break CompleteLoop
				}
			}
			timer.Reset(t.config.TimerProcessorCompleteTimerInterval())
		}
	}
}

func (t *timerQueueProcessorImpl) completeTimers() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.activeTimerProcessor.getAckLevel()

	if t.isGlobalNamespaceEnabled {
		for _, standbyTimerProcessor := range t.standbyTimerProcessors {
			ackLevel := standbyTimerProcessor.getAckLevel()
			if !compareTimerIDLess(&upperAckLevel, &ackLevel) {
				upperAckLevel = ackLevel
			}
		}

		for _, failoverInfo := range t.shard.GetAllTimerFailoverLevels() {
			if !upperAckLevel.VisibilityTimestamp.Before(failoverInfo.MinLevel) {
				upperAckLevel = timerKey{VisibilityTimestamp: failoverInfo.MinLevel}
			}
		}
	}

	t.logger.Debug("Start completing timer task", tag.AckLevel(lowerAckLevel), tag.AckLevel(upperAckLevel))
	if !compareTimerIDLess(&lowerAckLevel, &upperAckLevel) {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if lowerAckLevel.VisibilityTimestamp.Before(upperAckLevel.VisibilityTimestamp) {
		err := t.shard.GetExecutionManager().RangeCompleteTimerTask(&persistence.RangeCompleteTimerTaskRequest{
			ShardID:                 t.shard.GetShardID(),
			InclusiveBeginTimestamp: lowerAckLevel.VisibilityTimestamp,
			ExclusiveEndTimestamp:   upperAckLevel.VisibilityTimestamp,
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateTimerAckLevel(t.ackLevel.VisibilityTimestamp)
}
