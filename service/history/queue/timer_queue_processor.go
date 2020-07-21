// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	timerQueueProcessor struct {
		shard         shard.Context
		historyEngine engine.Engine
		taskProcessor task.Processor

		config                *config.Config
		isGlobalDomainEnabled bool
		currentClusterName    string

		metricsClient metrics.Client
		logger        log.Logger

		status       int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		ackLevel               time.Time
		taskAllocator          TaskAllocator
		activeTaskExecutor     task.Executor
		activeQueueProcessor   *timerQueueProcessorBase
		standbyQueueProcessors map[string]*timerQueueProcessorBase
		standbyQueueTimerGates map[string]RemoteTimerGate
	}
)

// NewTimerQueueProcessor creates a new timer QueueProcessor
func NewTimerQueueProcessor(
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	executionCache *execution.Cache,
	archivalClient archiver.Client,
) Processor {
	logger := shard.GetLogger().WithTags(tag.ComponentTimerQueue)
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	taskAllocator := NewTaskAllocator(shard)

	activeTaskExecutor := task.NewTimerActiveTaskExecutor(
		shard,
		archivalClient,
		executionCache,
		logger,
		shard.GetMetricsClient(),
		shard.GetConfig(),
	)

	activeQueueProcessor := newTimerQueueActiveProcessor(
		currentClusterName,
		shard,
		historyEngine,
		taskProcessor,
		taskAllocator,
		activeTaskExecutor,
		logger,
	)

	standbyQueueProcessors := make(map[string]*timerQueueProcessorBase)
	standbyQueueTimerGates := make(map[string]RemoteTimerGate)
	rereplicatorLogger := shard.GetLogger().WithTags(tag.ComponentHistoryResender)
	resenderLogger := shard.GetLogger().WithTags(tag.ComponentHistoryResender)
	for clusterName, info := range shard.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled || clusterName == currentClusterName {
			continue
		}

		historyRereplicator := xdc.NewHistoryRereplicator(
			currentClusterName,
			shard.GetDomainCache(),
			shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
			func(ctx context.Context, request *h.ReplicateRawEventsRequest) error {
				return historyEngine.ReplicateRawEvents(ctx, request)
			},
			shard.GetService().GetPayloadSerializer(),
			historyRereplicationTimeout,
			nil,
			rereplicatorLogger,
		)
		nDCHistoryResender := xdc.NewNDCHistoryResender(
			shard.GetDomainCache(),
			shard.GetService().GetClientBean().GetRemoteAdminClient(clusterName),
			func(ctx context.Context, request *h.ReplicateEventsV2Request) error {
				return historyEngine.ReplicateEventsV2(ctx, request)
			},
			shard.GetService().GetPayloadSerializer(),
			nil,
			resenderLogger,
		)
		standbyTaskExecutor := task.NewTimerStandbyTaskExecutor(
			shard,
			archivalClient,
			executionCache,
			historyRereplicator,
			nDCHistoryResender,
			logger,
			shard.GetMetricsClient(),
			clusterName,
			shard.GetConfig(),
		)
		standbyQueueProcessors[clusterName], standbyQueueTimerGates[clusterName] = newTimerQueueStandbyProcessor(
			clusterName,
			shard,
			historyEngine,
			taskProcessor,
			taskAllocator,
			standbyTaskExecutor,
			logger,
		)
	}

	return &timerQueueProcessor{
		shard:         shard,
		historyEngine: historyEngine,
		taskProcessor: taskProcessor,

		config:                shard.GetConfig(),
		isGlobalDomainEnabled: shard.GetClusterMetadata().IsGlobalDomainEnabled(),
		currentClusterName:    currentClusterName,

		metricsClient: shard.GetMetricsClient(),
		logger:        logger,

		status:       common.DaemonStatusInitialized,
		shutdownChan: make(chan struct{}),

		ackLevel:               shard.GetTimerAckLevel(),
		taskAllocator:          taskAllocator,
		activeTaskExecutor:     activeTaskExecutor,
		activeQueueProcessor:   activeQueueProcessor,
		standbyQueueProcessors: standbyQueueProcessors,
		standbyQueueTimerGates: standbyQueueTimerGates,
	}
}

func (t *timerQueueProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.activeQueueProcessor.Start()
	if t.isGlobalDomainEnabled {
		for _, standbyQueueProcessor := range t.standbyQueueProcessors {
			standbyQueueProcessor.Start()
		}
	}

	t.shutdownWG.Add(1)
	go t.completeTimerLoop()
}

func (t *timerQueueProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	t.activeQueueProcessor.Stop()
	if t.isGlobalDomainEnabled {
		for _, standbyQueueProcessor := range t.standbyQueueProcessors {
			standbyQueueProcessor.Stop()
		}
	}

	close(t.shutdownChan)
	common.AwaitWaitGroup(&t.shutdownWG, time.Minute)
}

func (t *timerQueueProcessor) NotifyNewTask(
	clusterName string,
	timerTasks []persistence.Task,
) {
	if clusterName == t.currentClusterName {
		t.activeQueueProcessor.notifyNewTimers(timerTasks)
		return
	}

	standbyQueueProcessor, ok := t.standbyQueueProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer processor for %s.", clusterName))
	}

	standbyQueueTimerGate, ok := t.standbyQueueTimerGates[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find timer gate for %s.", clusterName))
	}

	standbyQueueTimerGate.SetCurrentTime(t.shard.GetCurrentTime(clusterName))
	standbyQueueProcessor.notifyNewTimers(timerTasks)
}

func (t *timerQueueProcessor) FailoverDomain(
	domainIDs map[string]struct{},
) {
	minLevel := t.shard.GetTimerClusterAckLevel(t.currentClusterName)
	standbyClusterName := t.currentClusterName
	for clusterName, info := range t.shard.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		ackLevel := t.shard.GetTimerClusterAckLevel(clusterName)
		if ackLevel.Before(minLevel) {
			minLevel = ackLevel
			standbyClusterName = clusterName
		}
	}

	maxReadLevel := time.Time{}
	for _, queueState := range t.activeQueueProcessor.getProcessingQueueStates() {
		queueReadLevel := queueState.ReadLevel().(timerTaskKey).visibilityTimestamp
		if maxReadLevel.Before(queueReadLevel) {
			maxReadLevel = queueReadLevel
		}
	}
	maxReadLevel.Add(1 * time.Millisecond)

	t.logger.Info("Timer Failover Triggered",
		tag.WorkflowDomainIDs(domainIDs),
		tag.MinLevel(minLevel.UnixNano()),
		tag.MaxLevel(maxReadLevel.UnixNano()),
	)

	updateClusterAckLevelFn, failoverQueueProcessor := newTimerQueueFailoverProcessor(
		standbyClusterName,
		t.shard,
		t.historyEngine,
		t.taskProcessor,
		t.taskAllocator,
		t.activeTaskExecutor,
		t.logger,
		minLevel,
		maxReadLevel,
		domainIDs,
	)

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerDomainFailoverCallback function
	err := updateClusterAckLevelFn(newTimerTaskKey(minLevel, 0))
	if err != nil {
		t.logger.Error("Error update shard ack level", tag.Error(err))
	}
	failoverQueueProcessor.Start()
}

func (t *timerQueueProcessor) LockTaskProcessing() {
	t.taskAllocator.Lock()
}

func (t *timerQueueProcessor) UnlockTaskProcessing() {
	t.taskAllocator.Unlock()
}

func (t *timerQueueProcessor) completeTimerLoop() {
	defer t.shutdownWG.Done()

	completeTimer := time.NewTimer(t.config.TimerProcessorCompleteTimerInterval())
	defer completeTimer.Stop()

	for {
		select {
		case <-t.shutdownChan:
			if err := t.completeTimer(); err != nil {
				t.logger.Error("Error complete timer task", tag.Error(err))
			}
			return
		case <-completeTimer.C:
			for attempt := 0; attempt < t.config.TimerProcessorCompleteTimerFailureRetryCount(); attempt++ {
				err := t.completeTimer()
				if err == nil {
					break
				}

				t.logger.Error("Error complete timer task", tag.Error(err))
				if err == shard.ErrShardClosed {
					go t.Stop()
					return
				}
				backoff := time.Duration(attempt * 100)
				time.Sleep(backoff * time.Millisecond)

				select {
				case <-t.shutdownChan:
					// break the retry loop if shutdown chan is closed
					break
				default:
				}
			}

			completeTimer.Reset(t.config.TimerProcessorCompleteTimerInterval())
		}
	}
}

func (t *timerQueueProcessor) completeTimer() error {
	newAckLevel := maximumTimerTaskKey
	for _, queueState := range t.activeQueueProcessor.getProcessingQueueStates() {
		newAckLevel = minTaskKey(newAckLevel, queueState.AckLevel())
	}

	if t.isGlobalDomainEnabled {
		for _, standbyQueueProcessor := range t.standbyQueueProcessors {
			for _, queueState := range standbyQueueProcessor.getProcessingQueueStates() {
				newAckLevel = minTaskKey(newAckLevel, queueState.AckLevel())
			}
		}

		for _, failoverInfo := range t.shard.GetAllTimerFailoverLevels() {
			failoverLevel := newTimerTaskKey(failoverInfo.MinLevel, 0)
			newAckLevel = minTaskKey(newAckLevel, failoverLevel)
		}
	}

	if newAckLevel == maximumTimerTaskKey {
		panic("Unable to get timer queue processor ack level")
	}

	newAckLevelTimeStamp := newAckLevel.(timerTaskKey).visibilityTimestamp
	t.logger.Debug(fmt.Sprintf("Start completing timer task from: %v, to %v", t.ackLevel, newAckLevelTimeStamp))
	if !t.ackLevel.Before(newAckLevelTimeStamp) {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if err := t.shard.GetExecutionManager().RangeCompleteTimerTask(&persistence.RangeCompleteTimerTaskRequest{
		InclusiveBeginTimestamp: t.ackLevel,
		ExclusiveEndTimestamp:   newAckLevelTimeStamp,
	}); err != nil {
		return err
	}

	t.ackLevel = newAckLevelTimeStamp

	return t.shard.UpdateTimerAckLevel(t.ackLevel)
}

func newTimerQueueActiveProcessor(
	clusterName string,
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
) *timerQueueProcessorBase {
	config := shard.GetConfig()
	options := newTimerQueueProcessorOptions(config, true, false)

	logger = logger.WithTags(tag.ClusterName(clusterName))

	taskFilter := func(taskInfo task.Info) (bool, error) {
		timer, ok := taskInfo.(*persistence.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyActiveTask(timer.DomainID, timer)
	}

	maxReadLevel := func() task.Key {
		return newTimerTaskKey(shard.UpdateTimerMaxReadLevel(clusterName), 0)
	}

	updateClusterAckLevel := func(ackLevel task.Key) error {
		return shard.UpdateTimerClusterAckLevel(clusterName, ackLevel.(timerTaskKey).visibilityTimestamp)
	}

	timerQueueShutdown := func() error {
		return nil
	}

	// TODO: once persistency layer is implemented for multi-cursor queue,
	// initialize queue states with data loaded from DB.
	ackLevel := newTimerTaskKey(shard.GetTimerClusterAckLevel(clusterName), 0)
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			defaultProcessingQueueLevel,
			ackLevel,
			maximumTimerTaskKey,
			NewDomainFilter(nil, true),
		),
	}

	return newTimerQueueProcessorBase(
		clusterName,
		shard,
		processingQueueStates,
		taskProcessor,
		NewLocalTimerGate(shard.GetTimeSource()),
		options,
		maxReadLevel,
		updateClusterAckLevel,
		timerQueueShutdown,
		taskFilter,
		taskExecutor,
		logger,
		shard.GetMetricsClient(),
	)
}

func newTimerQueueStandbyProcessor(
	clusterName string,
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
) (*timerQueueProcessorBase, RemoteTimerGate) {
	config := shard.GetConfig()
	options := newTimerQueueProcessorOptions(config, false, false)

	logger = logger.WithTags(tag.ClusterName(clusterName))

	taskFilter := func(taskInfo task.Info) (bool, error) {
		timer, ok := taskInfo.(*persistence.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyStandbyTask(clusterName, timer.DomainID, timer)
	}

	maxReadLevel := func() task.Key {
		return newTimerTaskKey(shard.UpdateTimerMaxReadLevel(clusterName), 0)
	}

	updateClusterAckLevel := func(ackLevel task.Key) error {
		return shard.UpdateTimerClusterAckLevel(clusterName, ackLevel.(timerTaskKey).visibilityTimestamp)
	}

	timerQueueShutdown := func() error {
		return nil
	}

	// TODO: once persistency layer is implemented for multi-cursor queue,
	// initialize queue states with data loaded from DB.
	ackLevel := newTimerTaskKey(shard.GetTimerClusterAckLevel(clusterName), 0)
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			defaultProcessingQueueLevel,
			ackLevel,
			maximumTimerTaskKey,
			NewDomainFilter(nil, true),
		),
	}

	remoteTimerGate := NewRemoteTimerGate()
	remoteTimerGate.SetCurrentTime(shard.GetCurrentTime(clusterName))

	return newTimerQueueProcessorBase(
		clusterName,
		shard,
		processingQueueStates,
		taskProcessor,
		remoteTimerGate,
		options,
		maxReadLevel,
		updateClusterAckLevel,
		timerQueueShutdown,
		taskFilter,
		taskExecutor,
		logger,
		shard.GetMetricsClient(),
	), remoteTimerGate
}

func newTimerQueueFailoverProcessor(
	standbyClusterName string,
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
	minLevel, maxLevel time.Time,
	domainIDs map[string]struct{},
) (updateClusterAckLevelFn, *timerQueueProcessorBase) {
	config := shard.GetConfig()
	options := newTimerQueueProcessorOptions(config, true, true)

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	failoverStartTime := shard.GetTimeSource().Now()
	failoverUUID := uuid.New()
	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowDomainIDs(domainIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	taskFilter := func(taskInfo task.Info) (bool, error) {
		timer, ok := taskInfo.(*persistence.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyFailoverActiveTask(domainIDs, timer.DomainID, timer)
	}

	maxReadLevelTaskKey := newTimerTaskKey(maxLevel, 0)
	maxReadLevel := func() task.Key {
		return maxReadLevelTaskKey // this is a const
	}

	updateClusterAckLevel := func(ackLevel task.Key) error {
		return shard.UpdateTimerFailoverLevel(
			failoverUUID,
			persistence.TimerFailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     minLevel,
				CurrentLevel: ackLevel.(timerTaskKey).visibilityTimestamp,
				MaxLevel:     maxLevel,
				DomainIDs:    domainIDs,
			},
		)
	}

	timerQueueShutdown := func() error {
		return shard.DeleteTimerFailoverLevel(failoverUUID)
	}

	// TODO: once persistency layer is implemented for multi-cursor queue,
	// initialize queue states with data loaded from DB.
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			defaultProcessingQueueLevel,
			newTimerTaskKey(minLevel, 0),
			maxReadLevelTaskKey,
			NewDomainFilter(domainIDs, false),
		),
	}

	return updateClusterAckLevel, newTimerQueueProcessorBase(
		currentClusterName, // should use current cluster's time when doing domain failover
		shard,
		processingQueueStates,
		taskProcessor,
		NewLocalTimerGate(shard.GetTimeSource()),
		options,
		maxReadLevel,
		updateClusterAckLevel,
		timerQueueShutdown,
		taskFilter,
		taskExecutor,
		logger,
		shard.GetMetricsClient(),
	)
}
