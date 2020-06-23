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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/reset"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
	"github.com/uber/cadence/service/worker/archiver"
)

const (
	// TODO: move this constant to xdc package
	historyRereplicationTimeout = 30 * time.Second

	defaultProcessingQueueLevel = 0
)

var (
	errUnexpectedQueueTask = errors.New("unexpected queue task")

	maxTransferReadLevel = newTransferTaskKey(math.MaxInt64)
)

type (
	transferQueueProcessor struct {
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

		ackLevel               int64
		taskAllocator          TaskAllocator
		activeTaskExecutor     task.Executor
		activeQueueProcessor   *transferQueueProcessorBase
		standbyQueueProcessors map[string]*transferQueueProcessorBase
	}
)

// NewTransferQueueProcessor creates a new transfer QueueProcessor
func NewTransferQueueProcessor(
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	executionCache *execution.Cache,
	workflowResetor reset.WorkflowResetor,
	workflowResetter reset.WorkflowResetter,
	archivalClient archiver.Client,
) Processor {
	logger := shard.GetLogger().WithTags(tag.ComponentTransferQueue)
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	taskAllocator := NewTaskAllocator(shard)

	activeTaskExecutor := task.NewTransferActiveTaskExecutor(
		shard,
		archivalClient,
		executionCache,
		workflowResetor,
		workflowResetter,
		logger,
		shard.GetMetricsClient(),
		shard.GetConfig(),
	)

	activeQueueProcessor := newTransferQueueActiveProcessor(
		shard,
		historyEngine,
		taskProcessor,
		taskAllocator,
		activeTaskExecutor,
		logger,
	)

	standbyQueueProcessors := make(map[string]*transferQueueProcessorBase)
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
		standbyTaskExecutor := task.NewTransferStandbyTaskExecutor(
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
		standbyQueueProcessors[clusterName] = newTransferQueueStandbyProcessor(
			clusterName,
			shard,
			historyEngine,
			taskProcessor,
			taskAllocator,
			standbyTaskExecutor,
			logger,
		)
	}

	return &transferQueueProcessor{
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

		ackLevel:               shard.GetTransferAckLevel(),
		taskAllocator:          taskAllocator,
		activeTaskExecutor:     activeTaskExecutor,
		activeQueueProcessor:   activeQueueProcessor,
		standbyQueueProcessors: standbyQueueProcessors,
	}
}

func (t *transferQueueProcessor) Start() {
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
	go t.completeTransferLoop()
}

func (t *transferQueueProcessor) Stop() {
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

func (t *transferQueueProcessor) NotifyNewTask(
	clusterName string,
	transferTasks []persistence.Task,
) {
	if len(transferTasks) == 0 {
		return
	}

	if clusterName == t.currentClusterName {
		t.activeQueueProcessor.notifyNewTask()
		return
	}

	standbyQueueProcessor, ok := t.standbyQueueProcessors[clusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot find transfer processor for %s.", clusterName))
	}
	standbyQueueProcessor.notifyNewTask()
}

func (t *transferQueueProcessor) FailoverDomain(
	domainIDs map[string]struct{},
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

	maxReadLevel := int64(0)
	for _, queueState := range t.activeQueueProcessor.getProcessingQueueStates() {
		queueReadLevel := queueState.ReadLevel().(transferTaskKey).taskID
		if maxReadLevel < queueReadLevel {
			maxReadLevel = queueReadLevel
		}
	}
	// maxReadLevel is exclusive, so add 1
	maxReadLevel++

	t.logger.Info("Transfer Failover Triggered",
		tag.WorkflowDomainIDs(domainIDs),
		tag.MinLevel(minLevel),
		tag.MaxLevel(maxReadLevel))

	updateShardAckLevel, failoverQueueProcessor := newTransferQueueFailoverProcessor(
		t.shard,
		t.historyEngine,
		t.taskProcessor,
		t.taskAllocator,
		t.activeTaskExecutor,
		t.logger,
		minLevel,
		maxReadLevel,
		domainIDs,
		standbyClusterName,
	)

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerDomainFailoverCallback function
	err := updateShardAckLevel(newTransferTaskKey(minLevel))
	if err != nil {
		t.logger.Error("Error update shard ack level", tag.Error(err))
	}
	failoverQueueProcessor.Start()
}

func (t *transferQueueProcessor) LockTaskProcessing() {
	t.taskAllocator.Lock()
}

func (t *transferQueueProcessor) UnlockTaskProcessing() {
	t.taskAllocator.Unlock()
}

func (t *transferQueueProcessor) completeTransferLoop() {
	defer t.shutdownWG.Done()

	completeTimer := time.NewTimer(t.config.TransferProcessorCompleteTransferInterval())
	defer completeTimer.Stop()

	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			if err := t.completeTransfer(); err != nil {
				t.logger.Error("Error complete transfer task", tag.Error(err))
			}
			return
		case <-completeTimer.C:
			for attempt := 0; attempt < t.config.TransferProcessorCompleteTransferFailureRetryCount(); attempt++ {
				err := t.completeTransfer()
				if err == nil {
					break
				}

				t.logger.Error("Failed to complete transfer task", tag.Error(err))
				if err == shard.ErrShardClosed {
					// shard closed, trigger shutdown and bail out
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

			completeTimer.Reset(t.config.TransferProcessorCompleteTransferInterval())
		}
	}
}

func (t *transferQueueProcessor) completeTransfer() error {
	var newAckLevel task.Key
	for _, queueState := range t.activeQueueProcessor.getProcessingQueueStates() {
		if newAckLevel == nil {
			newAckLevel = queueState.AckLevel()
		} else {
			newAckLevel = minTaskKey(newAckLevel, queueState.AckLevel())
		}
	}

	if t.isGlobalDomainEnabled {
		for _, standbyQueueProcessor := range t.standbyQueueProcessors {
			for _, queueState := range standbyQueueProcessor.getProcessingQueueStates() {
				if newAckLevel == nil {
					newAckLevel = queueState.AckLevel()
				} else {
					newAckLevel = minTaskKey(newAckLevel, queueState.AckLevel())
				}
			}
		}

		for _, failoverInfo := range t.shard.GetAllTransferFailoverLevels() {
			failoverLevel := newTransferTaskKey(failoverInfo.MinLevel)
			if newAckLevel == nil {
				newAckLevel = failoverLevel
			} else {
				newAckLevel = minTaskKey(newAckLevel, failoverLevel)
			}
		}
	}

	if newAckLevel == nil {
		panic("Unable to get transfer queue processor ack level")
	}

	newAckLevelTaskID := newAckLevel.(transferTaskKey).taskID
	t.logger.Debug(fmt.Sprintf("Start completing transfer task from: %v, to %v.", t.ackLevel, newAckLevelTaskID))
	if t.ackLevel >= newAckLevelTaskID {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if err := t.shard.GetExecutionManager().RangeCompleteTransferTask(&persistence.RangeCompleteTransferTaskRequest{
		ExclusiveBeginTaskID: t.ackLevel,
		InclusiveEndTaskID:   newAckLevelTaskID,
	}); err != nil {
		return err
	}

	t.ackLevel = newAckLevelTaskID

	return t.shard.UpdateTransferAckLevel(newAckLevelTaskID)
}

func newTransferQueueActiveProcessor(
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
) *transferQueueProcessorBase {
	config := shard.GetConfig()
	options := newTransferQueueProcessorOptions(config, true, false)

	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	logger = logger.WithTags(tag.ClusterName(currentClusterName))

	taskFilter := func(taskInfo task.Info) (bool, error) {
		task, ok := taskInfo.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyActiveTask(task.DomainID, task)
	}

	maxReadLevel := func() task.Key {
		return newTransferTaskKey(shard.GetTransferMaxReadLevel())
	}

	updateTransferAckLevel := func(ackLevel task.Key) error {
		taskID := ackLevel.(transferTaskKey).taskID
		return shard.UpdateTransferClusterAckLevel(currentClusterName, taskID)
	}

	transferQueueShutdown := func() error {
		return nil
	}

	redispatchQueue := collection.NewConcurrentQueue()

	taskInitializer := func(taskInfo task.Info) task.Task {
		return task.NewTransferTask(
			shard,
			taskInfo,
			task.QueueTypeActiveTransfer,
			task.InitializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			taskFilter,
			taskExecutor,
			func(task task.Task) {
				redispatchQueue.Add(task)
			},
			shard.GetTimeSource(),
			config.TransferTaskMaxRetryCount,
			nil,
		)
	}

	// TODO: once persistency layer is implemented for multi-cursor queue,
	// initialize queue states with data loaded from DB.
	ackLevel := newTransferTaskKey(shard.GetTransferClusterAckLevel(currentClusterName))
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			defaultProcessingQueueLevel,
			ackLevel,
			maxTransferReadLevel,
			NewDomainFilter(nil, true),
		),
	}

	return newTransferQueueProcessorBase(
		shard,
		processingQueueStates,
		taskProcessor,
		redispatchQueue,
		options,
		maxReadLevel,
		updateTransferAckLevel,
		transferQueueShutdown,
		taskInitializer,
		logger,
		shard.GetMetricsClient(),
	)
}

func newTransferQueueStandbyProcessor(
	clusterName string,
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
) *transferQueueProcessorBase {
	config := shard.GetConfig()
	options := newTransferQueueProcessorOptions(config, false, false)

	logger = logger.WithTags(tag.ClusterName(clusterName))

	taskFilter := func(taskInfo task.Info) (bool, error) {
		task, ok := taskInfo.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyStandbyTask(clusterName, task.DomainID, task)
	}

	maxReadLevel := func() task.Key {
		return newTransferTaskKey(shard.GetTransferMaxReadLevel())
	}

	updateTransferAckLevel := func(ackLevel task.Key) error {
		taskID := ackLevel.(transferTaskKey).taskID
		return shard.UpdateTransferClusterAckLevel(clusterName, taskID)
	}

	transferQueueShutdown := func() error {
		return nil
	}

	redispatchQueue := collection.NewConcurrentQueue()

	taskInitializer := func(taskInfo task.Info) task.Task {
		return task.NewTransferTask(
			shard,
			taskInfo,
			task.QueueTypeStandbyTransfer,

			task.InitializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			taskFilter,
			taskExecutor,
			func(task task.Task) {
				redispatchQueue.Add(task)
			},
			shard.GetTimeSource(),
			config.TransferTaskMaxRetryCount,
			nil,
		)
	}

	// TODO: once persistency layer is implemented for multi-cursor queue,
	// initialize queue states with data loaded from DB.
	ackLevel := newTransferTaskKey(shard.GetTransferClusterAckLevel(clusterName))
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			defaultProcessingQueueLevel,
			ackLevel,
			maxTransferReadLevel,
			NewDomainFilter(nil, true),
		),
	}

	return newTransferQueueProcessorBase(
		shard,
		processingQueueStates,
		taskProcessor,
		redispatchQueue,
		options,
		maxReadLevel,
		updateTransferAckLevel,
		transferQueueShutdown,
		taskInitializer,
		logger,
		shard.GetMetricsClient(),
	)
}

func newTransferQueueFailoverProcessor(
	shard shard.Context,
	historyEngine engine.Engine,
	taskProcessor task.Processor,
	taskAllocator TaskAllocator,
	taskExecutor task.Executor,
	logger log.Logger,
	minLevel, maxLevel int64,
	domainIDs map[string]struct{},
	standbyClusterName string,
) (updateClusterAckLevelFn, *transferQueueProcessorBase) {
	config := shard.GetConfig()
	options := newTransferQueueProcessorOptions(config, true, true)

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	failoverUUID := uuid.New()
	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowDomainIDs(domainIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	taskFilter := func(taskInfo task.Info) (bool, error) {
		task, ok := taskInfo.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.VerifyFailoverActiveTask(domainIDs, task.DomainID, task)
	}

	maxReadLevelTaskKey := newTransferTaskKey(maxLevel)
	maxReadLevel := func() task.Key {
		return maxReadLevelTaskKey // this is a const
	}

	updateTransferAckLevel := func(ackLevel task.Key) error {
		taskID := ackLevel.(transferTaskKey).taskID
		return shard.UpdateTransferFailoverLevel(
			failoverUUID,
			persistence.TransferFailoverLevel{
				StartTime:    shard.GetTimeSource().Now(),
				MinLevel:     minLevel,
				CurrentLevel: taskID,
				MaxLevel:     maxLevel,
				DomainIDs:    domainIDs,
			},
		)
	}

	transferQueueShutdown := func() error {
		return shard.DeleteTransferFailoverLevel(failoverUUID)
	}

	redispatchQueue := collection.NewConcurrentQueue()

	taskInitializer := func(taskInfo task.Info) task.Task {
		return task.NewTransferTask(
			shard,
			taskInfo,
			task.QueueTypeActiveTransfer,
			task.InitializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			taskFilter,
			taskExecutor,
			func(task task.Task) {
				redispatchQueue.Add(task)
			},
			shard.GetTimeSource(),
			config.TransferTaskMaxRetryCount,
			nil,
		)
	}

	// TODO: once persistency layer is implemented for multi-cursor queue,
	// initialize queue states with data loaded from DB.
	processingQueueStates := []ProcessingQueueState{
		NewProcessingQueueState(
			defaultProcessingQueueLevel,
			newTransferTaskKey(minLevel),
			maxReadLevelTaskKey,
			NewDomainFilter(domainIDs, false),
		),
	}

	return updateTransferAckLevel, newTransferQueueProcessorBase(
		shard,
		processingQueueStates,
		taskProcessor,
		redispatchQueue,
		options,
		maxReadLevel,
		updateTransferAckLevel,
		transferQueueShutdown,
		taskInitializer,
		logger,
		shard.GetMetricsClient(),
	)
}
