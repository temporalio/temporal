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
	"errors"
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	tieredStorageQueueProcessor interface {
		common.Daemon
		NotifyNewTask(tieredStorageTasks []tasks.Task)
	}

	updateTieredStorageAckLevel func(ackLevel int64) error
	tieredStorageQueueShutdown  func() error

	tieredStorageQueueProcessorImpl struct {
		// from transferQueueActiveProcessorImpl (transferQueueProcessorImpl.activeTaskProcessor)
		*queueProcessorBase
		queueAckMgr
		shard                       shard.Context
		options                     *QueueProcessorOptions
		executionManager            persistence.ExecutionManager
		maxReadAckLevel             maxReadAckLevel
		updateTieredStorageAckLevel updateTieredStorageAckLevel
		tieredStorageQueueShutdown  tieredStorageQueueShutdown
		tieredStorageTaskFilter     taskFilter
		logger                      log.Logger
		metricsClient               metrics.Client
		taskExecutor                queueTaskExecutor

		// from transferQueueProcessorImpl
		config   *configs.Config
		ackLevel int64

		isStarted    int32
		isStopped    int32
		shutdownChan chan struct{}
	}
)

func newTieredStorageQueueProcessor(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	matchingClient matchingservice.MatchingServiceClient,
	_ historyservice.HistoryServiceClient,
	logger log.Logger,
) *tieredStorageQueueProcessorImpl {

	config := shard.GetConfig()
	logger = log.With(logger, tag.ComponentTieredStorageQueue)

	options := &QueueProcessorOptions{
		BatchSize:                           config.TieredStorageTaskBatchSize,
		WorkerCount:                         config.TieredStorageTaskWorkerCount,
		MaxPollRPS:                          config.TieredStorageProcessorMaxPollRPS,
		MaxPollInterval:                     config.TieredStorageProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TieredStorageProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TieredStorageProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TieredStorageProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                       config.TieredStorageTaskMaxRetryCount,
		RedispatchInterval:                  config.TieredStorageProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.TieredStorageProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.TieredStorageProcessorMaxRedispatchQueueSize,
		EnablePriorityTaskProcessor:         config.TieredStorageProcessorEnablePriorityTaskProcessor,
		MetricScope:                         metrics.TieredStorageQueueProcessorScope,
	}
	tieredStorageTaskFilter := func(taskInfo tasks.Task) (bool, error) {
		return true, nil
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateTieredStorageAckLevel := func(ackLevel int64) error {
		return shard.UpdateTieredStorageAckLevel(ackLevel)
	}

	tieredStorageQueueShutdown := func() error {
		return nil
	}

	retProcessor := &tieredStorageQueueProcessorImpl{
		shard:                       shard,
		options:                     options,
		maxReadAckLevel:             maxReadAckLevel,
		updateTieredStorageAckLevel: updateTieredStorageAckLevel,
		tieredStorageQueueShutdown:  tieredStorageQueueShutdown,
		tieredStorageTaskFilter:     tieredStorageTaskFilter,
		logger:                      logger,
		metricsClient:               historyEngine.metricsClient,
		taskExecutor: newTieredStorageQueueTaskExecutor(
			shard,
			historyEngine,
			logger,
			historyEngine.metricsClient,
			config,
			matchingClient,
		),

		config:       config,
		ackLevel:     shard.GetTieredStorageAckLevel(),
		shutdownChan: make(chan struct{}),

		queueAckMgr:        nil, // is set bellow
		queueProcessorBase: nil, // is set bellow
		executionManager:   shard.GetExecutionManager(),
	}

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		retProcessor,
		shard.GetTieredStorageAckLevel(),
		logger,
	)

	queueProcessorBase := newQueueProcessorBase(
		shard.GetClusterMetadata().GetCurrentClusterName(),
		shard,
		options,
		retProcessor,
		queueAckMgr,
		historyEngine.historyCache,
		logger,
		shard.GetMetricsClient().Scope(metrics.TieredStorageQueueProcessorScope),
	)
	retProcessor.queueAckMgr = queueAckMgr
	retProcessor.queueProcessorBase = queueProcessorBase

	return retProcessor
}

func (t *tieredStorageQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	t.queueProcessorBase.Start()
	go t.completeTaskLoop()
}

func (t *tieredStorageQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.queueProcessorBase.Stop()
	close(t.shutdownChan)
}

// NotifyNewTask - Notify the processor about the new tiered storage task arrival.
// This should be called each time new tiered storage task arrives, otherwise tasks maybe delayed.
func (t *tieredStorageQueueProcessorImpl) NotifyNewTask(
	tieredStorageTasks []tasks.Task,
) {
	if len(tieredStorageTasks) != 0 {
		t.notifyNewTask()
	}
}

func (t *tieredStorageQueueProcessorImpl) completeTaskLoop() {
	timer := time.NewTimer(t.config.TieredStorageProcessorCompleteTaskInterval())
	defer timer.Stop()

	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			err := t.completeTask()
			if err != nil {
				t.logger.Error("Error complete tiered storage task", tag.Error(err))
			}
			return
		case <-timer.C:
			for attempt := 1; attempt <= t.config.TieredStorageProcessorCompleteTaskFailureRetryCount(); attempt++ {
				err := t.completeTask()
				if err == nil {
					break
				}

				t.logger.Info("Failed to complete tiered storage task", tag.Error(err))
				if errors.Is(err, shard.ErrShardClosed) {
					// shard closed, trigger shutdown and bail out
					t.Stop()
					return
				}
				backoff := time.Duration((attempt-1)*100) * time.Millisecond
				time.Sleep(backoff)
			}
			timer.Reset(t.config.TieredStorageProcessorCompleteTaskInterval())
		}
	}
}

func (t *tieredStorageQueueProcessorImpl) completeTask() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.queueAckMgr.getQueueAckLevel()

	t.logger.Debug("Start completing tieredStorage task", tag.AckLevel(lowerAckLevel), tag.AckLevel(upperAckLevel))
	if lowerAckLevel >= upperAckLevel {
		return nil
	}

	t.metricsClient.IncCounter(metrics.TieredStorageQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if lowerAckLevel < upperAckLevel {
		err := t.shard.GetExecutionManager().RangeCompleteTieredStorageTask(&persistence.RangeCompleteTieredStorageTaskRequest{
			ShardID:              t.shard.GetShardID(),
			ExclusiveBeginTaskID: lowerAckLevel,
			InclusiveEndTaskID:   upperAckLevel,
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateTieredStorageAckLevel(upperAckLevel)
}

// queueProcessor interface
func (t *tieredStorageQueueProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

// taskExecutor interfaces
func (t *tieredStorageQueueProcessorImpl) getTaskFilter() taskFilter {
	return t.tieredStorageTaskFilter
}

func (t *tieredStorageQueueProcessorImpl) complete(
	taskInfo *taskInfo,
) {
	t.queueProcessorBase.complete(taskInfo.Task)
}

func (t *tieredStorageQueueProcessorImpl) process(
	ctx context.Context,
	taskInfo *taskInfo,
) (int, error) {
	return metrics.TieredStorageQueueProcessorScope, t.taskExecutor.execute(ctx, taskInfo.Task, taskInfo.shouldProcessTask)
}

// processor interfaces
func (t *tieredStorageQueueProcessorImpl) readTasks(
	readLevel int64,
) ([]tasks.Task, bool, error) {

	response, err := t.executionManager.GetTieredStorageTasks(&persistence.GetTieredStorageTasksRequest{
		ShardID:   t.shard.GetShardID(),
		MinTaskID: readLevel,
		MaxTaskID: t.maxReadAckLevel(),
		BatchSize: t.options.BatchSize(),
	})

	if err != nil {
		return nil, false, err
	}

	return response.Tasks, len(response.NextPageToken) != 0, nil
}

func (t *tieredStorageQueueProcessorImpl) updateAckLevel(
	ackLevel int64,
) error {
	return t.updateTieredStorageAckLevel(ackLevel)
}

func (t *tieredStorageQueueProcessorImpl) queueShutdown() error {
	return t.tieredStorageQueueShutdown()
}
