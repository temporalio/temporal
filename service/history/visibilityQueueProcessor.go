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

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	visibilityQueueProcessor interface {
		common.Daemon
		NotifyNewTask(visibilityTasks []tasks.Task)
	}

	updateVisibilityAckLevel func(ackLevel int64) error
	visibilityQueueShutdown  func() error

	visibilityQueueProcessorImpl struct {
		// from transferQueueActiveProcessorImpl (transferQueueProcessorImpl.activeTaskProcessor)
		*queueProcessorBase
		queueAckMgr
		shard                    shard.Context
		options                  *QueueProcessorOptions
		executionManager         persistence.ExecutionManager
		maxReadAckLevel          maxReadAckLevel
		updateVisibilityAckLevel updateVisibilityAckLevel
		visibilityQueueShutdown  visibilityQueueShutdown
		visibilityTaskFilter     taskFilter
		logger                   log.Logger
		metricsClient            metrics.Client
		taskExecutor             queueTaskExecutor

		// from transferQueueProcessorImpl
		config   *configs.Config
		ackLevel int64

		isStarted    int32
		isStopped    int32
		shutdownChan chan struct{}
	}
)

func newVisibilityQueueProcessor(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	visibilityMgr manager.VisibilityManager,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	queueTaskProcessor queueTaskProcessor,
	logger log.Logger,
) *visibilityQueueProcessorImpl {

	config := shard.GetConfig()
	logger = log.With(logger, tag.ComponentVisibilityQueue)

	options := &QueueProcessorOptions{
		BatchSize:                           config.VisibilityTaskBatchSize,
		WorkerCount:                         config.VisibilityTaskWorkerCount,
		MaxPollRPS:                          config.VisibilityProcessorMaxPollRPS,
		MaxPollInterval:                     config.VisibilityProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.VisibilityProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                       config.VisibilityTaskMaxRetryCount,
		RedispatchInterval:                  config.VisibilityProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.VisibilityProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.VisibilityProcessorMaxRedispatchQueueSize,
		EnablePriorityTaskProcessor:         config.VisibilityProcessorEnablePriorityTaskProcessor,
		MetricScope:                         metrics.VisibilityQueueProcessorScope,
	}
	visibilityTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		return true, nil
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateVisibilityAckLevel := func(ackLevel int64) error {
		return shard.UpdateVisibilityAckLevel(ackLevel)
	}

	visibilityQueueShutdown := func() error {
		return nil
	}

	retProcessor := &visibilityQueueProcessorImpl{
		shard:                    shard,
		options:                  options,
		maxReadAckLevel:          maxReadAckLevel,
		updateVisibilityAckLevel: updateVisibilityAckLevel,
		visibilityQueueShutdown:  visibilityQueueShutdown,
		visibilityTaskFilter:     visibilityTaskFilter,
		logger:                   logger,
		metricsClient:            historyEngine.metricsClient,
		taskExecutor: newVisibilityQueueTaskExecutor(
			shard,
			historyEngine,
			visibilityMgr,
			logger,
			historyEngine.metricsClient,
			config,
		),

		config:       config,
		ackLevel:     shard.GetVisibilityAckLevel(),
		shutdownChan: make(chan struct{}),

		queueAckMgr:        nil, // is set bellow
		queueProcessorBase: nil, // is set bellow
		executionManager:   shard.GetExecutionManager(),
	}

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		retProcessor,
		shard.GetVisibilityAckLevel(),
		logger,
	)

	redispatchQueue := collection.NewConcurrentQueue()

	visibilityQueueTaskInitializer := func(taskInfo queueTaskInfo) queueTask {
		return newVisibilityQueueTask(
			shard,
			taskInfo,
			historyEngine.metricsClient.Scope(
				getVisibilityTaskMetricsScope(taskInfo.GetTaskType()),
			),
			initializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
			visibilityTaskFilter,
			retProcessor.taskExecutor,
			redispatchQueue,
			shard.GetTimeSource(),
			options.MaxRetryCount,
			queueAckMgr,
		)
	}

	queueProcessorBase := newQueueProcessorBase(
		shard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard,
		options,
		retProcessor,
		queueTaskProcessor,
		queueAckMgr,
		redispatchQueue,
		historyEngine.historyCache,
		visibilityQueueTaskInitializer,
		logger,
		shard.GetMetricsClient().Scope(metrics.VisibilityQueueProcessorScope),
	)
	retProcessor.queueAckMgr = queueAckMgr
	retProcessor.queueProcessorBase = queueProcessorBase

	return retProcessor
}

// visibilityQueueProcessor implementation
func (t *visibilityQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	t.queueProcessorBase.Start()
	go t.completeTaskLoop()
}

func (t *visibilityQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.queueProcessorBase.Stop()
	close(t.shutdownChan)
}

// NotifyNewTask - Notify the processor about the new visibility task arrival.
// This should be called each time new visibility task arrives, otherwise tasks maybe delayed.
func (t *visibilityQueueProcessorImpl) NotifyNewTask(
	visibilityTasks []tasks.Task,
) {
	if len(visibilityTasks) != 0 {
		t.notifyNewTask()
	}
}

func (t *visibilityQueueProcessorImpl) completeTaskLoop() {
	timer := time.NewTimer(t.config.VisibilityProcessorCompleteTaskInterval())
	defer timer.Stop()

	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			err := t.completeTask()
			if err != nil {
				t.logger.Error("Error complete visibility task", tag.Error(err))
			}
			return
		case <-timer.C:
			for attempt := 1; attempt <= t.config.VisibilityProcessorCompleteTaskFailureRetryCount(); attempt++ {
				err := t.completeTask()
				if err == nil {
					break
				}

				t.logger.Info("Failed to complete visibility task", tag.Error(err))
				if errors.Is(err, shard.ErrShardClosed) {
					// shard closed, trigger shutdown and bail out
					t.Stop()
					return
				}
				backoff := time.Duration((attempt-1)*100) * time.Millisecond
				time.Sleep(backoff)
			}
			timer.Reset(t.config.VisibilityProcessorCompleteTaskInterval())
		}
	}
}

func (t *visibilityQueueProcessorImpl) completeTask() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.queueAckMgr.getQueueAckLevel()

	t.logger.Debug("Start completing visibility task", tag.AckLevel(lowerAckLevel), tag.AckLevel(upperAckLevel))
	if lowerAckLevel >= upperAckLevel {
		return nil
	}

	t.metricsClient.IncCounter(metrics.VisibilityQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if lowerAckLevel < upperAckLevel {
		err := t.shard.GetExecutionManager().RangeCompleteVisibilityTask(&persistence.RangeCompleteVisibilityTaskRequest{
			ShardID:              t.shard.GetShardID(),
			ExclusiveBeginTaskID: lowerAckLevel,
			InclusiveEndTaskID:   upperAckLevel,
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateVisibilityAckLevel(upperAckLevel)
}

// queueProcessor interface
func (t *visibilityQueueProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

// taskExecutor interfaces
func (t *visibilityQueueProcessorImpl) getTaskFilter() taskFilter {
	return t.visibilityTaskFilter
}

func (t *visibilityQueueProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.task)
}

func (t *visibilityQueueProcessorImpl) process(
	ctx context.Context,
	taskInfo *taskInfo,
) (int, error) {
	// TODO: task metricScope should be determined when creating taskInfo
	metricScope := getVisibilityTaskMetricsScope(taskInfo.task.GetTaskType())
	return metricScope, t.taskExecutor.execute(ctx, taskInfo.task, taskInfo.shouldProcessTask)
}

// processor interfaces
func (t *visibilityQueueProcessorImpl) readTasks(
	readLevel int64,
) ([]queueTaskInfo, bool, error) {

	response, err := t.executionManager.GetVisibilityTasks(&persistence.GetVisibilityTasksRequest{
		ShardID:      t.shard.GetShardID(),
		ReadLevel:    readLevel,
		MaxReadLevel: t.maxReadAckLevel(),
		BatchSize:    t.options.BatchSize(),
	})

	if err != nil {
		return nil, false, err
	}

	tasks := make([]queueTaskInfo, len(response.Tasks))
	for i := range response.Tasks {
		tasks[i] = response.Tasks[i]
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (t *visibilityQueueProcessorImpl) updateAckLevel(
	ackLevel int64,
) error {

	return t.updateVisibilityAckLevel(ackLevel)
}

func (t *visibilityQueueProcessorImpl) queueShutdown() error {
	return t.visibilityQueueShutdown()
}

// some aux stuff
func getVisibilityTaskMetricsScope(
	taskType enumsspb.TaskType,
) int {
	switch taskType {
	case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION:
		return metrics.VisibilityTaskStartExecutionScope
	case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
		return metrics.VisibilityTaskUpsertExecutionScope
	case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
		return metrics.VisibilityTaskCloseExecutionScope
	case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		return metrics.VisibilityTaskDeleteExecutionScope
	default:
		return metrics.VisibilityQueueProcessorScope
	}
}
