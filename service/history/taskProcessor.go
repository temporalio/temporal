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
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

type (
	taskProcessorOptions struct {
		queueSize   int
		workerCount int
	}

	taskInfo struct {
		// TODO: change to queueTaskExecutor
		processor taskExecutor
		task      queueTaskInfo

		attempt   int
		startTime time.Time
		logger    log.Logger

		// used by 2DC task life cycle
		// TODO remove when NDC task life cycle is implemented
		shouldProcessTask bool
	}

	taskProcessor struct {
		shard         ShardContext
		cache         *historyCache
		shutdownCh    chan struct{}
		tasksCh       chan *taskInfo
		config        *Config
		logger        log.Logger
		metricsClient metrics.Client
		timeSource    clock.TimeSource
		retryPolicy   backoff.RetryPolicy
		workerWG      sync.WaitGroup

		// worker coroutines notification
		workerNotificationChans []chan struct{}
		// duplicate numOfWorker from config.TimerTaskWorkerCount for dynamic config works correctly
		numOfWorker int
	}
)

func newTaskInfo(
	processor taskExecutor,
	task queueTaskInfo,
	logger log.Logger,
) *taskInfo {
	return &taskInfo{
		processor:         processor,
		task:              task,
		attempt:           0,
		startTime:         time.Now(), // used for metrics
		logger:            logger,
		shouldProcessTask: true,
	}
}

func newTaskProcessor(
	options taskProcessorOptions,
	shard ShardContext,
	historyCache *historyCache,
	logger log.Logger,
) *taskProcessor {

	workerNotificationChans := []chan struct{}{}
	for index := 0; index < options.workerCount; index++ {
		workerNotificationChans = append(workerNotificationChans, make(chan struct{}, 1))
	}

	base := &taskProcessor{
		shard:                   shard,
		cache:                   historyCache,
		shutdownCh:              make(chan struct{}),
		tasksCh:                 make(chan *taskInfo, options.queueSize),
		config:                  shard.GetConfig(),
		logger:                  logger,
		metricsClient:           shard.GetMetricsClient(),
		timeSource:              shard.GetTimeSource(),
		workerNotificationChans: workerNotificationChans,
		retryPolicy:             common.CreatePersistanceRetryPolicy(),
		numOfWorker:             options.workerCount,
	}

	return base
}

func (t *taskProcessor) start() {
	for i := 0; i < t.numOfWorker; i++ {
		t.workerWG.Add(1)
		notificationChan := t.workerNotificationChans[i]
		go t.taskWorker(notificationChan)
	}
	t.logger.Info("Task processor started.")
}

func (t *taskProcessor) stop() {
	close(t.shutdownCh)
	if success := common.AwaitWaitGroup(&t.workerWG, time.Minute); !success {
		t.logger.Warn("Task processor timed out on shutdown.")
	}
	t.logger.Info("Task processor shutdown.")
}

func (t *taskProcessor) taskWorker(
	notificationChan chan struct{},
) {
	defer t.workerWG.Done()

	for {
		select {
		case <-t.shutdownCh:
			return
		case task, ok := <-t.tasksCh:
			if !ok {
				return
			}
			t.processTaskAndAck(notificationChan, task)
		}
	}
}

func (t *taskProcessor) retryTasks() {
	for _, workerNotificationChan := range t.workerNotificationChans {
		select {
		case workerNotificationChan <- struct{}{}:
		default:
		}
	}
}

func (t *taskProcessor) addTask(
	task *taskInfo,
) bool {
	// We have a timer to fire.
	select {
	case t.tasksCh <- task:
	case <-t.shutdownCh:
		return false
	}
	return true
}

func (t *taskProcessor) processTaskAndAck(
	notificationChan <-chan struct{},
	task *taskInfo,
) {

	var scope metrics.Scope
	var err error

FilterLoop:
	for {
		select {
		case <-t.shutdownCh:
			// this must return without ack
			return
		default:
			task.shouldProcessTask, err = task.processor.getTaskFilter()(task.task)
			if err == nil {
				break FilterLoop
			}
			time.Sleep(loadNamespaceEntryForTimerTaskRetryDelay)
		}
	}

	op := func() error {
		scope, err = t.processTaskOnce(notificationChan, task)
		err := t.handleTaskError(scope, task, notificationChan, err)
		if err != nil {
			task.attempt++
			if task.attempt >= t.config.TimerTaskMaxRetryCount() {
				scope.RecordTimer(metrics.TaskAttemptTimer, time.Duration(task.attempt))
				task.logger.Error("Critical error processing task, retrying.",
					tag.Error(err), tag.OperationCritical, tag.TaskType(task.task.GetTaskType()))
			}
		}
		return err
	}
	retryCondition := func(err error) bool {
		select {
		case <-t.shutdownCh:
			return false
		default:
			return true
		}
	}

	for {
		select {
		case <-t.shutdownCh:
			// this must return without ack
			return
		default:
			err = backoff.Retry(op, t.retryPolicy, retryCondition)
			if err == nil {
				t.ackTaskOnce(scope, task)
				return
			}
		}
	}
}

func (t *taskProcessor) processTaskOnce(
	notificationChan <-chan struct{},
	task *taskInfo,
) (metrics.Scope, error) {

	select {
	case <-notificationChan:
	default:
	}

	startTime := t.timeSource.Now()
	scopeIdx, err := task.processor.process(task)
	scope := t.metricsClient.Scope(scopeIdx).Tagged(t.getNamespaceTagByID(task.task.GetNamespaceId()))
	if task.shouldProcessTask {
		scope.IncCounter(metrics.TaskRequests)
		scope.RecordTimer(metrics.TaskProcessingLatency, time.Since(startTime))
	}

	return scope, err
}

func (t *taskProcessor) handleTaskError(
	scope metrics.Scope,
	task *taskInfo,
	notificationChan <-chan struct{},
	err error,
) error {

	if err == nil {
		return nil
	}

	if _, ok := err.(*serviceerror.NotFound); ok {
		return nil
	}

	if transferTask, ok := task.task.(*persistenceblobs.TransferTaskInfo); ok &&
		transferTask.TaskType == enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION &&
		err == ErrMissingWorkflowStartEvent &&
		t.config.EnableDropStuckTaskByNamespaceID(task.task.GetNamespaceId()) { // use namespaceID here to avoid accessing namespaceCache
		scope.IncCounter(metrics.TransferTaskMissingEventCounter)
		task.logger.Error("Drop close execution transfer task due to corrupted workflow history", tag.Error(err), tag.LifeCycleProcessingFailed)
		return nil
	}

	// this is a transient error
	if err == ErrTaskRetry {
		scope.IncCounter(metrics.TaskStandbyRetryCounter)
		<-notificationChan
		return err
	}

	if err == ErrTaskDiscarded {
		scope.IncCounter(metrics.TaskDiscarded)
		err = nil
	}

	// this is a transient error
	// TODO remove this error check special case
	//  since the new task life cycle will not give up until task processed / verified
	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		if t.timeSource.Now().Sub(task.startTime) > 2*cache.NamespaceCacheRefreshInterval {
			scope.IncCounter(metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	scope.IncCounter(metrics.TaskFailures)

	if _, ok := err.(*persistence.CurrentWorkflowConditionFailedError); ok {
		task.logger.Error("More than 2 workflow are running.", tag.Error(err), tag.LifeCycleProcessingFailed)
		return nil
	}

	task.logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (t *taskProcessor) ackTaskOnce(
	scope metrics.Scope,
	task *taskInfo,
) {

	task.processor.complete(task)
	if task.shouldProcessTask {
		goVisibilityTime, _ := types.TimestampFromProto(task.task.GetVisibilityTime())
		scope.RecordTimer(metrics.TaskAttemptTimer, time.Duration(task.attempt))
		scope.RecordTimer(metrics.TaskLatency, time.Since(task.startTime))
		scope.RecordTimer(metrics.TaskQueueLatency, time.Since(goVisibilityTime))
	}
}

func (t *taskProcessor) getNamespaceTagByID(namespaceID string) metrics.Tag {
	namespace, err := t.shard.GetNamespaceCache().GetNamespaceName(namespaceID)
	if err != nil {
		t.logger.Error("Unable to get namespace", tag.Error(err))
		return metrics.NamespaceUnknownTag()
	}
	return metrics.NamespaceTag(namespace)
}
