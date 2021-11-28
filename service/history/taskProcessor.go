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
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

const (
	taskTimeout = time.Second * 10
)

type (
	taskProcessorOptions struct {
		queueSize   int
		workerCount int
	}

	taskInfo struct {
		tasks.Task

		// TODO: change to queueTaskExecutor
		processor taskExecutor

		attempt   int
		startTime time.Time

		userLatency time.Duration
		logger      log.Logger

		// used by 2DC task life cycle
		// TODO remove when NDC task life cycle is implemented
		shouldProcessTask bool
	}

	taskProcessor struct {
		shard         shard.Context
		cache         workflow.Cache
		shutdownCh    chan struct{}
		tasksCh       chan *taskInfo
		config        *configs.Config
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
	task tasks.Task,
	logger log.Logger,
) *taskInfo {
	return &taskInfo{
		Task: task,

		processor:         processor,
		attempt:           1,
		startTime:         time.Now().UTC(), // used for metrics
		userLatency:       0,
		logger:            logger,
		shouldProcessTask: true,
	}
}

func newTaskProcessor(
	options taskProcessorOptions,
	shard shard.Context,
	historyCache workflow.Cache,
	logger log.Logger,
) *taskProcessor {

	var workerNotificationChs []chan struct{}
	for index := 0; index < options.workerCount; index++ {
		workerNotificationChs = append(workerNotificationChs, make(chan struct{}, 1))
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
		workerNotificationChans: workerNotificationChs,
		retryPolicy:             common.CreatePersistenceRetryPolicy(),
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
			task.shouldProcessTask, err = task.processor.getTaskFilter()(task.Task)
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
			if task.attempt > t.config.TimerTaskMaxRetryCount() {
				scope.RecordDistribution(metrics.TaskAttemptTimer, task.attempt)
				task.logger.Error("Critical error processing task, retrying.",
					tag.Error(err), tag.OperationCritical, tag.Task(task.Task))
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

	ctx := context.Background()
	startTime := t.timeSource.Now()
	scopeIdx, err := task.processor.process(ctx, task)
	if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency); ok {
		task.userLatency += time.Duration(duration)
	}
	scope := t.metricsClient.Scope(scopeIdx).Tagged(t.getNamespaceTagByID(namespace.ID(task.GetNamespaceID())))
	if task.shouldProcessTask {
		scope.IncCounter(metrics.TaskRequests)
		scope.RecordTimer(metrics.TaskProcessingLatency, time.Since(startTime))
		scope.RecordTimer(metrics.TaskNoUserProcessingLatency, time.Since(startTime)-task.userLatency)
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

	// this is a transient error
	if err == consts.ErrTaskRetry {
		scope.IncCounter(metrics.TaskStandbyRetryCounter)
		select {
		case <-notificationChan:
		case <-t.shutdownCh:
		}
		return err
	}

	if err == consts.ErrTaskDiscarded {
		scope.IncCounter(metrics.TaskDiscarded)
		err = nil
	}

	// this is a transient error
	// TODO remove this error check special case
	//  since the new task life cycle will not give up until task processed / verified
	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		if t.timeSource.Now().Sub(task.startTime) > 2*namespace.CacheRefreshInterval {
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
		scope.RecordDistribution(metrics.TaskAttemptTimer, task.attempt)
		scope.RecordTimer(metrics.TaskLatency, time.Since(task.startTime))
		scope.RecordTimer(metrics.TaskQueueLatency, time.Since(task.GetVisibilityTime()))
		scope.RecordTimer(metrics.TaskUserLatency, task.userLatency)
		scope.RecordTimer(metrics.TaskNoUserLatency, time.Since(task.startTime)-task.userLatency)
		scope.RecordTimer(metrics.TaskNoUserQueueLatency, time.Since(task.GetVisibilityTime())-task.userLatency)
	}
}

func (t *taskProcessor) getNamespaceTagByID(namespaceID namespace.ID) metrics.Tag {
	namespaceName, err := t.shard.GetNamespaceRegistry().GetNamespaceName(namespaceID)
	if err != nil {
		t.logger.Error("Unable to get namespace", tag.Error(err))
		return metrics.NamespaceUnknownTag()
	}
	return metrics.NamespaceTag(namespaceName.String())
}
