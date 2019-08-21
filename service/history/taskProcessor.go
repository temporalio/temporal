// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	taskProcessorOptions struct {
		queueSize   int
		workerCount int
	}

	taskInfo struct {
		processor taskExecutor
		task      queueTaskInfo
	}

	taskProcessor struct {
		shard         ShardContext
		cache         *historyCache
		shutdownWG    sync.WaitGroup
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

func newTaskProcessor(
	options taskProcessorOptions,
	shard ShardContext,
	historyCache *historyCache,
	logger log.Logger,
) *taskProcessor {

	log := logger.WithTags(tag.ComponentTimerQueue)

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
		logger:                  log,
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
	t.logger.Info("Timer queue task processor started.")
}

func (t *taskProcessor) stop() {
	close(t.shutdownCh)
	close(t.tasksCh)
	if success := common.AwaitWaitGroup(&t.workerWG, time.Minute); !success {
		t.logger.Warn("Timer queue task processor timedout on shutdown.")
	}
	t.logger.Info("Timer queue task processor shutdown.")
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
		return true
	}
	return false
}

func (t *taskProcessor) processTaskAndAck(
	notificationChan <-chan struct{},
	task *taskInfo,
) {

	var scope int
	var shouldProcessTask bool
	var err error
	startTime := t.timeSource.Now()
	logger := t.initializeLoggerForTask(task.task)
	attempt := 0
	incAttempt := func() {
		attempt++
		if attempt >= t.config.TimerTaskMaxRetryCount() {
			t.metricsClient.RecordTimer(scope, metrics.TaskAttemptTimer, time.Duration(attempt))
			logger.Error("Critical error processing timer task, retrying.", tag.Error(err), tag.OperationCritical)
		}
	}

FilterLoop:
	for {
		select {
		case <-t.shutdownCh:
			// this must return without ack
			return
		default:
			shouldProcessTask, err = task.processor.getTaskFilter()(task.task)
			if err == nil {
				break FilterLoop
			}
			incAttempt()
			time.Sleep(loadDomainEntryForTimerTaskRetryDelay)
		}
	}

	op := func() error {
		scope, err = t.processTaskOnce(notificationChan, task, shouldProcessTask, logger)
		return t.handleTaskError(scope, startTime, notificationChan, err, logger)
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
				t.ackTaskOnce(task, scope, shouldProcessTask, startTime, attempt)
				return
			}
			incAttempt()
		}
	}
}

func (t *taskProcessor) processTaskOnce(
	notificationChan <-chan struct{},
	task *taskInfo,
	shouldProcessTask bool,
	logger log.Logger,
) (int, error) {

	select {
	case <-notificationChan:
	default:
	}

	startTime := t.timeSource.Now()
	scope, err := task.processor.process(task.task, shouldProcessTask)
	if shouldProcessTask {
		t.metricsClient.IncCounter(scope, metrics.TaskRequests)
		t.metricsClient.RecordTimer(scope, metrics.TaskProcessingLatency, time.Since(startTime))
	}

	return scope, err
}

func (t *taskProcessor) handleTaskError(
	scope int,
	startTime time.Time,
	notificationChan <-chan struct{},
	err error,
	logger log.Logger,
) error {

	if err == nil {
		return nil
	}

	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		return nil
	}

	// this is a transient error
	if err == ErrTaskRetry {
		t.metricsClient.IncCounter(scope, metrics.TaskStandbyRetryCounter)
		<-notificationChan
		return err
	}

	if err == ErrTaskDiscarded {
		t.metricsClient.IncCounter(scope, metrics.TaskDiscarded)
		err = nil
	}

	// this is a transient error
	if _, ok := err.(*workflow.DomainNotActiveError); ok {
		if t.timeSource.Now().Sub(startTime) > cache.DomainCacheRefreshInterval {
			t.metricsClient.IncCounter(scope, metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	t.metricsClient.IncCounter(scope, metrics.TaskFailures)

	if _, ok := err.(*persistence.CurrentWorkflowConditionFailedError); ok {
		logger.Error("More than 2 workflow are running.", tag.Error(err), tag.LifeCycleProcessingFailed)
		return nil
	}

	logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (t *taskProcessor) ackTaskOnce(
	task *taskInfo,
	scope int,
	reportMetrics bool,
	startTime time.Time,
	attempt int,
) {

	task.processor.complete(task.task)
	if reportMetrics {
		t.metricsClient.RecordTimer(scope, metrics.TaskAttemptTimer, time.Duration(attempt))
		t.metricsClient.RecordTimer(scope, metrics.TaskLatency, time.Since(startTime))
		t.metricsClient.RecordTimer(
			scope,
			metrics.TaskQueueLatency,
			time.Since(task.task.GetVisibilityTimestamp()),
		)
	}
}

func (t *taskProcessor) initializeLoggerForTask(
	task queueTaskInfo,
) log.Logger {

	logger := t.logger.WithTags(
		tag.ShardID(t.shard.GetShardID()),
		tag.TaskID(task.GetTaskID()),
		tag.FailoverVersion(task.GetVersion()),
		tag.TaskType(task.GetTaskType()),
		tag.WorkflowDomainID(task.GetDomainID()),
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunID(task.GetRunID()),
	)

	switch task := task.(type) {
	case *persistence.TimerTaskInfo:
		logger = logger.WithTags(
			tag.WorkflowTimeoutType(int64(task.TimeoutType)),
		)
		logger.Debug(fmt.Sprintf("Processing timer task: %v, type: %v", task.GetTaskID(), task.GetTaskType()))

	case *persistence.TransferTaskInfo:
		logger.Debug(fmt.Sprintf("Processing transfer task: %v, type: %v", task.GetTaskID(), task.GetTaskType()))

	case *persistence.ReplicationTaskInfo:
		logger.Debug(fmt.Sprintf("Processing replication task: %v, type: %v", task.GetTaskID(), task.GetTaskType()))
	}

	return logger
}
