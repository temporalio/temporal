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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/tokenbucket"
)

type (
	// QueueProcessorOptions is options passed to queue processor implementation
	QueueProcessorOptions struct {
		StartDelay                         dynamicconfig.DurationPropertyFn
		BatchSize                          dynamicconfig.IntPropertyFn
		WorkerCount                        dynamicconfig.IntPropertyFn
		MaxPollRPS                         dynamicconfig.IntPropertyFn
		MaxPollInterval                    dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
		UpdateAckInterval                  dynamicconfig.DurationPropertyFn
		UpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MaxRetryCount                      dynamicconfig.IntPropertyFn
		MetricScope                        int
	}

	queueProcessorBase struct {
		clusterName   string
		shard         ShardContext
		options       *QueueProcessorOptions
		processor     processor
		logger        bark.Logger
		metricsClient metrics.Client
		rateLimiter   tokenbucket.TokenBucket // Read rate limiter
		ackMgr        queueAckMgr
		retryPolicy   backoff.RetryPolicy

		// worker coroutines notification
		workerNotificationChans []chan struct{}

		lastPollTime time.Time

		notifyCh   chan struct{}
		status     int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}
	}
)

var (
	errUnexpectedQueueTask = errors.New("unexpected queue task")

	loadDomainEntryForQueueTaskRetryDelay = 100 * time.Millisecond
	loadQueueTaskThrottleRetryDelay       = 5 * time.Second
)

func newQueueProcessorBase(clusterName string, shard ShardContext, options *QueueProcessorOptions, processor processor, queueAckMgr queueAckMgr, logger bark.Logger) *queueProcessorBase {
	workerNotificationChans := []chan struct{}{}
	for index := 0; index < options.WorkerCount(); index++ {
		workerNotificationChans = append(workerNotificationChans, make(chan struct{}, 1))
	}

	p := &queueProcessorBase{
		clusterName:             clusterName,
		shard:                   shard,
		options:                 options,
		processor:               processor,
		rateLimiter:             tokenbucket.New(options.MaxPollRPS(), clock.NewRealTimeSource()),
		workerNotificationChans: workerNotificationChans,
		status:                  common.DaemonStatusInitialized,
		notifyCh:                make(chan struct{}, 1),
		shutdownCh:              make(chan struct{}),
		metricsClient:           shard.GetMetricsClient(),
		logger:                  logger,
		ackMgr:                  queueAckMgr,
		retryPolicy:             common.CreatePersistanceRetryPolicy(),
		lastPollTime:            time.Time{},
	}

	return p
}

func (p *queueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logging.LogQueueProcesorStartingEvent(p.logger)
	defer logging.LogQueueProcesorStartedEvent(p.logger)

	p.shutdownWG.Add(1)
	p.notifyNewTask()
	go p.processorPump()
}

func (p *queueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	logging.LogQueueProcesorShuttingDownEvent(p.logger)
	defer logging.LogQueueProcesorShutdownEvent(p.logger)

	close(p.shutdownCh)
	p.retryTasks()

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		logging.LogQueueProcesorShutdownTimedoutEvent(p.logger)
	}
}

func (p *queueProcessorBase) notifyNewTask() {
	var event struct{}
	select {
	case p.notifyCh <- event:
	default: // channel already has an event, don't block
	}
}

func (p *queueProcessorBase) processorPump() {
	<-time.NewTimer(backoff.NewJitter().JitDuration(p.options.StartDelay(), 0.99)).C

	defer p.shutdownWG.Done()
	tasksCh := make(chan queueTaskInfo, p.options.BatchSize())

	var workerWG sync.WaitGroup
	for i := 0; i < p.options.WorkerCount(); i++ {
		workerWG.Add(1)
		notificationChan := p.workerNotificationChans[i]
		go p.taskWorker(tasksCh, notificationChan, &workerWG)
	}

	jitter := backoff.NewJitter()
	pollTimer := time.NewTimer(jitter.JitDuration(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	updateAckTimer := time.NewTimer(jitter.JitDuration(
		p.options.UpdateAckInterval(),
		p.options.UpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

processorPumpLoop:
	for {
		select {
		case <-p.shutdownCh:
			break processorPumpLoop
		case <-p.ackMgr.getFinishedChan():
			// use a separate gorouting since the caller hold the shutdownWG
			go p.Stop()
		case <-p.notifyCh:
			p.processBatch(tasksCh)
		case <-pollTimer.C:
			pollTimer.Reset(jitter.JitDuration(
				p.options.MaxPollInterval(),
				p.options.MaxPollIntervalJitterCoefficient(),
			))
			if p.lastPollTime.Add(p.options.MaxPollInterval()).Before(time.Now()) {
				p.processBatch(tasksCh)
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(jitter.JitDuration(
				p.options.UpdateAckInterval(),
				p.options.UpdateAckIntervalJitterCoefficient(),
			))
			p.ackMgr.updateQueueAckLevel()
		}
	}

	p.logger.Info("Queue processor pump shutting down.")
	// This is the only pump which writes to tasksCh, so it is safe to close channel here
	close(tasksCh)
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		p.logger.Warn("Queue processor timedout on worker shutdown.")
	}

}

func (p *queueProcessorBase) processBatch(tasksCh chan<- queueTaskInfo) {

	if !p.rateLimiter.Consume(1, loadQueueTaskThrottleRetryDelay) {
		p.notifyNewTask() // re-enqueue the event
		return
	}

	p.lastPollTime = time.Now()
	tasks, more, err := p.ackMgr.readQueueTasks()

	if err != nil {
		p.logger.Warnf("Processor unable to retrieve tasks: %v", err)
		p.notifyNewTask() // re-enqueue the event
		return
	}

	if len(tasks) == 0 {
		return
	}

	for _, task := range tasks {
		select {
		case tasksCh <- task:
		case <-p.shutdownCh:
			return
		}
	}

	if more {
		// There might be more task
		// We return now to yield, but enqueue an event to poll later
		p.notifyNewTask()
	}

	return
}

func (p *queueProcessorBase) taskWorker(tasksCh <-chan queueTaskInfo, notificationChan <-chan struct{}, workerWG *sync.WaitGroup) {
	defer workerWG.Done()

	for {
		select {
		case <-p.shutdownCh:
			return
		case task, ok := <-tasksCh:
			if !ok {
				return
			}
			p.processTaskAndAck(notificationChan, task)
		}
	}
}

func (p *queueProcessorBase) retryTasks() {
	for _, workerNotificationChan := range p.workerNotificationChans {
		select {
		case workerNotificationChan <- struct{}{}:
		default:
		}
	}
}

func (p *queueProcessorBase) processTaskAndAck(notificationChan <-chan struct{}, task queueTaskInfo) {

	var scope int
	var shouldProcessTask bool
	var err error
	startTime := time.Now()
	logger := p.initializeLoggerForTask(task)
	attempt := 0
	incAttempt := func() {
		attempt++
		if attempt >= p.options.MaxRetryCount() {
			p.metricsClient.RecordTimer(scope, metrics.TaskAttemptTimer, time.Duration(attempt))
			switch task.(type) {
			case *persistence.TransferTaskInfo:
				logging.LogCriticalErrorEvent(logger, "Critical error processing transfer task, retrying.", err)
			case *persistence.ReplicationTaskInfo:
				logging.LogCriticalErrorEvent(logger, "Critical error processing replication task, retrying.", err)
			}
		}
	}

FilterLoop:
	for {
		select {
		case <-p.shutdownCh:
			// this must return without ack
			return
		default:
			shouldProcessTask, err = p.processor.getTaskFilter()(task)
			if err == nil {
				break FilterLoop
			}
			incAttempt()
			time.Sleep(loadDomainEntryForQueueTaskRetryDelay)
		}
	}

	op := func() error {
		scope, err = p.processTaskOnce(notificationChan, task, shouldProcessTask, logger)
		return p.handleTaskError(scope, startTime, notificationChan, err, logger)
	}
	retryCondition := func(err error) bool {
		select {
		case <-p.shutdownCh:
			return false
		default:
			return true
		}
	}

	for {
		select {
		case <-p.shutdownCh:
			// this must return without ack
			return
		default:
			err = backoff.Retry(op, p.retryPolicy, retryCondition)
			if err == nil {
				p.ackTaskOnce(task, scope, shouldProcessTask, startTime, attempt)
				return
			}
			incAttempt()
		}
	}
}

func (p *queueProcessorBase) processTaskOnce(notificationChan <-chan struct{}, task queueTaskInfo, shouldProcessTask bool, logger bark.Logger) (int, error) {
	select {
	case <-notificationChan:
	default:
	}

	startTime := time.Now()
	scope, err := p.processor.process(task, shouldProcessTask)
	if shouldProcessTask {
		p.metricsClient.IncCounter(scope, metrics.TaskRequests)
		p.metricsClient.RecordTimer(scope, metrics.TaskProcessingLatency, time.Since(startTime))
	}
	return scope, err
}

func (p *queueProcessorBase) handleTaskError(scope int, startTime time.Time,
	notificationChan <-chan struct{}, err error, logger bark.Logger) error {

	if err == nil {
		return nil
	}

	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		return nil
	}

	// this is a transient error
	if err == ErrTaskRetry {
		p.metricsClient.IncCounter(scope, metrics.TaskStandbyRetryCounter)
		<-notificationChan
		return err
	}

	if err == ErrTaskDiscarded {
		p.metricsClient.IncCounter(scope, metrics.TaskDiscarded)
		err = nil
	}

	// this is a transient error
	if _, ok := err.(*workflow.DomainNotActiveError); ok {
		if time.Now().Sub(startTime) > cache.DomainCacheRefreshInterval {
			p.metricsClient.IncCounter(scope, metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	p.metricsClient.IncCounter(scope, metrics.TaskFailures)

	if _, ok := err.(*persistence.CurrentWorkflowConditionFailedError); ok {
		logging.LogTaskProcessingFailedEvent(logger, "More than 2 workflow are running.", err)
		return nil
	}

	if _, ok := err.(*workflow.LimitExceededError); ok {
		p.metricsClient.IncCounter(scope, metrics.TaskLimitExceededCounter)
		logging.LogTaskProcessingFailedEvent(logger, "Task encounter limit exceeded error.", err)
		return err
	}

	logging.LogTaskProcessingFailedEvent(logger, "Fail to process task", err)
	return err
}

func (p *queueProcessorBase) ackTaskOnce(task queueTaskInfo, scope int, reportMetrics bool, startTime time.Time, attempt int) {
	p.ackMgr.completeQueueTask(task.GetTaskID())
	if reportMetrics {
		p.metricsClient.RecordTimer(scope, metrics.TaskAttemptTimer, time.Duration(attempt))
		p.metricsClient.RecordTimer(scope, metrics.TaskLatency, time.Since(startTime))
		p.metricsClient.RecordTimer(
			scope,
			metrics.TaskQueueLatency,
			time.Since(task.GetVisibilityTimestamp()),
		)
	}
}

func (p *queueProcessorBase) initializeLoggerForTask(task queueTaskInfo) bark.Logger {
	logger := p.logger.WithFields(bark.Fields{
		logging.TagHistoryShardID: p.shard.GetShardID(),
		logging.TagTaskID:         task.GetTaskID(),
		logging.TagTaskType:       task.GetTaskType(),
		logging.TagVersion:        task.GetVersion(),
	})

	switch task := task.(type) {
	case *persistence.TransferTaskInfo:
		logger = logger.WithFields(bark.Fields{
			logging.TagDomainID:            task.DomainID,
			logging.TagWorkflowExecutionID: task.WorkflowID,
			logging.TagWorkflowRunID:       task.RunID,
		})

		logger.Debug("Processing transfer task")
	case *persistence.ReplicationTaskInfo:
		logger = logger.WithFields(bark.Fields{
			logging.TagDomainID:            task.DomainID,
			logging.TagWorkflowExecutionID: task.WorkflowID,
			logging.TagWorkflowRunID:       task.RunID,
		})

		logger.Debug("Processing replication task")
	}

	return logger
}
