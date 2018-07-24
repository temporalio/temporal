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
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// QueueProcessorOptions is options passed to queue processor implementation
	QueueProcessorOptions struct {
		StartDelay                       dynamicconfig.DurationPropertyFn
		BatchSize                        dynamicconfig.IntPropertyFn
		WorkerCount                      dynamicconfig.IntPropertyFn
		MaxPollRPS                       dynamicconfig.IntPropertyFn
		MaxPollInterval                  dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		UpdateAckInterval                dynamicconfig.DurationPropertyFn
		MaxRetryCount                    dynamicconfig.IntPropertyFn
		MetricScope                      int
	}

	queueProcessorBase struct {
		clusterName   string
		shard         ShardContext
		options       *QueueProcessorOptions
		processor     processor
		logger        bark.Logger
		metricsClient metrics.Client
		rateLimiter   common.TokenBucket // Read rate limiter
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
		rateLimiter:             common.NewTokenBucket(options.MaxPollRPS(), common.NewRealTimeSource()),
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
	pollTimer := time.NewTimer(jitter.JitDuration(p.options.MaxPollInterval(), p.options.MaxPollIntervalJitterCoefficient()))
	defer pollTimer.Stop()

	updateAckTicker := time.NewTicker(p.options.UpdateAckInterval())
	defer updateAckTicker.Stop()

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
			pollTimer.Reset(jitter.JitDuration(p.options.MaxPollInterval(), p.options.MaxPollIntervalJitterCoefficient()))
			if p.lastPollTime.Add(p.options.MaxPollInterval()).Before(time.Now()) {
				p.processBatch(tasksCh)
			}
		case <-updateAckTicker.C:
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

	if !p.rateLimiter.Consume(1, p.options.MaxPollInterval()) {
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

	for _, tsk := range tasks {
		tasksCh <- tsk
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
			p.processWithRetry(notificationChan, task)
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

func (p *queueProcessorBase) processWithRetry(notificationChan <-chan struct{}, task queueTaskInfo) {

	var logger bark.Logger
	var err error
	startTime := time.Now()

	retryCount := 0
	op := func() error {
		err = p.processor.process(task)
		if err != nil && err != ErrTaskRetry {
			retryCount++
			logger = p.initializeLoggerForTask(task, logger)
			logging.LogTaskProcessingFailedEvent(logger, err)
		}
		return err
	}

ProcessRetryLoop:
	for retryCount < p.options.MaxRetryCount() {
		select {
		case <-p.shutdownCh:
			return
		default:
			// clear the existing notification
			select {
			case <-notificationChan:
			default:
			}

			err = backoff.Retry(op, p.retryPolicy, func(err error) bool {
				return err != ErrTaskRetry
			})

			if err != nil {
				if err == ErrTaskRetry {
					p.metricsClient.IncCounter(p.options.MetricScope, metrics.HistoryTaskStandbyRetryCounter)
					<-notificationChan
				} else if _, ok := err.(*workflow.DomainNotActiveError); ok && time.Now().Sub(startTime) > cache.DomainCacheRefreshInterval {
					p.metricsClient.IncCounter(p.options.MetricScope, metrics.HistoryTaskNotActiveCounter)
					return
				}
				continue ProcessRetryLoop
			}
			return
		}
	}

	// All attempts to process transfer task failed.  We won't be able to move the ackLevel so panic
	logger = p.initializeLoggerForTask(task, logger)
	switch task.(type) {
	case *persistence.TransferTaskInfo:
		// Cannot processes transfer task due to LimitExceededError after all retries
		// raise and alert and move on
		if _, ok := err.(*workflow.LimitExceededError); ok {
			logging.LogCriticalErrorEvent(logger, "Critical error processing transfer task.  Skipping.", err)
			p.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.CadenceCriticalFailures)
			return
		}

		logging.LogOperationPanicEvent(logger, "Retry count exceeded for transfer task", err)
	case *persistence.ReplicationTaskInfo:
		// Cannot processes replication task due to LimitExceededError after all retries
		// raise and alert and move on
		if _, ok := err.(*workflow.LimitExceededError); ok {
			logging.LogCriticalErrorEvent(logger, "Critical error processing replication task.  Skipping.", err)
			p.metricsClient.IncCounter(metrics.ReplicatorQueueProcessorScope, metrics.CadenceCriticalFailures)
			return
		}

		logging.LogOperationPanicEvent(logger, "Retry count exceeded for replication task", err)
	}
}

func (p *queueProcessorBase) initializeLoggerForTask(task queueTaskInfo, logger bark.Logger) bark.Logger {
	if logger != nil {
		return logger
	}

	logger = p.logger.WithFields(bark.Fields{
		logging.TagTaskID:   task.GetTaskID(),
		logging.TagTaskType: task.GetTaskType(),
		logging.TagVersion:  task.GetVersion(),
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
