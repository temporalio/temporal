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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
)

type (
	// QueueProcessorOptions is options passed to queue processor implementation
	QueueProcessorOptions struct {
		BatchSize           int
		WorkerCount         int
		MaxPollRPS          int
		MaxPollInterval     time.Duration
		UpdateAckInterval   time.Duration
		ForceUpdateInterval time.Duration
		MaxRetryCount       int
		MetricScope         int
	}

	queueProcessorBase struct {
		shard         ShardContext
		options       *QueueProcessorOptions
		processor     processor
		logger        bark.Logger
		metricsClient metrics.Client
		rateLimiter   common.TokenBucket // Read rate limiter
		ackMgr        queueAckMgr

		// worker coroutines notification
		workerNotificationChans []chan struct{}

		notifyCh   chan struct{}
		isStarted  int32
		isStopped  int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}
	}
)

var (
	errUnexpectedQueueTask = errors.New("unexpected queue task")
)

func newQueueProcessorBase(shard ShardContext, options *QueueProcessorOptions, processor processor, queueAckMgr queueAckMgr, logger bark.Logger) *queueProcessorBase {
	workerNotificationChans := []chan struct{}{}
	for index := 0; index < options.WorkerCount; index++ {
		workerNotificationChans = append(workerNotificationChans, make(chan struct{}, 1))
	}

	p := &queueProcessorBase{
		shard:                   shard,
		options:                 options,
		processor:               processor,
		rateLimiter:             common.NewTokenBucket(options.MaxPollRPS, common.NewRealTimeSource()),
		workerNotificationChans: workerNotificationChans,
		notifyCh:                make(chan struct{}, 1),
		shutdownCh:              make(chan struct{}),
		metricsClient:           shard.GetMetricsClient(),
		logger:                  logger,
		ackMgr:                  queueAckMgr,
	}

	return p
}

func (p *queueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return
	}

	logging.LogQueueProcesorStartingEvent(p.logger)
	defer logging.LogQueueProcesorStartedEvent(p.logger)

	p.shutdownWG.Add(1)
	p.notifyNewTask()
	go p.processorPump()
}

func (p *queueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&p.isStopped, 0, 1) {
		return
	}

	logging.LogQueueProcesorShuttingDownEvent(p.logger)
	defer logging.LogQueueProcesorShutdownEvent(p.logger)

	if atomic.LoadInt32(&p.isStarted) == 1 {
		close(p.shutdownCh)
	}

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
	defer p.shutdownWG.Done()
	tasksCh := make(chan queueTaskInfo, p.options.BatchSize)

	var workerWG sync.WaitGroup
	for i := 0; i < p.options.WorkerCount; i++ {
		workerWG.Add(1)
		notificationChan := p.workerNotificationChans[i]
		go p.taskWorker(tasksCh, notificationChan, &workerWG)
	}

	pollTimer := time.NewTimer(p.options.MaxPollInterval)
	updateAckTimer := time.NewTimer(p.options.UpdateAckInterval)

processorPumpLoop:
	for {
		select {
		case <-p.shutdownCh:
			break processorPumpLoop
		case <-p.ackMgr.getFinishedChan():
			p.Stop()
		case <-p.notifyCh:
			p.processBatch(tasksCh)
		case <-pollTimer.C:
			p.processBatch(tasksCh)
			pollTimer.Reset(p.options.MaxPollInterval)
		case <-updateAckTimer.C:
			p.ackMgr.updateAckLevel()
			updateAckTimer = time.NewTimer(p.options.UpdateAckInterval)
		}
	}

	p.logger.Info("Queue processor pump shutting down.")
	// This is the only pump which writes to tasksCh, so it is safe to close channel here
	close(tasksCh)
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		p.logger.Warn("Queue processor timed out on worker shutdown.")
	}
	updateAckTimer.Stop()
	pollTimer.Stop()
}

func (p *queueProcessorBase) processBatch(tasksCh chan<- queueTaskInfo) {

	if !p.rateLimiter.Consume(1, p.options.MaxPollInterval) {
		p.notifyNewTask() // re-enqueue the event
		return
	}

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
	p.logger.Debugf("Processing task: %v, type: %v", task.GetTaskID(), task.GetTaskType())
ProcessRetryLoop:
	for retryCount := 1; retryCount <= p.options.MaxRetryCount; {
		select {
		case <-p.shutdownCh:
			return
		default:
			// clear the existing notification
			select {
			case <-notificationChan:
			default:
			}

			err := p.processor.process(task)
			if err != nil {
				if err == ErrTaskRetry {
					<-notificationChan
				} else {
					logging.LogTaskProcessingFailedEvent(p.logger, task.GetTaskID(), task.GetTaskType(), err)
					backoff := time.Duration(retryCount * 100)
					time.Sleep(backoff * time.Millisecond)
					retryCount++
				}
				continue ProcessRetryLoop
			}
			return
		}
	}

	// All attempts to process transfer task failed.  We won't be able to move the ackLevel so panic
	logging.LogOperationPanicEvent(p.logger,
		fmt.Sprintf("Retry count exceeded for taskID: %v", task.GetTaskID()), nil)
}
