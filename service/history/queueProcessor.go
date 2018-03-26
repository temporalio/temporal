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
	"github.com/uber/cadence/common/backoff"
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
		ackMgr        *ackManager

		notifyCh   chan struct{}
		isStarted  int32
		isStopped  int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}
	}

	// TODO: Move AckManager to its own file
	// ackManager is created by QueueProcessor to keep track of the queue ackLevel for the shard.
	// It keeps track of read level when dispatching tasks to processor and maintains a map of outstanding tasks.
	// Outstanding tasks map uses the task id sequencer as the key, which is used by updateAckLevel to move the ack level
	// for the shard when all preceding tasks are acknowledged.
	ackManager struct {
		shard         ShardContext
		options       *QueueProcessorOptions
		processor     processor
		logger        bark.Logger
		metricsClient metrics.Client
		lastUpdated   time.Time

		sync.RWMutex
		outstandingTasks map[int64]bool
		readLevel        int64
		maxReadLevel     int64
		ackLevel         int64
	}
)

var (
	errUnexpectedQueueTask = errors.New("unexpected queue task")
)

func newQueueProcessor(shard ShardContext, options *QueueProcessorOptions, processor processor,
	ackLevel int64) *queueProcessorBase {
	logger := shard.GetLogger()
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: processor.GetName(),
	})

	p := &queueProcessorBase{
		shard:         shard,
		options:       options,
		processor:     processor,
		rateLimiter:   common.NewTokenBucket(options.MaxPollRPS, common.NewRealTimeSource()),
		notifyCh:      make(chan struct{}, 1),
		shutdownCh:    make(chan struct{}),
		metricsClient: shard.GetMetricsClient(),
		logger:        logger,
	}
	p.ackMgr = newAckManager(shard, options, processor, ackLevel, logger)

	return p
}

func newAckManager(shard ShardContext, options *QueueProcessorOptions, processor processor, ackLevel int64,
	logger bark.Logger) *ackManager {

	return &ackManager{
		shard:            shard,
		options:          options,
		processor:        processor,
		outstandingTasks: make(map[int64]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		logger:           logger,
		metricsClient:    shard.GetMetricsClient(),
		lastUpdated:      time.Now(),
	}
}

func (p *queueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return
	}

	logging.LogQueueProcesorStartingEvent(p.logger)
	defer logging.LogQueueProcesorStartedEvent(p.logger)

	p.shutdownWG.Add(1)
	p.NotifyNewTask()
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

func (p *queueProcessorBase) NotifyNewTask() {
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
		go p.taskWorker(tasksCh, &workerWG)
	}

	pollTimer := time.NewTimer(p.options.MaxPollInterval)
	updateAckTimer := time.NewTimer(p.options.UpdateAckInterval)

processorPumpLoop:
	for {
		select {
		case <-p.shutdownCh:
			break processorPumpLoop
		case <-p.notifyCh:
			p.processBatch(tasksCh)
		case <-pollTimer.C:
			p.processBatch(tasksCh)
			pollTimer = time.NewTimer(p.options.MaxPollInterval)
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
		p.NotifyNewTask() // re-enqueue the event
		return
	}

	tasks, err := p.ackMgr.readTransferTasks()

	if err != nil {
		p.logger.Warnf("Processor unable to retrieve tasks: %v", err)
		p.NotifyNewTask() // re-enqueue the event
		return
	}

	if len(tasks) == 0 {
		return
	}

	for _, tsk := range tasks {
		tasksCh <- tsk
	}

	if len(tasks) == p.options.BatchSize {
		// There might be more task
		// We return now to yield, but enqueue an event to poll later
		p.NotifyNewTask()
	}

	return
}

func (p *queueProcessorBase) taskWorker(tasksCh <-chan queueTaskInfo, workerWG *sync.WaitGroup) {
	defer workerWG.Done()

	for {
		select {
		case task, ok := <-tasksCh:
			if !ok {
				return
			}

			p.processWithRetry(task)
		}
	}
}

func (p *queueProcessorBase) processWithRetry(task queueTaskInfo) {
	p.logger.Debugf("Processing task: %v, type: %v", task.GetTaskID(), task.GetTaskType())
ProcessRetryLoop:
	for retryCount := 1; retryCount <= p.options.MaxRetryCount; retryCount++ {
		select {
		case <-p.shutdownCh:
			return
		default:
			err := p.processor.Process(task)

			if err != nil {
				logging.LogTaskProcessingFailedEvent(p.logger, task.GetTaskID(), task.GetTaskType(), err)
				backoff := time.Duration(retryCount * 100)
				time.Sleep(backoff * time.Millisecond)
				continue ProcessRetryLoop
			}

			p.ackMgr.completeTask(task.GetTaskID())
			return
		}
	}

	// All attempts to process transfer task failed.  We won't be able to move the ackLevel so panic
	logging.LogOperationPanicEvent(p.logger,
		fmt.Sprintf("Retry count exceeded for taskID: %v", task.GetTaskID()), nil)
}

func (a *ackManager) readTransferTasks() ([]queueTaskInfo, error) {
	a.RLock()
	rLevel := a.readLevel
	a.RUnlock()

	var tasks []queueTaskInfo
	op := func() error {
		var err error
		tasks, err = a.processor.ReadTasks(rLevel)
		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return tasks, nil
	}

	a.Lock()
	for _, task := range tasks {
		if a.readLevel >= task.GetTaskID() {
			a.logger.Fatalf("Next task ID is less than current read level.  TaskID: %v, ReadLevel: %v", task.GetTaskID(),
				a.readLevel)
		}
		a.logger.Debugf("Moving read level: %v", task.GetTaskID())
		a.readLevel = task.GetTaskID()
		a.outstandingTasks[a.readLevel] = false
	}
	a.Unlock()

	return tasks, nil
}

func (a *ackManager) completeTask(taskID int64) {
	a.Lock()
	if _, ok := a.outstandingTasks[taskID]; ok {
		a.outstandingTasks[taskID] = true
	}
	a.Unlock()
}

func (a *ackManager) updateAckLevel() {
	a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateCounter)
	initialAckLevel := a.ackLevel
	updatedAckLevel := a.ackLevel
	a.Lock()
MoveAckLevelLoop:
	for current := a.ackLevel + 1; current <= a.readLevel; current++ {
		// TODO: What happens if !ok?
		if acked, ok := a.outstandingTasks[current]; ok {
			if acked {
				err := a.processor.CompleteTask(current)

				if err != nil {
					a.logger.Warnf("Processor unable to complete task '%v': %v", current, err)
					break MoveAckLevelLoop
				}
				a.logger.Debugf("Updating ack level: %v", current)
				a.ackLevel = current
				updatedAckLevel = current
				delete(a.outstandingTasks, current)
			} else {
				break MoveAckLevelLoop
			}
		}
	}
	a.Unlock()

	// Do not update Acklevel if nothing changed upto force update interval
	if initialAckLevel == updatedAckLevel && time.Since(a.lastUpdated) < a.options.ForceUpdateInterval {
		return
	}

	// Always update ackLevel to detect if the shared is stolen
	if err := a.shard.UpdateTransferAckLevel(updatedAckLevel); err != nil {
		a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateFailedCounter)
		logging.LogOperationFailedEvent(a.logger, "Error updating ack level for shard", err)
	} else {
		a.lastUpdated = time.Now()
	}
}
