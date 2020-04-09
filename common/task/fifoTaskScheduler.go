package task

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	// FIFOTaskSchedulerOptions configs FIFO task scheduler
	FIFOTaskSchedulerOptions struct {
		QueueSize   int
		WorkerCount int
		RetryPolicy backoff.RetryPolicy
	}

	fifoTaskSchedulerImpl struct {
		status       int32
		logger       log.Logger
		metricsScope metrics.Scope
		dispatcherWG sync.WaitGroup
		taskCh       chan PriorityTask
		shutdownCh   chan struct{}

		processor Processor
	}
)

// NewFIFOTaskScheduler creates a new FIFO task scheduler
// it's an no-op implementation as it simply copy tasks from
// one task channel to another task channel.
// This scheduler is only for development purpose.
func NewFIFOTaskScheduler(
	logger log.Logger,
	metricsScope metrics.Scope,
	options *FIFOTaskSchedulerOptions,
) Scheduler {
	return &fifoTaskSchedulerImpl{
		status:       common.DaemonStatusInitialized,
		logger:       logger,
		metricsScope: metricsScope,
		taskCh:       make(chan PriorityTask, options.QueueSize),
		shutdownCh:   make(chan struct{}),
		processor: NewParallelTaskProcessor(
			logger,
			metricsScope,
			&ParallelTaskProcessorOptions{
				QueueSize:   options.QueueSize,
				WorkerCount: options.WorkerCount,
				RetryPolicy: options.RetryPolicy,
			},
		),
	}
}

func (f *fifoTaskSchedulerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	f.processor.Start()

	f.dispatcherWG.Add(1)
	go f.dispatcher()

	f.logger.Info("FIFO task scheduler started.")
}

func (f *fifoTaskSchedulerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(f.shutdownCh)

	f.processor.Stop()

	if success := common.AwaitWaitGroup(&f.dispatcherWG, time.Minute); !success {
		f.logger.Warn("FIFO task scheduler timedout on shutdown.")
	}

	f.logger.Info("FIFO task scheduler shutdown.")
}

func (f *fifoTaskSchedulerImpl) Submit(
	task PriorityTask,
) error {
	f.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
	sw := f.metricsScope.StartTimer(metrics.ParallelTaskSubmitLatency)
	defer sw.Stop()

	select {
	case f.taskCh <- task:
		return nil
	case <-f.shutdownCh:
		return ErrTaskSchedulerClosed
	}
}

func (f *fifoTaskSchedulerImpl) TrySubmit(
	task PriorityTask,
) (bool, error) {
	select {
	case f.taskCh <- task:
		f.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
		return true, nil
	case <-f.shutdownCh:
		return false, ErrTaskSchedulerClosed
	default:
		return false, nil
	}
}

func (f *fifoTaskSchedulerImpl) dispatcher() {
	defer f.dispatcherWG.Done()

	for {
		select {
		case task := <-f.taskCh:
			if err := f.processor.Submit(task); err != nil {
				f.logger.Error("failed to submit task to processor", tag.Error(err))
				task.Nack()
			}
		case <-f.shutdownCh:
			return
		}
	}
}
