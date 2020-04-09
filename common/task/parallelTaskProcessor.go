package task

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	// ParallelTaskProcessorOptions configs PriorityTaskProcessor
	ParallelTaskProcessorOptions struct {
		QueueSize   int
		WorkerCount int
		RetryPolicy backoff.RetryPolicy
	}

	parallelTaskProcessorImpl struct {
		status       int32
		tasksCh      chan Task
		shutdownCh   chan struct{}
		workerWG     sync.WaitGroup
		logger       log.Logger
		metricsScope metrics.Scope
		options      *ParallelTaskProcessorOptions
	}
)

var (
	// ErrTaskProcessorClosed is the error returned when submiting task to a stopped processor
	ErrTaskProcessorClosed = errors.New("task processor has already shutdown")
)

// NewParallelTaskProcessor creates a new PriorityTaskProcessor
func NewParallelTaskProcessor(
	logger log.Logger,
	metricsScope metrics.Scope,
	options *ParallelTaskProcessorOptions,
) Processor {
	return &parallelTaskProcessorImpl{
		status:       common.DaemonStatusInitialized,
		tasksCh:      make(chan Task, options.QueueSize),
		shutdownCh:   make(chan struct{}),
		logger:       logger,
		metricsScope: metricsScope,
		options:      options,
	}
}

func (p *parallelTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.workerWG.Add(p.options.WorkerCount)
	for i := 0; i < p.options.WorkerCount; i++ {
		go p.taskWorker()
	}
	p.logger.Info("Parallel task processor started.")
}

func (p *parallelTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(p.shutdownCh)
	if success := common.AwaitWaitGroup(&p.workerWG, time.Minute); !success {
		p.logger.Warn("Parallel task processor timedout on shutdown.")
	}
	p.logger.Info("Parallel task processor shutdown.")
}

func (p *parallelTaskProcessorImpl) Submit(task Task) error {
	p.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
	sw := p.metricsScope.StartTimer(metrics.ParallelTaskSubmitLatency)
	defer sw.Stop()

	select {
	case p.tasksCh <- task:
		return nil
	case <-p.shutdownCh:
		return ErrTaskProcessorClosed
	}
}

func (p *parallelTaskProcessorImpl) taskWorker() {
	defer p.workerWG.Done()

	for {
		select {
		case <-p.shutdownCh:
			return
		case task := <-p.tasksCh:
			p.executeTask(task)
		}
	}
}

func (p *parallelTaskProcessorImpl) executeTask(task Task) {
	sw := p.metricsScope.StartTimer(metrics.ParallelTaskTaskProcessingLatency)
	defer sw.Stop()

	op := func() error {
		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		if p.isStopped() {
			return false
		}
		return task.RetryErr(err)
	}

	if err := backoff.Retry(op, p.options.RetryPolicy, isRetryable); err != nil {
		if p.isStopped() {
			// neither ack or nack here
			return
		}

		// non-retryable error or exhausted all retries
		task.Nack()
		return
	}

	// no error
	task.Ack()
}

func (p *parallelTaskProcessorImpl) isStopped() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStopped
}
