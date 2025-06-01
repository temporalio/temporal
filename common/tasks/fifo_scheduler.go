package tasks

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	defaultMonitorTickerDuration = time.Minute
	defaultMonitorTickerJitter   = 0.15
)

var _ Scheduler[Task] = (*FIFOScheduler[Task])(nil)

type (
	// FIFOSchedulerOptions is the configs for FIFOScheduler
	FIFOSchedulerOptions struct {
		QueueSize   int
		WorkerCount dynamicconfig.TypedSubscribable[int]
	}

	FIFOScheduler[T Task] struct {
		status  int32
		options *FIFOSchedulerOptions

		logger log.Logger

		tasksChan  chan T
		shutdownWG sync.WaitGroup

		workerLock       sync.Mutex
		workerShutdownCh []chan struct{}

		workerCountSubscriptionCancelFn func()
	}
)

// NewFIFOScheduler creates a new FIFOScheduler
func NewFIFOScheduler[T Task](
	options *FIFOSchedulerOptions,
	logger log.Logger,
) *FIFOScheduler[T] {
	return &FIFOScheduler[T]{
		status:  common.DaemonStatusInitialized,
		options: options,

		logger: logger,

		tasksChan: make(chan T, options.QueueSize),
	}
}

func (f *FIFOScheduler[T]) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	initialWorkerCount, workerCountSubscriptionCancelFn := f.options.WorkerCount(f.updateWorkerCount)
	f.workerCountSubscriptionCancelFn = workerCountSubscriptionCancelFn
	f.updateWorkerCount(initialWorkerCount)

	f.logger.Info("fifo scheduler started")
}

func (f *FIFOScheduler[T]) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	f.workerCountSubscriptionCancelFn()
	f.updateWorkerCount(0)
	f.drainTasks()

	go func() {
		if success := common.AwaitWaitGroup(&f.shutdownWG, time.Minute); !success {
			f.logger.Warn("fifo scheduler timed out waiting for workers")
		}
	}()
	f.logger.Info("fifo scheduler stopped")
}

func (f *FIFOScheduler[T]) Submit(task T) {
	f.tasksChan <- task
	if f.isStopped() {
		f.drainTasks()
	}
}

func (f *FIFOScheduler[T]) TrySubmit(task T) bool {
	select {
	case f.tasksChan <- task:
		if f.isStopped() {
			f.drainTasks()
		}
		return true
	default:
		return false
	}
}

func (f *FIFOScheduler[T]) updateWorkerCount(targetWorkerNum int) {
	f.workerLock.Lock()
	defer f.workerLock.Unlock()

	if f.isStopped() {
		// Always set the value to 0 when scheduler is stopped,
		// in case there's a race condition between subscription callback invocation
		// and the invocation made from Stop()
		targetWorkerNum = 0
	}

	if targetWorkerNum < 0 {
		f.logger.Error("Target worker pool size is negative. Please fix the dynamic config.", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
		return
	}

	currentWorkerNum := len(f.workerShutdownCh)
	if targetWorkerNum == currentWorkerNum {
		return
	}

	if targetWorkerNum > currentWorkerNum {
		f.startWorkers(targetWorkerNum - currentWorkerNum)
	} else {
		f.stopWorkers(currentWorkerNum - targetWorkerNum)
	}

	f.logger.Info("Update worker pool size", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
}

func (f *FIFOScheduler[T]) startWorkers(
	count int,
) {
	for i := 0; i < count; i++ {
		shutdownCh := make(chan struct{})
		f.workerShutdownCh = append(f.workerShutdownCh, shutdownCh)

		f.shutdownWG.Add(1)
		go f.processTask(shutdownCh)
	}
}

func (f *FIFOScheduler[T]) stopWorkers(
	count int,
) {
	shutdownChToClose := f.workerShutdownCh[:count]
	f.workerShutdownCh = f.workerShutdownCh[count:]

	for _, shutdownCh := range shutdownChToClose {
		close(shutdownCh)
	}
}

func (f *FIFOScheduler[T]) processTask(
	shutdownCh chan struct{},
) {
	defer f.shutdownWG.Done()

	for {
		if f.isStopped() {
			return
		}

		select {
		case <-shutdownCh:
			return
		default:
		}

		select {
		case task := <-f.tasksChan:
			f.executeTask(task)

		case <-shutdownCh:
			return
		}
	}
}

func (f *FIFOScheduler[T]) executeTask(
	task T,
) {
	operation := func() error {
		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		return !f.isStopped() && task.IsRetryableError(err)
	}

	if err := backoff.ThrottleRetry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if f.isStopped() {
			task.Abort()
			return
		}

		task.Nack(err)
		return
	}

	task.Ack()
}

func (f *FIFOScheduler[T]) drainTasks() {
LoopDrain:
	for {
		select {
		case task := <-f.tasksChan:
			task.Abort()
		default:
			break LoopDrain
		}
	}
}

func (f *FIFOScheduler[T]) isStopped() bool {
	return atomic.LoadInt32(&f.status) == common.DaemonStatusStopped
}
