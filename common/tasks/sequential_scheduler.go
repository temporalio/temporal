package tasks

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ Scheduler[Task] = (*SequentialScheduler[Task])(nil)

type (
	SequentialSchedulerOptions struct {
		QueueSize   int
		WorkerCount dynamicconfig.TypedSubscribable[int]
	}

	SequentialScheduler[T Task] struct {
		status       int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		workerLock       sync.Mutex
		workerShutdownCh []chan struct{}

		workerCountSubscriptionCancelFn func()

		options      *SequentialSchedulerOptions
		queues       collection.ConcurrentTxMap
		queueFactory SequentialTaskQueueFactory[T]
		queueChan    chan SequentialTaskQueue[T]

		logger log.Logger

		// Metrics fields
		metricsHandler        metrics.Handler
		metricTagsFn          MetricTagsFn[T]
		timeSource            clock.TimeSource
		assignedWorkerCount   int64
	}
)

func NewSequentialScheduler[T Task](
	options *SequentialSchedulerOptions,
	taskQueueHashFn collection.HashFunc,
	taskQueueFactory SequentialTaskQueueFactory[T],
	logger log.Logger,
	metricsHandler metrics.Handler,
	metricTagsFn MetricTagsFn[T],
) *SequentialScheduler[T] {
	return &SequentialScheduler[T]{
		status:       common.DaemonStatusInitialized,
		shutdownChan: make(chan struct{}),
		options:      options,

		logger: logger,

		queueFactory: taskQueueFactory,
		queueChan:    make(chan SequentialTaskQueue[T], options.QueueSize),
		queues:       collection.NewShardedConcurrentTxMap(1024, taskQueueHashFn),

		metricsHandler:      metricsHandler,
		metricTagsFn:        metricTagsFn,
		timeSource:          clock.NewRealTimeSource(),
	}
}

func (s *SequentialScheduler[T]) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	initialWorkerCount, workerCountSubscriptionCancelFn := s.options.WorkerCount(s.updateWorkerCount)
	s.workerCountSubscriptionCancelFn = workerCountSubscriptionCancelFn
	s.updateWorkerCount(initialWorkerCount)

	if s.metricsHandler != nil {
		s.shutdownWG.Add(1)
		go s.exportMetricsLoop()
	}

	s.logger.Info("sequential scheduler started")
}

func (s *SequentialScheduler[T]) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(s.shutdownChan)
	s.workerCountSubscriptionCancelFn()
	s.updateWorkerCount(0)
	// must be called after the close of the shutdownChan
	s.drainTasks()

	go func() {
		if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
			s.logger.Warn("sequential scheduler timed out waiting for workers")
		}
	}()
	s.logger.Info("sequential scheduler stopped")
}

func (s *SequentialScheduler[T]) Submit(task T) {
	s.setEnqueueTime(task)
	s.recordTaskSubmitted(task)

	queue := s.queueFactory(task)
	queue.Add(task)

	_, fnEvaluated, err := s.queues.PutOrDo(
		queue.ID(),
		queue,
		func(key interface{}, value interface{}) error {
			value.(SequentialTaskQueue[T]).Add(task)
			return nil
		},
	)
	if err != nil {
		panic("Error is not expected as the evaluation function returns nil")
	}

	// if function evaluated, meaning that the task set is
	// already dispatched
	if fnEvaluated {
		if s.isStopped() {
			s.drainTasks()
		}
		return
	}

	// need to dispatch this task set
	select {
	case <-s.shutdownChan:
		task.Abort()
	case s.queueChan <- queue:
		if s.isStopped() {
			s.drainTasks()
		}
	}
}

func (s *SequentialScheduler[T]) TrySubmit(task T) bool {
	s.setEnqueueTime(task)

	queue := s.queueFactory(task)
	queue.Add(task)

	_, fnEvaluated, err := s.queues.PutOrDo(
		queue.ID(),
		queue,
		func(key interface{}, value interface{}) error {
			value.(SequentialTaskQueue[T]).Add(task)
			return nil
		},
	)
	if err != nil {
		panic("Error is not expected as the evaluation function returns nil")
	}
	if fnEvaluated {
		s.recordTaskSubmitted(task)
		if s.isStopped() {
			s.drainTasks()
		}
		return true
	}

	select {
	case s.queueChan <- queue:
		s.recordTaskSubmitted(task)
		if s.isStopped() {
			s.drainTasks()
		}
		return true
	default:
		return false
	}
}

func (s *SequentialScheduler[T]) updateWorkerCount(targetWorkerNum int) {
	s.workerLock.Lock()
	defer s.workerLock.Unlock()

	if s.isStopped() {
		// Always set the value to 0 when scheduler is stopped,
		// in case there's a race condition between subscription callback invocation
		// and the invocation made from Stop()
		targetWorkerNum = 0
	}

	if targetWorkerNum < 0 {
		s.logger.Error("Target worker pool size is negative. Please fix the dynamic config.", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
		return
	}

	currentWorkerNum := len(s.workerShutdownCh)
	if targetWorkerNum == currentWorkerNum {
		return
	}

	if targetWorkerNum > currentWorkerNum {
		s.startWorkers(targetWorkerNum - currentWorkerNum)
	} else {
		s.stopWorkers(currentWorkerNum - targetWorkerNum)
	}

	s.logger.Info("Update worker pool size", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
}

func (s *SequentialScheduler[T]) startWorkers(
	count int,
) {
	for i := 0; i < count; i++ {
		shutdownCh := make(chan struct{})
		s.workerShutdownCh = append(s.workerShutdownCh, shutdownCh)

		s.shutdownWG.Add(1)
		go s.pollTaskQueue(shutdownCh)
	}
}

func (s *SequentialScheduler[T]) stopWorkers(
	count int,
) {
	shutdownChToClose := s.workerShutdownCh[:count]
	s.workerShutdownCh = s.workerShutdownCh[count:]

	for _, shutdownCh := range shutdownChToClose {
		close(shutdownCh)
	}
}

func (s *SequentialScheduler[T]) pollTaskQueue(workerShutdownCh <-chan struct{}) {
	defer s.shutdownWG.Done()

	for {
		select {
		case <-s.shutdownChan:
			s.drainTasks()
			return
		case <-workerShutdownCh:
			return
		case queue := <-s.queueChan:
			s.processTaskQueue(queue, workerShutdownCh)
		}
	}
}

func (s *SequentialScheduler[T]) processTaskQueue(
	queue SequentialTaskQueue[T],
	workerShutdownCh <-chan struct{},
) {
	atomic.AddInt64(&s.assignedWorkerCount, 1)
	defer atomic.AddInt64(&s.assignedWorkerCount, -1)

	for {
		select {
		case <-s.shutdownChan:
			s.drainTasks()
			return
		case <-workerShutdownCh:
			// Put queue back to the queue channel
			s.queueChan <- queue
			return
		default:
			// NOTE: implicit assumption
			// 1. a queue is owned by a coroutine
			// 2. a coroutine will remove a task from its queue then execute the task; this coroutine will ack / nack / reschedule the task at the end
			// 3. queue will be deleted once queue is empty
			//
			// for batched tasks, if task is state
			// ack: behavior is same as normal task
			// nack: batched task will be broken into original tasks, and synchronously added to queue (so queue is not empty)
			// reschedule: behavior is same as normal task
			if !queue.IsEmpty() {
				s.executeTask(queue)
			} else {
				deleted := s.queues.RemoveIf(queue.ID(), func(key interface{}, value interface{}) bool {
					return value.(SequentialTaskQueue[T]).IsEmpty()
				})
				if deleted {
					return
				}
				// if deletion failed, meaning that task queue is offered with new task
				// continue execution
			}
		}
	}
}

// TODO: change this function to process all available tasks in the queue.
func (s *SequentialScheduler[T]) executeTask(queue SequentialTaskQueue[T]) {
	var panicErr error
	defer log.CapturePanic(s.logger, &panicErr)
	shouldRetry := true
	task := queue.Remove()

	s.recordQueueLatency(task)

	operation := func() (retErr error) {
		var executePanic error
		defer func() {
			if executePanic != nil {
				retErr = executePanic
				shouldRetry = false // do not retry if panic
			}
		}()
		defer log.CapturePanic(s.logger, &executePanic)

		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}
	isRetryable := func(err error) bool {
		return !s.isStopped() && shouldRetry && task.IsRetryableError(err)
	}
	if err := backoff.ThrottleRetry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if s.isStopped() {
			task.Abort()
			return
		}

		task.Nack(err)
		s.recordTaskCompleted(task)
		return
	}

	task.Ack()
	s.recordTaskCompleted(task)
}

func (s *SequentialScheduler[T]) drainTasks() {
LoopDrainQueues:
	for {
		select {
		case queue := <-s.queueChan:
		LoopDrainSingleQueue:
			for {
				for !queue.IsEmpty() {
					queue.Remove().Abort()
				}
				deleted := s.queues.RemoveIf(queue.ID(), func(key interface{}, value interface{}) bool {
					return value.(SequentialTaskQueue[T]).IsEmpty()
				})
				if deleted {
					break LoopDrainSingleQueue
				}
			}
		default:
			break LoopDrainQueues
		}
	}
}

func (s *SequentialScheduler[T]) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}

// Metrics helper methods

func (s *SequentialScheduler[T]) exportMetricsLoop() {
	defer s.shutdownWG.Done()

	ticker := time.NewTicker(metricsExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			s.emitGaugeMetrics()
		}
	}
}

func (s *SequentialScheduler[T]) emitGaugeMetrics() {
	s.workerLock.Lock()
	activeWorkers := len(s.workerShutdownCh)
	s.workerLock.Unlock()

	assignedWorkers := atomic.LoadInt64(&s.assignedWorkerCount)
	queueDepth := len(s.queueChan)
	activeQueues := s.queues.Len()

	metrics.SchedulerActiveWorkers.With(s.metricsHandler).Record(float64(activeWorkers))
	metrics.SchedulerAssignedWorkers.With(s.metricsHandler).Record(float64(assignedWorkers))
	metrics.SchedulerQueueDepth.With(s.metricsHandler).Record(float64(queueDepth))
	metrics.SchedulerActiveQueues.With(s.metricsHandler).Record(float64(activeQueues))
}

func (s *SequentialScheduler[T]) setEnqueueTime(task T) {
	if timestamped, ok := any(task).(SchedulerTimestampedTask); ok {
		timestamped.SetSchedulerEnqueueTime(s.timeSource.Now())
	}
}

func (s *SequentialScheduler[T]) recordQueueLatency(task T) {
	if s.metricsHandler == nil {
		return
	}
	if timestamped, ok := any(task).(SchedulerTimestampedTask); ok {
		enqueueTime := timestamped.GetSchedulerEnqueueTime()
		if !enqueueTime.IsZero() {
			latency := s.timeSource.Now().Sub(enqueueTime)
			metrics.SchedulerQueueLatency.With(s.metricsHandler).Record(latency, s.getMetricTags(task)...)
		}
	}
}

func (s *SequentialScheduler[T]) recordTaskSubmitted(task T) {
	if s.metricsHandler == nil {
		return
	}
	metrics.SchedulerTasksSubmitted.With(s.metricsHandler).Record(1, s.getMetricTags(task)...)
}

func (s *SequentialScheduler[T]) recordTaskCompleted(task T) {
	if s.metricsHandler == nil {
		return
	}
	metrics.SchedulerTasksCompleted.With(s.metricsHandler).Record(1, s.getMetricTags(task)...)
}

func (s *SequentialScheduler[T]) getMetricTags(task T) []metrics.Tag {
	if s.metricTagsFn != nil {
		return s.metricTagsFn(task)
	}
	return nil
}
