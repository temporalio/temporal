package task

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/collection"
	"github.com/temporalio/temporal/common/log"
)

type (
	sequentialTaskProcessorImpl struct {
		status       int32
		shutdownChan chan struct{}
		waitGroup    sync.WaitGroup

		coroutineSize    int
		taskqueues       collection.ConcurrentTxMap
		taskQueueFactory SequentialTaskQueueFactory
		taskqueueChan    chan SequentialTaskQueue

		metricsScope  int
		metricsClient metrics.Client
		logger        log.Logger
	}
)

// NewSequentialTaskProcessor create a new sequential tasks processor
func NewSequentialTaskProcessor(coroutineSize int, taskQueueHashFn collection.HashFunc, taskQueueFactory SequentialTaskQueueFactory,
	metricsClient metrics.Client, logger log.Logger) Processor {

	return &sequentialTaskProcessorImpl{
		status:           common.DaemonStatusInitialized,
		shutdownChan:     make(chan struct{}),
		coroutineSize:    coroutineSize,
		taskqueues:       collection.NewShardedConcurrentTxMap(1024, taskQueueHashFn),
		taskQueueFactory: taskQueueFactory,
		taskqueueChan:    make(chan SequentialTaskQueue, coroutineSize),

		metricsScope:  metrics.SequentialTaskProcessingScope,
		metricsClient: metricsClient,
		logger:        logger,
	}
}

func (t *sequentialTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.waitGroup.Add(t.coroutineSize)
	for i := 0; i < t.coroutineSize; i++ {
		go t.pollAndProcessTaskQueue()
	}
	t.logger.Info("Task processor started.")
}

func (t *sequentialTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(t.shutdownChan)
	if success := common.AwaitWaitGroup(&t.waitGroup, time.Minute); !success {
		t.logger.Warn("Task processor timeout trying to stop.")
	}
	t.logger.Info("Task processor stopped.")
}

func (t *sequentialTaskProcessorImpl) Submit(task Task) error {

	t.metricsClient.IncCounter(t.metricsScope, metrics.SequentialTaskSubmitRequest)
	metricsTimer := t.metricsClient.StartTimer(t.metricsScope, metrics.SequentialTaskSubmitLatency)
	defer metricsTimer.Stop()

	taskqueue := t.taskQueueFactory(task)
	taskqueue.Add(task)

	_, fnEvaluated, err := t.taskqueues.PutOrDo(
		taskqueue.QueueID(),
		taskqueue,
		func(key interface{}, value interface{}) error {
			value.(SequentialTaskQueue).Add(task)
			return nil
		},
	)
	if err != nil {
		return err
	}

	// if function evaluated, meaning that the task set is
	// already dispatched
	if fnEvaluated {
		t.metricsClient.IncCounter(t.metricsScope, metrics.SequentialTaskSubmitRequestTaskQueueExist)
		return nil
	}

	// need to dispatch this task set
	t.metricsClient.IncCounter(t.metricsScope, metrics.SequentialTaskSubmitRequestTaskQueueMissing)
	select {
	case <-t.shutdownChan:
	case t.taskqueueChan <- taskqueue:
	}
	return nil

}

func (t *sequentialTaskProcessorImpl) pollAndProcessTaskQueue() {
	defer t.waitGroup.Done()

	for {
		select {
		case <-t.shutdownChan:
			return
		case taskqueue := <-t.taskqueueChan:
			metricsTimer := t.metricsClient.StartTimer(t.metricsScope, metrics.SequentialTaskQueueProcessingLatency)
			t.processTaskQueue(taskqueue)
			metricsTimer.Stop()
		}
	}
}

func (t *sequentialTaskProcessorImpl) processTaskQueue(taskqueue SequentialTaskQueue) {
	for {
		select {
		case <-t.shutdownChan:
			return
		default:
			queueSize := taskqueue.Len()
			t.metricsClient.RecordTimer(t.metricsScope, metrics.SequentialTaskQueueSize, time.Duration(queueSize))
			if queueSize > 0 {
				t.processTaskOnce(taskqueue)
			} else {
				deleted := t.taskqueues.RemoveIf(taskqueue.QueueID(), func(key interface{}, value interface{}) bool {
					return value.(SequentialTaskQueue).IsEmpty()
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

func (t *sequentialTaskProcessorImpl) processTaskOnce(taskqueue SequentialTaskQueue) {
	metricsTimer := t.metricsClient.StartTimer(t.metricsScope, metrics.SequentialTaskTaskProcessingLatency)
	defer metricsTimer.Stop()

	task := taskqueue.Remove()
	err := task.Execute()
	err = task.HandleErr(err)

	if err != nil {
		if task.RetryErr(err) {
			taskqueue.Add(task)
		} else {
			t.logger.Error("Unable to process task", tag.Error(err))
			task.Nack()
		}
	} else {
		task.Ack()
	}
}
