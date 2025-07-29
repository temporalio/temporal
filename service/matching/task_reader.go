package matching

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
)

const (
	taskReaderOfferThrottleWait  = time.Second
	taskReaderThrottleRetryDelay = 3 * time.Second
)

type (
	taskReader struct {
		taskBuffer chan *persistencespb.AllocatedTaskInfo // tasks loaded from persistence
		notifyC    chan struct{}                          // Used as signal to notify pump of new tasks
		backlogMgr *backlogManagerImpl

		backoffTimerLock      sync.Mutex
		backoffTimer          *time.Timer
		retrier               backoff.Retrier
		backlogHeadCreateTime atomic.Int64
	}
)

func newTaskReader(backlogMgr *backlogManagerImpl) *taskReader {
	tr := &taskReader{
		backlogMgr: backlogMgr,
		notifyC:    make(chan struct{}, 1),
		// we always dequeue the head of the buffer and try to dispatch it to a poller
		// so allocate one less than desired target buffer size
		taskBuffer: make(chan *persistencespb.AllocatedTaskInfo, backlogMgr.config.GetTasksBatchSize()-1),
		retrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			clock.NewRealTimeSource(),
		),
	}
	tr.backlogHeadCreateTime.Store(-1)
	return tr
}

// Start taskReader background goroutines.
func (tr *taskReader) Start() {
	go tr.dispatchBufferedTasks()
	go tr.getTasksPump()
}

func (tr *taskReader) Signal() {
	var event struct{}
	select {
	case tr.notifyC <- event:
	default: // channel already has an event, don't block
	}
}

func (tr *taskReader) updateBacklogAge(task *internalTask) {
	if task.event.Data.CreateTime == nil {
		return // should not happen but for safety
	}
	ts := timestamp.TimeValue(task.event.Data.CreateTime).UnixNano()
	tr.backlogHeadCreateTime.Store(ts)
}

func (tr *taskReader) getBacklogHeadAge() time.Duration {
	if tr.backlogHeadCreateTime.Load() == -1 {
		return time.Duration(0)
	}
	return time.Since(time.Unix(0, tr.backlogHeadCreateTime.Load()))
}

func (tr *taskReader) dispatchBufferedTasks() {
	ctx := tr.backlogMgr.tqCtx

dispatchLoop:
	for ctx.Err() == nil {
		if len(tr.taskBuffer) == 0 {
			// reset the atomic since we have no tasks from the backlog
			tr.backlogHeadCreateTime.Store(-1)
		}
		select {
		case taskInfo, ok := <-tr.taskBuffer:
			if !ok { // Task queue getTasks pump is shutdown
				break dispatchLoop
			}
			task := newInternalTaskFromBacklog(taskInfo, tr.completeTask)
			for ctx.Err() == nil {
				tr.updateBacklogAge(task)
				taskCtx, cancel := context.WithTimeout(ctx, taskReaderOfferTimeout)
				err := tr.backlogMgr.processSpooledTask(taskCtx, task)
				cancel()
				if err == nil {
					continue dispatchLoop
				}

				var stickyUnavailable *serviceerrors.StickyWorkerUnavailable
				// if task is still valid (truly valid or unable to verify if task is valid)
				metrics.BufferThrottlePerTaskQueueCounter.With(tr.taggedMetricsHandler()).Record(1)
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) &&
					// StickyWorkerUnavailable is expected for versioned sticky queues
					!errors.As(err, &stickyUnavailable) {
					tr.throttledLogger().Error("taskReader: unexpected error dispatching task", tag.Error(err))
				}
				util.InterruptibleSleep(ctx, taskReaderOfferThrottleWait)
			}
			return
		case <-ctx.Done():
			return
		}
	}
}

func (tr *taskReader) completeTask(task *internalTask, res taskResponse) {
	tr.backlogMgr.completeTask(task, res.startErr)
}

// nolint:revive // can improve this later
func (tr *taskReader) getTasksPump() {
	ctx := tr.backlogMgr.tqCtx

	if err := tr.backlogMgr.WaitUntilInitialized(ctx); err != nil {
		return
	}

	updateAckTimer := time.NewTimer(tr.backlogMgr.config.UpdateAckInterval())
	defer updateAckTimer.Stop()

	tr.Signal() // prime pump
Loop:
	for {
		// Prioritize exiting over other processing
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-ctx.Done():
			return

		case <-tr.notifyC:
			batch, err := tr.getTaskBatch(ctx)
			tr.backlogMgr.signalIfFatal(err)
			if err != nil {
				// TODO: Should we ever stop retrying on db errors?
				if common.IsResourceExhausted(err) {
					tr.reEnqueueAfterDelay(taskReaderThrottleRetryDelay)
				} else {
					tr.reEnqueueAfterDelay(tr.retrier.NextBackOff(err))
				}
				continue Loop
			}
			tr.retrier.Reset()

			if len(batch.tasks) == 0 {
				tr.backlogMgr.taskAckManager.setReadLevelAfterGap(batch.readLevel)
				if !batch.isReadBatchDone {
					tr.Signal()
				}
				continue Loop
			}

			// only error here is due to context cancellation which we also
			// handle above
			_ = tr.addTasksToBuffer(ctx, batch.tasks)
			// There maybe more tasks. We yield now, but signal pump to check again later.
			tr.Signal()

		case <-updateAckTimer.C:
			err := tr.persistAckBacklogCountLevel(ctx)
			isConditionFailed := tr.backlogMgr.signalIfFatal(err)
			if err != nil && !isConditionFailed {
				tr.logger().Error("Persistent store operation failure",
					tag.StoreOperationUpdateTaskQueue,
					tag.Error(err))
				// keep going as saving ack is not critical
			}
			tr.Signal() // periodically signal pump to check persistence for tasks
			updateAckTimer = time.NewTimer(tr.backlogMgr.config.UpdateAckInterval())
		}
	}
}

func (tr *taskReader) getTaskBatchWithRange(
	ctx context.Context,
	readLevel int64,
	maxReadLevel int64,
) ([]*persistencespb.AllocatedTaskInfo, error) {
	response, err := tr.backlogMgr.db.GetTasks(ctx, subqueueZero, readLevel+1, maxReadLevel+1, tr.backlogMgr.config.GetTasksBatchSize())
	if err != nil {
		return nil, err
	}
	return response.Tasks, err
}

type getTasksBatchResponse struct {
	tasks           []*persistencespb.AllocatedTaskInfo
	readLevel       int64
	isReadBatchDone bool
}

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
// Also return a bool to indicate whether read is finished
func (tr *taskReader) getTaskBatch(ctx context.Context) (*getTasksBatchResponse, error) {
	var tasks []*persistencespb.AllocatedTaskInfo
	readLevel := tr.backlogMgr.taskAckManager.getReadLevel()
	maxReadLevel := tr.backlogMgr.db.GetMaxReadLevel(subqueueZero)

	// counter i is used to break and let caller check whether taskqueue is still alive and needs to resume read.
	for i := 0; i < 10 && readLevel < maxReadLevel; i++ {
		upper := readLevel + tr.backlogMgr.config.RangeSize
		if upper > maxReadLevel {
			upper = maxReadLevel
		}
		tasks, err := tr.getTaskBatchWithRange(ctx, readLevel, upper)
		if err != nil {
			return nil, err
		}
		// return as long as it grabs any tasks
		if len(tasks) > 0 {
			return &getTasksBatchResponse{
				tasks:           tasks,
				readLevel:       upper,
				isReadBatchDone: true,
			}, nil
		}
		readLevel = upper
	}
	return &getTasksBatchResponse{
		tasks:           tasks,
		readLevel:       readLevel,
		isReadBatchDone: readLevel == maxReadLevel,
	}, nil // caller will update readLevel when no task grabbed
}

func (tr *taskReader) addTasksToBuffer(
	ctx context.Context,
	tasks []*persistencespb.AllocatedTaskInfo,
) error {
	for _, t := range tasks {
		if IsTaskExpired(t) {
			// task is expired when "add tasks to buffer" is called, so when we read it
			metrics.ExpiredTasksPerTaskQueueCounter.With(tr.taggedMetricsHandler()).Record(1, metrics.TaskExpireStageReadTag)
			// Also increment readLevel for expired tasks otherwise it could result in
			// looping over the same tasks if all tasks read in the batch are expired
			tr.backlogMgr.taskAckManager.setReadLevel(t.GetTaskId())
			continue
		}
		if err := tr.addSingleTaskToBuffer(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (tr *taskReader) addSingleTaskToBuffer(
	ctx context.Context,
	task *persistencespb.AllocatedTaskInfo,
) error {
	tr.backlogMgr.taskAckManager.addTask(task.GetTaskId())
	select {
	case tr.taskBuffer <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tr *taskReader) persistAckBacklogCountLevel(ctx context.Context) error {
	ackLevel := tr.backlogMgr.taskAckManager.getAckLevel()
	return tr.backlogMgr.db.OldUpdateState(ctx, ackLevel)
}

func (tr *taskReader) logger() log.Logger {
	return tr.backlogMgr.logger
}

func (tr *taskReader) throttledLogger() log.ThrottledLogger {
	return tr.backlogMgr.throttledLogger
}

func (tr *taskReader) taggedMetricsHandler() metrics.Handler {
	return tr.backlogMgr.metricsHandler
}

func (tr *taskReader) reEnqueueAfterDelay(duration time.Duration) {
	tr.backoffTimerLock.Lock()
	defer tr.backoffTimerLock.Unlock()

	if tr.backoffTimer == nil {
		tr.backoffTimer = time.AfterFunc(duration, func() {
			tr.backoffTimerLock.Lock()
			defer tr.backoffTimerLock.Unlock()

			tr.Signal() // re-enqueue the event
			tr.backoffTimer = nil
		})
	}
}
