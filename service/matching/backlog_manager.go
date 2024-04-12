// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package matching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

var (
	// this retry policy is currently only used for matching persistence operations
	// that, if failed, the entire task queue needs to be reloaded
	persistenceOperationRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(1 * time.Second).
		WithExpirationInterval(30 * time.Second)
)

type (
	taskQueueManagerOpt func(*physicalTaskQueueManagerImpl)

	idBlockAllocator interface {
		RenewLease(context.Context) (taskQueueState, error)
		RangeID() int64
	}

	// backlogManagerImpl manages the backlog and persistence of a physical task queue
	backlogManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		SpoolTask(taskInfo *persistencespb.TaskInfo) error
		BacklogCountHint() int64
		BacklogStatus() *taskqueuepb.TaskQueueStatus
		String() string
	}

	backlogManagerImpl struct {
		pqMgr               physicalTaskQueueManager
		db                  *taskQueueDB
		taskWriter          *taskWriter
		taskReader          *taskReader // reads tasks from db and async matches it with poller
		taskGC              *taskGC
		taskAckManager      ackManager // tracks ackLevel for delivered messages
		config              *taskQueueConfig
		logger              log.Logger
		throttledLogger     log.ThrottledLogger
		matchingClient      matchingservice.MatchingServiceClient
		metricsHandler      metrics.Handler
		contextInfoProvider func(ctx context.Context) context.Context
		initializedError    *future.FutureImpl[struct{}]
		// skipFinalUpdate controls behavior on Stop: if it's false, we try to write one final
		// update before unloading
		skipFinalUpdate atomic.Bool
	}
)

var _ backlogManager = (*backlogManagerImpl)(nil)

func newBacklogManager(
	pqMgr physicalTaskQueueManager,
	config *taskQueueConfig,
	taskManager persistence.TaskManager,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
	contextInfoProvider func(ctx context.Context) context.Context,
) *backlogManagerImpl {
	db := newTaskQueueDB(taskManager, pqMgr.QueueKey(), logger)
	bmg := &backlogManagerImpl{
		pqMgr:               pqMgr,
		matchingClient:      matchingClient,
		metricsHandler:      metricsHandler,
		logger:              logger,
		throttledLogger:     throttledLogger,
		db:                  db,
		taskAckManager:      newAckManager(logger),
		taskGC:              newTaskGC(db, config),
		config:              config,
		contextInfoProvider: contextInfoProvider,
		initializedError:    future.NewFuture[struct{}](),
	}
	bmg.taskWriter = newTaskWriter(bmg)
	bmg.taskReader = newTaskReader(bmg)

	return bmg
}

// signalIfFatal calls UnloadFromPartitionManager of the physicalTaskQueueManager
// if and only if the supplied error represents a fatal condition, e.g. the existence
// of a newer lease by another backlogManager. Returns true if the unload signal
// is emitted, false otherwise.
func (c *backlogManagerImpl) signalIfFatal(err error) bool {
	if err == nil {
		return false
	}
	var condfail *persistence.ConditionFailedError
	if errors.As(err, &condfail) {
		c.metricsHandler.Counter(metrics.ConditionFailedErrorPerTaskQueueCounter.Name()).Record(1)
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager()
		return true
	}
	return false
}

func (c *backlogManagerImpl) Start() {
	c.taskWriter.Start()
	c.taskReader.Start()
}

func (c *backlogManagerImpl) Stop() {
	// Maybe try to write one final update of ack level and GC some tasks.
	// Skip the update if we never initialized (ackLevel will be -1 in that case).
	// Also skip if we're stopping due to lost ownership (the update will fail in that case).
	// Ignore any errors.
	// Note that it's fine to GC even if the update ack level fails because we did match the
	// tasks, the next owner will just read over an empty range.
	ackLevel := c.taskAckManager.getAckLevel()
	if ackLevel >= 0 && !c.skipFinalUpdate.Load() {
		ctx, cancel := c.newIOContext()
		defer cancel()

		_ = c.db.UpdateState(ctx, ackLevel)
		c.taskGC.RunNow(ctx, ackLevel)
	}
	c.taskWriter.Stop()
	c.taskReader.Stop()
}

func (c *backlogManagerImpl) SetInitializedError(err error) {
	c.initializedError.Set(struct{}{}, err)
	if err != nil {
		// We can't recover from here without starting over, so unload the whole task queue.
		// Skip final update since we never initialized.
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager()
	}
}

func (c *backlogManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	return err
}

func (c *backlogManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	_, err := c.taskWriter.appendTask(taskInfo)
	c.signalIfFatal(err)
	if err == nil {
		c.taskReader.Signal()
	}
	return err
}

func (c *backlogManagerImpl) processSpooledTask(
	ctx context.Context,
	task *internalTask,
) error {
	return c.pqMgr.ProcessSpooledTask(ctx, task)
}

func (c *backlogManagerImpl) BacklogCountHint() int64 {
	return c.taskAckManager.getBacklogCountHint()
}

func (c *backlogManagerImpl) BacklogStatus() *taskqueuepb.TaskQueueStatus {
	taskIDBlock := rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)
	return &taskqueuepb.TaskQueueStatus{
		ReadLevel:        c.taskAckManager.getReadLevel(),
		AckLevel:         c.taskAckManager.getAckLevel(),
		BacklogCountHint: c.BacklogCountHint(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: taskIDBlock.start,
			EndId:   taskIDBlock.end,
		},
	}
}

func (c *backlogManagerImpl) String() string {
	buf := new(bytes.Buffer)
	rangeID := c.db.RangeID()
	_, _ = fmt.Fprintf(buf, "RangeID=%v\n", rangeID)
	_, _ = fmt.Fprintf(buf, "TaskIDBlock=%+v\n", rangeIDToTaskIDBlock(rangeID, c.config.RangeSize))
	_, _ = fmt.Fprintf(buf, "AckLevel=%v\n", c.taskAckManager.ackLevel)
	_, _ = fmt.Fprintf(buf, "MaxTaskID=%v\n", c.taskAckManager.getReadLevel())

	return buf.String()
}

// completeTask marks a task as processed. Only tasks created by taskReader (i.e. backlog from db) reach
// here. As part of completion:
//   - task is deleted from the database when err is nil
//   - new task is created and current task is deleted when err is not nil
func (c *backlogManagerImpl) completeTask(task *persistencespb.AllocatedTaskInfo, err error) {
	if err != nil {
		// failed to start the task.
		// We cannot just remove it from persistence because then it will be lost.
		// We handle this by writing the task back to persistence with a higher taskID.
		// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
		// again the underlying reason for failing to start will be resolved.
		// Note that RecordTaskStarted only fails after retrying for a long time, so a single task will not be
		// re-written to persistence frequently.
		err = executeWithRetry(context.Background(), func(_ context.Context) error {
			_, err := c.taskWriter.appendTask(task.Data)
			return err
		})

		if err != nil {
			// OK, we also failed to write to persistence.
			// This should only happen in very extreme cases where persistence is completely down.
			// We still can't lose the old task, so we just unload the entire task queue
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationStopTaskQueue,
				tag.Error(err),
				tag.WorkflowTaskQueueName(c.queueKey().PersistenceName()),
				tag.WorkflowTaskQueueType(c.queueKey().TaskType()))
			// Skip final update since persistence is having problems.
			c.skipFinalUpdate.Store(true)
			c.pqMgr.UnloadFromPartitionManager()
			return
		}
		c.taskReader.Signal()
	}

	ackLevel := c.taskAckManager.completeTask(task.GetTaskId())

	// TODO: completeTaskFunc and task.finish() should take in a context
	ctx, cancel := c.newIOContext()
	defer cancel()
	c.taskGC.Run(ctx, ackLevel)
}

func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*rangeSize + 1,
		end:   rangeID * rangeSize,
	}
}

// Retry operation on transient error.
func executeWithRetry(
	ctx context.Context,
	operation func(context.Context) error,
) error {
	return backoff.ThrottleRetryContext(ctx, operation, persistenceOperationRetryPolicy, func(err error) bool {
		if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
			return false
		}
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			return false
		}
		return common.IsPersistenceTransientError(err)
	})
}

func (c *backlogManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	return c.contextInfoProvider(ctx), cancel
}

func (c *backlogManagerImpl) queueKey() *PhysicalTaskQueueKey {
	return c.pqMgr.QueueKey()
}
