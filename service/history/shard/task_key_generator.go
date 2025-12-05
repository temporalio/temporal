package shard

import (
	"fmt"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/tasks"
)

const (
	taskIDUninitialized = -1
)

type (
	renewRangeIDFn func() error

	taskKeyGenerator struct {
		nextTaskID         int64
		exclusiveMaxTaskID int64

		taskMinScheduledTime time.Time

		rangeSizeBits uint
		timeSource    clock.TimeSource
		logger        log.Logger

		renewRangeIDFn renewRangeIDFn
	}
)

func newTaskKeyGenerator(
	rangeSizeBits uint,
	timeSource clock.TimeSource,
	logger log.Logger,
	renewRangeIDFn renewRangeIDFn,
) *taskKeyGenerator {
	return &taskKeyGenerator{
		nextTaskID:         taskIDUninitialized,
		exclusiveMaxTaskID: taskIDUninitialized,
		rangeSizeBits:      rangeSizeBits,
		timeSource:         timeSource,
		logger:             logger,
		renewRangeIDFn:     renewRangeIDFn,
	}
}

func (a *taskKeyGenerator) setTaskKeys(
	taskMaps ...map[tasks.Category][]tasks.Task,
) error {
	now := a.timeSource.Now()
	// TODO: Truncation here is just to make sure task scheduled time has the same precision as the old logic.
	// Remove this truncation once we validate the rest of the code can worker correctly with higher precision.
	a.setTaskMinScheduledTime(now.Truncate(common.ScheduledTaskMinPrecision))

	for _, taskMap := range taskMaps {
		for category, tasksByCategory := range taskMap {
			isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
			for _, task := range tasksByCategory {
				id, err := a.generateTaskID()
				if err != nil {
					return err
				}
				task.SetTaskID(id)

				taskScheduledTime := now
				if isScheduledTask {
					// Persistence might loss precision when saving to DB.
					// Make the task scheduled time to have the same precision as DB here,
					// so that if the comparsion in the next step passes, it's guaranteed
					// the task can be retrieved from DB by queue processor.
					taskScheduledTime = task.GetVisibilityTime().
						Add(common.ScheduledTaskMinPrecision).
						Truncate(common.ScheduledTaskMinPrecision)

					if taskScheduledTime.Before(a.taskMinScheduledTime) {
						a.logger.Debug("New timer generated is less than min scheduled time",
							tag.WorkflowNamespaceID(task.GetNamespaceID()),
							tag.WorkflowID(task.GetWorkflowID()),
							tag.WorkflowRunID(task.GetRunID()),
							tag.TaskType(task.GetType()),
							tag.TaskID(id),
							tag.Timestamp(taskScheduledTime),
							tag.CursorTimestamp(a.taskMinScheduledTime),
							tag.ValueShardAllocateTimerBeforeRead,
						)
						// Theoritically we don't need to add the extra 1ms.
						// Guess it's just to be extra safe here.
						taskScheduledTime = a.taskMinScheduledTime.Add(common.ScheduledTaskMinPrecision)
					}
				}
				task.SetVisibilityTime(taskScheduledTime)

				a.logger.Debug("Assigning new task key",
					tag.WorkflowNamespaceID(task.GetNamespaceID()),
					tag.WorkflowID(task.GetWorkflowID()),
					tag.WorkflowRunID(task.GetRunID()),
					tag.TaskType(task.GetType()),
					tag.TaskID(id),
					tag.Timestamp(task.GetVisibilityTime()),
					tag.CursorTimestamp(a.taskMinScheduledTime),
				)
			}
		}
	}

	return nil
}

func (a *taskKeyGenerator) peekTaskKey(
	category tasks.Category,
) tasks.Key {
	switch category.Type() {
	case tasks.CategoryTypeImmediate:
		return tasks.NewImmediateKey(a.nextTaskID)
	case tasks.CategoryTypeScheduled:
		return tasks.NewKey(
			a.taskMinScheduledTime,
			a.nextTaskID,
		)
	default:
		panic(fmt.Sprintf("Unknown category type: %v", category.Type()))
	}
}

func (a *taskKeyGenerator) generateTaskKey(
	category tasks.Category,
) (tasks.Key, error) {
	id, err := a.generateTaskID()
	if err != nil {
		return tasks.Key{}, err
	}

	switch category.Type() {
	case tasks.CategoryTypeImmediate:
		return tasks.NewImmediateKey(id), nil
	case tasks.CategoryTypeScheduled:
		return tasks.NewKey(
			a.taskMinScheduledTime,
			id,
		), nil
	default:
		panic(fmt.Sprintf("Unknown category type: %v", category.Type()))
	}
}

func (a *taskKeyGenerator) setRangeID(rangeID int64) {
	a.nextTaskID = rangeID << a.rangeSizeBits
	a.exclusiveMaxTaskID = (rangeID + 1) << a.rangeSizeBits

	a.logger.Info("Task key range updated",
		tag.Number(a.nextTaskID),
		tag.NextNumber(a.exclusiveMaxTaskID),
	)
}

func (a *taskKeyGenerator) setTaskMinScheduledTime(
	taskMinScheduledTime time.Time,
) {
	a.taskMinScheduledTime = util.MaxTime(a.taskMinScheduledTime, taskMinScheduledTime)
}

func (a *taskKeyGenerator) generateTaskID() (int64, error) {
	if a.nextTaskID == taskIDUninitialized {
		a.logger.Panic("Range id is not initialized before generating task id")
	}

	if a.nextTaskID == a.exclusiveMaxTaskID {
		if err := a.renewRangeIDFn(); err != nil {
			return taskIDUninitialized, err
		}

		if a.nextTaskID == a.exclusiveMaxTaskID {
			a.logger.Panic("Renew rangeID succeeded, but rangeID in task key allocator is not updated.")
		}
	}

	taskID := a.nextTaskID
	a.nextTaskID++
	return taskID, nil
}
