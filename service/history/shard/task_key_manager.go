package shard

import (
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
)

type (
	taskKeyManager struct {
		generator *taskKeyGenerator
		tracker   *taskRequestTracker

		timeSource clock.TimeSource
		config     *configs.Config
		logger     log.Logger
	}
)

func newTaskKeyManager(
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	timeSource clock.TimeSource,
	config *configs.Config,
	logger log.Logger,
	renewRangeIDFn renewRangeIDFn,
) *taskKeyManager {
	return &taskKeyManager{
		generator: newTaskKeyGenerator(
			config.RangeSizeBits,
			timeSource,
			logger,
			renewRangeIDFn,
		),
		tracker:    newTaskRequestTracker(taskCategoryRegistry),
		timeSource: timeSource,
		logger:     logger,
		config:     config,
	}
}

func (m *taskKeyManager) setAndTrackTaskKeys(
	taskMaps ...map[tasks.Category][]tasks.Task,
) (taskRequestCompletionFn, error) {

	if err := m.generator.setTaskKeys(taskMaps...); err != nil {
		return nil, err
	}

	return m.tracker.track(taskMaps...), nil
}

func (m *taskKeyManager) peekTaskKey(
	category tasks.Category,
) tasks.Key {
	return m.generator.peekTaskKey(category)
}

func (m *taskKeyManager) generateTaskKey(
	category tasks.Category,
) (tasks.Key, error) {
	return m.generator.generateTaskKey(category)
}

func (m *taskKeyManager) drainTaskRequests() {
	m.tracker.drain()
}

func (m *taskKeyManager) setRangeID(
	rangeID int64,
) {
	m.generator.setRangeID(rangeID)

	// rangeID update means all pending add tasks requests either already succeeded
	// are guaranteed to fail, so we can clear pending requests in the tracker
	m.tracker.clear()
}

func (m *taskKeyManager) setTaskMinScheduledTime(
	taskMinScheduledTime time.Time,
) {
	m.generator.setTaskMinScheduledTime(taskMinScheduledTime)
}

func (m *taskKeyManager) getExclusiveReaderHighWatermark(
	category tasks.Category,
) tasks.Key {
	minTaskKey, ok := m.tracker.minTaskKey(category)
	if !ok {
		minTaskKey = tasks.MaximumKey
	}

	// TODO: should this be moved generator.setTaskKeys() ?
	m.setTaskMinScheduledTime(
		// TODO: Truncation here is just to make sure task scheduled time has the same precision as the old logic.
		// Remove this truncation once we validate the rest of the code can worker correctly with higher precision.
		m.timeSource.Now().Add(m.config.TimerProcessorMaxTimeShift()).Truncate(common.ScheduledTaskMinPrecision),
	)

	nextTaskKey := m.generator.peekTaskKey(category)

	exclusiveReaderHighWatermark := tasks.MinKey(
		minTaskKey,
		nextTaskKey,
	)
	if category.Type() == tasks.CategoryTypeScheduled {
		exclusiveReaderHighWatermark.TaskID = 0

		// TODO: Truncation here is just to make sure task scheduled time has the same precision as the old logic.
		// Remove this truncation once we validate the rest of the code can worker correctly with higher precision.
		exclusiveReaderHighWatermark.FireTime = exclusiveReaderHighWatermark.FireTime.
			Truncate(common.ScheduledTaskMinPrecision)
	}

	return exclusiveReaderHighWatermark
}
