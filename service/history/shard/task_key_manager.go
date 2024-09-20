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

package shard

import (
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
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
		m.timeSource.Now().Add(m.config.TimerProcessorMaxTimeShift()).Truncate(persistence.ScheduledTaskMinPrecision),
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
			Truncate(persistence.ScheduledTaskMinPrecision)
	}

	return exclusiveReaderHighWatermark
}
