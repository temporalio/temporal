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
		allocator *taskKeyAllocator
		tracker   *taskRequestTracker

		logger log.Logger
	}
)

func newTaskKeyManager(
	timeSource clock.TimeSource,
	config *configs.Config,
	logger log.Logger,
	renewRangeIDFn renewRangeIDFn,
) *taskKeyManager {
	manager := &taskKeyManager{
		tracker: newTaskRequestTracker(),
		logger:  logger,
	}
	manager.allocator = newTaskKeyAllocator(
		config.RangeSizeBits,
		timeSource,
		config.TimerProcessorMaxTimeShift,
		logger,
		renewRangeIDFn,
	)

	return manager
}

func (m *taskKeyManager) allocateTaskKey(
	taskMaps ...map[tasks.Category][]tasks.Task,
) (taskRequestCompletionFn, error) {

	if err := m.allocator.allocate(taskMaps...); err != nil {
		return nil, err
	}

	return m.tracker.track(taskMaps...), nil
}

func (m *taskKeyManager) peekNextTaskKey(
	category tasks.Category,
) tasks.Key {
	return m.allocator.peekNextTaskKey(category)
}

func (m *taskKeyManager) generateTaskKey(
	category tasks.Category,
) (tasks.Key, error) {
	return m.allocator.generateTaskKey(category)
}

func (m *taskKeyManager) drainTaskRequests() {
	m.tracker.drain()
}

func (m *taskKeyManager) setRangeID(
	rangeID int64,
) {
	m.allocator.setRangeID(rangeID)

	// rangeID update means all pending add tasks requests either already succeeded
	// are guaranteed to fail, so we can clear pending requests in the tracker
	m.tracker.clear()
}

func (m *taskKeyManager) setTaskMinScheduledTime(
	taskMinScheduledTime time.Time,
) {
	m.allocator.setTaskMinScheduledTime(taskMinScheduledTime)
}

func (m *taskKeyManager) getExclusiveReaderHighWatermark(
	category tasks.Category,
) tasks.Key {
	minTaskKey, ok := m.tracker.minTaskKey(category)
	if !ok {
		minTaskKey = tasks.MaximumKey
	}

	nextTaskKey := m.allocator.peekNextTaskKey(category)

	exclusiveReaderHighWatermark := tasks.MinKey(
		minTaskKey,
		nextTaskKey,
	)
	if category.Type() == tasks.CategoryTypeScheduled {
		exclusiveReaderHighWatermark.TaskID = 0
		exclusiveReaderHighWatermark.FireTime = exclusiveReaderHighWatermark.FireTime.
			Truncate(persistence.ScheduledTaskMinPrecision)
	}

	return exclusiveReaderHighWatermark
}
