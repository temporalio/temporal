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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	taskKeyManagerSuite struct {
		suite.Suite
		*require.Assertions

		rangeID       int64
		rangeSizeBits uint
		initialTaskID int64

		mockTimeSource *clock.EventTimeSource

		manager *taskKeyManager
	}
)

func TestTaskKeyManagerSuite(t *testing.T) {
	s := &taskKeyManagerSuite{}
	suite.Run(t, s)
}

func (s *taskKeyManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.rangeID = 1
	s.rangeSizeBits = 3 // 1 << 3 = 8 tasks per range
	s.initialTaskID = s.rangeID << int64(s.rangeSizeBits)
	config := tests.NewDynamicConfig()
	config.RangeSizeBits = s.rangeSizeBits
	s.mockTimeSource = clock.NewEventTimeSource()

	s.manager = newTaskKeyManager(
		tasks.NewDefaultTaskCategoryRegistry(),
		s.mockTimeSource,
		config,
		log.NewTestLogger(),
		func() error {
			s.rangeID++
			s.manager.setRangeID(s.rangeID)
			return nil
		},
	)
	s.manager.setRangeID(s.rangeID)
	s.manager.setTaskMinScheduledTime(time.Now().Add(-time.Second))
}

func (s *taskKeyManagerSuite) TestSetAndTrackTaskKeys() {
	now := time.Now()

	// this tests is just for making sure task keys are set and tracked
	// the actual logic of task key generation is tested in taskKeyGeneratorTest

	numTask := 5
	transferTasks := make([]tasks.Task, 0, numTask)
	for i := 0; i < numTask; i++ {
		transferTasks = append(
			transferTasks,
			tasks.NewFakeTask(
				tests.WorkflowKey,
				tasks.CategoryTransfer,
				now,
			),
		)
	}
	// assert that task keys are not set
	for _, transferTask := range transferTasks {
		s.Zero(transferTask.GetKey().TaskID)
	}

	completionFn, err := s.manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: transferTasks,
	})
	s.NoError(err)

	// assert that task keys are set after calling setAndTrackTaskKeys
	for _, transferTask := range transferTasks {
		s.NotZero(transferTask.GetKey().TaskID)
	}

	// assert that task keys are tracked
	highReaderWatermark := s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	s.Equal(s.initialTaskID, highReaderWatermark.TaskID)

	// assert that pending task keys are cleared after calling completionFn
	completionFn(nil)
	highReaderWatermark = s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	s.Equal(s.initialTaskID+int64(numTask), highReaderWatermark.TaskID)
}

func (s *taskKeyManagerSuite) TestSetRangeID() {
	_, err := s.manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: {
			tasks.NewFakeTask(
				tests.WorkflowKey,
				tasks.CategoryTransfer,
				time.Now(),
			),
		},
	})
	s.NoError(err)

	s.Equal(
		s.initialTaskID,
		s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer).TaskID,
	)

	s.rangeID++
	s.manager.setRangeID(s.rangeID)

	expectedNextTaskID := s.rangeID << int64(s.rangeSizeBits)
	s.Equal(
		expectedNextTaskID,
		s.manager.peekTaskKey(tasks.CategoryTransfer).TaskID,
	)

	// setRangeID should also clear pending task requests
	s.Equal(
		expectedNextTaskID,
		s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer).TaskID,
	)
}

func (s *taskKeyManagerSuite) TestGetExclusiveReaderHighWatermark_NoPendingTask() {
	highReaderWatermark := s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	s.Zero(tasks.NewImmediateKey(s.initialTaskID).CompareTo(highReaderWatermark))

	now := time.Now()
	s.mockTimeSource.Update(now)

	highReaderWatermark = s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTimer)
	// for scheduled category type, we only need to make sure TaskID is 0 and FireTime is moved forwarded
	s.Zero(highReaderWatermark.TaskID)
	s.True(highReaderWatermark.FireTime.After(now))
	s.False(highReaderWatermark.FireTime.After(now.Add(s.manager.config.TimerProcessorMaxTimeShift())))
}

func (s *taskKeyManagerSuite) TestGetExclusiveReaderHighWatermark_WithPendingTask() {
	now := time.Now()
	s.mockTimeSource.Update(now)

	transferTask := tasks.NewFakeTask(
		tests.WorkflowKey,
		tasks.CategoryTransfer,
		time.Now(),
	)
	timerTask := tasks.NewFakeTask(
		tests.WorkflowKey,
		tasks.CategoryTimer,
		now.Add(-time.Minute),
	)

	// make two calls here, otherwise the order for assgining task keys is not guaranteed
	_, err := s.manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: {transferTask},
	})
	s.NoError(err)
	_, err = s.manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTimer: {timerTask},
	})
	s.NoError(err)

	highReaderWatermark := s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	s.Zero(tasks.NewImmediateKey(s.initialTaskID).CompareTo(highReaderWatermark))

	highReaderWatermark = s.manager.getExclusiveReaderHighWatermark(tasks.CategoryTimer)
	s.Zero(tasks.NewKey(timerTask.GetVisibilityTime(), 0).CompareTo(highReaderWatermark))
}
