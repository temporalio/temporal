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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	taskKeyGeneratorSuite struct {
		suite.Suite
		*require.Assertions

		rangeID       int64
		rangeSizeBits uint

		mockTimeSource *clock.EventTimeSource

		generator *taskKeyGenerator
	}
)

func TestTaskKeyGeneratorSuite(t *testing.T) {
	s := &taskKeyGeneratorSuite{}
	suite.Run(t, s)
}

func (s *taskKeyGeneratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.rangeID = 1
	s.rangeSizeBits = 3 // 1 << 3 = 8 tasks per range
	s.mockTimeSource = clock.NewEventTimeSource()
	s.generator = newTaskKeyGenerator(
		s.rangeSizeBits,
		s.mockTimeSource,
		log.NewTestLogger(),
		func() error {
			s.rangeID++
			s.generator.setRangeID(s.rangeID)
			return nil
		},
	)
	s.generator.setRangeID(s.rangeID)
	s.generator.setTaskMinScheduledTime(time.Now().Add(-time.Second))
}

func (s *taskKeyGeneratorSuite) TestSetTaskKeys_ImmediateTasks() {
	now := time.Now()
	s.mockTimeSource.Update(now)

	numTask := 5
	transferTasks := make([]tasks.Task, 0, numTask)
	for i := 0; i < numTask; i++ {
		transferTasks = append(
			transferTasks,
			tasks.NewFakeTask(
				tests.WorkflowKey,
				tasks.CategoryTransfer,
				// use some random initial timestamp for the task
				now.Add(time.Duration(time.Second*time.Duration(rand.Int63n(100)-50))),
			),
		)
	}

	err := s.generator.setTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: transferTasks,
	})
	s.NoError(err)

	expectedTaskID := int64(s.rangeID << int64(s.rangeSizeBits))
	for _, transferTask := range transferTasks {
		actualKey := transferTask.GetKey()
		expectedKey := tasks.NewImmediateKey(expectedTaskID)
		s.Zero(expectedKey.CompareTo(actualKey))
		s.Equal(now, transferTask.GetVisibilityTime())

		expectedTaskID++
	}
}

func (s *taskKeyGeneratorSuite) TestSetTaskKeys_ScheduledTasks() {
	now := time.Now().Truncate(persistence.ScheduledTaskMinPrecision)
	s.mockTimeSource.Update(now)

	timerTasks := []tasks.Task{
		tasks.NewFakeTask(tests.WorkflowKey, tasks.CategoryTimer, now.Add(-time.Minute)),
		tasks.NewFakeTask(tests.WorkflowKey, tasks.CategoryTimer, now.Add(time.Minute)),
	}
	initialTaskID := int64(s.rangeID << int64(s.rangeSizeBits))
	expectedKeys := []tasks.Key{
		tasks.NewKey(now.Add(persistence.ScheduledTaskMinPrecision), initialTaskID),
		tasks.NewKey(now.Add(time.Minute).Add(persistence.ScheduledTaskMinPrecision), initialTaskID+1),
	}

	err := s.generator.setTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTimer: timerTasks,
	})
	s.NoError(err)

	for i, timerTask := range timerTasks {
		actualKey := timerTask.GetKey()
		expectedKey := expectedKeys[i]
		s.Zero(expectedKey.CompareTo(actualKey))
	}
}

func (s *taskKeyGeneratorSuite) TestSetTaskKeys_RenewRange() {
	now := time.Now()
	s.mockTimeSource.Update(now)

	initialRangeID := s.rangeID

	numTask := 10
	s.True(numTask > (1 << s.rangeSizeBits))

	transferTasks := make([]tasks.Task, 0, numTask)
	for i := 0; i < numTask; i++ {
		transferTasks = append(
			transferTasks,
			tasks.NewFakeTask(tests.WorkflowKey, tasks.CategoryTransfer, now),
		)
	}

	err := s.generator.setTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: transferTasks,
	})
	s.NoError(err)

	expectedTaskID := int64(initialRangeID << int64(s.rangeSizeBits))
	for _, transferTask := range transferTasks {
		actualKey := transferTask.GetKey()
		expectedKey := tasks.NewImmediateKey(expectedTaskID)
		s.Zero(expectedKey.CompareTo(actualKey))
		s.Equal(now, transferTask.GetVisibilityTime())

		expectedTaskID++
	}
	s.Equal(initialRangeID+1, s.rangeID)
}

func (s *taskKeyGeneratorSuite) TestPeekAndGenerateTaskKey() {
	nextTaskID := s.rangeID << int64(s.rangeSizeBits)
	nextKey := s.generator.peekTaskKey(tasks.CategoryTransfer)
	s.Zero(tasks.NewImmediateKey(nextTaskID).CompareTo(nextKey))

	generatedKey, err := s.generator.generateTaskKey(tasks.CategoryTransfer)
	s.NoError(err)
	s.Zero(nextKey.CompareTo(generatedKey))

	nextTaskID++
	now := time.Now().Truncate(persistence.ScheduledTaskMinPrecision)
	s.mockTimeSource.Update(now)
	s.generator.setTaskMinScheduledTime(now)
	nextKey = s.generator.peekTaskKey(tasks.CategoryTimer)
	s.Zero(tasks.NewKey(now, nextTaskID).CompareTo(nextKey))

	generatedKey, err = s.generator.generateTaskKey(tasks.CategoryTimer)
	s.NoError(err)
	s.Zero(nextKey.CompareTo(generatedKey))
}
