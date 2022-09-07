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

package queues

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"golang.org/x/exp/slices"
)

type (
	scheduledQueueSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockExecutionManager *persistence.MockExecutionManager

		scheduledQueue *scheduledQueue
	}
)

func TestScheduledQueueSuite(t *testing.T) {
	s := new(scheduledQueueSuite)
	suite.Run(t, s)
}

func (s *scheduledQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 1,
			},
		},
		tests.NewDynamicConfig(),
	)
	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.scheduledQueue = NewScheduledQueue(
		s.mockShard,
		tasks.CategoryTimer,
		NewFIFOScheduler(
			FIFOSchedulerOptions{
				WorkerCount: dynamicconfig.GetIntPropertyFn(10),
				QueueSize:   100,
			},
			log.NewTestLogger(),
		),
		nil,
		nil,
		testQueueOptions,
		nil,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
}

func (s *scheduledQueueSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *scheduledQueueSuite) TestPaginationFnProvider() {
	paginationFnProvider := s.scheduledQueue.paginationFnProvider

	r := NewRandomRange()

	testTaskKeys := []tasks.Key{
		tasks.NewKey(r.InclusiveMin.FireTime.Add(-time.Second), rand.Int63()),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(-time.Microsecond*10), rand.Int63()),
		tasks.NewKey(r.InclusiveMin.FireTime, r.ExclusiveMax.TaskID),
		tasks.NewKey(r.InclusiveMin.FireTime, r.ExclusiveMax.TaskID-1),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(time.Second), rand.Int63()),
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID),
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(time.Microsecond*10), rand.Int63()),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(time.Second), rand.Int63()),
	}
	slices.SortFunc(testTaskKeys, func(k1, k2 tasks.Key) bool {
		return k1.CompareTo(k2) < 0
	})
	shouldHaveNextPage := true
	if testTaskKeys[len(testTaskKeys)-1].CompareTo(r.ExclusiveMax) >= 0 {
		shouldHaveNextPage = false
	}

	expectedNumTasks := 0
	mockTasks := make([]tasks.Task, 0, len(testTaskKeys))
	for _, key := range testTaskKeys {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(key).AnyTimes()
		mockTask.EXPECT().GetVisibilityTime().Return(key.FireTime).Times(1)
		mockTask.EXPECT().SetVisibilityTime(key.FireTime.Truncate(scheduledTaskPrecision)).Times(1)
		mockTasks = append(mockTasks, mockTask)

		if r.ContainsKey(key) {
			expectedNumTasks++
		}
	}

	currentPageToken := []byte{1, 2, 3}
	nextPageToken := []byte{4, 5, 6}

	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(r.InclusiveMin.FireTime, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(r.ExclusiveMax.FireTime.Add(scheduledTaskPrecision), 0),
		BatchSize:           testQueueOptions.BatchSize(),
		NextPageToken:       currentPageToken,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         mockTasks,
		NextPageToken: nextPageToken,
	}, nil).Times(1)

	paginationFn := paginationFnProvider(r)
	loadedTasks, actualNextPageToken, err := paginationFn(currentPageToken)
	s.NoError(err)
	for _, task := range loadedTasks {
		s.True(r.ContainsKey(task.GetKey()))
	}
	s.Len(loadedTasks, expectedNumTasks)

	if shouldHaveNextPage {
		s.Equal(nextPageToken, actualNextPageToken)
	} else {
		s.Nil(actualNextPageToken)
	}
}

func (s *scheduledQueueSuite) TestLookAheadTask_HasLookAheadTask() {
	timerGate := timer.NewRemoteGate()
	s.scheduledQueue.timerGate = timerGate

	_, lookAheadTask := s.setupLookAheadMock(true)
	s.scheduledQueue.lookAheadTask()

	timerGate.SetCurrentTime(lookAheadTask.GetKey().FireTime)
	select {
	case <-s.scheduledQueue.timerGate.FireChan():
	default:
		s.Fail("timer gate should fire when look ahead task is due")
	}
}

func (s *scheduledQueueSuite) TestLookAheadTask_NoLookAheadTask() {
	timerGate := timer.NewRemoteGate()
	s.scheduledQueue.timerGate = timerGate

	lookAheadRange, _ := s.setupLookAheadMock(false)
	s.scheduledQueue.lookAheadTask()

	timerGate.SetCurrentTime(lookAheadRange.InclusiveMin.FireTime.Add(testQueueOptions.MaxPollInterval()))
	select {
	case <-s.scheduledQueue.timerGate.FireChan():
		s.Fail("timer gate should not fire")
	default:
	}
}

func (s *scheduledQueueSuite) TestLookAheadTask_ErrorLookAhead() {
	timerGate := timer.NewRemoteGate()
	s.scheduledQueue.timerGate = timerGate

	s.scheduledQueue.nonReadableScope = NewScope(
		NewRandomRange(),
		predicates.Universal[tasks.Task](),
	)

	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some random error")).Times(1)
	s.scheduledQueue.lookAheadTask()

	timerGate.SetCurrentTime(s.scheduledQueue.nonReadableScope.Range.InclusiveMin.FireTime)
	select {
	case <-s.scheduledQueue.timerGate.FireChan():
	default:
		s.Fail("timer gate should fire when time reaches look ahead range")
	}
}

func (s *scheduledQueueSuite) TestProcessNewRange_LookAheadPerformed() {
	timerGate := timer.NewRemoteGate()
	s.scheduledQueue.timerGate = timerGate

	// test if look ahead if performed after processing new range
	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{},
		NextPageToken: nil,
	}, nil).Times(1)

	s.scheduledQueue.processNewRange()
}

func (s *scheduledQueueSuite) TestProcessPollTimer_LookAheadPerformed() {
	timerGate := timer.NewRemoteGate()
	s.scheduledQueue.timerGate = timerGate
	s.scheduledQueue.pollTimer = time.NewTimer(time.Second)

	// test if look ahead if performed after processing poll timer
	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{},
		NextPageToken: nil,
	}, nil).Times(1)

	s.scheduledQueue.processPollTimer()
}

func (s *scheduledQueueSuite) setupLookAheadMock(
	hasLookAheadTask bool,
) (lookAheadRange Range, lookAheadTask *tasks.MockTask) {
	lookAheadMinTime := s.scheduledQueue.nonReadableScope.Range.InclusiveMin.FireTime
	lookAheadRange = NewRange(
		tasks.NewKey(lookAheadMinTime, 0),
		tasks.NewKey(lookAheadMinTime.Add(testQueueOptions.MaxPollInterval()), 0),
	)

	loadedTasks := []tasks.Task{}
	if hasLookAheadTask {
		lookAheadTask = tasks.NewMockTask(s.controller)
		lookAheadTask.EXPECT().GetKey().Return(NewRandomKeyInRange(lookAheadRange)).AnyTimes()

		loadedTasks = append(loadedTasks, lookAheadTask)
	}

	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: lookAheadRange.InclusiveMin,
		ExclusiveMaxTaskKey: lookAheadRange.ExclusiveMax,
		BatchSize:           1,
		NextPageToken:       nil,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         loadedTasks,
		NextPageToken: nil,
	}, nil).Times(1)

	return lookAheadRange, lookAheadTask
}
