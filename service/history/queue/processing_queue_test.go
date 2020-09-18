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

package queue

import (
	"sort"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	t "github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/task"
)

type (
	processingQueueSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		logger        log.Logger
		metricsClient metrics.Client
	}

	testKey struct {
		ID int
	}
)

func TestProcessingQueueSuite(t *testing.T) {
	s := new(processingQueueSuite)
	suite.Run(t, s)
}

func (s *processingQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
}

func (s *processingQueueSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *processingQueueSuite) TestAddTasks() {
	ackLevel := testKey{ID: 1}
	maxLevel := testKey{ID: 10}

	taskKeys := []task.Key{
		testKey{ID: 2},
		testKey{ID: 3},
		testKey{ID: 5},
		testKey{ID: 9},
	}
	tasks := make(map[task.Key]task.Task)
	for _, key := range taskKeys {
		mockTask := task.NewMockTask(s.controller)
		mockTask.EXPECT().GetDomainID().Return("some random domainID").AnyTimes()
		mockTask.EXPECT().GetWorkflowID().Return("some random workflowID").AnyTimes()
		mockTask.EXPECT().GetRunID().Return("some random runID").AnyTimes()
		mockTask.EXPECT().GetTaskType().Return(0).AnyTimes()
		tasks[key] = mockTask
	}

	queue := s.newTestProcessingQueue(
		0,
		ackLevel,
		ackLevel,
		maxLevel,
		NewDomainFilter(nil, true),
		make(map[task.Key]task.Task),
	)

	newReadLevel := testKey{ID: 10}
	queue.AddTasks(tasks, newReadLevel)
	s.Len(queue.outstandingTasks, len(taskKeys))
	s.Equal(newReadLevel, queue.state.readLevel)

	// add the same set of tasks again, should have no effect
	queue.AddTasks(tasks, newReadLevel)
	s.Len(queue.outstandingTasks, len(taskKeys))
	s.Equal(newReadLevel, queue.state.readLevel)
}

func (s *processingQueueSuite) TestUpdateAckLevel_WithPendingTasks() {
	ackLevel := testKey{ID: 1}
	maxLevel := testKey{ID: 10}

	taskKeys := []task.Key{
		testKey{ID: 2},
		testKey{ID: 3},
		testKey{ID: 5},
		testKey{ID: 8},
		testKey{ID: 10},
	}
	taskStates := []t.State{
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStateNacked,
		t.TaskStateAcked,
		t.TaskStatePending,
	}
	tasks := make(map[task.Key]task.Task)
	for i, key := range taskKeys {
		task := task.NewMockTask(s.controller)
		task.EXPECT().State().Return(taskStates[i]).AnyTimes()
		tasks[key] = task
	}

	queue := s.newTestProcessingQueue(
		0,
		ackLevel,
		taskKeys[len(taskKeys)-1],
		maxLevel,
		NewDomainFilter(nil, true),
		tasks,
	)

	newAckLevel, pendingTasks := queue.UpdateAckLevel()
	s.Equal(testKey{ID: 3}, newAckLevel)
	s.Equal(testKey{ID: 3}, queue.state.ackLevel)
	s.Equal(3, pendingTasks)
}

func (s *processingQueueSuite) TestUpdateAckLevel_NoPendingTasks() {
	ackLevel := testKey{ID: 1}
	readLevel := testKey{ID: 9}
	maxLevel := testKey{ID: 10}

	taskKeys := []task.Key{
		testKey{ID: 2},
		testKey{ID: 3},
		testKey{ID: 5},
		testKey{ID: 8},
	}
	taskStates := []t.State{
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStateAcked,
	}
	tasks := make(map[task.Key]task.Task)
	for i, key := range taskKeys {
		task := task.NewMockTask(s.controller)
		task.EXPECT().State().Return(taskStates[i]).AnyTimes()
		tasks[key] = task
	}

	queue := s.newTestProcessingQueue(
		0,
		ackLevel,
		readLevel,
		maxLevel,
		NewDomainFilter(nil, true),
		tasks,
	)

	newAckLevel, pendingTasks := queue.UpdateAckLevel()
	s.Equal(readLevel, newAckLevel)
	s.Equal(readLevel, queue.state.ackLevel)
	s.Equal(0, pendingTasks)
}

func (s *processingQueueSuite) TestUpdateAckLevel_TaskKeyLargerThanReadLevel() {
	ackLevel := testKey{ID: 1}
	readLevel := testKey{ID: 5}
	maxLevel := testKey{ID: 10}

	taskKeys := []task.Key{
		testKey{ID: 2},
		testKey{ID: 3},
		testKey{ID: 5},
		testKey{ID: 8},
		testKey{ID: 9},
	}
	taskStates := []t.State{
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStatePending,
	}
	tasks := make(map[task.Key]task.Task)
	for i, key := range taskKeys {
		task := task.NewMockTask(s.controller)
		task.EXPECT().State().Return(taskStates[i]).AnyTimes()
		tasks[key] = task
	}

	queue := s.newTestProcessingQueue(
		0,
		ackLevel,
		readLevel,
		maxLevel,
		NewDomainFilter(nil, true),
		tasks,
	)

	newAckLevel, pendingTasks := queue.UpdateAckLevel()
	s.Equal(readLevel, newAckLevel)
	s.Equal(readLevel, queue.state.ackLevel)
	s.Equal(2, pendingTasks)
}

func (s *processingQueueSuite) TestUpdateAckLevel_DeleteTaskBeyondAckLevel() {
	ackLevel := testKey{ID: 0}
	readLevel := testKey{ID: 7}
	maxLevel := testKey{ID: 10}

	taskKeys := []task.Key{
		testKey{ID: 1},
		testKey{ID: 2},
		testKey{ID: 3},
		testKey{ID: 4},
		testKey{ID: 5},
		testKey{ID: 6},
		testKey{ID: 7},
		testKey{ID: 9},
		testKey{ID: 10},
	}
	taskStates := []t.State{
		t.TaskStateAcked,
		t.TaskStatePending,
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStatePending,
		t.TaskStateAcked,
		t.TaskStateAcked,
		t.TaskStatePending,
	}
	tasks := make(map[task.Key]task.Task)
	for i, key := range taskKeys {
		task := task.NewMockTask(s.controller)
		task.EXPECT().State().Return(taskStates[i]).AnyTimes()
		tasks[key] = task
	}

	queue := s.newTestProcessingQueue(
		0,
		ackLevel,
		readLevel,
		maxLevel,
		NewDomainFilter(nil, true),
		tasks,
	)

	newAckLevel, pendingTasks := queue.UpdateAckLevel()
	s.Equal(testKey{ID: 1}, newAckLevel)
	s.Equal(testKey{ID: 1}, queue.state.ackLevel)
	s.Equal(6, pendingTasks) // 2 5 6 7 9 10
}

func (s *processingQueueSuite) TestSplit() {
	testCases := []struct {
		queue             *processingQueueImpl
		policyResult      []ProcessingQueueState
		expectedNewQueues []*processingQueueImpl
	}{
		{
			// test 1: no split needed
			queue: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 3},
				testKey{ID: 5},
				NewDomainFilter(nil, true),
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 2}, testKey{ID: 3}},
					[]string{"testDomain1", "testDomain1", "testDomain2"},
				),
			),
			policyResult: nil,
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 3},
					testKey{ID: 5},
					NewDomainFilter(nil, true),
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 2}: task.NewMockTask(s.controller),
						testKey{ID: 3}: task.NewMockTask(s.controller),
					},
				),
			},
		},
		{
			// test 2: split two domains to another level, doesn't change range
			queue: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain1": {}, "testDomain2": {}, "testDomain3": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 2}, testKey{ID: 3}, testKey{ID: 5}},
					[]string{"testDomain1", "testDomain1", "testDomain2", "testDomain3"},
				),
			),
			policyResult: []ProcessingQueueState{
				newProcessingQueueState(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: false,
					},
				),
				newProcessingQueueState(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
				),
			},
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 2}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 3}: task.NewMockTask(s.controller),
						testKey{ID: 5}: task.NewMockTask(s.controller),
					},
				),
			},
		},
		{
			// test 3: split into multiple new levels, while keeping the existing range
			queue: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    make(map[string]struct{}),
					ReverseMatch: true,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 2}, testKey{ID: 3}, testKey{ID: 5}},
					[]string{"testDomain1", "testDomain1", "testDomain2", "testDomain3"},
				),
			),
			policyResult: []ProcessingQueueState{
				newProcessingQueueState(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}},
						ReverseMatch: false,
					},
				),
				newProcessingQueueState(
					2,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain3": {}},
						ReverseMatch: false,
					},
				),
				newProcessingQueueState(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: true,
					},
				),
			},
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: true,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 2}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 3}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					2,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain3": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 5}: task.NewMockTask(s.controller),
					},
				),
			},
		},
		{
			// test 4: change the queue range
			queue: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 7},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    make(map[string]struct{}),
					ReverseMatch: true,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 2}, testKey{ID: 3}, testKey{ID: 5}, testKey{ID: 6}, testKey{ID: 7}},
					[]string{"testDomain1", "testDomain1", "testDomain2", "testDomain3", "testDomain1", "testDomain3"},
				),
			),
			policyResult: []ProcessingQueueState{
				newProcessingQueueState(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: true,
					},
				),
				newProcessingQueueState(
					0,
					testKey{ID: 5},
					testKey{ID: 7},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    make(map[string]struct{}),
						ReverseMatch: true,
					},
				),
				newProcessingQueueState(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: false,
					},
				),
			},
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: true,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 2}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 5},
					testKey{ID: 7},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    make(map[string]struct{}),
						ReverseMatch: true,
					},
					map[task.Key]task.Task{
						testKey{ID: 6}: task.NewMockTask(s.controller),
						testKey{ID: 7}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}, "testDomain3": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 3}: task.NewMockTask(s.controller),
						testKey{ID: 5}: task.NewMockTask(s.controller),
					},
				),
			},
		},
	}

	for _, tc := range testCases {
		mockPolicy := NewMockProcessingQueueSplitPolicy(s.controller)
		mockPolicy.EXPECT().Evaluate(ProcessingQueue(tc.queue)).Return(tc.policyResult).Times(1)

		newQueues := tc.queue.Split(mockPolicy)

		s.assertQueuesEqual(tc.expectedNewQueues, newQueues)
	}
}

func (s *processingQueueSuite) TestMerge() {
	testCases := []struct {
		queue1            *processingQueueImpl
		queue2            *processingQueueImpl
		expectedNewQueues []*processingQueueImpl
	}{
		{
			// test 1: no overlap in range
			queue1: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 1},
				testKey{ID: 10},
				NewDomainFilter(nil, true),
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}},
					[]string{"testDomain1"},
				),
			),
			queue2: s.newTestProcessingQueue(
				0,
				testKey{ID: 10},
				testKey{ID: 50},
				testKey{ID: 100},
				NewDomainFilter(nil, true),
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 50}},
					[]string{"testDomain2"},
				),
			),
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 1},
					testKey{ID: 10},
					NewDomainFilter(nil, true),
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 10},
					testKey{ID: 50},
					testKey{ID: 100},
					NewDomainFilter(nil, true),
					map[task.Key]task.Task{
						testKey{ID: 50}: task.NewMockTask(s.controller),
					},
				),
			},
		},
		{
			// test 2: same ack level
			queue1: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 7},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain1": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 4}, testKey{ID: 7}},
					[]string{"testDomain1", "testDomain1", "testDomain1"},
				),
			),
			queue2: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 3},
				testKey{ID: 5},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain2": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 2}, testKey{ID: 3}},
					[]string{"testDomain2", "testDomain2"},
				),
			),
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 3},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 2}: task.NewMockTask(s.controller),
						testKey{ID: 3}: task.NewMockTask(s.controller),
						testKey{ID: 4}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 5},
					testKey{ID: 7},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 7}: task.NewMockTask(s.controller),
					},
				),
			},
		},
		{
			// test 3: same max level
			queue1: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 7},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain1": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 4}, testKey{ID: 7}},
					[]string{"testDomain1", "testDomain1", "testDomain1"},
				),
			),
			queue2: s.newTestProcessingQueue(
				0,
				testKey{ID: 5},
				testKey{ID: 9},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain2": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 6}, testKey{ID: 8}, testKey{ID: 9}},
					[]string{"testDomain2", "testDomain2", "testDomain2"},
				),
			),
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 4}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 5},
					testKey{ID: 7},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 6}: task.NewMockTask(s.controller),
						testKey{ID: 7}: task.NewMockTask(s.controller),
						testKey{ID: 8}: task.NewMockTask(s.controller),
						testKey{ID: 9}: task.NewMockTask(s.controller),
					},
				),
			},
		},
		{
			// test 4: one queue contain another
			queue1: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 7},
				testKey{ID: 20},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain1": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 4}, testKey{ID: 7}},
					[]string{"testDomain1", "testDomain1", "testDomain1"},
				),
			),
			queue2: s.newTestProcessingQueue(
				0,
				testKey{ID: 5},
				testKey{ID: 9},
				testKey{ID: 10},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain2": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 6}, testKey{ID: 8}, testKey{ID: 9}},
					[]string{"testDomain2", "testDomain2", "testDomain2"},
				),
			),
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 4}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 5},
					testKey{ID: 7},
					testKey{ID: 10},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 6}: task.NewMockTask(s.controller),
						testKey{ID: 7}: task.NewMockTask(s.controller),
						testKey{ID: 8}: task.NewMockTask(s.controller),
						testKey{ID: 9}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 10},
					testKey{ID: 10},
					testKey{ID: 20},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{},
				),
			},
		},
		{
			// test 5: general case
			queue1: s.newTestProcessingQueue(
				0,
				testKey{ID: 0},
				testKey{ID: 3},
				testKey{ID: 15},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain1": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 1}, testKey{ID: 3}},
					[]string{"testDomain1", "testDomain1"},
				),
			),
			queue2: s.newTestProcessingQueue(
				0,
				testKey{ID: 5},
				testKey{ID: 17},
				testKey{ID: 20},
				DomainFilter{
					DomainIDs:    map[string]struct{}{"testDomain2": {}},
					ReverseMatch: false,
				},
				s.newMockTasksForDomain(
					[]task.Key{testKey{ID: 6}, testKey{ID: 8}, testKey{ID: 9}, testKey{ID: 17}},
					[]string{"testDomain2", "testDomain2", "testDomain2", "testDomain2"},
				),
			),
			expectedNewQueues: []*processingQueueImpl{
				s.newTestProcessingQueue(
					0,
					testKey{ID: 0},
					testKey{ID: 3},
					testKey{ID: 5},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 1}: task.NewMockTask(s.controller),
						testKey{ID: 3}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 5},
					testKey{ID: 5},
					testKey{ID: 15},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 6}: task.NewMockTask(s.controller),
						testKey{ID: 8}: task.NewMockTask(s.controller),
						testKey{ID: 9}: task.NewMockTask(s.controller),
					},
				),
				s.newTestProcessingQueue(
					0,
					testKey{ID: 15},
					testKey{ID: 17},
					testKey{ID: 20},
					DomainFilter{
						DomainIDs:    map[string]struct{}{"testDomain2": {}},
						ReverseMatch: false,
					},
					map[task.Key]task.Task{
						testKey{ID: 17}: task.NewMockTask(s.controller),
					},
				),
			},
		},
	}

	for _, tc := range testCases {
		queue1 := s.copyProcessingQueue(tc.queue1)
		queue2 := s.copyProcessingQueue(tc.queue2)
		s.assertQueuesEqual(tc.expectedNewQueues, queue1.Merge(queue2))

		queue1 = s.copyProcessingQueue(tc.queue1)
		queue2 = s.copyProcessingQueue(tc.queue2)
		s.assertQueuesEqual(tc.expectedNewQueues, queue2.Merge(queue1))
	}
}

func (s *processingQueueSuite) assertQueuesEqual(
	expectedQueues []*processingQueueImpl,
	actual []ProcessingQueue,
) {
	s.Equal(len(expectedQueues), len(actual))

	actualQueues := make([]*processingQueueImpl, 0, len(actual))
	for _, queue := range actual {
		actualQueues = append(actualQueues, queue.(*processingQueueImpl))
	}

	compFn := func(q1, q2 *processingQueueImpl) bool {
		if taskKeyEquals(q1.state.ackLevel, q2.state.ackLevel) {
			return q1.state.level < q2.state.level
		}

		return q1.state.ackLevel.Less(q2.state.ackLevel)
	}

	sort.Slice(expectedQueues, func(i, j int) bool {
		return compFn(expectedQueues[i], expectedQueues[j])
	})
	sort.Slice(actualQueues, func(i, j int) bool {
		return compFn(actualQueues[i], actualQueues[j])
	})

	for i := 0; i != len(expectedQueues); i++ {
		s.assertQueueEqual(expectedQueues[i], actualQueues[i])
	}
}

func (s *processingQueueSuite) assertQueueEqual(
	expected *processingQueueImpl,
	actual *processingQueueImpl,
) {
	s.Equal(expected.state, actual.state)
	s.Equal(len(expected.outstandingTasks), len(actual.outstandingTasks))
	expectedKeys := make([]task.Key, 0, len(expected.outstandingTasks))
	for key := range expected.outstandingTasks {
		expectedKeys = append(expectedKeys, key)
	}
	actualKeys := make([]task.Key, 0, len(actual.outstandingTasks))
	for key := range actual.outstandingTasks {
		actualKeys = append(actualKeys, key)
	}
	sort.Slice(expectedKeys, func(i, j int) bool {
		return expectedKeys[i].Less(expectedKeys[j])
	})
	sort.Slice(actualKeys, func(i, j int) bool {
		return actualKeys[i].Less(actualKeys[j])
	})
	for i := 0; i != len(expectedKeys); i++ {
		s.True(taskKeyEquals(expectedKeys[i], actualKeys[i]))
	}
}

func (s *processingQueueSuite) copyProcessingQueue(
	queue *processingQueueImpl,
) *processingQueueImpl {
	tasks := make(map[task.Key]task.Task)
	for key, task := range queue.outstandingTasks {
		tasks[key] = task
	}

	return s.newTestProcessingQueue(
		queue.state.level,
		queue.state.ackLevel,
		queue.state.readLevel,
		queue.state.maxLevel,
		queue.state.domainFilter.copy(),
		tasks,
	)
}

func (s *processingQueueSuite) newTestProcessingQueue(
	level int,
	ackLevel task.Key,
	readLevel task.Key,
	maxLevel task.Key,
	domainFilter DomainFilter,
	outstandingTasks map[task.Key]task.Task,
) *processingQueueImpl {
	return newProcessingQueue(
		&processingQueueStateImpl{
			level:        level,
			ackLevel:     ackLevel,
			readLevel:    readLevel,
			maxLevel:     maxLevel,
			domainFilter: domainFilter,
		},
		outstandingTasks,
		s.logger,
		s.metricsClient,
	)
}

func (s *processingQueueSuite) newMockTasksForDomain(
	keys []task.Key,
	domainID []string,
) map[task.Key]task.Task {
	tasks := make(map[task.Key]task.Task)
	s.Equal(len(keys), len(domainID))

	for i := 0; i != len(keys); i++ {
		mockTask := task.NewMockTask(s.controller)
		mockTask.EXPECT().GetDomainID().Return(domainID[i]).AnyTimes()
		tasks[keys[i]] = mockTask
	}

	return tasks
}

func (k testKey) Less(key task.Key) bool {
	return k.ID < key.(testKey).ID
}
