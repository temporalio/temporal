// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queue

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/service/history/task"
)

type (
	processingQueueCollectionSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		level int
	}
)

func TestProcessingQueueCollectionSuite(t *testing.T) {
	s := new(processingQueueCollectionSuite)
	suite.Run(t, s)
}

func (s *processingQueueCollectionSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.level = 0
}

func (s *processingQueueCollectionSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *processingQueueCollectionSuite) TestNewCollection_EmptyQueues() {
	queueCollection := NewProcessingQueueCollection(s.level, nil).(*processingQueueCollection)

	s.Nil(queueCollection.ActiveQueue())
}

func (s *processingQueueCollectionSuite) TestNewCollection_OutOfOrderQueues() {
	totalQueues := 4
	mockQueues := []ProcessingQueue{}
	for i := 0; i != totalQueues; i++ {
		mockQueues = append(mockQueues, NewMockProcessingQueue(s.controller))
	}
	mockQueues[0].(*MockProcessingQueue).EXPECT().State().Return(newProcessingQueueState(
		s.level,
		testKey{ID: 20},
		testKey{ID: 25},
		testKey{ID: 30},
		DomainFilter{},
	)).AnyTimes()
	mockQueues[1].(*MockProcessingQueue).EXPECT().State().Return(newProcessingQueueState(
		s.level,
		testKey{ID: 3},
		testKey{ID: 10},
		testKey{ID: 10},
		DomainFilter{},
	)).AnyTimes()
	mockQueues[2].(*MockProcessingQueue).EXPECT().State().Return(newProcessingQueueState(
		s.level,
		testKey{ID: 30},
		testKey{ID: 30},
		testKey{ID: 40},
		DomainFilter{},
	)).AnyTimes()
	mockQueues[3].(*MockProcessingQueue).EXPECT().State().Return(newProcessingQueueState(
		s.level,
		testKey{ID: 10},
		testKey{ID: 20},
		testKey{ID: 20},
		DomainFilter{},
	)).AnyTimes()
	expectedActiveQueue := mockQueues[0]

	queueCollection := NewProcessingQueueCollection(s.level, mockQueues).(*processingQueueCollection)

	s.Equal(expectedActiveQueue.State(), queueCollection.ActiveQueue().State())
	s.True(s.isQueuesSorted(queueCollection.queues))
}

func (s *processingQueueCollectionSuite) TestAddTasks_ReadNotFinished() {
	totalQueues := 4
	currentActiveIdx := 1
	newReadLevel := testKey{ID: 9}

	mockQueues := []*MockProcessingQueue{}
	for i := 0; i != totalQueues; i++ {
		mockQueues = append(mockQueues, NewMockProcessingQueue(s.controller))
	}
	mockQueues[currentActiveIdx].EXPECT().AddTasks(gomock.Any(), newReadLevel).Times(1)
	mockQueues[currentActiveIdx].EXPECT().State().Return(newProcessingQueueState(
		s.level,
		testKey{ID: 3},
		newReadLevel,
		testKey{ID: 10},
		DomainFilter{},
	)).AnyTimes()

	queueCollection := s.newTestProcessingQueueCollection(s.level, mockQueues)
	queueCollection.activeQueue = mockQueues[currentActiveIdx]

	queueCollection.AddTasks(map[task.Key]task.Task{}, newReadLevel)
	s.Equal(mockQueues[currentActiveIdx].State(), queueCollection.ActiveQueue().State())
}

func (s *processingQueueCollectionSuite) TestAddTask_ReadFinished() {
	totalQueues := 4
	currentActiveIdx := 1
	newReadLevel := testKey{ID: 10}

	mockQueues := []*MockProcessingQueue{}
	for i := 0; i != totalQueues; i++ {
		mockQueues = append(mockQueues, NewMockProcessingQueue(s.controller))
	}
	mockQueues[currentActiveIdx].EXPECT().AddTasks(gomock.Any(), newReadLevel).Times(1)
	for i := 0; i != totalQueues; i++ {
		mockQueues[i].EXPECT().State().Return(newProcessingQueueState(
			s.level,
			testKey{ID: 3},
			newReadLevel,
			testKey{ID: 10},
			DomainFilter{},
		)).AnyTimes()
	}

	queueCollection := s.newTestProcessingQueueCollection(s.level, mockQueues)
	queueCollection.activeQueue = mockQueues[currentActiveIdx]

	queueCollection.AddTasks(map[task.Key]task.Task{}, newReadLevel)
	s.Nil(queueCollection.ActiveQueue())
}

func (s *processingQueueCollectionSuite) TestUpdateAckLevels() {
	totalQueues := 5
	currentActiveIdx := 1
	mockQueues := []*MockProcessingQueue{}
	for i := 0; i != totalQueues; i++ {
		mockQueues = append(mockQueues, NewMockProcessingQueue(s.controller))
	}

	finishedQueueIdx := map[int]struct{}{0: {}, 2: {}, 3: {}}
	for i := 0; i != totalQueues; i++ {
		if _, ok := finishedQueueIdx[i]; ok {
			mockQueues[i].EXPECT().UpdateAckLevel().Return(testKey{ID: i}, 0).Times(1)
			mockQueues[i].EXPECT().State().Return(newProcessingQueueState(
				s.level,
				testKey{ID: i},
				testKey{ID: i},
				testKey{ID: i},
				DomainFilter{},
			)).AnyTimes()
		} else {
			mockQueues[i].EXPECT().UpdateAckLevel().Return(testKey{ID: i - i}, 1).Times(1)
			mockQueues[i].EXPECT().State().Return(newProcessingQueueState(
				s.level,
				testKey{ID: i - 1},
				testKey{ID: i},
				testKey{ID: i},
				DomainFilter{},
			)).AnyTimes()
		}
	}
	expectedActiveQueue := mockQueues[1]

	queueCollection := s.newTestProcessingQueueCollection(s.level, mockQueues)
	queueCollection.activeQueue = mockQueues[currentActiveIdx]

	ackLevel, totalPendingTasks := queueCollection.UpdateAckLevels()
	s.Equal(testKey{ID: 0}, ackLevel)
	s.Equal(totalQueues-len(finishedQueueIdx), totalPendingTasks)
	s.Len(queueCollection.queues, totalQueues-len(finishedQueueIdx))
	s.Equal(expectedActiveQueue.State(), queueCollection.ActiveQueue().State())
}

func (s *processingQueueCollectionSuite) TestSplit() {
	testCases := []struct {
		currentQueueStates []ProcessingQueueState
		splitResults       [][]ProcessingQueueState

		expectedActiveQueueState     ProcessingQueueState
		expectedNewQueueStates       []ProcessingQueueState
		expectedNextLevelQueueStates []ProcessingQueueState
	}{
		{}, // empty queue collection
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}}},
				),
			},
			splitResults: [][]ProcessingQueueState{
				{
					newProcessingQueueState(
						s.level,
						testKey{ID: 0},
						testKey{ID: 5},
						testKey{ID: 10},
						DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
					),
					newProcessingQueueState(
						s.level+1,
						testKey{ID: 0},
						testKey{ID: 5},
						testKey{ID: 10},
						DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
					),
				},
			},
			expectedActiveQueueState: newProcessingQueueState(
				s.level,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
			),
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
			},
			expectedNextLevelQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level+1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
		},
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 10},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 15},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}, "domain3": {}}},
				),
			},
			splitResults: [][]ProcessingQueueState{
				{
					newProcessingQueueState(
						s.level+1,
						testKey{ID: 0},
						testKey{ID: 5},
						testKey{ID: 5},
						DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
					),
					newProcessingQueueState(
						s.level,
						testKey{ID: 5},
						testKey{ID: 10},
						testKey{ID: 10},
						DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}}},
					),
					newProcessingQueueState(
						s.level,
						testKey{ID: 0},
						testKey{ID: 5},
						testKey{ID: 5},
						DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
					),
				},
				{
					newProcessingQueueState(
						s.level,
						testKey{ID: 10},
						testKey{ID: 15},
						testKey{ID: 20},
						DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}, "domain3": {}}},
					),
				},
			},
			expectedActiveQueueState: newProcessingQueueState(
				s.level,
				testKey{ID: 10},
				testKey{ID: 15},
				testKey{ID: 20},
				DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}, "domain3": {}}},
			),
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 5},
					testKey{ID: 10},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 15},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}, "domain3": {}}},
				),
			},
			expectedNextLevelQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level+1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
		},
	}

	for _, tc := range testCases {
		mockQueues := []ProcessingQueue{}
		for idx, queueState := range tc.currentQueueStates {
			mockQueue := NewMockProcessingQueue(s.controller)
			mockQueue.EXPECT().State().Return(queueState).AnyTimes()

			splitMockQueues := []ProcessingQueue{}
			for _, splitQueueState := range tc.splitResults[idx] {
				splitMockQueue := NewMockProcessingQueue(s.controller)
				splitMockQueue.EXPECT().State().Return(splitQueueState).AnyTimes()
				splitMockQueues = append(splitMockQueues, splitMockQueue)
			}
			mockQueue.EXPECT().Split(gomock.Any()).Return(splitMockQueues)

			mockQueues = append(mockQueues, mockQueue)
		}

		queueCollection := NewProcessingQueueCollection(s.level, mockQueues).(*processingQueueCollection)
		nextLevelQueues := queueCollection.Split(NewMockProcessingQueueSplitPolicy(s.controller))

		if tc.expectedActiveQueueState != nil {
			s.Equal(tc.expectedActiveQueueState, queueCollection.ActiveQueue().State())
		}
		for idx, expectedState := range tc.expectedNewQueueStates {
			s.Equal(expectedState, queueCollection.queues[idx].State())
		}
		for idx, expectedState := range tc.expectedNextLevelQueueStates {
			s.Equal(expectedState, nextLevelQueues[idx].State())
		}
	}
}

func (s *processingQueueCollectionSuite) TestMerge() {
	testCases := []struct {
		currentQueueStates  []ProcessingQueueState
		incomingQueueStates []ProcessingQueueState

		expectedActiveQueueState ProcessingQueueState
		expectedNewQueueStates   []ProcessingQueueState
	}{
		{}, // empty queue collection
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 20},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
			},
			incomingQueueStates:      nil,
			expectedActiveQueueState: nil,
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 20},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
			},
		},
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 20},
					testKey{ID: 25},
					testKey{ID: 30},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
			incomingQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 20},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
			},
			expectedActiveQueueState: newProcessingQueueState(
				s.level,
				testKey{ID: 20},
				testKey{ID: 25},
				testKey{ID: 30},
				DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
			),
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 20},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 20},
					testKey{ID: 25},
					testKey{ID: 30},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
		},
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 10},
					testKey{ID: 50},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
			},
			incomingQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 20},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
			expectedActiveQueueState: newProcessingQueueState(
				s.level,
				testKey{ID: 10},
				testKey{ID: 10},
				testKey{ID: 20},
				DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}}},
			),
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 10},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 10},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain2": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 20},
					testKey{ID: 20},
					testKey{ID: 50},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
			},
		},
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 20},
					testKey{ID: 20},
					testKey{ID: 30},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 60},
					testKey{ID: 75},
					testKey{ID: 70},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
			incomingQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 8},
					testKey{ID: 35},
					testKey{ID: 50},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 80},
					testKey{ID: 90},
					testKey{ID: 100},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
			},
			expectedActiveQueueState: newProcessingQueueState(
				s.level,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 8},
				DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
			),
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 8},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 8},
					testKey{ID: 8},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 20},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 20},
					testKey{ID: 20},
					testKey{ID: 30},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}, "domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 30},
					testKey{ID: 35},
					testKey{ID: 50},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 60},
					testKey{ID: 75},
					testKey{ID: 70},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 80},
					testKey{ID: 90},
					testKey{ID: 100},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
			},
		},
		{
			currentQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 15},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 30},
					testKey{ID: 40},
					testKey{ID: 50},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 60},
					testKey{ID: 65},
					testKey{ID: 70},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}}},
				),
			},
			incomingQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 15},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 18},
					testKey{ID: 18},
					testKey{ID: 100},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
			},
			expectedActiveQueueState: newProcessingQueueState(
				s.level,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
			),
			expectedNewQueueStates: []ProcessingQueueState{
				newProcessingQueueState(
					s.level,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 10},
					testKey{ID: 10},
					testKey{ID: 15},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 15},
					testKey{ID: 15},
					testKey{ID: 18},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 18},
					testKey{ID: 18},
					testKey{ID: 20},
					DomainFilter{DomainIDs: map[string]struct{}{"domain1": {}, "domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 20},
					testKey{ID: 20},
					testKey{ID: 30},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 30},
					testKey{ID: 30},
					testKey{ID: 50},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}, "domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 50},
					testKey{ID: 50},
					testKey{ID: 60},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 60},
					testKey{ID: 60},
					testKey{ID: 70},
					DomainFilter{DomainIDs: map[string]struct{}{"domain2": {}, "domain3": {}}},
				),
				newProcessingQueueState(
					s.level,
					testKey{ID: 70},
					testKey{ID: 70},
					testKey{ID: 100},
					DomainFilter{DomainIDs: map[string]struct{}{"domain3": {}}},
				),
			},
		},
	}

	for _, tc := range testCases {
		queues := []ProcessingQueue{}
		for _, queueState := range tc.currentQueueStates {
			queue := NewProcessingQueue(queueState, nil, nil)
			queues = append(queues, queue)
		}

		incomingQueues := []ProcessingQueue{}
		for _, queueState := range tc.incomingQueueStates {
			incomingQueue := NewProcessingQueue(queueState, nil, nil)
			incomingQueues = append(incomingQueues, incomingQueue)
		}

		queueCollection := NewProcessingQueueCollection(s.level, queues).(*processingQueueCollection)
		queueCollection.Merge(incomingQueues)

		if tc.expectedActiveQueueState != nil {
			s.Equal(tc.expectedActiveQueueState, queueCollection.ActiveQueue().State())
		}
		for idx, expectedState := range tc.expectedNewQueueStates {
			s.Equal(expectedState, queueCollection.queues[idx].State())
		}
	}
}

func (s *processingQueueCollectionSuite) isQueuesSorted(
	queues []ProcessingQueue,
) bool {
	if len(queues) <= 1 {
		return true
	}

	for i := 0; i != len(queues)-1; i++ {
		if !queues[i].State().AckLevel().Less(queues[i+1].State().AckLevel()) ||
			queues[i+1].State().AckLevel().Less(queues[i].State().MaxLevel()) {
			return false
		}
	}

	return true
}

func (s *processingQueueCollectionSuite) newTestProcessingQueueCollection(
	level int,
	mockQueues []*MockProcessingQueue,
) *processingQueueCollection {
	queues := make([]ProcessingQueue, 0, len(mockQueues))
	for _, mockQueue := range mockQueues {
		queues = append(queues, mockQueue)
	}

	return &processingQueueCollection{
		level:  level,
		queues: queues,
	}
}
