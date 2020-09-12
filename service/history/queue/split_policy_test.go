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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
	t "github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/task"
)

type (
	splitPolicySuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		logger       log.Logger
		metricsScope metrics.Scope
	}
)

func TestSplitPolicySuite(t *testing.T) {
	s := new(splitPolicySuite)
	suite.Run(t, s)
}

func (s *splitPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.metricsScope = metrics.NewClient(tally.NoopScope, metrics.History).Scope(metrics.TimerQueueProcessorScope)
}

func (s *splitPolicySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *splitPolicySuite) TestPendingTaskSplitPolicy() {
	maxNewQueueLevel := 3
	pendingTaskThreshold := map[int]int{
		0: 10,
		1: 100,
		2: 1000,
		3: 10000,
	}
	lookAheadTasks := 5
	lookAheadFunc := func(key task.Key, _ string) task.Key {
		currentID := key.(testKey).ID
		return testKey{ID: currentID + lookAheadTasks}
	}
	pendingTaskSplitPolicy := NewPendingTaskSplitPolicy(
		pendingTaskThreshold,
		dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
		lookAheadFunc,
		maxNewQueueLevel,
		s.logger,
		s.metricsScope,
	)

	testCases := []struct {
		currentState             ProcessingQueueState
		numPendingTasksPerDomain map[string]int //domainID -> number of pending tasks
		expectedNewStates        []ProcessingQueueState
	}{
		{
			currentState: newProcessingQueueState(
				101, // a level which has no threshold specified
				testKey{ID: 0},
				testKey{ID: 1000},
				testKey{ID: 10000},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			numPendingTasksPerDomain: map[string]int{
				"testDomain1": 1000,
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				3, // maxNewQueueLevel
				testKey{ID: 0},
				testKey{ID: 100000},
				testKey{ID: 1000000},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			numPendingTasksPerDomain: map[string]int{
				"testDomain2": 100000,
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				1,
				testKey{ID: 0},
				testKey{ID: 198},
				testKey{ID: 200},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			numPendingTasksPerDomain: map[string]int{
				"testDomain1": 99,
				"testDomain2": 99,
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				2,
				testKey{ID: 0},
				testKey{ID: 2002},
				testKey{ID: 2002 + lookAheadTasks + 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			numPendingTasksPerDomain: map[string]int{
				"testDomain1": 1001,
				"testDomain2": 1001,
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					3,
					testKey{ID: 0},
					testKey{ID: 2002},
					testKey{ID: 2002 + lookAheadTasks},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						false,
					),
				),
				newProcessingQueueState(
					2,
					testKey{ID: 2002 + lookAheadTasks},
					testKey{ID: 2002 + lookAheadTasks},
					testKey{ID: 2002 + lookAheadTasks + 10},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						false,
					),
				),
			},
		},
		{
			currentState: newProcessingQueueState(
				2,
				testKey{ID: 0},
				testKey{ID: 1001},
				testKey{ID: 1001 + lookAheadTasks},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}},
					false,
				),
			),
			numPendingTasksPerDomain: map[string]int{
				"testDomain1": 1001,
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					3,
					testKey{ID: 0},
					testKey{ID: 1001},
					testKey{ID: 1001 + lookAheadTasks},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}},
						false,
					),
				),
			},
		},
		{
			currentState: newProcessingQueueState(
				0,
				testKey{ID: 0},
				testKey{ID: 109},
				testKey{ID: 109 + lookAheadTasks + 100},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}},
					true,
				),
			),
			numPendingTasksPerDomain: map[string]int{
				"testDomain2": 9,
				"testDomain3": 100,
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					0,
					testKey{ID: 0},
					testKey{ID: 109},
					testKey{ID: 109 + lookAheadTasks},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain3": {}},
						true,
					),
				),
				newProcessingQueueState(
					0,
					testKey{ID: 109 + lookAheadTasks},
					testKey{ID: 109 + lookAheadTasks},
					testKey{ID: 109 + lookAheadTasks + 100},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}},
						true,
					),
				),
				newProcessingQueueState(
					1,
					testKey{ID: 0},
					testKey{ID: 109},
					testKey{ID: 109 + lookAheadTasks},
					NewDomainFilter(
						map[string]struct{}{"testDomain3": {}},
						false,
					),
				),
			},
		},
	}

	for _, tc := range testCases {
		outstandingTasks := make(map[task.Key]task.Task)
		for domainID, numPendingTasks := range tc.numPendingTasksPerDomain {
			for i := 0; i != numPendingTasks; i++ {
				mockTask := task.NewMockTask(s.controller)
				mockTask.EXPECT().GetDomainID().Return(domainID).MaxTimes(1)
				mockTask.EXPECT().State().Return(t.TaskStatePending).MaxTimes(1)
				outstandingTasks[task.NewMockKey(s.controller)] = mockTask
			}
		}

		queue := newProcessingQueue(
			tc.currentState,
			outstandingTasks,
			nil,
			nil,
		)

		s.assertQueueStatesEqual(tc.expectedNewStates, pendingTaskSplitPolicy.Evaluate(queue))
	}
}

func (s *splitPolicySuite) TestStuckTaskSplitPolicy() {
	maxNewQueueLevel := 3
	attemptThreshold := map[int]int{
		0: 10,
		1: 100,
		2: 1000,
		3: 10000,
	}

	stuckTaskSplitPolicy := NewStuckTaskSplitPolicy(
		attemptThreshold,
		dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
		maxNewQueueLevel,
		s.logger,
		s.metricsScope,
	)

	testCases := []struct {
		currentState        ProcessingQueueState
		pendingTaskAttempts map[string][]int // domainID -> list of task attempts
		expectedNewStates   []ProcessingQueueState
	}{
		{
			currentState: newProcessingQueueState(
				101, // a level which has no threshold specified
				testKey{ID: 0},
				testKey{ID: 3},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			pendingTaskAttempts: map[string][]int{
				"testDomain1": {1, 1000, 10000},
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				3, // maxNewQueueLevel
				testKey{ID: 0},
				testKey{ID: 1},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			pendingTaskAttempts: map[string][]int{
				"testDomain2": {100000},
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				1,
				testKey{ID: 0},
				testKey{ID: 4},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			pendingTaskAttempts: map[string][]int{
				"testDomain1": {0, 99},
				"testDomain2": {0, 99},
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				2,
				testKey{ID: 0},
				testKey{ID: 4},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			pendingTaskAttempts: map[string][]int{
				"testDomain1": {0, 99, 1001},
				"testDomain2": {1001},
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					3,
					testKey{ID: 0},
					testKey{ID: 4},
					testKey{ID: 4},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						false,
					),
				),
				newProcessingQueueState(
					2,
					testKey{ID: 4},
					testKey{ID: 4},
					testKey{ID: 10},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
						false,
					),
				),
			},
		},
		{
			currentState: newProcessingQueueState(
				2,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 5},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}},
					false,
				),
			),
			pendingTaskAttempts: map[string][]int{
				"testDomain1": {0, 1, 99, 1001, 0},
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					3,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}},
						false,
					),
				),
			},
		},
		{
			currentState: newProcessingQueueState(
				0,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}},
					true,
				),
			),
			pendingTaskAttempts: map[string][]int{
				"testDomain2": {0, 1, 9},
				"testDomain3": {1, 100},
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain3": {}},
						true,
					),
				),
				newProcessingQueueState(
					0,
					testKey{ID: 5},
					testKey{ID: 5},
					testKey{ID: 10},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}},
						true,
					),
				),
				newProcessingQueueState(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 5},
					NewDomainFilter(
						map[string]struct{}{"testDomain3": {}},
						false,
					),
				),
			},
		},
	}

	for _, tc := range testCases {
		outstandingTasks := make(map[task.Key]task.Task)
		for domainID, taskAttempts := range tc.pendingTaskAttempts {
			for _, attempt := range taskAttempts {
				mockTask := task.NewMockTask(s.controller)
				mockTask.EXPECT().GetDomainID().Return(domainID).MaxTimes(1)
				mockTask.EXPECT().GetAttempt().Return(attempt).MaxTimes(1)
				outstandingTasks[task.NewMockKey(s.controller)] = mockTask
			}
		}

		queue := newProcessingQueue(
			tc.currentState,
			outstandingTasks,
			nil,
			nil,
		)

		s.assertQueueStatesEqual(tc.expectedNewStates, stuckTaskSplitPolicy.Evaluate(queue))
	}
}

func (s *splitPolicySuite) TestSelectedDomainSplitPolicy() {
	newQueueLevel := 123

	testCases := []struct {
		currentState      ProcessingQueueState
		domainToSplit     map[string]struct{}
		expectedNewStates []ProcessingQueueState
	}{
		{
			currentState: newProcessingQueueState(
				0,
				testKey{ID: 0},
				testKey{ID: 0},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			domainToSplit:     map[string]struct{}{"testDomain3": {}, "testDomain4": {}},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				0,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					true,
				),
			),
			domainToSplit: map[string]struct{}{"testDomain3": {}},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					0,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}, "testDomain2": {}, "testDomain3": {}},
						true,
					),
				),
				newProcessingQueueState(
					newQueueLevel,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					NewDomainFilter(
						map[string]struct{}{"testDomain3": {}},
						false,
					),
				),
			},
		},
	}

	for _, tc := range testCases {
		queue := NewProcessingQueue(tc.currentState, nil, nil)
		splitPolicy := NewSelectedDomainSplitPolicy(tc.domainToSplit, newQueueLevel, s.logger, s.metricsScope)

		s.assertQueueStatesEqual(tc.expectedNewStates, splitPolicy.Evaluate(queue))
	}
}

func (s *splitPolicySuite) TestRandomSplitPolicy() {
	maxNewQueueLevel := 3
	lookAheadFunc := func(key task.Key, _ string) task.Key {
		currentID := key.(testKey).ID
		return testKey{ID: currentID + 10}
	}

	testCases := []struct {
		currentState             ProcessingQueueState
		splitProbability         float64
		numPendingTasksPerDomain map[string]int //domainID -> number of pending tasks
		expectedNewStates        []ProcessingQueueState
	}{
		{
			currentState: newProcessingQueueState(
				0,
				testKey{ID: 0},
				testKey{ID: 0},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			splitProbability:         0,
			numPendingTasksPerDomain: nil,
			expectedNewStates:        nil,
		},
		{
			currentState: newProcessingQueueState(
				3,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}, "testDomain2": {}},
					false,
				),
			),
			splitProbability: 1,
			numPendingTasksPerDomain: map[string]int{
				"testDomain1": 2,
				"testDomain2": 3,
			},
			expectedNewStates: nil,
		},
		{
			currentState: newProcessingQueueState(
				0,
				testKey{ID: 0},
				testKey{ID: 5},
				testKey{ID: 10},
				NewDomainFilter(
					map[string]struct{}{"testDomain1": {}},
					false,
				),
			),
			splitProbability: 1,
			numPendingTasksPerDomain: map[string]int{
				"testDomain1": 5,
			},
			expectedNewStates: []ProcessingQueueState{
				newProcessingQueueState(
					1,
					testKey{ID: 0},
					testKey{ID: 5},
					testKey{ID: 10},
					NewDomainFilter(
						map[string]struct{}{"testDomain1": {}},
						false,
					),
				),
			},
		},
	}

	for _, tc := range testCases {
		outstandingTasks := make(map[task.Key]task.Task)
		for domainID, numPendingTasks := range tc.numPendingTasksPerDomain {
			for i := 0; i != numPendingTasks; i++ {
				mockTask := task.NewMockTask(s.controller)
				mockTask.EXPECT().GetDomainID().Return(domainID).MaxTimes(1)
				outstandingTasks[task.NewMockKey(s.controller)] = mockTask
			}
		}

		queue := newProcessingQueue(
			tc.currentState,
			outstandingTasks,
			nil,
			nil,
		)
		splitPolicy := NewRandomSplitPolicy(
			tc.splitProbability,
			dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
			maxNewQueueLevel,
			lookAheadFunc,
			s.logger,
			s.metricsScope,
		)

		s.assertQueueStatesEqual(tc.expectedNewStates, splitPolicy.Evaluate(queue))
	}
}

func (s *splitPolicySuite) TestAggregatedSplitPolicy() {
	expectedNewStates := []ProcessingQueueState{
		NewMockProcessingQueueState(s.controller),
		NewMockProcessingQueueState(s.controller),
		NewMockProcessingQueueState(s.controller),
	}

	mockProcessingQueue := NewMockProcessingQueue(s.controller)

	totalPolicyNum := 4
	policyEvaluationResults := [][]ProcessingQueueState{
		nil,
		{},
		expectedNewStates,
	}

	mockSplitPolicies := []ProcessingQueueSplitPolicy{}
	for i := 0; i != totalPolicyNum; i++ {
		mockSplitPolicy := NewMockProcessingQueueSplitPolicy(s.controller)
		if i < len(policyEvaluationResults) {
			mockSplitPolicy.EXPECT().Evaluate(mockProcessingQueue).Return(policyEvaluationResults[i]).Times(1)
		}
		mockSplitPolicies = append(mockSplitPolicies, mockSplitPolicy)
	}

	aggregatedSplitPolicy := NewAggregatedSplitPolicy(mockSplitPolicies...)
	s.Equal(expectedNewStates, aggregatedSplitPolicy.Evaluate(mockProcessingQueue))
}

func (s *splitPolicySuite) assertQueueStatesEqual(
	expected []ProcessingQueueState,
	actual []ProcessingQueueState,
) {
	s.Equal(len(expected), len(actual))

	if len(expected) == 0 {
		return
	}

	compFn := func(s1, s2 ProcessingQueueState) bool {
		if taskKeyEquals(s1.AckLevel(), s2.AckLevel()) {
			return s1.Level() < s2.Level()
		}

		return s1.AckLevel().Less(s2.AckLevel())
	}

	sort.Slice(expected, func(i, j int) bool {
		return compFn(expected[i], expected[j])
	})
	sort.Slice(actual, func(i, j int) bool {
		return compFn(actual[i], actual[j])
	})

	for i := 0; i != len(expected); i++ {
		s.Equal(expected[i], actual[i])
	}
}
