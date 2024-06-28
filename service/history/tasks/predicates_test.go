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

package tasks

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/predicates"
)

type (
	predicatesSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestPredicateSuite(t *testing.T) {
	s := new(predicatesSuite)
	suite.Run(t, s)
}

func (s *predicatesSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *predicatesSuite) TestNamespacePredicate_Test() {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)
	for _, id := range namespaceIDs {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(id).Times(1)
		s.True(p.Test(mockTask))
	}

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).Times(1)
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestNamespacePredicate_Equals() {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)

	s.True(p.Equals(p))
	s.True(p.Equals(NewNamespacePredicate(namespaceIDs)))
	rand.Shuffle(
		len(namespaceIDs),
		func(i, j int) {
			namespaceIDs[i], namespaceIDs[j] = namespaceIDs[j], namespaceIDs[i]
		},
	)
	s.True(p.Equals(NewNamespacePredicate(namespaceIDs)))

	s.False(p.Equals(NewNamespacePredicate([]string{uuid.New(), uuid.New()})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestTypePredicate_Test() {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)
	for _, taskType := range types {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetType().Return(taskType).Times(1)
		s.True(p.Test(mockTask))
	}

	for _, taskType := range enumsspb.TaskType_value {
		if slices.Index(types, enumsspb.TaskType(taskType)) != -1 {
			continue
		}

		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(taskType)).Times(1)
		s.False(p.Test(mockTask))
	}
}

func (s *predicatesSuite) TestTypePredicate_Equals() {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)

	s.True(p.Equals(p))
	s.True(p.Equals(NewTypePredicate(types)))
	rand.Shuffle(
		len(types),
		func(i, j int) {
			types[i], types[j] = types[j], types[i]
		},
	)
	s.True(p.Equals(NewTypePredicate(types)))

	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
	})))
	s.False(p.Equals(NewNamespacePredicate([]string{uuid.New(), uuid.New()})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestDestinationPredicate_Test() {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)
	for _, dest := range destinations {
		mockTask := &StateMachineOutboundTask{Destination: dest}
		s.True(p.Test(mockTask))
	}

	mockTask := &StateMachineOutboundTask{Destination: uuid.New()}
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestDestinationPredicate_Equals() {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)

	s.True(p.Equals(p))
	s.True(p.Equals(NewDestinationPredicate(destinations)))
	rand.Shuffle(
		len(destinations),
		func(i, j int) {
			destinations[i], destinations[j] = destinations[j], destinations[i]
		},
	)
	s.True(p.Equals(NewDestinationPredicate(destinations)))

	s.False(p.Equals(NewDestinationPredicate([]string{uuid.New(), uuid.New()})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestStateMachineTaskTypePredicate_Test() {
	groups := []string{"1", "2"}

	p := NewOutboundTaskGroupPredicate(groups)
	for _, t := range groups {
		mockTask := &StateMachineOutboundTask{StateMachineTask: StateMachineTask{Info: &persistence.StateMachineTaskInfo{Type: t}}}
		s.True(p.Test(mockTask))
	}

	mockTask := &StateMachineOutboundTask{StateMachineTask: StateMachineTask{Info: &persistence.StateMachineTaskInfo{Type: "3"}}}
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestStateMachineTaskTypePredicate_Equals() {
	groups := []string{"1", "2"}

	p := NewOutboundTaskGroupPredicate(groups)

	s.True(p.Equals(p))
	s.True(p.Equals(NewOutboundTaskGroupPredicate(groups)))
	rand.Shuffle(
		len(groups),
		func(i, j int) {
			groups[i], groups[j] = groups[j], groups[i]
		},
	)
	s.True(p.Equals(NewOutboundTaskGroupPredicate(groups)))

	s.False(p.Equals(NewOutboundTaskGroupPredicate([]string{"3", "4"})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestAndPredicates() {
	testCases := []struct {
		predicateA     Predicate
		predicateB     Predicate
		expectedResult Predicate
	}{
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace2", "namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace2"}),
		},
		{
			predicateA: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
			expectedResult: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
		},
		{
			predicateA:     NewDestinationPredicate([]string{"dest1", "dest2"}),
			predicateB:     NewDestinationPredicate([]string{"dest2", "dest3"}),
			expectedResult: NewDestinationPredicate([]string{"dest2"}),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace3"}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA: NewNamespacePredicate([]string{"namespace1"}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			expectedResult: predicates.And[Task](
				NewNamespacePredicate([]string{"namespace1"}),
				NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				}),
			),
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedResult, AndPredicates(tc.predicateA, tc.predicateB))
	}
}

func (s *predicatesSuite) TestOrPredicates() {
	testCases := []struct {
		predicateA     Predicate
		predicateB     Predicate
		expectedResult Predicate
	}{
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace2", "namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"}),
		},
		{
			predicateA: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
			expectedResult: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
		},
		{
			predicateA:     NewDestinationPredicate([]string{"dest1", "dest2"}),
			predicateB:     NewDestinationPredicate([]string{"dest2", "dest3"}),
			expectedResult: NewDestinationPredicate([]string{"dest1", "dest2", "dest3"}),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"}),
		},
		{
			predicateA: NewNamespacePredicate([]string{"namespace1"}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			expectedResult: predicates.Or[Task](
				NewNamespacePredicate([]string{"namespace1"}),
				NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				}),
			),
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedResult, OrPredicates(tc.predicateA, tc.predicateB))
	}
}
