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
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

type (
	scopeSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestScopeSuite(t *testing.T) {
	s := new(scopeSuite)
	suite.Run(t, s)
}

func (s *scopeSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *scopeSuite) TearDownSuite() {
	s.controller.Finish()
}

func (s *scopeSuite) TestContains() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()

		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).Times(1)
		s.True(scope.Contains(mockTask))

		mockTask.EXPECT().GetKey().Return(tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1)).Times(1)
		s.False(scope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).MaxTimes(1)
	s.False(scope.Contains(mockTask))
}

func (s *scopeSuite) TestCanSplitByRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	s.True(scope.CanSplitByRange(r.InclusiveMin))
	s.True(scope.CanSplitByRange(r.ExclusiveMax))
	s.True(scope.CanSplitByRange(NewRandomKeyInRange(r)))

	s.False(scope.CanSplitByRange(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	s.False(scope.CanSplitByRange(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func (s *scopeSuite) TestSplitByRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	splitKey := NewRandomKeyInRange(r)

	leftScope, rightScope := scope.SplitByRange(splitKey)
	s.Equal(NewRange(r.InclusiveMin, splitKey), leftScope.Range)
	s.Equal(NewRange(splitKey, r.ExclusiveMax), rightScope.Range)
	s.Equal(predicate, leftScope.Predicate)
	s.Equal(predicate, rightScope.Predicate)
}

func (s *scopeSuite) TestSplitByPredicate_SamePredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passScope, failScope := scope.SplitByPredicate(splitPredicate)
	s.Equal(r, passScope.Range)
	s.Equal(r, failScope.Range)

	for _, namespaceID := range splitNamespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		if slices.Contains(namespaceIDs, namespaceID) {
			s.True(passScope.Contains(mockTask))
		} else {
			s.False(passScope.Contains(mockTask))
		}
		s.False(failScope.Contains(mockTask))
	}
	for _, namespaceID := range namespaceIDs {
		if slices.Contains(splitNamespaceIDs, namespaceID) {
			continue
		}

		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		s.False(passScope.Contains(mockTask))
		s.True(failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	s.False(passScope.Contains(mockTask))
	s.False(failScope.Contains(mockTask))
}

func (s *scopeSuite) TestSplitByPredicate_DifferentPredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitTaskTypes := []enumsspb.TaskType{
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
	}
	splitPredicate := tasks.NewTypePredicate(splitTaskTypes)
	passScope, failScope := scope.SplitByPredicate(splitPredicate)
	s.Equal(r, passScope.Range)
	s.Equal(r, failScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range splitTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).Times(2)

			s.True(passScope.Contains(mockTask))
			s.False(failScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(2)
		s.False(passScope.Contains(mockTask))
		s.True(failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range splitTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(2)

		s.False(passScope.Contains(mockTask))
		s.False(failScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(2)
	s.False(passScope.Contains(mockTask))
	s.False(failScope.Contains(mockTask))
}

func (s *scopeSuite) TestCanMergeByRange() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	testPredicates := []tasks.Predicate{
		predicate,
		tasks.NewNamespacePredicate(namespaceIDs),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}),
		tasks.NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER}),
	}
	s.True(predicate.Equals(testPredicates[0]))
	s.True(predicate.Equals(testPredicates[1]))
	s.False(predicate.Equals(testPredicates[2]))
	s.False(predicate.Equals(testPredicates[3]))

	for _, mergePredicate := range testPredicates {
		canMerge := predicate.Equals(mergePredicate)

		incomingScope := NewScope(r, mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))
	}

	incomingScope := NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate)
	s.False(scope.CanMergeByRange(incomingScope))

	incomingScope = NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate)
	s.False(scope.CanMergeByRange(incomingScope))
}

func (s *scopeSuite) TestMergeByRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	mergeRange := r
	mergedScope := scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(r, mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, r.InclusiveMin)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = NewRange(r.ExclusiveMax, tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = NewRange(NewRandomKeyInRange(r), tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(mergeRange, mergedScope.Range)
}

func (s *scopeSuite) TestCanMergeByPredicate() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	s.True(scope.CanMergeByPredicate(scope))
	s.True(scope.CanMergeByPredicate(NewScope(r, predicate)))
	s.True(scope.CanMergeByPredicate(NewScope(r, tasks.NewTypePredicate([]enumsspb.TaskType{}))))

	s.False(scope.CanMergeByPredicate(NewScope(NewRandomRange(), predicate)))
	s.False(scope.CanMergeByPredicate(NewScope(NewRandomRange(), predicates.Universal[tasks.Task]())))
}

func (s *scopeSuite) TestMergeByPredicate_SamePredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	mergePredicate := tasks.NewNamespacePredicate(mergeNamespaceIDs)
	mergedScope := scope.MergeByPredicate(NewScope(r, mergePredicate))
	s.Equal(r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		s.True(mergedScope.Contains(mockTask))
	}
	for _, namespaceID := range mergeNamespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		s.True(mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).Times(2)
	s.False(mergedScope.Contains(mockTask))
}

func (s *scopeSuite) TestMergeByPredicate_DifferentPredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeTaskTypes := []enumsspb.TaskType{
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
	}
	mergePredicate := tasks.NewTypePredicate(mergeTaskTypes)
	mergedScope := scope.MergeByPredicate(NewScope(r, mergePredicate))
	s.Equal(r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range mergeTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)
			s.True(mergedScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(1)
		s.True(mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range mergeTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)

		s.True(mergedScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(1)
	s.False(mergedScope.Contains(mockTask))
}
