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
	r := tasks.NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()

		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).Times(1)
		s.True(scope.Contains(mockTask))

		mockTask.EXPECT().GetKey().Return(tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1)).Times(1)
		s.False(scope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).MaxTimes(1)
	s.False(scope.Contains(mockTask))
}

func (s *scopeSuite) TestCanSplitRange() {
	r := tasks.NewRandomRange()
	predicate := predicates.All[tasks.Task]()
	scope := NewScope(r, predicate)

	s.True(scope.CanSplitRange(r.InclusiveMin))
	s.True(scope.CanSplitRange(r.ExclusiveMax))
	s.True(scope.CanSplitRange(tasks.NewRandomKeyInRange(r)))

	s.False(scope.CanSplitRange(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	s.False(scope.CanSplitRange(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func (s *scopeSuite) TestSplitRange() {
	r := tasks.NewRandomRange()
	predicate := predicates.All[tasks.Task]()
	scope := NewScope(r, predicate)

	splitKey := tasks.NewRandomKeyInRange(r)

	leftScope, rightScope := scope.SplitRange(splitKey)
	s.Equal(tasks.NewRange(r.InclusiveMin, splitKey), leftScope.Range)
	s.Equal(tasks.NewRange(splitKey, r.ExclusiveMax), rightScope.Range)
	s.Equal(predicate, leftScope.Predicate)
	s.Equal(predicate, rightScope.Predicate)
}

func (s *scopeSuite) TestSplitPredicate_SamePredicateType() {
	r := tasks.NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passScope, failScope := scope.SplitPredicate(splitPredicate)
	s.Equal(r, passScope.Range)
	s.Equal(r, failScope.Range)

	for _, namespaceID := range splitNamespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()

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
		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()

		s.False(passScope.Contains(mockTask))
		s.True(failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()
	s.False(passScope.Contains(mockTask))
	s.False(failScope.Contains(mockTask))
}

func (s *scopeSuite) TestSplitPredicate_DifferentPredicateType() {
	r := tasks.NewRandomRange()
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
	passScope, failScope := scope.SplitPredicate(splitPredicate)
	s.Equal(r, passScope.Range)
	s.Equal(r, failScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()

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
	mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range splitTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(2)

		s.False(passScope.Contains(mockTask))
		s.False(failScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(2)
	s.False(passScope.Contains(mockTask))
	s.False(failScope.Contains(mockTask))
}

func (s *scopeSuite) TestCanMergeRange() {
	r := tasks.NewRandomRange()
	predicate := predicates.All[tasks.Task]()
	scope := NewScope(r, predicate)

	s.True(scope.CanMergeRange(r))
	s.True(scope.CanMergeRange(tasks.NewRange(tasks.MinimumKey, r.InclusiveMin)))
	s.True(scope.CanMergeRange(tasks.NewRange(r.ExclusiveMax, tasks.MaximumKey)))
	s.True(scope.CanMergeRange(tasks.NewRange(tasks.MinimumKey, tasks.NewRandomKeyInRange(r))))
	s.True(scope.CanMergeRange(tasks.NewRange(tasks.NewRandomKeyInRange(r), tasks.MaximumKey)))
	s.True(scope.CanMergeRange(tasks.NewRange(tasks.MinimumKey, tasks.MaximumKey)))

	s.False(scope.CanMergeRange(tasks.NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	)))
	s.False(scope.CanMergeRange(tasks.NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	)))
}

func (s *scopeSuite) TestMergeRange() {
	r := tasks.NewRandomRange()
	predicate := predicates.All[tasks.Task]()
	scope := NewScope(r, predicate)

	mergeRange := r
	mergedScope := scope.MergeRange(mergeRange)
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(r, mergedScope.Range)

	mergeRange = tasks.NewRange(tasks.MinimumKey, r.InclusiveMin)
	mergedScope = scope.MergeRange(mergeRange)
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(tasks.NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = tasks.NewRange(r.ExclusiveMax, tasks.MaximumKey)
	mergedScope = scope.MergeRange(mergeRange)
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(tasks.NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = tasks.NewRange(tasks.MinimumKey, tasks.NewRandomKeyInRange(r))
	mergedScope = scope.MergeRange(mergeRange)
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(tasks.NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = tasks.NewRange(tasks.NewRandomKeyInRange(r), tasks.MaximumKey)
	mergedScope = scope.MergeRange(mergeRange)
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(tasks.NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = tasks.NewRange(tasks.MinimumKey, tasks.MaximumKey)
	mergedScope = scope.MergeRange(mergeRange)
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(mergeRange, mergedScope.Range)
}

func (s *scopeSuite) TestMergePredicate_SamePredicateType() {
	r := tasks.NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	mergePredicate := tasks.NewNamespacePredicate(mergeNamespaceIDs)
	mergedScope := scope.MergePredicate(mergePredicate)
	s.Equal(r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		s.True(mergedScope.Contains(mockTask))
	}
	for _, namespaceID := range mergeNamespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		s.True(mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).Times(2)
	s.False(mergedScope.Contains(mockTask))
}

func (s *scopeSuite) TestMergePredicate_DifferentPredicateType() {
	r := tasks.NewRandomRange()
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
	mergedScope := scope.MergePredicate(mergePredicate)
	s.Equal(r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range mergeTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)
			s.True(mergedScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(1)
		s.True(mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(tasks.NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range mergeTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)

		s.True(mergedScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(1)
	s.False(mergedScope.Contains(mockTask))
}
