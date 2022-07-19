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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/predicates"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/slices"
)

type (
	sliceSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		executableInitializer ExecutableInitializer
	}
)

func TestSliceSuite(t *testing.T) {
	s := new(sliceSuite)
	suite.Run(t, s)
}

func (s *sliceSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.executableInitializer = func(t tasks.Task) Executable {
		return NewMockExecutable(s.controller)
	}
}

func (s *sliceSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *sliceSuite) TestCanSplitByRange() {
	r := NewRandomRange()
	scope := NewScope(r, predicates.Universal[tasks.Task]())

	slice := NewSlice(nil, s.executableInitializer, scope)
	s.Equal(scope, slice.Scope())

	s.True(slice.CanSplitByRange(r.InclusiveMin))
	s.True(slice.CanSplitByRange(r.ExclusiveMax))
	s.True(slice.CanSplitByRange(NewRandomKeyInRange(r)))

	s.False(slice.CanSplitByRange(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	s.False(slice.CanSplitByRange(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func (s *sliceSuite) TestSplitByRange() {
	r := NewRandomRange()
	slice := s.newTestSlice(r, nil, nil)

	splitKey := NewRandomKeyInRange(r)
	leftSlice, rightSlice := slice.SplitByRange(splitKey)
	s.Equal(NewScope(
		NewRange(r.InclusiveMin, splitKey),
		slice.scope.Predicate,
	), leftSlice.Scope())
	s.Equal(NewScope(
		NewRange(splitKey, r.ExclusiveMax),
		slice.scope.Predicate,
	), rightSlice.Scope())

	s.validateSliceState(leftSlice.(*SliceImpl))
	s.validateSliceState(rightSlice.(*SliceImpl))

	s.Panics(func() { slice.stateSanityCheck() })
}

func (s *sliceSuite) TestSplitByPredicate() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passSlice, failSlice := slice.SplitByPredicate(splitPredicate)
	s.Equal(r, passSlice.Scope().Range)
	s.Equal(r, failSlice.Scope().Range)
	s.True(predicates.And[tasks.Task](slice.scope.Predicate, splitPredicate).Equals(passSlice.Scope().Predicate))
	s.True(predicates.And(slice.scope.Predicate, predicates.Not[tasks.Task](splitPredicate)).Equals(failSlice.Scope().Predicate))

	s.validateSliceState(passSlice.(*SliceImpl))
	s.validateSliceState(failSlice.(*SliceImpl))

	s.Panics(func() { slice.stateSanityCheck() })
}

func (s *sliceSuite) TestCanMergeWithSlice() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	slice := NewSlice(nil, nil, NewScope(r, predicate))

	testPredicates := []tasks.Predicate{
		predicate,
		tasks.NewNamespacePredicate(namespaceIDs),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}),
	}
	s.True(predicate.Equals(testPredicates[0]))
	s.True(predicate.Equals(testPredicates[1]))
	s.False(predicate.Equals(testPredicates[2]))

	for _, mergePredicate := range testPredicates {
		testSlice := NewSlice(nil, nil, NewScope(r, mergePredicate))
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate))
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate))
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate))
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate))
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate))
		s.True(slice.CanMergeWithSlice(testSlice))
	}

	s.False(slice.CanMergeWithSlice(slice))

	testSlice := NewSlice(nil, nil, NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate))
	s.False(slice.CanMergeWithSlice(testSlice))

	testSlice = NewSlice(nil, nil, NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate))
	s.False(slice.CanMergeWithSlice(testSlice))
}

func (s *sliceSuite) TestMergeWithSlice_SamePredicate() {
	r := NewRandomRange()
	slice := s.newTestSlice(r, nil, nil)
	totalExecutables := len(slice.outstandingExecutables)

	incomingRange := NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	incomingSlice := s.newTestSlice(incomingRange, nil, nil)
	totalExecutables += len(incomingSlice.outstandingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 1)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_SameRange() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.outstandingExecutables)

	taskTypes := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
	}
	incomingSlice := s.newTestSlice(r, nil, taskTypes)
	totalExecutables += len(incomingSlice.outstandingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 1)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_SameMinKey() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.outstandingExecutables)

	incomingRange := NewRange(
		r.InclusiveMin,
		NewRandomKeyInRange(NewRange(r.InclusiveMin, tasks.MaximumKey)),
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := s.newTestSlice(incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.outstandingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 2)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_SameMaxKey() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.outstandingExecutables)

	incomingRange := NewRange(
		NewRandomKeyInRange(NewRange(tasks.MinimumKey, r.ExclusiveMax)),
		r.ExclusiveMax,
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := s.newTestSlice(incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.outstandingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 2)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_DifferentMinMaxKey() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.outstandingExecutables)

	incomingMinKey := NewRandomKeyInRange(NewRange(r.InclusiveMin, r.ExclusiveMax))
	incomingRange := NewRange(
		incomingMinKey,
		NewRandomKeyInRange(NewRange(incomingMinKey, tasks.MaximumKey)),
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := s.newTestSlice(incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.outstandingExecutables)

	s.validateSliceState(slice)
	s.validateSliceState(incomingSlice)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 3)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestShrinkRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	slice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))
	slice.iterators = s.randomIteratorsInRange(r, rand.Intn(2), nil)

	executables := s.randomExecutablesInRange(r, 5)
	slices.SortFunc(executables, func(a, b Executable) bool {
		return a.GetKey().CompareTo(b.GetKey()) < 0
	})

	firstPendingIdx := len(executables)
	numAcked := 0
	for idx, executable := range executables {
		mockExecutable := executable.(*MockExecutable)
		acked := rand.Intn(10) < 8
		if acked {
			mockExecutable.EXPECT().State().Return(ctasks.TaskStateAcked).MaxTimes(1)
			numAcked++
		} else {
			mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
			if firstPendingIdx == len(executables) {
				firstPendingIdx = idx
			}
		}

		slice.outstandingExecutables[executable.GetKey()] = executable
	}

	slice.ShrinkRange()
	s.Len(slice.outstandingExecutables, len(executables)-numAcked)
	s.validateSliceState(slice)

	newInclusiveMin := r.ExclusiveMax
	if len(slice.iterators) != 0 {
		newInclusiveMin = slice.iterators[0].Range().InclusiveMin
	}

	if numAcked != len(executables) {
		newInclusiveMin = tasks.MinKey(newInclusiveMin, executables[firstPendingIdx].GetKey())
	}

	s.Equal(NewRange(newInclusiveMin, r.ExclusiveMax), slice.Scope().Range)
}

func (s *sliceSuite) TestSelectTasks_NoError() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)

	numTasks := 20
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {

			mockTasks := make([]tasks.Task, 0, numTasks)
			for i := 0; i != numTasks; i++ {
				mockTask := tasks.NewMockTask(s.controller)
				key := NewRandomKeyInRange(r)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()

				namespaceID := namespaceIDs[rand.Intn(len(namespaceIDs))]
				if i >= numTasks/2 {
					namespaceID = uuid.New() // should be filtered out
				}
				mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) bool {
				return a.GetKey().CompareTo(b.GetKey()) < 0
			})

			return mockTasks, nil, nil
		}
	}

	for _, batchSize := range []int{1, 2, 5, 10, 20, 100} {
		slice := NewSlice(paginationFnProvider, s.executableInitializer, NewScope(r, predicate))

		executables := make([]Executable, 0, numTasks)
		for {
			selectedExecutables, err := slice.SelectTasks(batchSize)
			s.NoError(err)
			if len(selectedExecutables) == 0 {
				break
			}

			executables = append(executables, selectedExecutables...)
		}

		s.Len(executables, numTasks/2) // half of tasks should be filtered out based on its namespaceID
		s.Empty(slice.iterators)
	}
}

func (s *sliceSuite) TestSelectTasks_Error() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	numTasks := 20
	loadErr := true
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			if loadErr {
				loadErr = false
				return nil, nil, errors.New("some random load task error")
			}

			mockTasks := make([]tasks.Task, 0, numTasks)
			for i := 0; i != numTasks; i++ {
				mockTask := tasks.NewMockTask(s.controller)
				key := NewRandomKeyInRange(r)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) bool {
				return a.GetKey().CompareTo(b.GetKey()) < 0
			})

			return mockTasks, nil, nil
		}
	}

	slice := NewSlice(paginationFnProvider, s.executableInitializer, NewScope(r, predicate))
	_, err := slice.SelectTasks(100)
	s.Error(err)

	executables, err := slice.SelectTasks(100)
	s.NoError(err)
	s.Len(executables, numTasks)
	s.Empty(slice.iterators)
}

func (s *sliceSuite) newTestSlice(
	r Range,
	namespaceIDs []string,
	taskTypes []enumsspb.TaskType,
) *SliceImpl {
	predicate := predicates.Universal[tasks.Task]()
	if len(namespaceIDs) != 0 {
		predicate = predicates.And[tasks.Task](
			predicate,
			tasks.NewNamespacePredicate(namespaceIDs),
		)
	}
	if len(taskTypes) != 0 {
		predicate = predicates.And[tasks.Task](
			predicate,
			tasks.NewTypePredicate(taskTypes),
		)
	}

	if len(namespaceIDs) == 0 {
		namespaceIDs = []string{uuid.New()}
	}

	if len(taskTypes) == 0 {
		taskTypes = []enumsspb.TaskType{enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION}
	}

	slice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))
	for _, executable := range s.randomExecutablesInRange(r, rand.Intn(20)) {
		slice.outstandingExecutables[executable.GetKey()] = executable

		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(namespaceIDs[rand.Intn(len(namespaceIDs))]).AnyTimes()
		mockExecutable.EXPECT().GetType().Return(taskTypes[rand.Intn(len(taskTypes))]).AnyTimes()
	}
	slice.iterators = s.randomIteratorsInRange(r, rand.Intn(10), nil)

	return slice
}

func (s *sliceSuite) validateMergedSlice(
	currentSlice *SliceImpl,
	incomingSlice *SliceImpl,
	mergedSlices []Slice,
	expectedTotalExecutables int,
) {
	expectedMergedRange := currentSlice.scope.Range.Merge(incomingSlice.scope.Range)
	actualMergedRange := mergedSlices[0].Scope().Range
	for _, mergedSlice := range mergedSlices {
		actualMergedRange = actualMergedRange.Merge(mergedSlice.Scope().Range)

		containedByCurrent := currentSlice.scope.Range.ContainsRange(mergedSlice.Scope().Range)
		containedByIncoming := incomingSlice.scope.Range.ContainsRange(mergedSlice.Scope().Range)
		if containedByCurrent && containedByIncoming {
			s.True(predicates.Or(
				currentSlice.scope.Predicate,
				incomingSlice.scope.Predicate,
			).Equals(mergedSlice.Scope().Predicate))
		} else if containedByCurrent {
			s.True(currentSlice.scope.Predicate.Equals(mergedSlice.Scope().Predicate))
		} else if containedByIncoming {
			s.True(incomingSlice.scope.Predicate.Equals(mergedSlice.Scope().Predicate))
		} else if currentSlice.scope.Predicate.Equals(incomingSlice.scope.Predicate) {
			s.True(currentSlice.scope.Predicate.Equals(mergedSlice.Scope().Predicate))
		} else {
			s.Fail("Merged slice range not contained by the merging slices")
		}

	}
	s.True(expectedMergedRange.Equals(actualMergedRange))

	actualTotalExecutables := 0
	for _, mergedSlice := range mergedSlices {
		mergedSliceImpl := mergedSlice.(*SliceImpl)
		s.validateSliceState(mergedSliceImpl)
		actualTotalExecutables += len(mergedSliceImpl.outstandingExecutables)
	}
	s.Equal(expectedTotalExecutables, actualTotalExecutables)

	s.Panics(func() { currentSlice.stateSanityCheck() })
	s.Panics(func() { incomingSlice.stateSanityCheck() })
}

func (s *sliceSuite) validateSliceState(
	slice *SliceImpl,
) {
	s.NotNil(slice.executableInitializer)

	for _, executable := range slice.outstandingExecutables {
		s.True(slice.scope.Contains(executable))
	}

	r := slice.Scope().Range
	for idx, iterator := range slice.iterators {
		s.True(r.ContainsRange(iterator.Range()))
		if idx != 0 {
			currentRange := iterator.Range()
			previousRange := slice.iterators[idx-1].Range()
			s.False(currentRange.CanMerge(previousRange))
			s.True(previousRange.ExclusiveMax.CompareTo(currentRange.InclusiveMin) < 0)
		}
	}

	s.False(slice.destroyed)
}

func (s *sliceSuite) randomExecutablesInRange(
	r Range,
	numExecutables int,
) []Executable {
	executables := make([]Executable, 0, numExecutables)
	for i := 0; i != numExecutables; i++ {
		mockExecutable := NewMockExecutable(s.controller)
		key := NewRandomKeyInRange(r)
		mockExecutable.EXPECT().GetKey().Return(key).AnyTimes()
		executables = append(executables, mockExecutable)
	}
	return executables
}

func (s *sliceSuite) randomIteratorsInRange(
	r Range,
	numIterators int,
	paginationFnProvider PaginationFnProvider,
) []Iterator {
	ranges := NewRandomOrderedRangesInRange(r, numIterators)

	iterators := make([]Iterator, 0, numIterators)
	for _, r := range ranges {
		start := NewRandomKeyInRange(r)
		end := NewRandomKeyInRange(r)
		if start.CompareTo(end) > 0 {
			start, end = end, start
		}
		iterator := NewIterator(paginationFnProvider, NewRange(start, end))
		iterators = append(iterators, iterator)
	}

	return iterators
}
