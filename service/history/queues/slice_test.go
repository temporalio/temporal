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

		executableInitializer executableInitializer
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
	scope := NewScope(r, predicates.All[tasks.Task]())

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
	predicate := predicates.All[tasks.Task]()
	scope := NewScope(r, predicates.All[tasks.Task]())

	slice := NewSlice(nil, s.executableInitializer, scope)
	for _, executable := range s.randomExecutablesInRange(r, 10) {
		slice.outstandingExecutables[executable.GetKey()] = executable
	}
	slice.iterators = s.randomIteratorsInRange(r, 5, nil)

	splitKey := NewRandomKeyInRange(r)
	leftSlice, rightSlice := slice.SplitByRange(splitKey)
	s.Equal(NewScope(
		NewRange(r.InclusiveMin, splitKey),
		predicate,
	), leftSlice.Scope())
	s.Equal(NewScope(
		NewRange(splitKey, r.ExclusiveMax),
		predicate,
	), rightSlice.Scope())

	s.validateSliceState(leftSlice.(*SliceImpl))
	s.validateSliceState(rightSlice.(*SliceImpl))

	s.Panics(func() { slice.validateNotDestroyed() })
}

func (s *sliceSuite) TestSplitByPredicate() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	slice := NewSlice(nil, s.executableInitializer, scope)
	for _, executable := range s.randomExecutablesInRange(r, 10) {
		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(namespaceIDs[rand.Intn(len(namespaceIDs))]).AnyTimes()
		slice.outstandingExecutables[executable.GetKey()] = executable
	}
	slice.iterators = s.randomIteratorsInRange(r, 5, nil)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.New(), uuid.New())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passSlice, failSlice := slice.SplitByPredicate(splitPredicate)
	s.Equal(r, passSlice.Scope().Range)
	s.Equal(r, failSlice.Scope().Range)
	s.True(predicates.And[tasks.Task](predicate, splitPredicate).Equals(passSlice.Scope().Predicate))
	s.True(predicates.And[tasks.Task](predicate, predicates.Not[tasks.Task](splitPredicate)).Equals(failSlice.Scope().Predicate))

	s.validateSliceState(passSlice.(*SliceImpl))
	s.validateSliceState(failSlice.(*SliceImpl))

	s.Panics(func() { slice.validateNotDestroyed() })
}

func (s *sliceSuite) TestCanMergeByRange() {
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
		canMerge := predicate.Equals(mergePredicate)

		testSlice := NewSlice(nil, nil, NewScope(r, mergePredicate))
		s.Equal(canMerge, slice.CanMergeByRange(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate))
		s.Equal(canMerge, slice.CanMergeByRange(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate))
		s.Equal(canMerge, slice.CanMergeByRange(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate))
		s.Equal(canMerge, slice.CanMergeByRange(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate))
		s.Equal(canMerge, slice.CanMergeByRange(testSlice))

		testSlice = NewSlice(nil, nil, NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate))
		s.Equal(canMerge, slice.CanMergeByRange(testSlice))
	}

	s.False(slice.CanMergeByRange(slice))

	testSlice := NewSlice(nil, nil, NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate))
	s.False(slice.CanMergeByRange(testSlice))

	testSlice = NewSlice(nil, nil, NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate))
	s.False(slice.CanMergeByRange(testSlice))
}

func (s *sliceSuite) TestMergeByRange() {
	r := NewRandomRange()
	predicate := predicates.All[tasks.Task]()

	slice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))
	for _, executable := range s.randomExecutablesInRange(r, rand.Intn(20)) {
		slice.outstandingExecutables[executable.GetKey()] = executable
	}
	totalExecutables := len(slice.outstandingExecutables)
	slice.iterators = s.randomIteratorsInRange(r, rand.Intn(10), nil)

	incomingRange := NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	incomingSlice := NewSlice(nil, s.executableInitializer, NewScope(incomingRange, predicate))
	for _, executable := range s.randomExecutablesInRange(incomingRange, rand.Intn(20)) {
		incomingSlice.outstandingExecutables[executable.GetKey()] = executable
	}
	totalExecutables += len(incomingSlice.outstandingExecutables)
	incomingSlice.iterators = s.randomIteratorsInRange(r, rand.Intn(10), nil)

	mergedSlice := slice.MergeByRange(incomingSlice)
	mergedSliceImpl := mergedSlice.(*SliceImpl)

	s.Equal(NewScope(
		NewRange(tasks.MinimumKey, r.ExclusiveMax),
		predicate,
	), mergedSlice.Scope())

	s.validateSliceState(mergedSliceImpl)
	s.Len(mergedSliceImpl.outstandingExecutables, totalExecutables)

	s.Panics(func() { slice.validateNotDestroyed() })
	s.Panics(func() { incomingSlice.validateNotDestroyed() })
}

func (s *sliceSuite) TestCanMergeByPredicate() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	slice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))

	testSlice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))
	s.True(slice.CanMergeByPredicate(testSlice))

	testSlice = NewSlice(nil, s.executableInitializer, NewScope(r, tasks.NewTypePredicate([]enumsspb.TaskType{})))
	s.True(slice.CanMergeByPredicate(testSlice))

	s.False(slice.CanMergeByPredicate(slice))

	testSlice = NewSlice(nil, s.executableInitializer, NewScope(NewRandomRange(), predicate))
	s.False(slice.CanMergeByPredicate(testSlice))

	testSlice = NewSlice(nil, s.executableInitializer, NewScope(NewRandomRange(), predicates.All[tasks.Task]()))
	s.False(slice.CanMergeByPredicate(testSlice))
}

func (s *sliceSuite) TestMergeByPredicate() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)

	slice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))
	for _, executable := range s.randomExecutablesInRange(r, rand.Intn(20)) {
		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(namespaceIDs[rand.Intn(len(namespaceIDs))]).AnyTimes()
		mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION).AnyTimes()
		slice.outstandingExecutables[executable.GetKey()] = executable
	}
	totalExecutables := len(slice.outstandingExecutables)
	slice.iterators = s.randomIteratorsInRange(r, rand.Intn(10), nil)

	taskTypes := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
	}
	incomingPredicate := tasks.NewTypePredicate(taskTypes)
	incomingSlice := NewSlice(nil, s.executableInitializer, NewScope(r, incomingPredicate))
	for _, executable := range s.randomExecutablesInRange(r, rand.Intn(20)) {
		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
		mockExecutable.EXPECT().GetType().Return(taskTypes[rand.Intn(len(taskTypes))]).AnyTimes()
		incomingSlice.outstandingExecutables[executable.GetKey()] = executable
	}
	totalExecutables += len(incomingSlice.outstandingExecutables)
	incomingSlice.iterators = s.randomIteratorsInRange(r, rand.Intn(10), nil)

	mergedSlice := slice.MergeByPredicate(incomingSlice)
	mergedSliceImpl := mergedSlice.(*SliceImpl)

	s.Equal(r, mergedSlice.Scope().Range)
	s.True(predicates.Or[tasks.Task](predicate, incomingPredicate).Equals(mergedSlice.Scope().Predicate))

	s.validateSliceState(mergedSliceImpl)
	s.Len(mergedSliceImpl.outstandingExecutables, totalExecutables)

	s.Panics(func() { slice.validateNotDestroyed() })
	s.Panics(func() { incomingSlice.validateNotDestroyed() })
}

func (s *sliceSuite) TestShrinkRange() {
	r := NewRandomRange()
	predicate := predicates.All[tasks.Task]()

	slice := NewSlice(nil, s.executableInitializer, NewScope(r, predicate))
	slice.iterators = s.randomIteratorsInRange(r, rand.Intn(2), nil)

	executables := s.randomExecutablesInRange(r, 5)
	slices.SortFunc(executables, func(a, b Executable) bool {
		return a.GetKey().CompareTo(b.GetKey()) < 0
	})

	firstPendingIdx := len(executables)
	for idx, executable := range executables {
		mockExecutable := executable.(*MockExecutable)
		acked := rand.Intn(10) < 8
		if acked {
			mockExecutable.EXPECT().State().Return(ctasks.TaskStateAcked).MaxTimes(1)
		} else {
			mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
			if firstPendingIdx == len(executables) {
				firstPendingIdx = idx
			}
		}

		slice.outstandingExecutables[executable.GetKey()] = executable
	}

	slice.ShrinkRange()
	s.Len(slice.outstandingExecutables, len(executables)-firstPendingIdx)

	newInclusiveMin := r.ExclusiveMax

	if firstPendingIdx == len(executables) {
		if len(slice.iterators) != 0 {
			newInclusiveMin = slice.iterators[0].Range().InclusiveMin
		}
	} else {
		newInclusiveMin = executables[firstPendingIdx].GetKey()
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
	predicate := predicates.All[tasks.Task]()

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
	paginationFnProvider paginationFnProvider,
) []Iterator {
	ranges := []Range{r}
	for len(ranges) < numIterators {
		r := ranges[0]
		left, right := r.Split(NewRandomKeyInRange(r))
		left.ExclusiveMax.FireTime.Add(-time.Nanosecond)
		right.InclusiveMin.FireTime.Add(time.Nanosecond)
		ranges = append(ranges[1:], left, right)
	}

	slices.SortFunc(ranges, func(a, b Range) bool {
		return a.InclusiveMin.CompareTo(b.InclusiveMin) < 0
	})

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
