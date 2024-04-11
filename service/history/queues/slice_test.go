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
	"golang.org/x/exp/slices"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/predicates"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	sliceSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		executableFactory ExecutableFactory
		monitor           *monitorImpl
	}
)

func TestSliceSuite(t *testing.T) {
	s := new(sliceSuite)
	suite.Run(t, s)
}

func (s *sliceSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.executableFactory = ExecutableFactoryFn(func(readerID int64, t tasks.Task) Executable {
		return NewExecutable(
			readerID,
			t,
			nil,
			nil,
			nil,
			NewNoopPriorityAssigner(),
			clock.NewRealTimeSource(),
			nil,
			nil,
			nil,
			metrics.NoopMetricsHandler,
		)
	})
	s.monitor = newMonitor(tasks.CategoryTypeScheduled, clock.NewRealTimeSource(), &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})
}

func (s *sliceSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *sliceSuite) TestCanSplitByRange() {
	r := NewRandomRange()
	scope := NewScope(r, predicates.Universal[tasks.Task]())

	slice := NewSlice(nil, s.executableFactory, s.monitor, scope, GrouperNamespaceID{})
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
	s.True(tasks.AndPredicates(slice.scope.Predicate, splitPredicate).Equals(passSlice.Scope().Predicate))
	s.True(predicates.And(slice.scope.Predicate, predicates.Not[tasks.Task](splitPredicate)).Equals(failSlice.Scope().Predicate))

	s.validateSliceState(passSlice.(*SliceImpl))
	s.validateSliceState(failSlice.(*SliceImpl))

	s.Panics(func() { slice.stateSanityCheck() })
}

func (s *sliceSuite) TestCanMergeWithSlice() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	slice := NewSlice(nil, nil, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})

	testPredicates := []tasks.Predicate{
		predicate,
		tasks.NewNamespacePredicate(namespaceIDs),
		tasks.NewNamespacePredicate([]string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}),
	}
	s.True(predicate.Equals(testPredicates[0]))
	s.True(predicate.Equals(testPredicates[1]))
	s.False(predicate.Equals(testPredicates[2]))

	for _, mergePredicate := range testPredicates {
		testSlice := NewSlice(nil, nil, s.monitor, NewScope(r, mergePredicate), GrouperNamespaceID{})
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, s.monitor, NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate), GrouperNamespaceID{})
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, s.monitor, NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate), GrouperNamespaceID{})
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, s.monitor, NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate), GrouperNamespaceID{})
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, s.monitor, NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate), GrouperNamespaceID{})
		s.True(slice.CanMergeWithSlice(testSlice))

		testSlice = NewSlice(nil, nil, s.monitor, NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate), GrouperNamespaceID{})
		s.True(slice.CanMergeWithSlice(testSlice))
	}

	s.False(slice.CanMergeWithSlice(slice))

	testSlice := NewSlice(nil, nil, s.monitor, NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate), GrouperNamespaceID{})
	s.False(slice.CanMergeWithSlice(testSlice))

	testSlice = NewSlice(nil, nil, s.monitor, NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate), GrouperNamespaceID{})
	s.False(slice.CanMergeWithSlice(testSlice))
}

func (s *sliceSuite) TestMergeWithSlice_SamePredicate() {
	r := NewRandomRange()
	slice := s.newTestSlice(r, nil, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingRange := NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	incomingSlice := s.newTestSlice(incomingRange, nil, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 1)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_SameRange() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	taskTypes := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
	}
	incomingSlice := s.newTestSlice(r, nil, taskTypes)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 1)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_SameMinKey() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingRange := NewRange(
		r.InclusiveMin,
		NewRandomKeyInRange(NewRange(r.InclusiveMin, tasks.MaximumKey)),
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := s.newTestSlice(incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 2)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_SameMaxKey() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingRange := NewRange(
		NewRandomKeyInRange(NewRange(tasks.MinimumKey, r.ExclusiveMax)),
		r.ExclusiveMax,
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := s.newTestSlice(incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 2)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestMergeWithSlice_DifferentMinMaxKey() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice := s.newTestSlice(r, namespaceIDs, nil)
	totalExecutables := len(slice.pendingExecutables)

	incomingMinKey := NewRandomKeyInRange(NewRange(r.InclusiveMin, r.ExclusiveMax))
	incomingRange := NewRange(
		incomingMinKey,
		NewRandomKeyInRange(NewRange(incomingMinKey, tasks.MaximumKey)),
	)
	incomingNamespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	incomingSlice := s.newTestSlice(incomingRange, incomingNamespaceIDs, nil)
	totalExecutables += len(incomingSlice.pendingExecutables)

	s.validateSliceState(slice)
	s.validateSliceState(incomingSlice)

	mergedSlices := slice.MergeWithSlice(incomingSlice)
	s.Len(mergedSlices, 3)

	s.validateMergedSlice(slice, incomingSlice, mergedSlices, totalExecutables)
}

func (s *sliceSuite) TestCompactWithSlice() {
	r1 := NewRandomRange()
	namespaceIDs := []string{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	slice1 := s.newTestSlice(r1, namespaceIDs, nil)
	totalExecutables := len(slice1.pendingExecutables)

	r2 := NewRandomRange()
	taskTypes := []enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT}
	slice2 := s.newTestSlice(r2, nil, taskTypes)
	totalExecutables += len(slice2.pendingExecutables)

	s.validateSliceState(slice1)
	s.validateSliceState(slice2)

	compactedSlice := slice1.CompactWithSlice(slice2).(*SliceImpl)
	compactedRange := compactedSlice.Scope().Range
	s.True(compactedRange.ContainsRange(r1))
	s.True(compactedRange.ContainsRange(r2))
	s.Equal(
		tasks.MinKey(r1.InclusiveMin, r2.InclusiveMin),
		compactedRange.InclusiveMin,
	)
	s.Equal(
		tasks.MaxKey(r1.ExclusiveMax, r2.ExclusiveMax),
		compactedRange.ExclusiveMax,
	)
	s.True(
		tasks.OrPredicates(slice1.scope.Predicate, slice2.scope.Predicate).
			Equals(compactedSlice.scope.Predicate),
	)

	s.validateSliceState(compactedSlice)
	s.Len(compactedSlice.pendingExecutables, totalExecutables)

	s.Panics(func() { slice1.stateSanityCheck() })
	s.Panics(func() { slice2.stateSanityCheck() })
}

func (s *sliceSuite) TestShrinkScope_ShrinkRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	slice := NewSlice(nil, s.executableFactory, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})
	slice.iterators = s.randomIteratorsInRange(r, rand.Intn(2), nil)

	executables := s.randomExecutablesInRange(r, 5)
	slices.SortFunc(executables, func(a, b Executable) int {
		return a.GetKey().CompareTo(b.GetKey())
	})

	firstPendingIdx := len(executables)
	numAcked := 0
	for idx, executable := range executables {
		mockExecutable := executable.(*MockExecutable)
		mockExecutable.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
		mockExecutable.EXPECT().GetTask().Return(mockExecutable).AnyTimes()

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

		slice.pendingExecutables[executable.GetKey()] = executable
	}

	s.Equal(numAcked, slice.ShrinkScope())
	s.Len(slice.pendingExecutables, len(executables)-numAcked)
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

func (s *sliceSuite) TestShrinkScope_ShrinkPredicate() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	slice := NewSlice(nil, s.executableFactory, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})
	slice.iterators = []Iterator{} // manually set iterators to be empty to trigger predicate update

	executables := s.randomExecutablesInRange(r, 100)
	slices.SortFunc(executables, func(a, b Executable) int {
		return a.GetKey().CompareTo(b.GetKey())
	})

	pendingNamespaceID := []string{uuid.New(), uuid.New()}
	s.True(len(pendingNamespaceID) <= shrinkPredicateMaxPendingKeys)
	for _, executable := range executables {
		mockExecutable := executable.(*MockExecutable)

		mockExecutable.EXPECT().GetTask().Return(mockExecutable).AnyTimes()

		acked := rand.Intn(10) < 8
		if acked {
			mockExecutable.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			mockExecutable.EXPECT().State().Return(ctasks.TaskStateAcked).MaxTimes(1)
		} else {
			mockExecutable.EXPECT().GetNamespaceID().Return(pendingNamespaceID[rand.Intn(len(pendingNamespaceID))]).AnyTimes()
			mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
		}

		slice.executableTracker.add(executable)
	}

	slice.ShrinkScope()
	s.validateSliceState(slice)

	namespacePredicate, ok := slice.Scope().Predicate.(*tasks.NamespacePredicate)
	s.True(ok)
	for namespaceID := range namespacePredicate.NamespaceIDs {
		s.True(slices.Index(pendingNamespaceID, namespaceID) != -1)
	}
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
				key := NewRandomKeyInRange(paginationRange)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()

				namespaceID := namespaceIDs[rand.Intn(len(namespaceIDs))]
				if i >= numTasks/2 {
					namespaceID = uuid.New() // should be filtered out
				}
				mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) int {
				return a.GetKey().CompareTo(b.GetKey())
			})

			return mockTasks, nil, nil
		}
	}

	for _, batchSize := range []int{1, 2, 5, 10, 20, 100} {
		slice := NewSlice(paginationFnProvider, s.executableFactory, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})

		executables := make([]Executable, 0, numTasks)
		for {
			selectedExecutables, err := slice.SelectTasks(DefaultReaderId, batchSize)
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

func (s *sliceSuite) TestSelectTasks_Error_NoLoadedTasks() {
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
				key := NewRandomKeyInRange(paginationRange)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) int {
				return a.GetKey().CompareTo(b.GetKey())
			})

			return mockTasks, nil, nil
		}
	}

	slice := NewSlice(paginationFnProvider, s.executableFactory, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})
	_, err := slice.SelectTasks(DefaultReaderId, 100)
	s.Error(err)

	executables, err := slice.SelectTasks(DefaultReaderId, 100)
	s.NoError(err)
	s.Len(executables, numTasks)
	s.Empty(slice.iterators)
}

func (s *sliceSuite) TestSelectTasks_Error_WithLoadedTasks() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()

	numTasks := 20
	loadErr := false
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			defer func() {
				loadErr = !loadErr
			}()

			if loadErr {
				return nil, nil, errors.New("some random load task error")
			}

			mockTasks := make([]tasks.Task, 0, numTasks)
			for i := 0; i != numTasks; i++ {
				mockTask := tasks.NewMockTask(s.controller)
				key := NewRandomKeyInRange(paginationRange)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
				mockTasks = append(mockTasks, mockTask)
			}

			slices.SortFunc(mockTasks, func(a, b tasks.Task) int {
				return a.GetKey().CompareTo(b.GetKey())
			})

			return mockTasks, []byte{1, 2, 3}, nil
		}
	}

	slice := NewSlice(paginationFnProvider, s.executableFactory, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})
	executables, err := slice.SelectTasks(DefaultReaderId, 100)
	s.NoError(err)
	s.Len(executables, numTasks)
	s.True(slice.MoreTasks())
}

func (s *sliceSuite) TestMoreTasks() {
	slice := s.newTestSlice(NewRandomRange(), nil, nil)

	s.True(slice.MoreTasks())

	slice.pendingExecutables = nil
	s.True(slice.MoreTasks())

	slice.iterators = nil
	s.False(slice.MoreTasks())
}

func (s *sliceSuite) TestClear() {
	slice := s.newTestSlice(NewRandomRange(), nil, nil)
	for _, executable := range slice.pendingExecutables {
		executable.(*MockExecutable).EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		executable.(*MockExecutable).EXPECT().Cancel().Times(1)
	}

	slice.Clear()
	s.Empty(slice.pendingExecutables)
	s.Len(slice.iterators, 1)
	s.Equal(slice.scope.Range, slice.iterators[0].Range())
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

	slice := NewSlice(nil, s.executableFactory, s.monitor, NewScope(r, predicate), GrouperNamespaceID{})
	for _, executable := range s.randomExecutablesInRange(r, rand.Intn(20)) {
		slice.pendingExecutables[executable.GetKey()] = executable

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
			s.True(tasks.OrPredicates(
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
		actualTotalExecutables += len(mergedSliceImpl.pendingExecutables)
	}
	s.Equal(expectedTotalExecutables, actualTotalExecutables)

	s.Panics(func() { currentSlice.stateSanityCheck() })
	s.Panics(func() { incomingSlice.stateSanityCheck() })
}

func (s *sliceSuite) validateSliceState(
	slice *SliceImpl,
) {
	s.NotNil(slice.executableFactory)

	for _, executable := range slice.pendingExecutables {
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
