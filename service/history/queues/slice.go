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
	"fmt"

	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/maps"
)

const (
	shrinkPredicateMaxPendingKeys = 3
)

type (

	// Slice manages the loading and status tracking of all
	// tasks within its Scope.
	// It also provides methods for splitting or merging with
	// another slice either by range or by predicate.
	Slice interface {
		Scope() Scope
		CanSplitByRange(tasks.Key) bool
		SplitByRange(tasks.Key) (left Slice, right Slice)
		SplitByPredicate(tasks.Predicate) (pass Slice, fail Slice)
		CanMergeWithSlice(Slice) bool
		MergeWithSlice(Slice) []Slice
		CompactWithSlice(Slice) Slice
		ShrinkScope() int
		SelectTasks(readerID int64, batchSize int) ([]Executable, error)
		MoreTasks() bool
		TaskStats() TaskStats
		Clear()
	}

	TaskStats struct {
		PendingPerKey map[any]int
	}

	SliceImpl struct {
		paginationFnProvider PaginationFnProvider
		executableFactory    ExecutableFactory

		destroyed bool

		scope     Scope
		iterators []Iterator

		*executableTracker
		monitor Monitor
	}
)

func NewSlice(
	paginationFnProvider PaginationFnProvider,
	executableFactory ExecutableFactory,
	monitor Monitor,
	scope Scope,
	grouper Grouper,
) *SliceImpl {
	return &SliceImpl{
		paginationFnProvider: paginationFnProvider,
		executableFactory:    executableFactory,
		scope:                scope,
		iterators: []Iterator{
			NewIterator(paginationFnProvider, scope.Range),
		},
		executableTracker: newExecutableTracker(grouper),
		monitor:           monitor,
	}
}

func (s *SliceImpl) Scope() Scope {
	s.stateSanityCheck()
	return s.scope
}

func (s *SliceImpl) CanSplitByRange(key tasks.Key) bool {
	s.stateSanityCheck()
	return s.scope.CanSplitByRange(key)
}

func (s *SliceImpl) SplitByRange(key tasks.Key) (left Slice, right Slice) {
	if !s.CanSplitByRange(key) {
		panic(fmt.Sprintf("Unable to split queue slice with range %v at %v", s.scope.Range, key))
	}

	return s.splitByRange(key)
}

func (s *SliceImpl) splitByRange(key tasks.Key) (left *SliceImpl, right *SliceImpl) {

	leftScope, rightScope := s.scope.SplitByRange(key)
	leftTaskTracker, rightTaskTracker := s.executableTracker.split(leftScope, rightScope)

	leftIterators := make([]Iterator, 0, len(s.iterators)/2)
	rightIterators := make([]Iterator, 0, len(s.iterators)/2)
	for _, iter := range s.iterators {
		iterRange := iter.Range()
		if leftScope.Range.ContainsRange(iterRange) {
			leftIterators = append(leftIterators, iter)
			continue
		}

		if rightScope.Range.ContainsRange(iterRange) {
			rightIterators = append(rightIterators, iter)
			continue
		}

		leftIter, rightIter := iter.Split(key)
		leftIterators = append(leftIterators, leftIter)
		rightIterators = append(rightIterators, rightIter)
	}

	s.destroy()

	return s.newSlice(leftScope, leftIterators, leftTaskTracker),
		s.newSlice(rightScope, rightIterators, rightTaskTracker)
}

func (s *SliceImpl) SplitByPredicate(predicate tasks.Predicate) (pass Slice, fail Slice) {
	s.stateSanityCheck()

	passScope, failScope := s.scope.SplitByPredicate(predicate)
	passTaskTracker, failTaskTracker := s.executableTracker.split(passScope, failScope)

	passIterators := make([]Iterator, 0, len(s.iterators))
	failIterators := make([]Iterator, 0, len(s.iterators))
	for _, iter := range s.iterators {
		// iter.Remaining() is basically a deep copy of iter
		passIterators = append(passIterators, iter)
		failIterators = append(failIterators, iter.Remaining())
	}

	s.destroy()

	return s.newSlice(passScope, passIterators, passTaskTracker),
		s.newSlice(failScope, failIterators, failTaskTracker)
}

func (s *SliceImpl) CanMergeWithSlice(slice Slice) bool {
	s.stateSanityCheck()

	return s != slice && s.scope.Range.CanMerge(slice.Scope().Range)
}

func (s *SliceImpl) MergeWithSlice(slice Slice) []Slice {
	if s.scope.Range.InclusiveMin.CompareTo(slice.Scope().Range.InclusiveMin) > 0 {
		return slice.MergeWithSlice(s)
	}

	if !s.CanMergeWithSlice(slice) {
		panic(fmt.Sprintf("Unable to merge queue slice having scope %v with slice having scope %v", s.scope, slice.Scope()))
	}

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unable to merge queue slice of type %T with type %T", s, slice))
	}

	if s.scope.CanMergeByRange(incomingSlice.scope) {
		return []Slice{s.mergeByRange(incomingSlice)}
	}

	mergedSlices := make([]Slice, 0, 3)
	currentLeftSlice, currentRightSlice := s.splitByRange(incomingSlice.Scope().Range.InclusiveMin)
	mergedSlices = appendMergedSlice(mergedSlices, currentLeftSlice)

	if currentRightMax := currentRightSlice.Scope().Range.ExclusiveMax; incomingSlice.CanSplitByRange(currentRightMax) {
		leftIncomingSlice, rightIncomingSlice := incomingSlice.splitByRange(currentRightMax)
		mergedMidSlice := currentRightSlice.mergeByPredicate(leftIncomingSlice)
		mergedSlices = appendMergedSlice(mergedSlices, mergedMidSlice)
		mergedSlices = appendMergedSlice(mergedSlices, rightIncomingSlice)
	} else {
		currentMidSlice, currentRightSlice := currentRightSlice.splitByRange(incomingSlice.Scope().Range.ExclusiveMax)
		mergedMidSlice := currentMidSlice.mergeByPredicate(incomingSlice)
		mergedSlices = appendMergedSlice(mergedSlices, mergedMidSlice)
		mergedSlices = appendMergedSlice(mergedSlices, currentRightSlice)
	}

	return mergedSlices

}

func (s *SliceImpl) mergeByRange(incomingSlice *SliceImpl) *SliceImpl {
	mergedTaskTracker := s.executableTracker.merge(incomingSlice.executableTracker)
	mergedIterators := s.mergeIterators(incomingSlice)

	s.destroy()
	incomingSlice.destroy()

	return s.newSlice(
		s.scope.MergeByRange(incomingSlice.scope),
		mergedIterators,
		mergedTaskTracker,
	)
}

func (s *SliceImpl) mergeByPredicate(incomingSlice *SliceImpl) *SliceImpl {
	mergedTaskTracker := s.executableTracker.merge(incomingSlice.executableTracker)
	mergedIterators := s.mergeIterators(incomingSlice)

	s.destroy()
	incomingSlice.destroy()

	return s.newSlice(
		s.scope.MergeByPredicate(incomingSlice.scope),
		mergedIterators,
		mergedTaskTracker,
	)
}

func (s *SliceImpl) mergeIterators(incomingSlice *SliceImpl) []Iterator {
	mergedIterators := make([]Iterator, 0, len(s.iterators)+len(incomingSlice.iterators))
	currentIterIdx := 0
	incomingIterIdx := 0
	for currentIterIdx < len(s.iterators) && incomingIterIdx < len(incomingSlice.iterators) {
		currentIter := s.iterators[currentIterIdx]
		incomingIter := incomingSlice.iterators[incomingIterIdx]

		if currentIter.Range().InclusiveMin.CompareTo(incomingIter.Range().InclusiveMin) < 0 {
			mergedIterators = s.appendIterator(mergedIterators, currentIter)
			currentIterIdx++
		} else {
			mergedIterators = s.appendIterator(mergedIterators, incomingIter)
			incomingIterIdx++
		}
	}

	for _, iterator := range s.iterators[currentIterIdx:] {
		mergedIterators = s.appendIterator(mergedIterators, iterator)
	}
	for _, iterator := range incomingSlice.iterators[incomingIterIdx:] {
		mergedIterators = s.appendIterator(mergedIterators, iterator)
	}

	validateIteratorsOrderedDisjoint(mergedIterators)

	return mergedIterators
}

func (s *SliceImpl) appendIterator(
	iterators []Iterator,
	iterator Iterator,
) []Iterator {
	if len(iterators) == 0 {
		return []Iterator{iterator}
	}

	size := len(iterators)
	if iterators[size-1].CanMerge(iterator) {
		iterators[size-1] = iterators[size-1].Merge(iterator)
		return iterators
	}

	return append(iterators, iterator)
}

func (s *SliceImpl) CompactWithSlice(slice Slice) Slice {
	s.stateSanityCheck()

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unable to compact queue slice of type %T with type %T", s, slice))
	}
	incomingSlice.stateSanityCheck()

	compactedScope := NewScope(
		NewRange(
			tasks.MinKey(s.scope.Range.InclusiveMin, incomingSlice.scope.Range.InclusiveMin),
			tasks.MaxKey(s.scope.Range.ExclusiveMax, incomingSlice.scope.Range.ExclusiveMax),
		),
		tasks.OrPredicates(s.scope.Predicate, incomingSlice.scope.Predicate),
	)

	compactedTaskTracker := s.executableTracker.merge(incomingSlice.executableTracker)
	compactedIterators := s.mergeIterators(incomingSlice)

	s.destroy()
	incomingSlice.destroy()

	return s.newSlice(
		compactedScope,
		compactedIterators,
		compactedTaskTracker,
	)
}

func (s *SliceImpl) ShrinkScope() int {
	s.stateSanityCheck()

	tasksCompleted := s.shrinkRange()
	s.shrinkPredicate()

	// shrinkRange shrinks the executableTracker, which may remove tracked pending executables. Set the
	// pending task count to reflect that.
	s.monitor.SetSlicePendingTaskCount(s, len(s.executableTracker.pendingExecutables))

	return tasksCompleted
}

func (s *SliceImpl) shrinkRange() int {
	minPendingTaskKey, tasksCompleted := s.executableTracker.shrink()

	minIteratorKey := tasks.MaximumKey
	if len(s.iterators) != 0 {
		minIteratorKey = s.iterators[0].Range().InclusiveMin
	}

	// pick min key for tasks in memory and in persistence
	newRangeMin := tasks.MinKey(minPendingTaskKey, minIteratorKey)

	// no pending task in memory and in persistence
	if newRangeMin == tasks.MaximumKey {
		newRangeMin = s.scope.Range.ExclusiveMax
	}

	s.scope.Range.InclusiveMin = newRangeMin

	return tasksCompleted
}

func (s *SliceImpl) shrinkPredicate() {
	if len(s.iterators) != 0 {
		// predicate can't be updated if there're still
		// tasks in persistence, as we don't know if those
		// tasks will be filtered out or not if predicate is updated.
		return
	}

	// TODO: this should be generic enough to shrink any predicate type, probably doesn't belong here.
	pendingPerKey := s.executableTracker.pendingPerKey
	if len(pendingPerKey) > shrinkPredicateMaxPendingKeys {
		// only shrink predicate if there're few keys left
		return
	}

	s.scope.Predicate = s.grouper.Predicate(maps.Keys(pendingPerKey))
}

func (s *SliceImpl) SelectTasks(readerID int64, batchSize int) ([]Executable, error) {
	s.stateSanityCheck()

	if len(s.iterators) == 0 {
		return []Executable{}, nil
	}

	defer func() {
		s.monitor.SetSlicePendingTaskCount(s, len(s.executableTracker.pendingExecutables))
	}()

	executables := make([]Executable, 0, batchSize)
	for len(executables) < batchSize && len(s.iterators) != 0 {
		if s.iterators[0].HasNext() {
			task, err := s.iterators[0].Next()
			if err != nil {
				s.iterators[0] = s.iterators[0].Remaining()
				if len(executables) != 0 {
					// NOTE: we must return the executables here
					// MoreTasks() will return true so queue reader will try to load again
					return executables, nil
				}
				return nil, err
			}

			taskKey := task.GetKey()
			if !s.scope.Range.ContainsKey(taskKey) {
				panic(fmt.Sprintf("Queue slice get task from iterator doesn't belong to its range, range: %v, task key %v",
					s.scope.Range, taskKey))
			}

			if !s.scope.Predicate.Test(task) {
				continue
			}

			executable := s.executableFactory.NewExecutable(task, readerID)
			s.executableTracker.add(executable)
			executables = append(executables, executable)
		} else {
			s.iterators = s.iterators[1:]
		}
	}

	return executables, nil
}

func (s *SliceImpl) MoreTasks() bool {
	s.stateSanityCheck()

	return len(s.iterators) != 0
}

func (s *SliceImpl) TaskStats() TaskStats {
	s.stateSanityCheck()

	return TaskStats{
		PendingPerKey: s.executableTracker.pendingPerKey,
	}
}

func (s *SliceImpl) Clear() {
	s.stateSanityCheck()

	s.ShrinkScope()

	s.iterators = []Iterator{
		NewIterator(s.paginationFnProvider, s.scope.Range),
	}
	s.executableTracker.clear()

	s.monitor.SetSlicePendingTaskCount(s, len(s.executableTracker.pendingExecutables))
}

func (s *SliceImpl) destroy() {
	s.destroyed = true
	s.iterators = nil
	s.executableTracker = nil
	s.monitor.RemoveSlice(s)
}

func (s *SliceImpl) stateSanityCheck() {
	if s.destroyed {
		panic("Can not invoke method on destroyed queue slice")
	}
}

func (s *SliceImpl) newSlice(
	scope Scope,
	iterators []Iterator,
	tracker *executableTracker,
) *SliceImpl {
	slice := &SliceImpl{
		paginationFnProvider: s.paginationFnProvider,
		executableFactory:    s.executableFactory,
		scope:                scope,
		iterators:            iterators,
		executableTracker:    tracker,
		monitor:              s.monitor,
	}
	slice.monitor.SetSlicePendingTaskCount(slice, len(slice.executableTracker.pendingExecutables))

	return slice
}

func appendMergedSlice(
	mergedSlices []Slice,
	s *SliceImpl,
) []Slice {
	if s.scope.IsEmpty() {
		s.destroy()
		return mergedSlices
	}

	return append(mergedSlices, s)
}

func validateIteratorsOrderedDisjoint(
	iterators []Iterator,
) {
	if len(iterators) <= 1 {
		return
	}

	for idx, iterator := range iterators[:len(iterators)-1] {
		nextIterator := iterators[idx+1]
		if iterator.Range().ExclusiveMax.CompareTo(nextIterator.Range().InclusiveMin) >= 0 {
			panic(fmt.Sprintf(
				"Found overlapping iterators in iterator list, left range: %v, right range: %v",
				iterator.Range(),
				nextIterator.Range(),
			))
		}
	}
}
