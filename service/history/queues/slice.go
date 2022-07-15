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

	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/maps"
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
		ShrinkRange()
		SelectTasks(int) ([]Executable, error)
	}

	ExecutableInitializer func(tasks.Task) Executable

	SliceImpl struct {
		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		destroyed bool

		scope     Scope
		iterators []Iterator

		// TODO: make task tracking a separate component
		outstandingExecutables map[tasks.Key]Executable
	}
)

func NewSlice(
	paginationFnProvider PaginationFnProvider,
	executableInitializer ExecutableInitializer,
	scope Scope,
) *SliceImpl {
	return &SliceImpl{
		paginationFnProvider:   paginationFnProvider,
		executableInitializer:  executableInitializer,
		scope:                  scope,
		outstandingExecutables: make(map[tasks.Key]Executable),
		iterators: []Iterator{
			NewIterator(paginationFnProvider, scope.Range),
		},
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
	leftExecutables, rightExecutables := s.splitExecutables(leftScope, rightScope)

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

	left = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  leftScope,
		outstandingExecutables: leftExecutables,
		iterators:              leftIterators,
	}
	right = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  rightScope,
		outstandingExecutables: rightExecutables,
		iterators:              rightIterators,
	}

	s.destroy()
	return left, right
}

func (s *SliceImpl) SplitByPredicate(predicate tasks.Predicate) (pass Slice, fail Slice) {
	s.stateSanityCheck()

	passScope, failScope := s.scope.SplitByPredicate(predicate)
	passExecutables, failExecutables := s.splitExecutables(passScope, failScope)

	passIterators := make([]Iterator, 0, len(s.iterators))
	failIterators := make([]Iterator, 0, len(s.iterators))
	for _, iter := range s.iterators {
		// iter.Remaining() is basically a deep copy of iter
		passIterators = append(passIterators, iter)
		failIterators = append(failIterators, iter.Remaining())
	}

	pass = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  passScope,
		outstandingExecutables: passExecutables,
		iterators:              passIterators,
	}
	fail = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  failScope,
		outstandingExecutables: failExecutables,
		iterators:              failIterators,
	}

	s.destroy()
	return pass, fail
}

func (s *SliceImpl) splitExecutables(
	thisScope Scope,
	thatScope Scope,
) (map[tasks.Key]Executable, map[tasks.Key]Executable) {
	thisExecutable := s.outstandingExecutables
	thatExecutables := make(map[tasks.Key]Executable, len(s.outstandingExecutables)/2)
	for key, executable := range s.outstandingExecutables {
		if thisScope.Contains(executable) {
			continue
		}

		if !thatScope.Contains(executable) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to its scope, scope: %v, task: %v, task type: %v",
				s.scope, executable.GetTask(), executable.GetType()))
		}
		delete(thisExecutable, key)
		thatExecutables[key] = executable
	}
	return thisExecutable, thatExecutables
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
	if !currentLeftSlice.scope.IsEmpty() {
		mergedSlices = append(mergedSlices, currentLeftSlice)
	}

	if currentRightMax := currentRightSlice.Scope().Range.ExclusiveMax; incomingSlice.CanSplitByRange(currentRightMax) {
		leftIncomingSlice, rightIncomingSlice := incomingSlice.splitByRange(currentRightMax)
		mergedMidSlice := currentRightSlice.mergeByPredicate(leftIncomingSlice)
		if !mergedMidSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, mergedMidSlice)
		}
		if !rightIncomingSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, rightIncomingSlice)
		}
	} else {
		currentMidSlice, currentRightSlice := currentRightSlice.splitByRange(incomingSlice.Scope().Range.ExclusiveMax)
		mergedMidSlice := currentMidSlice.mergeByPredicate(incomingSlice)
		if !mergedMidSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, mergedMidSlice)
		}
		if !currentRightSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, currentRightSlice)
		}
	}

	return mergedSlices

}

func (s *SliceImpl) mergeByRange(incomingSlice *SliceImpl) *SliceImpl {
	mergedSlice := &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  s.scope.MergeByRange(incomingSlice.scope),
		outstandingExecutables: s.mergeExecutables(incomingSlice),
		iterators:              s.mergeIterators(incomingSlice),
	}

	s.destroy()
	incomingSlice.destroy()

	return mergedSlice
}

func (s *SliceImpl) mergeByPredicate(incomingSlice *SliceImpl) *SliceImpl {
	mergedSlice := &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  s.scope.MergeByPredicate(incomingSlice.scope),
		outstandingExecutables: s.mergeExecutables(incomingSlice),
		iterators:              s.mergeIterators(incomingSlice),
	}

	s.destroy()
	incomingSlice.destroy()

	return mergedSlice
}

func (s *SliceImpl) mergeExecutables(incomingSlice *SliceImpl) map[tasks.Key]Executable {
	thisExecutables := s.outstandingExecutables
	thatExecutables := incomingSlice.outstandingExecutables
	if len(thisExecutables) < len(thatExecutables) {
		thisExecutables, thatExecutables = thatExecutables, thisExecutables
	}
	maps.Copy(thisExecutables, thatExecutables)
	return thisExecutables
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

func (s *SliceImpl) ShrinkRange() {
	s.stateSanityCheck()

	minPendingTaskKey := tasks.MaximumKey
	for key := range s.outstandingExecutables {
		if s.outstandingExecutables[key].State() == ctasks.TaskStateAcked {
			delete(s.outstandingExecutables, key)
			continue
		}

		minPendingTaskKey = tasks.MinKey(minPendingTaskKey, key)
	}

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
}

func (s *SliceImpl) SelectTasks(batchSize int) ([]Executable, error) {
	s.stateSanityCheck()

	if len(s.iterators) == 0 {
		return []Executable{}, nil
	}

	executables := make([]Executable, 0, batchSize)
	for len(executables) < batchSize && len(s.iterators) != 0 {
		if s.iterators[0].HasNext() {
			task, err := s.iterators[0].Next()
			if err != nil {
				s.iterators[0] = s.iterators[0].Remaining()
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

			executable := s.executableInitializer(task)
			s.outstandingExecutables[taskKey] = executable
			executables = append(executables, executable)
		} else {
			s.iterators = s.iterators[1:]
		}
	}

	return executables, nil
}

func (s *SliceImpl) destroy() {
	s.destroyed = true
	s.iterators = nil
	s.outstandingExecutables = nil
}

func (s *SliceImpl) stateSanityCheck() {
	if s.destroyed {
		panic("Can not invoke method on destroyed queue slice")
	}
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
