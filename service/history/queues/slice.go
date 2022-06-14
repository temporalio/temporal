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
	"sort"

	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Slice interface {
		Scope() Scope
		CanSplitByRange(tasks.Key) bool
		SplitByRange(tasks.Key) (left Slice, right Slice)
		SplitByPredicate(tasks.Predicate) (pass Slice, fail Slice)
		CanMergeByRange(Slice) bool
		MergeByRange(Slice) Slice
		CanMergeByPredicate(Slice) bool
		MergeByPredicate(Slice) Slice
		ShrinkRange()
		SelectTasks(int) ([]Executable, error)
	}

	executableInitializer func(tasks.Task) Executable

	SliceImpl struct {
		executableInitializer executableInitializer

		destroyed bool

		scope     Scope
		iterators []Iterator

		// TODO: make task tracking a separate component
		// and evaluate the performance of using a btree
		// for storing executables
		outstandingExecutables map[tasks.Key]Executable
	}
)

func NewSlice(
	paginationFnProvider paginationFnProvider,
	executableInitializer executableInitializer,
	scope Scope,
) *SliceImpl {
	return &SliceImpl{
		executableInitializer:  executableInitializer,
		scope:                  scope,
		outstandingExecutables: make(map[tasks.Key]Executable),
		iterators: []Iterator{
			NewIterator(paginationFnProvider, scope.Range),
		},
		destroyed: false,
	}
}

func (s *SliceImpl) Scope() Scope {
	s.validateNotDestroyed()
	return s.scope
}

func (s *SliceImpl) CanSplitByRange(key tasks.Key) bool {
	s.validateNotDestroyed()
	return s.scope.CanSplitByRange(key)
}

func (s *SliceImpl) SplitByRange(key tasks.Key) (leftSlice Slice, rightSlice Slice) {
	if !s.CanSplitByRange(key) {
		panic(fmt.Sprintf("Unable to split queue slice with range %v at %v", s.scope.Range, key))
	}

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

	leftSlice = &SliceImpl{
		executableInitializer:  s.executableInitializer,
		scope:                  leftScope,
		outstandingExecutables: leftExecutables,
		iterators:              leftIterators,
	}
	rightSlice = &SliceImpl{
		executableInitializer:  s.executableInitializer,
		scope:                  rightScope,
		outstandingExecutables: rightExecutables,
		iterators:              rightIterators,
	}

	s.destroy()
	return leftSlice, rightSlice
}

func (s *SliceImpl) SplitByPredicate(predicate tasks.Predicate) (passSlice Slice, failSlice Slice) {
	s.validateNotDestroyed()

	passScope, failScope := s.scope.SplitByPredicate(predicate)
	passExecutables, failExecutables := s.splitExecutables(passScope, failScope)

	passIterators := make([]Iterator, 0, len(s.iterators))
	failIterators := make([]Iterator, 0, len(s.iterators))
	for _, iter := range s.iterators {
		passIterators = append(passIterators, iter)
		failIterators = append(failIterators, iter.Remaining())
	}

	passSlice = &SliceImpl{
		executableInitializer:  s.executableInitializer,
		scope:                  passScope,
		outstandingExecutables: passExecutables,
		iterators:              passIterators,
	}
	failSlice = &SliceImpl{
		executableInitializer:  s.executableInitializer,
		scope:                  failScope,
		outstandingExecutables: failExecutables,
		iterators:              failIterators,
	}

	s.destroy()
	return passSlice, failSlice
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

func (s *SliceImpl) CanMergeByRange(slice Slice) bool {
	s.validateNotDestroyed()

	return s != slice && s.scope.CanMergeByRange(slice.Scope())
}

func (s *SliceImpl) MergeByRange(slice Slice) Slice {
	if !s.CanMergeByRange(slice) {
		panic(fmt.Sprintf("Unable to merge queue slice having scope %v with slice having scope %v by range", s.scope, slice.Scope()))
	}

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unable to merge queue slice of type %T with type %T", s, slice))
	}

	s.destroy()
	incomingSlice.destroy()
	return &SliceImpl{
		executableInitializer:  s.executableInitializer,
		scope:                  s.scope.MergeByRange(incomingSlice.scope),
		outstandingExecutables: s.mergeExecutables(incomingSlice),
		iterators:              s.mergeIterators(incomingSlice),
	}
}

func (s *SliceImpl) CanMergeByPredicate(slice Slice) bool {
	s.validateNotDestroyed()

	return s != slice && s.scope.CanMergeByPredicate(slice.Scope())
}

func (s *SliceImpl) MergeByPredicate(slice Slice) Slice {
	if !s.CanMergeByPredicate(slice) {
		panic(fmt.Sprintf("Unable to merge queue slice having scope %v with slice having scope %v by predicate", s.scope, slice.Scope()))
	}

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unable to merge queue slice of type %T with type %T", s, slice))
	}

	s.destroy()
	incomingSlice.destroy()
	return &SliceImpl{
		executableInitializer:  s.executableInitializer,
		scope:                  s.scope.MergeByPredicate(incomingSlice.scope),
		outstandingExecutables: s.mergeExecutables(incomingSlice),
		iterators:              s.mergeIterators(incomingSlice),
	}
}

func (s *SliceImpl) mergeExecutables(incomingSlice *SliceImpl) map[tasks.Key]Executable {
	thisExecutables := s.outstandingExecutables
	thatExecutables := incomingSlice.outstandingExecutables
	if len(thisExecutables) < len(thatExecutables) {
		thisExecutables, thatExecutables = thatExecutables, thisExecutables
	}

	for key, executable := range thatExecutables {
		thisExecutables[key] = executable
	}

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
	s.validateNotDestroyed()

	var taskKeys tasks.Keys
	taskKeys = make([]tasks.Key, 0, len(s.outstandingExecutables))
	for key := range s.outstandingExecutables {
		taskKeys = append(taskKeys, key)
	}
	sort.Sort(taskKeys)

	for _, key := range taskKeys {
		if s.outstandingExecutables[key].State() == ctasks.TaskStateAcked {
			delete(s.outstandingExecutables, key)
			continue
		}

		s.scope.Range.InclusiveMin = key
		break
	}

	if len(s.outstandingExecutables) == 0 {
		if len(s.iterators) == 0 {
			s.scope.Range.InclusiveMin = s.scope.Range.ExclusiveMax
		} else {
			s.scope.Range.InclusiveMin = s.iterators[0].Range().InclusiveMin
		}
	}
}

func (s *SliceImpl) SelectTasks(batchSize int) ([]Executable, error) {
	s.validateNotDestroyed()

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
}

func (s *SliceImpl) validateNotDestroyed() {
	if s.destroyed {
		panic("Can not invoke method on destroyed queue slice")
	}
}
