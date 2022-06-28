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

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Iterator interface {
		collection.Iterator[tasks.Task]

		Range() Range
		CanSplit(tasks.Key) bool
		Split(key tasks.Key) (left Iterator, right Iterator)
		CanMerge(Iterator) bool
		Merge(Iterator) Iterator
		Remaining() Iterator
	}

	PaginationFnProvider func(Range) collection.PaginationFn[tasks.Task]

	IteratorImpl struct {
		paginationFnProvider PaginationFnProvider
		remainingRange       Range

		pagingIterator collection.Iterator[tasks.Task]
	}
)

func NewIterator(
	paginationFnProvider PaginationFnProvider,
	r Range,
) *IteratorImpl {
	return &IteratorImpl{
		paginationFnProvider: paginationFnProvider,
		remainingRange:       r,

		// lazy initialized to prevent task pre-fetching on creating the iterator
		pagingIterator: nil,
	}
}

func (i *IteratorImpl) HasNext() bool {
	if i.pagingIterator == nil {
		i.pagingIterator = collection.NewPagingIterator(i.paginationFnProvider(i.remainingRange))
	}

	return i.pagingIterator.HasNext()
}

func (i *IteratorImpl) Next() (tasks.Task, error) {
	if !i.HasNext() {
		panic("Iterator encountered Next call when there is no next item")
	}

	task, err := i.pagingIterator.Next()
	if err != nil {
		return nil, err
	}

	i.remainingRange.InclusiveMin = task.GetKey().Next()
	return task, nil
}

func (i *IteratorImpl) Range() Range {
	return i.remainingRange
}

func (i *IteratorImpl) CanSplit(key tasks.Key) bool {
	return i.remainingRange.CanSplit(key)
}

func (i *IteratorImpl) Split(key tasks.Key) (left Iterator, right Iterator) {
	if !i.CanSplit(key) {
		panic(fmt.Sprintf("Unable to split iterator with range %v at %v", i.remainingRange, key))
	}

	leftRange, rightRange := i.remainingRange.Split(key)
	left = NewIterator(
		i.paginationFnProvider,
		leftRange,
	)
	right = NewIterator(
		i.paginationFnProvider,
		rightRange,
	)
	return left, right
}

func (i *IteratorImpl) CanMerge(iter Iterator) bool {
	return i.remainingRange.CanMerge(iter.Range())
}

func (i *IteratorImpl) Merge(iter Iterator) Iterator {
	if !i.CanMerge(iter) {
		panic(fmt.Sprintf("Unable to merge iterator range %v with incoming iterator range %v", i.remainingRange, iter.Range()))
	}

	return NewIterator(
		i.paginationFnProvider,
		i.remainingRange.Merge(iter.Range()),
	)
}

func (i *IteratorImpl) Remaining() Iterator {
	return NewIterator(
		i.paginationFnProvider,
		i.remainingRange,
	)
}
