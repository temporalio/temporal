// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

type testSequentialTaskQueue[T Task] struct {
	q  chan T
	id int
}

func newTestSequentialTaskQueue[T Task](id, capacity int) SequentialTaskQueue[T] {
	return &testSequentialTaskQueue[T]{
		q:  make(chan T, capacity),
		id: id,
	}
}

func (s *testSequentialTaskQueue[T]) ID() interface{} {
	return s.id
}

func (s *testSequentialTaskQueue[T]) Add(task T) {
	s.q <- task
}

func (s *testSequentialTaskQueue[T]) Remove() T {
	select {
	case t := <-s.q:
		return t
	default:
		var emptyT T
		return emptyT
	}
}

func (s *testSequentialTaskQueue[T]) IsEmpty() bool {
	return len(s.q) == 0
}

func (s *testSequentialTaskQueue[T]) Len() int {
	return len(s.q)
}
