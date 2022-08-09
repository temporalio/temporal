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

package predicates

type (
	NotImpl[T any] struct {
		Predicate Predicate[T]
	}
)

func Not[T any](
	predicate Predicate[T],
) Predicate[T] {
	switch p := predicate.(type) {
	case *NotImpl[T]:
		return p.Predicate
	case *UniversalImpl[T]:
		return Empty[T]()
	case *EmptyImpl[T]:
		return Universal[T]()
	default:
		return &NotImpl[T]{
			Predicate: predicate,
		}
	}
}

func (n *NotImpl[T]) Test(t T) bool {
	return !n.Predicate.Test(t)
}

func (n *NotImpl[T]) Equals(
	predicate Predicate[T],
) bool {
	notPredicate, ok := predicate.(*NotImpl[T])
	if !ok {
		return false
	}
	return n.Predicate.Equals(notPredicate.Predicate)
}
