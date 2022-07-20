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

import (
	"fmt"
)

type (
	OrImpl[T any] struct {
		// TODO: see if we can somehow order arbitrary predicats and store a sorted list
		Predicates []Predicate[T]
	}
)

func Or[T any](
	predicates ...Predicate[T],
) Predicate[T] {
	if len(predicates) < 2 {
		panic(fmt.Sprintf("Or requires at least 2 predicates, got %v", len(predicates)))
	}

	flattened := make([]Predicate[T], 0, len(predicates))
	for _, p := range predicates {
		switch p := p.(type) {
		case *OrImpl[T]:
			flattened = appendPredicates(flattened, p.Predicates...)
		case *UniversalImpl[T]:
			return p
		case *EmptyImpl[T]:
			continue
		default:
			flattened = appendPredicates(flattened, p)
		}
	}

	switch len(flattened) {
	case 0:
		return Empty[T]()
	case 1:
		return flattened[0]
	default:
		return &OrImpl[T]{
			Predicates: flattened,
		}
	}
}

func (o *OrImpl[T]) Test(t T) bool {
	for _, p := range o.Predicates {
		if p.Test(t) {
			return true
		}
	}

	return false
}

func (o *OrImpl[T]) Equals(
	predicate Predicate[T],
) bool {
	orPredicate, ok := predicate.(*OrImpl[T])
	if !ok {
		return false
	}

	return predicatesEqual(o.Predicates, orPredicate.Predicates)
}
