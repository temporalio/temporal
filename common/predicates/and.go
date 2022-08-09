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
	AndImpl[T any] struct {
		// TODO: see if we can somehow order arbitrary predicats and store a sorted list
		Predicates []Predicate[T]
	}
)

func And[T any](
	predicates ...Predicate[T],
) Predicate[T] {
	if len(predicates) < 2 {
		panic(fmt.Sprintf("And requires at least 2 predicates, got %v", len(predicates)))
	}

	flattened := make([]Predicate[T], 0, len(predicates))
	for _, p := range predicates {
		switch p := p.(type) {
		case *AndImpl[T]:
			flattened = appendPredicates(flattened, p.Predicates...)
		case *UniversalImpl[T]:
			continue
		case *EmptyImpl[T]:
			return p
		default:
			flattened = appendPredicates(flattened, p)
		}
	}

	switch len(flattened) {
	case 0:
		return Universal[T]()
	case 1:
		return flattened[0]
	default:
		return &AndImpl[T]{
			Predicates: flattened,
		}
	}
}

func (a *AndImpl[T]) Test(t T) bool {
	for _, p := range a.Predicates {
		if !p.Test(t) {
			return false
		}
	}

	return true
}

func (a *AndImpl[T]) Equals(
	predicate Predicate[T],
) bool {
	andPredicate, ok := predicate.(*AndImpl[T])
	if !ok {
		return false
	}

	return predicatesEqual(a.Predicates, andPredicate.Predicates)
}

// appendPredicates adds new predicates to the slice of existing predicates
// dropping any duplicated predicates where duplication is determined by Predicate.Equals.
// appendPredicates assumes that there's no duplication in new predicates.
func appendPredicates[T any](
	current []Predicate[T],
	new ...Predicate[T],
) []Predicate[T] {
	result := current

AppendLoop:
	for _, newPredicate := range new {
		for _, currentPredicate := range current {
			if currentPredicate.Equals(newPredicate) {
				continue AppendLoop
			}
		}

		result = append(result, newPredicate)
	}

	return result
}

// predicatesEqual assumes there's no duplication in the given slices of predicates
func predicatesEqual[T any](
	this []Predicate[T],
	that []Predicate[T],
) bool {
	if len(this) != len(that) {
		return false
	}

MatchLoop:
	for _, thisPredicate := range this {
		for _, thatPredicate := range that {
			if thisPredicate.Equals(thatPredicate) {
				continue MatchLoop
			}
		}

		return false
	}

	return true
}
