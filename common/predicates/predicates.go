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

import "fmt"

type (
	Predicate[T any] interface {
		Test(T) bool
	}

	AndImpl[T any] struct {
		Predicates []Predicate[T]
	}

	OrImpl[T any] struct {
		Predicates []Predicate[T]
	}

	NotImpl[T any] struct {
		Predicate Predicate[T]
	}

	AllImpl[T any] struct{}

	NoneImpl[T any] struct{}
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
			flattened = append(flattened, p.Predicates...)
		case *AllImpl[T]:
			continue
		case *NoneImpl[T]:
			return p
		default:
			flattened = append(flattened, p)
		}
	}

	switch len(flattened) {
	case 0:
		return All[T]()
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
			flattened = append(flattened, p.Predicates...)
		case *AllImpl[T]:
			return p
		case *NoneImpl[T]:
			continue
		default:
			flattened = append(flattened, p)
		}
	}

	switch len(flattened) {
	case 0:
		return None[T]()
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

func Not[T any](
	predicate Predicate[T],
) Predicate[T] {
	switch p := predicate.(type) {
	case *NotImpl[T]:
		return p.Predicate
	case *AllImpl[T]:
		return None[T]()
	case *NoneImpl[T]:
		return All[T]()
	default:
		return &NotImpl[T]{
			Predicate: predicate,
		}
	}
}

func (n *NotImpl[T]) Test(t T) bool {
	return !n.Predicate.Test(t)
}

func All[T any]() Predicate[T] {
	return &AllImpl[T]{}
}

func (a *AllImpl[T]) Test(t T) bool {
	return true
}

func None[T any]() Predicate[T] {
	return &NoneImpl[T]{}
}

func (n *NoneImpl[T]) Test(t T) bool {
	return false
}
