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

func (o *OrImpl[T]) Size() int {
	size := EmptyPredicateProtoSize
	for _, p := range o.Predicates {
		size += p.Size()
	}

	return size
}
