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

func (a *AndImpl[T]) Size() int {
	size := EmptyPredicateProtoSize
	for _, p := range a.Predicates {
		size += p.Size()
	}

	return size
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
