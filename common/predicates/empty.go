package predicates

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	EmptyImpl[T any] struct{}
)

var EmptyPredicateProtoSize = (&persistencespb.Predicate{
	PredicateType: enumsspb.PREDICATE_TYPE_EMPTY,
	Attributes: &persistencespb.Predicate_EmptyPredicateAttributes{
		EmptyPredicateAttributes: &persistencespb.EmptyPredicateAttributes{},
	},
}).Size()

func Empty[T any]() Predicate[T] {
	return &EmptyImpl[T]{}
}

func (n *EmptyImpl[T]) Test(t T) bool {
	return false
}

func (n *EmptyImpl[T]) Equals(
	predicate Predicate[T],
) bool {
	_, ok := predicate.(*EmptyImpl[T])
	return ok
}

func (*EmptyImpl[T]) Size() int {
	return EmptyPredicateProtoSize
}
