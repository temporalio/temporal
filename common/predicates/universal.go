package predicates

type (
	UniversalImpl[T any] struct{}
)

func Universal[T any]() Predicate[T] {
	return &UniversalImpl[T]{}
}

func (a *UniversalImpl[T]) Test(t T) bool {
	return true
}

func (a *UniversalImpl[T]) Equals(
	predicate Predicate[T],
) bool {
	_, ok := predicate.(*UniversalImpl[T])
	return ok
}

func (*UniversalImpl[T]) Size() int {
	return EmptyPredicateProtoSize
}
