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

func (n *NotImpl[T]) Size() int {
	return n.Predicate.Size() + EmptyPredicateProtoSize
}
