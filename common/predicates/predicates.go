package predicates

type (
	Predicate[T any] interface {
		// Test checks if the given entity statisfy the predicate or not
		Test(T) bool

		// Equals recursively checks if the given Predicate has the same
		// structure and value as the caller Predicate
		// NOTE: the result will contain false negatives, meaning even if
		// two predicates are mathmatically equivalent, Equals may still
		// return false.
		Equals(Predicate[T]) bool

		// Size gets the estimated size in bytes of this predicate.
		// Implementation may keep this estimate rough and mostly account for elements that may take up considerable
		// space such as strings and slices.
		Size() int
	}
)
