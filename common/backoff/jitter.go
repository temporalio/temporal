package backoff

import (
	"math/rand"
)

const fullCoefficient float64 = 1

// FullJitter return random number from 0 to input, inclusive, exclusive
func FullJitter[T ~int64 | ~int | ~int32 | ~float64 | ~float32](input T) T {
	return T(rand.Float64() * float64(input))
}

// Jitter return random number from (1-coefficient)*input to (1+coefficient)*input, inclusive, exclusive
func Jitter[T ~int64 | ~int | ~int32 | ~float64 | ~float32](input T, coefficient float64) T {
	validateCoefficient(coefficient)

	if coefficient == 0 {
		return input
	}

	base := float64(input) * (1 - coefficient)
	addon := rand.Float64() * 2 * (float64(input) - base)
	return T(base + addon)
}

func validateCoefficient(coefficient float64) {
	if coefficient < 0 || coefficient > 1 {
		panic("coefficient cannot be < 0 or > 1")
	}
}
