package backoff

import "math/rand"

// FullJitter return random number from 0 to input, inclusive, exclusive
func FullJitter[T ~int64 | ~int | ~int32 | ~float64 | ~float32](input T) T {
	return T(rand.Float64() * float64(input))
}

// Jitter return random number from (1-coefficient)*input to (1+coefficient)*input, inclusive, exclusive.
// coefficient is clamped into [0, 1]; values outside that range are coerced to the nearest bound.
func Jitter[T ~int64 | ~int | ~int32 | ~float64 | ~float32](input T, coefficient float64) T {
	coefficient = max(0.0, min(1.0, coefficient))

	if coefficient == 0 {
		return input
	}

	base := float64(input) * (1 - coefficient)
	addon := rand.Float64() * 2 * (float64(input) - base)
	return T(base + addon)
}
