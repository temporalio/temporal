package backoff

import (
	"math/rand"
	"time"
)

// JitDuration return random duration from (1-coefficient)*duration to (1+coefficient)*duration, inclusive, exclusive
func JitDuration(duration time.Duration, coefficient float64) time.Duration {
	validateCoefficient(coefficient)

	return time.Duration(JitInt64(duration.Nanoseconds(), coefficient))
}

// JitInt64 return random number from (1-coefficient)*input to (1+coefficient)*input, inclusive, exclusive
func JitInt64(input int64, coefficient float64) int64 {
	validateCoefficient(coefficient)

	base := int64(float64(input) * (1 - coefficient))
	addon := rand.Int63n(2 * (input - base))
	return base + addon
}

// JitFloat64 return random number from (1-coefficient)*input to (1+coefficient)*input, inclusive, exclusive
func JitFloat64(input float64, coefficient float64) float64 {
	validateCoefficient(coefficient)

	base := input * (1 - coefficient)
	addon := rand.Float64() * 2 * (input - base)
	return base + addon
}

func validateCoefficient(coefficient float64) {
	if coefficient < 0 || coefficient > 1 {
		panic("coefficient cannot be < 0 or > 1")
	}
}
