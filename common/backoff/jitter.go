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
