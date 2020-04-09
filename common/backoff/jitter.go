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
