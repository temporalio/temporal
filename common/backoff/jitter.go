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

const fullCoefficient float64 = 1

// FullJitter return random number from 0 to input, inclusive, exclusive
func FullJitter[T int64 | float64 | time.Duration](input T) T {
	return Jitter(input, fullCoefficient) / 2
}

// Jitter return random number from (1-coefficient)*input to (1+coefficient)*input, inclusive, exclusive
func Jitter[T int64 | float64 | time.Duration](input T, coefficient float64) T {
	validateCoefficient(coefficient)

	if coefficient == 0 {
		return input
	}

	var base float64
	var addon float64
	switch i := any(input).(type) {
	case time.Duration:
		input64 := i.Nanoseconds()
		if input64 == 0 {
			return input
		}
		base = float64(input64) * (1 - coefficient)
		addon = rand.Float64() * 2 * (float64(input64) - base)
	case int64:
		if i == 0 {
			return input
		}
		//base := int64(float64(input) * (1 - coefficient))
		//addon := rand.Int63n(2 * (i - base))
		//return T(base + addon)
		base = float64(i) * (1 - coefficient)
		addon = rand.Float64() * 2 * (float64(i) - base)
	case float64:
		base = i * (1 - coefficient)
		addon = rand.Float64() * 2 * (i - base)
	default:
		panic("The jitter type is not supported")
	}
	return T(base + addon)
}

func validateCoefficient(coefficient float64) {
	if coefficient < 0 || coefficient > 1 {
		panic("coefficient cannot be < 0 or > 1")
	}
}
