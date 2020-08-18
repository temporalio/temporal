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

package execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetNextDecisionTimeout(t *testing.T) {
	a := assert.New(t)
	defaultStartToCloseTimeout := 10 * time.Second
	expectedResult := []time.Duration{
		defaultStartToCloseTimeout,
		defaultStartToCloseTimeout,
		defaultInitIntervalForDecisionRetry,
		defaultInitIntervalForDecisionRetry * 2,
		defaultInitIntervalForDecisionRetry * 4,
		defaultMaxIntervalForDecisionRetry,
		defaultMaxIntervalForDecisionRetry,
		defaultMaxIntervalForDecisionRetry,
	}
	for i := 0; i < len(expectedResult); i++ {
		next := getNextDecisionTimeout(int64(i), defaultStartToCloseTimeout)
		expected := expectedResult[i]
		//print("Iter: ", i, ", Next Backoff: ", next.String(), "\n")
		min, max := getNextBackoffRange(expected)
		a.True(next >= min, "NextBackoff too low: actual: %v, expected: %v", next, expected)
		a.True(next <= max, "NextBackoff too high: actual: %v, expected: %v", next, expected)
	}
}

func getNextBackoffRange(duration time.Duration) (time.Duration, time.Duration) {
	rangeMin := time.Duration((1 - defaultJitterCoefficient) * float64(duration))
	return rangeMin, duration
}
