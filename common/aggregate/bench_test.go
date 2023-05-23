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

package aggregate

import (
	"math/rand"
	"testing"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

// BenchmarkRingMovingWindowAvg
// BenchmarkRingMovingWindowAvg-10    		12283236	        94.76 ns/op

// BenchmarkHealthRequestRateLimiter
// BenchmarkHealthRequestRateLimiter-10    	15905046	        71.08 ns/op

const (
	testWindowSize = 3 * time.Second
	testBufferSize = 200

	testRPS = 4000
)

func BenchmarkRingMovingWindowAvg(b *testing.B) {
	avg := NewMovingWindowAvgImpl(testWindowSize, testBufferSize)
	for i := 0; i < b.N; i++ {
		avg.Record(rand.Int63())
		avg.Average()
	}
}

func BenchmarkHealthRequestRateLimiter(b *testing.B) {
	testRequest := quotas.NewRequest(
		"bench-test-api",
		1,
		"bench-test-ns",
		"bench-test-caller-type",
		2,
		"bench-test",
	)

	healthSignals := NewPerShardPerNsHealthSignalAggregator(
		dynamicconfig.GetDurationPropertyFn(testWindowSize),
		dynamicconfig.GetIntPropertyFn(testBufferSize),
		metrics.NoopMetricsHandler,
	)

	rateLimiter := NewHealthRequestRateLimiterImpl(
		healthSignals,
		testWindowSize,
		func() float64 { return float64(testRPS) },
		float64(100),
		float64(1),
		0.3,
		0.1,
	)

	for i := 0; i < b.N; i++ {
		rateLimiter.Allow(time.Now(), testRequest)
	}
}
