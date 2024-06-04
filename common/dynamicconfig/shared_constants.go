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

package dynamicconfig

import (
	"time"

	"go.temporal.io/server/common/primitives"
)

const GlobalDefaultNumTaskQueuePartitions = 4

var defaultNumTaskQueuePartitions = []TypedConstrainedValue[int]{
	// The per-ns worker task queue in all namespaces should only have one partition, since
	// we'll only run one worker per task queue.
	{
		Constraints: Constraints{
			TaskQueueName: primitives.PerNSWorkerTaskQueue,
		},
		Value: 1,
	},

	// The system activity worker task queues in the system local namespace should only have
	// one partition, since we'll only run one worker per task queue.
	{
		Constraints: Constraints{
			TaskQueueName: primitives.AddSearchAttributesActivityTQ,
			Namespace:     primitives.SystemLocalNamespace,
		},
		Value: 1,
	},
	{
		Constraints: Constraints{
			TaskQueueName: primitives.DeleteNamespaceActivityTQ,
			Namespace:     primitives.SystemLocalNamespace,
		},
		Value: 1,
	},
	{
		Constraints: Constraints{
			TaskQueueName: primitives.MigrationActivityTQ,
			Namespace:     primitives.SystemLocalNamespace,
		},
		Value: 1,
	},

	// TODO: After we have a solution for ensuring no tasks are lost, add a constraint here for
	// all task queues in SystemLocalNamespace to have one partition.

	// Default for everything else:
	{
		Value: GlobalDefaultNumTaskQueuePartitions,
	},
}

var DefaultPerShardNamespaceRPSMax = GetIntPropertyFnFilteredByNamespace(0)

// params for controlling dynamic rate limiting options
type DynamicRateLimitingParams struct {
	// Enabled toggles whether dynamic rate limiting is enabled.
	Enabled bool
	// RefreshInterval is how often the rate limit and dynamic properties are refreshed. Should
	// be a string duratoin e.g. 10s even if the rate limiter is disabled, this property will
	// still determine how often the dynamic config is reevaluated.
	RefreshInterval time.Duration
	// LatencyThreshold is the maximum average latency in ms before the rate limiter should
	// backoff.
	LatencyThreshold float64
	// ErrorThreshold is the maximum ratio of errors:total_requests before the rate limiter
	// Should backoff. Should be between 0 and 1.
	ErrorThreshold float64
	// RateBackoffStepSize is the amount the rate limit multiplier is reduced when backing off.
	// Should be between 0 and 1
	RateBackoffStepSize float64
	// RateIncreaseStepSize is the amount the rate limit multiplier is increased when the
	// system is healthy and current rate < max rate. Should be between 0 and 1.
	RateIncreaseStepSize float64
	// RateMultiMin is the minimum the rate limit multiplier can be reduced to.
	RateMultiMin float64
	// RateMultiMax is the maximum the rate limit multiplier can be increased to.
	RateMultiMax float64
}

var DefaultDynamicRateLimitingParams = DynamicRateLimitingParams{
	Enabled:              false,
	RefreshInterval:      10 * time.Second,
	LatencyThreshold:     0.0, // will not do backoff based on latency
	ErrorThreshold:       0.0, // will not do backoff based on errors
	RateBackoffStepSize:  0.3,
	RateIncreaseStepSize: 0.1,
	RateMultiMin:         0.8,
	RateMultiMax:         1.0,
}

type CircuitBreakerSettings struct {
	// MaxRequests: Maximum number of requests allowed to pass through when
	// it is in half-open state (default 1).
	MaxRequests int
	// Interval: Cyclic period in closed state to clear the internal counts;
	// if interval is 0, then it never clears the internal counts (default 0).
	Interval time.Duration
	// Timeout: Period of open state before changing to half-open state (default 60s).`
	Timeout time.Duration
}
