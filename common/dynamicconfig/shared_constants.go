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
	"math/rand"

	"go.temporal.io/server/common/metrics"
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

const (
	// dynamic config map keys and defaults for client.DynamicRateLimitingParams for controlling dynamic rate limiting options
	// dynamicRateLimitEnabledKey toggles whether dynamic rate limiting is enabled
	dynamicRateLimitEnabledKey     = "enabled"
	dynamicRateLimitEnabledDefault = false
	// dynamicRateLimitRefreshIntervalKey is how often the rate limit and dynamic properties are refreshed. should be a string timestamp e.g. 10s
	// even if the rate limiter is disabled, this property will still determine how often the dynamic config is reevaluated
	dynamicRateLimitRefreshIntervalKey     = "refreshInterval"
	dynamicRateLimitRefreshIntervalDefault = "10s"
	// dynamicRateLimitLatencyThresholdKey is the maximum average latency in ms before the rate limiter should backoff
	dynamicRateLimitLatencyThresholdKey     = "latencyThreshold"
	dynamicRateLimitLatencyThresholdDefault = 0.0 // will not do backoff based on latency
	// dynamicRateLimitErrorThresholdKey is the maximum ratio of errors:total_requests before the rate limiter should backoff. should be between 0 and 1
	dynamicRateLimitErrorThresholdKey     = "errorThreshold"
	dynamicRateLimitErrorThresholdDefault = 0.0 // will not do backoff based on errors
	// dynamicRateLimitBackoffStepSizeKey is the amount the rate limit multiplier is reduced when backing off. should be between 0 and 1
	dynamicRateLimitBackoffStepSizeKey     = "rateBackoffStepSize"
	dynamicRateLimitBackoffStepSizeDefault = 0.3
	// dynamicRateLimitIncreaseStepSizeKey the amount the rate limit multiplier is increased when the system is healthy. should be between 0 and 1
	dynamicRateLimitIncreaseStepSizeKey     = "rateIncreaseStepSize"
	dynamicRateLimitIncreaseStepSizeDefault = 0.1
	// dynamicRateLimitMultiMinKey is the minimum the rate limit multiplier can be reduced to
	dynamicRateLimitMultiMinKey     = "rateMultiMin"
	dynamicRateLimitMultiMinDefault = 0.8
	// dynamicRateLimitMultiMaxKey is the maximum the rate limit multiplier can be increased to
	dynamicRateLimitMultiMaxKey     = "rateMultiMax"
	dynamicRateLimitMultiMaxDefault = 1.0
)

var DefaultDynamicRateLimitingParams = map[string]interface{}{
	dynamicRateLimitEnabledKey:          dynamicRateLimitEnabledDefault,
	dynamicRateLimitRefreshIntervalKey:  dynamicRateLimitRefreshIntervalDefault,
	dynamicRateLimitLatencyThresholdKey: dynamicRateLimitLatencyThresholdDefault,
	dynamicRateLimitErrorThresholdKey:   dynamicRateLimitErrorThresholdDefault,
	dynamicRateLimitBackoffStepSizeKey:  dynamicRateLimitBackoffStepSizeDefault,
	dynamicRateLimitIncreaseStepSizeKey: dynamicRateLimitIncreaseStepSizeDefault,
	dynamicRateLimitMultiMinKey:         dynamicRateLimitMultiMinDefault,
	dynamicRateLimitMultiMaxKey:         dynamicRateLimitMultiMaxDefault,
}

// AccessHistory is an interim config helper for dialing fraction of FE->History calls
// DEPRECATED: Remove once migration is complete
func AccessHistory(accessHistoryFraction FloatPropertyFn, metricsHandler metrics.Handler) bool {
	if rand.Float64() < accessHistoryFraction() {
		metricsHandler.Counter(metrics.AccessHistoryNew).Record(1)
		return true
	}
	metricsHandler.Counter(metrics.AccessHistoryOld).Record(1)
	return false
}
