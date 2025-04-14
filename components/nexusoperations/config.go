// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"strings"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/rpc/interceptor"
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"component.nexusoperations.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for making a single nexus start or cancel request.`,
)

var MinOperationTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"componenet.nexusoperations.limit.operation.timeout.min",
	0,
	`MinOperationTimeout is the minimum time remaining for an operation to complete for the server to make
RPCs. If the remaining operation timeout is less than this value, a non-retryable timeout error will be returned.`,
)

var MaxConcurrentOperations = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.operation.concurrency",
	// Temporary limit due to a persistence limitation, this will be increased when we change persistence to accept
	// partial sub state machine updates.
	30,
	`MaxConcurrentOperations limits the maximum allowed concurrent Nexus Operations for a given workflow execution.
Once the limit is reached, ScheduleNexusOperation commands will be rejected.`,
)

var EndpointNotFoundAlwaysNonRetryable = dynamicconfig.NewNamespaceBoolSetting(
	"component.nexusoperations.endpointNotFoundAlwaysNonRetryable",
	false,
	`When set to true, if an endpoint is not found when processing a ScheduleNexusOperation command, the command will be
	accepted and the operation will fail on the first attempt. This defaults to false to prevent endpoint registry
	propagation delay from failing operations.`,
)

var MaxServiceNameLength = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.service.name.length",
	1000,
	`MaxServiceNameLength limits the maximum allowed length for a Nexus Service name.
ScheduleNexusOperation commands with a service name that exceeds this limit will be rejected.
Uses Go's len() function to determine the length.`,
)

var MaxOperationNameLength = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.operation.name.length",
	1000,
	`MaxOperationNameLength limits the maximum allowed length for a Nexus Operation name.
ScheduleNexusOperation commands with an operation name that exceeds this limit will be rejected.
Uses Go's len() function to determine the length.`,
)

var MaxOperationTokenLength = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.operation.token.length",
	4096,
	`Limits the maximum allowed length for a Nexus Operation token. Tokens returned via start responses or via async
completions that exceed this limit will be rejected. Uses Go's len() function to determine the length.
Leave this limit long enough to fit a workflow ID and namespace name plus padding at minimum since that's what the SDKs
use as the token.`,
)

var MaxOperationHeaderSize = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.header.size",
	4096,
	`The maximum allowed header size for a Nexus Operation.
ScheduleNexusOperation commands with a "nexus_header" field that exceeds this limit will be rejected.
Uses Go's len() function on header keys and values to determine the total size.`,
)

// defaultDisallowedOperationHeaders - set in the convert function below due to a limitation in the dynamic config framework.
// TODO: restore after an upgrade to Go 1.24 and merging #7052.
var defaultDisallowedOperationHeaders = []string{
	"request-timeout",
	interceptor.DCRedirectionApiHeaderName,
	interceptor.DCRedirectionContextHeaderName,
	headers.CallerNameHeaderName,
	headers.CallerTypeHeaderName,
	headers.CallOriginHeaderName,
}

var DisallowedOperationHeaders = dynamicconfig.NewGlobalTypedSetting(
	"component.nexusoperations.disallowedHeaders",
	[]string(nil),
	`Case insensitive list of disallowed header keys for Nexus Operations.
ScheduleNexusOperation commands with a "nexus_header" field that contains any of these disallowed keys will be
rejected.`,
)

var MaxOperationScheduleToCloseTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"component.nexusoperations.limit.scheduleToCloseTimeout",
	0,
	`MaxOperationScheduleToCloseTimeout limits the maximum allowed duration of a Nexus Operation. ScheduleOperation
commands that specify no schedule-to-close timeout or a longer timeout than permitted will have their
schedule-to-close timeout capped to this value. 0 implies no limit.`,
)

var CallbackURLTemplate = dynamicconfig.NewGlobalStringSetting(
	"component.nexusoperations.callback.endpoint.template",
	"unset",
	`Controls the template for generating callback URLs included in Nexus operation requests, which are used to deliver asynchronous completion.
The template can be used to interpolate the {{.NamepaceName}} and {{.NamespaceID}} parameters to construct a publicly accessible URL.
Must be set in order to use Nexus Operations.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.nexusoperations.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every nexus StartOperation or CancelOperation request for a given operation.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.nexusoperations.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every nexus StartOperation or CancelOperation request for a given operation.`,
)

var MetricTagConfiguration = dynamicconfig.NewGlobalTypedSetting(
	"component.nexusoperations.metrics.tags",
	NexusMetricTagConfig{},
	`Controls which metric tags are included with Nexus operation metrics. This configuration supports:
1. Service name tag - adds the Nexus service name as a metric dimension (IncludeServiceTag)
2. Operation name tag - adds the Nexus operation name as a metric dimension (IncludeOperationTag)
3. Header-based tags - maps values from request headers to metric tags (HeaderTagMappings)

Note: default metric tags (like namespace, endpoint) are always included and not affected by this configuration.
Adding high-cardinality tags (like unique operation names) can significantly increase metric storage
requirements and query complexity. Consider the cardinality impact when enabling these tags.`,
)

type Config struct {
	Enabled                            dynamicconfig.BoolPropertyFn
	RequestTimeout                     dynamicconfig.DurationPropertyFnWithDestinationFilter
	MinOperationTimeout                dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxConcurrentOperations            dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxServiceNameLength               dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength             dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationTokenLength            dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationHeaderSize             dynamicconfig.IntPropertyFnWithNamespaceFilter
	DisallowedOperationHeaders         dynamicconfig.TypedPropertyFn[[]string]
	MaxOperationScheduleToCloseTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	PayloadSizeLimit                   dynamicconfig.IntPropertyFnWithNamespaceFilter
	CallbackURLTemplate                dynamicconfig.StringPropertyFn
	EndpointNotFoundAlwaysNonRetryable dynamicconfig.BoolPropertyFnWithNamespaceFilter
	RetryPolicy                        func() backoff.RetryPolicy
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:                 dynamicconfig.EnableNexus.Get(dc),
		RequestTimeout:          RequestTimeout.Get(dc),
		MinOperationTimeout:     MinOperationTimeout.Get(dc),
		MaxConcurrentOperations: MaxConcurrentOperations.Get(dc),
		MaxServiceNameLength:    MaxServiceNameLength.Get(dc),
		MaxOperationNameLength:  MaxOperationNameLength.Get(dc),
		MaxOperationTokenLength: MaxOperationTokenLength.Get(dc),
		MaxOperationHeaderSize:  MaxOperationHeaderSize.Get(dc),
		DisallowedOperationHeaders: dynamicconfig.NewGlobalCachedTypedValue(dc, DisallowedOperationHeaders, func(keys []string) ([]string, error) {
			// Override with defaults unless explicitly set.
			// Note that this prevents the ability to unset the config but that's an acceptable limitation.
			if len(keys) == 0 {
				keys = defaultDisallowedOperationHeaders
			}
			for i, k := range keys {
				keys[i] = strings.ToLower(k)
			}
			return keys, nil
		}).Get,
		MaxOperationScheduleToCloseTimeout: MaxOperationScheduleToCloseTimeout.Get(dc),
		PayloadSizeLimit:                   dynamicconfig.BlobSizeLimitError.Get(dc),
		CallbackURLTemplate:                CallbackURLTemplate.Get(dc),
		EndpointNotFoundAlwaysNonRetryable: EndpointNotFoundAlwaysNonRetryable.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
	}
}
