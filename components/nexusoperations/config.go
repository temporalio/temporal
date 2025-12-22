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

var MinRequestTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"component.nexusoperations.limit.request.timeout.min",
	time.Millisecond*1500,
	`MinRequestTimeout is the minimum time remaining for a request to complete for the server to make
RPCs. If the remaining request timeout is less than this value, a non-retryable timeout error will be returned.`,
)

var MinDispatchTaskTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"component.nexusoperations.limit.dispatch.task.timeout.min",
	time.Second,
	`MinDispatchTaskTimeout is the minimum time remaining for a request to be dispatched to the handler worker.
If the remaining request timeout is less than this value, a timeout error will be returned. Working in conjunction with
MinRequestTimeout, both configs help ensure that the server has enough time to complete a Nexus request.`,
)

var MaxConcurrentOperations = dynamicconfig.NewNamespaceIntSetting(
	"component.nexusoperations.limit.operation.concurrency",
	// Temporary limit due to a persistence limitation, this will be increased when we change persistence to accept
	// partial sub state machine updates.
	30,
	`MaxConcurrentOperations limits the maximum allowed concurrent Nexus Operations for a given workflow execution.
Once the limit is reached, ScheduleNexusOperation commands will be rejected.`,
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
	8192,
	`The maximum allowed header size for a Nexus Operation.
ScheduleNexusOperation commands with a "nexus_header" field that exceeds this limit will be rejected.
Uses Go's len() function on header keys and values to determine the total size.`,
)

var UseSystemCallbackURL = dynamicconfig.NewGlobalBoolSetting(
	"component.nexusoperations.useSystemCallbackURL",
	false,
	`UseSystemCallbackURL is a global feature toggle that controls how the executor generates
	callback URLs for worker targets in Nexus Operations.When set to true,
	the executor will use the fixed system callback URL ("temporal://system") for all worker targets,
	instead of generating URLs from the callback URL template.
	This simplifies configuration and improves reliability for worker callbacks.
	- false (default): The executor uses the callback URL template to generate callback URLs for worker targets.
	- true: The executor uses the fixed system callback URL ("temporal://system") for worker targets.
	Note: The default will switch to true in future releases.`,
)

var DisallowedOperationHeaders = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"component.nexusoperations.disallowedHeaders",
	func(in any) ([]string, error) {
		keys, err := dynamicconfig.ConvertStructure[[]string](nil)(in)
		if err != nil {
			return nil, err
		}
		for i, k := range keys {
			keys[i] = strings.ToLower(k)
		}
		return keys, nil
	},
	[]string{
		"request-timeout",
		interceptor.DCRedirectionApiHeaderName,
		interceptor.DCRedirectionContextHeaderName,
		headers.CallerNameHeaderName,
		headers.CallerTypeHeaderName,
		headers.CallOriginHeaderName,
	},
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

var RecordCancelRequestCompletionEvents = dynamicconfig.NewGlobalBoolSetting(
	"component.nexusoperations.recordCancelRequestCompletionEvents",
	true,
	`Boolean flag to control whether to record NexusOperationCancelRequestCompleted and
NexusOperationCancelRequestFailed events. Default true.`,
)

type Config struct {
	Enabled                             dynamicconfig.BoolPropertyFn
	RequestTimeout                      dynamicconfig.DurationPropertyFnWithDestinationFilter
	MinRequestTimeout                   dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxConcurrentOperations             dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxServiceNameLength                dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength              dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationTokenLength             dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationHeaderSize              dynamicconfig.IntPropertyFnWithNamespaceFilter
	DisallowedOperationHeaders          dynamicconfig.TypedPropertyFn[[]string]
	MaxOperationScheduleToCloseTimeout  dynamicconfig.DurationPropertyFnWithNamespaceFilter
	PayloadSizeLimit                    dynamicconfig.IntPropertyFnWithNamespaceFilter
	CallbackURLTemplate                 dynamicconfig.StringPropertyFn
	UseSystemCallbackURL                dynamicconfig.BoolPropertyFn
	RecordCancelRequestCompletionEvents dynamicconfig.BoolPropertyFn
	RetryPolicy                         func() backoff.RetryPolicy
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:                             dynamicconfig.EnableNexus.Get(dc),
		RequestTimeout:                      RequestTimeout.Get(dc),
		MinRequestTimeout:                   MinRequestTimeout.Get(dc),
		MaxConcurrentOperations:             MaxConcurrentOperations.Get(dc),
		MaxServiceNameLength:                MaxServiceNameLength.Get(dc),
		MaxOperationNameLength:              MaxOperationNameLength.Get(dc),
		MaxOperationTokenLength:             MaxOperationTokenLength.Get(dc),
		MaxOperationHeaderSize:              MaxOperationHeaderSize.Get(dc),
		DisallowedOperationHeaders:          DisallowedOperationHeaders.Get(dc),
		MaxOperationScheduleToCloseTimeout:  MaxOperationScheduleToCloseTimeout.Get(dc),
		PayloadSizeLimit:                    dynamicconfig.BlobSizeLimitError.Get(dc),
		CallbackURLTemplate:                 CallbackURLTemplate.Get(dc),
		UseSystemCallbackURL:                UseSystemCallbackURL.Get(dc),
		RecordCancelRequestCompletionEvents: RecordCancelRequestCompletionEvents.Get(dc),
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
