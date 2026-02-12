package nexusoperation

import (
	"strings"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/rpc/interceptor"
)

var ChasmNexusEnabled = dynamicconfig.NewGlobalBoolSetting(
	"nexusoperation.enableChasm",
	false,
	`Feature flag that controls whether the legacy HSM-based implementation (when flag is false; default) or the newer
CHASM-based implementation of Nexus will be used when scheduling new Nexus Operations.`,
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"nexusoperation.request.timeout",
	time.Second*10,
	`Timeout for making a single nexus start or cancel request.`,
)

var MinRequestTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"nexusoperation.limit.request.timeout.min",
	time.Millisecond*1500,
	`Minimum time remaining for a request to complete for the server to make RPCs. If the remaining request timeout is
less than this value, a non-retryable timeout error will be returned.`,
)

var MinDispatchTaskTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"nexusoperation.limit.dispatch.task.timeout.min",
	time.Second,
	`Minimum time remaining for a request to be dispatched to the handler worker. If the remaining request timeout is less
than this value, a timeout error will be returned. Working in conjunction with MinRequestTimeout, both configs help
ensure that the server has enough time to complete a Nexus request.`,
)

var MaxConcurrentOperations = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.operation.concurrency",
	2000,
	`Limits the maximum allowed concurrent Nexus Operations for a given workflow execution. Once the limit is reached,
ScheduleNexusOperation commands will be rejected.`,
)

var MaxServiceNameLength = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.service.name.length",
	1000,
	`Limits the maximum allowed length for a Nexus Service name. ScheduleNexusOperation commands with a service name that
exceeds this limit will be rejected.  Uses Go's len() function to determine the length.`,
)

var MaxOperationNameLength = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.operation.name.length",
	1000,
	`Limits the maximum allowed length for a Nexus Operation name. ScheduleNexusOperation commands with an operation name
that exceeds this limit will be rejected.  Uses Go's len() function to determine the length.`,
)

var MaxOperationTokenLength = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.operation.token.length",
	4096,
	`Limits the maximum allowed length for a Nexus Operation token. Tokens returned via start responses or via async
completions that exceed this limit will be rejected. Uses Go's len() function to determine the length.
Leave this limit long enough to fit a workflow ID and namespace name plus padding at minimum since that's what the SDKs
use as the token.`,
)

var MaxOperationHeaderSize = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.header.size",
	8192,
	`The maximum allowed header size for a Nexus Operation.
ScheduleNexusOperation commands with a "nexus_header" field that exceeds this limit will be rejected.
Uses Go's len() function on header keys and values to determine the total size.`,
)

var DisallowedOperationHeaders = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"nexusoperation.disallowedHeaders",
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
	`Case insensitive list of disallowed header keys for Nexus Operations. ScheduleNexusOperation commands with a
"nexus_header" field that contains any of these disallowed keys will be rejected.`,
)

var MaxOperationScheduleToCloseTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"nexusoperation.limit.scheduleToCloseTimeout",
	0,
	`Maximum allowed duration of a Nexus Operation. ScheduleOperation commands that specify no schedule-to-close timeout
or a longer timeout than permitted will have their schedule-to-close timeout capped to this value. 0 implies no limit.`,
)

var CallbackURLTemplate = dynamicconfig.NewGlobalStringSetting(
	"nexusoperation.callback.endpoint.template",
	"unset",
	`Controls the template for generating callback URLs included in Nexus operation requests, which are used to deliver
asynchronous completion for external endpoint targets. The template can be used to interpolate the {{.NamepaceName}}
and {{.NamespaceID}} parameters to construct a publicly accessible URL.
Must be set to call external endpoints.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"nexusoperation.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every nexus StartOperation or CancelOperation request for a given operation.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"nexusoperation.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every nexus StartOperation or CancelOperation request for a given operation.`,
)

var MetricTagConfiguration = dynamicconfig.NewGlobalTypedSetting(
	"nexusoperation.metrics.tags",
	NexusMetricTagConfig{},
	`Controls which metric tags are included with Nexus operation metrics. This configuration supports:
1. Service name tag - adds the Nexus service name as a metric dimension (IncludeServiceTag)
2. Operation name tag - adds the Nexus operation name as a metric dimension (IncludeOperationTag)
3. Header-based tags - maps values from request headers to metric tags (HeaderTagMappings)

Note: default metric tags (like namespace, endpoint) are always included and not affected by this configuration.
Adding high-cardinality tags (like unique operation names) can significantly increase metric storage requirements and
query complexity. Consider the cardinality impact when enabling these tags.`,
)

type Config struct {
	Enabled                             dynamicconfig.BoolPropertyFn
	ChasmEnabled                        dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ChasmNexusEnabled                   dynamicconfig.BoolPropertyFn
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

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:                            dynamicconfig.EnableNexus.Get(dc),
		ChasmEnabled:                       dynamicconfig.EnableChasm.Get(dc),
		ChasmNexusEnabled:                  ChasmNexusEnabled.Get(dc),
		RequestTimeout:                     RequestTimeout.Get(dc),
		MinRequestTimeout:                  MinRequestTimeout.Get(dc),
		MaxConcurrentOperations:            MaxConcurrentOperations.Get(dc),
		MaxServiceNameLength:               MaxServiceNameLength.Get(dc),
		MaxOperationNameLength:             MaxOperationNameLength.Get(dc),
		MaxOperationTokenLength:            MaxOperationTokenLength.Get(dc),
		MaxOperationHeaderSize:             MaxOperationHeaderSize.Get(dc),
		DisallowedOperationHeaders:         DisallowedOperationHeaders.Get(dc),
		MaxOperationScheduleToCloseTimeout: MaxOperationScheduleToCloseTimeout.Get(dc),
		PayloadSizeLimit:                   dynamicconfig.BlobSizeLimitError.Get(dc),
		CallbackURLTemplate:                CallbackURLTemplate.Get(dc),
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
