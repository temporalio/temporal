package nexusoperation

import (
	"fmt"
	"strings"
	"text/template"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/rpc/interceptor"
)

var LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"nexusoperation.longPollTimeout",
	20*time.Second,
	`Maximum timeout for nexus operation long-poll requests. Actual wait may be shorter to leave
longPollBuffer before the caller deadline.`,
)

var LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
	"nexusoperation.longPollBuffer",
	time.Second,
	`A buffer used to adjust the nexus operation long-poll timeouts.
 Specifically, nexus operation long-poll requests are timed out at a time which leaves at least the buffer's duration
 remaining before the caller's deadline, if permitted by the caller's deadline.`,
)

var Enabled = dynamicconfig.NewNamespaceBoolSetting(
	"nexusoperation.enableStandalone",
	false,
	`Toggles standalone Nexus operation functionality on the server.`,
)

var EnableChasmNexus = dynamicconfig.NewNamespaceBoolSetting(
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

var MaxConcurrentOperationsPerWorkflow = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.operation.concurrencyPerWorkflow.max",
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
		headers.PrincipalTypeHeaderName,
		headers.PrincipalNameHeaderName,
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

var CallbackURLTemplate = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"nexusoperation.callback.endpoint.template",
	func(in any) (*template.Template, error) {
		s, ok := in.(string)
		if !ok {
			return nil, fmt.Errorf("invalid config type: %T for nexusoperation.callback.endpoint.template, expected string", in)
		}
		if s == "unset" {
			return nil, nil
		}
		return template.New("NexusCallbackURL").Parse(s)
	},
	nil,
	`Controls the template for generating callback URLs included in Nexus operation requests, which are used to deliver
asynchronous completion for external endpoint targets. The template can be used to interpolate the {{.NamepaceName}}
and {{.NamespaceID}} parameters to construct a publicly accessible URL.
Must be set to call external endpoints.`,
)

type RetryPolicyConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

func (cfg RetryPolicyConfig) build() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(cfg.InitialInterval).
		WithMaximumInterval(cfg.MaxInterval).
		WithExpirationInterval(backoff.NoInterval)
}

var defaultRetryPolicyConfig = RetryPolicyConfig{
	InitialInterval: time.Second,
	MaxInterval:     time.Hour,
}

var RetryPolicy = dynamicconfig.NewGlobalTypedSettingWithConverter(
	"nexusoperation.retryPolicy",
	func(in any) (backoff.RetryPolicy, error) {
		cfg, err := dynamicconfig.ConvertStructure(defaultRetryPolicyConfig)(in)
		if err != nil {
			return nil, err
		}
		return cfg.build(), nil
	},
	defaultRetryPolicyConfig.build(),
	`The retry policy for nexus StartOperation or CancelOperation requests for a given operation.`,
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

var UseSystemCallbackURL = dynamicconfig.NewGlobalBoolSetting(
	"nexusoperation.useSystemCallbackURL",
	true,
	`Controls how the executor generates callback URLs for worker targets in Nexus Operations.
When true, uses the fixed system callback URL for all worker targets.`,
)

var MaxReasonLength = dynamicconfig.NewNamespaceIntSetting(
	"nexusoperation.limit.reasonLength",
	1000,
	`Limits the maximum allowed length for a reason string in Nexus operation requests.
Uses Go's len() function to determine the length.`,
)

var UseNewFailureWireFormat = dynamicconfig.NewNamespaceBoolSetting(
	"nexusoperation.useNewFailureWireFormat",
	true,
	`Controls whether to use the new failure wire format via an HTTP header that is attached to StartOperation requests.
Added for safety. Defaults to true. Likely to be removed in future server versions.`,
)

type Config struct {
	Enabled                             dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableChasm                         dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableChasmNexus                    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	NumHistoryShards                    int32
	LongPollBuffer                      dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollTimeout                     dynamicconfig.DurationPropertyFnWithNamespaceFilter
	RequestTimeout                      dynamicconfig.DurationPropertyFnWithDestinationFilter
	MinRequestTimeout                   dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxConcurrentOperationsPerWorkflow  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxServiceNameLength                dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationNameLength              dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationTokenLength             dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxOperationHeaderSize              dynamicconfig.IntPropertyFnWithNamespaceFilter
	DisallowedOperationHeaders          dynamicconfig.TypedPropertyFn[[]string]
	MaxOperationScheduleToCloseTimeout  dynamicconfig.DurationPropertyFnWithNamespaceFilter
	PayloadSizeLimit                    dynamicconfig.IntPropertyFnWithNamespaceFilter
	CallbackURLTemplate                 dynamicconfig.TypedPropertyFn[*template.Template]
	UseSystemCallbackURL                dynamicconfig.BoolPropertyFn
	PayloadSizeLimitWarn                dynamicconfig.IntPropertyFnWithNamespaceFilter
	UseNewFailureWireFormat             dynamicconfig.BoolPropertyFnWithNamespaceFilter
	RecordCancelRequestCompletionEvents dynamicconfig.BoolPropertyFn
	VisibilityMaxPageSize               dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit                    dynamicconfig.IntPropertyFn
	MaxReasonLength                     dynamicconfig.IntPropertyFnWithNamespaceFilter
	RetryPolicy                         func() backoff.RetryPolicy
}

func configProvider(dc *dynamicconfig.Collection, cfg *config.Persistence) *Config {
	return &Config{
		Enabled:                            Enabled.Get(dc),
		EnableChasm:                        dynamicconfig.EnableChasm.Get(dc),
		EnableChasmNexus:                   EnableChasmNexus.Get(dc),
		NumHistoryShards:                   cfg.NumHistoryShards,
		LongPollBuffer:                     LongPollBuffer.Get(dc),
		LongPollTimeout:                    LongPollTimeout.Get(dc),
		RequestTimeout:                     RequestTimeout.Get(dc),
		MinRequestTimeout:                  MinRequestTimeout.Get(dc),
		MaxConcurrentOperationsPerWorkflow: MaxConcurrentOperationsPerWorkflow.Get(dc),
		MaxServiceNameLength:               MaxServiceNameLength.Get(dc),
		MaxOperationNameLength:             MaxOperationNameLength.Get(dc),
		MaxOperationTokenLength:            MaxOperationTokenLength.Get(dc),
		MaxOperationHeaderSize:             MaxOperationHeaderSize.Get(dc),
		DisallowedOperationHeaders:         DisallowedOperationHeaders.Get(dc),
		MaxOperationScheduleToCloseTimeout: MaxOperationScheduleToCloseTimeout.Get(dc),
		PayloadSizeLimit:                   dynamicconfig.BlobSizeLimitError.Get(dc),
		PayloadSizeLimitWarn:               dynamicconfig.BlobSizeLimitWarn.Get(dc),
		CallbackURLTemplate:                CallbackURLTemplate.Get(dc),
		UseSystemCallbackURL:               UseSystemCallbackURL.Get(dc),
		UseNewFailureWireFormat:            UseNewFailureWireFormat.Get(dc),
		VisibilityMaxPageSize:              dynamicconfig.FrontendVisibilityMaxPageSize.Get(dc),
		MaxIDLengthLimit:                   dynamicconfig.MaxIDLengthLimit.Get(dc),
		MaxReasonLength:                    MaxReasonLength.Get(dc),
		RetryPolicy:                        RetryPolicy.Get(dc),
	}
}
