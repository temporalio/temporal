package activity

import (
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"activity.enableStandalone",
		true,
		`Toggles standalone activity functionality on the server.`,
	)

	StandaloneAsyncHandoffEnabled = dynamicconfig.NewNamespaceBoolSetting(
		"activity.standaloneAsyncHandoffEnabled",
		false,
		`Enables standalone activity handoff reporting and new async handoff options.`,
	)

	WorkflowAsyncHandoffEnabled = dynamicconfig.NewNamespaceBoolSetting(
		"activity.workflowAsyncHandoffEnabled",
		false,
		`Enables workflow activity handoff reporting and new async handoff options.`,
	)

	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"activity.longPollTimeout",
		common.DefaultLongPollTimeout,
		`Timeout for activity long-poll requests.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"activity.longPollBuffer",
		common.DefaultLongPollBuffer,
		`A buffer used to adjust the activity long-poll timeouts.
 Specifically, activity long-poll requests are timed out at a time which leaves at least the buffer's duration
 remaining before the caller's deadline, if permitted by the caller's deadline.`,
	)

	StartDelayEnabled = dynamicconfig.NewNamespaceBoolSetting(
		"activity.startDelayEnabled",
		true,
		`Allows non-zero start_delay on StartActivityExecution requests.`,
	)

	EnableEagerStart = dynamicconfig.NewNamespaceBoolSetting(
		"activity.enableEagerStart",
		true,
		`Allows the first standalone activity task to be returned directly by StartActivityExecution.`,
	)

	EnableCallbacks = dynamicconfig.NewNamespaceBoolSetting(
		"activity.enableCallbacks",
		false,
		`Allows attaching completion callbacks to standalone activity executions.`,
	)

	EnabledCallbackKinds = dynamicconfig.NewNamespaceTypedSettingWithConverter(
		"activity.enabledCallbackKinds",
		callbacks.ConvertEnabledKinds,
		[]callbacks.Kind{callbacks.KindNexus},
		`The list of completion callback kinds that may be attached to a standalone activity execution.
Only consulted when activity.enableCallbacks is set.`,
	)

	EnableStandaloneActivityOperatorCommands = dynamicconfig.NewNamespaceBoolSetting(
		"history.enableStandaloneActivityOperatorCommands",
		false,
		`Enables reset, pause, unpause, and update options commands for standalone activities.`,
	)
)

type Config struct {
	BlobSizeLimitError                        dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	BreakdownMetricsByTaskQueue               dynamicconfig.TypedPropertyFnWithTaskQueueFilter[bool]
	DefaultActivityRetryPolicy                dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
	EnableActivityEagerDispatchCheck          dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableCallbacks                           dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableEagerStart                          dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableStandaloneActivityOperatorCommands  dynamicconfig.BoolPropertyFnWithNamespaceFilter
	Enabled                                   dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnabledCallbackKinds                      dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]callbacks.Kind]
	LongPollBuffer                            dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollTimeout                           dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxCallbacksPerExecution                  dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit                          dynamicconfig.IntPropertyFn
	MaxUserMetadataDetailsSize                dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxUserMetadataSummarySize                dynamicconfig.IntPropertyFnWithNamespaceFilter
	MutableStateActivityFailureSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	StandaloneAsyncHandoffEnabled             dynamicconfig.BoolPropertyFnWithNamespaceFilter
	StartDelayEnabled                         dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityMaxPageSize                     dynamicconfig.IntPropertyFnWithNamespaceFilter
	WorkflowAsyncHandoffEnabled               dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		BlobSizeLimitError:                        dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:                         dynamicconfig.BlobSizeLimitWarn.Get(dc),
		BreakdownMetricsByTaskQueue:               dynamicconfig.MetricsBreakdownByTaskQueue.Get(dc),
		DefaultActivityRetryPolicy:                dynamicconfig.DefaultActivityRetryPolicy.Get(dc),
		EnableActivityEagerDispatchCheck:          dynamicconfig.EnableActivityEagerDispatchCheck.Get(dc),
		EnableCallbacks:                           EnableCallbacks.Get(dc),
		EnableEagerStart:                          EnableEagerStart.Get(dc),
		EnableStandaloneActivityOperatorCommands:  EnableStandaloneActivityOperatorCommands.Get(dc),
		Enabled:                                   Enabled.Get(dc),
		EnabledCallbackKinds:                      EnabledCallbackKinds.Get(dc),
		LongPollBuffer:                            LongPollBuffer.Get(dc),
		LongPollTimeout:                           LongPollTimeout.Get(dc),
		MaxCallbacksPerExecution:                  callback.MaxPerExecution.Get(dc),
		MaxIDLengthLimit:                          dynamicconfig.MaxIDLengthLimit.Get(dc),
		MaxUserMetadataDetailsSize:                dynamicconfig.MaxUserMetadataDetailsSize.Get(dc),
		MaxUserMetadataSummarySize:                dynamicconfig.MaxUserMetadataSummarySize.Get(dc),
		MutableStateActivityFailureSizeLimitError: dynamicconfig.MutableStateActivityFailureSizeLimitError.Get(dc),
		StandaloneAsyncHandoffEnabled:             StandaloneAsyncHandoffEnabled.Get(dc),
		StartDelayEnabled:                         StartDelayEnabled.Get(dc),
		VisibilityMaxPageSize:                     dynamicconfig.FrontendVisibilityMaxPageSize.Get(dc),
		WorkflowAsyncHandoffEnabled:               WorkflowAsyncHandoffEnabled.Get(dc),
	}
}

// linkValidatorProvider builds the linkValidator from dynamic config.
func linkValidatorProvider(dc *dynamicconfig.Collection) *linkValidator {
	return newLinkValidator(
		dynamicconfig.FrontendMaxLinksPerRequest.Get(dc),
		dynamicconfig.MaxLinksPerComponent.Get(dc),
		dynamicconfig.FrontendLinkMaxSize.Get(dc),
	)
}
