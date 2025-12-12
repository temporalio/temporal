package activity

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"activity.enableStandalone",
		false,
		`Toggles standalone activity functionality on the server.`,
	)

	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"activity.longPollTimeout",
		20*time.Second,
		`Timeout for activity long-poll requests.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"activity.longPollBuffer",
		time.Second,
		`A buffer used to adjust the activity long-poll timeouts.
 Specifically, activity long-poll requests are timed out at a time which leaves at least the buffer's duration
 remaining before the caller's deadline, if permitted by the caller's deadline.`,
	)
)

type Config struct {
	BlobSizeLimitError          dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn           dynamicconfig.IntPropertyFnWithNamespaceFilter
	BreakdownMetricsByTaskQueue dynamicconfig.TypedPropertyFnWithTaskQueueFilter[bool]
	Enabled                     dynamicconfig.BoolPropertyFnWithNamespaceFilter
	LongPollBuffer              dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollTimeout             dynamicconfig.DurationPropertyFnWithNamespaceFilter
	MaxIDLengthLimit            dynamicconfig.IntPropertyFn
	DefaultActivityRetryPolicy  dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		BlobSizeLimitError:          dynamicconfig.BlobSizeLimitError.Get(dc),
		BlobSizeLimitWarn:           dynamicconfig.BlobSizeLimitWarn.Get(dc),
		BreakdownMetricsByTaskQueue: dynamicconfig.MetricsBreakdownByTaskQueue.Get(dc),
		DefaultActivityRetryPolicy:  dynamicconfig.DefaultActivityRetryPolicy.Get(dc),
		Enabled:                     Enabled.Get(dc),
		LongPollBuffer:              LongPollBuffer.Get(dc),
		LongPollTimeout:             LongPollTimeout.Get(dc),
		MaxIDLengthLimit:            dynamicconfig.MaxIDLengthLimit.Get(dc),
	}
}
