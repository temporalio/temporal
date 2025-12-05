package activity

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var (
	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"chasm.activity.longPollTimeout",
		20*time.Second,
		`Timeout for activity long-poll requests.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"chasm.activity.longPollBuffer",
		time.Second,
		`A buffer used to adjust the activity long-poll timeouts.
 Specifically, activity long-poll requests are timed out at a time which leaves at least the buffer's duration
 remaining before the caller's deadline, if permitted by the caller's deadline.`,
	)
)

type Config struct {
	LongPollTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollBuffer  dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		LongPollTimeout: LongPollTimeout.Get(dc),
		LongPollBuffer:  LongPollBuffer.Get(dc),
	}
}
