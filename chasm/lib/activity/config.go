package activity

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var (
	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"chasm.activity.longPollTimeout",
		20*time.Second,
		`LongPollTimeout is a timeout for activity long-poll requests.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"chasm.activity.longPollBuffer",
		time.Second,
		`LongPollBuffer is used to adjust theactivity long-poll timeouts.
 Specifically, activity long-poll requests are timed out at a time which leaves at least LongPollBuffer
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
