package activity

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var (
	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"chasm.activity.longPollTimeout",
		20*time.Second,
		`LongPollTimeout is a timeout imposed on activity long-poll requests.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"chasm.activity.longPollBuffer",
		time.Second,
		`LongPollBuffer is the buffer time reserved before the caller's deadline to allow for response processing.`,
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
