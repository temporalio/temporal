package worker

import (
	"go.temporal.io/server/common/dynamicconfig"
)

type Config struct {
	// Delay for cleaning up inactive workers after their lease has expired.
	InactiveWorkerCleanupDelay dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		InactiveWorkerCleanupDelay: dynamicconfig.InactiveWorkerCleanupDelay.Get(dc),
	}
}
