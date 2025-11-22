package worker

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var (
	// InactiveWorkerCleanupDelay is the time to wait before cleaning up inactive workers after their lease expires.
	InactiveWorkerCleanupDelay = dynamicconfig.NewNamespaceDurationSetting(
		"worker.inactiveWorkerCleanupDelay",
		60*time.Minute,
		`InactiveWorkerCleanupDelay is the time to wait before cleaning up inactive workers after their lease expires.`,
	)
)

type Config struct {
	// Delay for cleaning up inactive workers after their lease has expired.
	InactiveWorkerCleanupDelay dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		InactiveWorkerCleanupDelay: InactiveWorkerCleanupDelay.Get(dc),
	}
}
