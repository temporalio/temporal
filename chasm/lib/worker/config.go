package worker

import (
	"go.temporal.io/server/common/dynamicconfig"
)

// InactiveWorkerCleanupDelay is the time to wait before cleaning up inactive workers after their lease expires.
var InactiveWorkerCleanupDelay = dynamicconfig.NewNamespaceDurationSetting(
	"worker.inactiveWorkerCleanupDelay",
	60*time.Minute,
	`InactiveWorkerCleanupDelay is the time to wait before cleaning up inactive workers after their lease expires.`,
)

type Config struct {
	// Future configs can be added here.
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{}
}
