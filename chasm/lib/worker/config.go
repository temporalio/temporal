package worker

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var EnableDurableWorkerHeartbeats = dynamicconfig.NewNamespaceBoolSetting(
	"worker.enableDurableWorkerHeartbeats",
	false,
	`Enable forwarding worker heartbeats from Frontend to the worker state machine.`,
)

var MinWorkerHeartbeatTimeout = dynamicconfig.NewNamespaceDurationSetting(
	"worker.minHeartbeatTimeout",
	// Leave slack beyond the SDK's default one-minute heartbeat interval.
	2*time.Minute,
	`Minimum allowed worker heartbeat timeout.`,
)

type Config struct {
	MinHeartbeatTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		MinHeartbeatTimeout: MinWorkerHeartbeatTimeout.Get(dc),
	}
}
