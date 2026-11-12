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

var WorkerGroupRetention = dynamicconfig.NewNamespaceDurationSetting(
	"worker.groupRetention",
	0,
	`How long closed worker groups are kept. Non-positive values use namespace retention. Namespace minimum retention still applies.`,
)

type Config struct {
	MinHeartbeatTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	GroupRetention      dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		MinHeartbeatTimeout: MinWorkerHeartbeatTimeout.Get(dc),
		GroupRetention:      WorkerGroupRetention.Get(dc),
	}
}
