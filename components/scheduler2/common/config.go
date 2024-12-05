package common

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

type (
	Tweakables struct {
		DefaultCatchupWindow              time.Duration // Default for catchup window
		MinCatchupWindow                  time.Duration // Minimum for catchup window
		MaxBufferSize                     int           // MaxBufferSize limits the number of buffered actions pending execution in total
		BackfillsPerIteration             int           // How many backfilled actions to buffer per iteration (implies rate limit since min sleep is 1s)
		CanceledTerminatedCountAsFailures bool          // Whether cancelled+terminated count for pause-on-failure

		// TODO - incomplete tweakables list
	}

	// V2 Scheduler dynamic config, shared among all sub state machines.
	Config struct {
		Tweakables       dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]
		ExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	}
)

var (
	CurrentTweakables = dynamicconfig.NewNamespaceTypedSetting(
		"component.scheduler.tweakables",
		DefaultTweakables,
		"A set of tweakable parameters for the V2 scheduler")

	ExecutionTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"component.scheduler.executionTimeout",
		time.Second*10,
		`ExecutionTimeout is the timeout for executing a single scheduler task.`,
	)

	DefaultTweakables = Tweakables{
		DefaultCatchupWindow:              365 * 24 * time.Hour,
		MinCatchupWindow:                  10 * time.Second,
		MaxBufferSize:                     1000,
		BackfillsPerIteration:             10,
		CanceledTerminatedCountAsFailures: false,
	}
)

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Tweakables:       CurrentTweakables.Get(dc),
		ExecutionTimeout: ExecutionTimeout.Get(dc),
	}
}
