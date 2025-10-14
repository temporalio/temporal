package scheduler

import (
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	Tweakables struct {
		DefaultCatchupWindow              time.Duration // Default for catchup window
		MinCatchupWindow                  time.Duration // Minimum for catchup window
		MaxBufferSize                     int           // MaxBufferSize limits the number of buffered actions pending execution in total
		CanceledTerminatedCountAsFailures bool          // Whether cancelled+terminated count for pause-on-failure
		RecentActionCount                 int           // Number of recent actions taken (workflow execution results) recorded in the ScheduleInfo metadata.
		MaxActionsPerExecution            int           // Limits the number of actions (startWorkflow, terminate/cancel) taken by ExecuteTask in a single iteration
		IdleTime                          time.Duration // How long to keep schedules after they're done
	}

	// Config is the CHASM Scheduler dynamic config, shared among all sub-components.
	Config struct {
		Tweakables         dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]
		ServiceCallTimeout dynamicconfig.DurationPropertyFn
		RetryPolicy        func() backoff.RetryPolicy
	}
)

var (
	CurrentTweakables = dynamicconfig.NewNamespaceTypedSetting(
		"chasm.scheduler.tweakables",
		DefaultTweakables,
		"A set of tweakable parameters for the CHASM scheduler.")

	RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
		"chasm.scheduler.retryPolicy.initialInterval",
		time.Second,
		`The initial backoff interval when retrying a failed task.`,
	)

	RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
		"chasm.scheduler.retryPolicy.maxInterval",
		time.Minute,
		`The maximum backoff interval when retrying a failed task.`,
	)

	ServiceCallTimeout = dynamicconfig.NewGlobalDurationSetting(
		"chasm.scheduler.serviceCallTimeout",
		2*time.Second,
		`The upper bound on how long a service call can take before being timed out.`,
	)

	DefaultTweakables = Tweakables{
		DefaultCatchupWindow:              365 * 24 * time.Hour,
		MinCatchupWindow:                  10 * time.Second,
		MaxBufferSize:                     1000,
		CanceledTerminatedCountAsFailures: false,
		RecentActionCount:                 10,
		MaxActionsPerExecution:            5,
		IdleTime:                          7 * 24 * time.Hour,
	}
)

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Tweakables:         CurrentTweakables.Get(dc),
		ServiceCallTimeout: ServiceCallTimeout.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
	}
}
