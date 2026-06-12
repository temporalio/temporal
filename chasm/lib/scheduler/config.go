package scheduler

import (
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	Tweakables struct {
		DefaultCatchupWindow              time.Duration // Default for catchup window
		MinCatchupWindow                  time.Duration // Minimum for catchup window
		MaxBufferSize                     int           // MaxBufferSize limits the number of buffered actions pending execution in total
		GeneratorBufferReserveSize        int           // Minimum number of spaces in `BufferedStarts` reserved for automated actions.
		CanceledTerminatedCountAsFailures bool          // Whether cancelled+terminated count for pause-on-failure
		MaxActionsPerExecution            int           // Limits the number of actions (startWorkflow, terminate/cancel) taken by ExecuteTask in a single iteration
		IdleTime                          time.Duration // How long to keep schedules after they're done
		EventLogMaxEntries                int           // Maximum EventLog entries retained per component; the earliest entries are dropped beyond this.
		EventLogMaxMessageLen             int           // Maximum byte length of an EventLog message; longer messages are truncated at a UTF-8 boundary.
	}

	// Config is the CHASM Scheduler dynamic config, shared among all sub-components.
	Config struct {
		Tweakables         dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]
		ServiceCallTimeout dynamicconfig.DurationPropertyFn
		RetryPolicy        func() backoff.RetryPolicy
	}
)

// tweakablesCtxKey keys the namespace-filtered Tweakables accessor that scheduler
// components read via the CHASM context (registered in Library.Components).
type tweakablesCtxKeyType struct{}

var tweakablesCtxKey = tweakablesCtxKeyType{}

// tweakablesFromContext returns the scheduler Tweakables for the context's namespace,
// falling back to DefaultTweakables when no config is registered.
func tweakablesFromContext(ctx chasm.Context) Tweakables {
	if fn, ok := ctx.Value(tweakablesCtxKey).(dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]); ok && fn != nil {
		return fn(ctx.NamespaceEntry().Name().String())
	}
	return DefaultTweakables
}

// contextValues builds the CHASM context values exposed to scheduler components.
func (c *Config) contextValues() map[any]any {
	var tweakables dynamicconfig.TypedPropertyFnWithNamespaceFilter[Tweakables]
	if c != nil {
		tweakables = c.Tweakables
	}
	return map[any]any{tweakablesCtxKey: tweakables}
}

var (
	CurrentTweakables = dynamicconfig.NewNamespaceTypedSetting(
		"scheduler.tweakables",
		DefaultTweakables,
		"A set of tweakable parameters for the CHASM scheduler.")

	RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
		"scheduler.retryPolicy.initialInterval",
		time.Second,
		`The initial backoff interval when retrying a failed task.`,
	)

	RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
		"scheduler.retryPolicy.maxInterval",
		time.Minute,
		`The maximum backoff interval when retrying a failed task.`,
	)

	ServiceCallTimeout = dynamicconfig.NewGlobalDurationSetting(
		"scheduler.serviceCallTimeout",
		2*time.Second,
		`The upper bound on how long a service call can take before being timed out.`,
	)

	// SentinelIdleTime is how long a CHASM sentinel reserves the schedule ID
	// before auto-closing via the idle task mechanism. Matches the dummy
	// workflow's duration.
	SentinelIdleTime = 15 * time.Minute

	DefaultTweakables = Tweakables{
		DefaultCatchupWindow:              365 * 24 * time.Hour,
		MinCatchupWindow:                  10 * time.Second,
		MaxBufferSize:                     1000,
		GeneratorBufferReserveSize:        50,
		CanceledTerminatedCountAsFailures: false,
		MaxActionsPerExecution:            5,
		IdleTime:                          7 * 24 * time.Hour,
		EventLogMaxEntries:                30,
		EventLogMaxMessageLen:             1000,
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
