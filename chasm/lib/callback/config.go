package callback

import (
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
)

// Dynamic config settings
var (
	AllowedAddresses = dynamicconfig.NewNamespaceTypedSettingWithConverter(
		"callback.allowedAddresses",
		allowedAddressConverter,
		AddressMatchRules{},
		`The per-namespace list of addresses that are allowed for callbacks and whether secure connections (https) are required.
URL: "temporal://system" is always allowed for worker callbacks. The default is no address rules.
URLs are checked against each in order when starting a workflow with attached callbacks and only need to match one to pass validation.
This configuration is required for external endpoint targets; any invalid entries are ignored. Each entry is a map with possible values:
     - "Pattern":string (required) the host:port pattern to which this config applies.
        Wildcards, '*', are supported and can match any number of characters (e.g. '*' matches everything, 'prefix.*.domain' matches 'prefix.a.domain' as well as 'prefix.a.b.domain').
     - "AllowInsecure":bool (optional, default=false) indicates whether https is required`)

	EnableStandaloneExecutions = dynamicconfig.NewNamespaceBoolSetting(
		"callback.enableStandaloneExecutions",
		true,
		`Toggles standalone callback execution functionality on the server.`,
	)

	LongPollBuffer = dynamicconfig.NewNamespaceDurationSetting(
		"callback.longPollBuffer",
		time.Second,
		`A buffer used to adjust the callback execution long-poll timeouts.
The long-poll response is sent before the caller's deadline by this amount of time.`,
	)

	LongPollTimeout = dynamicconfig.NewNamespaceDurationSetting(
		"callback.longPollTimeout",
		20*time.Second,
		`Timeout for callback execution long-poll requests.`,
	)

	MaxPerExecution = dynamicconfig.NewNamespaceIntSetting(
		"callback.maxPerExecution",
		2000,
		`MaxPerExecution is the maximum number of callbacks that can be attached to an execution (workflow or standalone activity).`,
	)

	RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
		"callback.request.timeout",
		time.Second*10,
		`RequestTimeout is the timeout for executing a single callback request.`,
	)

	// TODO(chrsmith): Adress the PR feedback at https://github.com/temporalio/temporal/pull/9805/changes#r3105952880
	// > We sould reuse the DC in the nexusoperation package.
	// > But we could also unify with the common struct for retry policies in common/retrypolicy.
	RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
		"callback.retryPolicy.initialInterval",
		time.Second,
		`The initial backoff interval between every callback request attempt for a given callback.`,
	)

	RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
		"callback.retryPolicy.maxInterval",
		time.Hour,
		`The maximum backoff interval between every callback request attempt for a given callback.`,
	)
)

type Config struct {
	// callback.* config values.
	AllowedAddresses           dynamicconfig.TypedPropertyFnWithNamespaceFilter[AddressMatchRules]
	EnableStandaloneExecutions dynamicconfig.BoolPropertyFnWithNamespaceFilter
	LongPollBuffer             dynamicconfig.DurationPropertyFnWithNamespaceFilter
	LongPollTimeout            dynamicconfig.DurationPropertyFnWithNamespaceFilter
	RequestTimeout             dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy                func() backoff.RetryPolicy

	// TODO: MaxPerExecution is missing. It is used as part of callback.Validator, and is loaded there.
	// Once HSM callbacks (components/callbacks) are removed, the callbackValidatorProvider in
	// frontend/fx.go can be moved into this package. And at that time, we can simply have the
	// callback.Validator inject callback.Config. (And have a single location for all config options.)
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	getRetryPolicyFn := func() backoff.RetryPolicy {
		return backoff.
			NewExponentialRetryPolicy(RetryPolicyInitialInterval.Get(dc)()).
			WithMaximumInterval(RetryPolicyMaximumInterval.Get(dc)()).
			WithExpirationInterval(backoff.NoInterval)
	}

	return &Config{
		EnableStandaloneExecutions: EnableStandaloneExecutions.Get(dc),
		LongPollBuffer:             LongPollBuffer.Get(dc),
		LongPollTimeout:            LongPollTimeout.Get(dc),
		RequestTimeout:             RequestTimeout.Get(dc),
		RetryPolicy:                getRetryPolicyFn,
	}
}
