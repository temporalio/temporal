package callback

import (
	"time"

	"go.temporal.io/server/common/backoff"
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
)

var MaxPerExecution = dynamicconfig.NewNamespaceIntSetting(
	"callback.maxPerExecution",
	2000,
	`MaxPerExecution is the maximum number of callbacks that can be attached to an execution (workflow or standalone activity).`,
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"callback.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for executing a single callback request.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every callback request attempt for a given callback.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"callback.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every callback request attempt for a given callback.`,
)

type Config struct {
	RequestTimeout dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy    func() backoff.RetryPolicy
}

func configProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		RequestTimeout: RequestTimeout.Get(dc),
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

var EncodeInternalTokenWithEnvelope = dynamicconfig.NewNamespaceBoolSetting(
	"callback.encodeInternalTokenWithEnvelope",
	false,
	`Controls how the internal CHASM Nexus completion callback token is encoded. When true the token is
encoded as a NexusOperationCompletion envelope; when false (default) it is the legacy bare base64-encoded
ChasmComponentRef. Gates a safe fleet-wide rollout of the envelope encoding: keep disabled until every
server can read it (any server able to read the envelope also accepts the legacy form), then enable
per-namespace.`,
)

var AllowedAddresses = dynamicconfig.NewNamespaceTypedSettingWithConverter(
	"callback.allowedAddresses",
	commoncallbacks.AllowedAddressConverter,
	commoncallbacks.AddressMatchRules{},
	`The per-namespace list of addresses that are allowed for callbacks and whether secure connections (https) are required.
URLs: "temporal://system" and "temporal://internal" are always allowed. The default is no address rules.
URLs are checked against each in order when starting a workflow or activitiy with attached callbacks or a standalone
callback and only need to match one to pass validation.  This configuration is required for external endpoint targets;
any invalid entries are ignored. Each entry is a map with possible values:
     - "Pattern":string (required) the host:port pattern to which this config applies.
        Wildcards, '*', are supported and can match any number of characters (e.g. '*' matches everything,
        'prefix.*.domain' matches 'prefix.a.domain' as well as 'prefix.a.b.domain').
     - "AllowInsecure":bool (optional, default=false) indicates whether https is required`)
