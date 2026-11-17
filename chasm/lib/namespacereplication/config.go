package namespacereplication

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

const (
	defaultLocalRetryInitialInterval    = time.Second
	defaultLocalRetryMaximumInterval    = 5 * time.Minute
	defaultLocalRetryExpirationInterval = 5 * time.Minute
	defaultPeerRetryInitialInterval     = time.Second
	defaultPeerRetryMaximumInterval     = 5 * time.Minute
	defaultPeerRetryExpirationInterval  = 7 * 24 * time.Hour
)

var (
	LocalRetryExpirationInterval = dynamicconfig.NewGlobalDurationSetting(
		"namespaceReplication.chasm.localRetry.expirationInterval",
		defaultLocalRetryExpirationInterval,
		`The total time to retry an unresolved local namespace mutation before recording an indeterminate terminal outcome.`,
	)
	PeerRetryInitialInterval = dynamicconfig.NewGlobalDurationSetting(
		"namespaceReplication.chasm.peerRetry.initialInterval",
		defaultPeerRetryInitialInterval,
		`The initial backoff interval between attempts to apply a namespace mutation to a peer cluster.`,
	)
	PeerRetryMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
		"namespaceReplication.chasm.peerRetry.maxInterval",
		defaultPeerRetryMaximumInterval,
		`The maximum backoff interval between attempts to apply a namespace mutation to a peer cluster.`,
	)
	PeerRetryExpirationInterval = dynamicconfig.NewGlobalDurationSetting(
		"namespaceReplication.chasm.peerRetry.expirationInterval",
		defaultPeerRetryExpirationInterval,
		`The total time to retry applying a namespace mutation to a peer cluster before recording a terminal failure.`,
	)
)

type config struct {
	localRetryExpirationInterval dynamicconfig.DurationPropertyFn
	peerRetryInitialInterval     dynamicconfig.DurationPropertyFn
	peerRetryMaximumInterval     dynamicconfig.DurationPropertyFn
	peerRetryExpirationInterval  dynamicconfig.DurationPropertyFn
}

func configProvider(dc *dynamicconfig.Collection) *config {
	return &config{
		localRetryExpirationInterval: LocalRetryExpirationInterval.Get(dc),
		peerRetryInitialInterval:     PeerRetryInitialInterval.Get(dc),
		peerRetryMaximumInterval:     PeerRetryMaximumInterval.Get(dc),
		peerRetryExpirationInterval:  PeerRetryExpirationInterval.Get(dc),
	}
}

func (c *config) localRetryExpiration() time.Duration {
	if c == nil || c.localRetryExpirationInterval == nil {
		return defaultLocalRetryExpirationInterval
	}
	if interval := c.localRetryExpirationInterval(); interval > 0 {
		return interval
	}
	return defaultLocalRetryExpirationInterval
}

type peerRetryPolicy struct {
	initialInterval    time.Duration
	maximumInterval    time.Duration
	expirationInterval time.Duration
}

func defaultPeerRetryPolicy() peerRetryPolicy {
	return peerRetryPolicy{
		initialInterval:    defaultPeerRetryInitialInterval,
		maximumInterval:    defaultPeerRetryMaximumInterval,
		expirationInterval: defaultPeerRetryExpirationInterval,
	}
}

func (c *config) peerRetryPolicy() peerRetryPolicy {
	policy := defaultPeerRetryPolicy()
	if c == nil {
		return policy
	}
	if interval := c.peerRetryInitialInterval(); interval > 0 {
		policy.initialInterval = interval
	}
	if interval := c.peerRetryMaximumInterval(); interval > 0 {
		policy.maximumInterval = interval
	}
	if interval := c.peerRetryExpirationInterval(); interval > 0 {
		policy.expirationInterval = interval
	}
	policy.maximumInterval = max(policy.initialInterval, policy.maximumInterval)
	return policy
}
