package replication

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

func TestManualLaneStreamPolicy(t *testing.T) {
	entry := func(remoteCluster, namespaceID string, tasksPerSecond float64) dynamicconfig.ReplicationLaneIsolationEntry {
		return dynamicconfig.ReplicationLaneIsolationEntry{
			RemoteCluster:  remoteCluster,
			NamespaceID:    namespaceID,
			TasksPerSecond: tasksPerSecond,
		}
	}
	lane := func(namespaceID namespace.ID) DesiredLane {
		return DesiredLane{Key: namespaceLaneKey(namespaceID), Filter: LaneFilter{NamespaceIDs: []namespace.ID{namespaceID}}}
	}
	testCases := []struct {
		name          string
		entries       []dynamicconfig.ReplicationLaneIsolationEntry
		expected      []DesiredLane
		expectedRates map[string]float64
		expectInvalid bool
	}{
		{name: "NoEntries", expectedRates: map[string]float64{}},
		{
			name: "MatchingRemoteClusterOrderedByNamespaceID",
			entries: []dynamicconfig.ReplicationLaneIsolationEntry{
				entry("remote", "ns-b", 2.5),
				entry("remote", "ns-a", 10),
				entry("other", "ns-c", 10),
			},
			expected:      []DesiredLane{lane("ns-a"), lane("ns-b")},
			expectedRates: map[string]float64{namespaceLaneKey("ns-a"): 10, namespaceLaneKey("ns-b"): 2.5},
		},
		{name: "OnlyOtherRemoteCluster", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("other", "ns-a", 10)}, expectedRates: map[string]float64{}},
		{
			name:          "SameNamespaceOtherRemoteCluster",
			entries:       []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", 10), entry("other", "ns-a", 5)},
			expected:      []DesiredLane{lane("ns-a")},
			expectedRates: map[string]float64{namespaceLaneKey("ns-a"): 10},
		},
		{name: "EmptyRemoteCluster", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("", "ns-a", 10)}, expectInvalid: true},
		{name: "EmptyNamespaceID", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "", 10)}, expectInvalid: true},
		{name: "ZeroRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", 0)}, expectInvalid: true},
		{name: "NegativeRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", -1)}, expectInvalid: true},
		{name: "NaNRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", math.NaN())}, expectInvalid: true},
		{name: "InfiniteRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", math.Inf(1))}, expectInvalid: true},
		{name: "DuplicateEntry", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", 10), entry("remote", "ns-a", 5)}, expectInvalid: true},
		{
			name: "InvalidEntryForOtherClusterIsolatesNothing",
			entries: []dynamicconfig.ReplicationLaneIsolationEntry{
				entry("remote", "ns-a", 10),
				entry("other", "ns-b", 0),
			},
			expectInvalid: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dc := dynamicconfig.NewMemoryClient()
			dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, tc.entries)
			factory := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger()))
			streamPolicy, err := factory.NewStreamPolicy(LaneStream{RemoteCluster: "remote", ShardID: 1})
			require.NoError(t, err)
			policy := streamPolicy.(*manualLaneStreamPolicy)

			lanes, err := policy.Evaluate(LaneSignals{})
			require.Equal(t, tc.expected, lanes)
			if tc.expectInvalid {
				// Waiting does not fix the config, so the policy asks to retire the lanes at once.
				evaluateErr, ok := errors.AsType[*LaneEvaluateError](err)
				require.True(t, ok)
				require.Equal(t, LaneErrorRetireAll, evaluateErr.Action)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedRates, policy.rates)
		})
	}
}

func TestManualLaneStreamPolicy_LimiterFollowsConfig(t *testing.T) {
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, []dynamicconfig.ReplicationLaneIsolationEntry{
		{RemoteCluster: "remote", NamespaceID: "ns-a", TasksPerSecond: 10},
	})
	policy, err := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger())).
		NewStreamPolicy(LaneStream{RemoteCluster: "remote", ShardID: 1})
	require.NoError(t, err)
	_, err = policy.Evaluate(LaneSignals{})
	require.NoError(t, err)
	limiter := policy.Limiter(namespaceLaneKey("ns-a")).(*rateLaneLimiter)
	require.InDelta(t, 10, limiter.rate(), 0)

	// A namespace removed from the config has no rate, so its lane is not limited while it
	// catches up to retire.
	dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, []dynamicconfig.ReplicationLaneIsolationEntry{})
	_, err = policy.Evaluate(LaneSignals{})
	require.NoError(t, err)
	require.Zero(t, limiter.rate())
}

func TestRateLaneLimiter_TokenBucket(t *testing.T) {
	limiter := newRateLaneLimiter(func() float64 { return 10 })
	now := time.Unix(1_000_000, 0)

	// The bucket starts full, with a burst equal to the rate.
	for range 10 {
		ok, _ := limiter.Reserve(now)
		require.True(t, ok)
	}
	ok, retryAfter := limiter.Reserve(now)
	require.False(t, ok)
	require.Equal(t, 100*time.Millisecond, retryAfter)

	// A refusal does not take a token, so asking again before the delay is refused with the
	// remaining delay, and asking after it is allowed.
	ok, retryAfter = limiter.Reserve(now.Add(40 * time.Millisecond))
	require.False(t, ok)
	require.Equal(t, 60*time.Millisecond, retryAfter)
	ok, _ = limiter.Reserve(now.Add(100 * time.Millisecond))
	require.True(t, ok)
}

func TestRateLaneLimiter_RateChangeAppliesAtOnce(t *testing.T) {
	tasksPerSecond := 1.0
	limiter := newRateLaneLimiter(func() float64 { return tasksPerSecond })
	now := time.Unix(1_000_000, 0)

	ok, _ := limiter.Reserve(now)
	require.True(t, ok)
	ok, retryAfter := limiter.Reserve(now)
	require.False(t, ok)
	require.Equal(t, time.Second, retryAfter)

	// The next reservation uses the new rate. The empty bucket stays empty, so the rise does
	// not let a burst through.
	tasksPerSecond = 10
	ok, retryAfter = limiter.Reserve(now)
	require.False(t, ok)
	require.Equal(t, 100*time.Millisecond, retryAfter)
	ok, _ = limiter.Reserve(now.Add(100 * time.Millisecond))
	require.True(t, ok)

	// A drop applies at once too.
	tasksPerSecond = 0.5
	ok, retryAfter = limiter.Reserve(now.Add(100 * time.Millisecond))
	require.False(t, ok)
	require.Equal(t, 2*time.Second, retryAfter)
}

func TestRateLaneLimiter_FractionalRate(t *testing.T) {
	limiter := newRateLaneLimiter(func() float64 { return 0.2 })
	now := time.Unix(1_000_000, 0)

	ok, _ := limiter.Reserve(now)
	require.True(t, ok)
	ok, retryAfter := limiter.Reserve(now)
	require.False(t, ok)
	require.Equal(t, 5*time.Second, retryAfter)
}

func TestRateLaneLimiter_NoLimit(t *testing.T) {
	limiter := newRateLaneLimiter(func() float64 { return 0 })
	now := time.Unix(1_000_000, 0)

	for range 100 {
		ok, retryAfter := limiter.Reserve(now)
		require.True(t, ok)
		require.Zero(t, retryAfter)
	}
}
