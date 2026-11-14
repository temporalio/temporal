package replication

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
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
		{name: "EmptyRemoteCluster", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("", "ns-a", 10)}, expectedRates: map[string]float64{}},
		{name: "EmptyNamespaceID", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "", 10)}, expectInvalid: true},
		{name: "ZeroRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", 0)}, expectInvalid: true},
		{name: "NegativeRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", -1)}, expectInvalid: true},
		{name: "NaNRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", math.NaN())}, expectInvalid: true},
		{name: "InfiniteRate", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", math.Inf(1))}, expectInvalid: true},
		{name: "DuplicateEntry", entries: []dynamicconfig.ReplicationLaneIsolationEntry{entry("remote", "ns-a", 10), entry("remote", "ns-a", 5)}, expectInvalid: true},
		{
			name: "InvalidEntryForOtherClusterDoesNotWithdrawLanes",
			entries: []dynamicconfig.ReplicationLaneIsolationEntry{
				entry("remote", "ns-a", 10),
				entry("other", "ns-b", 0),
			},
			expected:      []DesiredLane{lane("ns-a")},
			expectedRates: map[string]float64{namespaceLaneKey("ns-a"): 10},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dc := dynamicconfig.NewMemoryClient()
			dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, tc.entries)
			factory := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), log.NewNoopLogger())
			streamPolicy, err := factory.NewStreamPolicy(LaneStream{RemoteCluster: "remote", ShardID: 1})
			require.NoError(t, err)
			policy := streamPolicy.(*manualLaneStreamPolicy)

			lanes, err := policy.Evaluate(LaneSignals{})
			require.Equal(t, tc.expected, lanes)
			if tc.expectInvalid {
				// The stream treats the error as an answer with no lanes.
				require.ErrorContains(t, err, "invalid ReplicationLaneIsolation")
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
	policy, err := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), log.NewNoopLogger()).
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

func TestManualLaneStreamPolicyInvalidOtherClusterKeepsLimiter(t *testing.T) {
	t.Parallel()
	initial := []dynamicconfig.ReplicationLaneIsolationEntry{
		{RemoteCluster: "remote", NamespaceID: "ns-a", TasksPerSecond: 1},
		{RemoteCluster: "other", NamespaceID: "ns-b", TasksPerSecond: 1},
	}
	for _, tc := range []struct {
		name    string
		invalid []dynamicconfig.ReplicationLaneIsolationEntry
	}{
		{"InvalidRate", []dynamicconfig.ReplicationLaneIsolationEntry{{RemoteCluster: "other", NamespaceID: "ns-b", TasksPerSecond: 0}}},
		{"InvalidNamespace", []dynamicconfig.ReplicationLaneIsolationEntry{{RemoteCluster: "other", TasksPerSecond: 1}}},
		{"DuplicateNamespace", []dynamicconfig.ReplicationLaneIsolationEntry{initial[1], initial[1]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dc := dynamicconfig.NewMemoryClient()
			dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, initial)
			factory := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), log.NewNoopLogger())
			remote, err := factory.NewStreamPolicy(LaneStream{RemoteCluster: "remote", ShardID: 1})
			require.NoError(t, err)
			other, err := factory.NewStreamPolicy(LaneStream{RemoteCluster: "other", ShardID: 1})
			require.NoError(t, err)
			expected, err := remote.Evaluate(LaneSignals{})
			require.NoError(t, err)
			_, err = other.Evaluate(LaneSignals{})
			require.NoError(t, err)
			remoteLimiter := remote.Limiter(namespaceLaneKey("ns-a"))
			otherLimiter := other.Limiter(namespaceLaneKey("ns-b"))
			now := time.Unix(1_000_000, 0)
			allowed, _ := remoteLimiter.Reserve(now)
			require.True(t, allowed)
			allowed, _ = otherLimiter.Reserve(now)
			require.True(t, allowed)

			dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, append([]dynamicconfig.ReplicationLaneIsolationEntry{initial[0]}, tc.invalid...))
			lanes, err := remote.Evaluate(LaneSignals{})
			require.NoError(t, err)
			require.Equal(t, expected, lanes)
			allowed, retry := remoteLimiter.Reserve(now)
			require.False(t, allowed, "an unrelated config error must not remove this namespace's cap")
			require.Equal(t, time.Second, retry)

			lanes, err = other.Evaluate(LaneSignals{})
			require.ErrorContains(t, err, "invalid ReplicationLaneIsolation")
			require.Empty(t, lanes)
			allowed, _ = otherLimiter.Reserve(now)
			require.True(t, allowed, "the affected stream must retain its existing fail-open behavior")

			dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, initial)
			_, err = other.Evaluate(LaneSignals{})
			require.NoError(t, err)
			allowed, retry = otherLimiter.Reserve(now)
			require.False(t, allowed)
			require.Equal(t, time.Second, retry)
		})
	}
}

func TestManualLaneStreamPolicyIgnoresUnaddressedEntries(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	logger := log.NewMockLogger(ctrl)
	logger.EXPECT().Warn("Ignoring ReplicationLaneIsolation entry without RemoteCluster", gomock.Any())
	dc := dynamicconfig.NewMemoryClient()
	valid := dynamicconfig.ReplicationLaneIsolationEntry{RemoteCluster: "remote", NamespaceID: "ns-a", TasksPerSecond: 1}
	dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, []dynamicconfig.ReplicationLaneIsolationEntry{valid})
	policy, err := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), logger).
		NewStreamPolicy(LaneStream{RemoteCluster: "remote", ShardID: 1})
	require.NoError(t, err)
	expected, err := policy.Evaluate(LaneSignals{})
	require.NoError(t, err)
	limiter := policy.Limiter(namespaceLaneKey("ns-a"))
	now := time.Unix(1_000_000, 0)
	allowed, _ := limiter.Reserve(now)
	require.True(t, allowed)

	dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, []dynamicconfig.ReplicationLaneIsolationEntry{valid, {NamespaceID: "ns-b", TasksPerSecond: 1}})
	lanes, err := policy.Evaluate(LaneSignals{})
	require.NoError(t, err)
	require.Equal(t, expected, lanes)
	allowed, retry := limiter.Reserve(now)
	require.False(t, allowed)
	require.Equal(t, time.Second, retry)
}
