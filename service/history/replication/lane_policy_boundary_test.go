package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestSenderLaneRegistryOwnsDesiredFilter(t *testing.T) {
	t.Parallel()
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	filter := namespaceLaneFilter("ns-a")
	policyState := []byte{1}
	lane, _, err := registry.Create("first", filter, 100, policyState)
	require.NoError(t, err)

	// A policy can reuse the slices from its previous answer.
	filter.NamespaceIDs[0] = "ns-b"
	policyState[0] = 2
	current, ok := registry.SnapshotByID(lane.id)
	require.True(t, ok)
	require.Equal(t, namespaceLaneFilter("ns-a"), current.filter)
	require.Equal(t, []byte{1}, current.policyState)
	require.True(t, current.scope.Contains(retirementTask("ns-a", 101)))
	require.False(t, current.scope.Contains(retirementTask("ns-b", 101)))
}

func (s *laneStreamSenderSuite) TestPolicyInputsCannotChangeLaneOwnership() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	first := DesiredLane{Key: "first", Filter: namespaceLaneFilter("ns-a"), PolicyState: []byte{1}}
	var observedPolicyState byte
	gomock.InOrder(
		policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{first}, nil),
		policy.EXPECT().Evaluate(gomock.Any()).DoAndReturn(func(signals LaneSignals) ([]DesiredLane, error) {
			s.Len(signals.Lanes, 1)
			signals.Lanes[0].Filter.NamespaceIDs[0] = "ns-b"
			signals.Lanes[0].PolicyState[0] = 2
			current, ok := registry.SnapshotByKey("first")
			s.True(ok)
			observedPolicyState = current.policyState[0]
			return []DesiredLane{first, {Key: "second", Filter: namespaceLaneFilter("ns-a")}}, nil
		}),
	)
	policy.EXPECT().Limiter(gomock.Any()).Return(&fakeLaneLimiter{}).AnyTimes()
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000)).Times(2)
	ack := syncReplicationState(100, 100, 100)
	ack.MaxLanes = 8

	s.NoError(s.streamSender.reconcileLanes(ack, false))
	s.NoError(s.streamSender.reconcileLanes(ack, false))
	s.Equal(byte(1), observedPolicyState, "mutating policy inputs must not change registry state")
	current, ok := registry.SnapshotByKey("first")
	s.True(ok)
	s.Equal(namespaceLaneFilter("ns-a"), current.filter)
	owners := 0
	for _, lane := range registry.Snapshots() {
		if lane.scope.Contains(retirementTask("ns-a", 101)) {
			owners++
		}
	}
	s.Equal(1, owners, "mutating policy inputs must not bypass overlap validation")
}

func (s *laneStreamSenderSuite) TestPolicyPanicWhileLanesReadRequestedState() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	_, _, err = registry.Create("a", namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	s.streamSender.laneLimiters["a"] = &fakeLaneLimiter{}
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{
		{Key: "a", Filter: namespaceLaneFilter("ns-a")},
		{Key: "b", Filter: namespaceLaneFilter("ns-b")},
		{Key: "c", Filter: namespaceLaneFilter("ns-c")},
		{Key: "d", Filter: namespaceLaneFilter("ns-d")},
	}, nil)
	ctx, cancel := context.WithCancel(s.T().Context())
	ready := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	started := false
	defer func() {
		cancel()
		if started {
			await.Rcv(s.T(), done)
		}
	}()
	policy.EXPECT().Limiter(gomock.Any()).DoAndReturn(func(string) LaneLimiter {
		started = true
		go func() {
			s.streamSender.laneShouldBeRateLimited("a")
			ready <- struct{}{}
			for {
				select {
				case <-ctx.Done():
					done <- struct{}{}
					return
				default:
					s.streamSender.laneShouldBeRateLimited("a")
				}
			}
		}()
		await.Rcv(s.T(), ready)
		panic("limiter")
	})
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	ack := syncReplicationState(100, 100, 100)
	ack.MaxLanes = 4

	s.NoError(s.streamSender.reconcileLanes(ack, false))
	s.True(s.streamSender.lanePolicyDisabled.Load())
	s.False(s.streamSender.laneShouldBeRateLimited("a"))
	s.Len(registry.Snapshots(), 1, "a policy panic must not publish new lanes")
}
