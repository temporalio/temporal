package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchingClientReadLoadBalancerMode(t *testing.T) {
	for _, mode := range []MatchingReadLoadBalancerMode{
		MatchingReadLoadBalancerModeFewestPollers,
		MatchingReadLoadBalancerModeWeightedFewest,
		MatchingReadLoadBalancerModeBacklogWeighted,
	} {
		require.NoError(t, MatchingClientReadLoadBalancerMode.Validate(mode))
	}
	require.Error(t, MatchingClientReadLoadBalancerMode.Validate("unknown"))
}
