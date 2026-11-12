package scheduleaudit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceRateLimiterUsesFixedShards(t *testing.T) {
	limiter := NewNamespaceRateLimiter(10)
	require.Len(t, limiter.shards, namespaceRateLimiterShards)
	first := limiter.limiterFor("namespace-a")
	require.Same(t, first, limiter.limiterFor("namespace-a"))

}
