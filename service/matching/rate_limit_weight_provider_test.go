package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
)

func TestNewTaskQueueRateLimitWeightProvider_ClampedToMax(t *testing.T) {
	inner := TaskQueueRateLimitWeightProviderFunc(func(_ namespace.Name, _ string) float64 { return 2.0 })
	p := NewTaskQueueRateLimitWeightProvider(inner)
	require.InEpsilon(t, maxRateLimitWeight, p.GetRateLimitWeight("ns", "tq"), 0)
}

func TestNewTaskQueueRateLimitWeightProvider_ClampedToMin(t *testing.T) {
	inner := TaskQueueRateLimitWeightProviderFunc(func(_ namespace.Name, _ string) float64 { return -0.5 })
	p := NewTaskQueueRateLimitWeightProvider(inner)
	require.InDelta(t, minRateLimitWeight, p.GetRateLimitWeight("ns", "tq"), 0)
}

func TestNewTaskQueueRateLimitWeightProvider_PassthroughInRange(t *testing.T) {
	inner := TaskQueueRateLimitWeightProviderFunc(func(_ namespace.Name, _ string) float64 { return 0.5 })
	p := NewTaskQueueRateLimitWeightProvider(inner)
	require.InEpsilon(t, 0.5, p.GetRateLimitWeight("ns", "tq"), 0)
}
