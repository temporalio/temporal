package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
)

func TestNewTaskQueueRateLimitFractionProvider_ClampedToMax(t *testing.T) {
	inner := TaskQueueRateLimitFractionProviderFunc(func(_ namespace.Name, _ string, _ enumspb.TaskQueueType) float64 { return 2.0 })
	p := NewTaskQueueRateLimitFractionProvider(inner)
	require.InEpsilon(t, maxRateLimitFraction, p.GetRateLimitFraction("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW), 0)
}

func TestNewTaskQueueRateLimitFractionProvider_ClampedToMin(t *testing.T) {
	inner := TaskQueueRateLimitFractionProviderFunc(func(_ namespace.Name, _ string, _ enumspb.TaskQueueType) float64 { return -0.5 })
	p := NewTaskQueueRateLimitFractionProvider(inner)
	require.InDelta(t, minRateLimitFraction, p.GetRateLimitFraction("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW), 0)
}

func TestNewTaskQueueRateLimitFractionProvider_PassthroughInRange(t *testing.T) {
	inner := TaskQueueRateLimitFractionProviderFunc(func(_ namespace.Name, _ string, _ enumspb.TaskQueueType) float64 { return 0.5 })
	p := NewTaskQueueRateLimitFractionProvider(inner)
	require.InEpsilon(t, 0.5, p.GetRateLimitFraction("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW), 0)
}
