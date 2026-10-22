package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	sdkworker "go.temporal.io/sdk/worker"
)

func TestApplyPollerAutoscaling(t *testing.T) {
	autoscaling := func(initial, maximum int) sdkworker.PollerBehavior {
		return sdkworker.NewPollerBehaviorAutoscaling(sdkworker.PollerBehaviorAutoscalingOptions{
			InitialNumberOfPollers: initial,
			MinimumNumberOfPollers: 1,
			MaximumNumberOfPollers: maximum,
		})
	}

	t.Run("fixed counts become initial pollers", func(t *testing.T) {
		options := sdkworker.Options{
			MaxConcurrentWorkflowTaskPollers: 10,
			MaxConcurrentActivityTaskPollers: 3,
		}
		ApplyPollerAutoscaling(&options, 4)
		require.Zero(t, options.MaxConcurrentWorkflowTaskPollers)
		require.Zero(t, options.MaxConcurrentActivityTaskPollers)
		require.Equal(t, autoscaling(10, 40), options.WorkflowTaskPollerBehavior)
		require.Equal(t, autoscaling(3, 12), options.ActivityTaskPollerBehavior)
	})

	t.Run("unset counts use the SDK default", func(t *testing.T) {
		var options sdkworker.Options
		ApplyPollerAutoscaling(&options, 4)
		require.Equal(t, autoscaling(2, 8), options.WorkflowTaskPollerBehavior)
		require.Equal(t, autoscaling(2, 8), options.ActivityTaskPollerBehavior)
	})

	t.Run("multiplier below 1 is treated as 1", func(t *testing.T) {
		options := sdkworker.Options{MaxConcurrentWorkflowTaskPollers: 5}
		ApplyPollerAutoscaling(&options, 0)
		require.Equal(t, autoscaling(5, 5), options.WorkflowTaskPollerBehavior)
	})

	t.Run("existing poller behaviors are kept", func(t *testing.T) {
		existing := sdkworker.NewPollerBehaviorSimpleMaximum(sdkworker.PollerBehaviorSimpleMaximumOptions{MaximumNumberOfPollers: 7})
		options := sdkworker.Options{
			WorkflowTaskPollerBehavior:       existing,
			MaxConcurrentActivityTaskPollers: 3,
		}
		ApplyPollerAutoscaling(&options, 4)
		require.Equal(t, existing, options.WorkflowTaskPollerBehavior)
		require.Equal(t, autoscaling(3, 12), options.ActivityTaskPollerBehavior)
	})
}
