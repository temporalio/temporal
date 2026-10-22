package common

import (
	sdkworker "go.temporal.io/sdk/worker"
)

// sdkDefaultPollerCount is the number of pollers the SDK uses for each task type when no
// poller count or poller behavior is configured.
const sdkDefaultPollerCount = 2

// ApplyPollerAutoscaling replaces the fixed workflow and activity task poller counts in options
// with SDK poller autoscaling. Each fixed count (or the SDK default when unset) becomes the
// initial number of pollers, the minimum is 1, and the maximum is the initial number times
// maxMultiplier. Poller behaviors that are already set are left alone.
func ApplyPollerAutoscaling(options *sdkworker.Options, maxMultiplier int) {
	maxMultiplier = max(maxMultiplier, 1)
	if options.WorkflowTaskPollerBehavior == nil {
		options.WorkflowTaskPollerBehavior = autoscalingPollerBehavior(options.MaxConcurrentWorkflowTaskPollers, maxMultiplier)
		options.MaxConcurrentWorkflowTaskPollers = 0
	}
	if options.ActivityTaskPollerBehavior == nil {
		options.ActivityTaskPollerBehavior = autoscalingPollerBehavior(options.MaxConcurrentActivityTaskPollers, maxMultiplier)
		options.MaxConcurrentActivityTaskPollers = 0
	}
}

func autoscalingPollerBehavior(fixedCount, maxMultiplier int) sdkworker.PollerBehavior {
	initial := fixedCount
	if initial <= 0 {
		initial = sdkDefaultPollerCount
	}
	return sdkworker.NewPollerBehaviorAutoscaling(sdkworker.PollerBehaviorAutoscalingOptions{
		InitialNumberOfPollers: initial,
		MinimumNumberOfPollers: 1,
		MaximumNumberOfPollers: initial * maxMultiplier,
	})
}
