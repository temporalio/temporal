package canary

import (
	"context"

	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

const (
	// to force cuncurrent execution of activity and pagination on history events
	// we need to make this number larger than the default page size, which is 1000
	numConcurrentExec   = 25
	totalConcurrentExec = 250
)

func registerConcurrentExec(r registrar) {
	registerWorkflow(r, concurrentExecWorkflow, wfTypeConcurrentExec)
	registerActivity(r, concurrentExecActivity, activityTypeConcurrentExec)
}

// concurrentExecWorkflow is the workflow implementation to test
// 1. client side events pagination when reconstructing workflow state
// 2. concurrent execution of activities
func concurrentExecWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	profile, err := beginWorkflow(ctx, wfTypeConcurrentExec, scheduledTimeNanos)
	if err != nil {
		return err
	}

	selector := workflow.NewSelector(ctx)
	errors := make([]error, totalConcurrentExec)

	doActivity := func(index int) {
		now := workflow.Now(ctx).UnixNano()
		activityCtx := workflow.WithActivityOptions(ctx, newActivityOptions())

		future := workflow.ExecuteActivity(activityCtx, activityTypeConcurrentExec, now)
		selector.AddFuture(future, func(f workflow.Future) {
			// do not care about the return value
			errors[index] = f.Get(activityCtx, nil)
		})
	}

	for index := 0; index < numConcurrentExec; index++ {
		doActivity(index)
	}

	for index := numConcurrentExec; index < totalConcurrentExec; index++ {
		selector.Select(ctx)
		doActivity(index)
	}

	for index := 0; index < numConcurrentExec; index++ {
		selector.Select(ctx)
	}

	for _, err := range errors {
		if err != nil {
			workflow.GetLogger(ctx).Info("concurrentExecActivity failed", zap.Error(err))
			return profile.end(err)
		}
	}

	return profile.end(nil)
}

// concurrentExecActivity is the activity implementation for concurrent execution test
func concurrentExecActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeConcurrentExec, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	// canary test do not require any actual work to be done, so just return
	return nil
}
