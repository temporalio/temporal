package canary

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

const (
	activityDelay                           = 4 * time.Second
	activityStartToCloseTimeoutThreshold    = 1 * time.Second
	activityScheduleToStartTimeoutThreshold = 1 * time.Second
	activityScheduleToCloseTimeoutThreshold = activityStartToCloseTimeoutThreshold + activityScheduleToStartTimeoutThreshold
)

func registerTimeout(r registrar) {
	registerWorkflow(r, timeoutWorkflow, wfTypeTimeout)
	registerActivity(r, timeoutActivity, activityTypeTimeout)
}

// timeoutWorkflow is the workflow implementation to test for querying workflow status
func timeoutWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	profile, err := beginWorkflow(ctx, wfTypeConcurrentExec, scheduledTimeNanos)
	if err != nil {
		return err
	}

	now := workflow.Now(ctx).UnixNano()
	activityOptions := newActivityOptions()
	activityOptions.StartToCloseTimeout = activityStartToCloseTimeoutThreshold
	activityOptions.ScheduleToStartTimeout = activityScheduleToStartTimeoutThreshold
	activityOptions.ScheduleToCloseTimeout = activityScheduleToCloseTimeoutThreshold
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)
	activityFuture := workflow.ExecuteActivity(activityCtx, timeoutActivity, now)

	activityErr := activityFuture.Get(ctx, nil)
	if activityErr != nil {
		if _, ok := activityErr.(*workflow.TimeoutError); !ok {
			workflow.GetLogger(ctx).Info("activity timeout failed", zap.Error(activityErr))
		} else {
			activityErr = nil
		}
	} else {
		activityErr = fmt.Errorf("activity timeout error expected, but no error received")
		workflow.GetLogger(ctx).Info("activity timeout failed", zap.Error(activityErr))
	}

	return profile.end(activityErr)
}

// timeoutActivity is the activity implementation for timeout test
func timeoutActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeConcurrentExec, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	timer := time.NewTimer(activityDelay)
	select {
	case <-timer.C:
	}
	timer.Stop()

	return nil
}
