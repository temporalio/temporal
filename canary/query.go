package canary

import (
	"context"
	"fmt"

	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

const (
	queryType                     = "QueryWorkflow.QueryType"
	queryStatusStarted            = "QueryWorkflow.Status.Started"
	queryStatusCloseToFinished    = "QueryWorkflow.Status.CloseToFinished"
	queryStatusQueryHandlerFailed = "QueryWorkflow.Status.QueryHandlerFailed"
)

func registerQuery(r registrar) {
	registerWorkflow(r, queryWorkflow, wfTypeQuery)
	registerActivity(r, queryActivity, activityTypeQuery1)
	registerActivity(r, queryActivity, activityTypeQuery2)
}

// queryWorkflow is the workflow implementation to test for querying workflow status
func queryWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	status := queryStatusStarted
	err := workflow.SetQueryHandler(ctx, queryType, func() (string, error) {
		return status, nil
	})
	if err != nil {
		status = queryStatusQueryHandlerFailed
		return err
	}

	profile, err := beginWorkflow(ctx, wfTypeQuery, scheduledTimeNanos)
	if err != nil {
		return err
	}

	execution := workflow.GetInfo(ctx).WorkflowExecution

	var queryStatus string

	now := workflow.Now(ctx).UnixNano()
	activityCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
	err = workflow.ExecuteActivity(activityCtx, activityTypeQuery1, now, execution).Get(activityCtx, &queryStatus)
	if err != nil {
		workflow.GetLogger(ctx).Info("queryWorkflow failed", zap.Error(err))
		return profile.end(err)
	}
	if queryStatus != status {
		err := fmt.Errorf("queryWorkflow status expected %s, but is %s", status, queryStatus)
		workflow.GetLogger(ctx).Info("queryWorkflow failed, status is not expected", zap.Error(err))
		return profile.end(err)
	}

	status = queryStatusCloseToFinished

	activityCtx = workflow.WithActivityOptions(ctx, newActivityOptions())
	err = workflow.ExecuteActivity(activityCtx, activityTypeQuery2, now, execution).Get(activityCtx, &queryStatus)
	if err != nil {
		workflow.GetLogger(ctx).Info("queryWorkflow failed", zap.Error(err))
		return profile.end(err)
	}
	if queryStatus != status {
		err := fmt.Errorf("queryWorkflow status expected %s, but is %s", status, queryStatus)
		workflow.GetLogger(ctx).Info("queryWorkflow failed, status is not expected", zap.Error(err))
		return profile.end(err)
	}

	return profile.end(nil)
}

// queryActivity is the activity implementation for querying workflow status
func queryActivity(ctx context.Context, scheduledTimeNanos int64, execution workflow.Execution) (string, error) {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeConcurrentExec, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityContext(ctx).cadence
	encodedValue, err := client.QueryWorkflow(context.Background(), execution.ID, execution.RunID, queryType)
	if err != nil {
		return "", err
	}

	var result string
	err = encodedValue.Get(&result)

	return result, err
}
