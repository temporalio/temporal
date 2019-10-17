// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package canary

import (
	"context"
	"fmt"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	queryType                     = "QueryWorkflow.QueryType"
	queryStatusStarted            = "QueryWorkflow.Status.Started"
	queryStatusCloseToFinished    = "QueryWorkflow.Status.CloseToFinished"
	queryStatusQueryHandlerFailed = "QueryWorkflow.Status.QueryHandlerFailed"
)

func init() {
	registerWorkflow(queryWorkflow, wfTypeQuery)
	registerActivity(queryActivity, activityTypeQuery1)
	registerActivity(queryActivity, activityTypeQuery2)
}

// queryWorkflow is the workflow implementation to test for querying workflow status
func queryWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
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
