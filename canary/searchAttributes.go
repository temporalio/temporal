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
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	timeToWaitVisibilityDataOnESInDuration = 2 * time.Second
)

func init() {
	registerWorkflow(searchAttributesWorkflow, wfTypeSearchAttributes)
	registerActivity(searchAttributesActivity, activityTypeSearchAttributes)
}

// searchAttributesWorkflow tests the search attributes apis
func searchAttributesWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	var err error
	profile, err := beginWorkflow(ctx, wfTypeSearchAttributes, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	attr := map[string]interface{}{
		"CustomKeywordField": "canaryTest",
	}
	err = workflow.UpsertSearchAttributes(ctx, attr)
	if err != nil {
		workflow.GetLogger(ctx).Error("searchAttributes test failed", zap.Error(err))
		return profile.end(err)
	}

	execInfo := workflow.GetInfo(ctx).WorkflowExecution
	aCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
	workflow.Sleep(ctx, timeToWaitVisibilityDataOnESInDuration) // wait for visibility on ES
	now := workflow.Now(ctx).UnixNano()
	err = workflow.ExecuteActivity(aCtx, activityTypeSearchAttributes, now, execInfo).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("searchAttributes test failed", zap.Error(err))
		return profile.end(err)
	}

	return profile.end(err)
}

// searchAttributesActivity exercises the visibility apis
func searchAttributesActivity(ctx context.Context, scheduledTimeNanos int64, parentInfo workflow.Execution) error {
	var err error
	scope := activity.GetMetricsScope(ctx)
	scope, sw := recordActivityStart(scope, activityTypeSearchAttributes, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityContext(ctx).cadence
	if err := listWorkflow(client, parentInfo.ID, scope); err != nil {
		return err
	}
	if _, err := getMyHistory(client, parentInfo, scope); err != nil {
		return err
	}
	return err
}

func listWorkflow(client cadenceClient, wfID string, scope tally.Scope) error {
	pageSz := int32(1)
	startTime := time.Now().UnixNano() - int64(timeSkewToleranceDuration)
	endTime := time.Now().UnixNano() + int64(timeSkewToleranceDuration)
	queryStr := fmt.Sprintf("WorkflowID = '%s' and CustomKeywordField = '%s' and StartTime between %d and %d",
		wfID, "canaryTest", startTime, endTime)
	request := &shared.ListWorkflowExecutionsRequest{
		PageSize: &pageSz,
		Query:    &queryStr,
	}

	scope.Counter(listWorkflowsCount).Inc(1)
	sw := scope.Timer(listWorkflowsLatency).Start()
	resp, err := client.ListWorkflow(context.Background(), request)
	sw.Stop()
	if err != nil {
		scope.Counter(listWorkflowsFailureCount).Inc(1)
		return err
	}

	if len(resp.Executions) != 1 {
		scope.Counter(listWorkflowsFailureCount).Inc(1)
		err := fmt.Errorf("listWorkflow returned %d executions, expected=1", len(resp.Executions))
		return err
	}

	id := resp.Executions[0].Execution.GetWorkflowId()
	if id != wfID {
		scope.Counter(listWorkflowsFailureCount).Inc(1)
		err := fmt.Errorf("listWorkflow returned wrong workflow id %v", id)
		return err
	}

	return nil
}
