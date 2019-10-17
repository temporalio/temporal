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
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	timeSkewToleranceDuration = 5 * time.Minute
)

func init() {
	registerWorkflow(visibilityWorkflow, wfTypeVisibility)
	registerActivity(visibilityActivity, activityTypeVisibility)
}

// visibilityWorkflow tests the visibility apis
func visibilityWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	var err error
	profile, err := beginWorkflow(ctx, wfTypeVisibility, scheduledTimeNanos)
	if err != nil {
		return err
	}

	execInfo := workflow.GetInfo(ctx).WorkflowExecution
	aCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
	// wait for visibility on ES {}
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return profile.end(err)
	}
	now := workflow.Now(ctx).UnixNano()
	err = workflow.ExecuteActivity(aCtx, activityTypeVisibility, now, execInfo).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("visibility test failed", zap.Error(err))
		return profile.end(err)
	}

	return profile.end(nil)
}

// visibilityActivity exercises the visibility apis
func visibilityActivity(ctx context.Context, scheduledTimeNanos int64, parentInfo workflow.Execution) error {
	var err error
	scope := activity.GetMetricsScope(ctx)
	scope, sw := recordActivityStart(scope, activityTypeVisibility, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityContext(ctx).cadence
	if err := listMyWorkflow(client, parentInfo.ID, scope); err != nil {
		return err
	}
	if _, err := getMyHistory(client, parentInfo, scope); err != nil {
		return err
	}
	return err
}

func listMyWorkflow(client cadenceClient, wfID string, scope tally.Scope) error {
	pageSz := int32(1)
	startTime := time.Now().UnixNano() - int64(timeSkewToleranceDuration)
	endTime := time.Now().UnixNano() + int64(timeSkewToleranceDuration)
	request := &shared.ListOpenWorkflowExecutionsRequest{
		MaximumPageSize: &pageSz,
		ExecutionFilter: &shared.WorkflowExecutionFilter{WorkflowId: &wfID},
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: &startTime,
			LatestTime:   &endTime,
		},
	}

	scope.Counter(listOpenWorkflowsCount).Inc(1)
	sw := scope.Timer(listOpenWorkflowsLatency).Start()
	resp, err := client.ListOpenWorkflow(context.Background(), request)
	sw.Stop()
	if err != nil {
		scope.Counter(listOpenWorkflowsFailureCount).Inc(1)
		return err
	}

	if len(resp.Executions) != 1 {
		scope.Counter(listOpenWorkflowsFailureCount).Inc(1)
		err := fmt.Errorf("listOpenWorkflow returned %d executions, expected=1", len(resp.Executions))
		return err
	}

	id := resp.Executions[0].Execution.GetWorkflowId()
	if id != wfID {
		scope.Counter(listOpenWorkflowsFailureCount).Inc(1)
		err := fmt.Errorf("listOpenWorkflow returned wrong workflow id %v", id)
		return err
	}

	return nil
}

func getMyHistory(client cadenceClient, execInfo workflow.Execution, scope tally.Scope) ([]*shared.HistoryEvent, error) {
	scope.Counter(getWorkflowHistoryCount).Inc(1)
	sw := scope.Timer(getWorkflowHistoryLatency).Start()
	defer sw.Stop()

	events := []*shared.HistoryEvent{}
	iter := client.GetWorkflowHistory(context.Background(), execInfo.ID, execInfo.RunID, false, shared.HistoryEventFilterTypeAllEvent)

	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			scope.Counter(getWorkflowHistoryFailureCount).Inc(1)
			return nil, err
		}
		events = append(events, event)
	}

	if len(events) == 0 {
		return nil, errors.New("getWorkflowHistory returned history with 0 events")
	}
	return events, nil
}
