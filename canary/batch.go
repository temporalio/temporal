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

	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
)

func init() {
	registerWorkflow(batchWorkflow, wfTypeBatch)
	registerWorkflow(batchWorkflowParent, wfTypeBatchParent)
	registerWorkflow(batchWorkflowChild, wfTypeBatchChild)

	registerActivity(verifyBatchActivity, activityTypeVerifyBatch)
	registerActivity(startBatchWorkflow, activityTypeStartBatch)
}

const (
	// TODO: to get rid of them:
	//  after batch job has an API, we should use the API: https://github.com/uber/cadence/issues/2225
	sysDomainName             = "cadence-system"
	sysBatchWFTypeName        = "cadence-sys-batch-workflow"
	systemBatcherTaskListName = "cadence-sys-batcher-tasklist"

	// there are two level, so totally 5*5 + 5 == 30 descendants
	// default batch RPS is 50, so it will takes ~1 seconds to terminate all
	numChildrenPerLevel = 5
)

type (
	// BatchParams is from server repo
	// TODO: to get rid of it:
	//  after batch job has an API, we should use the API: https://github.com/uber/cadence/issues/2225
	BatchParams struct {
		DomainName string
		Query      string
		Reason     string
		BatchType  string
	}
)

func batchWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeBatch, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	cwo := workflow.ChildWorkflowOptions{
		ExecutionStartToCloseTimeout: childWorkflowTimeout,
		TaskStartToCloseTimeout:      decisionTaskTimeout,
	}

	ctx = workflow.WithChildOptions(ctx, cwo)
	var fs []workflow.ChildWorkflowFuture
	for i := 0; i < numChildrenPerLevel; i++ {
		f := workflow.ExecuteChildWorkflow(ctx, wfTypeBatchParent, scheduledTimeNanos)
		fs = append(fs, f)
	}
	// waiting for all workflow started
	for i := 0; i < numChildrenPerLevel; i++ {
		err := fs[i].GetChildWorkflowExecution().Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	// waiting for visibility
	workflow.Sleep(ctx, time.Second*2)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList:               taskListName,
		ScheduleToStartTimeout: scheduleToStartTimeout,
		StartToCloseTimeout:    activityTaskTimeout,
	})

	startTime := workflow.Now(ctx).Format(time.RFC3339)
	err = workflow.ExecuteActivity(ctx, activityTypeStartBatch, domain, startTime).Get(ctx, nil)
	if err != nil {
		return err
	}

	// waiting for visibility
	workflow.Sleep(ctx, time.Second*2)

	err = workflow.ExecuteActivity(ctx, activityTypeVerifyBatch, domain, startTime).Get(ctx, nil)

	return profile.end(err)
}

func batchWorkflowParent(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeBatchParent, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	cwo := workflow.ChildWorkflowOptions{
		ExecutionStartToCloseTimeout: childWorkflowTimeout,
		TaskStartToCloseTimeout:      decisionTaskTimeout,
	}

	ctx = workflow.WithChildOptions(ctx, cwo)
	var fs []workflow.ChildWorkflowFuture
	for i := 0; i < numChildrenPerLevel; i++ {
		f := workflow.ExecuteChildWorkflow(ctx, wfTypeBatchChild, scheduledTimeNanos)
		fs = append(fs, f)
	}
	// waiting for all workflow to finish
	for i := 0; i < numChildrenPerLevel; i++ {
		err := fs[i].Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	return profile.end(err)
}

func batchWorkflowChild(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeBatchChild, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}
	workflow.Sleep(ctx, time.Hour)
	return profile.end(err)
}

func startBatchWorkflow(ctx context.Context, domain, startTime string) error {
	sdkClient := getContextValue(ctx, ctxKeyActivitySystemClient).(*activityContext).cadence

	params := BatchParams{
		DomainName: domain,
		Query:      "WorkflowType = '" + wfTypeBatchParent + "' AND CloseTime = missing AND StartTime <'" + startTime + "' ",
		Reason:     "batch canary",
		BatchType:  "terminate",
	}

	options := client.StartWorkflowOptions{
		ExecutionStartToCloseTimeout:    childWorkflowTimeout,
		DecisionTaskStartToCloseTimeout: decisionTaskTimeout,
		TaskList:                        systemBatcherTaskListName,
	}

	run, err := sdkClient.ExecuteWorkflow(ctx, options, sysBatchWFTypeName, params)
	if err != nil {
		return err
	}
	err = run.Get(ctx, nil)

	return err
}

func verifyBatchActivity(ctx context.Context, domain, startTime string) error {
	svClient := getActivityContext(ctx).cadence.Service

	q1 := "WorkflowType = '" + wfTypeBatchParent + "' AND CloseTime = missing  AND StartTime <'" + startTime + "' "
	resp, err := svClient.CountWorkflowExecutions(ctx, &shared.CountWorkflowExecutionsRequest{
		Domain: &domain,
		Query:  &q1,
	})
	if err != nil {
		return err
	}
	if resp.GetCount() > 0 {
		return fmt.Errorf("still seeing open workflows for %v , %v, %v", wfTypeBatchParent, q1, resp.GetCount())
	}

	q2 := "WorkflowType = '" + wfTypeBatchChild + "' AND CloseTime = missing  AND StartTime <'" + startTime + "' "
	resp, err = svClient.CountWorkflowExecutions(ctx, &shared.CountWorkflowExecutionsRequest{
		Domain: &domain,
		Query:  &q2,
	})
	if err != nil {
		return err
	}
	if resp.GetCount() > 0 {
		return fmt.Errorf("still seeing open workflows for %v, %v, %v", wfTypeBatchChild, q2, resp.GetCount())
	}

	return nil
}
