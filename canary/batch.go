package canary

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/temporal"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.temporal.io/temporal/client"
	"go.temporal.io/temporal/workflow"
)

func registerBatch(r registrar) {
	registerWorkflow(r, batchWorkflow, wfTypeBatch)
	registerWorkflow(r, batchWorkflowParent, wfTypeBatchParent)
	registerWorkflow(r, batchWorkflowChild, wfTypeBatchChild)

	registerActivity(r, verifyBatchActivity, activityTypeVerifyBatch)
	registerActivity(r, startBatchWorkflow, activityTypeStartBatch)
}

const (
	// TODO: to get rid of them:
	//  after batch job has an API, we should use the API: https://github.com/uber/cadence/issues/2225
	sysBatchWFTypeName        = "temporal-sys-batch-workflow"
	systemBatcherTaskListName = "temporal-sys-batcher-tasklist"

	// there are two level, so totally 5*5 + 5 == 30 descendants
	// default batch RPS is 50, so it will takes ~1 seconds to terminate all
	numChildrenPerLevel = 5
)

type (
	// BatchParams is from server repo
	// TODO: to get rid of it:
	//  after batch job has an API, we should use the API: https://github.com/uber/cadence/issues/2225
	BatchParams struct {
		Namespace string
		Query     string
		Reason    string
		BatchType string
	}
)

func batchWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	profile, err := beginWorkflow(ctx, wfTypeBatch, scheduledTimeNanos)
	if err != nil {
		return err
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
			return profile.end(err)
		}
	}

	// waiting for visibility
	if err := workflow.Sleep(ctx, time.Second*2); err != nil {
		return profile.end(err)
	}

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    time.Second * 12,
		ExpirationInterval: time.Second * 3,
		MaximumAttempts:    4,
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList:               taskListName,
		ScheduleToStartTimeout: scheduleToStartTimeout,
		StartToCloseTimeout:    activityTaskTimeout,
		RetryPolicy:            retryPolicy,
	})

	startTime := workflow.Now(ctx).Format(time.RFC3339)
	err = workflow.ExecuteActivity(ctx, activityTypeStartBatch, namespace, startTime).Get(ctx, nil)
	if err != nil {
		return profile.end(err)
	}

	// waiting for visibility
	if err := workflow.Sleep(ctx, time.Second*2); err != nil {
		return profile.end(err)
	}

	err = workflow.ExecuteActivity(ctx, activityTypeVerifyBatch, namespace, startTime).Get(ctx, nil)

	return profile.end(err)
}

func batchWorkflowParent(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeBatchParent, scheduledTimeNanos)
	if err != nil {
		return err
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
			return profile.end(err)
		}
	}

	return profile.end(err)
}

func batchWorkflowChild(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeBatchChild, scheduledTimeNanos)
	if err != nil {
		return err
	}

	if err := workflow.Sleep(ctx, time.Hour); err != nil {
		return profile.end(err)
	}
	return profile.end(err)
}

func startBatchWorkflow(ctx context.Context, namespace, startTime string) error {
	sdkClient := getContextValue(ctx, ctxKeyActivitySystemClient).(*activityContext).cadence

	params := BatchParams{
		Namespace: namespace,
		Query:     "WorkflowType = '" + wfTypeBatchParent + "' AND CloseTime = missing AND StartTime <'" + startTime + "' ",
		Reason:    "batch canary",
		BatchType: "terminate",
	}

	options := client.StartWorkflowOptions{
		ExecutionStartToCloseTimeout:    childWorkflowTimeout,
		DecisionTaskStartToCloseTimeout: decisionTaskTimeout,
		TaskList:                        systemBatcherTaskListName,
		SearchAttributes: map[string]interface{}{
			"CustomNamespace": namespace,
			"Operator":        "admin",
		},
	}

	run, err := sdkClient.ExecuteWorkflow(ctx, options, sysBatchWFTypeName, params)
	if err != nil {
		return err
	}
	err = run.Get(ctx, nil)

	return err
}

func verifyBatchActivity(ctx context.Context, namespace, startTime string) error {
	svClient := getActivityContext(ctx).cadence.Service

	q1 := "WorkflowType = '" + wfTypeBatchParent + "' AND CloseTime = missing  AND StartTime <'" + startTime + "' "
	resp, err := svClient.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     q1,
	})
	if err != nil {
		return err
	}
	if resp.GetCount() > 0 {
		return fmt.Errorf("still seeing open workflows for %v , %v, %v", wfTypeBatchParent, q1, resp.GetCount())
	}

	q2 := "WorkflowType = '" + wfTypeBatchChild + "' AND CloseTime = missing  AND StartTime <'" + startTime + "' "
	resp, err = svClient.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     q2,
	})
	if err != nil {
		return err
	}
	if resp.GetCount() > 0 {
		return fmt.Errorf("still seeing open workflows for %v, %v, %v", wfTypeBatchChild, q2, resp.GetCount())
	}

	return nil
}
