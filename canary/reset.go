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

	"go.temporal.io/temporal"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
)

const (
	smallWait         = 1
	bigWait           = 30
	signalToTrigger   = "signalToTrigger"
	signalBeforeReset = "signalBeforeReset"
	signalAfterReset  = "signalAfterReset"
)

func registerReset(r registrar) {
	registerWorkflow(r, resetWorkflow, wfTypeReset)
	registerWorkflow(r, resetBaseWorkflow, wfTypeResetBase)

	registerActivity(r, triggerResetActivity, activityTypeTriggerReset)
	registerActivity(r, verifyResetActivity, activityTypeVerifyReset)
	registerActivity(r, resetBaseActivity, activityTypeResetBase)
}

func resetWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeReset, scheduledTimeNanos)
	if err != nil {
		return err
	}
	info := workflow.GetInfo(ctx)

	cwo := newChildWorkflowOptions(domain, wfTypeResetBase+"-"+info.WorkflowExecution.RunID)
	baseCtx := workflow.WithChildOptions(ctx, cwo)
	baseFuture := workflow.ExecuteChildWorkflow(baseCtx, wfTypeResetBase, workflow.Now(ctx).UnixNano(), info.WorkflowExecution.ID, info.WorkflowExecution.RunID, domain)
	var baseWE workflow.Execution
	if err := baseFuture.GetChildWorkflowExecution().Get(baseCtx, &baseWE); err != nil {
		return profile.end(err)
	}

	signalFuture1 := baseFuture.SignalChildWorkflow(baseCtx, signalBeforeReset, "signalValue")
	err = signalFuture1.Get(baseCtx, nil)
	if err != nil {
		return profile.end(err)
	}

	// use signal to wait for baseWF to get reach reset point
	var value string
	signalCh := workflow.GetSignalChannel(ctx, signalToTrigger)
	signalCh.Receive(ctx, &value)

	signalFuture2 := baseFuture.SignalChildWorkflow(baseCtx, signalAfterReset, "signalValue")
	err = signalFuture2.Get(baseCtx, nil)
	if err != nil {
		return profile.end(err)
	}

	expiration := time.Duration(info.ExecutionStartToCloseTimeoutSeconds) * time.Second
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList:               taskListName,
		ScheduleToStartTimeout: expiration,
		StartToCloseTimeout:    expiration,
	})
	var newWE workflow.Execution
	err = workflow.ExecuteActivity(activityCtx, activityTypeTriggerReset, domain, baseWE).Get(activityCtx, &newWE)
	if err != nil {
		return profile.end(err)
	}
	if err := workflow.Sleep(ctx, time.Duration(bigWait*2)*time.Second); err != nil {
		return profile.end(err)
	}
	err = workflow.ExecuteActivity(activityCtx, activityTypeVerifyReset, domain, newWE).Get(activityCtx, nil)

	return profile.end(err)
}

func resetBaseWorkflow(ctx workflow.Context, scheduledTimeNanos int64, parentID, parentRunID, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeResetBase, scheduledTimeNanos)
	if err != nil {
		return err
	}

	info := workflow.GetInfo(ctx)
	expiration := time.Duration(info.ExecutionStartToCloseTimeoutSeconds) * time.Second
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second * 5,
		BackoffCoefficient: 1,
		MaximumInterval:    time.Second * 5,
		ExpirationInterval: expiration,
		MaximumAttempts:    5,
	}

	activityCtxWithRetry := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList:               taskListName,
		ScheduleToStartTimeout: expiration,
		StartToCloseTimeout:    expiration,
		RetryPolicy:            retryPolicy,
	})
	activityCtxNoRetry := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskList:               taskListName,
		ScheduleToStartTimeout: expiration,
		StartToCloseTimeout:    expiration,
	})

	var f workflow.Future
	futures1 := []workflow.Future{}
	futures2 := []workflow.Future{}
	// completed before reset
	f = workflow.ExecuteActivity(activityCtxWithRetry, activityTypeResetBase, smallWait)
	futures1 = append(futures1, f)

	// started before reset
	f = workflow.ExecuteActivity(activityCtxNoRetry, activityTypeResetBase, bigWait)
	futures2 = append(futures2, f)

	// not started before reset(because it uses retry)
	f = workflow.ExecuteActivity(activityCtxWithRetry, activityTypeResetBase, bigWait)
	futures2 = append(futures2, f)

	// fired before reset
	f = workflow.NewTimer(ctx, time.Duration(smallWait*2)*time.Second)
	futures1 = append(futures1, f)

	// fired after reset
	f = workflow.NewTimer(ctx, time.Duration(bigWait)*time.Second)
	futures2 = append(futures2, f)

	// wait until first set of futures1 are done: 1 act, 1 timer
	for _, future := range futures1 {
		if err := future.Get(ctx, nil); err != nil {
			return profile.end(err)
		}
	}

	signalFuture := workflow.SignalExternalWorkflow(ctx, parentID, parentRunID, signalToTrigger, "signalValue")
	err = signalFuture.Get(ctx, nil)
	if err != nil {
		return profile.end(err)
	}

	var value string
	signalCh := workflow.GetSignalChannel(ctx, signalBeforeReset)
	signalCh.Receive(ctx, &value)

	signalCh = workflow.GetSignalChannel(ctx, signalAfterReset)
	signalCh.Receive(ctx, &value)

	return profile.end(err)
}

func triggerResetActivity(ctx context.Context, domain string, baseWE workflow.Execution) (workflow.Execution, error) {
	svClient := getActivityContext(ctx).cadence.Service

	reason := "reset canary"

	client := getActivityContext(ctx).cadence
	scope := activity.GetMetricsScope(ctx)
	resetEventID := int64(0)
	seenTrigger := false

	// reset to last decisionCompleted before signalToTrigger was sent
	// Since we are in the trigger activity, baseWF must have reached there

	events, err := getMyHistory(client, baseWE, scope)
	if err != nil {
		return workflow.Execution{}, err
	}
	for _, event := range events {
		if event.GetEventType() == enums.EventTypeDecisionTaskCompleted {
			resetEventID = event.GetEventId()
		}
		if event.GetEventType() == enums.EventTypeSignalExternalWorkflowExecutionInitiated {
			seenTrigger = true
			break
		}
	}

	if resetEventID == 0 || !seenTrigger {
		return workflow.Execution{}, fmt.Errorf("something went wrong...base workflow has not reach reset point, %v, %v", resetEventID, seenTrigger)
	}

	req := &workflowservice.ResetWorkflowExecutionRequest{
		Domain: domain,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: baseWE.ID,
			RunId:      baseWE.RunID,
		},
		Reason:                reason,
		DecisionFinishEventId: resetEventID,
		RequestId:             baseWE.RunID,
	}
	resp, err := svClient.ResetWorkflowExecution(ctx, req)
	if err != nil {
		return workflow.Execution{}, err
	}
	baseWE.RunID = resp.RunId
	return baseWE, nil
}

func verifyResetActivity(ctx context.Context, domain string, newWE workflow.Execution) error {
	svClient := getActivityContext(ctx).cadence.Service

	resp, err := svClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Domain: domain,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: newWE.ID,
			RunId:      newWE.RunID,
		},
	})
	if err != nil {
		return err
	}
	if resp.WorkflowExecutionInfo.CloseStatus == enums.WorkflowExecutionCloseStatusRunning || resp.WorkflowExecutionInfo.GetCloseStatus() != enums.WorkflowExecutionCloseStatusCompleted {
		return fmt.Errorf("new execution triggered by reset is not completed")
	}
	return nil
}

func resetBaseActivity(ctx context.Context, waitSecs int64) error {
	time.Sleep(time.Second * time.Duration(waitSecs))
	return nil
}
