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
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	sleepDuration = time.Second * 10
)

func init() {
	registerWorkflow(cancellationWorkflow, wfTypeCancellation)
	registerWorkflow(cancellationExternalWorkflow, wfTypeCancellationExternal)
	registerActivity(cancellationActivity, activityTypeCancellation)
	registerActivity(cancellationChildActivity, activityTypeCancellationChild)
}

// cancellationWorkflow is the workflow implementation to test for cancellation of workflows
func cancellationWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	profile, err := beginWorkflow(ctx, wfTypeCancellation, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	// this test cancel external workflow
	activityCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
	err = workflow.ExecuteActivity(activityCtx, activityTypeCancellation, workflow.Now(ctx).UnixNano()).Get(activityCtx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Info("cancel workflow failed", zap.Error(err))
		return profile.end(err)
	}

	// this test cancellation of child workflow, inside workflow
	cwo := newChildWorkflowOptions(domain, wfTypeCancellationExternal+"-child")
	childCtx := workflow.WithChildOptions(ctx, cwo)
	childCtx, cancel := workflow.WithCancel(childCtx)
	childFuture := workflow.ExecuteChildWorkflow(childCtx, wfTypeCancellationExternal, workflow.Now(ctx).UnixNano(), sleepDuration)
	workflow.Sleep(ctx, 1*time.Second)
	cancel()
	err = childFuture.Get(childCtx, nil)
	if err == nil {
		msg := "cancellationWorkflow failed: child workflow not cancelled"
		workflow.GetLogger(ctx).Info(msg)
		return profile.end(errors.New(msg))
	}
	if _, ok := err.(*cadence.CanceledError); !ok {
		workflow.GetLogger(ctx).Info("cancellationWorkflow failed: child workflow return non CanceledError", zap.Error(err))
		return profile.end(err)
	}

	// this test cancellation of child workflow, outside workflow
	activityCancelChildTestFn := func(useRunID bool) error {
		cwo := newChildWorkflowOptions(domain, wfTypeCancellationExternal+"-child-outside")
		childCtx := workflow.WithChildOptions(ctx, cwo)
		childFuture := workflow.ExecuteChildWorkflow(childCtx, wfTypeCancellationExternal, workflow.Now(ctx).UnixNano(), sleepDuration)
		childExecution := &workflow.Execution{}
		err := childFuture.GetChildWorkflowExecution().Get(childCtx, childExecution)
		if err != nil {
			return err
		}
		if !useRunID {
			childExecution.RunID = ""
		}
		// starts a activity to cancel this child workflow
		activityCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
		err = workflow.ExecuteActivity(activityCtx, activityTypeCancellationChild, workflow.Now(ctx).UnixNano(), childExecution).Get(activityCtx, nil)
		if err != nil {
			workflow.GetLogger(ctx).Info("cancel child workflow from activity failed", zap.Error(err))
			return err
		}
		err = childFuture.Get(childCtx, nil)
		if err == nil {
			msg := "cancellationWorkflow failed: child workflow not cancelled"
			workflow.GetLogger(ctx).Info(msg)
			return errors.New(msg)
		}
		if _, ok := err.(*cadence.CanceledError); !ok {
			workflow.GetLogger(ctx).Info("cancellationWorkflow failed: child workflow return non CanceledError", zap.Error(err))
			return err
		}
		return nil
	}
	err = activityCancelChildTestFn(false)
	if err != nil {
		return profile.end(err)
	}
	err = activityCancelChildTestFn(true)
	if err != nil {
		return profile.end(err)
	}

	decisionCancelTestFn := func(useRunID bool) error {
		workflowIDSuffix := "-with-run-ID"
		if !useRunID {
			workflowIDSuffix = "-without-run-ID"
		}
		cwo = newChildWorkflowOptions(domain, wfTypeCancellationExternal+workflowIDSuffix)
		childCtx = workflow.WithChildOptions(ctx, cwo)
		childFuture = workflow.ExecuteChildWorkflow(childCtx, wfTypeCancellationExternal, workflow.Now(ctx).UnixNano(), sleepDuration)
		childExecution := &workflow.Execution{}
		err = childFuture.GetChildWorkflowExecution().Get(childCtx, childExecution)
		if err != nil {
			return err
		}
		if !useRunID {
			childExecution.RunID = ""
		}
		cancellatinFuture := workflow.RequestCancelExternalWorkflow(ctx, childExecution.ID, childExecution.RunID)
		err = cancellatinFuture.Get(ctx, nil)
		if err != nil {
			workflow.GetLogger(ctx).Info("cancellationWorkflow failed: fail to send cancellation to child workflow", zap.Error(err))
			return err
		}
		err = childFuture.Get(childCtx, nil)
		if err == nil {
			msg := "cancellationWorkflow failed: child workflow not cancelled"
			workflow.GetLogger(ctx).Info(msg)
			return errors.New(msg)
		}
		if _, ok := err.(*cadence.CanceledError); !ok {
			workflow.GetLogger(ctx).Info("cancellationWorkflow failed: child workflow return non CanceledError", zap.Error(err))
			return err
		}
		return nil
	}

	err = decisionCancelTestFn(false)
	if err != nil {
		return profile.end(err)
	}
	err = decisionCancelTestFn(true)
	return profile.end(err)
}

// cancellationActivity is the activity implementation to test for cancellation of non child workflow, using API
func cancellationActivity(ctx context.Context, scheduledTimeNanos int64) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeCancellation, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityContext(ctx).cadence
	apiCancelTestFn := func(useRunID bool) error {
		opts := newWorkflowOptions(wfTypeCancellationExternal, childWorkflowTimeout)
		workflowRun, err := client.ExecuteWorkflow(context.Background(), opts, wfTypeCancellationExternal, scheduledTimeNanos, sleepDuration)
		if err != nil {
			return err
		}
		runID := ""
		if useRunID {
			runID = workflowRun.GetRunID()
		}
		err = client.CancelWorkflow(context.Background(), wfTypeCancellationExternal, runID)
		if err != nil {
			return err
		}
		err = workflowRun.Get(ctx, nil)
		if err == nil {
			return errors.New("cancellationWorkflow failed: non child workflow not cancelled")
		}
		if _, ok := err.(*cadence.CanceledError); !ok {
			return err
		}
		return nil
	}

	if err := apiCancelTestFn(false); err != nil {
		return err
	}
	return apiCancelTestFn(true)
}

// cancellationActivity is the activity implementation to test for cancellation of non child workflow, using API
func cancellationChildActivity(ctx context.Context, scheduledTimeNanos int64, execution workflow.Execution) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeCancellationChild, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityContext(ctx).cadence
	return client.CancelWorkflow(context.Background(), execution.ID, execution.RunID)
}

// cancellationExternalWorkflow is the workflow implementation to test for cancellation of non child workflow
func cancellationExternalWorkflow(ctx workflow.Context, scheduledTimeNanos int64, sleepDuration time.Duration) error {
	profile, err := beginWorkflow(ctx, wfTypeCancellationExternal, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	err = workflow.Sleep(ctx, sleepDuration)
	if err != nil {
		return profile.end(err)
	}

	return nil
}
