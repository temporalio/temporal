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

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	signalName  = "signal-name"
	signalValue = "canary.signal"
)

func init() {
	registerWorkflow(signalWorkflow, wfTypeSignal)
	registerWorkflow(signalExternalWorkflow, wfTypeSignalExternal)
	registerActivity(signalActivity, activityTypeSignal)
}

// signalWorkflow is the workflow implementation to test SignalWorkflowExecution API
// it simply spins up an activity which sends a signal to the parent
func signalWorkflow(ctx workflow.Context, scheduledTimeNanos int64, domain string) error {
	var err error
	profile, err := beginWorkflow(ctx, wfTypeSignal, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	execInfo := workflow.GetInfo(ctx).WorkflowExecution
	sigName := fmt.Sprintf("sig.%v", execInfo.RunID)
	sigCh := workflow.GetSignalChannel(ctx, sigName)

	aCtx := workflow.WithActivityOptions(ctx, newActivityOptions())
	err = workflow.ExecuteActivity(aCtx, activityTypeSignal, workflow.Now(ctx).UnixNano(), execInfo, sigName).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("signal activity failed", zap.Error(err))
		return profile.end(err)
	}

	var sigValue string
	sigCh.Receive(ctx, &sigValue)
	if sigValue != signalValue {
		workflow.GetLogger(ctx).Error("wrong signal value received", zap.String("value", sigValue))
		return profile.end(errors.New("invalid signal value"))
	}
	sigCh.Receive(ctx, &sigValue)
	if sigValue != signalValue {
		workflow.GetLogger(ctx).Error("wrong signal value received", zap.String("value", sigValue))
		return profile.end(errors.New("invalid signal value"))
	}

	cwo := newChildWorkflowOptions(domain, wfTypeSignalExternal+"-child")
	childCtx := workflow.WithChildOptions(ctx, cwo)
	childFuture := workflow.ExecuteChildWorkflow(childCtx, wfTypeSignalExternal, workflow.Now(ctx).UnixNano(), sleepDuration)
	signalFuture := childFuture.SignalChildWorkflow(childCtx, signalName, signalValue)
	err = signalFuture.Get(childCtx, nil)
	if err != nil {
		profile.end(err)
	}

	decisionSignalTestFn := func(useRunID bool) error {
		workflowIDSuffix := "-with-run-ID"
		if !useRunID {
			workflowIDSuffix = "-without-run-ID"
		}
		cwo = newChildWorkflowOptions(domain, wfTypeSignalExternal+workflowIDSuffix)
		childCtx = workflow.WithChildOptions(ctx, cwo)
		childFuture = workflow.ExecuteChildWorkflow(childCtx, wfTypeSignalExternal, workflow.Now(ctx).UnixNano(), sleepDuration)
		childExecution := &workflow.Execution{}
		err = childFuture.GetChildWorkflowExecution().Get(childCtx, childExecution)
		if err != nil {
			return err
		}
		if !useRunID {
			childExecution.RunID = ""
		}
		signalFuture = workflow.SignalExternalWorkflow(ctx, childExecution.ID, childExecution.RunID, signalName, signalValue)
		err = signalFuture.Get(ctx, nil)
		if err != nil {
			workflow.GetLogger(ctx).Info("signalWorkflow failed: fail to send siganl to child workflow", zap.Error(err))
			return err
		}
		err = childFuture.Get(childCtx, nil)
		if err != nil {
			workflow.GetLogger(ctx).Info("signalWorkflow failed: child workflow return err", zap.Error(err))
			return err
		}
		return nil
	}

	err = decisionSignalTestFn(false)
	if err != nil {
		return profile.end(err)
	}
	err = decisionSignalTestFn(true)
	return profile.end(err)
}

// signalActivity sends a signal to the parent workflow
// that created this activity
func signalActivity(ctx context.Context, scheduledTimeNanos int64, execInfo workflow.Execution, signalName string) error {
	scope := activity.GetMetricsScope(ctx)
	var err error
	scope, sw := recordActivityStart(scope, activityTypeSignal, scheduledTimeNanos)
	defer recordActivityEnd(scope, sw, err)

	client := getActivityContext(ctx).cadence
	err = client.SignalWorkflow(context.Background(), execInfo.ID, execInfo.RunID, signalName, signalValue)
	if err != nil {
		return err
	}
	err = client.SignalWorkflow(context.Background(), execInfo.ID, "", signalName, signalValue)
	if err != nil {
		return err
	}

	return nil
}

// signalExternalWorkflow receive a signal from the parent workflow
func signalExternalWorkflow(ctx workflow.Context, scheduledTimeNanos int64) error {
	profile, err := beginWorkflow(ctx, wfTypeSignalExternal, scheduledTimeNanos)
	if err != nil {
		return profile.end(err)
	}

	var value string
	signalCh := workflow.GetSignalChannel(ctx, signalName)
	signalCh.Receive(ctx, &value)
	if value != signalValue {
		workflow.GetLogger(ctx).Error("wrong signal value received", zap.String("value", value))
		return profile.end(errors.New("invalid signal value"))
	}

	return profile.end(nil)
}
