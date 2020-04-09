package canary

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"
)

const (
	signalName  = "signal-name"
	signalValue = "canary.signal"
)

func registerSignal(r registrar) {
	registerWorkflow(r, signalWorkflow, wfTypeSignal)
	registerWorkflow(r, signalExternalWorkflow, wfTypeSignalExternal)
	registerActivity(r, signalActivity, activityTypeSignal)
}

// signalWorkflow is the workflow implementation to test SignalWorkflowExecution API
// it simply spins up an activity which sends a signal to the parent
func signalWorkflow(ctx workflow.Context, scheduledTimeNanos int64, namespace string) error {
	var err error
	profile, err := beginWorkflow(ctx, wfTypeSignal, scheduledTimeNanos)
	if err != nil {
		return err
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

	cwo := newChildWorkflowOptions(namespace, wfTypeSignalExternal+"-child")
	childCtx := workflow.WithChildOptions(ctx, cwo)
	childFuture := workflow.ExecuteChildWorkflow(childCtx, wfTypeSignalExternal, workflow.Now(ctx).UnixNano(), sleepDuration)
	signalFuture := childFuture.SignalChildWorkflow(childCtx, signalName, signalValue)
	err = signalFuture.Get(childCtx, nil)
	if err != nil {
		return profile.end(err)
	}

	decisionSignalTestFn := func(useRunID bool) error {
		workflowIDSuffix := "-with-run-ID"
		if !useRunID {
			workflowIDSuffix = "-without-run-ID"
		}
		cwo = newChildWorkflowOptions(namespace, wfTypeSignalExternal+workflowIDSuffix)
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
