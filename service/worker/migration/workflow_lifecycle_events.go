package migration

import (
	"context"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/wideevents"
)

const (
	migrationWorkflowLifecycleVersion = "migration-workflow-lifecycle-events-20260819"
	maxWorkflowLifecycleErrorLength   = 2048
)

type migrationWorkflowLifecycle struct {
	namespace     string
	startedPhase  string
	finishedPhase string
	workflowType  string
	workflowID    string
	runID         string
	firstRunID    string
	input         map[string]any
}

func newMigrationWorkflowLifecycle(
	ctx workflow.Context,
	namespace string,
	startedPhase string,
	finishedPhase string,
	input map[string]any,
) migrationWorkflowLifecycle {
	info := workflow.GetInfo(ctx)
	firstRunID := info.FirstRunID
	if firstRunID == "" {
		firstRunID = info.WorkflowExecution.RunID
	}
	return migrationWorkflowLifecycle{
		namespace:     namespace,
		startedPhase:  startedPhase,
		finishedPhase: finishedPhase,
		workflowType:  info.WorkflowType.Name,
		workflowID:    info.WorkflowExecution.ID,
		runID:         info.WorkflowExecution.RunID,
		firstRunID:    firstRunID,
		input:         input,
	}
}

func (l migrationWorkflowLifecycle) isFirstRun() bool {
	return l.runID == l.firstRunID
}

func (l migrationWorkflowLifecycle) emitStarted(ctx workflow.Context) {
	l.emit(ctx, l.startedPhase, "", "", nil)
}

func (l migrationWorkflowLifecycle) emitFinished(
	ctx workflow.Context,
	err error,
	verifiedWorkflowCount *int64,
) {
	if workflow.IsContinueAsNewError(err) {
		return
	}

	status := wideevents.NamespaceMigrationWorkflowSucceeded
	errorMessage := ""
	if err != nil {
		status = wideevents.NamespaceMigrationWorkflowFailed
		if temporal.IsCanceledError(err) {
			status = wideevents.NamespaceMigrationWorkflowCanceled
		}
		errorMessage = err.Error()
		if len(errorMessage) > maxWorkflowLifecycleErrorLength {
			errorMessage = errorMessage[:maxWorkflowLifecycleErrorLength]
		}
	}

	disconnectedCtx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	l.emit(disconnectedCtx, l.finishedPhase, status, errorMessage, verifiedWorkflowCount)
}

func (l migrationWorkflowLifecycle) emit(
	ctx workflow.Context,
	phase string,
	status string,
	errorMessage string,
	verifiedWorkflowCount *int64,
) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var a *activities
	err := workflow.ExecuteActivity(activityCtx, a.EmitNamespaceMigrationWorkflowLifecycle,
		wideevents.NamespaceMigrationWorkflowLifecycleInput{
			Phase:                 phase,
			Namespace:             l.namespace,
			WorkflowType:          l.workflowType,
			WorkflowID:            l.workflowID,
			RunID:                 l.runID,
			FirstRunID:            l.firstRunID,
			Input:                 l.input,
			Status:                status,
			ErrorMessage:          errorMessage,
			VerifiedWorkflowCount: verifiedWorkflowCount,
		},
	).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Warn("Failed to emit namespace migration workflow lifecycle event", "error", err)
	}
}

func (a *activities) EmitNamespaceMigrationWorkflowLifecycle(
	_ context.Context,
	in wideevents.NamespaceMigrationWorkflowLifecycleInput,
) error {
	if a.emitNamespaceLifecycleEvents == nil || !a.emitNamespaceLifecycleEvents() {
		return nil
	}
	in.NamespaceID = a.namespaceIDForEvent(in.Namespace)
	wideevents.EmitNamespaceMigrationWorkflowLifecycle(a.EventLogger, in)
	return nil
}
