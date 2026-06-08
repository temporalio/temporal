package scheduleinvariants

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	OverdueNextActionTimeWorkflowID = "temporal-sys-schedule-invariants-overdue-next-action-time-scanner"
	OverdueNextActionTimeTaskQueue  = "temporal-sys-schedule-invariants-overdue-next-action-time-scanner-taskqueue-0"

	StuckOpenWorkflowID = "temporal-sys-schedule-invariants-stuck-open-scanner"
	StuckOpenTaskQueue  = "temporal-sys-schedule-invariants-stuck-open-scanner-taskqueue-0"

	UnknownStateWorkflowID = "temporal-sys-schedule-invariants-unknown-state-scanner"
	UnknownStateTaskQueue  = "temporal-sys-schedule-invariants-unknown-state-scanner-taskqueue-0"

	// activityStartToCloseTimeout is "effectively infinite" - the activity runs an
	// internal scan ticker forever and is only ever expected to exit on transient
	// failures (worker restart, network glitch, etc.).
	activityStartToCloseTimeout = 365 * 24 * time.Hour
	activityHeartbeatTimeout    = 30 * time.Second
	// workflowRunTimeout bounds a single workflow run; the workflow continues-as-new
	// after each activity exit, so this is just a safety net larger than the
	// activity's own timeout plus retry budget.
	workflowRunTimeout = 366 * 24 * time.Hour
)

var (
	// retryPolicy applies to the long-running scan activity: 10 attempts spaced
	// 5 minutes apart with no exponential backoff. Once exhausted the workflow
	// loops via continue-as-new.
	retryPolicy = &temporal.RetryPolicy{
		InitialInterval:    5 * time.Minute,
		BackoffCoefficient: 1.0,
		MaximumAttempts:    10,
	}

	OverdueNextActionTimeWFStartOptions = client.StartWorkflowOptions{
		ID:                    OverdueNextActionTimeWorkflowID,
		TaskQueue:             OverdueNextActionTimeTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:    workflowRunTimeout,
	}
	StuckOpenWFStartOptions = client.StartWorkflowOptions{
		ID:                    StuckOpenWorkflowID,
		TaskQueue:             StuckOpenTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:    workflowRunTimeout,
	}
	UnknownStateWFStartOptions = client.StartWorkflowOptions{
		ID:                    UnknownStateWorkflowID,
		TaskQueue:             UnknownStateTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:    workflowRunTimeout,
	}
)

// OverdueNextActionTimeWorkflow scans for schedules whose TemporalScheduleNextActionTime
// is in the past beyond the configured tolerance. The activity runs an internal scan
// ticker; this workflow continues-as-new each time the activity exits.
func OverdueNextActionTimeWorkflow(ctx workflow.Context) error {
	var a *Activities
	runScanActivity(ctx, a.ScanOverdueNextActionTime)
	return workflow.NewContinueAsNewError(ctx, OverdueNextActionTimeWorkflow)
}

// StuckOpenWorkflow scans for schedules that appear stuck open long after their CloseTime.
func StuckOpenWorkflow(ctx workflow.Context) error {
	var a *Activities
	runScanActivity(ctx, a.ScanStuckOpen)
	return workflow.NewContinueAsNewError(ctx, StuckOpenWorkflow)
}

// UnknownStateWorkflow scans for running, unpaused schedules with no
// TemporalScheduleNextActionTime.
func UnknownStateWorkflow(ctx workflow.Context) error {
	var a *Activities
	runScanActivity(ctx, a.ScanUnknownState)
	return workflow.NewContinueAsNewError(ctx, UnknownStateWorkflow)
}

// runScanActivity executes the long-running scan activity. The activity is expected
// to run 'infinitely', timing out on server restart and being retried here and eventually
// continue-as-new'd
func runScanActivity(ctx workflow.Context, activityFn any) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
		HeartbeatTimeout:    activityHeartbeatTimeout,
		RetryPolicy:         retryPolicy,
	})
	// Ignore the activity result intentionally: the activity is expected to exit
	// eventually (heartbeat timeout on worker restart, retry budget exhausted, etc.),
	// and we want to continue-as-new and rebirth the scan loop in every case.
	_ = workflow.ExecuteActivity(activityCtx, activityFn).Get(ctx, nil)
}
