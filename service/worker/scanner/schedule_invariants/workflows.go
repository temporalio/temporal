package schedule_invariants

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	OverdueNextActionTimeWorkflowName = "schedule-invariants-overdue-next-action-time-scanner"
	OverdueNextActionTimeActivityName = "scan-schedule-invariants-overdue-next-action-time"
	OverdueNextActionTimeWorkflowID   = "temporal-sys-schedule-invariants-overdue-next-action-time-scanner"
	OverdueNextActionTimeTaskQueue    = "temporal-sys-schedule-invariants-overdue-next-action-time-scanner-taskqueue-0"

	StuckOpenWorkflowName = "schedule-invariants-stuck-open-scanner"
	StuckOpenActivityName = "scan-schedule-invariants-stuck-open"
	StuckOpenWorkflowID   = "temporal-sys-schedule-invariants-stuck-open-scanner"
	StuckOpenTaskQueue    = "temporal-sys-schedule-invariants-stuck-open-scanner-taskqueue-0"

	UnknownStateWorkflowName = "schedule-invariants-unknown-state-scanner"
	UnknownStateActivityName = "scan-schedule-invariants-unknown-state"
	UnknownStateWorkflowID   = "temporal-sys-schedule-invariants-unknown-state-scanner"
	UnknownStateTaskQueue    = "temporal-sys-schedule-invariants-unknown-state-scanner-taskqueue-0"

	cronSchedule = "@every 15m"
)

var (
	OverdueNextActionTimeWFStartOptions = client.StartWorkflowOptions{
		ID:                    OverdueNextActionTimeWorkflowID,
		TaskQueue:             OverdueNextActionTimeTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          cronSchedule,
	}
	StuckOpenWFStartOptions = client.StartWorkflowOptions{
		ID:                    StuckOpenWorkflowID,
		TaskQueue:             StuckOpenTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          cronSchedule,
	}
	UnknownStateWFStartOptions = client.StartWorkflowOptions{
		ID:                    UnknownStateWorkflowID,
		TaskQueue:             UnknownStateTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		CronSchedule:          cronSchedule,
	}
)

// OverdueNextActionTimeWorkflow scans for schedules whose TemporalScheduleNextActionTime
// is in the past beyond the configured tolerance.
func OverdueNextActionTimeWorkflow(ctx workflow.Context) error {
	return runScanActivity(ctx, OverdueNextActionTimeActivityName)
}

// StuckOpenWorkflow scans for schedules that appear stuck open long after their CloseTime.
func StuckOpenWorkflow(ctx workflow.Context) error {
	return runScanActivity(ctx, StuckOpenActivityName)
}

// UnknownStateWorkflow scans for running, unpaused schedules with no
// TemporalScheduleNextActionTime.
func UnknownStateWorkflow(ctx workflow.Context) error {
	return runScanActivity(ctx, UnknownStateActivityName)
}

func runScanActivity(ctx workflow.Context, activityName string) error {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    30 * time.Second,
	})
	return workflow.ExecuteActivity(activityCtx, activityName).Get(ctx, nil)
}
