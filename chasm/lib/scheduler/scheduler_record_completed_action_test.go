package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
)

type recordCompletedActionTestCase struct {
	name           string
	setupScheduler func(*scheduler.Scheduler)
	workflowID     string
	workflowStatus enumspb.WorkflowExecutionStatus
	scheduleTime   time.Time
	validate       func(*testing.T, *scheduler.Scheduler)
}

func executeRecordCompletedAction(t *testing.T, tc recordCompletedActionTestCase) {
	sched, ctx, node := setupSchedulerForTest(t)

	if tc.setupScheduler != nil {
		tc.setupScheduler(sched)
	}

	sched.RecordCompletedAction(ctx, tc.scheduleTime, tc.workflowID, tc.workflowStatus)

	_, err := node.CloseTransaction()
	require.NoError(t, err)

	if tc.validate != nil {
		tc.validate(t, sched)
	}
}

// TestRecordCompletedAction_SingleWorkflowInRunningWorkflows verifies that when
// a workflow exists in both RunningWorkflows and RecentActions, it is removed
// from RunningWorkflows and its status is updated in RecentActions.
func TestRecordCompletedAction_SingleWorkflowInRunningWorkflows(t *testing.T) {
	tc := recordCompletedActionTestCase{
		name: "single workflow in RunningWorkflows",
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			}
		},
		workflowID:     "wf-1",
		workflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		scheduleTime:   time.Now(),
		validate: func(t *testing.T, sched *scheduler.Scheduler) {
			require.Empty(t, sched.Info.RunningWorkflows)
			require.Len(t, sched.Info.RecentActions, 1)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, sched.Info.RecentActions[0].StartWorkflowStatus)
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_MultipleWorkflowsSameID verifies that when multiple
// workflows with the same workflowID but different runIDs exist in RunningWorkflows,
// ALL of them are removed. This handles scenarios where workflows may have retried
// or continued-as-new, resulting in multiple runs with different runIDs.
func TestRecordCompletedAction_MultipleWorkflowsSameID(t *testing.T) {
	tc := recordCompletedActionTestCase{
		name: "multiple workflows with same workflowID but different runIDs",
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
				{WorkflowId: "wf-1", RunId: "run-2"},
				{WorkflowId: "wf-1", RunId: "run-3"},
				{WorkflowId: "other-wf", RunId: "other-run"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			}
		},
		workflowID:     "wf-1",
		workflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		scheduleTime:   time.Now(),
		validate: func(t *testing.T, sched *scheduler.Scheduler) {
			require.Len(t, sched.Info.RunningWorkflows, 1)
			require.Equal(t, "other-wf", sched.Info.RunningWorkflows[0].WorkflowId)

			for _, wf := range sched.Info.RunningWorkflows {
				require.NotEqual(t, "wf-1", wf.WorkflowId)
			}
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_WorkflowNotInRunningWorkflows verifies that when a
// workflow is not present in RunningWorkflows but exists in RecentActions, the
// function successfully updates RecentActions without errors. This can happen when
// a workflow completes after already being removed from RunningWorkflows.
func TestRecordCompletedAction_WorkflowNotInRunningWorkflows(t *testing.T) {
	tc := recordCompletedActionTestCase{
		name: "workflow not in RunningWorkflows",
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "other-wf", RunId: "other-run"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			}
		},
		workflowID:     "wf-1",
		workflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		scheduleTime:   time.Now(),
		validate: func(t *testing.T, sched *scheduler.Scheduler) {
			require.Len(t, sched.Info.RunningWorkflows, 1)
			require.Len(t, sched.Info.RecentActions, 1)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, sched.Info.RecentActions[0].StartWorkflowStatus)
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_WorkflowInBothLists verifies that when a workflow
// exists in both RunningWorkflows and RecentActions, it is properly removed from
// RunningWorkflows while the status in RecentActions is updated to reflect the
// completion state (e.g., FAILED, COMPLETED).
func TestRecordCompletedAction_WorkflowInBothLists(t *testing.T) {
	tc := recordCompletedActionTestCase{
		name: "workflow in both RunningWorkflows and RecentActions",
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
				{WorkflowId: "wf-2", RunId: "run-2"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			}
		},
		workflowID:     "wf-1",
		workflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		scheduleTime:   time.Now(),
		validate: func(t *testing.T, sched *scheduler.Scheduler) {
			require.Len(t, sched.Info.RunningWorkflows, 1)
			require.NotEqual(t, "wf-1", sched.Info.RunningWorkflows[0].WorkflowId)
			require.Len(t, sched.Info.RecentActions, 1)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, sched.Info.RecentActions[0].StartWorkflowStatus)
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_NewEntryToRecentActions verifies that when a workflow
// is in RunningWorkflows but NOT in RecentActions, a new entry is created in
// RecentActions with the provided scheduleTime. This handles cases where a workflow
// completes before its start was recorded in RecentActions.
func TestRecordCompletedAction_NewEntryToRecentActions(t *testing.T) {
	schedTime := time.Now().Add(-1 * time.Minute)
	tc := recordCompletedActionTestCase{
		name: "workflow in RunningWorkflows but not in RecentActions, adds new entry",
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{}
		},
		workflowID:     "wf-1",
		workflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		scheduleTime:   schedTime,
		validate: func(t *testing.T, sched *scheduler.Scheduler) {
			require.Empty(t, sched.Info.RunningWorkflows)
			require.Len(t, sched.Info.RecentActions, 1)

			action := sched.Info.RecentActions[0]
			require.Equal(t, "wf-1", action.StartWorkflowResult.WorkflowId)
			require.Equal(t, schedTime.Unix(), action.ScheduleTime.AsTime().Unix())
			require.Equal(t, schedTime.Unix(), action.ActualTime.AsTime().Unix())
		},
	}

	executeRecordCompletedAction(t, tc)
}
