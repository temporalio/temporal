package ndc

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/service/history/workflow"
)

// Lifecycle cases for the round-trip framework. See
// roundtrip_framework_test.go for what a case actually does.

// TestStartWorkflow is the smallest end-to-end slice: create a workflow on the active
// cluster and replicate it to a passive cluster that has never seen it. This exercises the
// first-sync snapshot path (applySnapshotWhenWorkflowNotExist), including a real history
// branch fork and event append on the passive side.
func (s *rtSuite) TestStartWorkflow() {
	s.runCase(rtCase{
		name: "StartWorkflow",
		steps: []rtStep{
			{name: "start-workflow", fn: rtStartWorkflow},
		},
	})
}

// TestWorkflowTaskLifecycle is the first case to exercise incremental mutation artifacts:
// step 1 creates the workflow (snapshot), and every step after it replicates only the
// delta. The framework asserts that steps 2+ really are mutations, so a config that
// silently disabled transition history would fail here rather than quietly degrading the
// whole suite to snapshot-only replication.
func (s *rtSuite) TestWorkflowTaskLifecycle() {
	s.runCase(rtCase{
		name: "WorkflowTaskLifecycle",
		steps: []rtStep{
			{name: "start-workflow", fn: rtStartWorkflow},
			{name: "start-workflow-task", fn: rtStartWorkflowTask},
			{name: "complete-workflow-task", fn: rtCompleteWorkflowTask, allowNoTasks: true},
			// A second workflow task, this time with the first one properly completed. The
			// point is to check whether the workflow-start timers are regenerated again, or
			// only on the CREATED -> RUNNING transition the first schedule performs.
			{name: "schedule-second-workflow-task", fn: rtScheduleWorkflowTask},
			{name: "start-second-workflow-task", fn: rtStartWorkflowTask},
			{name: "complete-second-workflow-task", fn: rtCompleteWorkflowTask, allowNoTasks: true},
		},
	})
}

// TestActivityLifecycle covers the schedule/start/complete path. Activity timers are the
// most likely place for active and passive to diverge: the active cluster generates them in
// closeTransactionHandleActivityUserTimerTasks at close-transaction time, while the passive
// cluster derives them from CreateNextActivityTimer during the task refresh.
func (s *rtSuite) TestActivityLifecycle() {
	s.runCase(rtCase{
		name: "ActivityLifecycle",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "schedule-activity", fn: rtScheduleActivity},
			rtStep{name: "start-activity", fn: rtStartActivity},
			rtStep{name: "complete-activity", fn: rtCompleteActivity, allowNoTasks: true},
		),
	})
}

// TestUserTimerLifecycle covers a pending user timer, which is where the earliest-timer-only
// optimization and the task-status mask live.
func (s *rtSuite) TestUserTimerLifecycle() {
	s.runCase(rtCase{
		name: "UserTimerLifecycle",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "start-timer", fn: rtStartTimer},
		),
	})
}

// TestChildWorkflowLifecycle covers a pending child, whose transfer task both sides derive
// from pending child info.
func (s *rtSuite) TestChildWorkflowLifecycle() {
	s.runCase(rtCase{
		name: "ChildWorkflowLifecycle",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "start-child-workflow", fn: rtStartChildWorkflow},
		),
	})
}

// TestCompleteWorkflow covers closing, which brings in the close transfer task, the
// retention timer and the close visibility task.
func (s *rtSuite) TestCompleteWorkflow() {
	s.runCase(rtCase{
		name: "CompleteWorkflow",
		steps: append(rtStartedWorkflowSteps(),
			rtStep{name: "complete-workflow", fn: rtCompleteWorkflow},
		),
	})
}

// rtStartedWorkflowSteps drives a workflow up to a completed first workflow task, which is
// the precondition for scheduling activities, timers and child workflows.
func rtStartedWorkflowSteps() []rtStep {
	return []rtStep{
		// The start step also schedules the first workflow task, as production does.
		{name: "start-workflow", fn: rtStartWorkflow},
		{name: "start-workflow-task", fn: rtStartWorkflowTask},
		{name: "complete-workflow-task", fn: rtCompleteWorkflowTask, allowNoTasks: true},
	}
}

// TestActivityRetries drives an activity through rtActivityRetries failed attempts,
// replicating after each one.
//
// Each attempt moves the activity's Attempt and Stamp and re-anchors its schedule-to-start
// deadline, which is exactly what getActivityTimerTaskStatus inspects when deciding which
// timer task mask bits survive replication. It is also the one lifecycle path where the
// dispatch task changes shape: a first attempt goes out as a transfer ActivityTask, while a
// retry goes out as a timer ActivityRetryTimerTask.
//
// A long sequence is worth having on its own: anything that accumulates per replicated
// delta shows up as a growing divergence here even when a single step looks clean.
func (s *rtSuite) TestActivityRetries() {
	steps := append(rtStartedWorkflowSteps(),
		rtStep{name: "schedule-retryable-activity", fn: rtScheduleRetryableActivity},
	)
	for attempt := 1; attempt <= rtActivityRetries; attempt++ {
		steps = append(steps,
			rtStep{
				name: fmt.Sprintf("start-attempt-%02d", attempt),
				fn:   rtStartActivity,
			},
			rtStep{
				name: fmt.Sprintf("fail-attempt-%02d", attempt),
				fn:   rtFailActivityWithRetry,
			},
		)
	}
	s.runCase(rtCase{name: "ActivityRetries", steps: steps})
}

// TestWorkflowBackoffTimer covers a workflow whose first workflow task is delayed, so the
// start emits a WorkflowBackoffTimerTask instead of dispatching a workflow task.
//
// This is the one start-task path the other cases never reach:
// RefreshTasksForWorkflowStart only calls GenerateDelayedWorkflowTasks when the workflow has
// not had a workflow task yet AND the started event carries a non-zero
// FirstWorkflowTaskBackoff, so every other case skips it entirely.
//
// The three subcases differ only in the started event's initiator, which is what
// GenerateDelayedWorkflowTasks turns into the task's WorkflowBackoffType. The backoff type
// is part of the task identity, so a passive side that reconstructed the task but lost the
// initiator would fail here rather than silently look equivalent.
func (s *rtSuite) TestWorkflowBackoffTimer() {
	backoffCase := func(name string, initiator enumspb.ContinueAsNewInitiator) rtCase {
		return rtCase{
			name: name,
			steps: []rtStep{
				{
					name: "start-workflow-with-backoff",
					fn: func(s *rtSuite, ms *workflow.MutableStateImpl) error {
						return rtStartWorkflowWith(s, ms, time.Minute, initiator)
					},
				},
				// The backoff timer fires and the first workflow task finally goes out. Worth
				// including because it is the transition that clears HadOrHasWorkflowTask, and
				// so the point after which the backoff task must not be regenerated.
				{name: "schedule-workflow-task", fn: rtScheduleWorkflowTask},
			},
		}
	}

	for _, tc := range []rtCase{
		backoffCase("DelayStart", enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED),
		backoffCase("Retry", enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY),
		backoffCase("Cron", enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE),
	} {
		s.SetupTest() // each subcase needs its own pair of clusters
		s.runCase(tc)
		s.TearDownTest()
	}
}
