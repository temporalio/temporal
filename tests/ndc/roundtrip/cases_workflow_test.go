package roundtrip

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/service/history/workflow"
)

// Cases for the workflow execution itself: start, backoff-delayed start, and close.

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
//
// This is also the only case that still reproduces
// DEFECT-passive-duplicates-workflow-run-timeout-timer: a backoff workflow schedules its
// first workflow task in a later transaction than the start, so CREATED -> RUNNING reaches
// the passive side as a mutation rather than inside the initial snapshot.
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
