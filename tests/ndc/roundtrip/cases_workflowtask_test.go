package roundtrip

import (
	"fmt"

	"go.temporal.io/server/service/history/workflow"
)

// Cases for workflow tasks.

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

// TestWorkflowTaskRetries drives a workflow task through repeated failures and timeouts.
//
// A workflow task retry is not a fresh workflow task: the attempt counter on execution info
// moves, and from attempt 2 onward the workflow task is *transient* -- no scheduled or
// started event is written to history until it finally completes, so the retry exists only in
// mutable state. That makes it a pointed replication case, because the passive cluster has to
// rebuild the workflow task's tasks from mutable state alone, with no events describing it.
//
// refreshWorkflowTaskTasks is the passive side of this, and it is gated on
// WorkflowTaskLastUpdateVersionedTransition and skips speculative workflow tasks entirely. On
// the active side the tasks come from GenerateScheduleWorkflowTaskTasks and
// GenerateStartWorkflowTaskTasks. ScheduleAttempt is part of the timeout task's identity
// here, so a passive side that rebuilt the task but lost the attempt count fails rather than
// looking equivalent.
//
// Both ways out of a started workflow task are covered, since they are separate code paths
// that both land on the same retry: an explicit failure (RespondWorkflowTaskFailed) and a
// start-to-close timeout (the timer queue executor).
func (s *rtSuite) TestWorkflowTaskRetries() {
	retryCase := func(name string, endAttempt func(*rtSuite, *workflow.MutableStateImpl) error) rtCase {
		steps := []rtStep{
			// Start also schedules the first workflow task, attempt 1.
			{name: "start-workflow", fn: rtStartWorkflow},
			{name: "start-workflow-task-attempt-1", fn: rtStartWorkflowTask},
		}
		// Two retries, so the case sees both the first transition into transient territory
		// (attempt 1 -> 2) and a transient-to-transient one (2 -> 3).
		for attempt := 1; attempt <= 2; attempt++ {
			steps = append(steps,
				rtStep{
					name:         fmt.Sprintf("end-attempt-%d", attempt),
					fn:           endAttempt,
					allowNoTasks: true,
				},
				rtStep{
					name: fmt.Sprintf("reschedule-attempt-%d", attempt+1),
					fn:   rtScheduleWorkflowTask,
					// The retry has to be dispatched again, so a transfer task is required.
					requireActive: []string{"*tasks.WorkflowTask"},
				},
				rtStep{
					name:          fmt.Sprintf("start-workflow-task-attempt-%d", attempt+1),
					fn:            rtStartWorkflowTask,
					requireActive: []string{"*tasks.WorkflowTaskTimeoutTask"},
				},
			)
		}
		// Completing resets the attempt counter and writes the deferred events for the
		// transient workflow task.
		steps = append(steps, rtStep{
			name:         "complete-workflow-task",
			fn:           rtCompleteWorkflowTask,
			allowNoTasks: true,
		})
		return rtCase{name: name, steps: steps}
	}

	for _, tc := range []rtCase{
		retryCase("AfterFailure", rtFailWorkflowTask),
		retryCase("AfterTimeout", rtTimeoutWorkflowTask),
	} {
		s.SetupTest() // each subcase needs its own pair of clusters
		s.runCase(tc)
		s.TearDownTest()
	}
}
