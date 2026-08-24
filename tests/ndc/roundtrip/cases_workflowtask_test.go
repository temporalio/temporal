package roundtrip

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
