package roundtrip

// Shared step sequences for the case files in this package.
//
// Cases are split one file per use case: cases_workflow_test.go,
// cases_workflowtask_test.go, cases_activity_test.go, cases_usertimer_test.go,
// cases_childworkflow_test.go. Anything shared by more than one of them lives here.
//
// See framework_test.go for what running a case actually does, and diff_test.go for how the
// two sides are compared.

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
