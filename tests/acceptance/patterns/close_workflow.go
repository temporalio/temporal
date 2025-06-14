package patterns

import (
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func CloseWorkflow(
	s *Scenario,
	usr *testenv.WorkflowClient,
	wkr *testenv.WorkflowWorker,
	wfe *model.WorkflowExecution,
	tq *model.TaskQueue,
) {
	s.Ensure(&wfe.IsRunning)
	s.Switch("Close Workflow",
		// TODO
		//LabeledFunction{
		//	Label: "Complete Command",
		//	Fn: func() {
		//		wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		//		Act(wkr, RespondWorkflowTaskCompleted{
		//			WorkflowTask: wft,
		//			Commands:     GenList(CompleteWorkflowExecutionCommand{}),
		//		})
		//	},
		//},

		LabeledFunction{
			Label: "Continue-as-New Command",
			Fn: func() {
				wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:     wft,
					Commands: GenList(ContinueAsNewWorkflowCommand{}),
				})
			},
		},

		LabeledFunction{
			Label: "Terminate Workflow",
			Fn: func() {
				Act(usr, TerminateWorkflowExecution{WorkflowExecution: wfe})
			},
		},
	)
	//ms.Verify(wfe.IsNotRunning) TODO
}
