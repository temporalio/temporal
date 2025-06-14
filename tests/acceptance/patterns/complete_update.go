package patterns

import (
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func AcceptAndCompleteUpdate(
	s *Scenario,
	upd *model.WorkflowUpdate,
	wkr *testenv.WorkflowWorker,
	tq *model.TaskQueue,
) {
	s.Switch("Complete Update",
		LabeledFunction{
			Label: "Accept and Complete Update in single workflow task",
			Fn: func() {
				wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task: wft,
					Messages: GenList(
						WorkflowUpdateAcceptMessage{Update: upd},
						WorkflowUpdateCompletionMessage{Update: upd}),
				})
			},
		},
		LabeledFunction{
			Label: "Accept then Complete Update in two workflow tasks",
			Fn: func() {
				wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:                       wft,
					Messages:                   GenList(WorkflowUpdateAcceptMessage{Update: upd}),
					ForceCreateNewWorkflowTask: GenJust(true),
				})

				wft = Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
				Act(wkr, RespondWorkflowTaskCompleted{
					Task:     wft,
					Messages: GenList(WorkflowUpdateCompletionMessage{Update: upd}),
				})
			},
		},
	)

	s.Await(&upd.Completed)
}
