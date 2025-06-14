package patterns

import (
	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func CompleteUpdate(
	s *Scenario,
	upd *model.WorkflowUpdate,
	wkr *testenv.WorkflowWorker,
	tq *model.TaskQueue,
) {
	s.Switch("Complete Update",
		LabeledFunction{
			Label: "Accept and Complete Update in single task",
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
		// TODO
		//LabeledFunction{
		//	Label: "Accept then Complete Update in two tasks",
		//	Fn: func() {
		//		wft1 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		//		Act(wkr, RespondWorkflowTaskCompleted{
		//			Task:     wft1,
		//			Messages: GenList(WorkflowUpdateAcceptMessage{Update: upd}),
		//		})
		//
		//		wft2 := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		//		Act(wkr, RespondWorkflowTaskCompleted{
		//			Task:     wft2,
		//			Messages: GenList(WorkflowUpdateCompletionMessage{Update: upd}),
		//		})
		//	},
		//},
	)

	s.Await(&upd.Completed)
}
