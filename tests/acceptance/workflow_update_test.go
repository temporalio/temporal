package acceptance

import (
	"testing"

	. "go.temporal.io/server/common/testing/stamp"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkflowUpdate(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Complete Update", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

			wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			Act(wkr, RespondWorkflowTaskCompleted{WorkflowTask: wft})

			upd := ActStart(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

			wft = Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
			Act(wkr, RespondWorkflowTaskCompleted{
				WorkflowTask: wft,
				Messages: GenList(
					WorkflowUpdateAcceptMessage{Update: upd},
					WorkflowUpdateCompletionMessage{Update: upd}),
			})

			s.Await(&upd.Completed)
		})
	})
}
