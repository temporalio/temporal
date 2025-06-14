package acceptance

import (
	"testing"

	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/patterns"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestUpdateWithStart(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Complete Update", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			upd := ActStart(usr, UpdateWithStart{
				StartWorkflow:  StartWorkflowExecution{TaskQueue: tq},
				UpdateWorkflow: UpdateWorkflowExecution{},
			})

			patterns.CompleteUpdate(s, upd, wkr, tq)
		})
	})
}
