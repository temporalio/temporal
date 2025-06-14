package acceptance

import (
	"testing"

	. "go.temporal.io/server/common/testing/stamp"
	. "go.temporal.io/server/tests/acceptance/patterns"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkflow(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Start and Complete", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{
				TaskQueue:        tq,
				RequestId:        RequestIDs,
				IdReusePolicy:    WorkflowIdReusePolicies,
				IdConflictPolicy: WorkflowIdConflictPolicies,
			}, WithSkipValidationError())
			Act(usr, GetWorkflowExecutionHistory{WorkflowExecution: wfe})

			CloseWorkflow(s, usr, wkr, wfe, tq)
		})
	})
}

func FuzzWorkflow(f *testing.F) {
	ts := NewTestSuite(f)

	f.Fuzz(func(t *testing.T, seed int) {
		ts.Fuzz(t, seed, func(s *Scenario) {
			_, _, tq, usr, _ := ts.NewWorkflowStack(s)

			_ = Act(usr, StartWorkflowExecution{
				TaskQueue:     tq,
				IdReusePolicy: WorkflowIdReusePolicies,
			})
		})
	})
}
