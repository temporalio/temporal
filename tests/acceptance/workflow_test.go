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

// TODO: check that AlreadyStarted was hit at least once
// TODO: check that `errIncompatibleIDReusePolicyTerminateIfRunning` was hit at least once

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

// TODO: `s.Dump` to dump the entire state

// TODO: emit code that:
// - actions that were turned into records
// - records in chronological order
// - properties that were waited on

// TODO: always run happy cases first to avoid having to run the entire test suite
