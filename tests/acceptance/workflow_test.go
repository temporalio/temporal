package acceptance

import (
	"testing"

	. "go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkflow(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Start and Complete", func(t *testing.T) {
		ts.Run(t, func(s *Scenario) {
			_, _, tq, usr, wkr := ts.NewWorkflowStack(s)

			wfe := Act(usr, StartWorkflowExecution{
				TaskQueue:                tq,
				RequestId:                AnyRequestID,
				WorkflowIdReusePolicy:    AnyWorkflowIdReusePolicy,
				WorkflowIdConflictPolicy: AnyWorkflowIdConflictPolicy,
			})
			Act(usr, GetWorkflowExecutionHistory{WorkflowExecution: wfe})

			completeWorkflow(s, usr, wkr, wfe, tq)
		})
	})
}

func completeWorkflow(
	s *Scenario,
	usr *WorkflowClient,
	wkr *WorkflowWorker,
	wfe *model.WorkflowExecution,
	tq *model.TaskQueue,
) {
	s.Ensure(&wfe.IsRunning)
	s.Switch("Complete Workflow",
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
			Label: "Terminate Workflow",
			Fn: func() {
				Act(usr, TerminateWorkflowExecution{WorkflowExecution: wfe})
			},
		},
	)
	//ms.Verify(wfe.IsNotRunning) TODO
}

// TODO: check that AlreadyStarted was hit at least once
// TODO: check that `errIncompatibleIDReusePolicyTerminateIfRunning` was hit at least once

func FuzzWorkflow(f *testing.F) {
	ts := NewTestSuite(f)

	f.Fuzz(func(t *testing.T, seed int) {
		ts.Fuzz(t, seed, func(s *Scenario) {
			_, _, tq, usr, _ := ts.NewWorkflowStack(s)

			_ = Act(usr, StartWorkflowExecution{
				TaskQueue:             tq,
				WorkflowIdReusePolicy: AnyWorkflowIdReusePolicy,
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
