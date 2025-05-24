// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NGetONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
		ts.RunScenarioMacro(t, func(ms *MacroScenario) {
			_, _, tq, usr, wkr := ts.NewNamespaceSetup(ms.Scenario)

			wfe := Act(usr, StartWorkflowExecution{
				TaskQueue:                tq,
				RequestId:                GenRequestID,
				WorkflowIdReusePolicy:    GenWorkflowIdReusePolicy,
				WorkflowIdConflictPolicy: GenWorkflowIdConflictPolicy,
			})
			Act(usr, GetWorkflowExecutionHistory{WorkflowExecution: wfe})

			completeWorkflow(ms, usr, wkr, wfe, tq)
		})
	})
}

func completeWorkflow(
	ms *MacroScenario,
	usr *WorkflowClient,
	wkr *WorkflowWorker,
	wfe *model.WorkflowExecution,
	tq *model.TaskQueue,
) {
	ms.Verify(wfe.IsRunning)
	ms.Switch("Complete Workflow",
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
	ms.Verify(wfe.IsNotRunning)
}

// TODO: check that AlreadyStarted was hit at least once
// TODO: check that `errIncompatibleIDReusePolicyTerminateIfRunning` was hit at least once

func FuzzWorkflow(f *testing.F) {
	ts := NewTestSuite(f)

	f.Fuzz(func(t *testing.T, seed int) {
		s := ts.NewScenario(t, WithSeed(seed))
		_, _, tq, usr, _ := ts.NewNamespaceSetup(s)

		_ = Act(usr, StartWorkflowExecution{
			TaskQueue:             tq,
			WorkflowIdReusePolicy: GenWorkflowIdReusePolicy,
		})
	})
}

// TODO: `s.Dump` to dump the entire state

// TODO: emit code that:
// - actions that were turned into records
// - records in chronological order
// - properties that were waited on

// TODO: always run happy cases first to avoid having to run the entire test suite
