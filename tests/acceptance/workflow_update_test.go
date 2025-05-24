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
	. "go.temporal.io/server/tests/acceptance/testenv"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

func TestWorkflowUpdate(t *testing.T) {
	ts := NewTestSuite(t)

	t.Run("Complete Update", func(t *testing.T) {
		s := ts.NewScenario(t)
		_, _, tq, usr, wkr := ts.NewNamespaceSetup(s)

		wfe := Act(usr, StartWorkflowExecution{TaskQueue: tq})

		wft := Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		Act(wkr, RespondWorkflowTaskCompleted{WorkflowTask: wft})

		upd := ActAsync(usr, UpdateWorkflowExecution{WorkflowExecution: wfe})

		wft = Act(wkr, PollWorkflowTaskQueue{TaskQueue: tq})
		Act(wkr, RespondWorkflowTaskCompleted{
			WorkflowTask: wft,
			Messages: GenList(
				WorkflowUpdateAcceptMessage{Update: upd},
				WorkflowUpdateCompletionMessage{Update: upd}),
		})

		s.Eventually(upd.IsDone)
	})
}
