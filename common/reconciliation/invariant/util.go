// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package invariant

import (
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

func checkBeforeFix(
	invariant Invariant,
	execution interface{},
) (*FixResult, *CheckResult) {
	checkResult := invariant.Check(execution)
	if checkResult.CheckResultType == CheckResultTypeHealthy {
		return &FixResult{
			FixResultType: FixResultTypeSkipped,
			InvariantType: invariant.Name(),
			CheckResult:   checkResult,
			Info:          "skipped fix because execution was healthy",
		}, nil
	}
	if checkResult.CheckResultType == CheckResultTypeFailed {
		return &FixResult{
			FixResultType: FixResultTypeFailed,
			InvariantType: invariant.Name(),
			CheckResult:   checkResult,
			Info:          "failed fix because check failed",
		}, nil
	}
	return nil, &checkResult
}

// Open returns true if workflow state is open false if workflow is closed
func Open(state int) bool {
	return state == persistence.WorkflowStateCreated || state == persistence.WorkflowStateRunning
}

// ExecutionOpen returns true if execution state is open false if workflow is closed
func ExecutionOpen(execution interface{}) bool {
	return Open(getExecution(execution).State)
}

// getExecution returns base Execution
func getExecution(execution interface{}) *entity.Execution {
	switch e := execution.(type) {
	case *entity.CurrentExecution:
		return &e.Execution
	case *entity.ConcreteExecution:
		return &e.Execution
	default:
		panic("unexpected execution type")
	}
}

// DeleteExecution deletes concrete execution and
// current execution conditionally on matching runID.
func DeleteExecution(
	exec interface{},
	pr persistence.Retryer,
) *FixResult {
	execution := getExecution(exec)
	if err := pr.DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
		DomainID:   execution.DomainID,
		WorkflowID: execution.WorkflowID,
		RunID:      execution.RunID,
	}); err != nil {
		return &FixResult{
			FixResultType: FixResultTypeFailed,
			Info:          "failed to delete concrete workflow execution",
			InfoDetails:   err.Error(),
		}
	}
	if err := pr.DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
		DomainID:   execution.DomainID,
		WorkflowID: execution.WorkflowID,
		RunID:      execution.RunID,
	}); err != nil {
		return &FixResult{
			FixResultType: FixResultTypeFailed,
			Info:          "failed to delete current workflow execution",
			InfoDetails:   err.Error(),
		}
	}
	return &FixResult{
		FixResultType: FixResultTypeFixed,
	}
}
