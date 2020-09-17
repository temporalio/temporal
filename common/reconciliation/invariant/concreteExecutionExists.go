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
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

type (
	concreteExecutionExists struct {
		pr persistence.Retryer
	}
)

// NewConcreteExecutionExists returns a new invariant for checking concrete execution
func NewConcreteExecutionExists(
	pr persistence.Retryer,
) Invariant {
	return &concreteExecutionExists{
		pr: pr,
	}
}

func (c *concreteExecutionExists) Check(
	execution interface{},
) CheckResult {

	currentExecution, ok := execution.(*entity.CurrentExecution)
	if !ok {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   c.Name(),
			Info:            "failed to check: expected current execution",
		}
	}

	if len(currentExecution.CurrentRunID) == 0 {
		// set the current run id
		var runIDCheckResult *CheckResult
		currentExecution, runIDCheckResult = c.validateCurrentRunID(currentExecution)
		if runIDCheckResult != nil {
			return *runIDCheckResult
		}
	}

	concreteExecResp, concreteExecErr := c.pr.IsWorkflowExecutionExists(&persistence.IsWorkflowExecutionExistsRequest{
		DomainID:   currentExecution.DomainID,
		WorkflowID: currentExecution.WorkflowID,
		RunID:      currentExecution.CurrentRunID,
	})
	if concreteExecErr != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   c.Name(),
			Info:            "failed to check if concrete execution exists",
			InfoDetails:     concreteExecErr.Error(),
		}
	}
	if !concreteExecResp.Exists {
		//verify if the current execution exists
		_, checkResult := c.validateCurrentRunID(currentExecution)
		if checkResult != nil {
			return *checkResult
		}
		return CheckResult{
			CheckResultType: CheckResultTypeCorrupted,
			InvariantName:   c.Name(),
			Info:            "execution is open without having concrete execution",
			InfoDetails: fmt.Sprintf("concrete execution not found. WorkflowId: %v, RunId: %v",
				currentExecution.WorkflowID, currentExecution.CurrentRunID),
		}
	}
	return CheckResult{
		CheckResultType: CheckResultTypeHealthy,
		InvariantName:   c.Name(),
	}
}

func (c *concreteExecutionExists) Fix(
	execution interface{},
) FixResult {

	currentExecution, _ := execution.(*entity.CurrentExecution)
	var runIDCheckResult *CheckResult
	if len(currentExecution.CurrentRunID) == 0 {
		// this is to set the current run ID prior to the check and fix operations
		currentExecution, runIDCheckResult = c.validateCurrentRunID(currentExecution)
		if runIDCheckResult != nil {
			return FixResult{
				FixResultType: FixResultTypeSkipped,
				CheckResult:   *runIDCheckResult,
				InvariantType: c.Name(),
			}
		}
	}
	fixResult, checkResult := checkBeforeFix(c, currentExecution)
	if fixResult != nil {
		return *fixResult
	}
	if err := c.pr.DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
		DomainID:   currentExecution.DomainID,
		WorkflowID: currentExecution.WorkflowID,
		RunID:      currentExecution.CurrentRunID,
	}); err != nil {
		return FixResult{
			FixResultType: FixResultTypeFailed,
			Info:          "failed to delete current workflow execution",
			InfoDetails:   err.Error(),
		}
	}
	return FixResult{
		FixResultType: FixResultTypeFixed,
		CheckResult:   *checkResult,
		InvariantType: c.Name(),
	}
}

func (c *concreteExecutionExists) Name() Name {
	return ConcreteExecutionExists
}

func (c *concreteExecutionExists) validateCurrentRunID(
	currentExecution *entity.CurrentExecution,
) (*entity.CurrentExecution, *CheckResult) {

	resp, err := c.pr.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   currentExecution.DomainID,
		WorkflowID: currentExecution.WorkflowID,
	})
	if err != nil {
		switch err.(type) {
		case *shared.EntityNotExistsError:
			return nil, &CheckResult{
				CheckResultType: CheckResultTypeHealthy,
				InvariantName:   c.Name(),
				Info:            "current execution does not exist.",
				InfoDetails:     err.Error(),
			}
		default:
			return nil, &CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   c.Name(),
				Info:            "failed to get current execution.",
				InfoDetails:     err.Error(),
			}
		}
	}

	if len(currentExecution.CurrentRunID) == 0 {
		currentExecution.CurrentRunID = resp.RunID
	}

	if currentExecution.CurrentRunID != resp.RunID {
		return nil, &CheckResult{
			CheckResultType: CheckResultTypeHealthy,
			InvariantName:   c.Name(),
		}
	}
	return currentExecution, nil
}
