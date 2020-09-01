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

package invariants

import (
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/common"
)

type (
	concreteExecutionExists struct {
		pr common.PersistenceRetryer
	}
)

// NewConcreteExecutionExists returns a new invariant for checking concrete execution
func NewConcreteExecutionExists(
	pr common.PersistenceRetryer,
) common.Invariant {
	return &concreteExecutionExists{
		pr: pr,
	}
}

func (c *concreteExecutionExists) Check(
	execution interface{},
) common.CheckResult {

	currentExecution, ok := execution.(*common.CurrentExecution)
	if !ok {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeFailed,
			InvariantType:   c.InvariantType(),
			Info:            "failed to check: expected current execution",
		}
	}

	if len(currentExecution.CurrentRunID) == 0 {
		// set the current run id
		var runIDCheckResult *common.CheckResult
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
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeFailed,
			InvariantType:   c.InvariantType(),
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
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeCorrupted,
			InvariantType:   c.InvariantType(),
			Info:            "execution is open without having concrete execution",
			InfoDetails: fmt.Sprintf("concrete execution not found. WorkflowId: %v, RunId: %v",
				currentExecution.WorkflowID, currentExecution.CurrentRunID),
		}
	}
	return common.CheckResult{
		CheckResultType: common.CheckResultTypeHealthy,
		InvariantType:   c.InvariantType(),
	}
}

func (c *concreteExecutionExists) Fix(
	execution interface{},
) common.FixResult {

	currentExecution, _ := execution.(*common.CurrentExecution)
	var runIDCheckResult *common.CheckResult
	if len(currentExecution.CurrentRunID) == 0 {
		// this is to set the current run ID prior to the check and fix operations
		currentExecution, runIDCheckResult = c.validateCurrentRunID(currentExecution)
		if runIDCheckResult != nil {
			return common.FixResult{
				FixResultType: common.FixResultTypeSkipped,
				CheckResult:   *runIDCheckResult,
				InvariantType: c.InvariantType(),
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
		return common.FixResult{
			FixResultType: common.FixResultTypeFailed,
			Info:          "failed to delete current workflow execution",
			InfoDetails:   err.Error(),
		}
	}
	return common.FixResult{
		FixResultType: common.FixResultTypeFixed,
		CheckResult:   *checkResult,
		InvariantType: c.InvariantType(),
	}
}

func (c *concreteExecutionExists) InvariantType() common.InvariantType {
	return common.ConcreteExecutionExistsInvariantType
}

func (c *concreteExecutionExists) validateCurrentRunID(
	currentExecution *common.CurrentExecution,
) (*common.CurrentExecution, *common.CheckResult) {

	resp, err := c.pr.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   currentExecution.DomainID,
		WorkflowID: currentExecution.WorkflowID,
	})
	if err != nil {
		switch err.(type) {
		case *shared.EntityNotExistsError:
			return nil, &common.CheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				InvariantType:   c.InvariantType(),
				Info:            "current execution does not exist.",
				InfoDetails:     err.Error(),
			}
		default:
			return nil, &common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   c.InvariantType(),
				Info:            "failed to get current execution.",
				InfoDetails:     err.Error(),
			}
		}
	}

	if len(currentExecution.CurrentRunID) == 0 {
		currentExecution.CurrentRunID = resp.RunID
	}

	if currentExecution.CurrentRunID != resp.RunID {
		return nil, &common.CheckResult{
			CheckResultType: common.CheckResultTypeHealthy,
			InvariantType:   c.InvariantType(),
		}
	}
	return currentExecution, nil
}
