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
	openCurrentExecution struct {
		pr persistence.Retryer
	}
)

// NewOpenCurrentExecution returns a new invariant for checking open current execution
func NewOpenCurrentExecution(
	pr persistence.Retryer,
) Invariant {
	return &openCurrentExecution{
		pr: pr,
	}
}

func (o *openCurrentExecution) Check(execution interface{}) CheckResult {
	concreteExecution, ok := execution.(*entity.ConcreteExecution)
	if !ok {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   o.Name(),
			Info:            "failed to check: expected concrete execution",
		}
	}
	if !Open(concreteExecution.State) {
		return CheckResult{
			CheckResultType: CheckResultTypeHealthy,
			InvariantName:   o.Name(),
		}
	}
	currentExecResp, currentExecErr := o.pr.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   concreteExecution.DomainID,
		WorkflowID: concreteExecution.WorkflowID,
	})
	stillOpen, stillOpenErr := ExecutionStillOpen(&concreteExecution.Execution, o.pr)
	if stillOpenErr != nil {
		return CheckResult{
			CheckResultType: CheckResultTypeFailed,
			InvariantName:   o.Name(),
			Info:            "failed to check if concrete execution is still open",
			InfoDetails:     stillOpenErr.Error(),
		}
	}
	if !stillOpen {
		return CheckResult{
			CheckResultType: CheckResultTypeHealthy,
			InvariantName:   o.Name(),
		}
	}
	if currentExecErr != nil {
		switch currentExecErr.(type) {
		case *shared.EntityNotExistsError:
			return CheckResult{
				CheckResultType: CheckResultTypeCorrupted,
				InvariantName:   o.Name(),
				Info:            "execution is open without having current execution",
				InfoDetails:     currentExecErr.Error(),
			}
		default:
			return CheckResult{
				CheckResultType: CheckResultTypeFailed,
				InvariantName:   o.Name(),
				Info:            "failed to check if current execution exists",
				InfoDetails:     currentExecErr.Error(),
			}
		}
	}
	if currentExecResp.RunID != concreteExecution.RunID {
		return CheckResult{
			CheckResultType: CheckResultTypeCorrupted,
			InvariantName:   o.Name(),
			Info:            "execution is open but current points at a different execution",
			InfoDetails:     fmt.Sprintf("current points at %v", currentExecResp.RunID),
		}
	}
	return CheckResult{
		CheckResultType: CheckResultTypeHealthy,
		InvariantName:   o.Name(),
	}
}

func (o *openCurrentExecution) Fix(execution interface{}) FixResult {
	fixResult, checkResult := checkBeforeFix(o, execution)
	if fixResult != nil {
		return *fixResult
	}
	fixResult = DeleteExecution(&execution, o.pr)
	fixResult.CheckResult = *checkResult
	fixResult.InvariantType = o.Name()
	return *fixResult
}

func (o *openCurrentExecution) Name() Name {
	return OpenCurrentExecution
}

// ExecutionStillOpen returns true if execution in persistence exists and is open, false otherwise.
// Returns error on failure to confirm.
func ExecutionStillOpen(
	exec *entity.Execution,
	pr persistence.Retryer,
) (bool, error) {
	req := &persistence.GetWorkflowExecutionRequest{
		DomainID: exec.DomainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: &exec.WorkflowID,
			RunId:      &exec.RunID,
		},
	}
	resp, err := pr.GetWorkflowExecution(req)
	if err != nil {
		switch err.(type) {
		case *shared.EntityNotExistsError:
			return false, nil
		default:
			return false, err
		}
	}
	return Open(resp.State.ExecutionInfo.State), nil
}
