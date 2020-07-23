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
	openCurrentExecution struct {
		pr common.PersistenceRetryer
	}
)

// NewOpenCurrentExecution returns a new invariant for checking open current execution
func NewOpenCurrentExecution(
	pr common.PersistenceRetryer,
) common.Invariant {
	return &openCurrentExecution{
		pr: pr,
	}
}

func (o *openCurrentExecution) Check(execution interface{}) common.CheckResult {
	concreteExecution, ok := execution.(common.ConcreteExecution)
	if !ok {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeFailed,
			InvariantType:   o.InvariantType(),
			Info:            "failed to check: expected concrete execution",
		}
	}
	if !common.Open(concreteExecution.State) {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeHealthy,
			InvariantType:   o.InvariantType(),
		}
	}
	currentExecResp, currentExecErr := o.pr.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   concreteExecution.DomainID,
		WorkflowID: concreteExecution.WorkflowID,
	})
	stillOpen, stillOpenErr := common.ExecutionStillOpen(&concreteExecution.Execution, o.pr)
	if stillOpenErr != nil {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeFailed,
			InvariantType:   o.InvariantType(),
			Info:            "failed to check if concrete execution is still open",
			InfoDetails:     stillOpenErr.Error(),
		}
	}
	if !stillOpen {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeHealthy,
			InvariantType:   o.InvariantType(),
		}
	}
	if currentExecErr != nil {
		switch currentExecErr.(type) {
		case *shared.EntityNotExistsError:
			return common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   o.InvariantType(),
				Info:            "execution is open without having current execution",
				InfoDetails:     currentExecErr.Error(),
			}
		default:
			return common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   o.InvariantType(),
				Info:            "failed to check if current execution exists",
				InfoDetails:     currentExecErr.Error(),
			}
		}
	}
	if currentExecResp.RunID != concreteExecution.RunID {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeCorrupted,
			InvariantType:   o.InvariantType(),
			Info:            "execution is open but current points at a different execution",
			InfoDetails:     fmt.Sprintf("current points at %v", currentExecResp.RunID),
		}
	}
	return common.CheckResult{
		CheckResultType: common.CheckResultTypeHealthy,
		InvariantType:   o.InvariantType(),
	}
}

func (o *openCurrentExecution) Fix(execution interface{}) common.FixResult {
	fixResult, checkResult := checkBeforeFix(o, execution)
	if fixResult != nil {
		return *fixResult
	}
	fixResult = common.DeleteExecution(&execution, o.pr)
	fixResult.CheckResult = *checkResult
	fixResult.InvariantType = o.InvariantType()
	return *fixResult
}

func (o *openCurrentExecution) InvariantType() common.InvariantType {
	return common.OpenCurrentExecutionInvariantType
}
