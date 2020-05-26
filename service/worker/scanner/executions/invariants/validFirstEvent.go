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
	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type (
	validFirstEvent struct {
		pr common.PersistenceRetryer
	}
)

// NewValidFirstEvent returns a new invariant for checking that first event of history is valid
func NewValidFirstEvent(
	pr common.PersistenceRetryer,
) common.Invariant {
	return &validFirstEvent{
		pr: pr,
	}
}

func (v *validFirstEvent) Check(_ common.Execution, resources *common.InvariantResourceBag) common.CheckResult {
	firstEvent := resources.History.HistoryEvents[0]
	if firstEvent.GetEventId() != c.FirstEventID {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeCorrupted,
			Info:            "got unexpected first eventID",
			InfoDetails:     fmt.Sprintf("expected %v but got %v", c.FirstEventID, firstEvent.GetEventId()),
		}
	}
	if firstEvent.GetEventType() != shared.EventTypeWorkflowExecutionStarted {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeCorrupted,
			Info:            "got unexpected first event type",
			InfoDetails:     fmt.Sprintf("expected %v but got %v", shared.EventTypeWorkflowExecutionStarted, firstEvent.GetEventType()),
		}
	}
	return common.CheckResult{
		CheckResultType: common.CheckResultTypeHealthy,
	}
}

func (v *validFirstEvent) Fix(execution common.Execution, resources *common.InvariantResourceBag) common.FixResult {
	fixResult, checkResult := checkBeforeFix(v, execution, resources)
	if fixResult != nil {
		return *fixResult
	}
	fixResult = common.DeleteExecution(&execution, v.pr)
	fixResult.CheckResult = *checkResult
	return *fixResult
}

func (v *validFirstEvent) InvariantType() common.InvariantType {
	return common.ValidFirstEventInvariantType
}
