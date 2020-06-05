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
	"github.com/uber/cadence/.gen/go/shared"

	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

const (
	historyPageSize = 1
)

type (
	historyExists struct {
		pr common.PersistenceRetryer
	}
)

// NewHistoryExists returns a new history exists invariant
func NewHistoryExists(
	pr common.PersistenceRetryer,
) common.Invariant {
	return &historyExists{
		pr: pr,
	}
}

func (h *historyExists) Check(execution common.Execution) common.CheckResult {
	readHistoryBranchReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   execution.BranchToken,
		MinEventID:    c.FirstEventID,
		MaxEventID:    c.FirstEventID + 1,
		PageSize:      historyPageSize,
		NextPageToken: nil,
		ShardID:       c.IntPtr(execution.ShardID),
	}
	readHistoryBranchResp, readHistoryBranchErr := h.pr.ReadHistoryBranch(readHistoryBranchReq)
	stillExists, existsCheckError := common.ExecutionStillExists(&execution, h.pr)
	if existsCheckError != nil {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeFailed,
			InvariantType:   h.InvariantType(),
			Info:            "failed to check if concrete execution still exists",
			InfoDetails:     existsCheckError.Error(),
		}
	}
	if !stillExists {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeHealthy,
			InvariantType:   h.InvariantType(),
			Info:            "determined execution was healthy because concrete execution no longer exists",
		}
	}
	if readHistoryBranchErr != nil {
		switch readHistoryBranchErr.(type) {
		case *shared.EntityNotExistsError:
			return common.CheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				InvariantType:   h.InvariantType(),
				Info:            "concrete execution exists but history does not exist",
				InfoDetails:     readHistoryBranchErr.Error(),
			}
		default:
			return common.CheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				InvariantType:   h.InvariantType(),
				Info:            "failed to verify if history exists",
				InfoDetails:     readHistoryBranchErr.Error(),
			}
		}
	}
	if readHistoryBranchResp == nil || len(readHistoryBranchResp.HistoryEvents) == 0 {
		return common.CheckResult{
			CheckResultType: common.CheckResultTypeCorrupted,
			InvariantType:   h.InvariantType(),
			Info:            "concrete execution exists but got empty history",
		}
	}
	return common.CheckResult{
		CheckResultType: common.CheckResultTypeHealthy,
		InvariantType:   h.InvariantType(),
	}
}

func (h *historyExists) Fix(execution common.Execution) common.FixResult {
	fixResult, checkResult := checkBeforeFix(h, execution)
	if fixResult != nil {
		return *fixResult
	}
	fixResult = common.DeleteExecution(&execution, h.pr)
	fixResult.CheckResult = *checkResult
	fixResult.InvariantType = h.InvariantType()
	return *fixResult
}

func (h *historyExists) InvariantType() common.InvariantType {
	return common.HistoryExistsInvariantType
}
