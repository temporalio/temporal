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

package iterator

import (
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
)

// ConcreteExecution is used to retrieve Concrete executions.
func ConcreteExecution(retryer persistence.Retryer, pageSize int) pagination.Iterator {
	return pagination.NewIterator(nil, getConcreteExecutions(retryer, pageSize, codec.NewThriftRWEncoder()))
}

func getConcreteExecutions(
	pr persistence.Retryer,
	pageSize int,
	encoder *codec.ThriftRWEncoder,
) pagination.FetchFn {
	return func(token pagination.PageToken) (pagination.Page, error) {
		req := &persistence.ListConcreteExecutionsRequest{
			PageSize: pageSize,
		}
		if token != nil {
			req.PageToken = token.([]byte)
		}
		resp, err := pr.ListConcreteExecutions(req)
		if err != nil {
			return pagination.Page{}, err
		}
		executions := make([]pagination.Entity, len(resp.Executions), len(resp.Executions))
		for i, e := range resp.Executions {
			branchToken, treeID, branchID, err := GetBranchToken(e, encoder)
			if err != nil {
				return pagination.Page{}, err
			}
			concreteExec := &entity.ConcreteExecution{
				BranchToken: branchToken,
				TreeID:      treeID,
				BranchID:    branchID,
				Execution: entity.Execution{
					ShardID:    pr.GetShardID(),
					DomainID:   e.ExecutionInfo.DomainID,
					WorkflowID: e.ExecutionInfo.WorkflowID,
					RunID:      e.ExecutionInfo.RunID,
					State:      e.ExecutionInfo.State,
				},
			}
			if err := concreteExec.Validate(); err != nil {
				return pagination.Page{}, err
			}
			executions[i] = concreteExec
		}
		var nextToken interface{} = resp.PageToken
		if len(resp.PageToken) == 0 {
			nextToken = nil
		}
		page := pagination.Page{
			CurrentToken: token,
			NextToken:    nextToken,
			Entities:     executions,
		}
		return page, nil
	}
}

// GetBranchToken returns the branchToken, treeID and branchID or error on failure.
func GetBranchToken(
	entity *persistence.ListConcreteExecutionsEntity,
	decoder *codec.ThriftRWEncoder,
) ([]byte, string, string, error) {
	branchToken := entity.ExecutionInfo.BranchToken
	if entity.VersionHistories != nil {
		versionHistory, err := entity.VersionHistories.GetCurrentVersionHistory()
		if err != nil {
			return nil, "", "", err
		}
		branchToken = versionHistory.GetBranchToken()
	}
	var branch shared.HistoryBranch
	if err := decoder.Decode(branchToken, &branch); err != nil {
		return nil, "", "", err
	}
	return branchToken, branch.GetTreeID(), branch.GetBranchID(), nil
}
