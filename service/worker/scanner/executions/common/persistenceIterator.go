// The MIT License (MIT)
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package common

import (
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
)

type (
	persistenceIterator struct {
		itr pagination.Iterator
	}
)

// NewPersistenceIterator returns a new paginated iterator over persistence
func NewPersistenceIterator(
	pr PersistenceRetryer,
	pageSize int,
	shardID int,
) ExecutionIterator {
	return &persistenceIterator{
		itr: pagination.NewIterator(nil, getPersistenceFetchPageFn(pr, pageSize, shardID)),
	}
}

// Next returns the next execution
func (i *persistenceIterator) Next() (*Execution, error) {
	exec, err := i.itr.Next()
	if exec != nil {
		return exec.(*Execution), err
	}
	return nil, err
}

// HasNext returns true if there is another execution, false otherwise.
func (i *persistenceIterator) HasNext() bool {
	return i.itr.HasNext()
}

func getPersistenceFetchPageFn(
	pr PersistenceRetryer,
	pageSize int,
	shardID int,
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
			branchToken := e.ExecutionInfo.BranchToken
			if e.VersionHistories != nil {
				versionHistory, err := e.VersionHistories.GetCurrentVersionHistory()
				if err != nil {
					return pagination.Page{}, err
				}
				branchToken = versionHistory.GetBranchToken()
			}
			executions[i] = &Execution{
				ShardID:     shardID,
				DomainID:    e.ExecutionInfo.DomainID,
				WorkflowID:  e.ExecutionInfo.WorkflowID,
				RunID:       e.ExecutionInfo.RunID,
				BranchToken: branchToken,
				State:       e.ExecutionInfo.State,
			}
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
