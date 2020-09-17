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

package persistence

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
)

// Retryer is used to retry requests to persistence with provided retry policy
type Retryer interface {
	ListConcreteExecutions(*ListConcreteExecutionsRequest) (*ListConcreteExecutionsResponse, error)
	ListCurrentExecutions(request *ListCurrentExecutionsRequest) (*ListCurrentExecutionsResponse, error)
	GetWorkflowExecution(*GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error)
	GetCurrentExecution(*GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error)
	IsWorkflowExecutionExists(request *IsWorkflowExecutionExistsRequest) (*IsWorkflowExecutionExistsResponse, error)
	ReadHistoryBranch(*ReadHistoryBranchRequest) (*ReadHistoryBranchResponse, error)
	DeleteWorkflowExecution(*DeleteWorkflowExecutionRequest) error
	DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error
	GetShardID() int
}

type (
	persistenceRetryer struct {
		execManager    ExecutionManager
		historyManager HistoryManager
		policy         backoff.RetryPolicy
	}
)

// NewPersistenceRetryer constructs a new Retryer
func NewPersistenceRetryer(
	execManager ExecutionManager,
	historyManager HistoryManager,
	policy backoff.RetryPolicy,
) Retryer {
	return &persistenceRetryer{
		execManager:    execManager,
		historyManager: historyManager,
		policy:         policy,
	}
}

// ListConcreteExecutions retries ListConcreteExecutions
func (pr *persistenceRetryer) ListConcreteExecutions(
	req *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	var resp *ListConcreteExecutionsResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.ListConcreteExecutions(req)
		return err
	}
	var err error
	err = backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
	if err == nil {
		return resp, nil
	}
	return nil, err
}

// GetWorkflowExecution retries GetWorkflowExecution
func (pr *persistenceRetryer) GetWorkflowExecution(
	req *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	var resp *GetWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.GetWorkflowExecution(req)
		return err
	}
	err := backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetCurrentExecution retries GetCurrentExecution
func (pr *persistenceRetryer) GetCurrentExecution(
	req *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	var resp *GetCurrentExecutionResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.GetCurrentExecution(req)
		return err
	}
	err := backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListCurrentExecutions retries ListCurrentExecutions
func (pr *persistenceRetryer) ListCurrentExecutions(
	req *ListCurrentExecutionsRequest,
) (*ListCurrentExecutionsResponse, error) {
	var resp *ListCurrentExecutionsResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.ListCurrentExecutions(req)
		return err
	}
	var err error
	err = backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
	if err == nil {
		return resp, nil
	}
	return nil, err
}

// IsWorkflowExecutionExists retries IsWorkflowExecutionExists
func (pr *persistenceRetryer) IsWorkflowExecutionExists(
	req *IsWorkflowExecutionExistsRequest,
) (*IsWorkflowExecutionExistsResponse, error) {
	var resp *IsWorkflowExecutionExistsResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.IsWorkflowExecutionExists(req)
		return err
	}
	err := backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ReadHistoryBranch retries ReadHistoryBranch
func (pr *persistenceRetryer) ReadHistoryBranch(
	req *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	var resp *ReadHistoryBranchResponse
	op := func() error {
		var err error
		resp, err = pr.historyManager.ReadHistoryBranch(req)
		return err
	}
	err := backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteWorkflowExecution retries DeleteWorkflowExecution
func (pr *persistenceRetryer) DeleteWorkflowExecution(
	req *DeleteWorkflowExecutionRequest,
) error {
	op := func() error {
		return pr.execManager.DeleteWorkflowExecution(req)
	}
	return backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
}

// DeleteCurrentWorkflowExecution retries DeleteCurrentWorkflowExecution
func (pr *persistenceRetryer) DeleteCurrentWorkflowExecution(
	req *DeleteCurrentWorkflowExecutionRequest,
) error {
	op := func() error {
		return pr.execManager.DeleteCurrentWorkflowExecution(req)
	}
	return backoff.Retry(op, pr.policy, common.IsPersistenceTransientError)
}

// GetShardID return shard id
func (pr *persistenceRetryer) GetShardID() int {
	return pr.execManager.GetShardID()
}
