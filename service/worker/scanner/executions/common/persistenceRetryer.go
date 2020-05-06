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
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/persistence"
)

type (
	persistenceRetryer struct {
		execManager    persistence.ExecutionManager
		historyManager persistence.HistoryManager
	}
)

var (
	retryPolicy = common.CreatePersistanceRetryPolicy()
)

// NewPersistenceRetryer constructs a new PersistenceRetryer
func NewPersistenceRetryer(
	execManager persistence.ExecutionManager,
	historyManager persistence.HistoryManager,
) PersistenceRetryer {
	return &persistenceRetryer{
		execManager:    execManager,
		historyManager: historyManager,
	}
}

// ListConcreteExecutions retries ListConcreteExecutions
func (pr *persistenceRetryer) ListConcreteExecutions(
	req *persistence.ListConcreteExecutionsRequest,
) (*persistence.ListConcreteExecutionsResponse, error) {
	var resp *persistence.ListConcreteExecutionsResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.ListConcreteExecutions(req)
		return err
	}
	var err error
	err = backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err == nil {
		return resp, nil
	}
	return nil, err
}

// GetWorkflowExecution retries GetWorkflowExecution
func (pr *persistenceRetryer) GetWorkflowExecution(
	req *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {
	var resp *persistence.GetWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.GetWorkflowExecution(req)
		return err
	}
	err := backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetCurrentExecution retries GetCurrentExecution
func (pr *persistenceRetryer) GetCurrentExecution(
	req *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {
	var resp *persistence.GetCurrentExecutionResponse
	op := func() error {
		var err error
		resp, err = pr.execManager.GetCurrentExecution(req)
		return err
	}
	err := backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ReadHistoryBranch retries ReadHistoryBranch
func (pr *persistenceRetryer) ReadHistoryBranch(
	req *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	var resp *persistence.ReadHistoryBranchResponse
	op := func() error {
		var err error
		resp, err = pr.historyManager.ReadHistoryBranch(req)
		return err
	}
	err := backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteWorkflowExecution retries DeleteWorkflowExecution
func (pr *persistenceRetryer) DeleteWorkflowExecution(
	req *persistence.DeleteWorkflowExecutionRequest,
) error {
	op := func() error {
		return pr.execManager.DeleteWorkflowExecution(req)
	}
	return backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
}

// DeleteCurrentWorkflowExecution retries DeleteCurrentWorkflowExecution
func (pr *persistenceRetryer) DeleteCurrentWorkflowExecution(
	req *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	op := func() error {
		return pr.execManager.DeleteCurrentWorkflowExecution(req)
	}
	return backoff.Retry(op, retryPolicy, common.IsPersistenceTransientError)
}
