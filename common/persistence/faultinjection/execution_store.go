// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

type (
	FaultInjectionExecutionStore struct {
		persistence.HistoryBranchUtilImpl
		baseStore persistence.ExecutionStore
		generator faultGenerator
	}
)

func NewFaultInjectionExecutionStore(
	baseStore persistence.ExecutionStore,
	generator faultGenerator,
) *FaultInjectionExecutionStore {
	return &FaultInjectionExecutionStore{
		baseStore: baseStore,
		generator: generator,
	}
}

func (e *FaultInjectionExecutionStore) Close() {
	e.baseStore.Close()
}

func (e *FaultInjectionExecutionStore) GetName() string {
	return e.baseStore.GetName()
}

func (e *FaultInjectionExecutionStore) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalGetWorkflowExecutionResponse, error) {
		return e.baseStore.GetWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalSetWorkflowExecutionRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.SetWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.UpdateWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalConflictResolveWorkflowExecutionRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.ConflictResolveWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
) (*persistence.InternalCreateWorkflowExecutionResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalCreateWorkflowExecutionResponse, error) {
		return e.baseStore.CreateWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteWorkflowExecutionRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.DeleteWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.DeleteCurrentWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.InternalGetCurrentExecutionResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalGetCurrentExecutionResponse, error) {
		return e.baseStore.GetCurrentExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ListConcreteExecutions(
	ctx context.Context,
	request *persistence.ListConcreteExecutionsRequest,
) (*persistence.InternalListConcreteExecutionsResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalListConcreteExecutionsResponse, error) {
		return e.baseStore.ListConcreteExecutions(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) AddHistoryTasks(
	ctx context.Context,
	request *persistence.InternalAddHistoryTasksRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.AddHistoryTasks(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetHistoryTasks(
	ctx context.Context,
	request *persistence.GetHistoryTasksRequest,
) (*persistence.InternalGetHistoryTasksResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalGetHistoryTasksResponse, error) {
		return e.baseStore.GetHistoryTasks(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) CompleteHistoryTask(
	ctx context.Context,
	request *persistence.CompleteHistoryTaskRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.CompleteHistoryTask(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.RangeCompleteHistoryTasks(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.PutReplicationTaskToDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (*persistence.InternalGetHistoryTasksResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalGetHistoryTasksResponse, error) {
		return e.baseStore.GetReplicationTasksFromDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.DeleteReplicationTaskFromDLQRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.DeleteReplicationTaskFromDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	return inject1(e.generator.generate(), func() (bool, error) {
		return e.baseStore.IsReplicationDLQEmpty(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) AppendHistoryNodes(
	ctx context.Context,
	request *persistence.InternalAppendHistoryNodesRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.AppendHistoryNodes(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteHistoryNodes(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryNodesRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.DeleteHistoryNodes(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.InternalReadHistoryBranchRequest,
) (*persistence.InternalReadHistoryBranchResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalReadHistoryBranchResponse, error) {
		return e.baseStore.ReadHistoryBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ForkHistoryBranch(
	ctx context.Context,
	request *persistence.InternalForkHistoryBranchRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.ForkHistoryBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteHistoryBranch(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryBranchRequest,
) error {
	return inject0(e.generator.generate(), func() error {
		return e.baseStore.DeleteHistoryBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetHistoryTreeContainingBranch(
	ctx context.Context,
	request *persistence.InternalGetHistoryTreeContainingBranchRequest,
) (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
		return e.baseStore.GetHistoryTreeContainingBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *persistence.GetAllHistoryTreeBranchesRequest,
) (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
	return inject1(e.generator.generate(), func() (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
		return e.baseStore.GetAllHistoryTreeBranches(ctx, request)
	})
}
