// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inp.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inp.
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

package persistence

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
)

type (
	shardRetryablePersistenceClient struct {
		persistence ShardManager
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}

	executionRetryablePersistenceClient struct {
		persistence ExecutionManager
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}

	taskRetryablePersistenceClient struct {
		persistence TaskManager
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}

	metadataRetryablePersistenceClient struct {
		persistence MetadataManager
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}

	clusterMetadataRetryablePersistenceClient struct {
		persistence ClusterMetadataManager
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}

	queueRetryablePersistenceClient struct {
		persistence Queue
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}

	nexusEndpointRetryablePersistenceClient struct {
		persistence NexusEndpointManager
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}
)

var _ ShardManager = (*shardRetryablePersistenceClient)(nil)
var _ ExecutionManager = (*executionRetryablePersistenceClient)(nil)
var _ TaskManager = (*taskRetryablePersistenceClient)(nil)
var _ MetadataManager = (*metadataRetryablePersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataRetryablePersistenceClient)(nil)
var _ Queue = (*queueRetryablePersistenceClient)(nil)
var _ NexusEndpointManager = (*nexusEndpointRetryablePersistenceClient)(nil)

// NewShardPersistenceRetryableClient creates a client to manage shards
func NewShardPersistenceRetryableClient(
	persistence ShardManager,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) ShardManager {
	return &shardRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

// NewExecutionPersistenceRetryableClient creates a client to manage executions
func NewExecutionPersistenceRetryableClient(
	persistence ExecutionManager,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) ExecutionManager {
	return &executionRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

// NewTaskPersistenceRetryableClient creates a client to manage tasks
func NewTaskPersistenceRetryableClient(
	persistence TaskManager,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) TaskManager {
	return &taskRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

// NewMetadataPersistenceRetryableClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceRetryableClient(
	persistence MetadataManager,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) MetadataManager {
	return &metadataRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

// NewClusterMetadataPersistenceRetryableClient creates a ClusterMetadataManager client to manage cluster metadata
func NewClusterMetadataPersistenceRetryableClient(
	persistence ClusterMetadataManager,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) ClusterMetadataManager {
	return &clusterMetadataRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

// NewQueuePersistenceRetryableClient creates a client to manage queue
func NewQueuePersistenceRetryableClient(
	persistence Queue,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) Queue {
	return &queueRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

// NewNexusEndpointPersistenceRetryableClient creates a NexusEndpointManager client to manage nexus endpoints
func NewNexusEndpointPersistenceRetryableClient(
	persistence NexusEndpointManager,
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) NexusEndpointManager {
	return &nexusEndpointRetryablePersistenceClient{
		persistence: persistence,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (p *shardRetryablePersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardRetryablePersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	var response *GetOrCreateShardResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetOrCreateShard(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *shardRetryablePersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.UpdateShard(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *shardRetryablePersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.AssertShardOwnership(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *shardRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionRetryablePersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionRetryablePersistenceClient) GetHistoryBranchUtil() HistoryBranchUtil {
	return p.persistence.GetHistoryBranchUtil()
}

func (p *executionRetryablePersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	var response *CreateWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CreateWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	var response *GetWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	var response *SetWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.SetWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	var response *UpdateWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.UpdateWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	var response *ConflictResolveWorkflowExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ConflictResolveWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteWorkflowExecution(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	var response *GetCurrentExecutionResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetCurrentExecution(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	var response *ListConcreteExecutionsResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ListConcreteExecutions(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.AddHistoryTasks(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	var response *GetHistoryTasksResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetHistoryTasks(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.CompleteHistoryTask(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.RangeCompleteHistoryTasks(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.PutReplicationTaskToDLQ(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	var response *GetHistoryTasksResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetReplicationTasksFromDLQ(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *executionRetryablePersistenceClient) IsReplicationDLQEmpty(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (bool, error) {
	var isEmpty bool
	op := func(ctx context.Context) error {
		var err error
		isEmpty, err = p.persistence.IsReplicationDLQEmpty(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return isEmpty, err
}

// AppendHistoryNodes add a node to history node table
func (p *executionRetryablePersistenceClient) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	var response *AppendHistoryNodesResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.AppendHistoryNodes(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionRetryablePersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	var response *AppendHistoryNodesResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.AppendRawHistoryNodes(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionRetryablePersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	var response *ReadHistoryBranchResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ReadHistoryBranch(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionRetryablePersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	var response *ReadHistoryBranchReverseResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ReadHistoryBranchReverse(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *executionRetryablePersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	var response *ReadHistoryBranchByBatchResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ReadHistoryBranchByBatch(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *executionRetryablePersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	var response *ReadRawHistoryBranchResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ReadRawHistoryBranch(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *executionRetryablePersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	var response *ForkHistoryBranchResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ForkHistoryBranch(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *executionRetryablePersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteHistoryBranch(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

// TrimHistoryBranch trims a branch
func (p *executionRetryablePersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	var response *TrimHistoryBranchResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.TrimHistoryBranch(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	var response *GetAllHistoryTreeBranchesResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetAllHistoryTreeBranches(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *executionRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskRetryablePersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskRetryablePersistenceClient) CreateTasks(
	ctx context.Context,
	request *CreateTasksRequest,
) (*CreateTasksResponse, error) {
	var response *CreateTasksResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CreateTasks(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	var response *GetTasksResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetTasks(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	var response int
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CompleteTasksLessThan(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	var response *CreateTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CreateTaskQueue(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	var response *UpdateTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.UpdateTaskQueue(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	var response *GetTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetTaskQueue(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	var response *ListTaskQueueResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ListTaskQueue(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteTaskQueue(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *taskRetryablePersistenceClient) GetTaskQueueUserData(
	ctx context.Context,
	request *GetTaskQueueUserDataRequest,
) (*GetTaskQueueUserDataResponse, error) {
	var response *GetTaskQueueUserDataResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetTaskQueueUserData(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) UpdateTaskQueueUserData(
	ctx context.Context,
	request *UpdateTaskQueueUserDataRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.UpdateTaskQueueUserData(ctx, request)
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return err
}

func (p *taskRetryablePersistenceClient) ListTaskQueueUserDataEntries(
	ctx context.Context,
	request *ListTaskQueueUserDataEntriesRequest,
) (*ListTaskQueueUserDataEntriesResponse, error) {
	var response *ListTaskQueueUserDataEntriesResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ListTaskQueueUserDataEntries(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) GetTaskQueuesByBuildId(ctx context.Context, request *GetTaskQueuesByBuildIdRequest) ([]string, error) {
	var response []string
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetTaskQueuesByBuildId(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) CountTaskQueuesByBuildId(ctx context.Context, request *CountTaskQueuesByBuildIdRequest) (int, error) {
	var response int
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CountTaskQueuesByBuildId(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *taskRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataRetryablePersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataRetryablePersistenceClient) CreateNamespace(
	ctx context.Context,
	request *CreateNamespaceRequest,
) (*CreateNamespaceResponse, error) {
	var response *CreateNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CreateNamespace(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *metadataRetryablePersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	var response *GetNamespaceResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetNamespace(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *metadataRetryablePersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.UpdateNamespace(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *metadataRetryablePersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.RenameNamespace(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *metadataRetryablePersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteNamespace(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *metadataRetryablePersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteNamespaceByName(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *metadataRetryablePersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	var response *ListNamespacesResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ListNamespaces(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *metadataRetryablePersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	var response *GetMetadataResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetMetadata(ctx)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *metadataRetryablePersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *metadataRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *clusterMetadataRetryablePersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *clusterMetadataRetryablePersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	var response *GetClusterMembersResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetClusterMembers(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *clusterMetadataRetryablePersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.UpsertClusterMembership(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *clusterMetadataRetryablePersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.PruneClusterMembership(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *clusterMetadataRetryablePersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	var response *ListClusterMetadataResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ListClusterMetadata(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *clusterMetadataRetryablePersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	var response *GetClusterMetadataResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetCurrentClusterMetadata(ctx)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *clusterMetadataRetryablePersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	var response *GetClusterMetadataResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetClusterMetadata(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *clusterMetadataRetryablePersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	var response bool
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.SaveClusterMetadata(ctx, request)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *clusterMetadataRetryablePersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteClusterMetadata(ctx, request)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *clusterMetadataRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *queueRetryablePersistenceClient) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.Init(ctx, blob)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *queueRetryablePersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.EnqueueMessage(ctx, blob)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *queueRetryablePersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	var response []*QueueMessage
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *queueRetryablePersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.UpdateAckLevel(ctx, metadata)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *queueRetryablePersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	var response *InternalQueueMetadata
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetAckLevels(ctx)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *queueRetryablePersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteMessagesBefore(ctx, messageID)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *queueRetryablePersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	var response int64
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.EnqueueMessageToDLQ(ctx, blob)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *queueRetryablePersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*QueueMessage, []byte, error) {
	var messages []*QueueMessage
	var nextPageToken []byte
	op := func(ctx context.Context) error {
		var err error
		messages, nextPageToken, err = p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return messages, nextPageToken, err
}

func (p *queueRetryablePersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}
func (p *queueRetryablePersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.UpdateDLQAckLevel(ctx, metadata)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *queueRetryablePersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	var response *InternalQueueMetadata
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetDLQAckLevels(ctx)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *queueRetryablePersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteMessageFromDLQ(ctx, messageID)
	}

	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}

func (p *queueRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *nexusEndpointRetryablePersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *nexusEndpointRetryablePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *nexusEndpointRetryablePersistenceClient) GetNexusEndpoint(
	ctx context.Context,
	request *GetNexusEndpointRequest,
) (*persistencepb.NexusEndpointEntry, error) {
	var response *persistencepb.NexusEndpointEntry
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.GetNexusEndpoint(ctx, request)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *nexusEndpointRetryablePersistenceClient) ListNexusEndpoints(
	ctx context.Context,
	request *ListNexusEndpointsRequest,
) (*ListNexusEndpointsResponse, error) {
	var response *ListNexusEndpointsResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.ListNexusEndpoints(ctx, request)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *nexusEndpointRetryablePersistenceClient) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *CreateOrUpdateNexusEndpointRequest,
) (*CreateOrUpdateNexusEndpointResponse, error) {
	var response *CreateOrUpdateNexusEndpointResponse
	op := func(ctx context.Context) error {
		var err error
		response, err = p.persistence.CreateOrUpdateNexusEndpoint(ctx, request)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
	return response, err
}

func (p *nexusEndpointRetryablePersistenceClient) DeleteNexusEndpoint(
	ctx context.Context,
	request *DeleteNexusEndpointRequest,
) error {
	op := func(ctx context.Context) error {
		return p.persistence.DeleteNexusEndpoint(ctx, request)
	}
	return backoff.ThrottleRetryContext(ctx, op, p.policy, p.isRetryable)
}
