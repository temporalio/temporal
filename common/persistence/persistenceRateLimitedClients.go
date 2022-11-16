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

package persistence

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/tasks"
)

const (
	RateLimitDefaultToken = 1
)

var (
	// ErrPersistenceLimitExceeded is the error indicating QPS limit reached.
	ErrPersistenceLimitExceeded = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "Persistence Max QPS Reached.")
)

type (
	shardRateLimitedPersistenceClient struct {
		rateLimiter quotas.RequestRateLimiter
		persistence ShardManager
		logger      log.Logger
	}

	executionRateLimitedPersistenceClient struct {
		rateLimiter quotas.RequestRateLimiter
		persistence ExecutionManager
		logger      log.Logger
	}

	taskRateLimitedPersistenceClient struct {
		rateLimiter quotas.RequestRateLimiter
		persistence TaskManager
		logger      log.Logger
	}

	metadataRateLimitedPersistenceClient struct {
		rateLimiter quotas.RequestRateLimiter
		persistence MetadataManager
		logger      log.Logger
	}

	clusterMetadataRateLimitedPersistenceClient struct {
		rateLimiter quotas.RequestRateLimiter
		persistence ClusterMetadataManager
		logger      log.Logger
	}

	queueRateLimitedPersistenceClient struct {
		rateLimiter quotas.RequestRateLimiter
		persistence Queue
		logger      log.Logger
	}
)

var _ ShardManager = (*shardRateLimitedPersistenceClient)(nil)
var _ ExecutionManager = (*executionRateLimitedPersistenceClient)(nil)
var _ TaskManager = (*taskRateLimitedPersistenceClient)(nil)
var _ MetadataManager = (*metadataRateLimitedPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataRateLimitedPersistenceClient)(nil)
var _ Queue = (*queueRateLimitedPersistenceClient)(nil)

// NewShardPersistenceRateLimitedClient creates a client to manage shards
func NewShardPersistenceRateLimitedClient(persistence ShardManager, rateLimiter quotas.RequestRateLimiter, logger log.Logger) ShardManager {
	return &shardRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewExecutionPersistenceRateLimitedClient creates a client to manage executions
func NewExecutionPersistenceRateLimitedClient(persistence ExecutionManager, rateLimiter quotas.RequestRateLimiter, logger log.Logger) ExecutionManager {
	return &executionRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewTaskPersistenceRateLimitedClient creates a client to manage tasks
func NewTaskPersistenceRateLimitedClient(persistence TaskManager, rateLimiter quotas.RequestRateLimiter, logger log.Logger) TaskManager {
	return &taskRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewMetadataPersistenceRateLimitedClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceRateLimitedClient(persistence MetadataManager, rateLimiter quotas.RequestRateLimiter, logger log.Logger) MetadataManager {
	return &metadataRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewClusterMetadataPersistenceRateLimitedClient creates a MetadataManager client to manage metadata
func NewClusterMetadataPersistenceRateLimitedClient(persistence ClusterMetadataManager, rateLimiter quotas.RequestRateLimiter, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewQueuePersistenceRateLimitedClient creates a client to manage queue
func NewQueuePersistenceRateLimitedClient(persistence Queue, rateLimiter quotas.RequestRateLimiter, logger log.Logger) Queue {
	return &queueRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

func (p *shardRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardRateLimitedPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	if ok := allow(ctx, "GetOrCreateShard", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetOrCreateShard(ctx, request)
	return response, err
}

func (p *shardRateLimitedPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	if ok := allow(ctx, "UpdateShard", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateShard(ctx, request)
	return err
}

func (p *shardRateLimitedPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	if ok := allow(ctx, "AssertShardOwnership", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.AssertShardOwnership(ctx, request)
	return err
}

func (p *shardRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionRateLimitedPersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	if ok := allow(ctx, "CreateWorkflowExecution", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	if ok := allow(ctx, "GetWorkflowExecution", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	if ok := allow(ctx, "SetWorkflowExecution", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	if ok := allow(ctx, "UpdateWorkflowExecution", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	resp, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	return resp, err
}

func (p *executionRateLimitedPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	if ok := allow(ctx, "ConflictResolveWorkflowExecution", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	if ok := allow(ctx, "DeleteWorkflowExecution", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteWorkflowExecution(ctx, request)
	return err
}

func (p *executionRateLimitedPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	if ok := allow(ctx, "DeleteCurrentWorkflowExecution", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	return err
}

func (p *executionRateLimitedPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	if ok := allow(ctx, "GetCurrentExecution", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetCurrentExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	if ok := allow(ctx, "ListConcreteExecutions", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	if ok := allow(ctx, "AddHistoryTasks", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.AddHistoryTasks(ctx, request)
	return err
}

func (p *executionRateLimitedPersistenceClient) GetHistoryTask(
	ctx context.Context,
	request *GetHistoryTaskRequest,
) (*GetHistoryTaskResponse, error) {
	if ok := allow(
		ctx,
		ConstructHistoryTaskAPI("GetHistoryTask", request.TaskCategory),
		p.rateLimiter,
	); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetHistoryTask(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	if ok := allow(
		ctx,
		ConstructHistoryTaskAPI("GetHistoryTasks", request.TaskCategory),
		p.rateLimiter,
	); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetHistoryTasks(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	if ok := allow(
		ctx,
		ConstructHistoryTaskAPI("CompleteHistoryTask", request.TaskCategory),
		p.rateLimiter,
	); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteHistoryTask(ctx, request)
	return err
}

func (p *executionRateLimitedPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	if ok := allow(
		ctx,
		ConstructHistoryTaskAPI("RangeCompleteHistoryTasks", request.TaskCategory),
		p.rateLimiter,
	); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RangeCompleteHistoryTasks(ctx, request)
	return err
}

func (p *executionRateLimitedPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	if ok := allow(ctx, "PutReplicationTaskToDLQ", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.PutReplicationTaskToDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	if ok := allow(ctx, "GetReplicationTasksFromDLQ", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.GetReplicationTasksFromDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	if ok := allow(ctx, "DeleteReplicationTaskFromDLQ", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	if ok := allow(ctx, "RangeDeleteReplicationTaskFromDLQ", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskRateLimitedPersistenceClient) CreateTasks(
	ctx context.Context,
	request *CreateTasksRequest,
) (*CreateTasksResponse, error) {
	if ok := allow(ctx, "CreateTasks", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateTasks(ctx, request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	if ok := allow(ctx, "GetTasks", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetTasks(ctx, request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) error {
	if ok := allow(ctx, "CompleteTask", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTask(ctx, request)
	return err
}

func (p *taskRateLimitedPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	if ok := allow(ctx, "CompleteTasksLessThan", p.rateLimiter); !ok {
		return 0, ErrPersistenceLimitExceeded
	}
	return p.persistence.CompleteTasksLessThan(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	if ok := allow(ctx, "CreateTaskQueue", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.CreateTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	if ok := allow(ctx, "UpdateTaskQueue", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.UpdateTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	if ok := allow(ctx, "GetTaskQueue", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.GetTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	if ok := allow(ctx, "ListTaskQueue", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.ListTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	if ok := allow(ctx, "DeleteTaskQueue", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}
	return p.persistence.DeleteTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataRateLimitedPersistenceClient) CreateNamespace(
	ctx context.Context,
	request *CreateNamespaceRequest,
) (*CreateNamespaceResponse, error) {
	if ok := allow(ctx, "CreateNamespace", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateNamespace(ctx, request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	if ok := allow(ctx, "GetNamespace", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetNamespace(ctx, request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	if ok := allow(ctx, "UpdateNamespace", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateNamespace(ctx, request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	if ok := allow(ctx, "RenameNamespace", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RenameNamespace(ctx, request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	if ok := allow(ctx, "DeleteNamespace", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteNamespace(ctx, request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	if ok := allow(ctx, "DeleteNamespaceByName", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteNamespaceByName(ctx, request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	if ok := allow(ctx, "ListNamespaces", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListNamespaces(ctx, request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	if ok := allow(ctx, "GetMetadata", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetMetadata(ctx)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	if ok := allow(ctx, "InitializeSystemNamespaces", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}
	return p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
}

func (p *metadataRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

// AppendHistoryNodes add a node to history node table
func (p *executionRateLimitedPersistenceClient) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	if ok := allow(ctx, "AppendHistoryNodes", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.AppendHistoryNodes(ctx, request)
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionRateLimitedPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	if ok := allow(ctx, "AppendRawHistoryNodes", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.AppendRawHistoryNodes(ctx, request)
}

// ParseHistoryBranchInfo parses the history branch for branch information
func (p *executionRateLimitedPersistenceClient) ParseHistoryBranchInfo(
	ctx context.Context,
	request *ParseHistoryBranchInfoRequest,
) (*ParseHistoryBranchInfoResponse, error) {
	// ParseHistoryBranchInfo implementation currently doesn't actually query DB
	// TODO: uncomment if necessary
	// if ok := allow(ctx, "ParseHistoryBranchInfo", p.rateLimiter); !ok {
	// 	return nil, ErrPersistenceLimitExceeded
	// }
	return p.persistence.ParseHistoryBranchInfo(ctx, request)
}

// UpdateHistoryBranchInfo updates the history branch with branch information
func (p *executionRateLimitedPersistenceClient) UpdateHistoryBranchInfo(
	ctx context.Context,
	request *UpdateHistoryBranchInfoRequest,
) (*UpdateHistoryBranchInfoResponse, error) {
	// UpdateHistoryBranchInfo implementation currently doesn't actually query DB
	// TODO: uncomment if necessary
	// if ok := allow(ctx, "UpdateHistoryBranchInfo", p.rateLimiter); !ok {
	// 	return nil, ErrPersistenceLimitExceeded
	// }
	return p.persistence.UpdateHistoryBranchInfo(ctx, request)
}

// NewHistoryBranch initializes a new history branch
func (p *executionRateLimitedPersistenceClient) NewHistoryBranch(
	ctx context.Context,
	request *NewHistoryBranchRequest,
) (*NewHistoryBranchResponse, error) {
	// NewHistoryBranch implementation currently doesn't actually query DB
	// TODO: uncomment if necessary
	// if ok := allow(ctx, "NewHistoryBranch", p.rateLimiter); !ok {
	// 	return nil, ErrPersistenceLimitExceeded
	// }
	return p.persistence.NewHistoryBranch(ctx, request)
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	if ok := allow(ctx, "ReadHistoryBranch", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	return response, err
}

// ReadHistoryBranchReverse returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	if ok := allow(ctx, "ReadHistoryBranchReverse", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	if ok := allow(ctx, "ReadHistoryBranchByBatch", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	if ok := allow(ctx, "ReadRawHistoryBranch", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *executionRateLimitedPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	if ok := allow(ctx, "ForkHistoryBranch", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *executionRateLimitedPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	if ok := allow(ctx, "DeleteHistoryBranch", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}
	err := p.persistence.DeleteHistoryBranch(ctx, request)
	return err
}

// TrimHistoryBranch trims a branch
func (p *executionRateLimitedPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	if ok := allow(ctx, "TrimHistoryBranch", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	resp, err := p.persistence.TrimHistoryBranch(ctx, request)
	return resp, err
}

// GetHistoryTree returns all branch information of a tree
func (p *executionRateLimitedPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {
	if ok := allow(ctx, "GetHistoryTree", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.GetHistoryTree(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	if ok := allow(ctx, "GetAllHistoryTreeBranches", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	return response, err
}

func (p *queueRateLimitedPersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob commonpb.DataBlob,
) error {
	if ok := allow(ctx, "EnqueueMessage", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.EnqueueMessage(ctx, blob)
}

func (p *queueRateLimitedPersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	if ok := allow(ctx, "ReadMessages", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
}

func (p *queueRateLimitedPersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	if ok := allow(ctx, "UpdateAckLevel", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.UpdateAckLevel(ctx, metadata)
}

func (p *queueRateLimitedPersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	if ok := allow(ctx, "GetAckLevels", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.GetAckLevels(ctx)
}

func (p *queueRateLimitedPersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	if ok := allow(ctx, "DeleteMessagesBefore", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.DeleteMessagesBefore(ctx, messageID)
}

func (p *queueRateLimitedPersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	if ok := allow(ctx, "EnqueueMessageToDLQ", p.rateLimiter); !ok {
		return EmptyQueueMessageID, ErrPersistenceLimitExceeded
	}

	return p.persistence.EnqueueMessageToDLQ(ctx, blob)
}

func (p *queueRateLimitedPersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*QueueMessage, []byte, error) {
	if ok := allow(ctx, "ReadMessagesFromDLQ", p.rateLimiter); !ok {
		return nil, nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
}

func (p *queueRateLimitedPersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	if ok := allow(ctx, "RangeDeleteMessagesFromDLQ", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
}
func (p *queueRateLimitedPersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	if ok := allow(ctx, "UpdateDLQAckLevel", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.UpdateDLQAckLevel(ctx, metadata)
}

func (p *queueRateLimitedPersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	if ok := allow(ctx, "GetDLQAckLevels", p.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.GetDLQAckLevels(ctx)
}

func (p *queueRateLimitedPersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	if ok := allow(ctx, "DeleteMessageFromDLQ", p.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.DeleteMessageFromDLQ(ctx, messageID)
}

func (p *queueRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *queueRateLimitedPersistenceClient) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return p.persistence.Init(ctx, blob)
}

func (c *clusterMetadataRateLimitedPersistenceClient) Close() {
	c.persistence.Close()
}

func (c *clusterMetadataRateLimitedPersistenceClient) GetName() string {
	return c.persistence.GetName()
}

func (c *clusterMetadataRateLimitedPersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	if ok := allow(ctx, "GetClusterMembers", c.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return c.persistence.GetClusterMembers(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	if ok := allow(ctx, "UpsertClusterMembership", c.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}
	return c.persistence.UpsertClusterMembership(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	if ok := allow(ctx, "PruneClusterMembership", c.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}
	return c.persistence.PruneClusterMembership(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	if ok := allow(ctx, "ListClusterMetadata", c.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return c.persistence.ListClusterMetadata(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	if ok := allow(ctx, "GetCurrentClusterMetadata", c.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return c.persistence.GetCurrentClusterMetadata(ctx)
}

func (c *clusterMetadataRateLimitedPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	if ok := allow(ctx, "GetClusterMetadata", c.rateLimiter); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return c.persistence.GetClusterMetadata(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	if ok := allow(ctx, "SaveClusterMetadata", c.rateLimiter); !ok {
		return false, ErrPersistenceLimitExceeded
	}
	return c.persistence.SaveClusterMetadata(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	if ok := allow(ctx, "DeleteClusterMetadata", c.rateLimiter); !ok {
		return ErrPersistenceLimitExceeded
	}
	return c.persistence.DeleteClusterMetadata(ctx, request)
}

func allow(
	ctx context.Context,
	api string,
	rateLimiter quotas.RequestRateLimiter,
) bool {
	callerInfo := headers.GetCallerInfo(ctx)
	return rateLimiter.Allow(time.Now().UTC(), quotas.NewRequest(
		api,
		RateLimitDefaultToken,
		callerInfo.CallerName,
		callerInfo.CallerType,
		callerInfo.CallOrigin,
	))
}

// TODO: change the value returned so it can also be used by
// persistence metrics client. For now, it's only used by rate
// limit client, and we don't really care about the actual value
// returned, as long as they are different from each task category.
func ConstructHistoryTaskAPI(
	baseAPI string,
	taskCategory tasks.Category,
) string {
	return baseAPI + taskCategory.Name()
}
