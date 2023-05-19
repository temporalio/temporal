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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
)

type (
	shardHealthSignalPersistenceClient struct {
		healthSignals aggregate.SignalAggregator[quotas.Request]
		persistence   ShardManager
		logger        log.Logger
	}

	executionHealthSignalPersistenceClient struct {
		healthSignals aggregate.SignalAggregator[quotas.Request]
		persistence   ExecutionManager
		logger        log.Logger
	}

	taskHealthSignalPersistenceClient struct {
		healthSignals aggregate.SignalAggregator[quotas.Request]
		persistence   TaskManager
		logger        log.Logger
	}

	metadataHealthSignalPersistenceClient struct {
		healthSignals aggregate.SignalAggregator[quotas.Request]
		persistence   MetadataManager
		logger        log.Logger
	}

	clusterMetadataHealthSignalPersistenceClient struct {
		healthSignals aggregate.SignalAggregator[quotas.Request]
		persistence   ClusterMetadataManager
		logger        log.Logger
	}

	queueHealthSignalPersistenceClient struct {
		healthSignals aggregate.SignalAggregator[quotas.Request]
		persistence   Queue
		logger        log.Logger
	}
)

var _ ShardManager = (*shardHealthSignalPersistenceClient)(nil)
var _ ExecutionManager = (*executionHealthSignalPersistenceClient)(nil)
var _ TaskManager = (*taskHealthSignalPersistenceClient)(nil)
var _ MetadataManager = (*metadataHealthSignalPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataHealthSignalPersistenceClient)(nil)
var _ Queue = (*queueHealthSignalPersistenceClient)(nil)

func NewShardPersistenceHealthSignalClient(persistence ShardManager, healthSignals aggregate.SignalAggregator[quotas.Request], logger log.Logger) ShardManager {
	return &shardHealthSignalPersistenceClient{
		persistence:   persistence,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewExecutionPersistenceHealthSignalClient(persistence ExecutionManager, healthSignals aggregate.SignalAggregator[quotas.Request], logger log.Logger) ExecutionManager {
	return &executionHealthSignalPersistenceClient{
		persistence:   persistence,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewTaskPersistenceHealthSignalClient(persistence TaskManager, healthSignals aggregate.SignalAggregator[quotas.Request], logger log.Logger) TaskManager {
	return &taskHealthSignalPersistenceClient{
		persistence:   persistence,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewMetadataPersistenceHealthSignalClient(persistence MetadataManager, healthSignals aggregate.SignalAggregator[quotas.Request], logger log.Logger) MetadataManager {
	return &metadataHealthSignalPersistenceClient{
		persistence:   persistence,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewClusterMetadataPersistenceHealthSignalClient(persistence ClusterMetadataManager, healthSignals aggregate.SignalAggregator[quotas.Request], logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataHealthSignalPersistenceClient{
		persistence:   persistence,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewQueuePersistenceHealthSignalClient(persistence Queue, healthSignals aggregate.SignalAggregator[quotas.Request], logger log.Logger) Queue {
	return &queueHealthSignalPersistenceClient{
		persistence:   persistence,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func (p *shardHealthSignalPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardHealthSignalPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	record := recordFn(ctx, "GetOrCreateShard", request.ShardID, p.healthSignals)
	response, err := p.persistence.GetOrCreateShard(ctx, request)
	record(err)
	return response, err
}

func (p *shardHealthSignalPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	record := recordFn(ctx, "UpdateShard", request.ShardInfo.GetShardId(), p.healthSignals)
	err := p.persistence.UpdateShard(ctx, request)
	record(err)
	return err
}

func (p *shardHealthSignalPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	record := recordFn(ctx, "AssertShardOwnership", request.ShardID, p.healthSignals)
	err := p.persistence.AssertShardOwnership(ctx, request)
	record(err)
	return err
}

func (p *shardHealthSignalPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionHealthSignalPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionHealthSignalPersistenceClient) GetHistoryBranchUtil() HistoryBranchUtil {
	return p.persistence.GetHistoryBranchUtil()
}

func (p *executionHealthSignalPersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	record := recordFn(ctx, "CreateWorkflowExecution", request.ShardID, p.healthSignals)
	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	record := recordFn(ctx, "GetWorkflowExecution", request.ShardID, p.healthSignals)
	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	record := recordFn(ctx, "SetWorkflowExecution", request.ShardID, p.healthSignals)
	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	record := recordFn(ctx, "UpdateWorkflowExecuton", request.ShardID, p.healthSignals)
	response, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	record := recordFn(ctx, "ConflictResolveWorkflowExecution", request.ShardID, p.healthSignals)
	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	record := recordFn(ctx, "DeleteWorkflowExecution", request.ShardID, p.healthSignals)
	err := p.persistence.DeleteWorkflowExecution(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	record := recordFn(ctx, "DeleteCurrentWorkflowExecution", request.ShardID, p.healthSignals)
	err := p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	record := recordFn(ctx, "GetCurrentExecution", request.ShardID, p.healthSignals)
	response, err := p.persistence.GetCurrentExecution(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	record := recordFn(ctx, "ListConcreteExecutions", request.ShardID, p.healthSignals)
	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) RegisterHistoryTaskReader(
	ctx context.Context,
	request *RegisterHistoryTaskReaderRequest,
) error {
	// hint methods don't actually hint DB, so don't go through persistence rate limiter
	return p.persistence.RegisterHistoryTaskReader(ctx, request)
}

func (p *executionHealthSignalPersistenceClient) UnregisterHistoryTaskReader(
	ctx context.Context,
	request *UnregisterHistoryTaskReaderRequest,
) {
	// hint methods don't actually hint DB, so don't go through persistence rate limiter
	p.persistence.UnregisterHistoryTaskReader(ctx, request)
}

func (p *executionHealthSignalPersistenceClient) UpdateHistoryTaskReaderProgress(
	ctx context.Context,
	request *UpdateHistoryTaskReaderProgressRequest,
) {
	// hint methods don't actually hint DB, so don't go through persistence rate limiter
	p.persistence.UpdateHistoryTaskReaderProgress(ctx, request)
}

func (p *executionHealthSignalPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	record := recordFn(ctx, "AddHistoryTasks", request.ShardID, p.healthSignals)
	err := p.persistence.AddHistoryTasks(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	record := recordFn(ctx, "GetHistoryTasks", request.ShardID, p.healthSignals)
	response, err := p.persistence.GetHistoryTasks(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	record := recordFn(ctx, "CompleteHistoryTask", request.ShardID, p.healthSignals)
	err := p.persistence.CompleteHistoryTask(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	record := recordFn(ctx, "RangeCompleteHistoryTasks", request.ShardID, p.healthSignals)
	err := p.persistence.RangeCompleteHistoryTasks(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	record := recordFn(ctx, "PutReplicationTaskToDLQ", request.ShardID, p.healthSignals)
	err := p.persistence.PutReplicationTaskToDLQ(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	record := recordFn(ctx, "GetReplicationTasksFromDLQ", request.ShardID, p.healthSignals)
	response, err := p.persistence.GetReplicationTasksFromDLQ(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	record := recordFn(ctx, "DeleteReplicationTaskFromDLQ", request.ShardID, p.healthSignals)
	err := p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	record := recordFn(ctx, "RangeDeleteReplicationTaskFromDLQ", request.ShardID, p.healthSignals)
	err := p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) IsReplicationDLQEmpty(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (bool, error) {
	record := recordFn(ctx, "IsReplicationDLQEmpty", request.ShardID, p.healthSignals)
	response, err := p.persistence.IsReplicationDLQEmpty(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	record := recordFn(ctx, "AppendHistoryNodes", request.ShardID, p.healthSignals)
	response, err := p.persistence.AppendHistoryNodes(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	record := recordFn(ctx, "AppendRawHistoryNodes", request.ShardID, p.healthSignals)
	response, err := p.persistence.AppendRawHistoryNodes(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	record := recordFn(ctx, "ReadHistoryBranch", request.ShardID, p.healthSignals)
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	record := recordFn(ctx, "ReadHistoryBranchReverse", request.ShardID, p.healthSignals)
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	record := recordFn(ctx, "ReadHistoryBranchByBatch", request.ShardID, p.healthSignals)
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	record := recordFn(ctx, "ReadRawHistoryBranch", request.ShardID, p.healthSignals)
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	record := recordFn(ctx, "ForkHistoryBranch", request.ShardID, p.healthSignals)
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	record := recordFn(ctx, "DeleteHistoryBranch", request.ShardID, p.healthSignals)
	err := p.persistence.DeleteHistoryBranch(ctx, request)
	record(err)
	return err
}

func (p *executionHealthSignalPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	record := recordFn(ctx, "TrimHistoryBranch", request.ShardID, p.healthSignals)
	resp, err := p.persistence.TrimHistoryBranch(ctx, request)
	record(err)
	return resp, err
}

func (p *executionHealthSignalPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {
	record := recordFn(ctx, "GetHistoryTree", request.ShardID, p.healthSignals)
	response, err := p.persistence.GetHistoryTree(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	record := recordFn(ctx, "GetAllHistoryTreeBranches", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	record(err)
	return response, err
}

func (p *executionHealthSignalPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskHealthSignalPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskHealthSignalPersistenceClient) CreateTasks(
	ctx context.Context,
	request *CreateTasksRequest,
) (*CreateTasksResponse, error) {
	record := recordFn(ctx, "CreateTasks", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.CreateTasks(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	record := recordFn(ctx, "GetTasks", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetTasks(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) error {
	record := recordFn(ctx, "CompleteTask", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.CompleteTask(ctx, request)
	record(err)
	return err
}

func (p *taskHealthSignalPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	record := recordFn(ctx, "CompleteTasksLessThan", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.CompleteTasksLessThan(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	record := recordFn(ctx, "CreateTaskQueue", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.CreateTaskQueue(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	record := recordFn(ctx, "UpdateTaskQueue", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.UpdateTaskQueue(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	record := recordFn(ctx, "GetTaskQueue", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetTaskQueue(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	record := recordFn(ctx, "ListTaskQueue", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.ListTaskQueue(ctx, request)
	record(err)
	return response, err
}

func (p *taskHealthSignalPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	record := recordFn(ctx, "DeleteTaskQueue", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.DeleteTaskQueue(ctx, request)
	record(err)
	return err
}

func (p *taskHealthSignalPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataHealthSignalPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataHealthSignalPersistenceClient) CreateNamespace(
	ctx context.Context,
	request *CreateNamespaceRequest,
) (*CreateNamespaceResponse, error) {
	record := recordFn(ctx, "CreateNamespace", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.CreateNamespace(ctx, request)
	record(err)
	return response, err
}

func (p *metadataHealthSignalPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	record := recordFn(ctx, "GetNamespace", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetNamespace(ctx, request)
	record(err)
	return response, err
}

func (p *metadataHealthSignalPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	record := recordFn(ctx, "UpdateNamespace", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.UpdateNamespace(ctx, request)
	record(err)
	return err
}

func (p *metadataHealthSignalPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	record := recordFn(ctx, "RenameNamespace", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.RenameNamespace(ctx, request)
	record(err)
	return err
}

func (p *metadataHealthSignalPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	record := recordFn(ctx, "DeleteNamespace", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.DeleteNamespace(ctx, request)
	record(err)
	return err
}

func (p *metadataHealthSignalPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	record := recordFn(ctx, "DeleteNamespaceByName", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.DeleteNamespaceByName(ctx, request)
	record(err)
	return err
}

func (p *metadataHealthSignalPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	record := recordFn(ctx, "ListNamespaces", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.ListNamespaces(ctx, request)
	record(err)
	return response, err
}

func (p *metadataHealthSignalPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	record := recordFn(ctx, "GetMetadata", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetMetadata(ctx)
	record(err)
	return response, err
}

func (p *metadataHealthSignalPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	record := recordFn(ctx, "InitializeSystemNamespaces", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
	record(err)
	return err
}

func (p *metadataHealthSignalPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *clusterMetadataHealthSignalPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *clusterMetadataHealthSignalPersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	record := recordFn(ctx, "GetClusterMembers", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetClusterMembers(ctx, request)
	record(err)
	return response, err
}

func (p *clusterMetadataHealthSignalPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	record := recordFn(ctx, "UpsertClusterMembership", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.UpsertClusterMembership(ctx, request)
	record(err)
	return err
}

func (p *clusterMetadataHealthSignalPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	record := recordFn(ctx, "PruneClusterMembership", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.PruneClusterMembership(ctx, request)
	record(err)
	return err
}

func (p *clusterMetadataHealthSignalPersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	record := recordFn(ctx, "ListClusterMetadata", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.ListClusterMetadata(ctx, request)
	record(err)
	return response, err
}

func (p *clusterMetadataHealthSignalPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	record := recordFn(ctx, "GetCurrentClusterMetadata", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetCurrentClusterMetadata(ctx)
	record(err)
	return response, err
}

func (p *clusterMetadataHealthSignalPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	record := recordFn(ctx, "GetClusterMetadata", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetClusterMetadata(ctx, request)
	record(err)
	return response, err
}

func (p *clusterMetadataHealthSignalPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	record := recordFn(ctx, "SaveClusterMetadata", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.SaveClusterMetadata(ctx, request)
	record(err)
	return response, err
}

func (p *clusterMetadataHealthSignalPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	record := recordFn(ctx, "DeleteClusterMetadata", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.DeleteClusterMetadata(ctx, request)
	record(err)
	return err
}

func (p *clusterMetadataHealthSignalPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *queueHealthSignalPersistenceClient) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return p.persistence.Init(ctx, blob)
}

func (p *queueHealthSignalPersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob commonpb.DataBlob,
) error {
	record := recordFn(ctx, "EnqueueMessage", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.EnqueueMessage(ctx, blob)
	record(err)
	return err
}

func (p *queueHealthSignalPersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	record := recordFn(ctx, "ReadMessages", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
	record(err)
	return response, err
}

func (p *queueHealthSignalPersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	record := recordFn(ctx, "UpdateAckLevel", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.UpdateAckLevel(ctx, metadata)
	record(err)
	return err
}

func (p *queueHealthSignalPersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	record := recordFn(ctx, "GetAckLevels", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetAckLevels(ctx)
	record(err)
	return response, err
}

func (p *queueHealthSignalPersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	record := recordFn(ctx, "DeleteMessagesBefore", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.DeleteMessagesBefore(ctx, messageID)
	record(err)
	return err
}

func (p *queueHealthSignalPersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	record := recordFn(ctx, "EnqueueMessageToDLQ", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.EnqueueMessageToDLQ(ctx, blob)
	record(err)
	return response, err
}

func (p *queueHealthSignalPersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*QueueMessage, []byte, error) {
	record := recordFn(ctx, "ReadMessagesFromDLQ", CallerSegmentMissing, p.healthSignals)
	response, data, err := p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	record(err)
	return response, data, err
}

func (p *queueHealthSignalPersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	record := recordFn(ctx, "RangeDeleteMessagesFromDLQ", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	record(err)
	return err
}
func (p *queueHealthSignalPersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	record := recordFn(ctx, "UpdateDLQAckLevel", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.UpdateDLQAckLevel(ctx, metadata)
	record(err)
	return err
}

func (p *queueHealthSignalPersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	record := recordFn(ctx, "GetDLQAckLevels", CallerSegmentMissing, p.healthSignals)
	response, err := p.persistence.GetDLQAckLevels(ctx)
	record(err)
	return response, err
}

func (p *queueHealthSignalPersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	record := recordFn(ctx, "DeleteMessageFromDLQ", CallerSegmentMissing, p.healthSignals)
	err := p.persistence.DeleteMessageFromDLQ(ctx, messageID)
	record(err)
	return err
}

func (p *queueHealthSignalPersistenceClient) Close() {
	p.persistence.Close()
}

func recordFn(
	ctx context.Context,
	api string,
	shardID int32,
	healthSignals aggregate.SignalAggregator[quotas.Request],
) func(err error) {
	callerInfo := headers.GetCallerInfo(ctx)
	return healthSignals.GetRecordFn(quotas.NewRequest(
		api,
		RateLimitDefaultToken,
		callerInfo.CallerName,
		callerInfo.CallerType,
		shardID,
		callerInfo.CallOrigin,
	))
}
