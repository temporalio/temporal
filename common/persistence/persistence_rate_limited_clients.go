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

	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/tasks"
)

const (
	RateLimitDefaultToken = 1
	CallerSegmentMissing  = -1
)

var (
	// ErrPersistenceLimitExceeded is the error indicating QPS limit reached.
	ErrPersistenceLimitExceeded = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		Message: "System Persistence Max QPS Reached.",
	}
	ErrPersistenceNamespaceLimitExceeded = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "Namespace Persistence Max QPS Reached.",
	}
)

type (
	shardRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          ShardManager
		logger               log.Logger
	}

	executionRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          ExecutionManager
		logger               log.Logger
	}

	taskRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          TaskManager
		logger               log.Logger
	}

	metadataRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          MetadataManager
		logger               log.Logger
	}

	clusterMetadataRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          ClusterMetadataManager
		logger               log.Logger
	}

	queueRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          Queue
		logger               log.Logger
	}

	nexusEndpointRateLimitedPersistenceClient struct {
		systemRateLimiter    quotas.RequestRateLimiter
		namespaceRateLimiter quotas.RequestRateLimiter
		persistence          NexusEndpointManager
		logger               log.Logger
	}
)

var _ ShardManager = (*shardRateLimitedPersistenceClient)(nil)
var _ ExecutionManager = (*executionRateLimitedPersistenceClient)(nil)
var _ TaskManager = (*taskRateLimitedPersistenceClient)(nil)
var _ MetadataManager = (*metadataRateLimitedPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataRateLimitedPersistenceClient)(nil)
var _ Queue = (*queueRateLimitedPersistenceClient)(nil)
var _ NexusEndpointManager = (*nexusEndpointRateLimitedPersistenceClient)(nil)

// NewShardPersistenceRateLimitedClient creates a client to manage shards
func NewShardPersistenceRateLimitedClient(persistence ShardManager, rateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) ShardManager {
	return &shardRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    rateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

// NewExecutionPersistenceRateLimitedClient creates a client to manage executions
func NewExecutionPersistenceRateLimitedClient(persistence ExecutionManager, systemRateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) ExecutionManager {
	return &executionRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

// NewTaskPersistenceRateLimitedClient creates a client to manage tasks
func NewTaskPersistenceRateLimitedClient(persistence TaskManager, systemRateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) TaskManager {
	return &taskRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

// NewMetadataPersistenceRateLimitedClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceRateLimitedClient(persistence MetadataManager, systemRateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) MetadataManager {
	return &metadataRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

// NewClusterMetadataPersistenceRateLimitedClient creates a ClusterMetadataManager client to manage cluster metadata
func NewClusterMetadataPersistenceRateLimitedClient(persistence ClusterMetadataManager, systemRateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

// NewQueuePersistenceRateLimitedClient creates a client to manage queue
func NewQueuePersistenceRateLimitedClient(persistence Queue, systemRateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) Queue {
	return &queueRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

// NewNexusEndpointPersistenceRateLimitedClient creates a NexusEndpointManager to manage nexus endpoints
func NewNexusEndpointPersistenceRateLimitedClient(persistence NexusEndpointManager, systemRateLimiter quotas.RequestRateLimiter, namespaceRateLimiter quotas.RequestRateLimiter, logger log.Logger) NexusEndpointManager {
	return &nexusEndpointRateLimitedPersistenceClient{
		persistence:          persistence,
		systemRateLimiter:    systemRateLimiter,
		namespaceRateLimiter: namespaceRateLimiter,
		logger:               logger,
	}
}

func (p *shardRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardRateLimitedPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	if err := allow(ctx, "GetOrCreateShard", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetOrCreateShard(ctx, request)
	return response, err
}

func (p *shardRateLimitedPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	if err := allow(ctx, "UpdateShard", request.ShardInfo.ShardId, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.UpdateShard(ctx, request)
}

func (p *shardRateLimitedPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	if err := allow(ctx, "AssertShardOwnership", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.AssertShardOwnership(ctx, request)
}

func (p *shardRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionRateLimitedPersistenceClient) GetHistoryBranchUtil() HistoryBranchUtil {
	return p.persistence.GetHistoryBranchUtil()
}

func (p *executionRateLimitedPersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	if err := allow(ctx, "CreateWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	if err := allow(ctx, "GetWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	if err := allow(ctx, "SetWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	if err := allow(ctx, "UpdateWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	resp, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	return resp, err
}

func (p *executionRateLimitedPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	if err := allow(ctx, "ConflictResolveWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	if err := allow(ctx, "DeleteWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.DeleteWorkflowExecution(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	if err := allow(ctx, "DeleteCurrentWorkflowExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	if err := allow(ctx, "GetCurrentExecution", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetCurrentExecution(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	if err := allow(ctx, "ListConcreteExecutions", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	if err := allow(ctx, "AddHistoryTasks", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.AddHistoryTasks(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	if err := allow(
		ctx,
		ConstructHistoryTaskAPI("GetHistoryTasks", request.TaskCategory),
		request.ShardID,
		p.systemRateLimiter,
		p.namespaceRateLimiter,
	); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetHistoryTasks(ctx, request)
	return response, err
}

func (p *executionRateLimitedPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	if err := allow(
		ctx,
		ConstructHistoryTaskAPI("CompleteHistoryTask", request.TaskCategory),
		request.ShardID,
		p.systemRateLimiter,
		p.namespaceRateLimiter,
	); err != nil {
		return err
	}

	return p.persistence.CompleteHistoryTask(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	if err := allow(
		ctx,
		ConstructHistoryTaskAPI("RangeCompleteHistoryTasks", request.TaskCategory),
		request.ShardID,
		p.systemRateLimiter,
		p.namespaceRateLimiter,
	); err != nil {
		return err
	}

	return p.persistence.RangeCompleteHistoryTasks(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	if err := allow(ctx, "PutReplicationTaskToDLQ", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.PutReplicationTaskToDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	if err := allow(ctx, "GetReplicationTasksFromDLQ", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	return p.persistence.GetReplicationTasksFromDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	if err := allow(ctx, "DeleteReplicationTaskFromDLQ", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	if err := allow(ctx, "RangeDeleteReplicationTaskFromDLQ", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionRateLimitedPersistenceClient) IsReplicationDLQEmpty(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (bool, error) {
	if err := allow(ctx, "IsReplicationDLQEmpty", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return true, err
	}

	return p.persistence.IsReplicationDLQEmpty(ctx, request)
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
	if err := allow(ctx, "CreateTasks", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.CreateTasks(ctx, request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	if err := allow(ctx, "GetTasks", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetTasks(ctx, request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	if err := allow(ctx, "CompleteTasksLessThan", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return 0, err
	}
	return p.persistence.CompleteTasksLessThan(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	if err := allow(ctx, "CreateTaskQueue", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.CreateTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	if err := allow(ctx, "UpdateTaskQueue", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.UpdateTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	if err := allow(ctx, "GetTaskQueue", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.GetTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	if err := allow(ctx, "ListTaskQueue", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.ListTaskQueue(ctx, request)
}

func (p *taskRateLimitedPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	if err := allow(ctx, "DeleteTaskQueue", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}
	return p.persistence.DeleteTaskQueue(ctx, request)
}

func (p taskRateLimitedPersistenceClient) GetTaskQueueUserData(
	ctx context.Context,
	request *GetTaskQueueUserDataRequest,
) (*GetTaskQueueUserDataResponse, error) {
	if err := allow(ctx, "GetTaskQueueUserData", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.GetTaskQueueUserData(ctx, request)
}

func (p taskRateLimitedPersistenceClient) UpdateTaskQueueUserData(
	ctx context.Context,
	request *UpdateTaskQueueUserDataRequest,
) error {
	if err := allow(ctx, "UpdateTaskQueueUserData", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}
	return p.persistence.UpdateTaskQueueUserData(ctx, request)
}

func (p taskRateLimitedPersistenceClient) ListTaskQueueUserDataEntries(
	ctx context.Context,
	request *ListTaskQueueUserDataEntriesRequest,
) (*ListTaskQueueUserDataEntriesResponse, error) {
	if err := allow(ctx, "ListTaskQueueUserDataEntries", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.ListTaskQueueUserDataEntries(ctx, request)
}

func (p taskRateLimitedPersistenceClient) GetTaskQueuesByBuildId(ctx context.Context, request *GetTaskQueuesByBuildIdRequest) ([]string, error) {
	if err := allow(ctx, "GetTaskQueuesByBuildId", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.GetTaskQueuesByBuildId(ctx, request)
}

func (p taskRateLimitedPersistenceClient) CountTaskQueuesByBuildId(ctx context.Context, request *CountTaskQueuesByBuildIdRequest) (int, error) {
	if err := allow(ctx, "CountTaskQueuesByBuildId", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return 0, err
	}
	return p.persistence.CountTaskQueuesByBuildId(ctx, request)
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
	if err := allow(ctx, "CreateNamespace", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.CreateNamespace(ctx, request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	if err := allow(ctx, "GetNamespace", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetNamespace(ctx, request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	if err := allow(ctx, "UpdateNamespace", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.UpdateNamespace(ctx, request)
}

func (p *metadataRateLimitedPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	if err := allow(ctx, "RenameNamespace", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.RenameNamespace(ctx, request)
}

func (p *metadataRateLimitedPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	if err := allow(ctx, "DeleteNamespace", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.DeleteNamespace(ctx, request)
}

func (p *metadataRateLimitedPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	if err := allow(ctx, "DeleteNamespaceByName", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.DeleteNamespaceByName(ctx, request)
}

func (p *metadataRateLimitedPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	if err := allow(ctx, "ListNamespaces", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.ListNamespaces(ctx, request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	if err := allow(ctx, "GetMetadata", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	response, err := p.persistence.GetMetadata(ctx)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	if err := allow(ctx, "InitializeSystemNamespaces", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
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
	if err := allow(ctx, "AppendHistoryNodes", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.AppendHistoryNodes(ctx, request)
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionRateLimitedPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	if err := allow(ctx, "AppendRawHistoryNodes", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.AppendRawHistoryNodes(ctx, request)
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	if err := allow(ctx, "ReadHistoryBranch", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	return response, err
}

// ReadHistoryBranchReverse returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	if err := allow(ctx, "ReadHistoryBranchReverse", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	if err := allow(ctx, "ReadHistoryBranchByBatch", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *executionRateLimitedPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	if err := allow(ctx, "ReadRawHistoryBranch", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *executionRateLimitedPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	if err := allow(ctx, "ForkHistoryBranch", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *executionRateLimitedPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	if err := allow(ctx, "DeleteHistoryBranch", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}
	return p.persistence.DeleteHistoryBranch(ctx, request)
}

// TrimHistoryBranch trims a branch
func (p *executionRateLimitedPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	if err := allow(ctx, "TrimHistoryBranch", request.ShardID, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	resp, err := p.persistence.TrimHistoryBranch(ctx, request)
	return resp, err
}

func (p *executionRateLimitedPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	if err := allow(ctx, "GetAllHistoryTreeBranches", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	return response, err
}

func (p *queueRateLimitedPersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	if err := allow(ctx, "EnqueueMessage", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.EnqueueMessage(ctx, blob)
}

func (p *queueRateLimitedPersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	if err := allow(ctx, "ReadMessages", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	return p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
}

func (p *queueRateLimitedPersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	if err := allow(ctx, "UpdateAckLevel", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.UpdateAckLevel(ctx, metadata)
}

func (p *queueRateLimitedPersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	if err := allow(ctx, "GetAckLevels", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	return p.persistence.GetAckLevels(ctx)
}

func (p *queueRateLimitedPersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	if err := allow(ctx, "DeleteMessagesBefore", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.DeleteMessagesBefore(ctx, messageID)
}

func (p *queueRateLimitedPersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	if err := allow(ctx, "EnqueueMessageToDLQ", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return EmptyQueueMessageID, err
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
	if err := allow(ctx, "ReadMessagesFromDLQ", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, nil, err
	}

	return p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
}

func (p *queueRateLimitedPersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	if err := allow(ctx, "RangeDeleteMessagesFromDLQ", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
}
func (p *queueRateLimitedPersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	if err := allow(ctx, "UpdateDLQAckLevel", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}

	return p.persistence.UpdateDLQAckLevel(ctx, metadata)
}

func (p *queueRateLimitedPersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	if err := allow(ctx, "GetDLQAckLevels", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}

	return p.persistence.GetDLQAckLevels(ctx)
}

func (p *queueRateLimitedPersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	if err := allow(ctx, "DeleteMessageFromDLQ", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
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
	if err := allow(ctx, "GetClusterMembers", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return c.persistence.GetClusterMembers(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	if err := allow(ctx, "UpsertClusterMembership", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return err
	}
	return c.persistence.UpsertClusterMembership(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	if err := allow(ctx, "PruneClusterMembership", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return err
	}
	return c.persistence.PruneClusterMembership(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	if err := allow(ctx, "ListClusterMetadata", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return c.persistence.ListClusterMetadata(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	if err := allow(ctx, "GetCurrentClusterMetadata", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return c.persistence.GetCurrentClusterMetadata(ctx)
}

func (c *clusterMetadataRateLimitedPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	if err := allow(ctx, "GetClusterMetadata", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return c.persistence.GetClusterMetadata(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	if err := allow(ctx, "SaveClusterMetadata", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return false, err
	}
	return c.persistence.SaveClusterMetadata(ctx, request)
}

func (c *clusterMetadataRateLimitedPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	if err := allow(ctx, "DeleteClusterMetadata", CallerSegmentMissing, c.systemRateLimiter, c.namespaceRateLimiter); err != nil {
		return err
	}
	return c.persistence.DeleteClusterMetadata(ctx, request)
}

func (p *nexusEndpointRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *nexusEndpointRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *nexusEndpointRateLimitedPersistenceClient) GetNexusEndpoint(
	ctx context.Context,
	request *GetNexusEndpointRequest,
) (*persistencepb.NexusEndpointEntry, error) {
	if err := allow(ctx, "GetNexusEndpoint", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.GetNexusEndpoint(ctx, request)
}

func (p *nexusEndpointRateLimitedPersistenceClient) ListNexusEndpoints(
	ctx context.Context,
	request *ListNexusEndpointsRequest,
) (*ListNexusEndpointsResponse, error) {
	if err := allow(ctx, "ListNexusEndpoints", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.ListNexusEndpoints(ctx, request)
}

func (p *nexusEndpointRateLimitedPersistenceClient) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *CreateOrUpdateNexusEndpointRequest,
) (*CreateOrUpdateNexusEndpointResponse, error) {
	if err := allow(ctx, "CreateOrUpdateNexusEndpoint", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return nil, err
	}
	return p.persistence.CreateOrUpdateNexusEndpoint(ctx, request)
}

func (p *nexusEndpointRateLimitedPersistenceClient) DeleteNexusEndpoint(
	ctx context.Context,
	request *DeleteNexusEndpointRequest,
) error {
	if err := allow(ctx, "DeleteNexusEndpoint", CallerSegmentMissing, p.systemRateLimiter, p.namespaceRateLimiter); err != nil {
		return err
	}
	return p.persistence.DeleteNexusEndpoint(ctx, request)
}

func allow(
	ctx context.Context,
	api string,
	shardID int32,
	systemRateLimiter quotas.RequestRateLimiter,
	namespaceRateLimiter quotas.RequestRateLimiter,
) error {
	callerInfo := headers.GetCallerInfo(ctx)
	// namespace-level rate limits has to be applied before system-level rate limits.
	now := time.Now().UTC()
	quotaRequest := quotas.NewRequest(
		api,
		RateLimitDefaultToken,
		callerInfo.CallerName,
		callerInfo.CallerType,
		shardID,
		callerInfo.CallOrigin,
	)
	if ok := namespaceRateLimiter.Allow(now, quotaRequest); !ok {
		return ErrPersistenceNamespaceLimitExceeded
	}
	if ok := systemRateLimiter.Allow(now, quotaRequest); !ok {
		return ErrPersistenceLimitExceeded
	}
	return nil
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
