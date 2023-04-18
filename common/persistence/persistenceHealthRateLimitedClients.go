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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backpressure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/quotas"
)

var (
	// ErrPersistenceUnhealthy is the error indicating persistence backoff has been enforced.
	// TODO: update cause and message
	ErrPersistenceUnhealthy = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "Persistence Max QPS Reached.")
)

type (
	shardHealthRateLimitedPersistenceClient struct {
		healthSignals backpressure.HealthSignalAggregator
		rateLimiter   quotas.DynamicRateLimiterImpl
		persistence   ShardManager
		logger        log.Logger
	}

	executionHealthRateLimitedPersistenceClient struct {
		healthSignals backpressure.HealthSignalAggregator
		rateLimiter   quotas.DynamicRateLimiterImpl
		persistence   ExecutionManager
		logger        log.Logger
	}

	taskHealthRateLimitedPersistenceClient struct {
		healthSignals backpressure.HealthSignalAggregator
		rateLimiter   quotas.DynamicRateLimiterImpl
		persistence   TaskManager
		logger        log.Logger
	}

	metadataHealthRateLimitedPersistenceClient struct {
		healthSignals backpressure.HealthSignalAggregator
		rateLimiter   quotas.DynamicRateLimiterImpl
		persistence   MetadataManager
		logger        log.Logger
	}

	clusterMetadataHealthRateLimitedPersistenceClient struct {
		healthSignals backpressure.HealthSignalAggregator
		rateLimiter   quotas.DynamicRateLimiterImpl
		persistence   ClusterMetadataManager
		logger        log.Logger
	}

	queueHealthRateLimitedPersistenceClient struct {
		healthSignals backpressure.HealthSignalAggregator
		rateLimiter   quotas.DynamicRateLimiterImpl
		persistence   Queue
		logger        log.Logger
	}
)

var _ ShardManager = (*shardHealthRateLimitedPersistenceClient)(nil)
var _ ExecutionManager = (*executionHealthRateLimitedPersistenceClient)(nil)
var _ TaskManager = (*taskHealthRateLimitedPersistenceClient)(nil)
var _ MetadataManager = (*metadataHealthRateLimitedPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataHealthRateLimitedPersistenceClient)(nil)
var _ Queue = (*queueHealthRateLimitedPersistenceClient)(nil)

func NewShardPersistenceHealthRateLimitedClient(persistence ShardManager, rateLimiter quotas.DynamicRateLimiterImpl, healthSignals backpressure.HealthSignalAggregator, logger log.Logger) ShardManager {
	return &shardHealthRateLimitedPersistenceClient{
		persistence:   persistence,
		rateLimiter:   rateLimiter,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewExecutionPersistenceHealthRateLimitedClient(persistence ExecutionManager, rateLimiter quotas.DynamicRateLimiterImpl, healthSignals backpressure.HealthSignalAggregator, logger log.Logger) ExecutionManager {
	return &executionHealthRateLimitedPersistenceClient{
		persistence:   persistence,
		rateLimiter:   rateLimiter,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewTaskPersistenceHealthRateLimitedClient(persistence TaskManager, rateLimiter quotas.DynamicRateLimiterImpl, healthSignals backpressure.HealthSignalAggregator, logger log.Logger) TaskManager {
	return &taskHealthRateLimitedPersistenceClient{
		persistence:   persistence,
		rateLimiter:   rateLimiter,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewMetadataPersistenceHealthRateLimitedClient(persistence MetadataManager, rateLimiter quotas.DynamicRateLimiterImpl, healthSignals backpressure.HealthSignalAggregator, logger log.Logger) MetadataManager {
	return &metadataHealthRateLimitedPersistenceClient{
		persistence:   persistence,
		rateLimiter:   rateLimiter,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewClusterMetadataPersistenceHealthRateLimitedClient(persistence ClusterMetadataManager, rateLimiter quotas.DynamicRateLimiterImpl, healthSignals backpressure.HealthSignalAggregator, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataHealthRateLimitedPersistenceClient{
		persistence:   persistence,
		rateLimiter:   rateLimiter,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func NewQueuePersistenceHealthRateLimitedClient(persistence Queue, rateLimiter quotas.DynamicRateLimiterImpl, healthSignals backpressure.HealthSignalAggregator, logger log.Logger) Queue {
	return &queueHealthRateLimitedPersistenceClient{
		persistence:   persistence,
		rateLimiter:   rateLimiter,
		healthSignals: healthSignals,
		logger:        logger,
	}
}

func (p *shardHealthRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardHealthRateLimitedPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	api := "GetOrCreateShard"
	if !p.rateLimiter.Allow() {
		// TODO: maybe should wait some duration instead of immediately returning an error?
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.GetOrCreateShard(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *shardHealthRateLimitedPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	api := "UpdateShard"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardInfo.GetShardId())
	err := p.persistence.UpdateShard(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardInfo.GetShardId())
	}
	return err
}

func (p *shardHealthRateLimitedPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	api := "AssertShardOwnership"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.AssertShardOwnership(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *shardHealthRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionHealthRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionHealthRateLimitedPersistenceClient) GetHistoryBranchUtil() HistoryBranchUtil {
	return p.persistence.GetHistoryBranchUtil()
}

func (p *executionHealthRateLimitedPersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	api := "CreateWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	api := "GetWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	api := "SetWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	api := "UpdateWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	resp, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return resp, err
}

func (p *executionHealthRateLimitedPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	api := "ConflictResolveWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	api := "DeleteWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.DeleteWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	api := "DeleteCurrentWorkflowExecution"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	api := "GetCurrentExecution"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.GetCurrentExecution(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	api := "ListConcreteExecutions"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) RegisterHistoryTaskReader(
	ctx context.Context,
	request *RegisterHistoryTaskReaderRequest,
) error {
	// hint methods don't actually hint DB, so don't go through persistence rate limiter
	return p.persistence.RegisterHistoryTaskReader(ctx, request)
}

func (p *executionHealthRateLimitedPersistenceClient) UnregisterHistoryTaskReader(
	ctx context.Context,
	request *UnregisterHistoryTaskReaderRequest,
) {
	// hint methods don't actually hint DB, so don't go through persistence rate limiter
	p.persistence.UnregisterHistoryTaskReader(ctx, request)
}

func (p *executionHealthRateLimitedPersistenceClient) UpdateHistoryTaskReaderProgress(
	ctx context.Context,
	request *UpdateHistoryTaskReaderProgressRequest,
) {
	// hint methods don't actually hint DB, so don't go through persistence rate limiter
	p.persistence.UpdateHistoryTaskReaderProgress(ctx, request)
}

func (p *executionHealthRateLimitedPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	api := "AddHistoryTasks"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.AddHistoryTasks(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	api := "GetHistoryTasks"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.GetHistoryTasks(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	api := "CompleteHistoryTask"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.CompleteHistoryTask(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	api := "RangeCompleteHistoryTasks"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.RangeCompleteHistoryTasks(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	api := "PutReplicationTaskToDLQ"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.PutReplicationTaskToDLQ(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	api := "GetReplicationTasksFromDLQ"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.GetReplicationTasksFromDLQ(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	api := "DeleteReplicationTaskFromDLQ"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	api := "RangeDeleteReplicationTaskFromDLQ"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	api := "AppendHistoryNodes"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.AppendHistoryNodes(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	api := "AppendRawHistoryNodes"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.AppendRawHistoryNodes(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	api := "ReadHistoryBranch"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	api := "ReadHistoryBranchReverse"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	api := "ReadHistoryBranchByBatch"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	api := "ReadRawHistoryBranch"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	api := "ForkHistoryBranch"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	api := "DeleteHistoryBranch"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	err := p.persistence.DeleteHistoryBranch(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return err
}

func (p *executionHealthRateLimitedPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	api := "TrimHistoryBranch"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, request.ShardID)
	resp, err := p.persistence.TrimHistoryBranch(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, request.ShardID)
	}
	return resp, err
}

func (p *executionHealthRateLimitedPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {
	api := "GetHistoryTree"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByShard(api, *request.ShardID)
	response, err := p.persistence.GetHistoryTree(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByShard(api, *request.ShardID)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	api := "GetAllHistoryTreeBranches"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *executionHealthRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskHealthRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskHealthRateLimitedPersistenceClient) CreateTasks(
	ctx context.Context,
	request *CreateTasksRequest,
) (*CreateTasksResponse, error) {
	api := "CreateTasks"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.TaskQueueInfo.Data.GetNamespaceId())
	response, err := p.persistence.CreateTasks(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.TaskQueueInfo.Data.GetNamespaceId())
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	api := "GetTasks"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.NamespaceID)
	response, err := p.persistence.GetTasks(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.NamespaceID)
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) error {
	api := "CompleteTask"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.TaskQueue.NamespaceID)
	err := p.persistence.CompleteTask(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.TaskQueue.NamespaceID)
	}
	return err
}

func (p *taskHealthRateLimitedPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	api := "CompleteTasksLessThan"
	if !p.rateLimiter.Allow() {
		return 0, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.NamespaceID)
	response, err := p.persistence.CompleteTasksLessThan(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.NamespaceID)
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	api := "CreateTaskQueue"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.TaskQueueInfo.GetNamespaceId())
	response, err := p.persistence.CreateTaskQueue(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.TaskQueueInfo.GetNamespaceId())
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	api := "UpdateTaskQueue"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.TaskQueueInfo.GetNamespaceId())
	response, err := p.persistence.UpdateTaskQueue(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.TaskQueueInfo.GetNamespaceId())
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	api := "GetTaskQueue"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.NamespaceID)
	response, err := p.persistence.GetTaskQueue(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.NamespaceID)
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	api := "ListTaskQueue"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.ListTaskQueue(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *taskHealthRateLimitedPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	api := "DeleteTaskQueue"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.TaskQueue.NamespaceID)
	err := p.persistence.DeleteTaskQueue(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.TaskQueue.NamespaceID)
	}
	return err
}

func (p *taskHealthRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataHealthRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataHealthRateLimitedPersistenceClient) CreateNamespace(
	ctx context.Context,
	request *CreateNamespaceRequest,
) (*CreateNamespaceResponse, error) {
	api := "CreateNamespace"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.Namespace.GetInfo().GetId())
	response, err := p.persistence.CreateNamespace(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.Namespace.GetInfo().GetId())
	}
	return response, err
}

func (p *metadataHealthRateLimitedPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	api := "GetNamespace"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	// TODO: only Name or ID will be specified, need to assign appropriately
	defer p.healthSignals.RecordLatencyByNamespace(api, request.ID)
	response, err := p.persistence.GetNamespace(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.ID)
	}
	return response, err
}

func (p *metadataHealthRateLimitedPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	api := "UpdateNamespace"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.Namespace.GetInfo().GetId())
	err := p.persistence.UpdateNamespace(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.Namespace.GetInfo().GetId())
	}
	return err
}

func (p *metadataHealthRateLimitedPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	api := "RenameNamespace"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	// TODO: handle rename in rate limiter metrics also
	defer p.healthSignals.RecordLatencyByNamespace(api, request.PreviousName)
	err := p.persistence.RenameNamespace(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.PreviousName)
	}
	return err
}

func (p *metadataHealthRateLimitedPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	api := "DeleteNamespace"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.ID)
	err := p.persistence.DeleteNamespace(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.ID)
	}
	return err
}

func (p *metadataHealthRateLimitedPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	api := "DeleteNamespaceByName"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatencyByNamespace(api, request.Name)
	err := p.persistence.DeleteNamespaceByName(ctx, request)
	if err != nil {
		p.healthSignals.RecordErrorByNamespace(api, request.Name)
	}
	return err
}

func (p *metadataHealthRateLimitedPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	api := "ListNamespaces"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.ListNamespaces(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *metadataHealthRateLimitedPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	api := "GetMetadata"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetMetadata(ctx)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *metadataHealthRateLimitedPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	api := "InitializeSystemNamespaces"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *metadataHealthRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	api := "GetClusterMembers"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetClusterMembers(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	api := "UpsertClusterMembership"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.UpsertClusterMembership(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	api := "PruneClusterMembership"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.PruneClusterMembership(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	api := "ListClusterMetadata"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.ListClusterMetadata(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	api := "GetCurrentClusterMetadata"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetCurrentClusterMetadata(ctx)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	api := "GetClusterMetadata"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetClusterMetadata(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	api := "SaveClusterMetadata"
	if !p.rateLimiter.Allow() {
		return false, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.SaveClusterMetadata(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	api := "DeleteClusterMetadata"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.DeleteClusterMetadata(ctx, request)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *clusterMetadataHealthRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *queueHealthRateLimitedPersistenceClient) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return p.persistence.Init(ctx, blob)
}

func (p *queueHealthRateLimitedPersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob commonpb.DataBlob,
) error {
	api := "EnqueueMessage"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.EnqueueMessage(ctx, blob)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *queueHealthRateLimitedPersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	api := "ReadMessages"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *queueHealthRateLimitedPersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	api := "UpdateAckLevel"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.UpdateAckLevel(ctx, metadata)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *queueHealthRateLimitedPersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	api := "GetAckLevels"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetAckLevels(ctx)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *queueHealthRateLimitedPersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	api := "DeleteMessagesBefore"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.DeleteMessagesBefore(ctx, messageID)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *queueHealthRateLimitedPersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	api := "EnqueueMessageToDLQ"
	if !p.rateLimiter.Allow() {
		return EmptyQueueMessageID, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.EnqueueMessageToDLQ(ctx, blob)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *queueHealthRateLimitedPersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*QueueMessage, []byte, error) {
	api := "ReadMessagesFromDLQ"
	if !p.rateLimiter.Allow() {
		return nil, nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, data, err := p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, data, err
}

func (p *queueHealthRateLimitedPersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	api := "RangeDeleteMessagesFromDLQ"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}
func (p *queueHealthRateLimitedPersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	api := "UpdateDLQAckLevel"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.UpdateDLQAckLevel(ctx, metadata)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *queueHealthRateLimitedPersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	api := "GetDLQAckLevels"
	if !p.rateLimiter.Allow() {
		return nil, ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	response, err := p.persistence.GetDLQAckLevels(ctx)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return response, err
}

func (p *queueHealthRateLimitedPersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	api := "DeleteMessageFromDLQ"
	if !p.rateLimiter.Allow() {
		return ErrPersistenceLimitExceeded
	}

	defer p.healthSignals.RecordLatency(api)
	err := p.persistence.DeleteMessageFromDLQ(ctx, messageID)
	if err != nil {
		p.healthSignals.RecordError(api)
	}
	return err
}

func (p *queueHealthRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}
