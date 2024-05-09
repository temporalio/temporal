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
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type (
	metricEmitter struct {
		metricsHandler metrics.Handler
		logger         log.Logger
	}

	shardPersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   ShardManager
	}

	executionPersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   ExecutionManager
	}

	taskPersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   TaskManager
	}

	metadataPersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   MetadataManager
	}

	clusterMetadataPersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   ClusterMetadataManager
	}

	queuePersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   Queue
	}

	nexusEndpointPersistenceClient struct {
		metricEmitter
		healthSignals HealthSignalAggregator
		persistence   NexusEndpointManager
	}
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*executionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)
var _ MetadataManager = (*metadataPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataPersistenceClient)(nil)
var _ Queue = (*queuePersistenceClient)(nil)
var _ NexusEndpointManager = (*nexusEndpointPersistenceClient)(nil)

// NewShardPersistenceMetricsClient creates a client to manage shards
func NewShardPersistenceMetricsClient(persistence ShardManager, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) ShardManager {
	return &shardPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

// NewExecutionPersistenceMetricsClient creates a client to manage executions
func NewExecutionPersistenceMetricsClient(persistence ExecutionManager, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) ExecutionManager {
	return &executionPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

// NewTaskPersistenceMetricsClient creates a client to manage tasks
func NewTaskPersistenceMetricsClient(persistence TaskManager, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) TaskManager {
	return &taskPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

// NewMetadataPersistenceMetricsClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceMetricsClient(persistence MetadataManager, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) MetadataManager {
	return &metadataPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

// NewClusterMetadataPersistenceMetricsClient creates a ClusterMetadataManager client to manage cluster metadata
func NewClusterMetadataPersistenceMetricsClient(persistence ClusterMetadataManager, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

// NewQueuePersistenceMetricsClient creates a client to manage queue
func NewQueuePersistenceMetricsClient(persistence Queue, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) Queue {
	return &queuePersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

// NewNexusEndpointPersistenceMetricsClient creates a NexusEndpointManager to manage nexus endpoints
func NewNexusEndpointPersistenceMetricsClient(persistence NexusEndpointManager, metricsHandler metrics.Handler, healthSignals HealthSignalAggregator, logger log.Logger) NexusEndpointManager {
	return &nexusEndpointPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		healthSignals: healthSignals,
		persistence:   persistence,
	}
}

func (p *shardPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (_ *GetOrCreateShardResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		latency := time.Since(startTime)
		p.healthSignals.Record(request.ShardID, caller, latency, retErr)
		p.recordRequestMetrics(metrics.PersistenceGetOrCreateShardScope, caller, latency, retErr)
	}()
	return p.persistence.GetOrCreateShard(ctx, request)
}

func (p *shardPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardInfo.GetShardId(), caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateShardScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateShard(ctx, request)
}

func (p *shardPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceAssertShardOwnershipScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.AssertShardOwnership(ctx, request)
}

func (p *shardPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionPersistenceClient) GetHistoryBranchUtil() HistoryBranchUtil {
	return p.persistence.GetHistoryBranchUtil()
}

func (p *executionPersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (_ *CreateWorkflowExecutionResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CreateWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (_ *GetWorkflowExecutionResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (_ *SetWorkflowExecutionResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceSetWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.SetWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (_ *UpdateWorkflowExecutionResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (_ *ConflictResolveWorkflowExecutionResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceConflictResolveWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ConflictResolveWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (_ *GetCurrentExecutionResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetCurrentExecutionScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetCurrentExecution(ctx, request)
}

func (p *executionPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (_ *ListConcreteExecutionsResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceListConcreteExecutionsScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ListConcreteExecutions(ctx, request)
}

func (p *executionPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceAddTasksScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.AddHistoryTasks(ctx, request)
}

func (p *executionPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (_ *GetHistoryTasksResponse, retErr error) {
	var operation string
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		operation = metrics.PersistenceGetTransferTasksScope
	case tasks.CategoryIDTimer:
		operation = metrics.PersistenceGetTimerTasksScope
	case tasks.CategoryIDVisibility:
		operation = metrics.PersistenceGetVisibilityTasksScope
	case tasks.CategoryIDReplication:
		operation = metrics.PersistenceGetReplicationTasksScope
	case tasks.CategoryIDArchival:
		operation = metrics.PersistenceGetArchivalTasksScope
	case tasks.CategoryIDOutbound:
		operation = metrics.PersistenceGetOutboundTasksScope
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(operation, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetHistoryTasks(ctx, request)
}

func (p *executionPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) (retErr error) {
	var operation string
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		operation = metrics.PersistenceCompleteTransferTaskScope
	case tasks.CategoryIDTimer:
		operation = metrics.PersistenceCompleteTimerTaskScope
	case tasks.CategoryIDVisibility:
		operation = metrics.PersistenceCompleteVisibilityTaskScope
	case tasks.CategoryIDReplication:
		operation = metrics.PersistenceCompleteReplicationTaskScope
	case tasks.CategoryIDArchival:
		operation = metrics.PersistenceCompleteArchivalTaskScope
	case tasks.CategoryIDOutbound:
		operation = metrics.PersistenceCompleteOutboundTasksScope
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(operation, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CompleteHistoryTask(ctx, request)
}

func (p *executionPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) (retErr error) {
	var operation string
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		operation = metrics.PersistenceRangeCompleteTransferTasksScope
	case tasks.CategoryIDTimer:
		operation = metrics.PersistenceRangeCompleteTimerTasksScope
	case tasks.CategoryIDVisibility:
		operation = metrics.PersistenceRangeCompleteVisibilityTasksScope
	case tasks.CategoryIDReplication:
		operation = metrics.PersistenceRangeCompleteReplicationTasksScope
	case tasks.CategoryIDArchival:
		operation = metrics.PersistenceRangeCompleteArchivalTasksScope
	case tasks.CategoryIDOutbound:
		operation = metrics.PersistenceRangeCompleteOutboundTasksScope
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(operation, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.RangeCompleteHistoryTasks(ctx, request)
}

func (p *executionPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistencePutReplicationTaskToDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.PutReplicationTaskToDLQ(ctx, request)
}

func (p *executionPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (_ *GetHistoryTasksResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetReplicationTasksFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetReplicationTasksFromDLQ(ctx, request)
}

func (p *executionPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteReplicationTaskFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionPersistenceClient) IsReplicationDLQEmpty(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (_ bool, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(request.ShardID, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetReplicationTasksFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.IsReplicationDLQEmpty(ctx, request)
}

func (p *executionPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskPersistenceClient) CreateTasks(
	ctx context.Context,
	request *CreateTasksRequest,
) (_ *CreateTasksResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateTasksScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CreateTasks(ctx, request)
}

func (p *taskPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (_ *GetTasksResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetTasksScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetTasks(ctx, request)
}

func (p *taskPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (_ int, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCompleteTasksLessThanScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CompleteTasksLessThan(ctx, request)
}

func (p *taskPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (_ *CreateTaskQueueResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateTaskQueueScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CreateTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (_ *UpdateTaskQueueResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateTaskQueueScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (_ *GetTaskQueueResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetTaskQueueScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (_ *ListTaskQueueResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceListTaskQueueScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ListTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteTaskQueueScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) GetTaskQueueUserData(
	ctx context.Context,
	request *GetTaskQueueUserDataRequest,
) (_ *GetTaskQueueUserDataResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetTaskQueueUserDataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetTaskQueueUserData(ctx, request)
}

func (p *taskPersistenceClient) UpdateTaskQueueUserData(
	ctx context.Context,
	request *UpdateTaskQueueUserDataRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateTaskQueueUserDataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateTaskQueueUserData(ctx, request)
}

func (p *taskPersistenceClient) ListTaskQueueUserDataEntries(
	ctx context.Context,
	request *ListTaskQueueUserDataEntriesRequest,
) (_ *ListTaskQueueUserDataEntriesResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceListTaskQueueUserDataEntriesScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ListTaskQueueUserDataEntries(ctx, request)
}

func (p *taskPersistenceClient) GetTaskQueuesByBuildId(ctx context.Context, request *GetTaskQueuesByBuildIdRequest) (_ []string, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetTaskQueuesByBuildIdScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetTaskQueuesByBuildId(ctx, request)
}

func (p *taskPersistenceClient) CountTaskQueuesByBuildId(ctx context.Context, request *CountTaskQueuesByBuildIdRequest) (_ int, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCountTaskQueuesByBuildIdScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CountTaskQueuesByBuildId(ctx, request)
}

func (p *taskPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataPersistenceClient) CreateNamespace(
	ctx context.Context,
	request *CreateNamespaceRequest,
) (_ *CreateNamespaceResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateNamespaceScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CreateNamespace(ctx, request)
}

func (p *metadataPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (_ *GetNamespaceResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetNamespaceScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetNamespace(ctx, request)
}

func (p *metadataPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateNamespaceScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateNamespace(ctx, request)
}

func (p *metadataPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceRenameNamespaceScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.RenameNamespace(ctx, request)
}

func (p *metadataPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteNamespaceScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteNamespace(ctx, request)
}

func (p *metadataPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteNamespaceByNameScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteNamespaceByName(ctx, request)
}

func (p *metadataPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (_ *ListNamespacesResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceListNamespacesScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ListNamespaces(ctx, request)
}

func (p *metadataPersistenceClient) GetMetadata(
	ctx context.Context,
) (_ *GetMetadataResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetMetadataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetMetadata(ctx)
}

func (p *metadataPersistenceClient) Close() {
	p.persistence.Close()
}

// AppendHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (_ *AppendHistoryNodesResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceAppendHistoryNodesScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.AppendHistoryNodes(ctx, request)
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (_ *AppendHistoryNodesResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceAppendRawHistoryNodesScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.AppendRawHistoryNodes(ctx, request)
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (_ *ReadHistoryBranchResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.recordRequestMetrics(metrics.PersistenceReadHistoryBranchScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ReadHistoryBranch(ctx, request)
}

func (p *executionPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (_ *ReadHistoryBranchReverseResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.recordRequestMetrics(metrics.PersistenceReadHistoryBranchReverseScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ReadHistoryBranchReverse(ctx, request)
}

// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
func (p *executionPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (_ *ReadHistoryBranchByBatchResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.recordRequestMetrics(metrics.PersistenceReadHistoryBranchScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ReadHistoryBranchByBatch(ctx, request)
}

// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
func (p *executionPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (_ *ReadRawHistoryBranchResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.recordRequestMetrics(metrics.PersistenceReadRawHistoryBranchScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ReadRawHistoryBranch(ctx, request)
}

// ForkHistoryBranch forks a new branch from an old branch
func (p *executionPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (_ *ForkHistoryBranchResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.recordRequestMetrics(metrics.PersistenceForkHistoryBranchScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ForkHistoryBranch(ctx, request)
}

// DeleteHistoryBranch removes a branch
func (p *executionPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.recordRequestMetrics(metrics.PersistenceDeleteHistoryBranchScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteHistoryBranch(ctx, request)
}

// TrimHistoryBranch trims a branch
func (p *executionPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (_ *TrimHistoryBranchResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceTrimHistoryBranchScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.TrimHistoryBranch(ctx, request)
}

func (p *executionPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (_ *GetAllHistoryTreeBranchesResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetAllHistoryTreeBranchesScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetAllHistoryTreeBranches(ctx, request)
}

func (p *queuePersistenceClient) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return p.persistence.Init(ctx, blob)
}

func (p *queuePersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceEnqueueMessageScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.EnqueueMessage(ctx, blob)
}

func (p *queuePersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) (_ []*QueueMessage, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceReadQueueMessagesScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
}

func (p *queuePersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateAckLevelScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateAckLevel(ctx, metadata)
}

func (p *queuePersistenceClient) GetAckLevels(
	ctx context.Context,
) (_ *InternalQueueMetadata, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetAckLevelScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetAckLevels(ctx)
}

func (p *queuePersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteMessagesBeforeScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteMessagesBefore(ctx, messageID)
}

func (p *queuePersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (_ int64, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceEnqueueMessageToDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.EnqueueMessageToDLQ(ctx, blob)
}

func (p *queuePersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) (_ []*QueueMessage, _ []byte, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceReadMessagesFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
}

func (p *queuePersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteMessageFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteMessageFromDLQ(ctx, messageID)
}

func (p *queuePersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceRangeDeleteMessagesFromDLQScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
}

func (p *queuePersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateDLQAckLevelScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpdateDLQAckLevel(ctx, metadata)
}

func (p *queuePersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (_ *InternalQueueMetadata, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetDLQAckLevelScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetDLQAckLevels(ctx)
}

func (p *queuePersistenceClient) Close() {
	p.persistence.Close()
}

func (p *clusterMetadataPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *clusterMetadataPersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (_ *ListClusterMetadataResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceListClusterMetadataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ListClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (_ *GetClusterMetadataResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetCurrentClusterMetadataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetCurrentClusterMetadata(ctx)
}

func (p *clusterMetadataPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (_ *GetClusterMetadataResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetClusterMetadataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (_ bool, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceSaveClusterMetadataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.SaveClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteClusterMetadataScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *clusterMetadataPersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (_ *GetClusterMembersResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetClusterMembersScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetClusterMembers(ctx, request)
}

func (p *clusterMetadataPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceUpsertClusterMembershipScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.UpsertClusterMembership(ctx, request)
}

func (p *clusterMetadataPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistencePruneClusterMembershipScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.PruneClusterMembership(ctx, request)
}

func (p *metadataPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceInitializeSystemNamespaceScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
}

func (p *nexusEndpointPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *nexusEndpointPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *nexusEndpointPersistenceClient) GetNexusEndpoint(
	ctx context.Context,
	request *GetNexusEndpointRequest,
) (_ *persistencespb.NexusEndpointEntry, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceGetNexusEndpointScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.GetNexusEndpoint(ctx, request)
}

func (p *nexusEndpointPersistenceClient) ListNexusEndpoints(
	ctx context.Context,
	request *ListNexusEndpointsRequest,
) (_ *ListNexusEndpointsResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceListNexusEndpointsScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.ListNexusEndpoints(ctx, request)
}

func (p *nexusEndpointPersistenceClient) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *CreateOrUpdateNexusEndpointRequest,
) (_ *CreateOrUpdateNexusEndpointResponse, retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateOrUpdateNexusEndpointScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.CreateOrUpdateNexusEndpoint(ctx, request)
}

func (p *nexusEndpointPersistenceClient) DeleteNexusEndpoint(
	ctx context.Context,
	request *DeleteNexusEndpointRequest,
) (retErr error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	startTime := time.Now().UTC()
	defer func() {
		p.healthSignals.Record(CallerSegmentMissing, caller, time.Since(startTime), retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteNexusEndpointScope, caller, time.Since(startTime), retErr)
	}()
	return p.persistence.DeleteNexusEndpoint(ctx, request)
}

func (p *metricEmitter) recordRequestMetrics(operation string, caller string, latency time.Duration, err error) {
	handler := p.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller))
	metrics.PersistenceRequests.With(handler).Record(1)
	metrics.PersistenceLatency.With(handler).Record(latency)
	updateErrorMetric(handler, p.logger, operation, err)
}

func updateErrorMetric(handler metrics.Handler, logger log.Logger, operation string, err error) {
	if err != nil {
		metrics.PersistenceErrorWithType.With(handler).Record(1, metrics.ServiceErrorTypeTag(err))
		switch err := err.(type) {
		case *ShardAlreadyExistError,
			*ShardOwnershipLostError,
			*AppendHistoryTimeoutError,
			*CurrentWorkflowConditionFailedError,
			*WorkflowConditionFailedError,
			*ConditionFailedError,
			*TimeoutError,
			*serviceerror.InvalidArgument,
			*serviceerror.NamespaceAlreadyExists,
			*serviceerror.NotFound,
			*serviceerror.NamespaceNotFound:
			// no-op

		case *serviceerror.ResourceExhausted:
			metrics.PersistenceErrResourceExhaustedCounter.With(handler).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
		default:
			logger.Error("Operation failed with internal error.", tag.Error(err), tag.Operation(operation))
			metrics.PersistenceFailures.With(handler).Record(1)
		}
	}
}
