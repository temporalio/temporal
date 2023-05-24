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
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
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
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*executionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)
var _ MetadataManager = (*metadataPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataPersistenceClient)(nil)
var _ Queue = (*queuePersistenceClient)(nil)

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

func (p *shardPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (_ *GetOrCreateShardResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetOrCreateShardScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetOrCreateShardScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetOrCreateShard(ctx, request)
}

func (p *shardPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpdateShardScope, request.ShardInfo.GetShardId(), p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateShardScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpdateShard(ctx, request)
}

func (p *shardPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceAssertShardOwnershipScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceAssertShardOwnershipScope, callerInfo.CallerName, startTime, retErr)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceCreateWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.CreateWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (_ *GetWorkflowExecutionResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (_ *SetWorkflowExecutionResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceSetWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceSetWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.SetWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (_ *UpdateWorkflowExecutionResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpdateWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpdateWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (_ *ConflictResolveWorkflowExecutionResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceConflictResolveWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceConflictResolveWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ConflictResolveWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteCurrentWorkflowExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
}

func (p *executionPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (_ *GetCurrentExecutionResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetCurrentExecutionScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetCurrentExecutionScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetCurrentExecution(ctx, request)
}

func (p *executionPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (_ *ListConcreteExecutionsResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceListConcreteExecutionsScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceListConcreteExecutionsScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ListConcreteExecutions(ctx, request)
}

func (p *executionPersistenceClient) RegisterHistoryTaskReader(
	ctx context.Context,
	request *RegisterHistoryTaskReaderRequest,
) error {
	// hint methods won't go through persistence rate limiter
	// so also not emitting any persistence request/error metrics
	return p.persistence.RegisterHistoryTaskReader(ctx, request)
}

func (p *executionPersistenceClient) UnregisterHistoryTaskReader(
	ctx context.Context,
	request *UnregisterHistoryTaskReaderRequest,
) {
	// hint methods won't go through persistence rate limiter
	// so also not emitting any persistence request/error metrics
	p.persistence.UnregisterHistoryTaskReader(ctx, request)
}

func (p *executionPersistenceClient) UpdateHistoryTaskReaderProgress(
	ctx context.Context,
	request *UpdateHistoryTaskReaderProgressRequest,
) {
	// hint methods won't go through persistence rate limiter
	// so also not emitting any persistence request/error metrics
	p.persistence.UpdateHistoryTaskReaderProgress(ctx, request)
}

func (p *executionPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceAddTasksScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceAddTasksScope, callerInfo.CallerName, startTime, retErr)
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
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, operation, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(operation, callerInfo.CallerName, startTime, retErr)
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
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, operation, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(operation, callerInfo.CallerName, startTime, retErr)
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
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, operation, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(operation, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.RangeCompleteHistoryTasks(ctx, request)
}

func (p *executionPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistencePutReplicationTaskToDLQScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistencePutReplicationTaskToDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.PutReplicationTaskToDLQ(ctx, request)
}

func (p *executionPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (_ *GetHistoryTasksResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetReplicationTasksFromDLQScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetReplicationTasksFromDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetReplicationTasksFromDLQ(ctx, request)
}

func (p *executionPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteReplicationTaskFromDLQScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteReplicationTaskFromDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
}

func (p *executionPersistenceClient) IsReplicationDLQEmpty(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (_ bool, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetReplicationTasksFromDLQScope, request.ShardID, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetReplicationTasksFromDLQScope, callerInfo.CallerName, startTime, retErr)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceCreateTasksScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateTasksScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.CreateTasks(ctx, request)
}

func (p *taskPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (_ *GetTasksResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetTasksScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetTasksScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetTasks(ctx, request)
}

func (p *taskPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceCompleteTaskScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceCompleteTaskScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.CompleteTask(ctx, request)
}

func (p *taskPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (_ int, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceCompleteTasksLessThanScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceCompleteTasksLessThanScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.CompleteTasksLessThan(ctx, request)
}

func (p *taskPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (_ *CreateTaskQueueResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceCreateTaskQueueScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateTaskQueueScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.CreateTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (_ *UpdateTaskQueueResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpdateTaskQueueScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateTaskQueueScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpdateTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (_ *GetTaskQueueResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetTaskQueueScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetTaskQueueScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (_ *ListTaskQueueResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceListTaskQueueScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceListTaskQueueScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ListTaskQueue(ctx, request)
}

func (p *taskPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteTaskQueueScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteTaskQueueScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteTaskQueue(ctx, request)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceCreateNamespaceScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceCreateNamespaceScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.CreateNamespace(ctx, request)
}

func (p *metadataPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (_ *GetNamespaceResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetNamespaceScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetNamespaceScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetNamespace(ctx, request)
}

func (p *metadataPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpdateNamespaceScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateNamespaceScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpdateNamespace(ctx, request)
}

func (p *metadataPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceRenameNamespaceScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceRenameNamespaceScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.RenameNamespace(ctx, request)
}

func (p *metadataPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteNamespaceScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteNamespaceScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteNamespace(ctx, request)
}

func (p *metadataPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteNamespaceByNameScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteNamespaceByNameScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteNamespaceByName(ctx, request)
}

func (p *metadataPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (_ *ListNamespacesResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceListNamespacesScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceListNamespacesScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ListNamespaces(ctx, request)
}

func (p *metadataPersistenceClient) GetMetadata(
	ctx context.Context,
) (_ *GetMetadataResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetMetadataScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetMetadataScope, callerInfo.CallerName, startTime, retErr)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceAppendHistoryNodesScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceAppendHistoryNodesScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.AppendHistoryNodes(ctx, request)
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (_ *AppendHistoryNodesResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceAppendRawHistoryNodesScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceAppendRawHistoryNodesScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.AppendRawHistoryNodes(ctx, request)
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (_ *ReadHistoryBranchResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceReadHistoryBranchScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceReadHistoryBranchScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ReadHistoryBranch(ctx, request)
}

func (p *executionPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (_ *ReadHistoryBranchReverseResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceReadHistoryBranchReverseScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceReadHistoryBranchReverseScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ReadHistoryBranchReverse(ctx, request)
}

// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
func (p *executionPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (_ *ReadHistoryBranchByBatchResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceReadHistoryBranchScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceReadHistoryBranchScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ReadHistoryBranchByBatch(ctx, request)
}

// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
func (p *executionPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (_ *ReadRawHistoryBranchResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceReadRawHistoryBranchScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceReadRawHistoryBranchScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ReadRawHistoryBranch(ctx, request)
}

// ForkHistoryBranch forks a new branch from an old branch
func (p *executionPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (_ *ForkHistoryBranchResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceForkHistoryBranchScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceForkHistoryBranchScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ForkHistoryBranch(ctx, request)
}

// DeleteHistoryBranch removes a branch
func (p *executionPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteHistoryBranchScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteHistoryBranchScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteHistoryBranch(ctx, request)
}

// TrimHistoryBranch trims a branch
func (p *executionPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (_ *TrimHistoryBranchResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceTrimHistoryBranchScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceTrimHistoryBranchScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.TrimHistoryBranch(ctx, request)
}

func (p *executionPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (_ *GetAllHistoryTreeBranchesResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetAllHistoryTreeBranchesScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetAllHistoryTreeBranchesScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetAllHistoryTreeBranches(ctx, request)
}

// GetHistoryTree returns all branch information of a tree
func (p *executionPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (_ *GetHistoryTreeResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetHistoryTreeScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetHistoryTreeScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetHistoryTree(ctx, request)
}

func (p *queuePersistenceClient) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return p.persistence.Init(ctx, blob)
}

func (p *queuePersistenceClient) EnqueueMessage(
	ctx context.Context,
	blob commonpb.DataBlob,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceEnqueueMessageScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceEnqueueMessageScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.EnqueueMessage(ctx, blob)
}

func (p *queuePersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) (_ []*QueueMessage, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceReadQueueMessagesScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceReadQueueMessagesScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
}

func (p *queuePersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpdateAckLevelScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateAckLevelScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpdateAckLevel(ctx, metadata)
}

func (p *queuePersistenceClient) GetAckLevels(
	ctx context.Context,
) (_ *InternalQueueMetadata, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetAckLevelScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetAckLevelScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetAckLevels(ctx)
}

func (p *queuePersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteMessagesBeforeScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteMessagesBeforeScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteMessagesBefore(ctx, messageID)
}

func (p *queuePersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (_ int64, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceEnqueueMessageToDLQScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceEnqueueMessageToDLQScope, callerInfo.CallerName, startTime, retErr)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceReadMessagesFromDLQScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceReadMessagesFromDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
}

func (p *queuePersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteMessageFromDLQScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteMessageFromDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.DeleteMessageFromDLQ(ctx, messageID)
}

func (p *queuePersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceRangeDeleteMessagesFromDLQScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceRangeDeleteMessagesFromDLQScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
}

func (p *queuePersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpdateDLQAckLevelScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpdateDLQAckLevelScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpdateDLQAckLevel(ctx, metadata)
}

func (p *queuePersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (_ *InternalQueueMetadata, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetDLQAckLevelScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetDLQAckLevelScope, callerInfo.CallerName, startTime, retErr)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceListClusterMetadataScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceListClusterMetadataScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.ListClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (_ *GetClusterMetadataResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetCurrentClusterMetadataScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetCurrentClusterMetadataScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetCurrentClusterMetadata(ctx)
}

func (p *clusterMetadataPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (_ *GetClusterMetadataResponse, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetClusterMetadataScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetClusterMetadataScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (_ bool, retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceSaveClusterMetadataScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceSaveClusterMetadataScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.SaveClusterMetadata(ctx, request)
}

func (p *clusterMetadataPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceDeleteClusterMetadataScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceDeleteClusterMetadataScope, callerInfo.CallerName, startTime, retErr)
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
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceGetClusterMembersScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceGetClusterMembersScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.GetClusterMembers(ctx, request)
}

func (p *clusterMetadataPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceUpsertClusterMembershipScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceUpsertClusterMembershipScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.UpsertClusterMembership(ctx, request)
}

func (p *clusterMetadataPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistencePruneClusterMembershipScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistencePruneClusterMembershipScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.PruneClusterMembership(ctx, request)
}

func (p *metadataPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) (retErr error) {
	callerInfo := headers.GetCallerInfo(ctx)
	signalFn := signalRecordFn(callerInfo, metrics.PersistenceInitializeSystemNamespaceScope, CallerSegmentMissing, p.healthSignals)
	startTime := time.Now().UTC()
	defer func() {
		signalFn(retErr)
		p.recordRequestMetrics(metrics.PersistenceInitializeSystemNamespaceScope, callerInfo.CallerName, startTime, retErr)
	}()
	return p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
}

func (p *metricEmitter) recordRequestMetrics(operation string, caller string, startTime time.Time, err error) {
	handler := p.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller))
	handler.Counter(metrics.PersistenceRequests.GetMetricName()).Record(1)
	handler.Timer(metrics.PersistenceLatency.GetMetricName()).Record(time.Since(startTime))
	updateErrorMetric(handler, p.logger, operation, err)
}

func updateErrorMetric(handler metrics.Handler, logger log.Logger, operation string, err error) {
	if err != nil && handler != metrics.NoopMetricsHandler {
		handler.Counter(metrics.PersistenceErrorWithType.GetMetricName()).Record(1, metrics.ServiceErrorTypeTag(err))
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
			handler.Counter(metrics.PersistenceErrResourceExhaustedCounter.GetMetricName()).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
		default:
			logger.Error("Operation failed with internal error.", tag.Error(err), tag.Operation(operation))
			handler.Counter(metrics.PersistenceFailures.GetMetricName()).Record(1)
		}
	}
}

func signalRecordFn(
	callerInfo headers.CallerInfo,
	api string,
	shardID int32,
	healthSignals HealthSignalAggregator,
) func(err error) {
	return healthSignals.GetRecordFn(quotas.NewRequest(
		api,
		RateLimitDefaultToken,
		callerInfo.CallerName,
		callerInfo.CallerType,
		shardID,
		callerInfo.CallOrigin,
	))
}
