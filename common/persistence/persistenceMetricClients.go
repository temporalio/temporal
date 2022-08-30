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
		persistence ShardManager
	}

	executionPersistenceClient struct {
		metricEmitter
		persistence ExecutionManager
	}

	taskPersistenceClient struct {
		metricEmitter
		persistence TaskManager
	}

	metadataPersistenceClient struct {
		metricEmitter
		persistence MetadataManager
	}

	clusterMetadataPersistenceClient struct {
		metricEmitter
		persistence ClusterMetadataManager
	}

	queuePersistenceClient struct {
		metricEmitter
		persistence Queue
	}
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*executionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)
var _ MetadataManager = (*metadataPersistenceClient)(nil)
var _ ClusterMetadataManager = (*clusterMetadataPersistenceClient)(nil)
var _ Queue = (*queuePersistenceClient)(nil)

// NewShardPersistenceMetricsClient creates a client to manage shards
func NewShardPersistenceMetricsClient(persistence ShardManager, metricsHandler metrics.Handler, logger log.Logger) ShardManager {
	return &shardPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		persistence: persistence,
	}
}

// NewExecutionPersistenceMetricsClient creates a client to manage executions
func NewExecutionPersistenceMetricsClient(persistence ExecutionManager, metricsHandler metrics.Handler, logger log.Logger) ExecutionManager {
	return &executionPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		persistence: persistence,
	}
}

// NewTaskPersistenceMetricsClient creates a client to manage tasks
func NewTaskPersistenceMetricsClient(persistence TaskManager, metricsHandler metrics.Handler, logger log.Logger) TaskManager {
	return &taskPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		persistence: persistence,
	}
}

// NewMetadataPersistenceMetricsClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceMetricsClient(persistence MetadataManager, metricsHandler metrics.Handler, logger log.Logger) MetadataManager {
	return &metadataPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		persistence: persistence,
	}
}

// NewClusterMetadataPersistenceMetricsClient creates a ClusterMetadataManager client to manage cluster metadata
func NewClusterMetadataPersistenceMetricsClient(persistence ClusterMetadataManager, metricsHandler metrics.Handler, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataPersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		persistence: persistence,
	}
}

// NewQueuePersistenceMetricsClient creates a client to manage queue
func NewQueuePersistenceMetricsClient(persistence Queue, metricsHandler metrics.Handler, logger log.Logger) Queue {
	return &queuePersistenceClient{
		metricEmitter: metricEmitter{
			metricsHandler: metricsHandler,
			logger:         logger,
		},
		persistence: persistence,
	}
}

func (p *shardPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardPersistenceClient) GetOrCreateShard(
	ctx context.Context,
	request *GetOrCreateShardRequest,
) (*GetOrCreateShardResponse, error) {
	operationName := metrics.PersistenceGetOrCreateShardOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(operationName))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetOrCreateShard(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *shardPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	operationName := metrics.PersistenceUpdateShardOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(operationName))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.UpdateShard(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *shardPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	operationName := metrics.PersistenceAssertShardOwnershipOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(operationName))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.AssertShardOwnership(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *shardPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *executionPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *executionPersistenceClient) CreateWorkflowExecution(
	ctx context.Context,
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	operationName := metrics.PersistenceCreateWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(operationName))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))

	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}

	return response, err
}

func (p *executionPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	operationName := metrics.PersistenceGetWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetWorkflowExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	operationName := metrics.PersistenceSetWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceSetWorkflowExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	operationName := metrics.PersistenceUpdateWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateWorkflowExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	operationName := metrics.PersistenceConflictResolveWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceConflictResolveWorkflowExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	operationName := metrics.PersistenceDeleteWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteWorkflowExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	operationName := metrics.PersistenceDeleteCurrentWorkflowExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteCurrentWorkflowExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	operationName := metrics.PersistenceGetCurrentExecutionOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetCurrentExecutionOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetCurrentExecution(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	operationName := metrics.PersistenceListConcreteExecutionsOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceListConcreteExecutionsOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	operationName := metrics.PersistenceAddTasksOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceAddTasksOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.AddHistoryTasks(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) GetHistoryTask(
	ctx context.Context,
	request *GetHistoryTaskRequest,
) (*GetHistoryTaskResponse, error) {
	taskCategory, err := mapTaskCategoryIDToOperationTag(request.TaskCategory.ID())
	if err != nil {
		return nil, err
	}

	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(taskCategory))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetHistoryTask(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(taskCategory, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	taskCategory, err := mapTasksCategoryIDToOperationTag(request.TaskCategory.ID())
	if err != nil {
		return nil, err
	}

	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(taskCategory))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetHistoryTasks(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(taskCategory, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	taskCategory, err := mapTaskCategoryIDToOperationTag(request.TaskCategory.ID())
	if err != nil {
		return err
	}

	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(taskCategory))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err = p.persistence.CompleteHistoryTask(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(taskCategory, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	taskCategory, err := mapTasksCategoryIDToOperationTag(request.TaskCategory.ID())
	if err != nil {
		return err
	}

	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(taskCategory))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err = p.persistence.RangeCompleteHistoryTasks(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(taskCategory, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	operationName := metrics.PersistencePutReplicationTaskToDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistencePutReplicationTaskToDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.PutReplicationTaskToDLQ(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	operationName := metrics.PersistenceGetReplicationTasksFromDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetReplicationTasksFromDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetReplicationTasksFromDLQ(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	operationName := metrics.PersistenceDeleteReplicationTaskFromDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteReplicationTaskFromDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *executionPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	operationName := metrics.PersistenceRangeDeleteReplicationTaskFromDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceRangeDeleteReplicationTaskFromDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
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
) (*CreateTasksResponse, error) {
	operationName := metrics.PersistenceCreateTaskOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceCreateTaskOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.CreateTasks(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	operationName := metrics.PersistenceGetTasksOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetTasksOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetTasks(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) error {
	operationName := metrics.PersistenceCompleteTaskOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceCompleteTaskOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.CompleteTask(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *taskPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	operationName := metrics.PersistenceCompleteTasksLessThanOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceCompleteTasksLessThanOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.CompleteTasksLessThan(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	operationName := metrics.PersistenceCreateTaskQueueOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceCreateTaskQueueOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.CreateTaskQueue(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	operationName := metrics.PersistenceUpdateTaskQueueOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateTaskQueueOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.UpdateTaskQueue(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	operationName := metrics.PersistenceGetTaskQueueOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetTaskQueueOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetTaskQueue(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	operationName := metrics.PersistenceListTaskQueueOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceListTaskQueueOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ListTaskQueue(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *taskPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	operationName := metrics.PersistenceDeleteTaskQueueOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteTaskQueueOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteTaskQueue(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
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
) (*CreateNamespaceResponse, error) {
	operationName := metrics.PersistenceCreateNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceCreateNamespaceOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.CreateNamespace(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *metadataPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	operationName := metrics.PersistenceGetNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetNamespaceOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetNamespace(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *metadataPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	operationName := metrics.PersistenceUpdateNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateNamespaceOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.UpdateNamespace(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *metadataPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	operationName := metrics.PersistenceRenameNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceRenameNamespaceOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.RenameNamespace(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *metadataPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	operationName := metrics.PersistenceDeleteNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteNamespaceOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteNamespace(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *metadataPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	operationName := metrics.PersistenceDeleteNamespaceByNameOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteNamespaceByNameOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteNamespaceByName(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *metadataPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	operationName := metrics.PersistenceListNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceListNamespaceOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ListNamespaces(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *metadataPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	operationName := metrics.PersistenceGetMetadataOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetMetadataOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetMetadata(ctx)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *metadataPersistenceClient) Close() {
	p.persistence.Close()
}

// AppendHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	operationName := metrics.PersistenceAppendHistoryNodesOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceAppendHistoryNodesOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.AppendHistoryNodes(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	operationName := metrics.PersistenceAppendRawHistoryNodesOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceAppendRawHistoryNodesOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.AppendRawHistoryNodes(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// ParseHistoryBranchInfo parses the history branch for branch information
func (p *executionPersistenceClient) ParseHistoryBranchInfo(
	ctx context.Context,
	request *ParseHistoryBranchInfoRequest,
) (*ParseHistoryBranchInfoResponse, error) {
	operationName := metrics.PersistenceParseHistoryBranchInfoOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceParseHistoryBranchInfoOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ParseHistoryBranchInfo(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// UpdateHistoryBranchInfo updates the history branch with branch information
func (p *executionPersistenceClient) UpdateHistoryBranchInfo(
	ctx context.Context,
	request *UpdateHistoryBranchInfoRequest,
) (*UpdateHistoryBranchInfoResponse, error) {
	operationName := metrics.PersistenceUpdateHistoryBranchInfoOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateHistoryBranchInfoOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.UpdateHistoryBranchInfo(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// NewHistoryBranch initializes a new history branch
func (p *executionPersistenceClient) NewHistoryBranch(
	ctx context.Context,
	request *NewHistoryBranchRequest,
) (*NewHistoryBranchResponse, error) {
	operationName := metrics.PersistenceNewHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceNewHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.NewHistoryBranch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	operationName := metrics.PersistenceReadHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceReadHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	operationName := metrics.PersistenceReadHistoryBranchReverseOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceReadHistoryBranchReverseOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
func (p *executionPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	operationName := metrics.PersistenceReadHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceReadHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
func (p *executionPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	operationName := metrics.PersistenceReadHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceReadHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *executionPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	operationName := metrics.PersistenceForkHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceForkHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *executionPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	operationName := metrics.PersistenceDeleteHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteHistoryBranch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

// TrimHistoryBranch trims a branch
func (p *executionPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	operationName := metrics.PersistenceTrimHistoryBranchOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceTrimHistoryBranchOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.TrimHistoryBranch(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *executionPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	operationName := metrics.PersistenceGetAllHistoryTreeBranchesOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetAllHistoryTreeBranchesOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

// GetHistoryTree returns all branch information of a tree
func (p *executionPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {
	operationName := metrics.PersistenceGetHistoryTreeOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetHistoryTreeOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetHistoryTree(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
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
) error {
	operationName := metrics.PersistenceEnqueueMessageOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceEnqueueMessageOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.EnqueueMessage(ctx, blob)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *queuePersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	operationName := metrics.PersistenceReadQueueMessagesOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceReadQueueMessagesOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *queuePersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	operationName := metrics.PersistenceUpdateAckLevelOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateAckLevelOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.UpdateAckLevel(ctx, metadata)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *queuePersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	operationName := metrics.PersistenceGetAckLevelOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetAckLevelOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetAckLevels(ctx)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *queuePersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	operationName := metrics.PersistenceDeleteQueueMessagesOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteQueueMessagesOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteMessagesBefore(ctx, messageID)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *queuePersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	operationName := metrics.PersistenceEnqueueMessageToDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceEnqueueMessageToDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.EnqueueMessageToDLQ(ctx, blob)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *queuePersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*QueueMessage, []byte, error) {
	operationName := metrics.PersistenceReadQueueMessagesFromDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceReadQueueMessagesFromDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, token, err := p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, token, err
}

func (p *queuePersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	operationName := metrics.PersistenceDeleteQueueMessageFromDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteQueueMessageFromDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteMessageFromDLQ(ctx, messageID)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *queuePersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	operationName := metrics.PersistenceRangeDeleteMessagesFromDLQOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceRangeDeleteMessagesFromDLQOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *queuePersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	operationName := metrics.PersistenceUpdateDLQAckLevelOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateDLQAckLevelOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.UpdateDLQAckLevel(ctx, metadata)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *queuePersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	operationName := metrics.PersistenceGetDLQAckLevelOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetDLQAckLevelOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetDLQAckLevels(ctx)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
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
) (*ListClusterMetadataResponse, error) {
	operationName := metrics.PersistenceListClusterMetadataOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceListClusterMetadataOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.ListClusterMetadata(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *clusterMetadataPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	operationName := metrics.PersistenceGetClusterMetadataOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetClusterMetadataOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetCurrentClusterMetadata(ctx)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *clusterMetadataPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	operationName := metrics.PersistenceGetClusterMetadataOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetClusterMetadataOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetClusterMetadata(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *clusterMetadataPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	operationName := metrics.PersistenceSaveClusterMetadataOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceSaveClusterMetadataOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.SaveClusterMetadata(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *clusterMetadataPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	operationName := metrics.PersistenceDeleteClusterMetadataOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceDeleteClusterMetadataOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.DeleteClusterMetadata(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *clusterMetadataPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *clusterMetadataPersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	operationName := metrics.PersistenceGetClusterMembersOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceGetClusterMembersOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	response, err := p.persistence.GetClusterMembers(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return response, err
}

func (p *clusterMetadataPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	operationName := metrics.PersistenceUpsertClusterMembershipOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpsertClusterMembershipOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.UpsertClusterMembership(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *clusterMetadataPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	operationName := metrics.PersistencePruneClusterMembershipOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistencePruneClusterMembershipOperation))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.PruneClusterMembership(ctx, request)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *metadataPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	operationName := metrics.PersistenceInitializeSystemNamespaceOperation
	metricsHandler := p.metricsHandler.WithTags(metrics.OperationTag(operationName))
	metricsHandler.Counter(metrics.PersistenceRequests.MetricName.String()).Record(1)

	startTime := time.Now()
	err := p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
	metricsHandler.Timer(metrics.PersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	if err != nil {
		p.updateErrorMetric(operationName, metricsHandler, err)
	}
	return err
}

func (p *metricEmitter) updateErrorMetric(operation string, handler metrics.Handler, err error) {

	handler.Counter(metrics.PersistenceErrorWithType.MetricName.String()).Record(1, metrics.ServiceErrorTypeTag(err))
	switch err := err.(type) {
	case *ShardAlreadyExistError,
		*ShardOwnershipLostError,
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
		handler.Counter(metrics.PersistenceErrResourceExhaustedCounter.MetricName.String()).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
	default:
		p.logger.Error("Operation failed with internal error.", tag.Error(err), tag.Operation(operation))
		handler.Counter(metrics.PersistenceFailures.MetricName.String()).Record(1)
	}
}

func mapTaskCategoryIDToOperationTag(categoryID int32) (string, error) {
	switch categoryID {
	case tasks.CategoryIDTransfer:
		return metrics.PersistenceGetTransferTaskOperation, nil
	case tasks.CategoryIDTimer:
		return metrics.PersistenceGetTimerTaskOperation, nil
	case tasks.CategoryIDVisibility:
		return metrics.PersistenceGetVisibilityTaskOperation, nil
	case tasks.CategoryIDReplication:
		return metrics.PersistenceGetReplicationTaskOperation, nil
	default:
		return "", serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", categoryID))
	}
}

func mapTasksCategoryIDToOperationTag(categoryID int32) (string, error) {
	switch categoryID {
	case tasks.CategoryIDTransfer:
		return metrics.PersistenceGetTransferTasksOperation, nil
	case tasks.CategoryIDTimer:
		return metrics.PersistenceGetTimerTasksOperation, nil
	case tasks.CategoryIDVisibility:
		return metrics.PersistenceGetVisibilityTasksOperation, nil
	case tasks.CategoryIDReplication:
		return metrics.PersistenceGetReplicationTasksOperation, nil
	default:
		return "", serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", categoryID))
	}
}
