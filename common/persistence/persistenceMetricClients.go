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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type (
	metricEmitter struct {
		metricClient metrics.Client
		logger       log.Logger
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
func NewShardPersistenceMetricsClient(persistence ShardManager, metricClient metrics.Client, logger log.Logger) ShardManager {
	return &shardPersistenceClient{
		metricEmitter: metricEmitter{
			metricClient: metricClient,
			logger:       logger,
		},
		persistence: persistence,
	}
}

// NewExecutionPersistenceMetricsClient creates a client to manage executions
func NewExecutionPersistenceMetricsClient(persistence ExecutionManager, metricClient metrics.Client, logger log.Logger) ExecutionManager {
	return &executionPersistenceClient{
		metricEmitter: metricEmitter{
			metricClient: metricClient,
			logger:       logger,
		},
		persistence: persistence,
	}
}

// NewTaskPersistenceMetricsClient creates a client to manage tasks
func NewTaskPersistenceMetricsClient(persistence TaskManager, metricClient metrics.Client, logger log.Logger) TaskManager {
	return &taskPersistenceClient{
		metricEmitter: metricEmitter{
			metricClient: metricClient,
			logger:       logger,
		},
		persistence: persistence,
	}
}

// NewMetadataPersistenceMetricsClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceMetricsClient(persistence MetadataManager, metricClient metrics.Client, logger log.Logger) MetadataManager {
	return &metadataPersistenceClient{
		metricEmitter: metricEmitter{
			metricClient: metricClient,
			logger:       logger,
		},
		persistence: persistence,
	}
}

// NewClusterMetadataPersistenceMetricsClient creates a ClusterMetadataManager client to manage cluster metadata
func NewClusterMetadataPersistenceMetricsClient(persistence ClusterMetadataManager, metricClient metrics.Client, logger log.Logger) ClusterMetadataManager {
	return &clusterMetadataPersistenceClient{
		metricEmitter: metricEmitter{
			metricClient: metricClient,
			logger:       logger,
		},
		persistence: persistence,
	}
}

// NewQueuePersistenceMetricsClient creates a client to manage queue
func NewQueuePersistenceMetricsClient(persistence Queue, metricClient metrics.Client, logger log.Logger) Queue {
	return &queuePersistenceClient{
		metricEmitter: metricEmitter{
			metricClient: metricClient,
			logger:       logger,
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
	p.metricClient.IncCounter(metrics.PersistenceGetOrCreateShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetOrCreateShardScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetOrCreateShard(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetOrCreateShardScope, err)
	}

	return response, err
}

func (p *shardPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateShardScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateShard(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateShardScope, err)
	}

	return err
}

func (p *shardPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceAssertShardOwnershipScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceAssertShardOwnershipScope, metrics.PersistenceLatency)
	err := p.persistence.AssertShardOwnership(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAssertShardOwnershipScope, err)
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
	p.metricClient.IncCounter(metrics.PersistenceCreateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceSetWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceSetWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceSetWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.PersistenceLatency)
	resp, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateWorkflowExecutionScope, err)
	}

	return resp, err
}

func (p *executionPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceConflictResolveWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceConflictResolveWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceConflictResolveWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteWorkflowExecutionScope, err)
	}

	return err
}

func (p *executionPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, err)
	}

	return err
}

func (p *executionPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetCurrentExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetCurrentExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetCurrentExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetCurrentExecutionScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListConcreteExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListConcreteExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListConcreteExecutionsScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceAddTasksScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceAddTasksScope, metrics.PersistenceLatency)
	err := p.persistence.AddHistoryTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAddTasksScope, err)
	}

	return err
}

func (p *executionPersistenceClient) GetHistoryTask(
	ctx context.Context,
	request *GetHistoryTaskRequest,
) (*GetHistoryTaskResponse, error) {
	var scopeIdx int
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		scopeIdx = metrics.PersistenceGetTransferTaskScope
	case tasks.CategoryIDTimer:
		scopeIdx = metrics.PersistenceGetTimerTaskScope
	case tasks.CategoryIDVisibility:
		scopeIdx = metrics.PersistenceGetVisibilityTaskScope
	case tasks.CategoryIDReplication:
		scopeIdx = metrics.PersistenceGetReplicationTaskScope
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	p.metricClient.IncCounter(scopeIdx, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(scopeIdx, metrics.PersistenceLatency)
	response, err := p.persistence.GetHistoryTask(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, err)
	}

	return response, err
}

func (p *executionPersistenceClient) GetHistoryTasks(
	ctx context.Context,
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	var scopeIdx int
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		scopeIdx = metrics.PersistenceGetTransferTasksScope
	case tasks.CategoryIDTimer:
		scopeIdx = metrics.PersistenceGetTimerTasksScope
	case tasks.CategoryIDVisibility:
		scopeIdx = metrics.PersistenceGetVisibilityTasksScope
	case tasks.CategoryIDReplication:
		scopeIdx = metrics.PersistenceGetReplicationTasksScope
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	p.metricClient.IncCounter(scopeIdx, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(scopeIdx, metrics.PersistenceLatency)
	response, err := p.persistence.GetHistoryTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, err)
	}

	return response, err
}

func (p *executionPersistenceClient) CompleteHistoryTask(
	ctx context.Context,
	request *CompleteHistoryTaskRequest,
) error {
	var scopeIdx int
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		scopeIdx = metrics.PersistenceCompleteTransferTaskScope
	case tasks.CategoryIDTimer:
		scopeIdx = metrics.PersistenceCompleteTimerTaskScope
	case tasks.CategoryIDVisibility:
		scopeIdx = metrics.PersistenceCompleteVisibilityTaskScope
	case tasks.CategoryIDReplication:
		scopeIdx = metrics.PersistenceCompleteReplicationTaskScope
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	p.metricClient.IncCounter(scopeIdx, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(scopeIdx, metrics.PersistenceLatency)
	err := p.persistence.CompleteHistoryTask(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, err)
	}

	return err
}

func (p *executionPersistenceClient) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *RangeCompleteHistoryTasksRequest,
) error {
	var scopeIdx int
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		scopeIdx = metrics.PersistenceRangeCompleteTransferTasksScope
	case tasks.CategoryIDTimer:
		scopeIdx = metrics.PersistenceRangeCompleteTimerTasksScope
	case tasks.CategoryIDVisibility:
		scopeIdx = metrics.PersistenceRangeCompleteVisibilityTasksScope
	case tasks.CategoryIDReplication:
		scopeIdx = metrics.PersistenceRangeCompleteReplicationTasksScope
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category type: %v", request.TaskCategory))
	}

	p.metricClient.IncCounter(scopeIdx, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(scopeIdx, metrics.PersistenceLatency)
	err := p.persistence.RangeCompleteHistoryTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, err)
	}

	return err
}

func (p *executionPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistencePutReplicationTaskToDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistencePutReplicationTaskToDLQScope, metrics.PersistenceLatency)
	err := p.persistence.PutReplicationTaskToDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistencePutReplicationTaskToDLQScope, err)
	}

	return err
}

func (p *executionPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetReplicationTasksFromDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetReplicationTasksFromDLQScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetReplicationTasksFromDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetReplicationTasksFromDLQScope, err)
	}

	return response, err
}

func (p *executionPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteReplicationTaskFromDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteReplicationTaskFromDLQScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteReplicationTaskFromDLQScope, err)
	}

	return nil
}

func (p *executionPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, metrics.PersistenceLatency)
	err := p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, err)
	}

	return nil
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
	p.metricClient.IncCounter(metrics.PersistenceCreateTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateTaskScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateTaskScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetTasksScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetTasksScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTasksScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceCompleteTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTask(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTaskScope, err)
	}

	return err
}

func (p *taskPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	p.metricClient.IncCounter(metrics.PersistenceCompleteTasksLessThanScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteTasksLessThanScope, metrics.PersistenceLatency)
	result, err := p.persistence.CompleteTasksLessThan(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTasksLessThanScope, err)
	}
	return result, err
}

func (p *taskPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceCreateTaskQueueScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateTaskQueueScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateTaskQueueScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceUpdateTaskQueueScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateTaskQueueScope, metrics.PersistenceLatency)
	response, err := p.persistence.UpdateTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateTaskQueueScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetTaskQueueScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceGetTaskQueueScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetTaskQueue(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTaskQueueScope, err)
	}
	return response, err
}

func (p *taskPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListTaskQueueScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceListTaskQueueScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListTaskQueue(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListTaskQueueScope, err)
	}
	return response, err
}

func (p *taskPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteTaskQueueScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteTaskQueueScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteTaskQueue(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteTaskQueueScope, err)
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
	p.metricClient.IncCounter(metrics.PersistenceCreateNamespaceScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateNamespaceScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateNamespaceScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetNamespaceScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetNamespaceScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetNamespaceScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateNamespaceScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateNamespaceScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateNamespaceScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceRenameNamespaceScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRenameNamespaceScope, metrics.PersistenceLatency)
	err := p.persistence.RenameNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRenameNamespaceScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteNamespaceScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteNamespaceScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteNamespaceScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteNamespaceByNameScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteNamespaceByNameScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteNamespaceByName(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteNamespaceByNameScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListNamespaceScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListNamespaceScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListNamespaces(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListNamespaceScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetMetadataScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetMetadataScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetMetadata(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetMetadataScope, err)
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
	p.metricClient.IncCounter(metrics.PersistenceAppendHistoryNodesScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceAppendHistoryNodesScope, metrics.PersistenceLatency)
	resp, err := p.persistence.AppendHistoryNodes(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendHistoryNodesScope, err)
	}
	return resp, err
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceAppendRawHistoryNodesScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceAppendRawHistoryNodesScope, metrics.PersistenceLatency)
	resp, err := p.persistence.AppendRawHistoryNodes(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendRawHistoryNodesScope, err)
	}
	return resp, err
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, err)
	}
	return response, err
}

func (p *executionPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchReverseScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchReverseScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchReverseScope, err)
	}
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
func (p *executionPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, err)
	}
	return response, err
}

// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
func (p *executionPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, err)
	}
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *executionPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceForkHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceForkHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceForkHistoryBranchScope, err)
	}
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *executionPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteHistoryBranchScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteHistoryBranch(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteHistoryBranchScope, err)
	}
	return err
}

// TrimHistoryBranch trims a branch
func (p *executionPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceTrimHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceTrimHistoryBranchScope, metrics.PersistenceLatency)
	resp, err := p.persistence.TrimHistoryBranch(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceTrimHistoryBranchScope, err)
	}
	return resp, err
}

func (p *executionPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetAllHistoryTreeBranchesScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceGetAllHistoryTreeBranchesScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetAllHistoryTreeBranchesScope, err)
	}
	return response, err
}

// GetHistoryTree returns all branch information of a tree
func (p *executionPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetHistoryTreeScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceGetHistoryTreeScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetHistoryTree(ctx, request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetHistoryTreeScope, err)
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
	p.metricClient.IncCounter(metrics.PersistenceEnqueueMessageScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceEnqueueMessageScope, metrics.PersistenceLatency)
	err := p.persistence.EnqueueMessage(ctx, blob)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceEnqueueMessageScope, err)
	}

	return err
}

func (p *queuePersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadQueueMessagesScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceReadQueueMessagesScope, metrics.PersistenceLatency)
	result, err := p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadQueueMessagesScope, err)
	}

	return result, err
}

func (p *queuePersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateAckLevelScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateAckLevelScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateAckLevel(ctx, metadata)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateAckLevelScope, err)
	}

	return err
}

func (p *queuePersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetAckLevelScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetAckLevelScope, metrics.PersistenceLatency)
	result, err := p.persistence.GetAckLevels(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetAckLevelScope, err)
	}

	return result, err
}

func (p *queuePersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteQueueMessagesScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteQueueMessagesScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteMessagesBefore(ctx, messageID)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteQueueMessagesScope, err)
	}

	return err
}

func (p *queuePersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	p.metricClient.IncCounter(metrics.PersistenceEnqueueMessageToDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceEnqueueMessageToDLQScope, metrics.PersistenceLatency)
	messageID, err := p.persistence.EnqueueMessageToDLQ(ctx, blob)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceEnqueueMessageToDLQScope, err)
	}

	return messageID, err
}

func (p *queuePersistenceClient) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*QueueMessage, []byte, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadQueueMessagesFromDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceReadQueueMessagesFromDLQScope, metrics.PersistenceLatency)
	result, token, err := p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadQueueMessagesFromDLQScope, err)
	}

	return result, token, err
}

func (p *queuePersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteQueueMessageFromDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteQueueMessageFromDLQScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteMessageFromDLQ(ctx, messageID)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteQueueMessageFromDLQScope, err)
	}

	return err
}

func (p *queuePersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	p.metricClient.IncCounter(metrics.PersistenceRangeDeleteMessagesFromDLQScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRangeDeleteMessagesFromDLQScope, metrics.PersistenceLatency)
	err := p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeDeleteMessagesFromDLQScope, err)
	}

	return err
}

func (p *queuePersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateDLQAckLevelScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateDLQAckLevelScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateDLQAckLevel(ctx, metadata)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateDLQAckLevelScope, err)
	}

	return err
}

func (p *queuePersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetDLQAckLevelScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetDLQAckLevelScope, metrics.PersistenceLatency)
	result, err := p.persistence.GetDLQAckLevels(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetDLQAckLevelScope, err)
	}

	return result, err
}

func (p *queuePersistenceClient) Close() {
	p.persistence.Close()
}

func (c *clusterMetadataPersistenceClient) Close() {
	c.persistence.Close()
}

func (c *clusterMetadataPersistenceClient) ListClusterMetadata(
	ctx context.Context,
	request *ListClusterMetadataRequest,
) (*ListClusterMetadataResponse, error) {
	// This is a wrapper of GetClusterMetadata API, use the same scope here
	c.metricClient.IncCounter(metrics.PersistenceListClusterMetadataScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceListClusterMetadataScope, metrics.PersistenceLatency)
	result, err := c.persistence.ListClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceListClusterMetadataScope, err)
	}

	return result, err
}

func (c *clusterMetadataPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	// This is a wrapper of GetClusterMetadata API, use the same scope here
	c.metricClient.IncCounter(metrics.PersistenceGetClusterMetadataScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceGetClusterMetadataScope, metrics.PersistenceLatency)
	result, err := c.persistence.GetCurrentClusterMetadata(ctx)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceGetClusterMetadataScope, err)
	}

	return result, err
}

func (c *clusterMetadataPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	c.metricClient.IncCounter(metrics.PersistenceGetClusterMetadataScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceGetClusterMetadataScope, metrics.PersistenceLatency)
	result, err := c.persistence.GetClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceGetClusterMetadataScope, err)
	}

	return result, err
}

func (c *clusterMetadataPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	c.metricClient.IncCounter(metrics.PersistenceSaveClusterMetadataScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceSaveClusterMetadataScope, metrics.PersistenceLatency)
	applied, err := c.persistence.SaveClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceSaveClusterMetadataScope, err)
	}

	return applied, err
}

func (c *clusterMetadataPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	c.metricClient.IncCounter(metrics.PersistenceDeleteClusterMetadataScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceDeleteClusterMetadataScope, metrics.PersistenceLatency)
	err := c.persistence.DeleteClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceDeleteClusterMetadataScope, err)
	}

	return err
}

func (c *clusterMetadataPersistenceClient) GetName() string {
	return c.persistence.GetName()
}

func (c *clusterMetadataPersistenceClient) GetClusterMembers(
	ctx context.Context,
	request *GetClusterMembersRequest,
) (*GetClusterMembersResponse, error) {
	c.metricClient.IncCounter(metrics.PersistenceGetClusterMembersScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceGetClusterMembersScope, metrics.PersistenceLatency)
	res, err := c.persistence.GetClusterMembers(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceGetClusterMembersScope, err)
	}

	return res, err
}

func (c *clusterMetadataPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	c.metricClient.IncCounter(metrics.PersistenceUpsertClusterMembershipScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceUpsertClusterMembershipScope, metrics.PersistenceLatency)
	err := c.persistence.UpsertClusterMembership(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceUpsertClusterMembershipScope, err)
	}

	return err
}

func (c *clusterMetadataPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	c.metricClient.IncCounter(metrics.PersistencePruneClusterMembershipScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistencePruneClusterMembershipScope, metrics.PersistenceLatency)
	err := c.persistence.PruneClusterMembership(ctx, request)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistencePruneClusterMembershipScope, err)
	}

	return err
}

func (c *metadataPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	c.metricClient.IncCounter(metrics.PersistenceInitializeSystemNamespaceScope, metrics.PersistenceRequests)

	sw := c.metricClient.StartTimer(metrics.PersistenceInitializeSystemNamespaceScope, metrics.PersistenceLatency)
	err := c.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
	sw.Stop()

	if err != nil {
		c.updateErrorMetric(metrics.PersistenceInitializeSystemNamespaceScope, err)
	}

	return err
}

func (p *metricEmitter) updateErrorMetric(scope int, err error) {
	p.metricClient.Scope(scope, metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.PersistenceErrorWithType)

	switch err.(type) {
	case *ShardAlreadyExistError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrShardExistsCounter)
	case *ShardOwnershipLostError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrShardOwnershipLostCounter)
	case *CurrentWorkflowConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrCurrentWorkflowConditionFailedCounter)
	case *WorkflowConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrWorkflowConditionFailedCounter)
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *serviceerror.InvalidArgument:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBadRequestCounter)
	case *serviceerror.NamespaceAlreadyExists:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrNamespaceAlreadyExistsCounter)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *serviceerror.ResourceExhausted:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.", tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}
