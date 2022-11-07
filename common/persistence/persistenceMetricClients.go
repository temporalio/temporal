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

	"go.temporal.io/server/common/headers"
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetOrCreateShardScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetOrCreateShardScope, caller)
	response, err := p.persistence.GetOrCreateShard(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetOrCreateShardScope, caller, err)
	}

	return response, err
}

func (p *shardPersistenceClient) UpdateShard(
	ctx context.Context,
	request *UpdateShardRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateShardScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateShardScope, caller)
	err := p.persistence.UpdateShard(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateShardScope, caller, err)
	}

	return err
}

func (p *shardPersistenceClient) AssertShardOwnership(
	ctx context.Context,
	request *AssertShardOwnershipRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceAssertShardOwnershipScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceAssertShardOwnershipScope, caller)
	err := p.persistence.AssertShardOwnership(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAssertShardOwnershipScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceCreateWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceCreateWorkflowExecutionScope, caller)
	response, err := p.persistence.CreateWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateWorkflowExecutionScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) GetWorkflowExecution(
	ctx context.Context,
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetWorkflowExecutionScope, caller)
	response, err := p.persistence.GetWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetWorkflowExecutionScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) SetWorkflowExecution(
	ctx context.Context,
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceSetWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceSetWorkflowExecutionScope, caller)
	response, err := p.persistence.SetWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceSetWorkflowExecutionScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateWorkflowExecutionScope, caller)
	resp, err := p.persistence.UpdateWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateWorkflowExecutionScope, caller, err)
	}

	return resp, err
}

func (p *executionPersistenceClient) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceConflictResolveWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceConflictResolveWorkflowExecutionScope, caller)
	response, err := p.persistence.ConflictResolveWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceConflictResolveWorkflowExecutionScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *DeleteWorkflowExecutionRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteWorkflowExecutionScope, caller)
	err := p.persistence.DeleteWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteWorkflowExecutionScope, caller, err)
	}

	return err
}

func (p *executionPersistenceClient) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, caller)
	err := p.persistence.DeleteCurrentWorkflowExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, caller, err)
	}

	return err
}

func (p *executionPersistenceClient) GetCurrentExecution(
	ctx context.Context,
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetCurrentExecutionScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetCurrentExecutionScope, caller)
	response, err := p.persistence.GetCurrentExecution(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetCurrentExecutionScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) ListConcreteExecutions(
	ctx context.Context,
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceListConcreteExecutionsScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceListConcreteExecutionsScope, caller)
	response, err := p.persistence.ListConcreteExecutions(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListConcreteExecutionsScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) AddHistoryTasks(
	ctx context.Context,
	request *AddHistoryTasksRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceAddTasksScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceAddTasksScope, caller)
	err := p.persistence.AddHistoryTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAddTasksScope, caller, err)
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

	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(scopeIdx, caller)

	sw := p.startLatencyTimer(scopeIdx, caller)
	response, err := p.persistence.GetHistoryTask(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, caller, err)
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

	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(scopeIdx, caller)

	sw := p.startLatencyTimer(scopeIdx, caller)
	response, err := p.persistence.GetHistoryTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, caller, err)
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

	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(scopeIdx, caller)

	sw := p.startLatencyTimer(scopeIdx, caller)
	err := p.persistence.CompleteHistoryTask(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, caller, err)
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

	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(scopeIdx, caller)

	sw := p.startLatencyTimer(scopeIdx, caller)
	err := p.persistence.RangeCompleteHistoryTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(scopeIdx, caller, err)
	}

	return err
}

func (p *executionPersistenceClient) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *PutReplicationTaskToDLQRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistencePutReplicationTaskToDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistencePutReplicationTaskToDLQScope, caller)
	err := p.persistence.PutReplicationTaskToDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistencePutReplicationTaskToDLQScope, caller, err)
	}

	return err
}

func (p *executionPersistenceClient) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *GetReplicationTasksFromDLQRequest,
) (*GetHistoryTasksResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetReplicationTasksFromDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetReplicationTasksFromDLQScope, caller)
	response, err := p.persistence.GetReplicationTasksFromDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetReplicationTasksFromDLQScope, caller, err)
	}

	return response, err
}

func (p *executionPersistenceClient) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteReplicationTaskFromDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteReplicationTaskFromDLQScope, caller)
	err := p.persistence.DeleteReplicationTaskFromDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteReplicationTaskFromDLQScope, caller, err)
	}

	return nil
}

func (p *executionPersistenceClient) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, caller)
	err := p.persistence.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeDeleteReplicationTaskFromDLQScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceCreateTaskScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceCreateTaskScope, caller)
	response, err := p.persistence.CreateTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateTaskScope, caller, err)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(
	ctx context.Context,
	request *GetTasksRequest,
) (*GetTasksResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetTasksScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetTasksScope, caller)
	response, err := p.persistence.GetTasks(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTasksScope, caller, err)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(
	ctx context.Context,
	request *CompleteTaskRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceCompleteTaskScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceCompleteTaskScope, caller)
	err := p.persistence.CompleteTask(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTaskScope, caller, err)
	}

	return err
}

func (p *taskPersistenceClient) CompleteTasksLessThan(
	ctx context.Context,
	request *CompleteTasksLessThanRequest,
) (int, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceCompleteTasksLessThanScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceCompleteTasksLessThanScope, caller)
	result, err := p.persistence.CompleteTasksLessThan(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTasksLessThanScope, caller, err)
	}
	return result, err
}

func (p *taskPersistenceClient) CreateTaskQueue(
	ctx context.Context,
	request *CreateTaskQueueRequest,
) (*CreateTaskQueueResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceCreateTaskQueueScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceCreateTaskQueueScope, caller)
	response, err := p.persistence.CreateTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateTaskQueueScope, caller, err)
	}

	return response, err
}

func (p *taskPersistenceClient) UpdateTaskQueue(
	ctx context.Context,
	request *UpdateTaskQueueRequest,
) (*UpdateTaskQueueResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateTaskQueueScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateTaskQueueScope, caller)
	response, err := p.persistence.UpdateTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateTaskQueueScope, caller, err)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTaskQueue(
	ctx context.Context,
	request *GetTaskQueueRequest,
) (*GetTaskQueueResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetTaskQueueScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetTaskQueueScope, caller)
	response, err := p.persistence.GetTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTaskQueueScope, caller, err)
	}
	return response, err
}

func (p *taskPersistenceClient) ListTaskQueue(
	ctx context.Context,
	request *ListTaskQueueRequest,
) (*ListTaskQueueResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceListTaskQueueScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceListTaskQueueScope, caller)
	response, err := p.persistence.ListTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListTaskQueueScope, caller, err)
	}
	return response, err
}

func (p *taskPersistenceClient) DeleteTaskQueue(
	ctx context.Context,
	request *DeleteTaskQueueRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteTaskQueueScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteTaskQueueScope, caller)
	err := p.persistence.DeleteTaskQueue(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteTaskQueueScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceCreateNamespaceScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceCreateNamespaceScope, caller)
	response, err := p.persistence.CreateNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateNamespaceScope, caller, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) GetNamespace(
	ctx context.Context,
	request *GetNamespaceRequest,
) (*GetNamespaceResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetNamespaceScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetNamespaceScope, caller)
	response, err := p.persistence.GetNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetNamespaceScope, caller, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) UpdateNamespace(
	ctx context.Context,
	request *UpdateNamespaceRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateNamespaceScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateNamespaceScope, caller)
	err := p.persistence.UpdateNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateNamespaceScope, caller, err)
	}

	return err
}

func (p *metadataPersistenceClient) RenameNamespace(
	ctx context.Context,
	request *RenameNamespaceRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceRenameNamespaceScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceRenameNamespaceScope, caller)
	err := p.persistence.RenameNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRenameNamespaceScope, caller, err)
	}

	return err
}

func (p *metadataPersistenceClient) DeleteNamespace(
	ctx context.Context,
	request *DeleteNamespaceRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteNamespaceScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteNamespaceScope, caller)
	err := p.persistence.DeleteNamespace(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteNamespaceScope, caller, err)
	}

	return err
}

func (p *metadataPersistenceClient) DeleteNamespaceByName(
	ctx context.Context,
	request *DeleteNamespaceByNameRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteNamespaceByNameScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteNamespaceByNameScope, caller)
	err := p.persistence.DeleteNamespaceByName(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteNamespaceByNameScope, caller, err)
	}

	return err
}

func (p *metadataPersistenceClient) ListNamespaces(
	ctx context.Context,
	request *ListNamespacesRequest,
) (*ListNamespacesResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceListNamespaceScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceListNamespaceScope, caller)
	response, err := p.persistence.ListNamespaces(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListNamespaceScope, caller, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) GetMetadata(
	ctx context.Context,
) (*GetMetadataResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetMetadataScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetMetadataScope, caller)
	response, err := p.persistence.GetMetadata(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetMetadataScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceAppendHistoryNodesScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceAppendHistoryNodesScope, caller)
	resp, err := p.persistence.AppendHistoryNodes(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendHistoryNodesScope, caller, err)
	}
	return resp, err
}

// AppendRawHistoryNodes add a node to history node table
func (p *executionPersistenceClient) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceAppendRawHistoryNodesScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceAppendRawHistoryNodesScope, caller)
	resp, err := p.persistence.AppendRawHistoryNodes(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendRawHistoryNodesScope, caller, err)
	}
	return resp, err
}

// ParseHistoryBranchInfo parses the history branch for branch information
func (p *executionPersistenceClient) ParseHistoryBranchInfo(
	ctx context.Context,
	request *ParseHistoryBranchInfoRequest,
) (*ParseHistoryBranchInfoResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceParseHistoryBranchInfoScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceParseHistoryBranchInfoScope, caller)
	resp, err := p.persistence.ParseHistoryBranchInfo(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceParseHistoryBranchInfoScope, caller, err)
	}
	return resp, err
}

// UpdateHistoryBranchInfo updates the history branch with branch information
func (p *executionPersistenceClient) UpdateHistoryBranchInfo(
	ctx context.Context,
	request *UpdateHistoryBranchInfoRequest,
) (*UpdateHistoryBranchInfoResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateHistoryBranchInfoScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateHistoryBranchInfoScope, caller)
	resp, err := p.persistence.UpdateHistoryBranchInfo(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateHistoryBranchInfoScope, caller, err)
	}
	return resp, err
}

// NewHistoryBranch initializes a new history branch
func (p *executionPersistenceClient) NewHistoryBranch(
	ctx context.Context,
	request *NewHistoryBranchRequest,
) (*NewHistoryBranchResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceNewHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceNewHistoryBranchScope, caller)
	response, err := p.persistence.NewHistoryBranch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceNewHistoryBranchScope, caller, err)
	}
	return response, err
}

// ReadHistoryBranch returns history node data for a branch
func (p *executionPersistenceClient) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceReadHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceReadHistoryBranchScope, caller)
	response, err := p.persistence.ReadHistoryBranch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, caller, err)
	}
	return response, err
}

func (p *executionPersistenceClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceReadHistoryBranchReverseScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceReadHistoryBranchReverseScope, caller)
	response, err := p.persistence.ReadHistoryBranchReverse(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchReverseScope, caller, err)
	}
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
func (p *executionPersistenceClient) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceReadHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceReadHistoryBranchScope, caller)
	response, err := p.persistence.ReadHistoryBranchByBatch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, caller, err)
	}
	return response, err
}

// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
func (p *executionPersistenceClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceReadHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceReadHistoryBranchScope, caller)
	response, err := p.persistence.ReadRawHistoryBranch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, caller, err)
	}
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *executionPersistenceClient) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceForkHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceForkHistoryBranchScope, caller)
	response, err := p.persistence.ForkHistoryBranch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceForkHistoryBranchScope, caller, err)
	}
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *executionPersistenceClient) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteHistoryBranchScope, caller)
	err := p.persistence.DeleteHistoryBranch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteHistoryBranchScope, caller, err)
	}
	return err
}

// TrimHistoryBranch trims a branch
func (p *executionPersistenceClient) TrimHistoryBranch(
	ctx context.Context,
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceTrimHistoryBranchScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceTrimHistoryBranchScope, caller)
	resp, err := p.persistence.TrimHistoryBranch(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceTrimHistoryBranchScope, caller, err)
	}
	return resp, err
}

func (p *executionPersistenceClient) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetAllHistoryTreeBranchesScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetAllHistoryTreeBranchesScope, caller)
	response, err := p.persistence.GetAllHistoryTreeBranches(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetAllHistoryTreeBranchesScope, caller, err)
	}
	return response, err
}

// GetHistoryTree returns all branch information of a tree
func (p *executionPersistenceClient) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetHistoryTreeScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetHistoryTreeScope, caller)
	response, err := p.persistence.GetHistoryTree(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetHistoryTreeScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceEnqueueMessageScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceEnqueueMessageScope, caller)
	err := p.persistence.EnqueueMessage(ctx, blob)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceEnqueueMessageScope, caller, err)
	}

	return err
}

func (p *queuePersistenceClient) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*QueueMessage, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceReadQueueMessagesScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceReadQueueMessagesScope, caller)
	result, err := p.persistence.ReadMessages(ctx, lastMessageID, maxCount)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadQueueMessagesScope, caller, err)
	}

	return result, err
}

func (p *queuePersistenceClient) UpdateAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateAckLevelScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateAckLevelScope, caller)
	err := p.persistence.UpdateAckLevel(ctx, metadata)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateAckLevelScope, caller, err)
	}

	return err
}

func (p *queuePersistenceClient) GetAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetAckLevelScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetAckLevelScope, caller)
	result, err := p.persistence.GetAckLevels(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetAckLevelScope, caller, err)
	}

	return result, err
}

func (p *queuePersistenceClient) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteQueueMessagesScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteQueueMessagesScope, caller)
	err := p.persistence.DeleteMessagesBefore(ctx, messageID)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteQueueMessagesScope, caller, err)
	}

	return err
}

func (p *queuePersistenceClient) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceEnqueueMessageToDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceEnqueueMessageToDLQScope, caller)
	messageID, err := p.persistence.EnqueueMessageToDLQ(ctx, blob)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceEnqueueMessageToDLQScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceReadQueueMessagesFromDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceReadQueueMessagesFromDLQScope, caller)
	result, token, err := p.persistence.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadQueueMessagesFromDLQScope, caller, err)
	}

	return result, token, err
}

func (p *queuePersistenceClient) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteQueueMessageFromDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteQueueMessageFromDLQScope, caller)
	err := p.persistence.DeleteMessageFromDLQ(ctx, messageID)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteQueueMessageFromDLQScope, caller, err)
	}

	return err
}

func (p *queuePersistenceClient) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceRangeDeleteMessagesFromDLQScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceRangeDeleteMessagesFromDLQScope, caller)
	err := p.persistence.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeDeleteMessagesFromDLQScope, caller, err)
	}

	return err
}

func (p *queuePersistenceClient) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *InternalQueueMetadata,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpdateDLQAckLevelScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpdateDLQAckLevelScope, caller)
	err := p.persistence.UpdateDLQAckLevel(ctx, metadata)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateDLQAckLevelScope, caller, err)
	}

	return err
}

func (p *queuePersistenceClient) GetDLQAckLevels(
	ctx context.Context,
) (*InternalQueueMetadata, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetDLQAckLevelScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetDLQAckLevelScope, caller)
	result, err := p.persistence.GetDLQAckLevels(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetDLQAckLevelScope, caller, err)
	}

	return result, err
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
	caller := headers.GetCallerInfo(ctx).CallerName
	// This is a wrapper of GetClusterMetadata API, use the same scope here
	p.recordRequest(metrics.PersistenceListClusterMetadataScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceListClusterMetadataScope, caller)
	result, err := p.persistence.ListClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClusterMetadataScope, caller, err)
	}

	return result, err
}

func (p *clusterMetadataPersistenceClient) GetCurrentClusterMetadata(
	ctx context.Context,
) (*GetClusterMetadataResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	// This is a wrapper of GetClusterMetadata API, use the same scope here
	p.recordRequest(metrics.PersistenceGetClusterMetadataScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetClusterMetadataScope, caller)
	result, err := p.persistence.GetCurrentClusterMetadata(ctx)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetClusterMetadataScope, caller, err)
	}

	return result, err
}

func (p *clusterMetadataPersistenceClient) GetClusterMetadata(
	ctx context.Context,
	request *GetClusterMetadataRequest,
) (*GetClusterMetadataResponse, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetClusterMetadataScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetClusterMetadataScope, caller)
	result, err := p.persistence.GetClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetClusterMetadataScope, caller, err)
	}

	return result, err
}

func (p *clusterMetadataPersistenceClient) SaveClusterMetadata(
	ctx context.Context,
	request *SaveClusterMetadataRequest,
) (bool, error) {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceSaveClusterMetadataScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceSaveClusterMetadataScope, caller)
	applied, err := p.persistence.SaveClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceSaveClusterMetadataScope, caller, err)
	}

	return applied, err
}

func (p *clusterMetadataPersistenceClient) DeleteClusterMetadata(
	ctx context.Context,
	request *DeleteClusterMetadataRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceDeleteClusterMetadataScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceDeleteClusterMetadataScope, caller)
	err := p.persistence.DeleteClusterMetadata(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteClusterMetadataScope, caller, err)
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
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceGetClusterMembersScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceGetClusterMembersScope, caller)
	res, err := p.persistence.GetClusterMembers(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetClusterMembersScope, caller, err)
	}

	return res, err
}

func (p *clusterMetadataPersistenceClient) UpsertClusterMembership(
	ctx context.Context,
	request *UpsertClusterMembershipRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistenceUpsertClusterMembershipScope, caller)

	sw := p.startLatencyTimer(metrics.PersistenceUpsertClusterMembershipScope, caller)
	err := p.persistence.UpsertClusterMembership(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpsertClusterMembershipScope, caller, err)
	}

	return err
}

func (p *clusterMetadataPersistenceClient) PruneClusterMembership(
	ctx context.Context,
	request *PruneClusterMembershipRequest,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName
	p.recordRequest(metrics.PersistencePruneClusterMembershipScope, caller)

	sw := p.startLatencyTimer(metrics.PersistencePruneClusterMembershipScope, caller)
	err := p.persistence.PruneClusterMembership(ctx, request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistencePruneClusterMembershipScope, caller, err)
	}

	return err
}

func (p *metadataPersistenceClient) InitializeSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
) error {
	caller := headers.GetCallerInfo(ctx).CallerName

	p.recordRequest(metrics.PersistenceInitializeSystemNamespaceScope, caller)
	sw := p.startLatencyTimer(metrics.PersistenceInitializeSystemNamespaceScope, caller)
	err := p.persistence.InitializeSystemNamespaces(ctx, currentClusterName)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceInitializeSystemNamespaceScope, caller, err)
	}

	return err
}

func (p *metricEmitter) recordRequest(scope int, caller string) {
	p.metricClient.Scope(scope, metrics.NamespaceTag(caller)).IncCounter(metrics.PersistenceRequests)
}

func (p *metricEmitter) startLatencyTimer(scope int, caller string) metrics.Stopwatch {
	return p.metricClient.Scope(scope, metrics.NamespaceTag(caller)).StartTimer(metrics.PersistenceLatency)
}

func (p *metricEmitter) updateErrorMetric(scope int, caller string, err error) {
	metricScope := p.metricClient.Scope(scope, metrics.NamespaceTag(caller))
	metricScope.Tagged(metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.PersistenceErrorWithType)

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
		metricScope.Tagged(metrics.ResourceExhaustedCauseTag(err.Cause)).IncCounter(metrics.PersistenceErrResourceExhaustedCounter)
	default:
		p.logger.Error("Operation failed with internal error.", tag.Error(err), tag.MetricScope(scope))
		metricScope.IncCounter(metrics.PersistenceFailures)
	}
}
