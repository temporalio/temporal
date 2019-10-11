// Copyright (c) 2017 Uber Technologies, Inc.
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
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
)

type (
	shardPersistenceClient struct {
		metricClient metrics.Client
		persistence  ShardManager
		logger       log.Logger
	}

	workflowExecutionPersistenceClient struct {
		metricClient metrics.Client
		persistence  ExecutionManager
		logger       log.Logger
	}

	taskPersistenceClient struct {
		metricClient metrics.Client
		persistence  TaskManager
		logger       log.Logger
	}

	historyV2PersistenceClient struct {
		metricClient metrics.Client
		persistence  HistoryV2Manager
		logger       log.Logger
	}

	metadataPersistenceClient struct {
		metricClient metrics.Client
		persistence  MetadataManager
		logger       log.Logger
	}

	visibilityPersistenceClient struct {
		metricClient metrics.Client
		persistence  VisibilityManager
		logger       log.Logger
	}

	queuePersistenceClient struct {
		metricClient metrics.Client
		persistence  Queue
		logger       log.Logger
	}
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*workflowExecutionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)
var _ HistoryV2Manager = (*historyV2PersistenceClient)(nil)
var _ MetadataManager = (*metadataPersistenceClient)(nil)
var _ VisibilityManager = (*visibilityPersistenceClient)(nil)
var _ Queue = (*queuePersistenceClient)(nil)

// NewShardPersistenceMetricsClient creates a client to manage shards
func NewShardPersistenceMetricsClient(persistence ShardManager, metricClient metrics.Client, logger log.Logger) ShardManager {
	return &shardPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewWorkflowExecutionPersistenceMetricsClient creates a client to manage executions
func NewWorkflowExecutionPersistenceMetricsClient(persistence ExecutionManager, metricClient metrics.Client, logger log.Logger) ExecutionManager {
	return &workflowExecutionPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewTaskPersistenceMetricsClient creates a client to manage tasks
func NewTaskPersistenceMetricsClient(persistence TaskManager, metricClient metrics.Client, logger log.Logger) TaskManager {
	return &taskPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewHistoryV2PersistenceMetricsClient creates a HistoryV2Manager client to manage workflow execution history
func NewHistoryV2PersistenceMetricsClient(persistence HistoryV2Manager, metricClient metrics.Client, logger log.Logger) HistoryV2Manager {
	return &historyV2PersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewMetadataPersistenceMetricsClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceMetricsClient(persistence MetadataManager, metricClient metrics.Client, logger log.Logger) MetadataManager {
	return &metadataPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewVisibilityPersistenceMetricsClient creates a client to manage visibility
func NewVisibilityPersistenceMetricsClient(persistence VisibilityManager, metricClient metrics.Client, logger log.Logger) VisibilityManager {
	return &visibilityPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewQueuePersistenceMetricsClient creates a client to manage queue
func NewQueuePersistenceMetricsClient(persistence Queue, metricClient metrics.Client, logger log.Logger) Queue {
	return &queuePersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

func (p *shardPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardPersistenceClient) CreateShard(request *CreateShardRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCreateShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateShardScope, metrics.PersistenceLatency)
	err := p.persistence.CreateShard(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateShardScope, err)
	}

	return err
}

func (p *shardPersistenceClient) GetShard(
	request *GetShardRequest) (*GetShardResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetShardScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetShard(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetShardScope, err)
	}

	return response, err
}

func (p *shardPersistenceClient) UpdateShard(request *UpdateShardRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateShardScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateShard(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateShardScope, err)
	}

	return err
}

func (p *shardPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *ShardAlreadyExistError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrShardExistsCounter)
	case *ShardOwnershipLostError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrShardOwnershipLostCounter)
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.", tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *shardPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *workflowExecutionPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *workflowExecutionPersistenceClient) GetShardID() int {
	return p.persistence.GetShardID()
}

func (p *workflowExecutionPersistenceClient) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceCreateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) (*UpdateWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.PersistenceLatency)
	resp, err := p.persistence.UpdateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateWorkflowExecutionScope, err)
	}

	return resp, err
}

func (p *workflowExecutionPersistenceClient) ConflictResolveWorkflowExecution(request *ConflictResolveWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceConflictResolveWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceConflictResolveWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.ConflictResolveWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceConflictResolveWorkflowExecutionScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) ResetWorkflowExecution(request *ResetWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceResetWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceResetWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.ResetWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceResetWorkflowExecutionScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteWorkflowExecutionScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteCurrentWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteCurrentWorkflowExecutionScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetCurrentExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetCurrentExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetCurrentExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetCurrentExecutionScope, err)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetTransferTasksScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetTransferTasksScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetTransferTasks(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTransferTasksScope, err)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetReplicationTasksScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetReplicationTasksScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetReplicationTasks(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetReplicationTasksScope, err)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCompleteTransferTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteTransferTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTransferTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTransferTaskScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceRangeCompleteTransferTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRangeCompleteTransferTaskScope, metrics.PersistenceLatency)
	err := p.persistence.RangeCompleteTransferTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeCompleteTransferTaskScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCompleteReplicationTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteReplicationTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteReplicationTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteReplicationTaskScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetTimerIndexTasksScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetTimerIndexTasksScope, metrics.PersistenceLatency)
	resonse, err := p.persistence.GetTimerIndexTasks(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTimerIndexTasksScope, err)
	}

	return resonse, err
}

func (p *workflowExecutionPersistenceClient) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCompleteTimerTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteTimerTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTimerTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTimerTaskScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceRangeCompleteTimerTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRangeCompleteTimerTaskScope, metrics.PersistenceLatency)
	err := p.persistence.RangeCompleteTimerTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeCompleteTimerTaskScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteTask(request *DeleteTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteTaskScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteTaskScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRangeCompleteTimerTaskScope, err)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *WorkflowExecutionAlreadyStartedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrExecutionAlreadyStartedCounter)
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *ShardOwnershipLostError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrShardOwnershipLostCounter)
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *CurrentWorkflowConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrCurrentWorkflowConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.",
			tag.Error(err), tag.MetricScope(scope), tag.ShardID(p.GetShardID()))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *workflowExecutionPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskPersistenceClient) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceCreateTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateTaskScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateTasks(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateTaskScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetTasksScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetTasksScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetTasks(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetTasksScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(request *CompleteTaskRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCompleteTaskScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTask(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTaskScope, err)
	}

	return err
}

func (p *taskPersistenceClient) CompleteTasksLessThan(request *CompleteTasksLessThanRequest) (int, error) {
	p.metricClient.IncCounter(metrics.PersistenceCompleteTasksLessThanScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteTasksLessThanScope, metrics.PersistenceLatency)
	result, err := p.persistence.CompleteTasksLessThan(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteTasksLessThanScope, err)
	}
	return result, err
}

func (p *taskPersistenceClient) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceLeaseTaskListScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceLeaseTaskListScope, metrics.PersistenceLatency)
	response, err := p.persistence.LeaseTaskList(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceLeaseTaskListScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) ListTaskList(request *ListTaskListRequest) (*ListTaskListResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListTaskListScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceListTaskListScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListTaskList(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListTaskListScope, err)
	}
	return response, err
}

func (p *taskPersistenceClient) DeleteTaskList(request *DeleteTaskListRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteTaskListScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteTaskListScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteTaskList(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteTaskListScope, err)
	}
	return err
}

func (p *taskPersistenceClient) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceUpdateTaskListScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateTaskListScope, metrics.PersistenceLatency)
	response, err := p.persistence.UpdateTaskList(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateTaskListScope, err)
	}

	return response, err
}

func (p *taskPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.",
			tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *taskPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataPersistenceClient) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceCreateDomainScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateDomainScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateDomain(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCreateDomainScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetDomainScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetDomainScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetDomain(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetDomainScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) UpdateDomain(request *UpdateDomainRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateDomainScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateDomainScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateDomain(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateDomainScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) DeleteDomain(request *DeleteDomainRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteDomainScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteDomainScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteDomain(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteDomainScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteDomainByNameScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteDomainByNameScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteDomainByName(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteDomainByNameScope, err)
	}

	return err
}

func (p *metadataPersistenceClient) ListDomains(request *ListDomainsRequest) (*ListDomainsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListDomainScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListDomainScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListDomains(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListDomainScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) GetMetadata() (*GetMetadataResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetMetadataScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetMetadataScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetMetadata()
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetMetadataScope, err)
	}

	return response, err
}

func (p *metadataPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *workflow.DomainAlreadyExistsError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrDomainAlreadyExistsCounter)
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *workflow.BadRequestError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBadRequestCounter)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.",
			tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *visibilityPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *visibilityPersistenceClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceLatency)
	err := p.persistence.RecordWorkflowExecutionStarted(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRecordWorkflowExecutionStartedScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceLatency)
	err := p.persistence.RecordWorkflowExecutionClosed(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRecordWorkflowExecutionClosedScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceUpsertWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpsertWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.UpsertWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpsertWorkflowExecutionScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListOpenWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListOpenWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListOpenWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListOpenWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsByTypeScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsByTypeScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetClosedWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetClosedWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetClosedWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetClosedWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceScanWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceScanWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ScanWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceScanWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceCountWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCountWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.CountWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCountWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.",
			tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *visibilityPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *historyV2PersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *historyV2PersistenceClient) Close() {
	p.persistence.Close()
}

// AppendHistoryNodes add(or override) a node to a history branch
func (p *historyV2PersistenceClient) AppendHistoryNodes(request *AppendHistoryNodesRequest) (*AppendHistoryNodesResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceAppendHistoryNodesScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceAppendHistoryNodesScope, metrics.PersistenceLatency)
	resp, err := p.persistence.AppendHistoryNodes(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendHistoryNodesScope, err)
	}
	return resp, err
}

// ReadHistoryBranch returns history node data for a branch
func (p *historyV2PersistenceClient) ReadHistoryBranch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadHistoryBranch(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, err)
	}
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
func (p *historyV2PersistenceClient) ReadHistoryBranchByBatch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchByBatchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadHistoryBranchByBatch(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, err)
	}
	return response, err
}

// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
func (p *historyV2PersistenceClient) ReadRawHistoryBranch(request *ReadHistoryBranchRequest) (*ReadRawHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceReadHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ReadRawHistoryBranch(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceReadHistoryBranchScope, err)
	}
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *historyV2PersistenceClient) ForkHistoryBranch(request *ForkHistoryBranchRequest) (*ForkHistoryBranchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceForkHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceForkHistoryBranchScope, metrics.PersistenceLatency)
	response, err := p.persistence.ForkHistoryBranch(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceForkHistoryBranchScope, err)
	}
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *historyV2PersistenceClient) DeleteHistoryBranch(request *DeleteHistoryBranchRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteHistoryBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteHistoryBranchScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteHistoryBranch(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteHistoryBranchScope, err)
	}
	return err
}

// CompleteForkBranch complete forking process
func (p *historyV2PersistenceClient) CompleteForkBranch(request *CompleteForkBranchRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCompleteForkBranchScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceCompleteForkBranchScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteForkBranch(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCompleteForkBranchScope, err)
	}
	return err
}

func (p *historyV2PersistenceClient) GetAllHistoryTreeBranches(request *GetAllHistoryTreeBranchesRequest) (*GetAllHistoryTreeBranchesResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetAllHistoryTreeBranchesScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceGetAllHistoryTreeBranchesScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetAllHistoryTreeBranches(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetAllHistoryTreeBranchesScope, err)
	}
	return response, err
}

// GetHistoryTree returns all branch information of a tree
func (p *historyV2PersistenceClient) GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetHistoryTreeScope, metrics.PersistenceRequests)
	sw := p.metricClient.StartTimer(metrics.PersistenceGetHistoryTreeScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetHistoryTree(request)
	sw.Stop()
	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetHistoryTreeScope, err)
	}
	return response, err
}

func (p *historyV2PersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.",
			tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *queuePersistenceClient) EnqueueMessage(message []byte) error {
	p.metricClient.IncCounter(metrics.PersistenceEnqueueMessageScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceEnqueueMessageScope, metrics.PersistenceLatency)
	err := p.persistence.EnqueueMessage(message)
	sw.Stop()

	if err != nil {
		p.metricClient.IncCounter(metrics.PersistenceEnqueueMessageScope, metrics.PersistenceFailures)
	}

	return err
}

func (p *queuePersistenceClient) ReadMessages(lastMessageID int, maxCount int) ([]*QueueMessage, error) {
	p.metricClient.IncCounter(metrics.PersistenceReadQueueMessagesScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceReadQueueMessagesScope, metrics.PersistenceLatency)
	result, err := p.persistence.ReadMessages(lastMessageID, maxCount)
	sw.Stop()

	if err != nil {
		p.metricClient.IncCounter(metrics.PersistenceReadQueueMessagesScope, metrics.PersistenceFailures)
	}

	return result, err
}

func (p *queuePersistenceClient) UpdateAckLevel(messageID int, clusterName string) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateAckLevelScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateAckLevelScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateAckLevel(messageID, clusterName)
	sw.Stop()

	if err != nil {
		p.metricClient.IncCounter(metrics.PersistenceUpdateAckLevelScope, metrics.PersistenceFailures)
	}

	return err
}

func (p *queuePersistenceClient) GetAckLevels() (map[string]int, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetAckLevelScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetAckLevelScope, metrics.PersistenceLatency)
	result, err := p.persistence.GetAckLevels()
	sw.Stop()

	if err != nil {
		p.metricClient.IncCounter(metrics.PersistenceGetAckLevelScope, metrics.PersistenceFailures)
	}

	return result, err
}

func (p *queuePersistenceClient) DeleteMessagesBefore(messageID int) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteQueueMessagesScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteQueueMessagesScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteMessagesBefore(messageID)
	sw.Stop()

	if err != nil {
		p.metricClient.IncCounter(metrics.PersistenceDeleteQueueMessagesScope, metrics.PersistenceFailures)
	}

	return err
}

func (p *queuePersistenceClient) Close() {
	p.persistence.Close()
}
