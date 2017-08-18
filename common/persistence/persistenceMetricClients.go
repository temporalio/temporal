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
	"github.com/uber/cadence/common/metrics"
)

type (
	shardPersistenceClient struct {
		metricClient metrics.Client
		persistence  ShardManager
	}

	workflowExecutionPersistenceClient struct {
		metricClient metrics.Client
		persistence  ExecutionManager
	}

	taskPersistenceClient struct {
		metricClient metrics.Client
		persistence  TaskManager
	}

	historyPersistenceClient struct {
		metricClient metrics.Client
		persistence  HistoryManager
	}

	metadataPersistenceClient struct {
		metricClient metrics.Client
		persistence  MetadataManager
	}
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*workflowExecutionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)
var _ HistoryManager = (*historyPersistenceClient)(nil)
var _ MetadataManager = (*metadataPersistenceClient)(nil)

// NewShardPersistenceClient creates a client to manage shards
func NewShardPersistenceClient(persistence ShardManager, metricClient metrics.Client) ShardManager {
	return &shardPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
	}
}

// NewWorkflowExecutionPersistenceClient creates a client to manage executions
func NewWorkflowExecutionPersistenceClient(persistence ExecutionManager, metricClient metrics.Client) ExecutionManager {
	return &workflowExecutionPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
	}
}

// NewTaskPersistenceClient creates a client to manage tasks
func NewTaskPersistenceClient(persistence TaskManager, metricClient metrics.Client) TaskManager {
	return &taskPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
	}
}

// NewHistoryPersistenceClient creates a HistoryManager client to manage workflow execution history
func NewHistoryPersistenceClient(persistence HistoryManager, metricClient metrics.Client) HistoryManager {
	return &historyPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
	}
}

// NewMetadataPersistenceClient creates a HistoryManager client to manage workflow execution history
func NewMetadataPersistenceClient(persistence MetadataManager, metricClient metrics.Client) MetadataManager {
	return &metadataPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
	}
}

func (p *shardPersistenceClient) CreateShard(request *CreateShardRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceCreateShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCreateShardScope, metrics.PersistenceLatency)
	err := p.persistence.CreateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ShardAlreadyExistError); ok {
			p.metricClient.IncCounter(metrics.PersistenceCreateShardScope, metrics.PersistenceErrShardExistsCounter)
		} else {
			p.metricClient.IncCounter(metrics.PersistenceCreateShardScope, metrics.PersistenceFailures)
		}
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
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			p.metricClient.IncCounter(metrics.PersistenceGetShardScope, metrics.CadenceErrEntityNotExistsCounter)
		default:
			p.metricClient.IncCounter(metrics.PersistenceGetShardScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}

func (p *shardPersistenceClient) UpdateShard(request *UpdateShardRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateShardScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateShardScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ShardOwnershipLostError); ok {
			p.metricClient.IncCounter(metrics.PersistenceUpdateShardScope, metrics.PersistenceErrShardOwnershipLostCounter)
		} else {
			p.metricClient.IncCounter(metrics.PersistenceUpdateShardScope, metrics.PersistenceFailures)
		}
	}

	return err
}

func (p *shardPersistenceClient) Close() {
	p.persistence.Close()
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

func (p *workflowExecutionPersistenceClient) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpdateWorkflowExecutionScope, err)
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

func (p *workflowExecutionPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *workflow.WorkflowExecutionAlreadyStartedError:
		p.metricClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
	case *ShardOwnershipLostError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrShardOwnershipLostCounter)
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *workflowExecutionPersistenceClient) Close() {
	p.persistence.Close()
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
	default:
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *taskPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *historyPersistenceClient) AppendHistoryEvents(request *AppendHistoryEventsRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceAppendHistoryEventsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceAppendHistoryEventsScope, metrics.PersistenceLatency)
	err := p.persistence.AppendHistoryEvents(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendHistoryEventsScope, err)
	}

	return err
}

func (p *historyPersistenceClient) GetWorkflowExecutionHistory(
	request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetWorkflowExecutionHistoryScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetWorkflowExecutionHistoryScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetWorkflowExecutionHistory(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetWorkflowExecutionHistoryScope, err)
	}

	return response, err
}

func (p *historyPersistenceClient) DeleteWorkflowExecutionHistory(
	request *DeleteWorkflowExecutionHistoryRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceDeleteWorkflowExecutionHistoryScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceDeleteWorkflowExecutionHistoryScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteWorkflowExecutionHistory(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceDeleteWorkflowExecutionHistoryScope, err)
	}

	return err
}

func (p *historyPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *historyPersistenceClient) Close() {
	p.persistence.Close()
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

func (p *metadataPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *workflow.DomainAlreadyExistsError:
		p.metricClient.IncCounter(scope, metrics.CadenceErrDomainAlreadyExistsCounter)
	case *workflow.EntityNotExistsError:
		p.metricClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *workflow.BadRequestError:
		p.metricClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	default:
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}
