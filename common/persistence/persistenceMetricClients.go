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
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
)

type (
	shardPersistenceClient struct {
		metricClient metrics.Client
		persistence  ShardManager
		logger       bark.Logger
	}

	workflowExecutionPersistenceClient struct {
		metricClient metrics.Client
		persistence  ExecutionManager
		logger       bark.Logger
	}

	taskPersistenceClient struct {
		metricClient metrics.Client
		persistence  TaskManager
		logger       bark.Logger
	}

	historyPersistenceClient struct {
		metricClient metrics.Client
		persistence  HistoryManager
		logger       bark.Logger
	}

	historyV2PersistenceClient struct {
		metricClient metrics.Client
		persistence  HistoryV2Manager
		logger       bark.Logger
	}

	metadataPersistenceClient struct {
		metricClient metrics.Client
		persistence  MetadataManager
		logger       bark.Logger
	}

	visibilityPersistenceClient struct {
		metricClient metrics.Client
		persistence  VisibilityManager
		logger       bark.Logger
	}
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*workflowExecutionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)
var _ HistoryManager = (*historyPersistenceClient)(nil)
var _ HistoryV2Manager = (*historyV2PersistenceClient)(nil)
var _ MetadataManager = (*metadataPersistenceClient)(nil)
var _ VisibilityManager = (*visibilityPersistenceClient)(nil)

// NewShardPersistenceMetricsClient creates a client to manage shards
func NewShardPersistenceMetricsClient(persistence ShardManager, metricClient metrics.Client, logger bark.Logger) ShardManager {
	return &shardPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewWorkflowExecutionPersistenceMetricsClient creates a client to manage executions
func NewWorkflowExecutionPersistenceMetricsClient(persistence ExecutionManager, metricClient metrics.Client, logger bark.Logger) ExecutionManager {
	return &workflowExecutionPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewTaskPersistenceMetricsClient creates a client to manage tasks
func NewTaskPersistenceMetricsClient(persistence TaskManager, metricClient metrics.Client, logger bark.Logger) TaskManager {
	return &taskPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewHistoryPersistenceMetricsClient creates a HistoryManager client to manage workflow execution history
func NewHistoryPersistenceMetricsClient(persistence HistoryManager, metricClient metrics.Client, logger bark.Logger) HistoryManager {
	return &historyPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewHistoryV2PersistenceMetricsClient creates a HistoryManager client to manage workflow execution history
func NewHistoryV2PersistenceMetricsClient(persistence HistoryV2Manager, metricClient metrics.Client, logger bark.Logger) HistoryV2Manager {
	return &historyV2PersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewMetadataPersistenceMetricsClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceMetricsClient(persistence MetadataManager, metricClient metrics.Client, logger bark.Logger) MetadataManager {
	return &metadataPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

// NewVisibilityPersistenceMetricsClient creates a client to manage visibility
func NewVisibilityPersistenceMetricsClient(persistence VisibilityManager, metricClient metrics.Client, logger bark.Logger) VisibilityManager {
	return &visibilityPersistenceClient{
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
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
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

func (p *workflowExecutionPersistenceClient) ResetMutableState(request *ResetMutableStateRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceResetMutableStateScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceResetMutableStateScope, metrics.PersistenceLatency)
	err := p.persistence.ResetMutableState(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceResetMutableStateScope, err)
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
		p.logger.WithFields(bark.Fields{
			logging.TagScope:          scope,
			logging.TagHistoryShardID: p.GetShardID(),
			logging.TagErr:            err,
		}).Error("Operation failed with internal error.")
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
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *taskPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *historyPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *historyPersistenceClient) AppendHistoryEvents(request *AppendHistoryEventsRequest) (*AppendHistoryEventsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceAppendHistoryEventsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceAppendHistoryEventsScope, metrics.PersistenceLatency)
	resp, err := p.persistence.AppendHistoryEvents(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceAppendHistoryEventsScope, err)
	}

	return resp, err
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

func (p *historyPersistenceClient) GetWorkflowExecutionHistoryByBatch(
	request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryByBatchResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetWorkflowExecutionHistoryScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetWorkflowExecutionHistoryScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetWorkflowExecutionHistoryByBatch(request)
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
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *historyPersistenceClient) Close() {
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
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
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
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
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
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}
