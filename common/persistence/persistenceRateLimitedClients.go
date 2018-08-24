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
	"github.com/uber/cadence/common"
)

var (
	// ErrPersistenceLimitExceeded is the error indicating QPS limit reached.
	ErrPersistenceLimitExceeded = &workflow.ServiceBusyError{Message: "Persistence Max QPS Reached."}
)

type (
	shardRateLimitedPersistenceClient struct {
		rateLimiter common.TokenBucket
		persistence ShardManager
		logger      bark.Logger
	}

	workflowExecutionRateLimitedPersistenceClient struct {
		rateLimiter common.TokenBucket
		persistence ExecutionManager
		logger      bark.Logger
	}

	taskRateLimitedPersistenceClient struct {
		rateLimiter common.TokenBucket
		persistence TaskManager
		logger      bark.Logger
	}

	historyRateLimitedPersistenceClient struct {
		rateLimiter common.TokenBucket
		persistence HistoryManager
		logger      bark.Logger
	}

	metadataRateLimitedPersistenceClient struct {
		rateLimiter common.TokenBucket
		persistence MetadataManager
		logger      bark.Logger
	}

	visibilityRateLimitedPersistenceClient struct {
		rateLimiter common.TokenBucket
		persistence VisibilityManager
		logger      bark.Logger
	}
)

var _ ShardManager = (*shardRateLimitedPersistenceClient)(nil)
var _ ExecutionManager = (*workflowExecutionRateLimitedPersistenceClient)(nil)
var _ TaskManager = (*taskRateLimitedPersistenceClient)(nil)
var _ HistoryManager = (*historyRateLimitedPersistenceClient)(nil)
var _ MetadataManager = (*metadataRateLimitedPersistenceClient)(nil)
var _ VisibilityManager = (*visibilityRateLimitedPersistenceClient)(nil)

// NewShardPersistenceRateLimitedClient creates a client to manage shards
func NewShardPersistenceRateLimitedClient(persistence ShardManager, rateLimiter common.TokenBucket, logger bark.Logger) ShardManager {
	return &shardRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewWorkflowExecutionPersistenceRateLimitedClient creates a client to manage executions
func NewWorkflowExecutionPersistenceRateLimitedClient(persistence ExecutionManager, rateLimiter common.TokenBucket, logger bark.Logger) ExecutionManager {
	return &workflowExecutionRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewTaskPersistenceRateLimitedClient creates a client to manage tasks
func NewTaskPersistenceRateLimitedClient(persistence TaskManager, rateLimiter common.TokenBucket, logger bark.Logger) TaskManager {
	return &taskRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewHistoryPersistenceRateLimitedClient creates a HistoryManager client to manage workflow execution history
func NewHistoryPersistenceRateLimitedClient(persistence HistoryManager, rateLimiter common.TokenBucket, logger bark.Logger) HistoryManager {
	return &historyRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewMetadataPersistenceRateLimitedClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceRateLimitedClient(persistence MetadataManager, rateLimiter common.TokenBucket, logger bark.Logger) MetadataManager {
	return &metadataRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewVisibilityPersistenceRateLimitedClient creates a client to manage visibility
func NewVisibilityPersistenceRateLimitedClient(persistence VisibilityManager, rateLimiter common.TokenBucket, logger bark.Logger) VisibilityManager {
	return &visibilityRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

func (p *shardRateLimitedPersistenceClient) CreateShard(request *CreateShardRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CreateShard(request)
	return err
}

func (p *shardRateLimitedPersistenceClient) GetShard(request *GetShardRequest) (*GetShardResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetShard(request)
	return response, err
}

func (p *shardRateLimitedPersistenceClient) UpdateShard(request *UpdateShardRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateShard(request)
	return err
}

func (p *shardRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *workflowExecutionRateLimitedPersistenceClient) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateWorkflowExecution(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetWorkflowExecution(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateWorkflowExecution(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) ResetMutableState(request *ResetMutableStateRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.ResetMutableState(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteWorkflowExecution(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetCurrentExecution(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetTransferTasks(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetReplicationTasks(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTransferTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RangeCompleteTransferTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteReplicationTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	resonse, err := p.persistence.GetTimerIndexTasks(request)
	return resonse, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTimerTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RangeCompleteTimerTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskRateLimitedPersistenceClient) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateTasks(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetTasks(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) CompleteTask(request *CompleteTaskRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTask(request)
	return err
}

func (p *taskRateLimitedPersistenceClient) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.LeaseTaskList(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.UpdateTaskList(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *historyRateLimitedPersistenceClient) AppendHistoryEvents(request *AppendHistoryEventsRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.AppendHistoryEvents(request)
	return err
}

func (p *historyRateLimitedPersistenceClient) GetWorkflowExecutionHistory(request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetWorkflowExecutionHistory(request)
	return response, err
}

func (p *historyRateLimitedPersistenceClient) DeleteWorkflowExecutionHistory(request *DeleteWorkflowExecutionHistoryRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteWorkflowExecutionHistory(request)
	return err
}

func (p *historyRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataRateLimitedPersistenceClient) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateDomain(request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetDomain(request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) UpdateDomain(request *UpdateDomainRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateDomain(request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) DeleteDomain(request *DeleteDomainRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteDomain(request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteDomainByName(request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) ListDomains(request *ListDomainsRequest) (*ListDomainsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListDomains(request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetMetadata() (*GetMetadataResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetMetadata()
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *visibilityRateLimitedPersistenceClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RecordWorkflowExecutionStarted(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RecordWorkflowExecutionClosed(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutions(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutions(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	if ok, _ := p.rateLimiter.TryConsume(1); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetClosedWorkflowExecution(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}
