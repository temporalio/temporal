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
	"github.com/uber/cadence/common/quotas"
)

var (
	// ErrPersistenceLimitExceeded is the error indicating QPS limit reached.
	ErrPersistenceLimitExceeded = &workflow.ServiceBusyError{Message: "Persistence Max QPS Reached."}
	// ErrPersistenceLimitExceededForList is the error indicating QPS limit reached for list visibility.
	ErrPersistenceLimitExceededForList = &workflow.ServiceBusyError{Message: "Persistence Max QPS Reached for List Operations."}
)

type (
	shardRateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence ShardManager
		logger      log.Logger
	}

	workflowExecutionRateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence ExecutionManager
		logger      log.Logger
	}

	taskRateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence TaskManager
		logger      log.Logger
	}

	historyV2RateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence HistoryV2Manager
		logger      log.Logger
	}

	metadataRateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence MetadataManager
		logger      log.Logger
	}

	visibilityRateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence VisibilityManager
		logger      log.Logger
	}

	queueRateLimitedPersistenceClient struct {
		rateLimiter quotas.Limiter
		persistence Queue
		logger      log.Logger
	}
)

var _ ShardManager = (*shardRateLimitedPersistenceClient)(nil)
var _ ExecutionManager = (*workflowExecutionRateLimitedPersistenceClient)(nil)
var _ TaskManager = (*taskRateLimitedPersistenceClient)(nil)
var _ HistoryV2Manager = (*historyV2RateLimitedPersistenceClient)(nil)
var _ MetadataManager = (*metadataRateLimitedPersistenceClient)(nil)
var _ VisibilityManager = (*visibilityRateLimitedPersistenceClient)(nil)
var _ Queue = (*queueRateLimitedPersistenceClient)(nil)

// NewShardPersistenceRateLimitedClient creates a client to manage shards
func NewShardPersistenceRateLimitedClient(persistence ShardManager, rateLimiter quotas.Limiter, logger log.Logger) ShardManager {
	return &shardRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewWorkflowExecutionPersistenceRateLimitedClient creates a client to manage executions
func NewWorkflowExecutionPersistenceRateLimitedClient(persistence ExecutionManager, rateLimiter quotas.Limiter, logger log.Logger) ExecutionManager {
	return &workflowExecutionRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewTaskPersistenceRateLimitedClient creates a client to manage tasks
func NewTaskPersistenceRateLimitedClient(persistence TaskManager, rateLimiter quotas.Limiter, logger log.Logger) TaskManager {
	return &taskRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewHistoryV2PersistenceRateLimitedClient creates a HistoryV2Manager client to manage workflow execution history
func NewHistoryV2PersistenceRateLimitedClient(persistence HistoryV2Manager, rateLimiter quotas.Limiter, logger log.Logger) HistoryV2Manager {
	return &historyV2RateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewMetadataPersistenceRateLimitedClient creates a MetadataManager client to manage metadata
func NewMetadataPersistenceRateLimitedClient(persistence MetadataManager, rateLimiter quotas.Limiter, logger log.Logger) MetadataManager {
	return &metadataRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewVisibilityPersistenceRateLimitedClient creates a client to manage visibility
func NewVisibilityPersistenceRateLimitedClient(persistence VisibilityManager, rateLimiter quotas.Limiter, logger log.Logger) VisibilityManager {
	return &visibilityRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

// NewQueuePersistenceRateLimitedClient creates a client to manage queue
func NewQueuePersistenceRateLimitedClient(persistence Queue, rateLimiter quotas.Limiter, logger log.Logger) Queue {
	return &queueRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

func (p *shardRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *shardRateLimitedPersistenceClient) CreateShard(request *CreateShardRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CreateShard(request)
	return err
}

func (p *shardRateLimitedPersistenceClient) GetShard(request *GetShardRequest) (*GetShardResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetShard(request)
	return response, err
}

func (p *shardRateLimitedPersistenceClient) UpdateShard(request *UpdateShardRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateShard(request)
	return err
}

func (p *shardRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetShardID() int {
	return p.persistence.GetShardID()
}

func (p *workflowExecutionRateLimitedPersistenceClient) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateWorkflowExecution(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetWorkflowExecution(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) (*UpdateWorkflowExecutionResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	resp, err := p.persistence.UpdateWorkflowExecution(request)
	return resp, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) ConflictResolveWorkflowExecution(request *ConflictResolveWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.ConflictResolveWorkflowExecution(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) ResetWorkflowExecution(request *ResetWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.ResetWorkflowExecution(request)
	return err
}

// CompleteForkBranch complete forking process
func (p *historyV2RateLimitedPersistenceClient) CompleteForkBranch(request *CompleteForkBranchRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}
	err := p.persistence.CompleteForkBranch(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteWorkflowExecution(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteCurrentWorkflowExecution(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetCurrentExecution(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetTransferTasks(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetReplicationTasks(request)
	return response, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTransferTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RangeCompleteTransferTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteReplicationTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	resonse, err := p.persistence.GetTimerIndexTasks(request)
	return resonse, err
}

func (p *workflowExecutionRateLimitedPersistenceClient) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTimerTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RangeCompleteTimerTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) DeleteTask(request *DeleteTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteTask(request)
	return err
}

func (p *workflowExecutionRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *taskRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *taskRateLimitedPersistenceClient) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateTasks(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetTasks(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) CompleteTask(request *CompleteTaskRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.CompleteTask(request)
	return err
}

func (p *taskRateLimitedPersistenceClient) CompleteTasksLessThan(request *CompleteTasksLessThanRequest) (int, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return 0, ErrPersistenceLimitExceeded
	}
	return p.persistence.CompleteTasksLessThan(request)
}

func (p *taskRateLimitedPersistenceClient) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.LeaseTaskList(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.UpdateTaskList(request)
	return response, err
}

func (p *taskRateLimitedPersistenceClient) ListTaskList(request *ListTaskListRequest) (*ListTaskListResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.ListTaskList(request)
}

func (p *taskRateLimitedPersistenceClient) DeleteTaskList(request *DeleteTaskListRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}
	return p.persistence.DeleteTaskList(request)
}

func (p *taskRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *metadataRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *metadataRateLimitedPersistenceClient) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.CreateDomain(request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetDomain(request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) UpdateDomain(request *UpdateDomainRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpdateDomain(request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) DeleteDomain(request *DeleteDomainRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteDomain(request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.DeleteDomainByName(request)
	return err
}

func (p *metadataRateLimitedPersistenceClient) ListDomains(request *ListDomainsRequest) (*ListDomainsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListDomains(request)
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) GetMetadata() (*GetMetadataResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetMetadata()
	return response, err
}

func (p *metadataRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *visibilityRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *visibilityRateLimitedPersistenceClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RecordWorkflowExecutionStarted(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.RecordWorkflowExecutionClosed(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpsertWorkflowExecution(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutions(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutions(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.GetClosedWorkflowExecution(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}
	return p.persistence.DeleteWorkflowExecution(request)
}

func (p *visibilityRateLimitedPersistenceClient) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.ListWorkflowExecutions(request)
}

func (p *visibilityRateLimitedPersistenceClient) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.ScanWorkflowExecutions(request)
}

func (p *visibilityRateLimitedPersistenceClient) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.CountWorkflowExecutions(request)
}

func (p *visibilityRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

func (p *historyV2RateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *historyV2RateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}

// AppendHistoryNodes add(or override) a node to a history branch
func (p *historyV2RateLimitedPersistenceClient) AppendHistoryNodes(request *AppendHistoryNodesRequest) (*AppendHistoryNodesResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	return p.persistence.AppendHistoryNodes(request)
}

// ReadHistoryBranch returns history node data for a branch
func (p *historyV2RateLimitedPersistenceClient) ReadHistoryBranch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadHistoryBranch(request)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *historyV2RateLimitedPersistenceClient) ReadHistoryBranchByBatch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchByBatchResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadHistoryBranchByBatch(request)
	return response, err
}

// ReadHistoryBranchByBatch returns history node data for a branch
func (p *historyV2RateLimitedPersistenceClient) ReadRawHistoryBranch(request *ReadHistoryBranchRequest) (*ReadRawHistoryBranchResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ReadRawHistoryBranch(request)
	return response, err
}

// ForkHistoryBranch forks a new branch from a old branch
func (p *historyV2RateLimitedPersistenceClient) ForkHistoryBranch(request *ForkHistoryBranchRequest) (*ForkHistoryBranchResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.ForkHistoryBranch(request)
	return response, err
}

// DeleteHistoryBranch removes a branch
func (p *historyV2RateLimitedPersistenceClient) DeleteHistoryBranch(request *DeleteHistoryBranchRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}
	err := p.persistence.DeleteHistoryBranch(request)
	return err
}

// GetHistoryTree returns all branch information of a tree
func (p *historyV2RateLimitedPersistenceClient) GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.GetHistoryTree(request)
	return response, err
}

func (p *historyV2RateLimitedPersistenceClient) GetAllHistoryTreeBranches(request *GetAllHistoryTreeBranchesRequest) (*GetAllHistoryTreeBranchesResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}
	response, err := p.persistence.GetAllHistoryTreeBranches(request)
	return response, err
}

func (p *queueRateLimitedPersistenceClient) EnqueueMessage(message []byte) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.EnqueueMessage(message)
}

func (p *queueRateLimitedPersistenceClient) ReadMessages(lastMessageID int, maxCount int) ([]*QueueMessage, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.ReadMessages(lastMessageID, maxCount)
}

func (p *queueRateLimitedPersistenceClient) UpdateAckLevel(messageID int, clusterName string) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.UpdateAckLevel(messageID, clusterName)
}

func (p *queueRateLimitedPersistenceClient) GetAckLevels() (map[string]int, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, ErrPersistenceLimitExceeded
	}

	return p.persistence.GetAckLevels()
}

func (p *queueRateLimitedPersistenceClient) DeleteMessagesBefore(messageID int) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return ErrPersistenceLimitExceeded
	}

	return p.persistence.DeleteMessagesBefore(messageID)
}

func (p *queueRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}
