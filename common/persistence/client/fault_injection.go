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

package client

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
)

type (
	FaultInjectionDataStoreFactory struct {
		baseFactory    DataStoreFactory
		config         *config.FaultInjection
		errorGenerator ErrorGenerator

		TaskStore      *FaultInjectionTaskStore
		ShardStore     *FaultInjectionShardStore
		MetadataStore  *FaultInjectionMetadataStore
		ExecutionStore *FaultInjectionExecutionStore
		Queue          *FaultInjectionQueue
		ClusterMDStore *FaultInjectionClusterMetadataStore
	}

	// FaultInjectionShardStore is a lower level of ShardManager
	FaultInjectionShardStore struct {
		baseShardStore persistence.ShardStore
		config         *config.FaultInjection
		ErrorGenerator ErrorGenerator
	}

	// FaultInjectionTaskStore is a lower level of TaskManager
	FaultInjectionTaskStore struct {
		baseTaskStore  persistence.TaskStore
		config         *config.FaultInjection
		ErrorGenerator ErrorGenerator
	}

	// FaultInjectionMetadataStore is a lower level of MetadataManager
	FaultInjectionMetadataStore struct {
		baseMetadataStore persistence.MetadataStore
		config            *config.FaultInjection
		ErrorGenerator    ErrorGenerator
	}

	// FaultInjectionClusterMetadataStore is a lower level of ClusterMetadataManager.
	// There is no Internal constructs needed to abstract away at the interface level currently,
	//  so we can reimplement the ClusterMetadataManager and leave this as a placeholder.
	FaultInjectionClusterMetadataStore struct {
		baseCMStore    persistence.ClusterMetadataStore
		config         *config.FaultInjection
		ErrorGenerator ErrorGenerator
	}

	// FaultInjectionExecutionStore is used to manage workflow execution including mutable states / history / tasks.
	FaultInjectionExecutionStore struct {
		baseExecutionStore persistence.ExecutionStore
		config             *config.FaultInjection
		ErrorGenerator     ErrorGenerator
	}

	// FaultInjectionQueue is a store to enqueue and get messages
	FaultInjectionQueue struct {
		baseQueue      persistence.Queue
		config         *config.FaultInjection
		ErrorGenerator ErrorGenerator
	}
)

// from errors.go ConvertError
var defaultErrors = []FaultWeight{
	// adding random error here to verify resiliency for API bugs
	{
		errFactory: func(msg string) error {
			return errors.New(fmt.Sprintf("FaultInjection poison pill: %s", msg))
		},
		weight: 1,
	},
	{
		errFactory: func(msg string) error {
			return serviceerror.NewInternal(msg)
		},
		weight: 1,
	},
	{
		errFactory: func(msg string) error {
			return &persistence.TimeoutError{Msg: msg}
		},
		weight: 1,
	},
	{
		errFactory: func(msg string) error {
			return serviceerror.NewResourceExhausted(msg)
		},
		weight: 1,
	},
}

func newErrorGenerator(config *config.FaultInjection, errorWeights []FaultWeight) ErrorGenerator {
	return NewDefaultErrorGenerator(config.Rate, errorWeights)
}

func NewFaultInjectionDatastoreFactory(
	config *config.FaultInjection,
	baseFactory DataStoreFactory,
) *FaultInjectionDataStoreFactory {
	errorGenerator := newErrorGenerator(
		config,
		[]FaultWeight{
			// from sqldb see NewShardStore
			{
				errFactory: func(data string) error { return fmt.Errorf("FaultInjectionDataStoreFactory: %s", data) },
				weight:     1,
			},
		},
	)
	return &FaultInjectionDataStoreFactory{
		baseFactory:    baseFactory,
		config:         config,
		errorGenerator: errorGenerator,
	}
}

func (d *FaultInjectionDataStoreFactory) Close() {
	d.baseFactory.Close()
}

func (d *FaultInjectionDataStoreFactory) UpdateRate(rate float64) {
	d.errorGenerator.UpdateRate(rate)
	d.TaskStore.UpdateRate(rate)
	d.ShardStore.UpdateRate(rate)
	d.MetadataStore.UpdateRate(rate)
	d.ExecutionStore.UpdateRate(rate)
	d.Queue.UpdateRate(rate)
	d.ClusterMDStore.UpdateRate(rate)
}

func (d *FaultInjectionDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	if d.TaskStore == nil {
		baseFactory, err := d.baseFactory.NewTaskStore()
		if err != nil {
			return nil, err
		}
		d.TaskStore, err = NewFaultInjectionTaskStore(d.config, baseFactory)
		if err != nil {
			return nil, err
		}
	}
	return d.TaskStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	if d.ShardStore == nil {
		baseFactory, err := d.baseFactory.NewShardStore()
		if err != nil {
			return nil, err
		}
		d.ShardStore, err = NewFaultInjectionShardStore(d.config, baseFactory)
		if err != nil {
			return nil, err
		}
	}
	return d.ShardStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	if d.MetadataStore == nil {
		baseStore, err := d.baseFactory.NewMetadataStore()
		if err != nil {
			return nil, err
		}
		d.MetadataStore, err = NewFaultInjectionMetadataStore(d.config, baseStore)
		if err != nil {
			return nil, err
		}
	}
	return d.MetadataStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	if d.ExecutionStore == nil {
		baseStore, err := d.baseFactory.NewExecutionStore()
		if err != nil {
			return nil, err
		}
		d.ExecutionStore, err = NewFaultInjectionExecutionStore(d.config, baseStore)
		if err != nil {
			return nil, err
		}

	}
	return d.ExecutionStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	if d.Queue == nil {
		baseQueue, err := d.baseFactory.NewQueue(queueType)
		if err != nil {
			return baseQueue, err
		}
		d.Queue, err = NewFaultInjectionQueue(d.config, baseQueue)
		if err != nil {
			return nil, err
		}

	}
	return d.Queue, nil
}

func (d *FaultInjectionDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	if d.ClusterMDStore == nil {
		baseStore, err := d.baseFactory.NewClusterMetadataStore()
		if err != nil {
			return nil, err
		}
		d.ClusterMDStore, err = NewFaultInjectionClusterMetadataStore(d.config, baseStore)
		if err != nil {
			return nil, err
		}

	}
	return d.ClusterMDStore, nil
}

func NewFaultInjectionQueue(config *config.FaultInjection, baseQueue persistence.Queue) (*FaultInjectionQueue, error) {
	errorGenerator := newErrorGenerator(config,
		append(defaultErrors,
			FaultWeight{
				errFactory: func(msg string) error {
					return &persistence.ShardOwnershipLostError{
						ShardID: -1,
						Msg:     fmt.Sprintf("FaultInjectionQueue injected, %s", msg),
					}
				},
				weight: 1,
			},
		),
	)

	return &FaultInjectionQueue{
		baseQueue:      baseQueue,
		config:         config,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (q *FaultInjectionQueue) Close() {
	q.baseQueue.Close()
}

func (q *FaultInjectionQueue) Init(blob *commonpb.DataBlob) error {
	// potentially Init can return golang errors from blob.go encode/decode.
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.Init(blob)
}

func (q *FaultInjectionQueue) EnqueueMessage(blob commonpb.DataBlob) error {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.EnqueueMessage(blob)
}

func (q *FaultInjectionQueue) ReadMessages(lastMessageID int64, maxCount int) ([]*persistence.QueueMessage, error) {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return q.baseQueue.ReadMessages(lastMessageID, maxCount)
}

func (q *FaultInjectionQueue) DeleteMessagesBefore(messageID int64) error {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.DeleteMessagesBefore(messageID)
}

func (q *FaultInjectionQueue) UpdateAckLevel(metadata *persistence.InternalQueueMetadata) error {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.UpdateAckLevel(metadata)
}

func (q *FaultInjectionQueue) GetAckLevels() (*persistence.InternalQueueMetadata, error) {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return q.baseQueue.GetAckLevels()
}

func (q *FaultInjectionQueue) EnqueueMessageToDLQ(blob commonpb.DataBlob) (int64, error) {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return 0, err
	}
	return q.baseQueue.EnqueueMessageToDLQ(blob)
}

func (q *FaultInjectionQueue) ReadMessagesFromDLQ(
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return nil, nil, err
	}
	return q.baseQueue.ReadMessagesFromDLQ(firstMessageID, lastMessageID, pageSize, pageToken)
}

func (q *FaultInjectionQueue) DeleteMessageFromDLQ(messageID int64) error {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.DeleteMessageFromDLQ(messageID)
}

func (q *FaultInjectionQueue) RangeDeleteMessagesFromDLQ(firstMessageID int64, lastMessageID int64) error {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.RangeDeleteMessagesFromDLQ(firstMessageID, lastMessageID)
}

func (q *FaultInjectionQueue) UpdateDLQAckLevel(metadata *persistence.InternalQueueMetadata) error {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.UpdateDLQAckLevel(metadata)
}

func (q *FaultInjectionQueue) GetDLQAckLevels() (*persistence.InternalQueueMetadata, error) {
	if err := q.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return q.baseQueue.GetDLQAckLevels()
}

func (q *FaultInjectionQueue) UpdateRate(rate float64) {
	q.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionExecutionStore(config *config.FaultInjection, executionStore persistence.ExecutionStore) (
	*FaultInjectionExecutionStore,
	error,
) {
	errorGenerator := newErrorGenerator(
		config,
		append(
			defaultErrors,
			FaultWeight{
				errFactory: func(msg string) error {
					return &persistence.ShardOwnershipLostError{
						ShardID: -1,
						Msg:     fmt.Sprintf("FaultInjectionQueue injected, %s", msg),
					}
				},
				weight: 1,
			},
		),
	)
	return &FaultInjectionExecutionStore{
		baseExecutionStore: executionStore,
		config:             config,
		ErrorGenerator:     errorGenerator,
	}, nil
}

func (e *FaultInjectionExecutionStore) Close() {
	e.baseExecutionStore.Close()
}

func (e *FaultInjectionExecutionStore) GetName() string {
	return e.baseExecutionStore.GetName()
}

func (e *FaultInjectionExecutionStore) GetWorkflowExecution(request *persistence.GetWorkflowExecutionRequest) (
	*persistence.InternalGetWorkflowExecutionResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) UpdateWorkflowExecution(request *persistence.InternalUpdateWorkflowExecutionRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.UpdateWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) ConflictResolveWorkflowExecution(request *persistence.InternalConflictResolveWorkflowExecutionRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.ConflictResolveWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) CreateWorkflowExecution(request *persistence.InternalCreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.CreateWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) DeleteWorkflowExecution(request *persistence.DeleteWorkflowExecutionRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) DeleteCurrentWorkflowExecution(request *persistence.DeleteCurrentWorkflowExecutionRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteCurrentWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) GetCurrentExecution(request *persistence.GetCurrentExecutionRequest) (
	*persistence.InternalGetCurrentExecutionResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetCurrentExecution(request)
}

func (e *FaultInjectionExecutionStore) ListConcreteExecutions(request *persistence.ListConcreteExecutionsRequest) (
	*persistence.InternalListConcreteExecutionsResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.ListConcreteExecutions(request)
}

func (e *FaultInjectionExecutionStore) AddTasks(request *persistence.AddTasksRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.AddTasks(request)
}

func (e *FaultInjectionExecutionStore) GetTransferTask(request *persistence.GetTransferTaskRequest) (
	*persistence.GetTransferTaskResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTransferTask(request)
}

func (e *FaultInjectionExecutionStore) GetTransferTasks(request *persistence.GetTransferTasksRequest) (
	*persistence.GetTransferTasksResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTransferTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteTransferTask(request *persistence.CompleteTransferTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteTransferTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteTransferTask(request *persistence.RangeCompleteTransferTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteTransferTask(request)
}

func (e *FaultInjectionExecutionStore) GetTimerTask(request *persistence.GetTimerTaskRequest) (
	*persistence.GetTimerTaskResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTimerTask(request)
}

func (e *FaultInjectionExecutionStore) GetTimerIndexTasks(request *persistence.GetTimerIndexTasksRequest) (
	*persistence.GetTimerIndexTasksResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTimerIndexTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteTimerTask(request *persistence.CompleteTimerTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteTimerTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteTimerTask(request *persistence.RangeCompleteTimerTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteTimerTask(request)
}

func (e *FaultInjectionExecutionStore) GetReplicationTask(request *persistence.GetReplicationTaskRequest) (
	*persistence.GetReplicationTaskResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetReplicationTask(request)
}

func (e *FaultInjectionExecutionStore) GetReplicationTasks(request *persistence.GetReplicationTasksRequest) (
	*persistence.GetReplicationTasksResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetReplicationTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteReplicationTask(request *persistence.CompleteReplicationTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteReplicationTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteReplicationTask(request *persistence.RangeCompleteReplicationTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteReplicationTask(request)
}

func (e *FaultInjectionExecutionStore) PutReplicationTaskToDLQ(request *persistence.PutReplicationTaskToDLQRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.PutReplicationTaskToDLQ(request)
}

func (e *FaultInjectionExecutionStore) GetReplicationTasksFromDLQ(request *persistence.GetReplicationTasksFromDLQRequest) (
	*persistence.GetReplicationTasksFromDLQResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetReplicationTasksFromDLQ(request)
}

func (e *FaultInjectionExecutionStore) DeleteReplicationTaskFromDLQ(request *persistence.DeleteReplicationTaskFromDLQRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteReplicationTaskFromDLQ(request)
}

func (e *FaultInjectionExecutionStore) RangeDeleteReplicationTaskFromDLQ(request *persistence.RangeDeleteReplicationTaskFromDLQRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeDeleteReplicationTaskFromDLQ(request)
}

func (e *FaultInjectionExecutionStore) GetVisibilityTask(request *persistence.GetVisibilityTaskRequest) (
	*persistence.GetVisibilityTaskResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetVisibilityTask(request)
}

func (e *FaultInjectionExecutionStore) GetVisibilityTasks(request *persistence.GetVisibilityTasksRequest) (
	*persistence.GetVisibilityTasksResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetVisibilityTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteVisibilityTask(request *persistence.CompleteVisibilityTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteVisibilityTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteVisibilityTask(request *persistence.RangeCompleteVisibilityTaskRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteVisibilityTask(request)
}

func (e *FaultInjectionExecutionStore) AppendHistoryNodes(request *persistence.InternalAppendHistoryNodesRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.AppendHistoryNodes(request)
}

func (e *FaultInjectionExecutionStore) DeleteHistoryNodes(request *persistence.InternalDeleteHistoryNodesRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteHistoryNodes(request)
}

func (e *FaultInjectionExecutionStore) ReadHistoryBranch(request *persistence.InternalReadHistoryBranchRequest) (
	*persistence.InternalReadHistoryBranchResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.ReadHistoryBranch(request)
}

func (e *FaultInjectionExecutionStore) ForkHistoryBranch(request *persistence.InternalForkHistoryBranchRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.ForkHistoryBranch(request)
}

func (e *FaultInjectionExecutionStore) DeleteHistoryBranch(request *persistence.InternalDeleteHistoryBranchRequest) error {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteHistoryBranch(request)
}

func (e *FaultInjectionExecutionStore) GetHistoryTree(request *persistence.GetHistoryTreeRequest) (
	*persistence.InternalGetHistoryTreeResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetHistoryTree(request)
}

func (e *FaultInjectionExecutionStore) GetAllHistoryTreeBranches(request *persistence.GetAllHistoryTreeBranchesRequest) (
	*persistence.InternalGetAllHistoryTreeBranchesResponse,
	error,
) {
	if err := e.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetAllHistoryTreeBranches(request)
}

func (e *FaultInjectionExecutionStore) UpdateRate(rate float64) {
	e.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionClusterMetadataStore(config *config.FaultInjection, baseStore persistence.ClusterMetadataStore) (
	*FaultInjectionClusterMetadataStore,
	error,
) {
	errorGenerator := newErrorGenerator(config, defaultErrors)
	return &FaultInjectionClusterMetadataStore{
		baseCMStore:    baseStore,
		config:         config,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (c *FaultInjectionClusterMetadataStore) Close() {
	c.baseCMStore.Close()
}

func (c *FaultInjectionClusterMetadataStore) GetName() string {
	return c.baseCMStore.GetName()
}

func (c *FaultInjectionClusterMetadataStore) GetClusterMetadata() (
	*persistence.InternalGetClusterMetadataResponse,
	error,
) {
	if err := c.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return c.baseCMStore.GetClusterMetadata()
}

func (c *FaultInjectionClusterMetadataStore) SaveClusterMetadata(request *persistence.InternalSaveClusterMetadataRequest) (
	bool,
	error,
) {
	if err := c.ErrorGenerator.Generate(); err != nil {
		return false, err
	}
	return c.baseCMStore.SaveClusterMetadata(request)
}

func (c *FaultInjectionClusterMetadataStore) GetClusterMembers(request *persistence.GetClusterMembersRequest) (
	*persistence.GetClusterMembersResponse,
	error,
) {
	if err := c.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return c.baseCMStore.GetClusterMembers(request)
}

func (c *FaultInjectionClusterMetadataStore) UpsertClusterMembership(request *persistence.UpsertClusterMembershipRequest) error {
	if err := c.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return c.baseCMStore.UpsertClusterMembership(request)
}

func (c *FaultInjectionClusterMetadataStore) PruneClusterMembership(request *persistence.PruneClusterMembershipRequest) error {
	if err := c.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return c.baseCMStore.PruneClusterMembership(request)
}

func (c *FaultInjectionClusterMetadataStore) UpdateRate(rate float64) {
	c.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionMetadataStore(
	config *config.FaultInjection,
	metadataStore persistence.MetadataStore,
) (*FaultInjectionMetadataStore, error) {
	errorGenerator := newErrorGenerator(config, defaultErrors)
	return &FaultInjectionMetadataStore{
		baseMetadataStore: metadataStore,
		config:            config,
		ErrorGenerator:    errorGenerator,
	}, nil
}

func (m *FaultInjectionMetadataStore) Close() {
	m.baseMetadataStore.Close()
}

func (m *FaultInjectionMetadataStore) GetName() string {
	return m.baseMetadataStore.GetName()
}

func (m *FaultInjectionMetadataStore) CreateNamespace(request *persistence.InternalCreateNamespaceRequest) (
	*persistence.CreateNamespaceResponse,
	error,
) {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.CreateNamespace(request)
}

func (m *FaultInjectionMetadataStore) GetNamespace(request *persistence.GetNamespaceRequest) (
	*persistence.InternalGetNamespaceResponse,
	error,
) {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.GetNamespace(request)
}

func (m *FaultInjectionMetadataStore) UpdateNamespace(request *persistence.InternalUpdateNamespaceRequest) error {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return m.baseMetadataStore.UpdateNamespace(request)
}

func (m *FaultInjectionMetadataStore) DeleteNamespace(request *persistence.DeleteNamespaceRequest) error {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return m.baseMetadataStore.DeleteNamespace(request)
}

func (m *FaultInjectionMetadataStore) DeleteNamespaceByName(request *persistence.DeleteNamespaceByNameRequest) error {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return m.baseMetadataStore.DeleteNamespaceByName(request)
}

func (m *FaultInjectionMetadataStore) ListNamespaces(request *persistence.ListNamespacesRequest) (
	*persistence.InternalListNamespacesResponse,
	error,
) {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.ListNamespaces(request)
}

func (m *FaultInjectionMetadataStore) GetMetadata() (*persistence.GetMetadataResponse, error) {
	if err := m.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.GetMetadata()
}

func (m *FaultInjectionMetadataStore) UpdateRate(rate float64) {
	m.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionTaskStore(
	config *config.FaultInjection,
	baseTaskStore persistence.TaskStore,
) (*FaultInjectionTaskStore, error) {
	errorGenerator := newErrorGenerator(config, defaultErrors)

	return &FaultInjectionTaskStore{
		baseTaskStore:  baseTaskStore,
		config:         config,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (t *FaultInjectionTaskStore) Close() {
	t.baseTaskStore.Close()
}

func (t *FaultInjectionTaskStore) GetName() string {
	return t.baseTaskStore.GetName()
}

func (t *FaultInjectionTaskStore) CreateTaskQueue(request *persistence.InternalCreateTaskQueueRequest) error {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.CreateTaskQueue(request)
}

func (t *FaultInjectionTaskStore) GetTaskQueue(request *persistence.InternalGetTaskQueueRequest) (
	*persistence.InternalGetTaskQueueResponse,
	error,
) {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	// todomigryz newPersistedTypeMismatchError
	return t.baseTaskStore.GetTaskQueue(request)
}

func (t *FaultInjectionTaskStore) ExtendLease(request *persistence.InternalExtendLeaseRequest) error {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.ExtendLease(request)
}

func (t *FaultInjectionTaskStore) UpdateTaskQueue(request *persistence.InternalUpdateTaskQueueRequest) (
	*persistence.UpdateTaskQueueResponse,
	error,
) {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.UpdateTaskQueue(request)
}

func (t *FaultInjectionTaskStore) ListTaskQueue(request *persistence.ListTaskQueueRequest) (
	*persistence.InternalListTaskQueueResponse,
	error,
) {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.ListTaskQueue(request)
}

func (t *FaultInjectionTaskStore) DeleteTaskQueue(request *persistence.DeleteTaskQueueRequest) error {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.DeleteTaskQueue(request)
}

func (t *FaultInjectionTaskStore) CreateTasks(request *persistence.InternalCreateTasksRequest) (
	*persistence.CreateTasksResponse,
	error,
) {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.CreateTasks(request)
}

func (t *FaultInjectionTaskStore) GetTasks(request *persistence.GetTasksRequest) (
	*persistence.InternalGetTasksResponse,
	error,
) {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.GetTasks(request)
}

func (t *FaultInjectionTaskStore) CompleteTask(request *persistence.CompleteTaskRequest) error {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.CompleteTask(request)
}

func (t *FaultInjectionTaskStore) CompleteTasksLessThan(request *persistence.CompleteTasksLessThanRequest) (
	int,
	error,
) {
	if err := t.ErrorGenerator.Generate(); err != nil {
		return 0, err
	}
	return t.baseTaskStore.CompleteTasksLessThan(request)
}

func (t *FaultInjectionTaskStore) UpdateRate(rate float64) {
	t.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionShardStore(
	config *config.FaultInjection,
	baseShardStore persistence.ShardStore,
) (*FaultInjectionShardStore, error) {
	errorWeights := append(
		defaultErrors,
		FaultWeight{
			errFactory: func(msg string) error {
				return &persistence.ShardOwnershipLostError{
					ShardID: -1,
					Msg:     fmt.Sprintf("FaultInjectionShardStore injected, %s", msg),
				}
			},
			weight: 1,
		},
	)
	errorGenerator := newErrorGenerator(config, errorWeights)
	return &FaultInjectionShardStore{
		baseShardStore: baseShardStore,
		config:         config,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (s *FaultInjectionShardStore) Close() {
	s.baseShardStore.Close()
}

func (s *FaultInjectionShardStore) GetName() string {
	return s.baseShardStore.GetName()
}

func (s *FaultInjectionShardStore) GetClusterName() string {
	return s.baseShardStore.GetClusterName()
}

func (s *FaultInjectionShardStore) CreateShard(request *persistence.InternalCreateShardRequest) error {
	if err := s.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return s.baseShardStore.CreateShard(request)
}

func (s *FaultInjectionShardStore) GetShard(request *persistence.InternalGetShardRequest) (
	*persistence.InternalGetShardResponse,
	error,
) {
	if err := s.ErrorGenerator.Generate(); err != nil {
		return nil, err
	}
	return s.baseShardStore.GetShard(request)
}

func (s *FaultInjectionShardStore) UpdateShard(request *persistence.InternalUpdateShardRequest) error {
	if err := s.ErrorGenerator.Generate(); err != nil {
		return err
	}
	return s.baseShardStore.UpdateShard(request)
}

func (s *FaultInjectionShardStore) UpdateRate(rate float64) {
	s.ErrorGenerator.UpdateRate(rate)
}
