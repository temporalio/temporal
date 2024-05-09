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
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
)

type (
	FaultInjectionDataStoreFactory struct {
		baseFactory    DataStoreFactory
		config         *config.FaultInjection
		ErrorGenerator ErrorGenerator

		TaskStore          *FaultInjectionTaskStore
		ShardStore         *FaultInjectionShardStore
		MetadataStore      *FaultInjectionMetadataStore
		ExecutionStore     *FaultInjectionExecutionStore
		Queue              *FaultInjectionQueue
		QueueV2            *FaultInjectionQueueV2
		ClusterMDStore     *FaultInjectionClusterMetadataStore
		NexusEndpointStore *FaultInjectionNexusEndpointStore
	}

	FaultInjectionShardStore struct {
		baseShardStore persistence.ShardStore
		ErrorGenerator ErrorGenerator
	}

	FaultInjectionTaskStore struct {
		baseTaskStore  persistence.TaskStore
		ErrorGenerator ErrorGenerator
	}

	FaultInjectionMetadataStore struct {
		baseMetadataStore persistence.MetadataStore
		ErrorGenerator    ErrorGenerator
	}

	FaultInjectionClusterMetadataStore struct {
		baseCMStore    persistence.ClusterMetadataStore
		ErrorGenerator ErrorGenerator
	}

	FaultInjectionExecutionStore struct {
		persistence.HistoryBranchUtilImpl
		baseExecutionStore persistence.ExecutionStore
		ErrorGenerator     ErrorGenerator
	}

	FaultInjectionQueue struct {
		baseQueue      persistence.Queue
		ErrorGenerator ErrorGenerator
	}

	FaultInjectionQueueV2 struct {
		baseQueue      persistence.QueueV2
		ErrorGenerator ErrorGenerator
	}

	FaultInjectionNexusEndpointStore struct {
		baseNexusEndpointStore persistence.NexusEndpointStore
		ErrorGenerator         ErrorGenerator
	}
)

// from errors.go ConvertError
var defaultErrors = []FaultWeight{
	{
		errFactory: func(msg string) *fault {
			return newFault(serviceerror.NewUnavailable(fmt.Sprintf("serviceerror.NewUnavailable: %s", msg)))
		},
		weight: 1,
	},
	{
		errFactory: func(msg string) *fault {
			return newFault(&persistence.TimeoutError{Msg: fmt.Sprintf("persistence.TimeoutError: %s", msg)})
		},
		weight: 1,
	},
	{
		errFactory: func(msg string) *fault {
			return newFault(&serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
				Message: fmt.Sprintf("serviceerror.NewResourceExhausted: %s", msg),
			})
		},
		weight: 1,
	},
}

func newErrorGenerator(rate float64, errorWeights []FaultWeight) ErrorGenerator {
	return NewDefaultErrorGenerator(rate, errorWeights)
}

func NewFaultInjectionDatastoreFactory(
	config *config.FaultInjection,
	baseFactory DataStoreFactory,
) *FaultInjectionDataStoreFactory {
	errorGenerator := newErrorGenerator(
		config.Rate,
		[]FaultWeight{
			{
				errFactory: func(data string) *fault { return newFault(fmt.Errorf("FaultInjectionDataStoreFactory: %s", data)) },
				weight:     1,
			},
		},
	)
	return &FaultInjectionDataStoreFactory{
		baseFactory:    baseFactory,
		config:         config,
		ErrorGenerator: errorGenerator,
	}
}

func (d *FaultInjectionDataStoreFactory) Close() {
	d.baseFactory.Close()
}

func (d *FaultInjectionDataStoreFactory) UpdateRate(rate float64) {
	d.ErrorGenerator.UpdateRate(rate)
	d.TaskStore.UpdateRate(rate)
	d.ShardStore.UpdateRate(rate)
	d.MetadataStore.UpdateRate(rate)
	d.ExecutionStore.UpdateRate(rate)
	d.Queue.UpdateRate(rate)
	d.ClusterMDStore.UpdateRate(rate)
	d.NexusEndpointStore.UpdateRate(rate)
}

func (d *FaultInjectionDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	if d.TaskStore == nil {
		baseFactory, err := d.baseFactory.NewTaskStore()
		if err != nil {
			return nil, err
		}
		if storeConfig, ok := d.config.Targets.DataStores[config.TaskStoreName]; ok {
			d.TaskStore = &FaultInjectionTaskStore{
				baseTaskStore:  baseFactory,
				ErrorGenerator: NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.TaskStore, err = NewFaultInjectionTaskStore(d.ErrorGenerator.Rate(), baseFactory)
			if err != nil {
				return nil, err
			}
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
		if storeConfig, ok := d.config.Targets.DataStores[config.ShardStoreName]; ok {
			d.ShardStore = &FaultInjectionShardStore{
				baseShardStore: baseFactory,
				ErrorGenerator: NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.ShardStore, err = NewFaultInjectionShardStore(d.ErrorGenerator.Rate(), baseFactory)
			if err != nil {
				return nil, err
			}
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
		if storeConfig, ok := d.config.Targets.DataStores[config.MetadataStoreName]; ok {
			d.MetadataStore = &FaultInjectionMetadataStore{
				baseMetadataStore: baseStore,
				ErrorGenerator:    NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.MetadataStore, err = NewFaultInjectionMetadataStore(d.ErrorGenerator.Rate(), baseStore)
			if err != nil {
				return nil, err
			}
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
		if storeConfig, ok := d.config.Targets.DataStores[config.ExecutionStoreName]; ok {
			d.ExecutionStore = &FaultInjectionExecutionStore{
				baseExecutionStore: baseStore,
				ErrorGenerator:     NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.ExecutionStore, err = NewFaultInjectionExecutionStore(d.ErrorGenerator.Rate(), baseStore)
			if err != nil {
				return nil, err
			}
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
		if storeConfig, ok := d.config.Targets.DataStores[config.QueueName]; ok {
			d.Queue = &FaultInjectionQueue{
				baseQueue:      baseQueue,
				ErrorGenerator: NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.Queue, err = NewFaultInjectionQueue(d.ErrorGenerator.Rate(), baseQueue)
			if err != nil {
				return nil, err
			}
		}
	}
	return d.Queue, nil
}

func (d *FaultInjectionDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if d.QueueV2 == nil {
		baseQueue, err := d.baseFactory.NewQueueV2()
		if err != nil {
			return baseQueue, err
		}
		if storeConfig, ok := d.config.Targets.DataStores[config.QueueV2Name]; ok {
			d.QueueV2 = &FaultInjectionQueueV2{
				baseQueue:      baseQueue,
				ErrorGenerator: NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.QueueV2 = NewFaultInjectionQueueV2(d.ErrorGenerator.Rate(), baseQueue)
		}
	}
	return d.QueueV2, nil
}

func (d *FaultInjectionDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	if d.ClusterMDStore == nil {
		baseStore, err := d.baseFactory.NewClusterMetadataStore()
		if err != nil {
			return nil, err
		}
		if storeConfig, ok := d.config.Targets.DataStores[config.ClusterMDStoreName]; ok {
			d.ClusterMDStore = &FaultInjectionClusterMetadataStore{
				baseCMStore:    baseStore,
				ErrorGenerator: NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.ClusterMDStore, err = NewFaultInjectionClusterMetadataStore(d.ErrorGenerator.Rate(), baseStore)
			if err != nil {
				return nil, err
			}
		}

	}
	return d.ClusterMDStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	if d.NexusEndpointStore == nil {
		baseStore, err := d.baseFactory.NewNexusEndpointStore()
		if err != nil {
			return nil, err
		}
		if storeConfig, ok := d.config.Targets.DataStores[config.NexusEndpointStoreName]; ok {
			d.NexusEndpointStore = &FaultInjectionNexusEndpointStore{
				baseNexusEndpointStore: baseStore,
				ErrorGenerator:         NewTargetedDataStoreErrorGenerator(&storeConfig),
			}
		} else {
			d.NexusEndpointStore, err = NewFaultInjectionNexusEndpointStore(d.ErrorGenerator.Rate(), baseStore)
			if err != nil {
				return nil, err
			}
		}
	}
	return d.NexusEndpointStore, nil
}

func NewFaultInjectionQueue(rate float64, baseQueue persistence.Queue) (*FaultInjectionQueue, error) {
	errorGenerator := newErrorGenerator(rate,
		append(defaultErrors,
			FaultWeight{
				errFactory: func(msg string) *fault {
					return newFault(&persistence.ShardOwnershipLostError{
						ShardID: -1,
						Msg:     fmt.Sprintf("FaultInjectionQueue injected, %s", msg),
					})
				},
				weight: 1,
			},
		),
	)

	return &FaultInjectionQueue{
		baseQueue:      baseQueue,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (q *FaultInjectionQueue) Close() {
	q.baseQueue.Close()
}

func (q *FaultInjectionQueue) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	// potentially Init can return golang errors from blob.go encode/decode.
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.Init(ctx, blob)
	})
}

func (q *FaultInjectionQueue) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.EnqueueMessage(ctx, blob)
	})
}

func (q *FaultInjectionQueue) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	return inject1(q.ErrorGenerator.Generate(), func() ([]*persistence.QueueMessage, error) {
		return q.baseQueue.ReadMessages(ctx, lastMessageID, maxCount)
	})
}

func (q *FaultInjectionQueue) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.DeleteMessagesBefore(ctx, messageID)
	})
}

func (q *FaultInjectionQueue) UpdateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.UpdateAckLevel(ctx, metadata)
	})
}

func (q *FaultInjectionQueue) GetAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	return inject1(q.ErrorGenerator.Generate(), func() (*persistence.InternalQueueMetadata, error) {
		return q.baseQueue.GetAckLevels(ctx)
	})
}

func (q *FaultInjectionQueue) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	return inject1(q.ErrorGenerator.Generate(), func() (int64, error) {
		return q.baseQueue.EnqueueMessageToDLQ(ctx, blob)
	})
}

func (q *FaultInjectionQueue) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	return inject2(q.ErrorGenerator.Generate(), func() ([]*persistence.QueueMessage, []byte, error) {
		return q.baseQueue.ReadMessagesFromDLQ(ctx, firstMessageID, lastMessageID, pageSize, pageToken)
	})
}

func (q *FaultInjectionQueue) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.DeleteMessageFromDLQ(ctx, messageID)
	})
}

func (q *FaultInjectionQueue) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.RangeDeleteMessagesFromDLQ(ctx, firstMessageID, lastMessageID)
	})
}

func (q *FaultInjectionQueue) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return inject0(q.ErrorGenerator.Generate(), func() error {
		return q.baseQueue.UpdateDLQAckLevel(ctx, metadata)
	})
}

func (q *FaultInjectionQueue) GetDLQAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	return inject1(q.ErrorGenerator.Generate(), func() (*persistence.InternalQueueMetadata, error) {
		return q.baseQueue.GetDLQAckLevels(ctx)
	})
}

func (q *FaultInjectionQueue) UpdateRate(rate float64) {
	q.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionQueueV2(rate float64, baseQueue persistence.QueueV2) *FaultInjectionQueueV2 {
	errorGenerator := newErrorGenerator(rate, defaultErrors)

	return &FaultInjectionQueueV2{
		baseQueue:      baseQueue,
		ErrorGenerator: errorGenerator,
	}
}

func (f *FaultInjectionQueueV2) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	return inject1(f.ErrorGenerator.Generate(), func() (*persistence.InternalEnqueueMessageResponse, error) {
		return f.baseQueue.EnqueueMessage(ctx, request)
	})
}

func (f *FaultInjectionQueueV2) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	return inject1(f.ErrorGenerator.Generate(), func() (*persistence.InternalReadMessagesResponse, error) {
		return f.baseQueue.ReadMessages(ctx, request)
	})
}

func (f *FaultInjectionQueueV2) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	return inject1(f.ErrorGenerator.Generate(), func() (*persistence.InternalCreateQueueResponse, error) {
		return f.baseQueue.CreateQueue(ctx, request)
	})
}

func (f *FaultInjectionQueueV2) RangeDeleteMessages(
	ctx context.Context,
	request *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	return inject1(f.ErrorGenerator.Generate(), func() (*persistence.InternalRangeDeleteMessagesResponse, error) {
		return f.baseQueue.RangeDeleteMessages(ctx, request)
	})
}

func (f *FaultInjectionQueueV2) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	return inject1(f.ErrorGenerator.Generate(), func() (*persistence.InternalListQueuesResponse, error) {
		return f.baseQueue.ListQueues(ctx, request)
	})
}

func NewFaultInjectionExecutionStore(
	rate float64,
	executionStore persistence.ExecutionStore,
) (*FaultInjectionExecutionStore, error) {
	// TODO: inject shard ownership lost ever after
	// queue processor can notify shard upon unloading itself
	// when shard ownership lost error is encountered.
	errorGenerator := newErrorGenerator(
		rate,
		defaultErrors,
	)
	return &FaultInjectionExecutionStore{
		baseExecutionStore: executionStore,
		ErrorGenerator:     errorGenerator,
	}, nil
}

func (e *FaultInjectionExecutionStore) Close() {
	e.baseExecutionStore.Close()
}

func (e *FaultInjectionExecutionStore) GetName() string {
	return e.baseExecutionStore.GetName()
}

func (e *FaultInjectionExecutionStore) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalGetWorkflowExecutionResponse, error) {
		return e.baseExecutionStore.GetWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalSetWorkflowExecutionRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.SetWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.UpdateWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalConflictResolveWorkflowExecutionRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.ConflictResolveWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
) (*persistence.InternalCreateWorkflowExecutionResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalCreateWorkflowExecutionResponse, error) {
		return e.baseExecutionStore.CreateWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteWorkflowExecutionRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.DeleteWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.DeleteCurrentWorkflowExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.InternalGetCurrentExecutionResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalGetCurrentExecutionResponse, error) {
		return e.baseExecutionStore.GetCurrentExecution(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ListConcreteExecutions(
	ctx context.Context,
	request *persistence.ListConcreteExecutionsRequest,
) (*persistence.InternalListConcreteExecutionsResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalListConcreteExecutionsResponse, error) {
		return e.baseExecutionStore.ListConcreteExecutions(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) AddHistoryTasks(
	ctx context.Context,
	request *persistence.InternalAddHistoryTasksRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.AddHistoryTasks(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetHistoryTasks(
	ctx context.Context,
	request *persistence.GetHistoryTasksRequest,
) (*persistence.InternalGetHistoryTasksResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalGetHistoryTasksResponse, error) {
		return e.baseExecutionStore.GetHistoryTasks(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) CompleteHistoryTask(
	ctx context.Context,
	request *persistence.CompleteHistoryTaskRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.CompleteHistoryTask(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.RangeCompleteHistoryTasks(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.PutReplicationTaskToDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (*persistence.InternalGetHistoryTasksResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalGetHistoryTasksResponse, error) {
		return e.baseExecutionStore.GetReplicationTasksFromDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.DeleteReplicationTaskFromDLQRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.DeleteReplicationTaskFromDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.RangeDeleteReplicationTaskFromDLQ(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (bool, error) {
		return e.baseExecutionStore.IsReplicationDLQEmpty(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) AppendHistoryNodes(
	ctx context.Context,
	request *persistence.InternalAppendHistoryNodesRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.AppendHistoryNodes(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteHistoryNodes(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryNodesRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.DeleteHistoryNodes(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.InternalReadHistoryBranchRequest,
) (*persistence.InternalReadHistoryBranchResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalReadHistoryBranchResponse, error) {
		return e.baseExecutionStore.ReadHistoryBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) ForkHistoryBranch(
	ctx context.Context,
	request *persistence.InternalForkHistoryBranchRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.ForkHistoryBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) DeleteHistoryBranch(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryBranchRequest,
) error {
	return inject0(e.ErrorGenerator.Generate(), func() error {
		return e.baseExecutionStore.DeleteHistoryBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetHistoryTreeContainingBranch(
	ctx context.Context,
	request *persistence.InternalGetHistoryTreeContainingBranchRequest,
) (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
		return e.baseExecutionStore.GetHistoryTreeContainingBranch(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *persistence.GetAllHistoryTreeBranchesRequest,
) (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
	return inject1(e.ErrorGenerator.Generate(), func() (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
		return e.baseExecutionStore.GetAllHistoryTreeBranches(ctx, request)
	})
}

func (e *FaultInjectionExecutionStore) UpdateRate(rate float64) {
	e.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionClusterMetadataStore(
	rate float64,
	baseStore persistence.ClusterMetadataStore,
) (*FaultInjectionClusterMetadataStore, error) {
	errorGenerator := newErrorGenerator(rate, defaultErrors)
	return &FaultInjectionClusterMetadataStore{
		baseCMStore:    baseStore,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (c *FaultInjectionClusterMetadataStore) Close() {
	c.baseCMStore.Close()
}

func (c *FaultInjectionClusterMetadataStore) GetName() string {
	return c.baseCMStore.GetName()
}

func (c *FaultInjectionClusterMetadataStore) ListClusterMetadata(
	ctx context.Context,
	request *persistence.InternalListClusterMetadataRequest,
) (*persistence.InternalListClusterMetadataResponse, error) {
	return inject1(c.ErrorGenerator.Generate(), func() (*persistence.InternalListClusterMetadataResponse, error) {
		return c.baseCMStore.ListClusterMetadata(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) GetClusterMetadata(
	ctx context.Context,
	request *persistence.InternalGetClusterMetadataRequest,
) (*persistence.InternalGetClusterMetadataResponse, error) {
	return inject1(c.ErrorGenerator.Generate(), func() (*persistence.InternalGetClusterMetadataResponse, error) {
		return c.baseCMStore.GetClusterMetadata(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *persistence.InternalSaveClusterMetadataRequest,
) (bool, error) {
	return inject1(c.ErrorGenerator.Generate(), func() (bool, error) {
		return c.baseCMStore.SaveClusterMetadata(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) DeleteClusterMetadata(
	ctx context.Context,
	request *persistence.InternalDeleteClusterMetadataRequest,
) error {
	return inject0(c.ErrorGenerator.Generate(), func() error {
		return c.baseCMStore.DeleteClusterMetadata(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) GetClusterMembers(
	ctx context.Context,
	request *persistence.GetClusterMembersRequest,
) (*persistence.GetClusterMembersResponse, error) {
	return inject1(c.ErrorGenerator.Generate(), func() (*persistence.GetClusterMembersResponse, error) {
		return c.baseCMStore.GetClusterMembers(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) UpsertClusterMembership(
	ctx context.Context,
	request *persistence.UpsertClusterMembershipRequest,
) error {
	return inject0(c.ErrorGenerator.Generate(), func() error {
		return c.baseCMStore.UpsertClusterMembership(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) PruneClusterMembership(
	ctx context.Context,
	request *persistence.PruneClusterMembershipRequest,
) error {
	return inject0(c.ErrorGenerator.Generate(), func() error {
		return c.baseCMStore.PruneClusterMembership(ctx, request)
	})
}

func (c *FaultInjectionClusterMetadataStore) UpdateRate(rate float64) {
	c.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionMetadataStore(
	rate float64,
	metadataStore persistence.MetadataStore,
) (*FaultInjectionMetadataStore, error) {
	errorGenerator := newErrorGenerator(rate, defaultErrors)
	return &FaultInjectionMetadataStore{
		baseMetadataStore: metadataStore,
		ErrorGenerator:    errorGenerator,
	}, nil
}

func (m *FaultInjectionMetadataStore) Close() {
	m.baseMetadataStore.Close()
}

func (m *FaultInjectionMetadataStore) GetName() string {
	return m.baseMetadataStore.GetName()
}

func (m *FaultInjectionMetadataStore) CreateNamespace(
	ctx context.Context,
	request *persistence.InternalCreateNamespaceRequest,
) (*persistence.CreateNamespaceResponse, error) {
	return inject1(m.ErrorGenerator.Generate(), func() (*persistence.CreateNamespaceResponse, error) {
		return m.baseMetadataStore.CreateNamespace(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) GetNamespace(
	ctx context.Context,
	request *persistence.GetNamespaceRequest,
) (*persistence.InternalGetNamespaceResponse, error) {
	return inject1(m.ErrorGenerator.Generate(), func() (*persistence.InternalGetNamespaceResponse, error) {
		return m.baseMetadataStore.GetNamespace(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) UpdateNamespace(
	ctx context.Context,
	request *persistence.InternalUpdateNamespaceRequest,
) error {
	return inject0(m.ErrorGenerator.Generate(), func() error {
		return m.baseMetadataStore.UpdateNamespace(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) RenameNamespace(
	ctx context.Context,
	request *persistence.InternalRenameNamespaceRequest,
) error {
	return inject0(m.ErrorGenerator.Generate(), func() error {
		return m.baseMetadataStore.RenameNamespace(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) DeleteNamespace(
	ctx context.Context,
	request *persistence.DeleteNamespaceRequest,
) error {
	return inject0(m.ErrorGenerator.Generate(), func() error {
		return m.baseMetadataStore.DeleteNamespace(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) DeleteNamespaceByName(
	ctx context.Context,
	request *persistence.DeleteNamespaceByNameRequest,
) error {
	return inject0(m.ErrorGenerator.Generate(), func() error {
		return m.baseMetadataStore.DeleteNamespaceByName(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) ListNamespaces(
	ctx context.Context,
	request *persistence.InternalListNamespacesRequest,
) (*persistence.InternalListNamespacesResponse, error) {
	return inject1(m.ErrorGenerator.Generate(), func() (*persistence.InternalListNamespacesResponse, error) {
		return m.baseMetadataStore.ListNamespaces(ctx, request)
	})
}

func (m *FaultInjectionMetadataStore) GetMetadata(
	ctx context.Context,
) (*persistence.GetMetadataResponse, error) {
	return inject1(m.ErrorGenerator.Generate(), func() (*persistence.GetMetadataResponse, error) {
		return m.baseMetadataStore.GetMetadata(ctx)
	})
}

func (m *FaultInjectionMetadataStore) UpdateRate(rate float64) {
	m.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionTaskStore(
	rate float64,
	baseTaskStore persistence.TaskStore,
) (*FaultInjectionTaskStore, error) {
	errorGenerator := newErrorGenerator(rate, defaultErrors)

	return &FaultInjectionTaskStore{
		baseTaskStore:  baseTaskStore,
		ErrorGenerator: errorGenerator,
	}, nil
}

func (t *FaultInjectionTaskStore) Close() {
	t.baseTaskStore.Close()
}

func (t *FaultInjectionTaskStore) GetName() string {
	return t.baseTaskStore.GetName()
}

func (t *FaultInjectionTaskStore) CreateTaskQueue(
	ctx context.Context,
	request *persistence.InternalCreateTaskQueueRequest,
) error {
	return inject0(t.ErrorGenerator.Generate(), func() error {
		return t.baseTaskStore.CreateTaskQueue(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) GetTaskQueue(
	ctx context.Context,
	request *persistence.InternalGetTaskQueueRequest,
) (*persistence.InternalGetTaskQueueResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.InternalGetTaskQueueResponse, error) {
		return t.baseTaskStore.GetTaskQueue(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) UpdateTaskQueue(
	ctx context.Context,
	request *persistence.InternalUpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.UpdateTaskQueueResponse, error) {
		return t.baseTaskStore.UpdateTaskQueue(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) ListTaskQueue(
	ctx context.Context,
	request *persistence.ListTaskQueueRequest,
) (*persistence.InternalListTaskQueueResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.InternalListTaskQueueResponse, error) {
		return t.baseTaskStore.ListTaskQueue(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) DeleteTaskQueue(
	ctx context.Context,
	request *persistence.DeleteTaskQueueRequest,
) error {
	return inject0(t.ErrorGenerator.Generate(), func() error {
		return t.baseTaskStore.DeleteTaskQueue(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) CreateTasks(
	ctx context.Context,
	request *persistence.InternalCreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.CreateTasksResponse, error) {
		return t.baseTaskStore.CreateTasks(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.InternalGetTasksResponse, error) {
		return t.baseTaskStore.GetTasks(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (int, error) {
		return t.baseTaskStore.CompleteTasksLessThan(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) GetTaskQueueUserData(ctx context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.InternalGetTaskQueueUserDataResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.InternalGetTaskQueueUserDataResponse, error) {
		return t.baseTaskStore.GetTaskQueueUserData(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) UpdateTaskQueueUserData(ctx context.Context, request *persistence.InternalUpdateTaskQueueUserDataRequest) error {
	return inject0(t.ErrorGenerator.Generate(), func() error {
		return t.baseTaskStore.UpdateTaskQueueUserData(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
		return t.baseTaskStore.ListTaskQueueUserDataEntries(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) GetTaskQueuesByBuildId(ctx context.Context, request *persistence.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	return inject1(t.ErrorGenerator.Generate(), func() ([]string, error) {
		return t.baseTaskStore.GetTaskQueuesByBuildId(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) CountTaskQueuesByBuildId(ctx context.Context, request *persistence.CountTaskQueuesByBuildIdRequest) (int, error) {
	return inject1(t.ErrorGenerator.Generate(), func() (int, error) {
		return t.baseTaskStore.CountTaskQueuesByBuildId(ctx, request)
	})
}

func (t *FaultInjectionTaskStore) UpdateRate(rate float64) {
	t.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionShardStore(
	rate float64,
	baseShardStore persistence.ShardStore,
) (*FaultInjectionShardStore, error) {
	errorWeights := append(
		defaultErrors,
		FaultWeight{
			errFactory: func(msg string) *fault {
				return newFault(&persistence.ShardOwnershipLostError{
					ShardID: -1,
					Msg:     fmt.Sprintf("FaultInjectionShardStore injected, %s", msg),
				})
			},
			weight: 1,
		},
	)
	errorGenerator := newErrorGenerator(rate, errorWeights)
	return &FaultInjectionShardStore{
		baseShardStore: baseShardStore,
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

func (s *FaultInjectionShardStore) GetOrCreateShard(
	ctx context.Context,
	request *persistence.InternalGetOrCreateShardRequest,
) (*persistence.InternalGetOrCreateShardResponse, error) {
	return inject1(s.ErrorGenerator.Generate(), func() (*persistence.InternalGetOrCreateShardResponse, error) {
		return s.baseShardStore.GetOrCreateShard(ctx, request)
	})
}

func (s *FaultInjectionShardStore) UpdateShard(
	ctx context.Context,
	request *persistence.InternalUpdateShardRequest,
) error {
	return inject0(s.ErrorGenerator.Generate(), func() error {
		return s.baseShardStore.UpdateShard(ctx, request)
	})
}

func (s *FaultInjectionShardStore) AssertShardOwnership(
	ctx context.Context,
	request *persistence.AssertShardOwnershipRequest,
) error {
	return inject0(s.ErrorGenerator.Generate(), func() error {
		return s.baseShardStore.AssertShardOwnership(ctx, request)
	})
}

func (s *FaultInjectionShardStore) UpdateRate(rate float64) {
	s.ErrorGenerator.UpdateRate(rate)
}

func NewFaultInjectionNexusEndpointStore(
	rate float64,
	baseNexusEndpointStore persistence.NexusEndpointStore,
) (*FaultInjectionNexusEndpointStore, error) {
	errorGenerator := newErrorGenerator(rate, defaultErrors)
	return &FaultInjectionNexusEndpointStore{
		baseNexusEndpointStore: baseNexusEndpointStore,
		ErrorGenerator:         errorGenerator,
	}, nil
}

func (n *FaultInjectionNexusEndpointStore) GetName() string {
	return n.baseNexusEndpointStore.GetName()
}

func (n *FaultInjectionNexusEndpointStore) Close() {
	n.baseNexusEndpointStore.Close()
}

func (n *FaultInjectionNexusEndpointStore) GetNexusEndpoint(
	ctx context.Context,
	request *persistence.GetNexusEndpointRequest,
) (*persistence.InternalNexusEndpoint, error) {
	return inject1(n.ErrorGenerator.Generate(), func() (*persistence.InternalNexusEndpoint, error) {
		return n.baseNexusEndpointStore.GetNexusEndpoint(ctx, request)
	})
}

func (n *FaultInjectionNexusEndpointStore) ListNexusEndpoints(
	ctx context.Context,
	request *persistence.ListNexusEndpointsRequest,
) (*persistence.InternalListNexusEndpointsResponse, error) {
	return inject1(n.ErrorGenerator.Generate(), func() (*persistence.InternalListNexusEndpointsResponse, error) {
		return n.baseNexusEndpointStore.ListNexusEndpoints(ctx, request)
	})
}

func (n *FaultInjectionNexusEndpointStore) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *persistence.InternalCreateOrUpdateNexusEndpointRequest,
) error {
	return inject0(n.ErrorGenerator.Generate(), func() error {
		return n.baseNexusEndpointStore.CreateOrUpdateNexusEndpoint(ctx, request)
	})
}

func (n *FaultInjectionNexusEndpointStore) DeleteNexusEndpoint(
	ctx context.Context,
	request *persistence.DeleteNexusEndpointRequest,
) error {
	return inject0(n.ErrorGenerator.Generate(), func() error {
		return n.baseNexusEndpointStore.DeleteNexusEndpoint(ctx, request)
	})
}

func (n *FaultInjectionNexusEndpointStore) UpdateRate(rate float64) {
	n.ErrorGenerator.UpdateRate(rate)
}
