package client

import (
	"fmt"
	"math/rand"
	"time"

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
	}

	// FaultInjectionShardStore is a lower level of ShardManager
	FaultInjectionShardStore struct {
		baseShardStore persistence.ShardStore
		config         *config.FaultInjection
		errorGenerator ErrorGenerator
	}

	// FaultInjectionTaskStore is a lower level of TaskManager
	FaultInjectionTaskStore struct {
		baseTaskStore  persistence.TaskStore
		config         *config.FaultInjection
		errorGenerator ErrorGenerator
	}

	// FaultInjectionMetadataStore is a lower level of MetadataManager
	FaultInjectionMetadataStore struct {
		baseMetadataStore persistence.MetadataStore
		config            *config.FaultInjection
		errorGenerator    ErrorGenerator
	}

	// FaultInjectionClusterMetadataStore is a lower level of ClusterMetadataManager.
	// There is no Internal constructs needed to abstract away at the interface level currently,
	//  so we can reimplement the ClusterMetadataManager and leave this as a placeholder.
	FaultInjectionClusterMetadataStore struct {
		baseCMStore    persistence.ClusterMetadataStore
		config         *config.FaultInjection
		errorGenerator ErrorGenerator
	}

	// FaultInjectionExecutionStore is used to manage workflow execution including mutable states / history / tasks.
	FaultInjectionExecutionStore struct {
		baseExecutionStore persistence.ExecutionStore
		config             *config.FaultInjection
		errorGenerator     ErrorGenerator
	}

	// FaultInjectionQueue is a store to enqueue and get messages
	FaultInjectionQueue struct {
		baseQueue      persistence.Queue
		config         *config.FaultInjection
		errorGenerator ErrorGenerator
	}

	ErrorGenerator interface {
		Generate() error
	}

	DefaultErrorGenerator struct {
		rate float64
		r    *rand.Rand
	}

	NoopErrorGenerator struct{}
)

func NewDefaultErrorGenerator(rate float64) *DefaultErrorGenerator {
	return &DefaultErrorGenerator{
		rate: rate,
		r:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (p *DefaultErrorGenerator) Generate() error {
	if p.rate <= 0 {
		return nil
	}

	if roll := p.r.Float64(); roll < p.rate {
		msg := fmt.Sprintf("FaultInjection injected error with rate %f, roll: %f", p.rate, roll)
		errTypeRoll := p.r.Float64()
		if errTypeRoll < 0.1 {
			return serviceerror.NewInternal(msg)
		}
		return &persistence.TimeoutError{Msg: msg}
	}

	return nil
}

func NewNoopErrorGenerator() *NoopErrorGenerator {
	return &NoopErrorGenerator{}
}

func (p *NoopErrorGenerator) Generate() error {
	return nil
}

func newErrorGenerator(config *config.FaultInjection) ErrorGenerator {
	var errorGenerator ErrorGenerator
	if config.Rate > 0 {
		errorGenerator = NewDefaultErrorGenerator(config.Rate)
	} else {
		errorGenerator = NewNoopErrorGenerator()
	}
	return errorGenerator
}

func NewFaultInjectionDatastoreFactory(
	config *config.FaultInjection,
	baseFactory DataStoreFactory,
) *FaultInjectionDataStoreFactory {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionDataStoreFactory{
		baseFactory:    baseFactory,
		config:         config,
		errorGenerator: errorGenerator,
	}
}

func (d *FaultInjectionDataStoreFactory) Close() {
	d.baseFactory.Close()
}

func (d *FaultInjectionDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	baseFactory, err := d.baseFactory.NewTaskStore()
	if err != nil {
		return baseFactory, err
	}
	return NewFaultInjectionTaskStore(d.config, baseFactory)
}

func (d *FaultInjectionDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	baseFactory, err := d.baseFactory.NewShardStore()
	if err != nil {
		return baseFactory, err
	}
	return NewFaultInjectionShardStore(d.config, baseFactory)
}

func (d *FaultInjectionDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	baseStore, err := d.baseFactory.NewMetadataStore()
	if err != nil {
		return baseStore, err
	}
	return NewFaultInjectionMetadataStore(d.config, baseStore)
}

func (d *FaultInjectionDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	baseStore, err := d.baseFactory.NewExecutionStore()
	if err != nil {
		return baseStore, err
	}
	return NewFaultInjectionExecutionStore(d.config, baseStore)
}

func (d *FaultInjectionDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	baseQueue, err := d.baseFactory.NewQueue(queueType)
	if err != nil {
		return baseQueue, err
	}
	return NewFaultInjectionQueue(d.config, baseQueue)
}

func (d *FaultInjectionDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	baseStore, err := d.baseFactory.NewClusterMetadataStore()
	if err != nil {
		return baseStore, err
	}
	return NewFaultInjectionClusterMetadataStore(d.config, baseStore)
}

func NewFaultInjectionQueue(config *config.FaultInjection, baseQueue persistence.Queue) (*FaultInjectionQueue, error) {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionQueue{
		baseQueue:      baseQueue,
		config:         config,
		errorGenerator: errorGenerator,
	}, nil
}

func (q *FaultInjectionQueue) Close() {
	q.baseQueue.Close()
}

func (q *FaultInjectionQueue) Init(blob *commonpb.DataBlob) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.Init(blob)
}

func (q *FaultInjectionQueue) EnqueueMessage(blob commonpb.DataBlob) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.EnqueueMessage(blob)
}

func (q *FaultInjectionQueue) ReadMessages(lastMessageID int64, maxCount int) ([]*persistence.QueueMessage, error) {
	if err := q.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return q.baseQueue.ReadMessages(lastMessageID, maxCount)
}

func (q *FaultInjectionQueue) DeleteMessagesBefore(messageID int64) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.DeleteMessagesBefore(messageID)
}

func (q *FaultInjectionQueue) UpdateAckLevel(metadata *persistence.InternalQueueMetadata) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.UpdateAckLevel(metadata)
}

func (q *FaultInjectionQueue) GetAckLevels() (*persistence.InternalQueueMetadata, error) {
	if err := q.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return q.baseQueue.GetAckLevels()
}

func (q *FaultInjectionQueue) EnqueueMessageToDLQ(blob commonpb.DataBlob) (int64, error) {
	if err := q.errorGenerator.Generate(); err != nil {
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
	if err := q.errorGenerator.Generate(); err != nil {
		return nil, nil, err
	}
	return q.baseQueue.ReadMessagesFromDLQ(firstMessageID, lastMessageID, pageSize, pageToken)
}

func (q *FaultInjectionQueue) DeleteMessageFromDLQ(messageID int64) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.DeleteMessageFromDLQ(messageID)
}

func (q *FaultInjectionQueue) RangeDeleteMessagesFromDLQ(firstMessageID int64, lastMessageID int64) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.RangeDeleteMessagesFromDLQ(firstMessageID, lastMessageID)
}

func (q *FaultInjectionQueue) UpdateDLQAckLevel(metadata *persistence.InternalQueueMetadata) error {
	if err := q.errorGenerator.Generate(); err != nil {
		return err
	}
	return q.baseQueue.UpdateDLQAckLevel(metadata)
}

func (q *FaultInjectionQueue) GetDLQAckLevels() (*persistence.InternalQueueMetadata, error) {
	if err := q.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return q.baseQueue.GetDLQAckLevels()
}

func NewFaultInjectionExecutionStore(config *config.FaultInjection, executionStore persistence.ExecutionStore) (
	persistence.ExecutionStore,
	error,
) {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionExecutionStore{
		baseExecutionStore: executionStore,
		config:             config,
		errorGenerator:     errorGenerator,
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
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) UpdateWorkflowExecution(request *persistence.InternalUpdateWorkflowExecutionRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.UpdateWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) ConflictResolveWorkflowExecution(request *persistence.InternalConflictResolveWorkflowExecutionRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.ConflictResolveWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) CreateWorkflowExecution(request *persistence.InternalCreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.CreateWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) DeleteWorkflowExecution(request *persistence.DeleteWorkflowExecutionRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) DeleteCurrentWorkflowExecution(request *persistence.DeleteCurrentWorkflowExecutionRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteCurrentWorkflowExecution(request)
}

func (e *FaultInjectionExecutionStore) GetCurrentExecution(request *persistence.GetCurrentExecutionRequest) (
	*persistence.InternalGetCurrentExecutionResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetCurrentExecution(request)
}

func (e *FaultInjectionExecutionStore) ListConcreteExecutions(request *persistence.ListConcreteExecutionsRequest) (
	*persistence.InternalListConcreteExecutionsResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.ListConcreteExecutions(request)
}

func (e *FaultInjectionExecutionStore) AddTasks(request *persistence.AddTasksRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.AddTasks(request)
}

func (e *FaultInjectionExecutionStore) GetTransferTask(request *persistence.GetTransferTaskRequest) (
	*persistence.GetTransferTaskResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTransferTask(request)
}

func (e *FaultInjectionExecutionStore) GetTransferTasks(request *persistence.GetTransferTasksRequest) (
	*persistence.GetTransferTasksResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTransferTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteTransferTask(request *persistence.CompleteTransferTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteTransferTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteTransferTask(request *persistence.RangeCompleteTransferTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteTransferTask(request)
}

func (e *FaultInjectionExecutionStore) GetTimerTask(request *persistence.GetTimerTaskRequest) (
	*persistence.GetTimerTaskResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTimerTask(request)
}

func (e *FaultInjectionExecutionStore) GetTimerIndexTasks(request *persistence.GetTimerIndexTasksRequest) (
	*persistence.GetTimerIndexTasksResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetTimerIndexTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteTimerTask(request *persistence.CompleteTimerTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteTimerTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteTimerTask(request *persistence.RangeCompleteTimerTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteTimerTask(request)
}

func (e *FaultInjectionExecutionStore) GetReplicationTask(request *persistence.GetReplicationTaskRequest) (
	*persistence.GetReplicationTaskResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetReplicationTask(request)
}

func (e *FaultInjectionExecutionStore) GetReplicationTasks(request *persistence.GetReplicationTasksRequest) (
	*persistence.GetReplicationTasksResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetReplicationTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteReplicationTask(request *persistence.CompleteReplicationTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteReplicationTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteReplicationTask(request *persistence.RangeCompleteReplicationTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteReplicationTask(request)
}

func (e *FaultInjectionExecutionStore) PutReplicationTaskToDLQ(request *persistence.PutReplicationTaskToDLQRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.PutReplicationTaskToDLQ(request)
}

func (e *FaultInjectionExecutionStore) GetReplicationTasksFromDLQ(request *persistence.GetReplicationTasksFromDLQRequest) (
	*persistence.GetReplicationTasksFromDLQResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetReplicationTasksFromDLQ(request)
}

func (e *FaultInjectionExecutionStore) DeleteReplicationTaskFromDLQ(request *persistence.DeleteReplicationTaskFromDLQRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteReplicationTaskFromDLQ(request)
}

func (e *FaultInjectionExecutionStore) RangeDeleteReplicationTaskFromDLQ(request *persistence.RangeDeleteReplicationTaskFromDLQRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeDeleteReplicationTaskFromDLQ(request)
}

func (e *FaultInjectionExecutionStore) GetVisibilityTask(request *persistence.GetVisibilityTaskRequest) (
	*persistence.GetVisibilityTaskResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetVisibilityTask(request)
}

func (e *FaultInjectionExecutionStore) GetVisibilityTasks(request *persistence.GetVisibilityTasksRequest) (
	*persistence.GetVisibilityTasksResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetVisibilityTasks(request)
}

func (e *FaultInjectionExecutionStore) CompleteVisibilityTask(request *persistence.CompleteVisibilityTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.CompleteVisibilityTask(request)
}

func (e *FaultInjectionExecutionStore) RangeCompleteVisibilityTask(request *persistence.RangeCompleteVisibilityTaskRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.RangeCompleteVisibilityTask(request)
}

func (e *FaultInjectionExecutionStore) AppendHistoryNodes(request *persistence.InternalAppendHistoryNodesRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.AppendHistoryNodes(request)
}

func (e *FaultInjectionExecutionStore) DeleteHistoryNodes(request *persistence.InternalDeleteHistoryNodesRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteHistoryNodes(request)
}

func (e *FaultInjectionExecutionStore) ReadHistoryBranch(request *persistence.InternalReadHistoryBranchRequest) (
	*persistence.InternalReadHistoryBranchResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.ReadHistoryBranch(request)
}

func (e *FaultInjectionExecutionStore) ForkHistoryBranch(request *persistence.InternalForkHistoryBranchRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.ForkHistoryBranch(request)
}

func (e *FaultInjectionExecutionStore) DeleteHistoryBranch(request *persistence.InternalDeleteHistoryBranchRequest) error {
	if err := e.errorGenerator.Generate(); err != nil {
		return err
	}
	return e.baseExecutionStore.DeleteHistoryBranch(request)
}

func (e *FaultInjectionExecutionStore) GetHistoryTree(request *persistence.GetHistoryTreeRequest) (
	*persistence.InternalGetHistoryTreeResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetHistoryTree(request)
}

func (e *FaultInjectionExecutionStore) GetAllHistoryTreeBranches(request *persistence.GetAllHistoryTreeBranchesRequest) (
	*persistence.InternalGetAllHistoryTreeBranchesResponse,
	error,
) {
	if err := e.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return e.baseExecutionStore.GetAllHistoryTreeBranches(request)
}

func NewFaultInjectionClusterMetadataStore(config *config.FaultInjection, baseStore persistence.ClusterMetadataStore) (
	persistence.ClusterMetadataStore,
	error,
) {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionClusterMetadataStore{
		baseCMStore:    baseStore,
		config:         config,
		errorGenerator: errorGenerator,
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
	if err := c.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return c.baseCMStore.GetClusterMetadata()
}

func (c *FaultInjectionClusterMetadataStore) SaveClusterMetadata(request *persistence.InternalSaveClusterMetadataRequest) (
	bool,
	error,
) {
	if err := c.errorGenerator.Generate(); err != nil {
		return false, err
	}
	return c.baseCMStore.SaveClusterMetadata(request)
}

func (c *FaultInjectionClusterMetadataStore) GetClusterMembers(request *persistence.GetClusterMembersRequest) (
	*persistence.GetClusterMembersResponse,
	error,
) {
	if err := c.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return c.baseCMStore.GetClusterMembers(request)
}

func (c *FaultInjectionClusterMetadataStore) UpsertClusterMembership(request *persistence.UpsertClusterMembershipRequest) error {
	if err := c.errorGenerator.Generate(); err != nil {
		return err
	}
	return c.baseCMStore.UpsertClusterMembership(request)
}

func (c *FaultInjectionClusterMetadataStore) PruneClusterMembership(request *persistence.PruneClusterMembershipRequest) error {
	if err := c.errorGenerator.Generate(); err != nil {
		return err
	}
	return c.baseCMStore.PruneClusterMembership(request)
}

func NewFaultInjectionMetadataStore(
	config *config.FaultInjection,
	metadataStore persistence.MetadataStore,
) (persistence.MetadataStore, error) {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionMetadataStore{
		baseMetadataStore: metadataStore,
		config:            config,
		errorGenerator:    errorGenerator,
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
	if err := m.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.CreateNamespace(request)
}

func (m *FaultInjectionMetadataStore) GetNamespace(request *persistence.GetNamespaceRequest) (
	*persistence.InternalGetNamespaceResponse,
	error,
) {
	if err := m.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.GetNamespace(request)
}

func (m *FaultInjectionMetadataStore) UpdateNamespace(request *persistence.InternalUpdateNamespaceRequest) error {
	if err := m.errorGenerator.Generate(); err != nil {
		return err
	}
	return m.baseMetadataStore.UpdateNamespace(request)
}

func (m *FaultInjectionMetadataStore) DeleteNamespace(request *persistence.DeleteNamespaceRequest) error {
	if err := m.errorGenerator.Generate(); err != nil {
		return err
	}
	return m.baseMetadataStore.DeleteNamespace(request)
}

func (m *FaultInjectionMetadataStore) DeleteNamespaceByName(request *persistence.DeleteNamespaceByNameRequest) error {
	if err := m.errorGenerator.Generate(); err != nil {
		return err
	}
	return m.baseMetadataStore.DeleteNamespaceByName(request)
}

func (m *FaultInjectionMetadataStore) ListNamespaces(request *persistence.ListNamespacesRequest) (
	*persistence.InternalListNamespacesResponse,
	error,
) {
	if err := m.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.ListNamespaces(request)
}

func (m *FaultInjectionMetadataStore) GetMetadata() (*persistence.GetMetadataResponse, error) {
	if err := m.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return m.baseMetadataStore.GetMetadata()
}

func NewFaultInjectionTaskStore(
	config *config.FaultInjection,
	baseTaskStore persistence.TaskStore,
) (persistence.TaskStore, error) {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionTaskStore{
		baseTaskStore:  baseTaskStore,
		config:         config,
		errorGenerator: errorGenerator,
	}, nil
}

func (t *FaultInjectionTaskStore) Close() {
	t.baseTaskStore.Close()
}

func (t *FaultInjectionTaskStore) GetName() string {
	return t.baseTaskStore.GetName()
}

func (t *FaultInjectionTaskStore) CreateTaskQueue(request *persistence.InternalCreateTaskQueueRequest) error {
	if err := t.errorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.CreateTaskQueue(request)
}

func (t *FaultInjectionTaskStore) GetTaskQueue(request *persistence.InternalGetTaskQueueRequest) (
	*persistence.InternalGetTaskQueueResponse,
	error,
) {
	if err := t.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.GetTaskQueue(request)
}

func (t *FaultInjectionTaskStore) ExtendLease(request *persistence.InternalExtendLeaseRequest) error {
	if err := t.errorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.ExtendLease(request)
}

func (t *FaultInjectionTaskStore) UpdateTaskQueue(request *persistence.InternalUpdateTaskQueueRequest) (
	*persistence.UpdateTaskQueueResponse,
	error,
) {
	if err := t.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.UpdateTaskQueue(request)
}

func (t *FaultInjectionTaskStore) ListTaskQueue(request *persistence.ListTaskQueueRequest) (
	*persistence.InternalListTaskQueueResponse,
	error,
) {
	if err := t.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.ListTaskQueue(request)
}

func (t *FaultInjectionTaskStore) DeleteTaskQueue(request *persistence.DeleteTaskQueueRequest) error {
	if err := t.errorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.DeleteTaskQueue(request)
}

func (t *FaultInjectionTaskStore) CreateTasks(request *persistence.InternalCreateTasksRequest) (
	*persistence.CreateTasksResponse,
	error,
) {
	if err := t.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.CreateTasks(request)
}

func (t *FaultInjectionTaskStore) GetTasks(request *persistence.GetTasksRequest) (
	*persistence.InternalGetTasksResponse,
	error,
) {
	if err := t.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return t.baseTaskStore.GetTasks(request)
}

func (t *FaultInjectionTaskStore) CompleteTask(request *persistence.CompleteTaskRequest) error {
	if err := t.errorGenerator.Generate(); err != nil {
		return err
	}
	return t.baseTaskStore.CompleteTask(request)
}

func (t *FaultInjectionTaskStore) CompleteTasksLessThan(request *persistence.CompleteTasksLessThanRequest) (
	int,
	error,
) {
	if err := t.errorGenerator.Generate(); err != nil {
		return 0, err
	}
	return t.baseTaskStore.CompleteTasksLessThan(request)
}

func NewFaultInjectionShardStore(
	config *config.FaultInjection,
	baseShardStore persistence.ShardStore,
) (persistence.ShardStore, error) {
	errorGenerator := newErrorGenerator(config)
	return &FaultInjectionShardStore{
		baseShardStore: baseShardStore,
		config:         config,
		errorGenerator: errorGenerator,
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
	if err := s.errorGenerator.Generate(); err != nil {
		return err
	}
	return s.baseShardStore.CreateShard(request)
}

func (s *FaultInjectionShardStore) GetShard(request *persistence.InternalGetShardRequest) (
	*persistence.InternalGetShardResponse,
	error,
) {
	if err := s.errorGenerator.Generate(); err != nil {
		return nil, err
	}
	return s.baseShardStore.GetShard(request)
}

func (s *FaultInjectionShardStore) UpdateShard(request *persistence.InternalUpdateShardRequest) error {
	if err := s.errorGenerator.Generate(); err != nil {
		return err
	}
	return s.baseShardStore.UpdateShard(request)
}
