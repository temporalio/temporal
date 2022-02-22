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

//go:generate mockgen -copyright_file ../../LICENSE -package mock -source $GOFILE -destination mock/store_mock.go -aux_files go.temporal.io/server/common/persistence=dataInterfaces.go

package persistence

import (
	"context"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/tasks"
)

const (
	EmptyQueueMessageID = int64(-1)
	MinQueueMessageID   = EmptyQueueMessageID + 1
	MaxQueueMessageID   = math.MaxInt64
)

type (
	// ////////////////////////////////////////////////////////////////////
	// Persistence interface is a lower layer of dataInterface.
	// The intention is to let different persistence implementation(SQL,Cassandra/etc) share some common logic
	// Right now the only common part is serialization/deserialization.
	// ////////////////////////////////////////////////////////////////////

	// ShardStore is a lower level of ShardManager
	ShardStore interface {
		Closeable
		GetName() string
		GetClusterName() string
		GetOrCreateShard(request *InternalGetOrCreateShardRequest) (*InternalGetOrCreateShardResponse, error)
		UpdateShard(request *InternalUpdateShardRequest) error
	}

	// TaskStore is a lower level of TaskManager
	TaskStore interface {
		Closeable
		GetName() string
		CreateTaskQueue(request *InternalCreateTaskQueueRequest) error
		GetTaskQueue(request *InternalGetTaskQueueRequest) (*InternalGetTaskQueueResponse, error)
		UpdateTaskQueue(request *InternalUpdateTaskQueueRequest) (*UpdateTaskQueueResponse, error)
		ListTaskQueue(request *ListTaskQueueRequest) (*InternalListTaskQueueResponse, error)
		DeleteTaskQueue(request *DeleteTaskQueueRequest) error
		CreateTasks(request *InternalCreateTasksRequest) (*CreateTasksResponse, error)
		GetTasks(request *GetTasksRequest) (*InternalGetTasksResponse, error)
		CompleteTask(request *CompleteTaskRequest) error
		CompleteTasksLessThan(request *CompleteTasksLessThanRequest) (int, error)
	}
	// MetadataStore is a lower level of MetadataManager
	MetadataStore interface {
		Closeable
		GetName() string
		CreateNamespace(request *InternalCreateNamespaceRequest) (*CreateNamespaceResponse, error)
		GetNamespace(request *GetNamespaceRequest) (*InternalGetNamespaceResponse, error)
		UpdateNamespace(request *InternalUpdateNamespaceRequest) error
		DeleteNamespace(request *DeleteNamespaceRequest) error
		DeleteNamespaceByName(request *DeleteNamespaceByNameRequest) error
		ListNamespaces(request *ListNamespacesRequest) (*InternalListNamespacesResponse, error)
		GetMetadata() (*GetMetadataResponse, error)
	}

	// ClusterMetadataStore is a lower level of ClusterMetadataManager.
	// There is no Internal constructs needed to abstract away at the interface level currently,
	//  so we can reimplement the ClusterMetadataManager and leave this as a placeholder.
	ClusterMetadataStore interface {
		Closeable
		GetName() string
		ListClusterMetadata(request *InternalListClusterMetadataRequest) (*InternalListClusterMetadataResponse, error)
		GetClusterMetadata(request *InternalGetClusterMetadataRequest) (*InternalGetClusterMetadataResponse, error)
		SaveClusterMetadata(request *InternalSaveClusterMetadataRequest) (bool, error)
		DeleteClusterMetadata(request *InternalDeleteClusterMetadataRequest) error
		// Membership APIs
		GetClusterMembers(request *GetClusterMembersRequest) (*GetClusterMembersResponse, error)
		UpsertClusterMembership(request *UpsertClusterMembershipRequest) error
		PruneClusterMembership(request *PruneClusterMembershipRequest) error
	}

	// ExecutionStore is used to manage workflow execution including mutable states / history / tasks.
	ExecutionStore interface {
		Closeable
		GetName() string
		// The below three APIs are related to serialization/deserialization
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *InternalUpdateWorkflowExecutionRequest) error
		ConflictResolveWorkflowExecution(request *InternalConflictResolveWorkflowExecutionRequest) error

		CreateWorkflowExecution(request *InternalCreateWorkflowExecutionRequest) (*InternalCreateWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error
		GetCurrentExecution(request *GetCurrentExecutionRequest) (*InternalGetCurrentExecutionResponse, error)

		// Scan related methods
		ListConcreteExecutions(request *ListConcreteExecutionsRequest) (*InternalListConcreteExecutionsResponse, error)

		// Tasks related APIs
		AddTasks(request *InternalAddTasksRequest) error

		// transfer tasks
		GetTransferTask(request *GetTransferTaskRequest) (*InternalGetTransferTaskResponse, error)
		GetTransferTasks(request *GetTransferTasksRequest) (*InternalGetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error
		RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error

		// timer tasks
		GetTimerTask(request *GetTimerTaskRequest) (*InternalGetTimerTaskResponse, error)
		GetTimerTasks(request *GetTimerTasksRequest) (*InternalGetTimerTasksResponse, error)
		CompleteTimerTask(request *CompleteTimerTaskRequest) error
		RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error

		// replication tasks
		GetReplicationTask(request *GetReplicationTaskRequest) (*InternalGetReplicationTaskResponse, error)
		GetReplicationTasks(request *GetReplicationTasksRequest) (*InternalGetReplicationTasksResponse, error)
		CompleteReplicationTask(request *CompleteReplicationTaskRequest) error
		RangeCompleteReplicationTask(request *RangeCompleteReplicationTaskRequest) error
		PutReplicationTaskToDLQ(request *PutReplicationTaskToDLQRequest) error
		GetReplicationTasksFromDLQ(request *GetReplicationTasksFromDLQRequest) (*InternalGetReplicationTasksFromDLQResponse, error)
		DeleteReplicationTaskFromDLQ(request *DeleteReplicationTaskFromDLQRequest) error
		RangeDeleteReplicationTaskFromDLQ(request *RangeDeleteReplicationTaskFromDLQRequest) error

		// visibility tasks
		GetVisibilityTask(request *GetVisibilityTaskRequest) (*InternalGetVisibilityTaskResponse, error)
		GetVisibilityTasks(request *GetVisibilityTasksRequest) (*InternalGetVisibilityTasksResponse, error)
		CompleteVisibilityTask(request *CompleteVisibilityTaskRequest) error
		RangeCompleteVisibilityTask(request *RangeCompleteVisibilityTaskRequest) error

		// The below are history V2 APIs
		// V2 regards history events growing as a tree, decoupled from workflow concepts

		// AppendHistoryNodes add a node to history node table
		AppendHistoryNodes(request *InternalAppendHistoryNodesRequest) error
		// DeleteHistoryNodes delete a node from history node table
		DeleteHistoryNodes(request *InternalDeleteHistoryNodesRequest) error
		// ReadHistoryBranch returns history node data for a branch
		ReadHistoryBranch(request *InternalReadHistoryBranchRequest) (*InternalReadHistoryBranchResponse, error)
		// ForkHistoryBranch forks a new branch from a old branch
		ForkHistoryBranch(request *InternalForkHistoryBranchRequest) error
		// DeleteHistoryBranch removes a branch
		DeleteHistoryBranch(request *InternalDeleteHistoryBranchRequest) error
		// GetHistoryTree returns all branch information of a tree
		GetHistoryTree(request *GetHistoryTreeRequest) (*InternalGetHistoryTreeResponse, error)
		// GetAllHistoryTreeBranches returns all branches of all trees
		GetAllHistoryTreeBranches(request *GetAllHistoryTreeBranchesRequest) (*InternalGetAllHistoryTreeBranchesResponse, error)
	}

	// Queue is a store to enqueue and get messages
	Queue interface {
		Closeable
		Init(blob *commonpb.DataBlob) error
		EnqueueMessage(blob commonpb.DataBlob) error
		ReadMessages(lastMessageID int64, maxCount int) ([]*QueueMessage, error)
		DeleteMessagesBefore(messageID int64) error
		UpdateAckLevel(metadata *InternalQueueMetadata) error
		GetAckLevels() (*InternalQueueMetadata, error)

		EnqueueMessageToDLQ(blob commonpb.DataBlob) (int64, error)
		ReadMessagesFromDLQ(firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) ([]*QueueMessage, []byte, error)
		DeleteMessageFromDLQ(messageID int64) error
		RangeDeleteMessagesFromDLQ(firstMessageID int64, lastMessageID int64) error
		UpdateDLQAckLevel(metadata *InternalQueueMetadata) error
		GetDLQAckLevels() (*InternalQueueMetadata, error)
	}

	// QueueMessage is the message that stores in the queue
	QueueMessage struct {
		QueueType QueueType `json:"queue_type"`
		ID        int64     `json:"message_id"`
		Data      []byte    `json:"message_payload"`
		Encoding  string    `json:"message_encoding"`
	}

	InternalQueueMetadata struct {
		Blob    *commonpb.DataBlob
		Version int64
	}

	// InternalGetOrCreateShardRequest is used by ShardStore to retrieve or create a shard.
	// GetOrCreateShard should: if shard exists, return it. If not, call CreateShardInfo and
	// create the shard with the returned value.
	InternalGetOrCreateShardRequest struct {
		ShardID          int32
		CreateShardInfo  func() (rangeID int64, shardInfo *commonpb.DataBlob, err error)
		LifecycleContext context.Context // cancelled when shard is unloaded
	}

	// InternalGetOrCreateShardResponse is the response to GetShard
	InternalGetOrCreateShardResponse struct {
		ShardInfo *commonpb.DataBlob
	}

	// InternalUpdateShardRequest is used by ShardStore to update a shard
	InternalUpdateShardRequest struct {
		ShardID         int32
		RangeID         int64
		ShardInfo       *commonpb.DataBlob
		PreviousRangeID int64
	}

	InternalCreateTaskQueueRequest struct {
		NamespaceID   string
		TaskQueue     string
		TaskType      enumspb.TaskQueueType
		RangeID       int64
		TaskQueueInfo *commonpb.DataBlob

		TaskQueueKind enumspb.TaskQueueKind
		ExpiryTime    *time.Time
	}

	InternalGetTaskQueueRequest struct {
		NamespaceID string
		TaskQueue   string
		TaskType    enumspb.TaskQueueType
	}

	InternalGetTaskQueueResponse struct {
		RangeID       int64
		TaskQueueInfo *commonpb.DataBlob
	}

	InternalUpdateTaskQueueRequest struct {
		NamespaceID   string
		TaskQueue     string
		TaskType      enumspb.TaskQueueType
		RangeID       int64
		TaskQueueInfo *commonpb.DataBlob

		TaskQueueKind enumspb.TaskQueueKind
		ExpiryTime    *time.Time

		PrevRangeID int64
	}

	InternalCreateTasksRequest struct {
		NamespaceID   string
		TaskQueue     string
		TaskType      enumspb.TaskQueueType
		RangeID       int64
		TaskQueueInfo *commonpb.DataBlob
		Tasks         []*InternalCreateTask
	}

	InternalCreateTask struct {
		TaskId     int64
		ExpiryTime *time.Time
		Task       *commonpb.DataBlob
	}

	InternalGetTasksResponse struct {
		Tasks         []*commonpb.DataBlob
		NextPageToken []byte
	}

	InternalListTaskQueueResponse struct {
		Items         []*InternalListTaskQueueItem
		NextPageToken []byte
	}

	InternalListTaskQueueItem struct {
		TaskQueue *commonpb.DataBlob // serialized PersistedTaskQueueInfo
		RangeID   int64
	}

	// DataBlob represents a blob for any binary data.
	// It contains raw data, and metadata(right now only encoding) in other field
	// Note that it should be only used for Persistence layer, below dataInterface and application(historyEngine/etc)

	// InternalCreateWorkflowExecutionRequest is used to write a new workflow execution
	InternalCreateWorkflowExecutionRequest struct {
		ShardID int32
		RangeID int64

		Mode CreateWorkflowMode

		PreviousRunID            string
		PreviousLastWriteVersion int64

		NewWorkflowSnapshot  InternalWorkflowSnapshot
		NewWorkflowNewEvents []*InternalAppendHistoryNodesRequest
	}

	// InternalCreateWorkflowExecutionResponse is the response from persistence for create new workflow execution
	InternalCreateWorkflowExecutionResponse struct {
	}

	// InternalWorkflowMutableState indicates workflow related state for Persistence Interface
	InternalWorkflowMutableState struct {
		ActivityInfos       map[int64]*commonpb.DataBlob  // ActivityInfo
		TimerInfos          map[string]*commonpb.DataBlob // TimerInfo
		ChildExecutionInfos map[int64]*commonpb.DataBlob  // ChildExecutionInfo
		RequestCancelInfos  map[int64]*commonpb.DataBlob  // RequestCancelInfo
		SignalInfos         map[int64]*commonpb.DataBlob  // SignalInfo
		SignalRequestedIDs  []string
		ExecutionInfo       *commonpb.DataBlob // WorkflowExecutionInfo
		ExecutionState      *commonpb.DataBlob // WorkflowExecutionState
		NextEventID         int64
		BufferedEvents      []*commonpb.DataBlob
		Checksum            *commonpb.DataBlob // persistencespb.Checksum
		DBRecordVersion     int64
	}

	// InternalUpdateWorkflowExecutionRequest is used to update a workflow execution for Persistence Interface
	InternalUpdateWorkflowExecutionRequest struct {
		ShardID int32
		RangeID int64

		Mode UpdateWorkflowMode

		UpdateWorkflowMutation  InternalWorkflowMutation
		UpdateWorkflowNewEvents []*InternalAppendHistoryNodesRequest
		NewWorkflowSnapshot     *InternalWorkflowSnapshot
		NewWorkflowNewEvents    []*InternalAppendHistoryNodesRequest
	}

	// InternalConflictResolveWorkflowExecutionRequest is used to reset workflow execution state for Persistence Interface
	InternalConflictResolveWorkflowExecutionRequest struct {
		ShardID int32
		RangeID int64

		Mode ConflictResolveWorkflowMode

		// workflow to be resetted
		ResetWorkflowSnapshot        InternalWorkflowSnapshot
		ResetWorkflowEventsNewEvents []*InternalAppendHistoryNodesRequest
		// maybe new workflow
		NewWorkflowSnapshot        *InternalWorkflowSnapshot
		NewWorkflowEventsNewEvents []*InternalAppendHistoryNodesRequest

		// current workflow
		CurrentWorkflowMutation        *InternalWorkflowMutation
		CurrentWorkflowEventsNewEvents []*InternalAppendHistoryNodesRequest
	}

	// InternalAddTasksRequest is used to write new tasks
	InternalAddTasksRequest struct {
		ShardID int32
		RangeID int64

		NamespaceID string
		WorkflowID  string
		RunID       string

		TransferTasks    map[tasks.Key]commonpb.DataBlob
		TimerTasks       map[tasks.Key]commonpb.DataBlob
		ReplicationTasks map[tasks.Key]commonpb.DataBlob
		VisibilityTasks  map[tasks.Key]commonpb.DataBlob
	}

	// InternalWorkflowMutation is used as generic workflow execution state mutation for Persistence Interface
	InternalWorkflowMutation struct {
		// TODO: properly set this on call sites
		NamespaceID string
		WorkflowID  string
		RunID       string

		ExecutionInfo      *persistencespb.WorkflowExecutionInfo
		ExecutionInfoBlob  *commonpb.DataBlob
		ExecutionState     *persistencespb.WorkflowExecutionState
		ExecutionStateBlob *commonpb.DataBlob
		NextEventID        int64
		StartVersion       int64
		LastWriteVersion   int64
		DBRecordVersion    int64

		UpsertActivityInfos       map[int64]*commonpb.DataBlob
		DeleteActivityInfos       map[int64]struct{}
		UpsertTimerInfos          map[string]*commonpb.DataBlob
		DeleteTimerInfos          map[string]struct{}
		UpsertChildExecutionInfos map[int64]*commonpb.DataBlob
		DeleteChildExecutionInfos map[int64]struct{}
		UpsertRequestCancelInfos  map[int64]*commonpb.DataBlob
		DeleteRequestCancelInfos  map[int64]struct{}
		UpsertSignalInfos         map[int64]*commonpb.DataBlob
		DeleteSignalInfos         map[int64]struct{}
		UpsertSignalRequestedIDs  map[string]struct{}
		DeleteSignalRequestedIDs  map[string]struct{}
		NewBufferedEvents         *commonpb.DataBlob
		ClearBufferedEvents       bool

		TransferTasks    map[tasks.Key]commonpb.DataBlob
		TimerTasks       map[tasks.Key]commonpb.DataBlob
		ReplicationTasks map[tasks.Key]commonpb.DataBlob
		VisibilityTasks  map[tasks.Key]commonpb.DataBlob

		Condition int64

		Checksum *commonpb.DataBlob
	}

	// InternalWorkflowSnapshot is used as generic workflow execution state snapshot for Persistence Interface
	InternalWorkflowSnapshot struct {
		// TODO: properly set this on call sites
		NamespaceID string
		WorkflowID  string
		RunID       string

		ExecutionInfo      *persistencespb.WorkflowExecutionInfo
		ExecutionInfoBlob  *commonpb.DataBlob
		ExecutionState     *persistencespb.WorkflowExecutionState
		ExecutionStateBlob *commonpb.DataBlob
		StartVersion       int64
		LastWriteVersion   int64
		NextEventID        int64
		DBRecordVersion    int64

		ActivityInfos       map[int64]*commonpb.DataBlob
		TimerInfos          map[string]*commonpb.DataBlob
		ChildExecutionInfos map[int64]*commonpb.DataBlob
		RequestCancelInfos  map[int64]*commonpb.DataBlob
		SignalInfos         map[int64]*commonpb.DataBlob
		SignalRequestedIDs  map[string]struct{}

		TransferTasks    map[tasks.Key]commonpb.DataBlob
		TimerTasks       map[tasks.Key]commonpb.DataBlob
		ReplicationTasks map[tasks.Key]commonpb.DataBlob
		VisibilityTasks  map[tasks.Key]commonpb.DataBlob

		Condition int64

		Checksum *commonpb.DataBlob
	}

	InternalGetCurrentExecutionResponse struct {
		RunID          string
		ExecutionState *persistencespb.WorkflowExecutionState
	}

	// InternalHistoryNode represent a history node metadata
	InternalHistoryNode struct {
		// The first eventID becomes the nodeID to be appended
		NodeID int64
		// requested TransactionID for this write operation. For the same eventID, the node with larger TransactionID always wins
		TransactionID int64
		// TransactionID for events before these events. For events chaining
		PrevTransactionID int64
		// The events to be appended
		Events *commonpb.DataBlob
	}

	// InternalAppendHistoryNodesRequest is used to append a batch of history nodes
	InternalAppendHistoryNodesRequest struct {
		// True if it is the first append request to the branch
		IsNewBranch bool
		// The info for clean up data in background
		Info string
		// The branch to be appended
		BranchInfo *persistencespb.HistoryBranch
		// Serialized TreeInfo
		TreeInfo *commonpb.DataBlob
		// The history node
		Node InternalHistoryNode
		// Used in sharded data stores to identify which shard to use
		ShardID int32
	}

	// InternalGetWorkflowExecutionResponse is the response to GetworkflowExecution for Persistence Interface
	InternalGetWorkflowExecutionResponse struct {
		State           *InternalWorkflowMutableState
		DBRecordVersion int64
	}

	// InternalListConcreteExecutionsResponse is the response to ListConcreteExecutions for Persistence Interface
	InternalListConcreteExecutionsResponse struct {
		States        []*InternalWorkflowMutableState
		NextPageToken []byte
	}

	InternalGetTransferTaskResponse struct {
		Task commonpb.DataBlob
	}

	InternalGetTransferTasksResponse struct {
		Tasks         []commonpb.DataBlob
		NextPageToken []byte
	}

	InternalGetTimerTaskResponse struct {
		Task commonpb.DataBlob
	}

	InternalGetTimerTasksResponse struct {
		Tasks         []commonpb.DataBlob
		NextPageToken []byte
	}

	InternalGetReplicationTaskResponse struct {
		Task commonpb.DataBlob
	}

	InternalGetReplicationTasksResponse struct {
		Tasks         []commonpb.DataBlob
		NextPageToken []byte
	}

	InternalGetReplicationTasksFromDLQResponse = InternalGetReplicationTasksResponse

	InternalGetVisibilityTaskResponse struct {
		Task commonpb.DataBlob
	}

	InternalGetVisibilityTasksResponse struct {
		Tasks         []commonpb.DataBlob
		NextPageToken []byte
	}

	// InternalForkHistoryBranchRequest is used to fork a history branch
	InternalForkHistoryBranchRequest struct {
		// The base branch to fork from
		ForkBranchInfo *persistencespb.HistoryBranch
		// Serialized TreeInfo
		TreeInfo *commonpb.DataBlob
		// The nodeID to fork from, the new branch will start from ( inclusive ), the base branch will stop at(exclusive)
		ForkNodeID int64
		// branchID of the new branch
		NewBranchID string
		// the info for clean up data in background
		Info string
		// Used in sharded data stores to identify which shard to use
		ShardID int32
	}

	// InternalForkHistoryBranchResponse is the response to ForkHistoryBranchRequest
	InternalForkHistoryBranchResponse struct {
		// branchInfo to represent the new branch
		NewBranchInfo *persistencespb.HistoryBranch
	}

	// InternalDeleteHistoryNodesRequest is used to remove a history node
	InternalDeleteHistoryNodesRequest struct {
		// Used in sharded data stores to identify which shard to use
		ShardID int32
		// The branch to be appended
		BranchInfo *persistencespb.HistoryBranch
		// node ID of the history node
		NodeID int64
		// transaction ID of the history node
		TransactionID int64
	}

	// InternalDeleteHistoryBranchRequest is used to remove a history branch
	InternalDeleteHistoryBranchRequest struct {
		// Used in sharded data stores to identify which shard to use
		ShardID  int32
		TreeId   string // TreeId, BranchId is used to delete target history branch itself.
		BranchId string
		// branch ranges is used to delete range of history nodes from target branch and it ancestors.
		BranchRanges []InternalDeleteHistoryBranchRange
	}

	// InternalDeleteHistoryBranchRange is used to delete a range of history nodes of a branch
	InternalDeleteHistoryBranchRange struct {
		BranchId    string
		BeginNodeId int64 // delete nodes with ID >= BeginNodeId
	}

	// InternalReadHistoryBranchRequest is used to read a history branch
	InternalReadHistoryBranchRequest struct {
		// The tree of branch range to be read
		TreeID string
		// The branch range to be read
		BranchID string
		// Get the history nodes from MinNodeID. Inclusive.
		MinNodeID int64
		// Get the history nodes upto MaxNodeID.  Exclusive.
		MaxNodeID int64
		// passing thru for pagination
		PageSize int
		// Pagination token
		NextPageToken []byte
		// Used in sharded data stores to identify which shard to use
		ShardID int32
		// whether to only return metadata, excluding node content
		MetadataOnly bool
	}

	// InternalCompleteForkBranchRequest is used to update some tree/branch meta data for forking
	InternalCompleteForkBranchRequest struct {
		// branch to be updated
		BranchInfo persistencespb.HistoryBranch
		// whether fork is successful
		Success bool
		// Used in sharded data stores to identify which shard to use
		ShardID int32
	}

	// InternalReadHistoryBranchResponse is the response to ReadHistoryBranchRequest
	InternalReadHistoryBranchResponse struct {
		// History nodes
		Nodes []InternalHistoryNode
		// Pagination token
		NextPageToken []byte
	}

	// InternalGetAllHistoryTreeBranchesResponse is response to GetAllHistoryTreeBranches
	// Only used by persistence layer
	InternalGetAllHistoryTreeBranchesResponse struct {
		// pagination token
		NextPageToken []byte
		// all branches of all trees
		Branches []InternalHistoryBranchDetail
	}

	// InternalHistoryBranchDetail used by InternalGetAllHistoryTreeBranchesResponse
	InternalHistoryBranchDetail struct {
		TreeID   string
		BranchID string
		Encoding string
		Data     []byte // HistoryTreeInfo blob
	}

	// InternalGetHistoryTreeResponse is response to GetHistoryTree
	// Only used by persistence layer
	InternalGetHistoryTreeResponse struct {
		// TreeInfos
		TreeInfos []*commonpb.DataBlob
	}

	// InternalCreateNamespaceRequest is used to create the namespace
	InternalCreateNamespaceRequest struct {
		ID        string
		Name      string
		Namespace *commonpb.DataBlob
		IsGlobal  bool
	}

	// InternalGetNamespaceResponse is the response for GetNamespace
	InternalGetNamespaceResponse struct {
		Namespace           *commonpb.DataBlob
		IsGlobal            bool
		NotificationVersion int64
	}

	// InternalUpdateNamespaceRequest is used to update namespace
	InternalUpdateNamespaceRequest struct {
		Id                  string
		Name                string
		Namespace           *commonpb.DataBlob
		NotificationVersion int64
		IsGlobal            bool
	}

	// InternalListNamespacesResponse is the response for GetNamespace
	InternalListNamespacesResponse struct {
		Namespaces    []*InternalGetNamespaceResponse
		NextPageToken []byte
	}

	// InternalListClusterMetadataRequest is the request for ListClusterMetadata
	InternalListClusterMetadataRequest struct {
		PageSize      int
		NextPageToken []byte
	}

	// InternalListClusterMetadataResponse is the response for ListClusterMetadata
	InternalListClusterMetadataResponse struct {
		ClusterMetadata []*InternalGetClusterMetadataResponse
		NextPageToken   []byte
	}

	// InternalGetClusterMetadataRequest is the request for GetClusterMetadata
	InternalGetClusterMetadataRequest struct {
		ClusterName string
	}

	// InternalGetClusterMetadataResponse is the response for GetClusterMetadata
	InternalGetClusterMetadataResponse struct {
		// Serialized MutableCusterMetadata.
		ClusterMetadata *commonpb.DataBlob
		Version         int64
	}

	// InternalSaveClusterMetadataRequest is the request for SaveClusterMetadata
	InternalSaveClusterMetadataRequest struct {
		ClusterName string
		// Serialized MutableCusterMetadata.
		ClusterMetadata *commonpb.DataBlob
		Version         int64
	}

	// InternalDeleteClusterMetadataRequest is the request for DeleteClusterMetadata
	InternalDeleteClusterMetadataRequest struct {
		ClusterName string
	}

	// InternalUpsertClusterMembershipRequest is the request to UpsertClusterMembership
	InternalUpsertClusterMembershipRequest struct {
		ClusterMember
		RecordExpiry time.Time
	}
)
