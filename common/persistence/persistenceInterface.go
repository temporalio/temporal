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
	"fmt"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	// Right now the only common part is serialization/deserialization, and only ExecutionManager/HistoryManager need it.
	// ShardManager/TaskManager/MetadataManager are the same.
	// ////////////////////////////////////////////////////////////////////

	// ShardStore is a lower level of ShardManager
	ShardStore = ShardManager
	// TaskStore is a lower level of TaskManager
	TaskStore = TaskManager
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
		GetClusterMetadata() (*InternalGetClusterMetadataResponse, error)
		SaveClusterMetadata(request *InternalSaveClusterMetadataRequest) (bool, error)
		// Membership APIs
		GetClusterMembers(request *GetClusterMembersRequest) (*GetClusterMembersResponse, error)
		UpsertClusterMembership(request *UpsertClusterMembershipRequest) error
		PruneClusterMembership(request *PruneClusterMembershipRequest) error
	}

	// ExecutionStore is used to manage workflow executions for Persistence layer
	ExecutionStore interface {
		Closeable
		GetName() string
		GetShardID() int32
		// The below three APIs are related to serialization/deserialization
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *InternalUpdateWorkflowExecutionRequest) error
		ConflictResolveWorkflowExecution(request *InternalConflictResolveWorkflowExecutionRequest) error

		CreateWorkflowExecution(request *InternalCreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error
		GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error)

		// Scan related methods
		ListConcreteExecutions(request *ListConcreteExecutionsRequest) (*InternalListConcreteExecutionsResponse, error)

		// Tasks related APIs
		AddTasks(request *AddTasksRequest) error

		// transfer tasks
		GetTransferTask(request *GetTransferTaskRequest) (*GetTransferTaskResponse, error)
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error
		RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error

		// timer tasks
		GetTimerTask(request *GetTimerTaskRequest) (*GetTimerTaskResponse, error)
		GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error)
		CompleteTimerTask(request *CompleteTimerTaskRequest) error
		RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error

		// replication tasks
		GetReplicationTask(request *GetReplicationTaskRequest) (*GetReplicationTaskResponse, error)
		GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error)
		CompleteReplicationTask(request *CompleteReplicationTaskRequest) error
		RangeCompleteReplicationTask(request *RangeCompleteReplicationTaskRequest) error
		PutReplicationTaskToDLQ(request *PutReplicationTaskToDLQRequest) error
		GetReplicationTasksFromDLQ(request *GetReplicationTasksFromDLQRequest) (*GetReplicationTasksFromDLQResponse, error)
		DeleteReplicationTaskFromDLQ(request *DeleteReplicationTaskFromDLQRequest) error
		RangeDeleteReplicationTaskFromDLQ(request *RangeDeleteReplicationTaskFromDLQRequest) error

		// visibility tasks
		GetVisibilityTask(request *GetVisibilityTaskRequest) (*GetVisibilityTaskResponse, error)
		GetVisibilityTasks(request *GetVisibilityTasksRequest) (*GetVisibilityTasksResponse, error)
		CompleteVisibilityTask(request *CompleteVisibilityTaskRequest) error
		RangeCompleteVisibilityTask(request *RangeCompleteVisibilityTaskRequest) error
	}

	// HistoryStore is to manager workflow history events
	HistoryStore interface {
		Closeable
		GetName() string

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

	// VisibilityStore is the store interface for visibility
	VisibilityStore interface {
		Closeable
		GetName() string
		RecordWorkflowExecutionStarted(request *InternalRecordWorkflowExecutionStartedRequest) error
		RecordWorkflowExecutionClosed(request *InternalRecordWorkflowExecutionClosedRequest) error
		UpsertWorkflowExecution(request *InternalUpsertWorkflowExecutionRequest) error
		ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*InternalListWorkflowExecutionsResponse, error)
		GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*InternalGetClosedWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error
		ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*InternalListWorkflowExecutionsResponse, error)
		ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*InternalListWorkflowExecutionsResponse, error)
		CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error)
	}

	// Queue is a store to enqueue and get messages
	Queue interface {
		Closeable
		EnqueueMessage(blob commonpb.DataBlob) error
		ReadMessages(lastMessageID int64, maxCount int) ([]*QueueMessage, error)
		DeleteMessagesBefore(messageID int64) error
		UpdateAckLevel(messageID int64, clusterName string) error
		GetAckLevels() (map[string]int64, error)

		EnqueueMessageToDLQ(blob commonpb.DataBlob) (int64, error)
		ReadMessagesFromDLQ(firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) ([]*QueueMessage, []byte, error)
		DeleteMessageFromDLQ(messageID int64) error
		RangeDeleteMessagesFromDLQ(firstMessageID int64, lastMessageID int64) error
		UpdateDLQAckLevel(messageID int64, clusterName string) error
		GetDLQAckLevels() (map[string]int64, error)
	}

	// QueueMessage is the message that stores in the queue
	QueueMessage struct {
		QueueType QueueType `json:"queue_type"`
		ID        int64     `json:"message_id"`
		Data      []byte    `json:"message_payload"`
		Encoding  string    `json:"message_encoding"`
	}

	// DataBlob represents a blob for any binary data.
	// It contains raw data, and metadata(right now only encoding) in other field
	// Note that it should be only used for Persistence layer, below dataInterface and application(historyEngine/etc)

	// InternalCreateWorkflowExecutionRequest is used to write a new workflow execution
	InternalCreateWorkflowExecutionRequest struct {
		RangeID int64

		Mode CreateWorkflowMode

		PreviousRunID            string
		PreviousLastWriteVersion int64

		NewWorkflowSnapshot InternalWorkflowSnapshot
	}

	// InternalWorkflowMutableState indicates workflow related state for Persistence Interface
	InternalWorkflowMutableState struct {
		ActivityInfos       map[int64]*persistencespb.ActivityInfo
		TimerInfos          map[string]*persistencespb.TimerInfo
		ChildExecutionInfos map[int64]*persistencespb.ChildExecutionInfo
		RequestCancelInfos  map[int64]*persistencespb.RequestCancelInfo
		SignalInfos         map[int64]*persistencespb.SignalInfo
		SignalRequestedIDs  []string
		ExecutionInfo       *persistencespb.WorkflowExecutionInfo
		ExecutionState      *persistencespb.WorkflowExecutionState
		NextEventID         int64

		BufferedEvents  []*commonpb.DataBlob
		Checksum        *persistencespb.Checksum
		DBRecordVersion int64
	}

	// InternalUpdateWorkflowExecutionRequest is used to update a workflow execution for Persistence Interface
	InternalUpdateWorkflowExecutionRequest struct {
		RangeID int64

		Mode UpdateWorkflowMode

		UpdateWorkflowMutation InternalWorkflowMutation

		NewWorkflowSnapshot *InternalWorkflowSnapshot
	}

	// InternalConflictResolveWorkflowExecutionRequest is used to reset workflow execution state for Persistence Interface
	InternalConflictResolveWorkflowExecutionRequest struct {
		RangeID int64

		Mode ConflictResolveWorkflowMode

		// workflow to be resetted
		ResetWorkflowSnapshot InternalWorkflowSnapshot

		// maybe new workflow
		NewWorkflowSnapshot *InternalWorkflowSnapshot

		// current workflow
		CurrentWorkflowMutation *InternalWorkflowMutation
	}

	// InternalWorkflowMutation is used as generic workflow execution state mutation for Persistence Interface
	InternalWorkflowMutation struct {
		ExecutionInfo    *persistencespb.WorkflowExecutionInfo
		ExecutionState   *persistencespb.WorkflowExecutionState
		NextEventID      int64
		LastWriteVersion int64
		DBRecordVersion  int64

		UpsertActivityInfos       map[int64]*persistencespb.ActivityInfo
		DeleteActivityInfos       map[int64]struct{}
		UpsertTimerInfos          map[string]*persistencespb.TimerInfo
		DeleteTimerInfos          map[string]struct{}
		UpsertChildExecutionInfos map[int64]*persistencespb.ChildExecutionInfo
		DeleteChildExecutionInfos map[int64]struct{}
		UpsertRequestCancelInfos  map[int64]*persistencespb.RequestCancelInfo
		DeleteRequestCancelInfos  map[int64]struct{}
		UpsertSignalInfos         map[int64]*persistencespb.SignalInfo
		DeleteSignalInfos         map[int64]struct{}
		UpsertSignalRequestedIDs  map[string]struct{}
		DeleteSignalRequestedIDs  map[string]struct{}
		NewBufferedEvents         *commonpb.DataBlob
		ClearBufferedEvents       bool

		TransferTasks    []Task
		TimerTasks       []Task
		ReplicationTasks []Task
		VisibilityTasks  []Task

		Condition int64

		Checksum *persistencespb.Checksum
	}

	// InternalWorkflowSnapshot is used as generic workflow execution state snapshot for Persistence Interface
	InternalWorkflowSnapshot struct {
		ExecutionInfo    *persistencespb.WorkflowExecutionInfo
		ExecutionState   *persistencespb.WorkflowExecutionState
		LastWriteVersion int64
		NextEventID      int64
		DBRecordVersion  int64

		ActivityInfos       map[int64]*persistencespb.ActivityInfo
		TimerInfos          map[string]*persistencespb.TimerInfo
		ChildExecutionInfos map[int64]*persistencespb.ChildExecutionInfo
		RequestCancelInfos  map[int64]*persistencespb.RequestCancelInfo
		SignalInfos         map[int64]*persistencespb.SignalInfo
		SignalRequestedIDs  map[string]struct{}

		TransferTasks    []Task
		TimerTasks       []Task
		ReplicationTasks []Task
		VisibilityTasks  []Task

		Condition int64

		Checksum *persistencespb.Checksum
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
		// branch to be deleted
		BranchInfo *persistencespb.HistoryBranch
		// Used in sharded data stores to identify which shard to use
		ShardID int32
		// Max EndNodeID of each  branch. This is used to determine the range of nodes that
		BranchesMaxEndNodeID map[string]int64
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

	// VisibilityWorkflowExecutionInfo is visibility info for internal response
	VisibilityWorkflowExecutionInfo struct {
		WorkflowID       string
		RunID            string
		TypeName         string
		StartTime        time.Time
		ExecutionTime    time.Time
		CloseTime        time.Time
		Status           enumspb.WorkflowExecutionStatus
		HistoryLength    int64
		Memo             *commonpb.DataBlob
		TaskQueue        string
		SearchAttributes map[string]interface{}
	}

	// InternalListWorkflowExecutionsResponse is response from ListWorkflowExecutions
	InternalListWorkflowExecutionsResponse struct {
		Executions []*VisibilityWorkflowExecutionInfo
		// Token to read next page if there are more workflow executions beyond page size.
		// Use this to set NextPageToken on ListWorkflowExecutionsRequest to read the next page.
		NextPageToken []byte
	}

	// InternalGetClosedWorkflowExecutionResponse is response from GetWorkflowExecution
	InternalGetClosedWorkflowExecutionResponse struct {
		Execution *VisibilityWorkflowExecutionInfo
	}

	// InternalRecordWorkflowExecutionStartedRequest request to RecordWorkflowExecutionStarted
	InternalVisibilityRequestBase struct {
		NamespaceID        string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     time.Time
		Status             enumspb.WorkflowExecutionStatus
		ExecutionTimestamp time.Time
		TaskID             int64
		ShardID            int32
		Memo               *commonpb.DataBlob
		TaskQueue          string
		SearchAttributes   *commonpb.SearchAttributes
	}

	// InternalRecordWorkflowExecutionStartedRequest request to RecordWorkflowExecutionStarted
	InternalRecordWorkflowExecutionStartedRequest struct {
		*InternalVisibilityRequestBase
	}

	// InternalRecordWorkflowExecutionClosedRequest is request to RecordWorkflowExecutionClosed
	InternalRecordWorkflowExecutionClosedRequest struct {
		*InternalVisibilityRequestBase
		CloseTimestamp time.Time
		HistoryLength  int64
		Retention      *time.Duration
	}

	// InternalUpsertWorkflowExecutionRequest is request to UpsertWorkflowExecution
	InternalUpsertWorkflowExecutionRequest struct {
		*InternalVisibilityRequestBase
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
	}

	// InternalListNamespacesResponse is the response for GetNamespace
	InternalListNamespacesResponse struct {
		Namespaces    []*InternalGetNamespaceResponse
		NextPageToken []byte
	}

	// InternalInitializeImmutableClusterMetadataRequest is a request of InitializeImmutableClusterMetadata
	// These values can only be set a single time upon cluster initialization.
	InternalInitializeImmutableClusterMetadataRequest struct {
		// Serialized ImmutableCusterMetadata to persist.
		ImmutableClusterMetadata *commonpb.DataBlob
	}

	// InternalInitializeImmutableClusterMetadataResponse is a request of InitializeImmutableClusterMetadata
	InternalInitializeImmutableClusterMetadataResponse struct {
		// Serialized ImmutableCusterMetadata that is currently persisted.
		PersistedImmutableMetadata *commonpb.DataBlob
		RequestApplied             bool
	}

	// InternalGetImmutableClusterMetadataResponse is the response to GetImmutableClusterMetadata
	// These values are set a single time upon cluster initialization.
	InternalGetImmutableClusterMetadataResponse struct {
		// Serialized ImmutableCusterMetadata.
		ImmutableClusterMetadata *commonpb.DataBlob
	}

	InternalGetClusterMetadataResponse struct {
		// Serialized MutableCusterMetadata.
		ClusterMetadata *commonpb.DataBlob
		Version         int64
	}

	InternalSaveClusterMetadataRequest struct {
		// Serialized MutableCusterMetadata.
		ClusterMetadata *commonpb.DataBlob
		Version         int64
	}

	// InternalUpsertClusterMembershipRequest is the request to UpsertClusterMembership
	InternalUpsertClusterMembershipRequest struct {
		ClusterMember
		RecordExpiry time.Time
	}
)

// NewDataBlob returns a new DataBlob
func NewDataBlob(data []byte, encodingTypeStr string) *commonpb.DataBlob {
	if len(data) == 0 {
		return nil
	}

	encodingType, ok := enumspb.EncodingType_value[encodingTypeStr]
	if !ok || enumspb.EncodingType(encodingType) != enumspb.ENCODING_TYPE_PROTO3 {
		panic(fmt.Sprintf("Invalid incoding: \"%v\"", encodingTypeStr))
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: enumspb.EncodingType(encodingType),
	}
}

// FromDataBlob decodes a datablob into a (payload, encodingType) tuple
func FromDataBlob(blob *commonpb.DataBlob) ([]byte, string) {
	if blob == nil || len(blob.Data) == 0 {
		return nil, ""
	}
	return blob.Data, blob.EncodingType.String()
}

// NewDataBlobFromProto convert data blob from Proto representation
func NewDataBlobFromProto(blob *commonpb.DataBlob) *commonpb.DataBlob {
	return &commonpb.DataBlob{
		EncodingType: blob.GetEncodingType(),
		Data:         blob.Data,
	}
}
