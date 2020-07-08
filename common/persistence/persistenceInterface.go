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
	"time"

	commonpb "go.temporal.io/temporal-proto/common/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	failurepb "go.temporal.io/temporal-proto/failure/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/checksum"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
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
		// Initialize immutable metadata for the cluster. Takes no action if already initialized.
		InitializeImmutableClusterMetadata(request *InternalInitializeImmutableClusterMetadataRequest) (*InternalInitializeImmutableClusterMetadataResponse, error)
		GetImmutableClusterMetadata() (*InternalGetImmutableClusterMetadataResponse, error)
		// Membership APIs
		GetClusterMembers(request *GetClusterMembersRequest) (*GetClusterMembersResponse, error)
		UpsertClusterMembership(request *UpsertClusterMembershipRequest) error
		PruneClusterMembership(request *PruneClusterMembershipRequest) error
	}

	// ExecutionStore is used to manage workflow executions for Persistence layer
	ExecutionStore interface {
		Closeable
		GetName() string
		GetShardID() int
		// The below three APIs are related to serialization/deserialization
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *InternalUpdateWorkflowExecutionRequest) error
		ConflictResolveWorkflowExecution(request *InternalConflictResolveWorkflowExecutionRequest) error
		ResetWorkflowExecution(request *InternalResetWorkflowExecutionRequest) error

		CreateWorkflowExecution(request *InternalCreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error
		GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error)

		// Transfer task related methods
		GetTransferTask(request *GetTransferTaskRequest) (*GetTransferTaskResponse, error)
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error
		RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error

		// Replication task related methods
		GetReplicationTask(request *GetReplicationTaskRequest) (*GetReplicationTaskResponse, error)
		GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error)
		CompleteReplicationTask(request *CompleteReplicationTaskRequest) error
		RangeCompleteReplicationTask(request *RangeCompleteReplicationTaskRequest) error
		PutReplicationTaskToDLQ(request *PutReplicationTaskToDLQRequest) error
		GetReplicationTasksFromDLQ(request *GetReplicationTasksFromDLQRequest) (*GetReplicationTasksFromDLQResponse, error)
		DeleteReplicationTaskFromDLQ(request *DeleteReplicationTaskFromDLQRequest) error
		RangeDeleteReplicationTaskFromDLQ(request *RangeDeleteReplicationTaskFromDLQRequest) error

		// Timer related methods.
		GetTimerTask(request *GetTimerTaskRequest) (*GetTimerTaskResponse, error)
		GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error)
		CompleteTimerTask(request *CompleteTimerTaskRequest) error
		RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error

		// Scan related methods
		ListConcreteExecutions(request *ListConcreteExecutionsRequest) (*InternalListConcreteExecutionsResponse, error)
	}

	// HistoryStore is to manager workflow history events
	HistoryStore interface {
		Closeable
		GetName() string

		// The below are history V2 APIs
		// V2 regards history events growing as a tree, decoupled from workflow concepts

		// AppendHistoryNodes add(or override) a node to a history branch
		AppendHistoryNodes(request *InternalAppendHistoryNodesRequest) error
		// ReadHistoryBranch returns history node data for a branch
		ReadHistoryBranch(request *InternalReadHistoryBranchRequest) (*InternalReadHistoryBranchResponse, error)
		// ForkHistoryBranch forks a new branch from a old branch
		ForkHistoryBranch(request *InternalForkHistoryBranchRequest) (*InternalForkHistoryBranchResponse, error)
		// DeleteHistoryBranch removes a branch
		DeleteHistoryBranch(request *InternalDeleteHistoryBranchRequest) error
		// GetHistoryTree returns all branch information of a tree
		GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error)
		// GetAllHistoryTreeBranches returns all branches of all trees
		GetAllHistoryTreeBranches(request *GetAllHistoryTreeBranchesRequest) (*GetAllHistoryTreeBranchesResponse, error)
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
		EnqueueMessage(messagePayload []byte) error
		ReadMessages(lastMessageID int64, maxCount int) ([]*QueueMessage, error)
		DeleteMessagesBefore(messageID int64) error
		UpdateAckLevel(messageID int64, clusterName string) error
		GetAckLevels() (map[string]int64, error)
		EnqueueMessageToDLQ(messagePayload []byte) (int64, error)
		ReadMessagesFromDLQ(firstMessageID int64, lastMessageID int64, pageSize int, pageToken []byte) ([]*QueueMessage, []byte, error)
		DeleteMessageFromDLQ(messageID int64) error
		RangeDeleteMessagesFromDLQ(firstMessageID int64, lastMessageID int64) error
		UpdateDLQAckLevel(messageID int64, clusterName string) error
		GetDLQAckLevels() (map[string]int64, error)
	}

	// QueueMessage is the message that stores in the queue
	QueueMessage struct {
		ID        int64     `json:"message_id"`
		QueueType QueueType `json:"queue_type"`
		Payload   []byte    `json:"message_payload"`
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

	// InternalWorkflowExecutionInfo describes a workflow execution for Persistence Interface
	InternalWorkflowExecutionInfo struct {
		NamespaceID                        string
		WorkflowID                         string
		RunID                              string
		ParentNamespaceID                  string
		ParentWorkflowID                   string
		ParentRunID                        string
		InitiatedID                        int64
		CompletionEventBatchID             int64
		CompletionEvent                    *serialization.DataBlob
		TaskQueue                          string
		WorkflowTypeName                   string
		WorkflowRunTimeout                 int32
		WorkflowExecutionTimeout           int32
		WorkflowTaskTimeout                int32
		State                              enumsspb.WorkflowExecutionState
		Status                             enumspb.WorkflowExecutionStatus
		LastFirstEventID                   int64
		LastEventTaskID                    int64
		NextEventID                        int64
		LastProcessedEvent                 int64
		StartTimestamp                     time.Time
		LastUpdatedTimestamp               time.Time
		CreateRequestID                    string
		SignalCount                        int32
		DecisionVersion                    int64
		DecisionScheduleID                 int64
		DecisionStartedID                  int64
		DecisionRequestID                  string
		DecisionTimeout                    int32
		DecisionAttempt                    int64
		DecisionStartedTimestamp           int64
		DecisionScheduledTimestamp         int64
		DecisionOriginalScheduledTimestamp int64
		CancelRequested                    bool
		CancelRequestID                    string
		StickyTaskQueue                    string
		StickyScheduleToStartTimeout       int32
		ClientLibraryVersion               string
		ClientFeatureVersion               string
		ClientImpl                         string
		AutoResetPoints                    *serialization.DataBlob
		// for retry
		Attempt                int32
		HasRetryPolicy         bool
		InitialInterval        int32
		BackoffCoefficient     float64
		MaximumInterval        int32
		ExpirationTime         time.Time
		MaximumAttempts        int32
		NonRetryableErrorTypes []string
		BranchToken            []byte
		CronSchedule           string
		Memo                   map[string]*commonpb.Payload
		SearchAttributes       map[string]*commonpb.Payload

		// attributes which are not related to mutable state at all
		HistorySize int64
	}

	// InternalWorkflowMutableState indicates workflow related state for Persistence Interface
	InternalWorkflowMutableState struct {
		ExecutionInfo    *InternalWorkflowExecutionInfo
		ReplicationState *ReplicationState
		VersionHistories *serialization.DataBlob
		ActivityInfos    map[int64]*InternalActivityInfo

		TimerInfos          map[string]*persistenceblobs.TimerInfo
		ChildExecutionInfos map[int64]*InternalChildExecutionInfo
		RequestCancelInfos  map[int64]*persistenceblobs.RequestCancelInfo
		SignalInfos         map[int64]*persistenceblobs.SignalInfo
		SignalRequestedIDs  map[string]struct{}
		BufferedEvents      []*serialization.DataBlob

		Checksum checksum.Checksum
	}

	// InternalActivityInfo details  for Persistence Interface
	InternalActivityInfo struct {
		Version                  int64
		ScheduleID               int64
		ScheduledEventBatchID    int64
		ScheduledEvent           *serialization.DataBlob
		ScheduledTime            time.Time
		StartedID                int64
		StartedEvent             *serialization.DataBlob
		StartedTime              time.Time
		ActivityID               string
		RequestID                string
		Details                  *commonpb.Payloads
		ScheduleToStartTimeout   int32
		ScheduleToCloseTimeout   int32
		StartToCloseTimeout      int32
		HeartbeatTimeout         int32
		CancelRequested          bool
		CancelRequestID          int64
		LastHeartBeatUpdatedTime time.Time
		TimerTaskStatus          int32
		// For retry
		Attempt                int32
		NamespaceID            string
		StartedIdentity        string
		TaskQueue              string
		HasRetryPolicy         bool
		InitialInterval        int32
		BackoffCoefficient     float64
		MaximumInterval        int32
		ExpirationTime         time.Time
		MaximumAttempts        int32
		NonRetryableErrorTypes []string
		LastFailure            *failurepb.Failure
		LastWorkerIdentity     string
		// Not written to database - This is used only for deduping heartbeat timer creation
		LastHeartbeatTimeoutVisibilityInSeconds int64
	}

	// InternalChildExecutionInfo has details for pending child executions for Persistence Interface
	InternalChildExecutionInfo struct {
		Version               int64
		InitiatedID           int64
		InitiatedEventBatchID int64
		InitiatedEvent        *serialization.DataBlob
		StartedID             int64
		StartedWorkflowID     string
		StartedRunID          string
		StartedEvent          *serialization.DataBlob
		CreateRequestID       string
		Namespace             string
		WorkflowTypeName      string
		ParentClosePolicy     enumspb.ParentClosePolicy
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

		// TODO deprecate this once nDC migration is completed
		//  basically should use CurrentWorkflowMutation instead
		CurrentWorkflowCAS *CurrentWorkflowCAS
	}

	// InternalResetWorkflowExecutionRequest is used to reset workflow execution state for Persistence Interface
	InternalResetWorkflowExecutionRequest struct {
		RangeID int64

		// for base run (we need to make sure the baseRun hasn't been deleted after forking)
		BaseRunID          string
		BaseRunNextEventID int64

		// for current workflow record
		CurrentRunID          string
		CurrentRunNextEventID int64

		// for current mutable state
		CurrentWorkflowMutation *InternalWorkflowMutation

		// For new mutable state
		NewWorkflowSnapshot InternalWorkflowSnapshot
	}

	// InternalWorkflowMutation is used as generic workflow execution state mutation for Persistence Interface
	InternalWorkflowMutation struct {
		ExecutionInfo    *InternalWorkflowExecutionInfo
		ReplicationState *ReplicationState
		VersionHistories *serialization.DataBlob
		StartVersion     int64
		LastWriteVersion int64

		UpsertActivityInfos       []*InternalActivityInfo
		DeleteActivityInfos       []int64
		UpsertTimerInfos          []*persistenceblobs.TimerInfo
		DeleteTimerInfos          []string
		UpsertChildExecutionInfos []*InternalChildExecutionInfo
		DeleteChildExecutionInfo  *int64
		UpsertRequestCancelInfos  []*persistenceblobs.RequestCancelInfo
		DeleteRequestCancelInfo   *int64
		UpsertSignalInfos         []*persistenceblobs.SignalInfo
		DeleteSignalInfo          *int64
		UpsertSignalRequestedIDs  []string
		DeleteSignalRequestedID   string
		NewBufferedEvents         *serialization.DataBlob
		ClearBufferedEvents       bool

		TransferTasks    []Task
		TimerTasks       []Task
		ReplicationTasks []Task

		Condition int64

		Checksum checksum.Checksum
	}

	// InternalWorkflowSnapshot is used as generic workflow execution state snapshot for Persistence Interface
	InternalWorkflowSnapshot struct {
		ExecutionInfo    *InternalWorkflowExecutionInfo
		ReplicationState *ReplicationState
		VersionHistories *serialization.DataBlob
		StartVersion     int64
		LastWriteVersion int64

		ActivityInfos       []*InternalActivityInfo
		TimerInfos          []*persistenceblobs.TimerInfo
		ChildExecutionInfos []*InternalChildExecutionInfo
		RequestCancelInfos  []*persistenceblobs.RequestCancelInfo
		SignalInfos         []*persistenceblobs.SignalInfo
		SignalRequestedIDs  []string

		TransferTasks    []Task
		TimerTasks       []Task
		ReplicationTasks []Task

		Condition int64

		Checksum checksum.Checksum
	}

	// InternalAppendHistoryEventsRequest is used to append new events to workflow execution history  for Persistence Interface
	InternalAppendHistoryEventsRequest struct {
		NamespaceID       string
		Execution         commonpb.WorkflowExecution
		FirstEventID      int64
		EventBatchVersion int64
		RangeID           int64
		TransactionID     int64
		Events            *serialization.DataBlob
		Overwrite         bool
	}

	// InternalAppendHistoryNodesRequest is used to append a batch of history nodes
	InternalAppendHistoryNodesRequest struct {
		// True if it is the first append request to the branch
		IsNewBranch bool
		// The info for clean up data in background
		Info string
		// The branch to be appended
		BranchInfo *persistenceblobs.HistoryBranch
		// The first eventID becomes the nodeID to be appended
		NodeID int64
		// The events to be appended
		Events *serialization.DataBlob
		// Requested TransactionID for conditional update
		TransactionID int64
		// Used in sharded data stores to identify which shard to use
		ShardID int
	}

	// InternalGetWorkflowExecutionResponse is the response to GetworkflowExecution for Persistence Interface
	InternalGetWorkflowExecutionResponse struct {
		State *InternalWorkflowMutableState
	}

	// InternalListConcreteExecutionsResponse is the response to ListConcreteExecutions for Persistence Interface
	InternalListConcreteExecutionsResponse struct {
		ExecutionInfos []*InternalWorkflowExecutionInfo
		NextPageToken  []byte
	}

	// InternalForkHistoryBranchRequest is used to fork a history branch
	InternalForkHistoryBranchRequest struct {
		// The base branch to fork from
		ForkBranchInfo *persistenceblobs.HistoryBranch
		// The nodeID to fork from, the new branch will start from ( inclusive ), the base branch will stop at(exclusive)
		ForkNodeID int64
		// branchID of the new branch
		NewBranchID string
		// the info for clean up data in background
		Info string
		// Used in sharded data stores to identify which shard to use
		ShardID int
	}

	// InternalForkHistoryBranchResponse is the response to ForkHistoryBranchRequest
	InternalForkHistoryBranchResponse struct {
		// branchInfo to represent the new branch
		NewBranchInfo *persistenceblobs.HistoryBranch
	}

	// InternalDeleteHistoryBranchRequest is used to remove a history branch
	InternalDeleteHistoryBranchRequest struct {
		// branch to be deleted
		BranchInfo *persistenceblobs.HistoryBranch
		// Used in sharded data stores to identify which shard to use
		ShardID int
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
		// LastNodeID is the last known node ID attached to a history node
		LastNodeID int64
		// LastTransactionID is the last known transaction ID attached to a history node
		LastTransactionID int64
		// Used in sharded data stores to identify which shard to use
		ShardID int
	}

	// InternalCompleteForkBranchRequest is used to update some tree/branch meta data for forking
	InternalCompleteForkBranchRequest struct {
		// branch to be updated
		BranchInfo persistenceblobs.HistoryBranch
		// whether fork is successful
		Success bool
		// Used in sharded data stores to identify which shard to use
		ShardID int
	}

	// InternalReadHistoryBranchResponse is the response to ReadHistoryBranchRequest
	InternalReadHistoryBranchResponse struct {
		// History events
		History []*serialization.DataBlob
		// Pagination token
		NextPageToken []byte
		// LastNodeID is the last known node ID attached to a history node
		LastNodeID int64
		// LastTransactionID is the last known transaction ID attached to a history node
		LastTransactionID int64
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
		Memo             *serialization.DataBlob
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
	InternalRecordWorkflowExecutionStartedRequest struct {
		NamespaceID        string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		RunTimeout         int64
		TaskID             int64
		Memo               *serialization.DataBlob
		TaskQueue          string
		SearchAttributes   map[string]*commonpb.Payload
	}

	// InternalRecordWorkflowExecutionClosedRequest is request to RecordWorkflowExecutionClosed
	InternalRecordWorkflowExecutionClosedRequest struct {
		NamespaceID        string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		TaskID             int64
		Memo               *serialization.DataBlob
		TaskQueue          string
		SearchAttributes   map[string]*commonpb.Payload
		CloseTimestamp     int64
		Status             enumspb.WorkflowExecutionStatus
		HistoryLength      int64
		RetentionSeconds   int64
	}

	// InternalUpsertWorkflowExecutionRequest is request to UpsertWorkflowExecution
	InternalUpsertWorkflowExecutionRequest struct {
		NamespaceID        string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		WorkflowTimeout    int64
		TaskID             int64
		Memo               *serialization.DataBlob
		TaskQueue          string
		SearchAttributes   map[string]*commonpb.Payload
	}

	// InternalCreateNamespaceRequest is used to create the namespace
	InternalCreateNamespaceRequest struct {
		ID        string
		Name      string
		Namespace *serialization.DataBlob
		IsGlobal  bool
	}

	// InternalGetNamespaceResponse is the response for GetNamespace
	InternalGetNamespaceResponse struct {
		Namespace           *serialization.DataBlob
		IsGlobal            bool
		NotificationVersion int64
	}

	// InternalUpdateNamespaceRequest is used to update namespace
	InternalUpdateNamespaceRequest struct {
		Id                  string
		Name                string
		Namespace           *serialization.DataBlob
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
		ImmutableClusterMetadata *serialization.DataBlob
	}

	// InternalInitializeImmutableClusterMetadataResponse is a request of InitializeImmutableClusterMetadata
	InternalInitializeImmutableClusterMetadataResponse struct {
		// Serialized ImmutableCusterMetadata that is currently persisted.
		PersistedImmutableMetadata *serialization.DataBlob
		RequestApplied             bool
	}

	// InternalGetImmutableClusterMetadataResponse is the response to GetImmutableClusterMetadata
	// These values are set a single time upon cluster initialization.
	InternalGetImmutableClusterMetadataResponse struct {
		// Serialized ImmutableCusterMetadata.
		ImmutableClusterMetadata *serialization.DataBlob
	}

	// InternalUpsertClusterMembershipRequest is the request to UpsertClusterMembership
	InternalUpsertClusterMembershipRequest struct {
		ClusterMember
		RecordExpiry time.Time
	}
)

// NewDataBlob returns a new DataBlob
func NewDataBlob(data []byte, encodingType common.EncodingType) *serialization.DataBlob {
	if data == nil || len(data) == 0 {
		return nil
	}
	if encodingType != common.EncodingTypeProto3 && data[0] == 'Y' {
		panic(fmt.Sprintf("Invalid incoding: \"%v\"", encodingType))
	}
	return &serialization.DataBlob{
		Data:     data,
		Encoding: encodingType,
	}
}

// FromDataBlob decodes a datablob into a (payload, encodingType) tuple
func FromDataBlob(blob *serialization.DataBlob) ([]byte, string) {
	if blob == nil || len(blob.Data) == 0 {
		return nil, ""
	}
	return blob.Data, string(blob.Encoding)
}

// NewDataBlobFromProto convert data blob from Proto representation
func NewDataBlobFromProto(blob *commonpb.DataBlob) *serialization.DataBlob {
	switch blob.GetEncodingType() {
	case enumspb.ENCODING_TYPE_JSON:
		return &serialization.DataBlob{
			Encoding: common.EncodingTypeJSON,
			Data:     blob.Data,
		}
	case enumspb.ENCODING_TYPE_PROTO3:
		return &serialization.DataBlob{
			Encoding: common.EncodingTypeProto3,
			Data:     blob.Data,
		}
	default:
		panic(fmt.Sprintf("NewDataBlobFromProto seeing unsupported enconding type: %v", blob.GetEncodingType()))
	}
}

func InternalWorkflowExecutionInfoToProto(executionInfo *InternalWorkflowExecutionInfo, startVersion int64, currentVersion int64, replicationState *ReplicationState, versionHistories *serialization.DataBlob) (*persistenceblobs.WorkflowExecutionInfo, *persistenceblobs.WorkflowExecutionState, error) {
	state := &persistenceblobs.WorkflowExecutionState{
		CreateRequestId: executionInfo.CreateRequestID,
		State:           executionInfo.State,
		Status:          executionInfo.Status,
		RunId:           executionInfo.RunID,
	}

	info := &persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:                             executionInfo.NamespaceID,
		WorkflowId:                              executionInfo.WorkflowID,
		TaskQueue:                               executionInfo.TaskQueue,
		WorkflowTypeName:                        executionInfo.WorkflowTypeName,
		WorkflowRunTimeoutSeconds:               executionInfo.WorkflowRunTimeout,
		WorkflowExecutionTimeoutSeconds:         executionInfo.WorkflowExecutionTimeout,
		WorkflowTaskTimeoutSeconds:              executionInfo.WorkflowTaskTimeout,
		LastFirstEventId:                        executionInfo.LastFirstEventID,
		LastEventTaskId:                         executionInfo.LastEventTaskID,
		LastProcessedEvent:                      executionInfo.LastProcessedEvent,
		StartTimeNanos:                          executionInfo.StartTimestamp.UnixNano(),
		LastUpdatedTimeNanos:                    executionInfo.LastUpdatedTimestamp.UnixNano(),
		DecisionVersion:                         executionInfo.DecisionVersion,
		DecisionScheduleId:                      executionInfo.DecisionScheduleID,
		DecisionStartedId:                       executionInfo.DecisionStartedID,
		DecisionRequestId:                       executionInfo.DecisionRequestID,
		DecisionTimeout:                         executionInfo.DecisionTimeout,
		DecisionAttempt:                         executionInfo.DecisionAttempt,
		DecisionStartedTimestampNanos:           executionInfo.DecisionStartedTimestamp,
		DecisionScheduledTimestampNanos:         executionInfo.DecisionScheduledTimestamp,
		DecisionOriginalScheduledTimestampNanos: executionInfo.DecisionOriginalScheduledTimestamp,
		StickyTaskQueue:                         executionInfo.StickyTaskQueue,
		StickyScheduleToStartTimeout:            int64(executionInfo.StickyScheduleToStartTimeout),
		ClientLibraryVersion:                    executionInfo.ClientLibraryVersion,
		ClientFeatureVersion:                    executionInfo.ClientFeatureVersion,
		ClientImpl:                              executionInfo.ClientImpl,
		SignalCount:                             int64(executionInfo.SignalCount),
		HistorySize:                             executionInfo.HistorySize,
		CronSchedule:                            executionInfo.CronSchedule,
		CompletionEventBatchId:                  executionInfo.CompletionEventBatchID,
		HasRetryPolicy:                          executionInfo.HasRetryPolicy,
		RetryAttempt:                            int64(executionInfo.Attempt),
		RetryInitialIntervalSeconds:             executionInfo.InitialInterval,
		RetryBackoffCoefficient:                 executionInfo.BackoffCoefficient,
		RetryMaximumIntervalSeconds:             executionInfo.MaximumInterval,
		RetryMaximumAttempts:                    executionInfo.MaximumAttempts,
		RetryNonRetryableErrorTypes:             executionInfo.NonRetryableErrorTypes,
		EventStoreVersion:                       EventStoreVersion,
		EventBranchToken:                        executionInfo.BranchToken,
		AutoResetPoints:                         executionInfo.AutoResetPoints.Data,
		AutoResetPointsEncoding:                 executionInfo.AutoResetPoints.GetEncoding().String(),
		SearchAttributes:                        executionInfo.SearchAttributes,
		Memo:                                    executionInfo.Memo,
	}

	if !executionInfo.ExpirationTime.IsZero() {
		info.RetryExpirationTimeNanos = executionInfo.ExpirationTime.UnixNano()
	}

	completionEvent := executionInfo.CompletionEvent
	if completionEvent != nil {
		info.CompletionEvent = completionEvent.Data
		info.CompletionEventEncoding = string(completionEvent.Encoding)
	}

	info.StartVersion = startVersion
	info.CurrentVersion = currentVersion
	if replicationState == nil && versionHistories == nil {
		// both unspecified
		// this is allowed
	} else if replicationState != nil && versionHistories == nil {
		info.ReplicationData = &persistenceblobs.ReplicationData{LastReplicationInfo: replicationState.LastReplicationInfo, LastWriteEventId: replicationState.LastWriteEventID}
	} else if versionHistories != nil && replicationState == nil {
		info.VersionHistories = versionHistories.Data
		info.VersionHistoriesEncoding = string(versionHistories.GetEncoding())
	} else {
		return nil, nil, serviceerror.NewInternal(fmt.Sprintf("build workflow execution with both version histories and replication state."))
	}

	if executionInfo.ParentNamespaceID != "" {
		info.ParentNamespaceId = executionInfo.ParentNamespaceID
		info.ParentWorkflowId = executionInfo.ParentWorkflowID
		info.ParentRunId = executionInfo.ParentRunID
		info.InitiatedId = executionInfo.InitiatedID
		info.CompletionEvent = nil
	}

	if executionInfo.CancelRequested {
		info.CancelRequested = true
		info.CancelRequestId = executionInfo.CancelRequestID
	}
	return info, state, nil
}

func ProtoWorkflowExecutionToPartialInternalExecution(info *persistenceblobs.WorkflowExecutionInfo, state *persistenceblobs.WorkflowExecutionState, nextEventID int64) *InternalWorkflowExecutionInfo {
	executionInfo := &InternalWorkflowExecutionInfo{
		NamespaceID:                        info.NamespaceId,
		WorkflowID:                         info.WorkflowId,
		RunID:                              state.RunId,
		NextEventID:                        nextEventID,
		TaskQueue:                          info.GetTaskQueue(),
		WorkflowTypeName:                   info.GetWorkflowTypeName(),
		WorkflowExecutionTimeout:           info.GetWorkflowExecutionTimeoutSeconds(),
		WorkflowRunTimeout:                 info.GetWorkflowRunTimeoutSeconds(),
		WorkflowTaskTimeout:                info.GetWorkflowTaskTimeoutSeconds(),
		State:                              state.GetState(),
		Status:                             state.GetStatus(),
		LastFirstEventID:                   info.GetLastFirstEventId(),
		LastProcessedEvent:                 info.GetLastProcessedEvent(),
		StartTimestamp:                     time.Unix(0, info.GetStartTimeNanos()),
		LastUpdatedTimestamp:               time.Unix(0, info.GetLastUpdatedTimeNanos()),
		CreateRequestID:                    state.GetCreateRequestId(),
		DecisionVersion:                    info.GetDecisionVersion(),
		DecisionScheduleID:                 info.GetDecisionScheduleId(),
		DecisionStartedID:                  info.GetDecisionStartedId(),
		DecisionRequestID:                  info.GetDecisionRequestId(),
		DecisionTimeout:                    info.GetDecisionTimeout(),
		DecisionAttempt:                    info.GetDecisionAttempt(),
		DecisionStartedTimestamp:           info.GetDecisionStartedTimestampNanos(),
		DecisionScheduledTimestamp:         info.GetDecisionScheduledTimestampNanos(),
		DecisionOriginalScheduledTimestamp: info.GetDecisionOriginalScheduledTimestampNanos(),
		StickyTaskQueue:                    info.GetStickyTaskQueue(),
		StickyScheduleToStartTimeout:       int32(info.GetStickyScheduleToStartTimeout()),
		ClientLibraryVersion:               info.GetClientLibraryVersion(),
		ClientFeatureVersion:               info.GetClientFeatureVersion(),
		ClientImpl:                         info.GetClientImpl(),
		SignalCount:                        int32(info.GetSignalCount()),
		HistorySize:                        info.GetHistorySize(),
		CronSchedule:                       info.GetCronSchedule(),
		CompletionEventBatchID:             common.EmptyEventID,
		HasRetryPolicy:                     info.GetHasRetryPolicy(),
		Attempt:                            int32(info.GetRetryAttempt()),
		InitialInterval:                    info.GetRetryInitialIntervalSeconds(),
		BackoffCoefficient:                 info.GetRetryBackoffCoefficient(),
		MaximumInterval:                    info.GetRetryMaximumIntervalSeconds(),
		MaximumAttempts:                    info.GetRetryMaximumAttempts(),
		BranchToken:                        info.GetEventBranchToken(),
		NonRetryableErrorTypes:             info.GetRetryNonRetryableErrorTypes(),
		SearchAttributes:                   info.GetSearchAttributes(),
		Memo:                               info.GetMemo(),
	}

	if info.GetRetryExpirationTimeNanos() != 0 {
		executionInfo.ExpirationTime = time.Unix(0, info.GetRetryExpirationTimeNanos())
	}

	if info.ParentNamespaceId != "" {
		executionInfo.ParentNamespaceID = info.ParentNamespaceId
		executionInfo.ParentWorkflowID = info.GetParentWorkflowId()
		executionInfo.ParentRunID = info.ParentRunId
		executionInfo.InitiatedID = info.GetInitiatedId()
		if executionInfo.CompletionEvent != nil {
			executionInfo.CompletionEvent = nil
		}
	}

	if info.GetCancelRequested() {
		executionInfo.CancelRequested = true
		executionInfo.CancelRequestID = info.GetCancelRequestId()
	}

	executionInfo.CompletionEventBatchID = info.CompletionEventBatchId

	if info.CompletionEvent != nil {
		executionInfo.CompletionEvent = NewDataBlob(info.CompletionEvent,
			common.EncodingType(info.GetCompletionEventEncoding()))
	}

	if info.AutoResetPoints != nil {
		executionInfo.AutoResetPoints = NewDataBlob(info.AutoResetPoints,
			common.EncodingType(info.GetAutoResetPointsEncoding()))
	}
	return executionInfo
}

func ProtoActivityInfoToInternalActivityInfo(decoded *persistenceblobs.ActivityInfo) *InternalActivityInfo {
	info := &InternalActivityInfo{
		NamespaceID:              decoded.GetNamespaceId(),
		ScheduleID:               decoded.GetScheduleId(),
		Details:                  decoded.LastHeartbeatDetails,
		LastHeartBeatUpdatedTime: *timestamp.TimestampFromProto(decoded.LastHeartbeatUpdatedTime).ToTime(),
		Version:                  decoded.GetVersion(),
		ScheduledEventBatchID:    decoded.GetScheduledEventBatchId(),
		ScheduledEvent:           NewDataBlob(decoded.ScheduledEvent, common.EncodingType(decoded.GetScheduledEventEncoding())),
		ScheduledTime:            time.Unix(0, decoded.GetScheduledTimeNanos()),
		StartedID:                decoded.GetStartedId(),
		StartedTime:              time.Unix(0, decoded.GetStartedTimeNanos()),
		ActivityID:               decoded.GetActivityId(),
		RequestID:                decoded.GetRequestId(),
		ScheduleToStartTimeout:   decoded.GetScheduleToStartTimeoutSeconds(),
		ScheduleToCloseTimeout:   decoded.GetScheduleToCloseTimeoutSeconds(),
		StartToCloseTimeout:      decoded.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeout:         decoded.GetHeartbeatTimeoutSeconds(),
		CancelRequested:          decoded.GetCancelRequested(),
		CancelRequestID:          decoded.GetCancelRequestId(),
		TimerTaskStatus:          decoded.GetTimerTaskStatus(),
		Attempt:                  decoded.GetAttempt(),
		StartedIdentity:          decoded.GetStartedIdentity(),
		TaskQueue:                decoded.GetTaskQueue(),
		HasRetryPolicy:           decoded.GetHasRetryPolicy(),
		InitialInterval:          decoded.GetRetryInitialIntervalSeconds(),
		BackoffCoefficient:       decoded.GetRetryBackoffCoefficient(),
		MaximumInterval:          decoded.GetRetryMaximumIntervalSeconds(),
		MaximumAttempts:          decoded.GetRetryMaximumAttempts(),
		NonRetryableErrorTypes:   decoded.GetRetryNonRetryableErrorTypes(),
		LastFailure:              decoded.GetRetryLastFailure(),
		LastWorkerIdentity:       decoded.GetRetryLastWorkerIdentity(),
	}
	if decoded.GetRetryExpirationTimeNanos() != 0 {
		info.ExpirationTime = time.Unix(0, decoded.GetRetryExpirationTimeNanos())
	}
	if decoded.StartedEvent != nil {
		info.StartedEvent = NewDataBlob(decoded.StartedEvent, common.EncodingType(decoded.GetStartedEventEncoding()))
	}
	return info
}

func (v *InternalActivityInfo) ToProto() *persistenceblobs.ActivityInfo {
	scheduledEvent, scheduledEncoding := FromDataBlob(v.ScheduledEvent)
	startEvent, startEncoding := FromDataBlob(v.StartedEvent)

	info := &persistenceblobs.ActivityInfo{
		NamespaceId:                   v.NamespaceID,
		ScheduleId:                    v.ScheduleID,
		LastHeartbeatDetails:          v.Details,
		LastHeartbeatUpdatedTime:      timestamp.TimestampFromTime(&v.LastHeartBeatUpdatedTime).ToProto(),
		Version:                       v.Version,
		ScheduledEventBatchId:         v.ScheduledEventBatchID,
		ScheduledEvent:                scheduledEvent,
		ScheduledEventEncoding:        scheduledEncoding,
		ScheduledTimeNanos:            v.ScheduledTime.UnixNano(),
		StartedId:                     v.StartedID,
		StartedEvent:                  startEvent,
		StartedEventEncoding:          startEncoding,
		StartedTimeNanos:              v.StartedTime.UnixNano(),
		ActivityId:                    v.ActivityID,
		RequestId:                     v.RequestID,
		ScheduleToStartTimeoutSeconds: v.ScheduleToStartTimeout,
		ScheduleToCloseTimeoutSeconds: v.ScheduleToCloseTimeout,
		StartToCloseTimeoutSeconds:    v.StartToCloseTimeout,
		HeartbeatTimeoutSeconds:       v.HeartbeatTimeout,
		CancelRequested:               v.CancelRequested,
		CancelRequestId:               v.CancelRequestID,
		TimerTaskStatus:               v.TimerTaskStatus,
		Attempt:                       v.Attempt,
		TaskQueue:                     v.TaskQueue,
		StartedIdentity:               v.StartedIdentity,
		HasRetryPolicy:                v.HasRetryPolicy,
		RetryInitialIntervalSeconds:   v.InitialInterval,
		RetryBackoffCoefficient:       v.BackoffCoefficient,
		RetryMaximumIntervalSeconds:   v.MaximumInterval,
		RetryMaximumAttempts:          v.MaximumAttempts,
		RetryNonRetryableErrorTypes:   v.NonRetryableErrorTypes,
		RetryLastFailure:              v.LastFailure,
		RetryLastWorkerIdentity:       v.LastWorkerIdentity,
	}
	if !v.ExpirationTime.IsZero() {
		info.RetryExpirationTimeNanos = v.ExpirationTime.UnixNano()
	}
	return info
}

func (v *InternalChildExecutionInfo) ToProto() *persistenceblobs.ChildExecutionInfo {
	initiateEvent, initiateEncoding := FromDataBlob(v.InitiatedEvent)
	startEvent, startEncoding := FromDataBlob(v.StartedEvent)

	info := &persistenceblobs.ChildExecutionInfo{
		Version:                v.Version,
		InitiatedId:            v.InitiatedID,
		InitiatedEventBatchId:  v.InitiatedEventBatchID,
		InitiatedEvent:         initiateEvent,
		InitiatedEventEncoding: initiateEncoding,
		StartedEvent:           startEvent,
		StartedEventEncoding:   startEncoding,
		StartedId:              v.StartedID,
		StartedWorkflowId:      v.StartedWorkflowID,
		StartedRunId:           v.StartedRunID,
		CreateRequestId:        v.CreateRequestID,
		Namespace:              v.Namespace,
		WorkflowTypeName:       v.WorkflowTypeName,
		ParentClosePolicy:      v.ParentClosePolicy,
	}
	return info
}

func ProtoChildExecutionInfoToInternal(rowInfo *persistenceblobs.ChildExecutionInfo) *InternalChildExecutionInfo {
	return &InternalChildExecutionInfo{
		InitiatedID:           rowInfo.GetInitiatedId(),
		InitiatedEventBatchID: rowInfo.GetInitiatedEventBatchId(),
		Version:               rowInfo.GetVersion(),
		StartedID:             rowInfo.GetStartedId(),
		StartedWorkflowID:     rowInfo.GetStartedWorkflowId(),
		StartedRunID:          rowInfo.GetStartedRunId(),
		CreateRequestID:       rowInfo.GetCreateRequestId(),
		Namespace:             rowInfo.GetNamespace(),
		WorkflowTypeName:      rowInfo.GetWorkflowTypeName(),
		ParentClosePolicy:     enumspb.ParentClosePolicy(rowInfo.GetParentClosePolicy()),
		InitiatedEvent:        NewDataBlob(rowInfo.InitiatedEvent, common.EncodingType(rowInfo.InitiatedEventEncoding)),
		StartedEvent:          NewDataBlob(rowInfo.StartedEvent, common.EncodingType(rowInfo.StartedEventEncoding)),
	}
}
