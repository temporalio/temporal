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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/history/v1"
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

	// InternalWorkflowMutableState indicates workflow related state for Persistence Interface
	InternalWorkflowMutableState struct {
		ExecutionInfo    *WorkflowExecutionInfo
		VersionHistories *history.VersionHistories
		ActivityInfos    map[int64]*persistenceblobs.ActivityInfo

		TimerInfos          map[string]*persistenceblobs.TimerInfo
		ChildExecutionInfos map[int64]*persistenceblobs.ChildExecutionInfo
		RequestCancelInfos  map[int64]*persistenceblobs.RequestCancelInfo
		SignalInfos         map[int64]*persistenceblobs.SignalInfo
		SignalRequestedIDs  map[string]struct{}
		BufferedEvents      []*serialization.DataBlob

		Checksum checksum.Checksum
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
		ExecutionInfo    *WorkflowExecutionInfo
		VersionHistories *history.VersionHistories
		StartVersion     int64
		LastWriteVersion int64

		UpsertActivityInfos       []*persistenceblobs.ActivityInfo
		DeleteActivityInfos       []int64
		UpsertTimerInfos          []*persistenceblobs.TimerInfo
		DeleteTimerInfos          []string
		UpsertChildExecutionInfos []*persistenceblobs.ChildExecutionInfo
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
		ExecutionInfo    *WorkflowExecutionInfo
		VersionHistories *history.VersionHistories
		StartVersion     int64
		LastWriteVersion int64

		ActivityInfos       []*persistenceblobs.ActivityInfo
		TimerInfos          []*persistenceblobs.TimerInfo
		ChildExecutionInfos []*persistenceblobs.ChildExecutionInfo
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
		ExecutionInfos []*WorkflowExecutionInfo
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
func NewDataBlob(data []byte, encodingTypeStr string) *serialization.DataBlob {
	if len(data) == 0 {
		return nil
	}

	encodingType, ok := enumspb.EncodingType_value[encodingTypeStr]
	if !ok || enumspb.EncodingType(encodingType) != enumspb.ENCODING_TYPE_PROTO3 {
		panic(fmt.Sprintf("Invalid incoding: \"%v\"", encodingTypeStr))
	}

	return &serialization.DataBlob{
		Data:     data,
		Encoding: enumspb.EncodingType(encodingType),
	}
}

// FromDataBlob decodes a datablob into a (payload, encodingType) tuple
func FromDataBlob(blob *serialization.DataBlob) ([]byte, string) {
	if blob == nil || len(blob.Data) == 0 {
		return nil, ""
	}
	return blob.Data, blob.Encoding.String()
}

// NewDataBlobFromProto convert data blob from Proto representation
func NewDataBlobFromProto(blob *commonpb.DataBlob) *serialization.DataBlob {
	return &serialization.DataBlob{
		Encoding: blob.GetEncodingType(),
		Data:     blob.Data,
	}
}

func truncateDurationToSecondsInt64(d *time.Duration) int64 {
	return int64(d.Truncate(time.Second).Seconds())
}

func InternalWorkflowExecutionInfoToProto(executionInfo *WorkflowExecutionInfo, startVersion int64, versionHistories *history.VersionHistories) (*persistenceblobs.WorkflowExecutionInfo, *persistenceblobs.WorkflowExecutionState, error) {
	state := &persistenceblobs.WorkflowExecutionState{
		CreateRequestId: executionInfo.CreateRequestID,
		State:           executionInfo.State,
		Status:          executionInfo.Status,
		RunId:           executionInfo.RunID,
	}

	info := &persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:                       executionInfo.NamespaceID,
		WorkflowId:                        executionInfo.WorkflowID,
		FirstExecutionRunId:               executionInfo.FirstExecutionRunID,
		TaskQueue:                         executionInfo.TaskQueue,
		WorkflowTypeName:                  executionInfo.WorkflowTypeName,
		WorkflowRunTimeout:                timestamp.DurationFromSeconds(executionInfo.WorkflowRunTimeout),
		WorkflowExecutionTimeout:          timestamp.DurationFromSeconds(executionInfo.WorkflowExecutionTimeout),
		DefaultWorkflowTaskTimeout:        timestamp.DurationFromSeconds(executionInfo.DefaultWorkflowTaskTimeout),
		LastFirstEventId:                  executionInfo.LastFirstEventID,
		LastEventTaskId:                   executionInfo.LastEventTaskID,
		LastProcessedEvent:                executionInfo.LastProcessedEvent,
		StartTime:                         &executionInfo.StartTimestamp,
		LastUpdateTime:                    &executionInfo.LastUpdatedTimestamp,
		WorkflowTaskVersion:               executionInfo.WorkflowTaskVersion,
		WorkflowTaskScheduleId:            executionInfo.WorkflowTaskScheduleID,
		WorkflowTaskStartedId:             executionInfo.WorkflowTaskStartedID,
		WorkflowTaskRequestId:             executionInfo.WorkflowTaskRequestID,
		WorkflowTaskTimeout:               timestamp.DurationFromSeconds(executionInfo.WorkflowTaskTimeout),
		WorkflowTaskAttempt:               executionInfo.WorkflowTaskAttempt,
		WorkflowTaskStartedTime:           timestamp.UnixOrZeroTimePtr(executionInfo.WorkflowTaskStartedTimestamp),
		WorkflowTaskScheduledTime:         timestamp.UnixOrZeroTimePtr(executionInfo.WorkflowTaskScheduledTimestamp),
		WorkflowTaskOriginalScheduledTime: timestamp.UnixOrZeroTimePtr(executionInfo.WorkflowTaskOriginalScheduledTimestamp),
		StickyTaskQueue:                   executionInfo.StickyTaskQueue,
		StickyScheduleToStartTimeout:      timestamp.DurationFromSeconds(executionInfo.StickyScheduleToStartTimeout),
		ClientLibraryVersion:              executionInfo.ClientLibraryVersion,
		ClientFeatureVersion:              executionInfo.ClientFeatureVersion,
		ClientImpl:                        executionInfo.ClientImpl,
		SignalCount:                       executionInfo.SignalCount,
		CronSchedule:                      executionInfo.CronSchedule,
		CompletionEventBatchId:            executionInfo.CompletionEventBatchID,
		HasRetryPolicy:                    executionInfo.HasRetryPolicy,
		RetryAttempt:                      executionInfo.Attempt,
		RetryInitialInterval:              timestamp.DurationFromSeconds(executionInfo.InitialInterval),
		RetryBackoffCoefficient:           executionInfo.BackoffCoefficient,
		RetryMaximumInterval:              timestamp.DurationFromSeconds(executionInfo.MaximumInterval),
		RetryMaximumAttempts:              executionInfo.MaximumAttempts,
		RetryNonRetryableErrorTypes:       executionInfo.NonRetryableErrorTypes,
		EventStoreVersion:                 EventStoreVersion,
		EventBranchToken:                  executionInfo.BranchToken,
		AutoResetPoints:                   executionInfo.AutoResetPoints,
		SearchAttributes:                  executionInfo.SearchAttributes,
		Memo:                              executionInfo.Memo,
		CompletionEvent:                   executionInfo.CompletionEvent,
		// Dual write to move away from HistorySize in future
		HistorySize:                       executionInfo.ExecutionStats.GetHistorySize(),
		ExecutionStats: 				   executionInfo.ExecutionStats,
	}

	if !executionInfo.WorkflowExpirationTime.IsZero() {
		info.RetryExpirationTime = timestamp.TimestampFromTimePtr(&executionInfo.WorkflowExpirationTime).ToTime()
	}

	info.StartVersion = startVersion
	info.VersionHistories = versionHistories

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

func ProtoWorkflowExecutionToPartialInternalExecution(info *persistenceblobs.WorkflowExecutionInfo, state *persistenceblobs.WorkflowExecutionState, nextEventID int64) *WorkflowExecutionInfo {
	executionInfo := &WorkflowExecutionInfo{
		NamespaceID:                            info.NamespaceId,
		WorkflowID:                             info.WorkflowId,
		RunID:                                  state.RunId,
		FirstExecutionRunID:                    info.FirstExecutionRunId,
		NextEventID:                            nextEventID,
		TaskQueue:                              info.GetTaskQueue(),
		WorkflowTypeName:                       info.GetWorkflowTypeName(),
		WorkflowExecutionTimeout:               truncateDurationToSecondsInt64(info.GetWorkflowExecutionTimeout()),
		WorkflowRunTimeout:                     truncateDurationToSecondsInt64(info.GetWorkflowRunTimeout()),
		DefaultWorkflowTaskTimeout:             truncateDurationToSecondsInt64(info.GetDefaultWorkflowTaskTimeout()),
		State:                                  state.GetState(),
		Status:                                 state.GetStatus(),
		LastFirstEventID:                       info.GetLastFirstEventId(),
		LastProcessedEvent:                     info.GetLastProcessedEvent(),
		StartTimestamp:                         timestamp.TimeValue(info.GetStartTime()),
		LastUpdatedTimestamp:                   timestamp.TimeValue(info.GetLastUpdateTime()),
		CreateRequestID:                        state.GetCreateRequestId(),
		WorkflowTaskVersion:                    info.GetWorkflowTaskVersion(),
		WorkflowTaskScheduleID:                 info.GetWorkflowTaskScheduleId(),
		WorkflowTaskStartedID:                  info.GetWorkflowTaskStartedId(),
		WorkflowTaskRequestID:                  info.GetWorkflowTaskRequestId(),
		WorkflowTaskTimeout:                    truncateDurationToSecondsInt64(info.GetWorkflowTaskTimeout()),
		WorkflowTaskAttempt:                    info.GetWorkflowTaskAttempt(),
		WorkflowTaskStartedTimestamp:           timestamp.TimeValue(info.GetWorkflowTaskStartedTime()).UnixNano(),
		WorkflowTaskScheduledTimestamp:         timestamp.TimeValue(info.GetWorkflowTaskScheduledTime()).UnixNano(),
		WorkflowTaskOriginalScheduledTimestamp: timestamp.TimeValue(info.GetWorkflowTaskOriginalScheduledTime()).UnixNano(),
		StickyTaskQueue:                        info.GetStickyTaskQueue(),
		StickyScheduleToStartTimeout:           truncateDurationToSecondsInt64(info.GetStickyScheduleToStartTimeout()),
		ClientLibraryVersion:                   info.GetClientLibraryVersion(),
		ClientFeatureVersion:                   info.GetClientFeatureVersion(),
		ClientImpl:                             info.GetClientImpl(),
		SignalCount:                            info.GetSignalCount(),
		ExecutionStats:                         info.GetExecutionStats(),
		CronSchedule:                           info.GetCronSchedule(),
		CompletionEventBatchID:                 common.EmptyEventID,
		HasRetryPolicy:                         info.GetHasRetryPolicy(),
		Attempt:                                info.GetRetryAttempt(),
		InitialInterval:                        truncateDurationToSecondsInt64(info.GetRetryInitialInterval()),
		BackoffCoefficient:                     info.GetRetryBackoffCoefficient(),
		MaximumInterval:                        truncateDurationToSecondsInt64(info.GetRetryMaximumInterval()),
		MaximumAttempts:                        info.GetRetryMaximumAttempts(),
		BranchToken:                            info.GetEventBranchToken(),
		NonRetryableErrorTypes:                 info.GetRetryNonRetryableErrorTypes(),
		SearchAttributes:                       info.GetSearchAttributes(),
		Memo:                                   info.GetMemo(),
		CompletionEvent:                        info.GetCompletionEvent(),
		AutoResetPoints:                        info.GetAutoResetPoints(),
	}

	// Back compat for GetHistorySize
	if info.GetHistorySize() >= 0 && info.GetExecutionStats() == nil {
		executionInfo.ExecutionStats = &persistenceblobs.ExecutionStats{HistorySize: 0}
	}

	if info.GetExecutionStats() == nil {
		executionInfo.ExecutionStats = &persistenceblobs.ExecutionStats{}
	}

	if info.GetRetryExpirationTime() != nil {
		executionInfo.WorkflowExpirationTime = *info.GetRetryExpirationTime()
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
	return executionInfo
}
