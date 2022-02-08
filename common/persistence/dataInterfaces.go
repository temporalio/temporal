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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination dataInterfaces_mock.go

package persistence

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"
)

// CreateWorkflowMode workflow creation mode
type CreateWorkflowMode int

// QueueType is an enum that represents various queue types in persistence
type QueueType int32

// Queue types used in queue table
// Use positive numbers for queue type
// Negative numbers are reserved for DLQ

const (
	NamespaceReplicationQueueType QueueType = iota + 1
)

// Create Workflow Execution Mode
const (
	// CreateWorkflowModeBrandNew fail if current record exists
	// Only applicable for CreateWorkflowExecution
	CreateWorkflowModeBrandNew CreateWorkflowMode = iota
	// CreateWorkflowModeWorkflowIDReuse update current record only if workflow is closed
	// Only applicable for CreateWorkflowExecution
	CreateWorkflowModeWorkflowIDReuse
	// CreateWorkflowModeZombie do not update current record since workflow is in zombie state
	// applicable for CreateWorkflowExecution, UpdateWorkflowExecution
	CreateWorkflowModeZombie
)

// UpdateWorkflowMode update mode
type UpdateWorkflowMode int

// Update Workflow Execution Mode
const (
	// UpdateWorkflowModeUpdateCurrent update workflow, including current record
	// NOTE: update on current record is a condition update
	UpdateWorkflowModeUpdateCurrent UpdateWorkflowMode = iota
	// UpdateWorkflowModeBypassCurrent update workflow, without current record
	// NOTE: current record CANNOT point to the workflow to be updated
	UpdateWorkflowModeBypassCurrent
)

// ConflictResolveWorkflowMode conflict resolve mode
type ConflictResolveWorkflowMode int

// Conflict Resolve Workflow Mode
const (
	// ConflictResolveWorkflowModeUpdateCurrent conflict resolve workflow, including current record
	// NOTE: update on current record is a condition update
	ConflictResolveWorkflowModeUpdateCurrent ConflictResolveWorkflowMode = iota
	// ConflictResolveWorkflowModeBypassCurrent conflict resolve workflow, without current record
	// NOTE: current record CANNOT point to the workflow to be updated
	ConflictResolveWorkflowModeBypassCurrent
)

// UnknownNumRowsAffected is returned when the number of rows that an API affected cannot be determined
const UnknownNumRowsAffected = -1

const (
	// InitialFailoverNotificationVersion is the initial failover version for a namespace
	InitialFailoverNotificationVersion int64 = 0
)

const numItemsInGarbageInfo = 3

type (
	// InvalidPersistenceRequestError represents invalid request to persistence
	InvalidPersistenceRequestError struct {
		Msg string
	}

	// CurrentWorkflowConditionFailedError represents a failed conditional update for current workflow record
	CurrentWorkflowConditionFailedError struct {
		Msg              string
		RequestID        string
		RunID            string
		State            enumsspb.WorkflowExecutionState
		Status           enumspb.WorkflowExecutionStatus
		LastWriteVersion int64
	}

	// WorkflowConditionFailedError represents a failed conditional update for workflow record
	WorkflowConditionFailedError struct {
		Msg             string
		NextEventID     int64
		DBRecordVersion int64
	}

	// ConditionFailedError represents a failed conditional update for execution record
	ConditionFailedError struct {
		Msg string
	}

	// ShardAlreadyExistError is returned when conditionally creating a shard fails
	ShardAlreadyExistError struct {
		Msg string
	}

	// ShardOwnershipLostError is returned when conditional update fails due to RangeID for the shard
	ShardOwnershipLostError struct {
		ShardID int32
		Msg     string
	}

	// TimeoutError is returned when a write operation fails due to a timeout
	TimeoutError struct {
		Msg string
	}

	// TransactionSizeLimitError is returned when the transaction size is too large
	TransactionSizeLimitError struct {
		Msg string
	}

	// ShardInfoWithFailover describes a shard
	ShardInfoWithFailover struct {
		*persistencespb.ShardInfo
		TransferFailoverLevels map[string]TransferFailoverLevel // uuid -> TransferFailoverLevel
		TimerFailoverLevels    map[string]TimerFailoverLevel    // uuid -> TimerFailoverLevel
	}

	// TransferFailoverLevel contains corresponding start / end level
	TransferFailoverLevel struct {
		StartTime    time.Time
		MinLevel     int64
		CurrentLevel int64
		MaxLevel     int64
		NamespaceIDs map[string]struct{}
	}

	// TimerFailoverLevel contains namespace IDs and corresponding start / end level
	TimerFailoverLevel struct {
		StartTime    time.Time
		MinLevel     time.Time
		CurrentLevel time.Time
		MaxLevel     time.Time
		NamespaceIDs map[string]struct{}
	}

	// TaskQueueKey is the struct used to identity TaskQueues
	TaskQueueKey struct {
		NamespaceID   string
		TaskQueueName string
		TaskQueueType enumspb.TaskQueueType
	}

	// GetOrCreateShardRequest is used to get shard information, or supply
	// initial information to create a shard in executions table
	GetOrCreateShardRequest struct {
		ShardID          int32
		InitialShardInfo *persistencespb.ShardInfo // optional, zero value will be used if missing
		LifecycleContext context.Context           // cancelled when shard is unloaded
	}

	// GetOrCreateShardResponse is the response to GetOrCreateShard
	GetOrCreateShardResponse struct {
		ShardInfo *persistencespb.ShardInfo
	}

	// UpdateShardRequest  is used to update shard information
	UpdateShardRequest struct {
		ShardInfo       *persistencespb.ShardInfo
		PreviousRangeID int64
	}

	// AddTasksRequest is used to write new tasks
	AddTasksRequest struct {
		ShardID int32
		RangeID int64

		NamespaceID string
		WorkflowID  string
		RunID       string

		TransferTasks    []tasks.Task
		TimerTasks       []tasks.Task
		ReplicationTasks []tasks.Task
		VisibilityTasks  []tasks.Task
	}

	// CreateWorkflowExecutionRequest is used to write a new workflow execution
	CreateWorkflowExecutionRequest struct {
		ShardID int32
		RangeID int64

		Mode CreateWorkflowMode

		PreviousRunID            string
		PreviousLastWriteVersion int64

		NewWorkflowSnapshot WorkflowSnapshot
		NewWorkflowEvents   []*WorkflowEvents
	}

	// CreateWorkflowExecutionResponse is the response to CreateWorkflowExecutionRequest
	CreateWorkflowExecutionResponse struct {
		NewMutableStateStats MutableStateStatistics
	}

	// GetWorkflowExecutionRequest is used to retrieve the info of a workflow execution
	GetWorkflowExecutionRequest struct {
		ShardID     int32
		NamespaceID string
		WorkflowID  string
		RunID       string
	}

	// GetWorkflowExecutionResponse is the response to GetWorkflowExecutionRequest
	GetWorkflowExecutionResponse struct {
		State             *persistencespb.WorkflowMutableState
		DBRecordVersion   int64
		MutableStateStats MutableStateStatistics
	}

	// GetCurrentExecutionRequest is used to retrieve the current RunId for an execution
	GetCurrentExecutionRequest struct {
		ShardID     int32
		NamespaceID string
		WorkflowID  string
	}

	// ListConcreteExecutionsRequest is request to ListConcreteExecutions
	ListConcreteExecutionsRequest struct {
		ShardID   int32
		PageSize  int
		PageToken []byte
	}

	// ListConcreteExecutionsResponse is response to ListConcreteExecutions
	ListConcreteExecutionsResponse struct {
		States    []*persistencespb.WorkflowMutableState
		PageToken []byte
	}

	// GetCurrentExecutionResponse is the response to GetCurrentExecution
	GetCurrentExecutionResponse struct {
		StartRequestID string
		RunID          string
		State          enumsspb.WorkflowExecutionState
		Status         enumspb.WorkflowExecutionStatus
	}

	// UpdateWorkflowExecutionRequest is used to update a workflow execution
	UpdateWorkflowExecutionRequest struct {
		ShardID int32
		RangeID int64

		Mode UpdateWorkflowMode

		UpdateWorkflowMutation WorkflowMutation
		UpdateWorkflowEvents   []*WorkflowEvents
		NewWorkflowSnapshot    *WorkflowSnapshot
		NewWorkflowEvents      []*WorkflowEvents
	}

	// UpdateWorkflowExecutionResponse is response for UpdateWorkflowExecutionRequest
	UpdateWorkflowExecutionResponse struct {
		UpdateMutableStateStats MutableStateStatistics
		NewMutableStateStats    *MutableStateStatistics
	}

	// ConflictResolveWorkflowExecutionRequest is used to reset workflow execution state for a single run
	ConflictResolveWorkflowExecutionRequest struct {
		ShardID int32
		RangeID int64

		Mode ConflictResolveWorkflowMode

		// workflow to be resetted
		ResetWorkflowSnapshot WorkflowSnapshot
		ResetWorkflowEvents   []*WorkflowEvents

		// maybe new workflow
		NewWorkflowSnapshot *WorkflowSnapshot
		NewWorkflowEvents   []*WorkflowEvents

		// current workflow
		CurrentWorkflowMutation *WorkflowMutation
		CurrentWorkflowEvents   []*WorkflowEvents
	}

	ConflictResolveWorkflowExecutionResponse struct {
		ResetMutableStateStats   MutableStateStatistics
		NewMutableStateStats     *MutableStateStatistics
		CurrentMutableStateStats *MutableStateStatistics
	}

	// WorkflowEvents is used as generic workflow history events transaction container
	WorkflowEvents struct {
		NamespaceID string
		WorkflowID  string
		RunID       string
		BranchToken []byte
		PrevTxnID   int64
		TxnID       int64
		Events      []*historypb.HistoryEvent
	}

	// WorkflowMutation is used as generic workflow execution state mutation
	WorkflowMutation struct {
		ExecutionInfo  *persistencespb.WorkflowExecutionInfo
		ExecutionState *persistencespb.WorkflowExecutionState
		// TODO deprecate NextEventID in favor of DBRecordVersion
		NextEventID int64

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
		NewBufferedEvents         []*historypb.HistoryEvent
		ClearBufferedEvents       bool

		TransferTasks    []tasks.Task
		ReplicationTasks []tasks.Task
		TimerTasks       []tasks.Task
		VisibilityTasks  []tasks.Task

		// TODO deprecate Condition in favor of DBRecordVersion
		Condition       int64
		DBRecordVersion int64
		Checksum        *persistencespb.Checksum
	}

	// WorkflowSnapshot is used as generic workflow execution state snapshot
	WorkflowSnapshot struct {
		ExecutionInfo  *persistencespb.WorkflowExecutionInfo
		ExecutionState *persistencespb.WorkflowExecutionState
		// TODO deprecate NextEventID in favor of DBRecordVersion
		NextEventID int64

		ActivityInfos       map[int64]*persistencespb.ActivityInfo
		TimerInfos          map[string]*persistencespb.TimerInfo
		ChildExecutionInfos map[int64]*persistencespb.ChildExecutionInfo
		RequestCancelInfos  map[int64]*persistencespb.RequestCancelInfo
		SignalInfos         map[int64]*persistencespb.SignalInfo
		SignalRequestedIDs  map[string]struct{}

		TransferTasks    []tasks.Task
		ReplicationTasks []tasks.Task
		TimerTasks       []tasks.Task
		VisibilityTasks  []tasks.Task

		// TODO deprecate Condition in favor of DBRecordVersion
		Condition       int64
		DBRecordVersion int64
		Checksum        *persistencespb.Checksum
	}

	// DeleteWorkflowExecutionRequest is used to delete a workflow execution
	DeleteWorkflowExecutionRequest struct {
		ShardID     int32
		NamespaceID string
		WorkflowID  string
		RunID       string
	}

	// DeleteCurrentWorkflowExecutionRequest is used to delete the current workflow execution
	DeleteCurrentWorkflowExecutionRequest struct {
		ShardID     int32
		NamespaceID string
		WorkflowID  string
		RunID       string
	}

	// GetTransferTaskRequest is the request for GetTransferTask
	GetTransferTaskRequest struct {
		ShardID int32
		TaskID  int64
	}

	// GetTransferTaskResponse is the response to GetTransferTask
	GetTransferTaskResponse struct {
		Task tasks.Task
	}

	// GetTransferTasksRequest is used to read tasks from the transfer task queue
	GetTransferTasksRequest struct {
		ShardID       int32
		ReadLevel     int64
		MaxReadLevel  int64
		BatchSize     int
		NextPageToken []byte
	}

	// GetTransferTasksResponse is the response to GetTransferTasksRequest
	GetTransferTasksResponse struct {
		Tasks         []tasks.Task
		NextPageToken []byte
	}

	// GetVisibilityTaskRequest is the request for GetVisibilityTask
	GetVisibilityTaskRequest struct {
		ShardID int32
		TaskID  int64
	}

	// GetVisibilityTaskResponse is the response to GetVisibilityTask
	GetVisibilityTaskResponse struct {
		Task tasks.Task
	}

	// GetVisibilityTasksRequest is used to read tasks from the visibility task queue
	GetVisibilityTasksRequest struct {
		ShardID       int32
		ReadLevel     int64
		MaxReadLevel  int64
		BatchSize     int
		NextPageToken []byte
	}

	// GetVisibilityTasksResponse is the response to GetVisibilityTasksRequest
	GetVisibilityTasksResponse struct {
		Tasks         []tasks.Task
		NextPageToken []byte
	}

	// GetReplicationTaskRequest is the request for GetReplicationTask
	GetReplicationTaskRequest struct {
		ShardID int32
		TaskID  int64
	}

	// GetReplicationTaskResponse is the response to GetReplicationTask
	GetReplicationTaskResponse struct {
		Task tasks.Task
	}

	// GetReplicationTasksRequest is used to read tasks from the replication task queue
	GetReplicationTasksRequest struct {
		ShardID       int32
		MinTaskID     int64
		MaxTaskID     int64
		BatchSize     int
		NextPageToken []byte
	}

	// GetReplicationTasksResponse is the response to GetReplicationTask
	GetReplicationTasksResponse struct {
		Tasks         []tasks.Task
		NextPageToken []byte
	}

	// CompleteTransferTaskRequest is used to complete a task in the transfer task queue
	CompleteTransferTaskRequest struct {
		ShardID int32
		TaskID  int64
	}

	// RangeCompleteTransferTaskRequest is used to complete a range of tasks in the transfer task queue
	RangeCompleteTransferTaskRequest struct {
		ShardID              int32
		ExclusiveBeginTaskID int64
		InclusiveEndTaskID   int64
	}

	// CompleteVisibilityTaskRequest is used to complete a task in the visibility task queue
	CompleteVisibilityTaskRequest struct {
		ShardID int32
		TaskID  int64
	}

	// RangeCompleteVisibilityTaskRequest is used to complete a range of tasks in the visibility task queue
	RangeCompleteVisibilityTaskRequest struct {
		ShardID              int32
		ExclusiveBeginTaskID int64
		InclusiveEndTaskID   int64
	}

	// CompleteReplicationTaskRequest is used to complete a task in the replication task queue
	CompleteReplicationTaskRequest struct {
		ShardID int32
		TaskID  int64
	}

	// RangeCompleteReplicationTaskRequest is used to complete a range of task in the replication task queue
	RangeCompleteReplicationTaskRequest struct {
		ShardID            int32
		InclusiveEndTaskID int64
	}

	// PutReplicationTaskToDLQRequest is used to put a replication task to dlq
	PutReplicationTaskToDLQRequest struct {
		ShardID           int32
		SourceClusterName string
		TaskInfo          *persistencespb.ReplicationTaskInfo
	}

	// GetReplicationTasksFromDLQRequest is used to get replication tasks from dlq
	GetReplicationTasksFromDLQRequest struct {
		ShardID           int32
		SourceClusterName string
		GetReplicationTasksRequest
	}

	// DeleteReplicationTaskFromDLQRequest is used to delete replication task from DLQ
	DeleteReplicationTaskFromDLQRequest struct {
		ShardID           int32
		SourceClusterName string
		TaskID            int64
	}

	// RangeDeleteReplicationTaskFromDLQRequest is used to delete replication tasks from DLQ
	RangeDeleteReplicationTaskFromDLQRequest struct {
		ShardID              int32
		SourceClusterName    string
		ExclusiveBeginTaskID int64
		InclusiveEndTaskID   int64
	}

	// GetReplicationTasksFromDLQResponse is the response for GetReplicationTasksFromDLQ
	GetReplicationTasksFromDLQResponse = GetReplicationTasksResponse

	// RangeCompleteTimerTaskRequest is used to complete a range of tasks in the timer task queue
	RangeCompleteTimerTaskRequest struct {
		ShardID                 int32
		InclusiveBeginTimestamp time.Time
		ExclusiveEndTimestamp   time.Time
	}

	// CompleteTimerTaskRequest is used to complete a task in the timer task queue
	CompleteTimerTaskRequest struct {
		ShardID             int32
		VisibilityTimestamp time.Time
		TaskID              int64
	}

	// CreateTaskQueueRequest create a new task queue
	CreateTaskQueueRequest struct {
		RangeID       int64
		TaskQueueInfo *persistencespb.TaskQueueInfo
	}

	// CreateTaskQueueResponse is the response to CreateTaskQueue
	CreateTaskQueueResponse struct {
	}

	// UpdateTaskQueueRequest is used to update task queue implementation information
	UpdateTaskQueueRequest struct {
		RangeID       int64
		TaskQueueInfo *persistencespb.TaskQueueInfo

		PrevRangeID int64
	}

	// UpdateTaskQueueResponse is the response to UpdateTaskQueue
	UpdateTaskQueueResponse struct {
	}

	// GetTaskQueueRequest get the target task queue
	GetTaskQueueRequest struct {
		NamespaceID string
		TaskQueue   string
		TaskType    enumspb.TaskQueueType
	}

	// GetTaskQueueResponse is the response to GetTaskQueue
	GetTaskQueueResponse struct {
		RangeID       int64
		TaskQueueInfo *persistencespb.TaskQueueInfo
	}

	// ListTaskQueueRequest contains the request params needed to invoke ListTaskQueue API
	ListTaskQueueRequest struct {
		PageSize  int
		PageToken []byte
	}

	// ListTaskQueueResponse is the response from ListTaskQueue API
	ListTaskQueueResponse struct {
		Items         []*PersistedTaskQueueInfo
		NextPageToken []byte
	}

	// DeleteTaskQueueRequest contains the request params needed to invoke DeleteTaskQueue API
	DeleteTaskQueueRequest struct {
		TaskQueue *TaskQueueKey
		RangeID   int64
	}

	// CreateTasksRequest is used to create a new task for a workflow execution
	CreateTasksRequest struct {
		TaskQueueInfo *PersistedTaskQueueInfo
		Tasks         []*persistencespb.AllocatedTaskInfo
	}

	// CreateTasksResponse is the response to CreateTasksRequest
	CreateTasksResponse struct {
	}

	PersistedTaskQueueInfo struct {
		Data    *persistencespb.TaskQueueInfo
		RangeID int64
	}

	// GetTasksRequest is used to retrieve tasks of a task queue
	GetTasksRequest struct {
		NamespaceID        string
		TaskQueue          string
		TaskType           enumspb.TaskQueueType
		MinTaskIDExclusive int64 // exclusive
		MaxTaskIDInclusive int64 // inclusive
		PageSize           int
		NextPageToken      []byte
	}

	// GetTasksResponse is the response to GetTasksRequests
	GetTasksResponse struct {
		Tasks         []*persistencespb.AllocatedTaskInfo
		NextPageToken []byte
	}

	// CompleteTaskRequest is used to complete a task
	CompleteTaskRequest struct {
		TaskQueue *TaskQueueKey
		TaskID    int64
	}

	// CompleteTasksLessThanRequest contains the request params needed to invoke CompleteTasksLessThan API
	CompleteTasksLessThanRequest struct {
		NamespaceID   string
		TaskQueueName string
		TaskType      enumspb.TaskQueueType
		TaskID        int64 // Tasks less than or equal to this ID will be completed
		Limit         int   // Limit on the max number of tasks that can be completed. Required param
	}

	// GetTimerTaskRequest is the request for GetTimerTask
	GetTimerTaskRequest struct {
		ShardID             int32
		TaskID              int64
		VisibilityTimestamp time.Time
	}

	// GetTimerTaskResponse is the response to GetTimerTask
	GetTimerTaskResponse struct {
		Task tasks.Task
	}

	// GetTimerTasksRequest is the request for GetTimerTasks
	// TODO: replace this with an iterator that can configure min and max index.
	GetTimerTasksRequest struct {
		ShardID       int32
		MinTimestamp  time.Time
		MaxTimestamp  time.Time
		BatchSize     int
		NextPageToken []byte
	}

	// GetTimerTasksResponse is the response for GetTimerTasks
	GetTimerTasksResponse struct {
		Tasks         []tasks.Task
		NextPageToken []byte
	}

	// CreateNamespaceRequest is used to create the namespace
	CreateNamespaceRequest struct {
		Namespace         *persistencespb.NamespaceDetail
		IsGlobalNamespace bool
	}

	// CreateNamespaceResponse is the response for CreateNamespace
	CreateNamespaceResponse struct {
		ID string
	}

	// GetNamespaceRequest is used to read namespace
	GetNamespaceRequest struct {
		ID   string
		Name string
	}

	// GetNamespaceResponse is the response for GetNamespace
	GetNamespaceResponse struct {
		Namespace           *persistencespb.NamespaceDetail
		IsGlobalNamespace   bool
		NotificationVersion int64
	}

	// UpdateNamespaceRequest is used to update namespace
	UpdateNamespaceRequest struct {
		Namespace           *persistencespb.NamespaceDetail
		IsGlobalNamespace   bool
		NotificationVersion int64
	}

	// DeleteNamespaceRequest is used to delete namespace entry from namespaces table
	DeleteNamespaceRequest struct {
		ID string
	}

	// DeleteNamespaceByNameRequest is used to delete namespace entry from namespaces_by_name table
	DeleteNamespaceByNameRequest struct {
		Name string
	}

	// ListNamespacesRequest is used to list namespaces
	ListNamespacesRequest struct {
		PageSize      int
		NextPageToken []byte
	}

	// ListNamespacesResponse is the response for GetNamespace
	ListNamespacesResponse struct {
		Namespaces    []*GetNamespaceResponse
		NextPageToken []byte
	}

	// GetMetadataResponse is the response for GetMetadata
	GetMetadataResponse struct {
		NotificationVersion int64
	}

	// MutableStateStatistics is the size stats for MutableState
	MutableStateStatistics struct {
		TotalSize         int
		HistoryStatistics *HistoryStatistics

		// Breakdown of size into more granular stats
		ExecutionInfoSize  int
		ExecutionStateSize int

		ActivityInfoSize      int
		TimerInfoSize         int
		ChildInfoSize         int
		RequestCancelInfoSize int
		SignalInfoSize        int
		SignalRequestIDSize   int
		BufferedEventsSize    int

		// Item count for various information captured within mutable state
		ActivityInfoCount      int
		TimerInfoCount         int
		ChildInfoCount         int
		RequestCancelInfoCount int
		SignalInfoCount        int
		SignalRequestIDCount   int
		BufferedEventsCount    int
	}

	HistoryStatistics struct {
		SizeDiff  int
		CountDiff int
	}

	// AppendHistoryNodesRequest is used to append a batch of history nodes
	AppendHistoryNodesRequest struct {
		// The shard to get history node data
		ShardID int32
		// true if this is the first append request to the branch
		IsNewBranch bool
		// the info for clean up data in background
		Info string
		// The branch to be appended
		BranchToken []byte
		// The batch of events to be appended. The first eventID will become the nodeID of this batch
		Events []*historypb.HistoryEvent
		// TransactionID for events before these events. For events chaining
		PrevTransactionID int64
		// requested TransactionID for this write operation. For the same eventID, the node with larger TransactionID always wins
		TransactionID int64
	}

	// AppendHistoryNodesResponse is a response to AppendHistoryNodesRequest
	AppendHistoryNodesResponse struct {
		// the size of the event data that has been appended
		Size int
	}

	// ReadHistoryBranchRequest is used to read a history branch
	ReadHistoryBranchRequest struct {
		// The shard to get history branch data
		ShardID int32
		// The branch to be read
		BranchToken []byte
		// Get the history nodes from MinEventID. Inclusive.
		MinEventID int64
		// Get the history nodes upto MaxEventID.  Exclusive.
		MaxEventID int64
		// Maximum number of batches of events per page. Not that number of events in a batch >=1, it is not number of events per page.
		// However for a single page, it is also possible that the returned events is less than PageSize (event zero events) due to stale events.
		PageSize int
		// Token to continue reading next page of history append transactions.  Pass in empty slice for first page
		NextPageToken []byte
	}

	// ReadHistoryBranchResponse is the response to ReadHistoryBranchRequest
	ReadHistoryBranchResponse struct {
		// History events
		HistoryEvents []*historypb.HistoryEvent
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on ReadHistoryBranchRequest to read the next page.
		// Empty means we have reached the last page, not need to continue
		NextPageToken []byte
		// Size of history read from store
		Size int
	}

	// ReadHistoryBranchByBatchResponse is the response to ReadHistoryBranchRequest
	ReadHistoryBranchByBatchResponse struct {
		// History events by batch
		History []*historypb.History
		// TransactionID for relevant History batch
		TransactionIDs []int64
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on ReadHistoryBranchRequest to read the next page.
		// Empty means we have reached the last page, not need to continue
		NextPageToken []byte
		// Size of history read from store
		Size int
	}

	// ReadRawHistoryBranchResponse is the response to ReadHistoryBranchRequest
	ReadRawHistoryBranchResponse struct {
		// HistoryEventBlobs history event blobs
		HistoryEventBlobs []*commonpb.DataBlob
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on ReadHistoryBranchRequest to read the next page.
		// Empty means we have reached the last page, not need to continue
		NextPageToken []byte
		// Size of history read from store
		Size int
	}

	// ForkHistoryBranchRequest is used to fork a history branch
	ForkHistoryBranchRequest struct {
		// The shard to get history branch data
		ShardID int32
		// The base branch to fork from
		ForkBranchToken []byte
		// The nodeID to fork from, the new branch will start from ( inclusive ), the base branch will stop at(exclusive)
		// Application must provide a void forking nodeID, it must be a valid nodeID in that branch. A valid nodeID is the firstEventID of a valid batch of events.
		// And ForkNodeID > 1 because forking from 1 doesn't make any sense.
		ForkNodeID int64
		// the info for clean up data in background
		Info string
	}

	// ForkHistoryBranchResponse is the response to ForkHistoryBranchRequest
	ForkHistoryBranchResponse struct {
		// branchToken to represent the new branch
		NewBranchToken []byte
	}

	// CompleteForkBranchRequest is used to complete forking
	CompleteForkBranchRequest struct {
		// the new branch returned from ForkHistoryBranchRequest
		BranchToken []byte
		// true means the fork is success, will update the flag, otherwise will delete the new branch
		Success bool
		// The shard to update history branch data
		ShardID *int
	}

	// DeleteHistoryBranchRequest is used to remove a history branch
	DeleteHistoryBranchRequest struct {
		// The shard to delete history branch data
		ShardID int32
		// branch to be deleted
		BranchToken []byte
	}

	// TrimHistoryBranchRequest is used to validate & trim a history branch
	TrimHistoryBranchRequest struct {
		// The shard to delete history branch data
		ShardID int32
		// branch to be validated & trimmed
		BranchToken []byte
		// known valid node ID
		NodeID int64
		// known valid transaction ID
		TransactionID int64
	}

	// TrimHistoryBranchResponse is the response to TrimHistoryBranchRequest
	TrimHistoryBranchResponse struct {
	}

	// GetHistoryTreeRequest is used to retrieve branch info of a history tree
	GetHistoryTreeRequest struct {
		// A UUID of a tree
		TreeID string
		// Get data from this shard
		ShardID *int32
		// optional: can provide treeID via branchToken if treeID is empty
		BranchToken []byte
	}

	// HistoryBranchDetail contains detailed information of a branch
	HistoryBranchDetail struct {
		TreeID   string
		BranchID string
		ForkTime *time.Time
		Info     string
	}

	// GetHistoryTreeResponse is a response to GetHistoryTreeRequest
	GetHistoryTreeResponse struct {
		// all branches of a tree
		Branches []*persistencespb.HistoryBranch
	}

	// GetAllHistoryTreeBranchesRequest is a request of GetAllHistoryTreeBranches
	GetAllHistoryTreeBranchesRequest struct {
		// pagination token
		NextPageToken []byte
		// maximum number of branches returned per page
		PageSize int
	}

	// GetAllHistoryTreeBranchesResponse is a response to GetAllHistoryTreeBranches
	GetAllHistoryTreeBranchesResponse struct {
		// pagination token
		NextPageToken []byte
		// all branches of all trees
		Branches []HistoryBranchDetail
	}

	// ListClusterMetadataRequest is the request to ListClusterMetadata
	ListClusterMetadataRequest struct {
		PageSize      int
		NextPageToken []byte
	}

	// ListClusterMetadataResponse is the response to ListClusterMetadata
	ListClusterMetadataResponse struct {
		ClusterMetadata []*GetClusterMetadataResponse
		NextPageToken   []byte
	}

	// GetClusterMetadataRequest is the request to GetClusterMetadata
	GetClusterMetadataRequest struct {
		ClusterName string
	}

	// GetClusterMetadataResponse is the response to GetClusterMetadata
	GetClusterMetadataResponse struct {
		persistencespb.ClusterMetadata
		Version int64
	}

	// SaveClusterMetadataRequest is the request to SaveClusterMetadata
	SaveClusterMetadataRequest struct {
		persistencespb.ClusterMetadata
		Version int64
	}

	// DeleteClusterMetadataRequest is the request to DeleteClusterMetadata
	DeleteClusterMetadataRequest struct {
		ClusterName string
	}

	// GetClusterMembersRequest is the request to GetClusterMembers
	GetClusterMembersRequest struct {
		LastHeartbeatWithin time.Duration
		RPCAddressEquals    net.IP
		HostIDEquals        uuid.UUID
		RoleEquals          ServiceType
		SessionStartedAfter time.Time
		NextPageToken       []byte
		PageSize            int
	}

	// GetClusterMembersResponse is the response to GetClusterMembers
	GetClusterMembersResponse struct {
		ActiveMembers []*ClusterMember
		NextPageToken []byte
	}

	// ClusterMember is used as a response to GetClusterMembers
	ClusterMember struct {
		Role          ServiceType
		HostID        uuid.UUID
		RPCAddress    net.IP
		RPCPort       uint16
		SessionStart  time.Time
		LastHeartbeat time.Time
		RecordExpiry  time.Time
	}

	// UpsertClusterMembershipRequest is the request to UpsertClusterMembership
	UpsertClusterMembershipRequest struct {
		Role         ServiceType
		HostID       uuid.UUID
		RPCAddress   net.IP
		RPCPort      uint16
		SessionStart time.Time
		RecordExpiry time.Duration
	}

	// PruneClusterMembershipRequest is the request to PruneClusterMembership
	PruneClusterMembershipRequest struct {
		MaxRecordsPruned int
	}

	// Closeable is an interface for any entity that supports a close operation to release resources
	Closeable interface {
		Close()
	}

	// ShardManager is used to manage all shards
	ShardManager interface {
		Closeable
		GetName() string
		GetOrCreateShard(request *GetOrCreateShardRequest) (*GetOrCreateShardResponse, error)
		UpdateShard(request *UpdateShardRequest) error
	}

	// ExecutionManager is used to manage workflow executions
	ExecutionManager interface {
		Closeable
		GetName() string

		CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) (*UpdateWorkflowExecutionResponse, error)
		ConflictResolveWorkflowExecution(request *ConflictResolveWorkflowExecutionRequest) (*ConflictResolveWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error
		GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error)

		// Scan operations

		ListConcreteExecutions(request *ListConcreteExecutionsRequest) (*ListConcreteExecutionsResponse, error)

		// Tasks related APIs

		AddTasks(request *AddTasksRequest) error

		// transfer tasks

		GetTransferTask(request *GetTransferTaskRequest) (*GetTransferTaskResponse, error)
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error
		RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error

		// timer tasks

		GetTimerTask(request *GetTimerTaskRequest) (*GetTimerTaskResponse, error)
		GetTimerTasks(request *GetTimerTasksRequest) (*GetTimerTasksResponse, error)
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

		// The below are history V2 APIs
		// V2 regards history events growing as a tree, decoupled from workflow concepts
		// For Temporal, treeID is new runID, except for fork(reset), treeID will be the runID that it forks from.

		// AppendHistoryNodes add a node to history node table
		AppendHistoryNodes(request *AppendHistoryNodesRequest) (*AppendHistoryNodesResponse, error)
		// ReadHistoryBranch returns history node data for a branch
		ReadHistoryBranch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchResponse, error)
		// ReadHistoryBranchByBatch returns history node data for a branch ByBatch
		ReadHistoryBranchByBatch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchByBatchResponse, error)
		// ReadRawHistoryBranch returns history node raw data for a branch ByBatch
		// NOTE: this API should only be used by 3+DC
		ReadRawHistoryBranch(request *ReadHistoryBranchRequest) (*ReadRawHistoryBranchResponse, error)
		// ForkHistoryBranch forks a new branch from a old branch
		ForkHistoryBranch(request *ForkHistoryBranchRequest) (*ForkHistoryBranchResponse, error)
		// DeleteHistoryBranch removes a branch
		// If this is the last branch to delete, it will also remove the root node
		DeleteHistoryBranch(request *DeleteHistoryBranchRequest) error
		// TrimHistoryBranch validate & trim a history branch
		TrimHistoryBranch(request *TrimHistoryBranchRequest) (*TrimHistoryBranchResponse, error)
		// GetHistoryTree returns all branch information of a tree
		GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error)
		// GetAllHistoryTreeBranches returns all branches of all trees
		GetAllHistoryTreeBranches(request *GetAllHistoryTreeBranchesRequest) (*GetAllHistoryTreeBranchesResponse, error)
	}

	// TaskManager is used to manage tasks
	TaskManager interface {
		Closeable
		GetName() string
		CreateTaskQueue(request *CreateTaskQueueRequest) (*CreateTaskQueueResponse, error)
		UpdateTaskQueue(request *UpdateTaskQueueRequest) (*UpdateTaskQueueResponse, error)
		GetTaskQueue(request *GetTaskQueueRequest) (*GetTaskQueueResponse, error)
		ListTaskQueue(request *ListTaskQueueRequest) (*ListTaskQueueResponse, error)
		DeleteTaskQueue(request *DeleteTaskQueueRequest) error
		CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error)
		GetTasks(request *GetTasksRequest) (*GetTasksResponse, error)
		CompleteTask(request *CompleteTaskRequest) error
		// CompleteTasksLessThan completes tasks less than or equal to the given task id
		// This API takes a limit parameter which specifies the count of maxRows that
		// can be deleted. This parameter may be ignored by the underlying storage, but
		// its mandatory to specify it. On success this method returns the number of rows
		// actually deleted. If the underlying storage doesn't support "limit", all rows
		// less than or equal to taskID will be deleted.
		// On success, this method returns:
		//  - number of rows actually deleted, if limit is honored
		//  - UnknownNumRowsDeleted, when all rows below value are deleted
		CompleteTasksLessThan(request *CompleteTasksLessThanRequest) (int, error)
	}

	// MetadataManager is used to manage metadata CRUD for namespace entities
	MetadataManager interface {
		Closeable
		GetName() string
		CreateNamespace(request *CreateNamespaceRequest) (*CreateNamespaceResponse, error)
		GetNamespace(request *GetNamespaceRequest) (*GetNamespaceResponse, error)
		UpdateNamespace(request *UpdateNamespaceRequest) error
		DeleteNamespace(request *DeleteNamespaceRequest) error
		DeleteNamespaceByName(request *DeleteNamespaceByNameRequest) error
		ListNamespaces(request *ListNamespacesRequest) (*ListNamespacesResponse, error)
		GetMetadata() (*GetMetadataResponse, error)
		InitializeSystemNamespaces(currentClusterName string) error
	}

	// ClusterMetadataManager is used to manage cluster-wide metadata and configuration
	ClusterMetadataManager interface {
		Closeable
		GetName() string
		GetClusterMembers(request *GetClusterMembersRequest) (*GetClusterMembersResponse, error)
		UpsertClusterMembership(request *UpsertClusterMembershipRequest) error
		PruneClusterMembership(request *PruneClusterMembershipRequest) error
		ListClusterMetadata(request *ListClusterMetadataRequest) (*ListClusterMetadataResponse, error)
		GetCurrentClusterMetadata() (*GetClusterMetadataResponse, error)
		GetClusterMetadata(request *GetClusterMetadataRequest) (*GetClusterMetadataResponse, error)
		SaveClusterMetadata(request *SaveClusterMetadataRequest) (bool, error)
		DeleteClusterMetadata(request *DeleteClusterMetadataRequest) error
	}
)

func (e *InvalidPersistenceRequestError) Error() string {
	return e.Msg
}

func (e *CurrentWorkflowConditionFailedError) Error() string {
	return e.Msg
}

func (e *WorkflowConditionFailedError) Error() string {
	return e.Msg
}

func (e *ConditionFailedError) Error() string {
	return e.Msg
}

func (e *ShardAlreadyExistError) Error() string {
	return e.Msg
}

func (e *ShardOwnershipLostError) Error() string {
	return e.Msg
}

func (e *TimeoutError) Error() string {
	return e.Msg
}

func (e *TransactionSizeLimitError) Error() string {
	return e.Msg
}

// UnixMilliseconds returns t as a Unix time, the number of milliseconds elapsed since January 1, 1970 UTC.
// It should be used for all CQL timestamp.
func UnixMilliseconds(t time.Time) int64 {
	// Handling zero time separately because UnixNano is undefined for zero times.
	if t.IsZero() {
		return 0
	}

	unixNano := t.UnixNano()
	if unixNano < 0 {
		// Time is before January 1, 1970 UTC
		return 0
	}
	return unixNano / int64(time.Millisecond)
}

// NewHistoryBranchToken return a new branch token
func NewHistoryBranchToken(treeID string) ([]byte, error) {
	branchID := primitives.NewUUID().String()
	bi := &persistencespb.HistoryBranch{
		TreeId:    treeID,
		BranchId:  branchID,
		Ancestors: []*persistencespb.HistoryBranchRange{},
	}
	datablob, err := serialization.HistoryBranchToBlob(bi)
	if err != nil {
		return nil, err
	}
	token := datablob.Data
	return token, nil
}

// NewHistoryBranchTokenByBranchID return a new branch token with treeID/branchID
func NewHistoryBranchTokenByBranchID(treeID, branchID string) ([]byte, error) {
	bi := &persistencespb.HistoryBranch{
		TreeId:    treeID,
		BranchId:  branchID,
		Ancestors: []*persistencespb.HistoryBranchRange{},
	}
	datablob, err := serialization.HistoryBranchToBlob(bi)
	if err != nil {
		return nil, err
	}
	token := datablob.Data
	return token, nil
}

// BuildHistoryGarbageCleanupInfo combine the workflow identity information into a string
func BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID string) string {
	return fmt.Sprintf("%v:%v:%v", namespaceID, workflowID, runID)
}

// SplitHistoryGarbageCleanupInfo returns workflow identity information
func SplitHistoryGarbageCleanupInfo(info string) (namespaceID, workflowID, runID string, err error) {
	ss := strings.Split(info, ":")
	// workflowID can contain ":" so len(ss) can be greater than 3
	if len(ss) < numItemsInGarbageInfo {
		return "", "", "", fmt.Errorf("not able to split info for  %s", info)
	}
	namespaceID = ss[0]
	runID = ss[len(ss)-1]
	workflowEnd := len(info) - len(runID) - 1
	workflowID = info[len(namespaceID)+1 : workflowEnd]
	return
}

// NewGetReplicationTasksFromDLQRequest creates a new GetReplicationTasksFromDLQRequest
func NewGetReplicationTasksFromDLQRequest(
	shardID int32,
	sourceClusterName string,
	readLevel int64,
	maxReadLevel int64,
	batchSize int,
	nextPageToken []byte,
) *GetReplicationTasksFromDLQRequest {
	return &GetReplicationTasksFromDLQRequest{
		ShardID:           shardID,
		SourceClusterName: sourceClusterName,
		GetReplicationTasksRequest: GetReplicationTasksRequest{
			MinTaskID:     readLevel,
			MaxTaskID:     maxReadLevel,
			BatchSize:     batchSize,
			NextPageToken: nextPageToken,
		},
	}
}

type ServiceType int

const (
	All ServiceType = iota
	Frontend
	History
	Matching
	Worker
)
