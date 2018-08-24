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
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// TODO remove this table version
// this is a temp version indicating where is the domain resides
// either V1 or V2
const (
	DomainTableVersionV1 = 1
	DomainTableVersionV2 = 2
)

// Domain status
const (
	DomainStatusRegistered = iota
	DomainStatusDeprecated
	DomainStatusDeleted
)

// Workflow execution states
const (
	WorkflowStateCreated = iota
	WorkflowStateRunning
	WorkflowStateCompleted
)

// Workflow execution close status
const (
	WorkflowCloseStatusNone = iota
	WorkflowCloseStatusCompleted
	WorkflowCloseStatusFailed
	WorkflowCloseStatusCanceled
	WorkflowCloseStatusTerminated
	WorkflowCloseStatusContinuedAsNew
	WorkflowCloseStatusTimedOut
)

// Types of task lists
const (
	TaskListTypeDecision = iota
	TaskListTypeActivity
)

// Kinds of task lists
const (
	TaskListKindNormal = iota
	TaskListKindSticky
)

// Transfer task types
const (
	TransferTaskTypeDecisionTask = iota
	TransferTaskTypeActivityTask
	TransferTaskTypeCloseExecution
	TransferTaskTypeCancelExecution
	TransferTaskTypeStartChildExecution
	TransferTaskTypeSignalExecution
)

// Types of replication tasks
const (
	ReplicationTaskTypeHistory = iota
	ReplicationTaskTypeHeartbeat
)

// Types of timers
const (
	TaskTypeDecisionTimeout = iota
	TaskTypeActivityTimeout
	TaskTypeUserTimer
	TaskTypeWorkflowTimeout
	TaskTypeDeleteHistoryEvent
	TaskTypeActivityRetryTimer
	TaskTypeWorkflowRetryTimer
)

type (
	// ConditionFailedError represents a failed conditional put
	ConditionFailedError struct {
		Msg string
	}

	// ShardAlreadyExistError is returned when conditionally creating a shard fails
	ShardAlreadyExistError struct {
		Msg string
	}

	// ShardOwnershipLostError is returned when conditional update fails due to RangeID for the shard
	ShardOwnershipLostError struct {
		ShardID int
		Msg     string
	}

	// WorkflowExecutionAlreadyStartedError is returned when creating a new workflow failed.
	WorkflowExecutionAlreadyStartedError struct {
		Msg            string
		StartRequestID string
		RunID          string
		State          int
		CloseStatus    int
		StartVersion   int64
	}

	// TimeoutError is returned when a write operation fails due to a timeout
	TimeoutError struct {
		Msg string
	}

	// ShardInfo describes a shard
	ShardInfo struct {
		ShardID                   int
		Owner                     string
		RangeID                   int64
		StolenSinceRenew          int
		UpdatedAt                 time.Time
		ReplicationAckLevel       int64
		TransferAckLevel          int64
		TimerAckLevel             time.Time
		ClusterTransferAckLevel   map[string]int64
		ClusterTimerAckLevel      map[string]time.Time
		TransferFailoverLevels    map[string]TransferFailoverLevel // uuid -> TransferFailoverLevel
		TimerFailoverLevels       map[string]TimerFailoverLevel    // uuid -> TimerFailoverLevel
		DomainNotificationVersion int64
	}

	// TransferFailoverLevel contains corresponding start / end level
	TransferFailoverLevel struct {
		MinLevel     int64
		CurrentLevel int64
		MaxLevel     int64
		DomainIDs    []string
	}

	// TimerFailoverLevel contains domain IDs and corresponding start / end level
	TimerFailoverLevel struct {
		MinLevel     time.Time
		CurrentLevel time.Time
		MaxLevel     time.Time
		DomainIDs    []string
	}

	// WorkflowExecutionInfo describes a workflow execution
	WorkflowExecutionInfo struct {
		DomainID                     string
		WorkflowID                   string
		RunID                        string
		ParentDomainID               string
		ParentWorkflowID             string
		ParentRunID                  string
		InitiatedID                  int64
		CompletionEvent              []byte
		TaskList                     string
		WorkflowTypeName             string
		WorkflowTimeout              int32
		DecisionTimeoutValue         int32
		ExecutionContext             []byte
		State                        int
		CloseStatus                  int
		LastFirstEventID             int64
		NextEventID                  int64
		LastProcessedEvent           int64
		StartTimestamp               time.Time
		LastUpdatedTimestamp         time.Time
		CreateRequestID              string
		DecisionVersion              int64
		DecisionScheduleID           int64
		DecisionStartedID            int64
		DecisionRequestID            string
		DecisionTimeout              int32
		DecisionAttempt              int64
		DecisionTimestamp            int64
		CancelRequested              bool
		CancelRequestID              string
		StickyTaskList               string
		StickyScheduleToStartTimeout int32
		ClientLibraryVersion         string
		ClientFeatureVersion         string
		ClientImpl                   string
		// for retry
		Attempt            int32
		HasRetryPolicy     bool
		InitialInterval    int32
		BackoffCoefficient float64
		MaximumInterval    int32
		ExpirationTime     time.Time
		MaximumAttempts    int32
		NonRetriableErrors []string
	}

	// ReplicationState represents mutable state information for global domains.
	// This information is used by replication protocol when applying events from remote clusters
	ReplicationState struct {
		CurrentVersion      int64
		StartVersion        int64
		LastWriteVersion    int64
		LastWriteEventID    int64
		LastReplicationInfo map[string]*ReplicationInfo
	}

	// TransferTaskInfo describes a transfer task
	TransferTaskInfo struct {
		DomainID                string
		WorkflowID              string
		RunID                   string
		VisibilityTimestamp     time.Time
		TaskID                  int64
		TargetDomainID          string
		TargetWorkflowID        string
		TargetRunID             string
		TargetChildWorkflowOnly bool
		TaskList                string
		TaskType                int
		ScheduleID              int64
		Version                 int64
	}

	// ReplicationTaskInfo describes the replication task created for replication of history events
	ReplicationTaskInfo struct {
		DomainID            string
		WorkflowID          string
		RunID               string
		TaskID              int64
		TaskType            int
		FirstEventID        int64
		NextEventID         int64
		Version             int64
		LastReplicationInfo map[string]*ReplicationInfo
	}

	// TimerTaskInfo describes a timer task.
	TimerTaskInfo struct {
		DomainID            string
		WorkflowID          string
		RunID               string
		VisibilityTimestamp time.Time
		TaskID              int64
		TaskType            int
		TimeoutType         int
		EventID             int64
		ScheduleAttempt     int64
		Version             int64
	}

	// TaskListInfo describes a state of a task list implementation.
	TaskListInfo struct {
		DomainID string
		Name     string
		TaskType int
		RangeID  int64
		AckLevel int64
		Kind     int
	}

	// TaskInfo describes either activity or decision task
	TaskInfo struct {
		DomainID               string
		WorkflowID             string
		RunID                  string
		TaskID                 int64
		ScheduleID             int64
		ScheduleToStartTimeout int32
	}

	// Task is the generic interface for workflow tasks
	Task interface {
		GetType() int
		GetVersion() int64
		SetVersion(version int64)
		GetTaskID() int64
		SetTaskID(id int64)
		GetVisibilityTimestamp() time.Time
		SetVisibilityTimestamp(timestamp time.Time)
	}

	// ActivityTask identifies a transfer task for activity
	ActivityTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		DomainID            string
		TaskList            string
		ScheduleID          int64
		Version             int64
	}

	// DecisionTask identifies a transfer task for decision
	DecisionTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		DomainID            string
		TaskList            string
		ScheduleID          int64
		Version             int64
	}

	// CloseExecutionTask identifies a transfer task for deletion of execution
	CloseExecutionTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}

	// DeleteHistoryEventTask identifies a timer task for deletion of history events of completed execution.
	DeleteHistoryEventTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}

	// DecisionTimeoutTask identifies a timeout task.
	DecisionTimeoutTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		ScheduleAttempt     int64
		TimeoutType         int
		Version             int64
	}

	// WorkflowTimeoutTask identifies a timeout task.
	WorkflowTimeoutTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}

	// CancelExecutionTask identifies a transfer task for cancel of execution
	CancelExecutionTask struct {
		VisibilityTimestamp     time.Time
		TaskID                  int64
		TargetDomainID          string
		TargetWorkflowID        string
		TargetRunID             string
		TargetChildWorkflowOnly bool
		InitiatedID             int64
		Version                 int64
	}

	// SignalExecutionTask identifies a transfer task for signal execution
	SignalExecutionTask struct {
		VisibilityTimestamp     time.Time
		TaskID                  int64
		TargetDomainID          string
		TargetWorkflowID        string
		TargetRunID             string
		TargetChildWorkflowOnly bool
		InitiatedID             int64
		Version                 int64
	}

	// StartChildExecutionTask identifies a transfer task for starting child execution
	StartChildExecutionTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		TargetDomainID      string
		TargetWorkflowID    string
		InitiatedID         int64
		Version             int64
	}

	// ActivityTimeoutTask identifies a timeout task.
	ActivityTimeoutTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		TimeoutType         int
		EventID             int64
		Attempt             int64
		Version             int64
	}

	// UserTimerTask identifies a timeout task.
	UserTimerTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		Version             int64
	}

	// ActivityRetryTimerTask to schedule a retry task for activity
	ActivityRetryTimerTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		Version             int64
		Attempt             int32
	}

	// WorkflowRetryTimerTask to schedule first decision task for retried workflow
	WorkflowRetryTimerTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		Version             int64
	}

	// HistoryReplicationTask is the transfer task created for shipping history replication events to other clusters
	HistoryReplicationTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		FirstEventID        int64
		NextEventID         int64
		Version             int64
		LastReplicationInfo map[string]*ReplicationInfo
	}

	// ReplicationInfo represents the information stored for last replication event details per cluster
	ReplicationInfo struct {
		Version     int64
		LastEventID int64
	}

	// WorkflowMutableState indicates workflow related state
	WorkflowMutableState struct {
		ActivitInfos             map[int64]*ActivityInfo
		TimerInfos               map[string]*TimerInfo
		ChildExecutionInfos      map[int64]*ChildExecutionInfo
		RequestCancelInfos       map[int64]*RequestCancelInfo
		SignalInfos              map[int64]*SignalInfo
		SignalRequestedIDs       map[string]struct{}
		ExecutionInfo            *WorkflowExecutionInfo
		ReplicationState         *ReplicationState
		BufferedEvents           []*SerializedHistoryEventBatch
		BufferedReplicationTasks map[int64]*BufferedReplicationTask
	}

	// ActivityInfo details.
	ActivityInfo struct {
		Version                  int64
		ScheduleID               int64
		ScheduledEvent           []byte
		ScheduledTime            time.Time
		StartedID                int64
		StartedEvent             []byte
		StartedTime              time.Time
		ActivityID               string
		RequestID                string
		Details                  []byte
		ScheduleToStartTimeout   int32
		ScheduleToCloseTimeout   int32
		StartToCloseTimeout      int32
		HeartbeatTimeout         int32
		CancelRequested          bool
		CancelRequestID          int64
		LastHeartBeatUpdatedTime time.Time
		TimerTaskStatus          int32
		// For retry
		Attempt            int32
		DomainID           string
		StartedIdentity    string
		TaskList           string
		HasRetryPolicy     bool
		InitialInterval    int32
		BackoffCoefficient float64
		MaximumInterval    int32
		ExpirationTime     time.Time
		MaximumAttempts    int32
		NonRetriableErrors []string
		// Not written to database - This is used only for deduping heartbeat timer creation
		LastTimeoutVisibility int64
	}

	// TimerInfo details - metadata about user timer info.
	TimerInfo struct {
		Version    int64
		TimerID    string
		StartedID  int64
		ExpiryTime time.Time
		TaskID     int64
	}

	// ChildExecutionInfo has details for pending child executions.
	ChildExecutionInfo struct {
		Version         int64
		InitiatedID     int64
		InitiatedEvent  []byte
		StartedID       int64
		StartedEvent    []byte
		CreateRequestID string
	}

	// RequestCancelInfo has details for pending external workflow cancellations
	RequestCancelInfo struct {
		Version         int64
		InitiatedID     int64
		CancelRequestID string
	}

	// SignalInfo has details for pending external workflow signal
	SignalInfo struct {
		Version         int64
		InitiatedID     int64
		SignalRequestID string
		SignalName      string
		Input           []byte
		Control         []byte
	}

	// BufferedReplicationTask has details to handle out of order receive of history events
	BufferedReplicationTask struct {
		FirstEventID  int64
		NextEventID   int64
		Version       int64
		History       *SerializedHistoryEventBatch
		NewRunHistory *SerializedHistoryEventBatch
	}

	// CreateShardRequest is used to create a shard in executions table
	CreateShardRequest struct {
		ShardInfo *ShardInfo
	}

	// GetShardRequest is used to get shard information
	GetShardRequest struct {
		ShardID int
	}

	// GetShardResponse is the response to GetShard
	GetShardResponse struct {
		ShardInfo *ShardInfo
	}

	// UpdateShardRequest  is used to update shard information
	UpdateShardRequest struct {
		ShardInfo       *ShardInfo
		PreviousRangeID int64
	}

	// CreateWorkflowExecutionRequest is used to write a new workflow execution
	CreateWorkflowExecutionRequest struct {
		RequestID                   string
		DomainID                    string
		Execution                   workflow.WorkflowExecution
		ParentDomainID              string
		ParentExecution             *workflow.WorkflowExecution
		InitiatedID                 int64
		TaskList                    string
		WorkflowTypeName            string
		WorkflowTimeout             int32
		DecisionTimeoutValue        int32
		ExecutionContext            []byte
		NextEventID                 int64
		LastProcessedEvent          int64
		TransferTasks               []Task
		ReplicationTasks            []Task
		TimerTasks                  []Task
		RangeID                     int64
		DecisionVersion             int64
		DecisionScheduleID          int64
		DecisionStartedID           int64
		DecisionStartToCloseTimeout int32
		ContinueAsNew               bool
		PreviousRunID               string
		ReplicationState            *ReplicationState
		Attempt                     int32
		HasRetryPolicy              bool
		InitialInterval             int32
		BackoffCoefficient          float64
		MaximumInterval             int32
		ExpirationTime              time.Time
		MaximumAttempts             int32
		NonRetriableErrors          []string
	}

	// CreateWorkflowExecutionResponse is the response to CreateWorkflowExecutionRequest
	CreateWorkflowExecutionResponse struct {
	}

	// GetWorkflowExecutionRequest is used to retrieve the info of a workflow execution
	GetWorkflowExecutionRequest struct {
		DomainID  string
		Execution workflow.WorkflowExecution
	}

	// GetWorkflowExecutionResponse is the response to GetworkflowExecutionRequest
	GetWorkflowExecutionResponse struct {
		State *WorkflowMutableState
	}

	// GetCurrentExecutionRequest is used to retrieve the current RunId for an execution
	GetCurrentExecutionRequest struct {
		DomainID   string
		WorkflowID string
	}

	// GetCurrentExecutionResponse is the response to GetCurrentExecution
	GetCurrentExecutionResponse struct {
		StartRequestID string
		RunID          string
		State          int
		CloseStatus    int
	}

	// UpdateWorkflowExecutionRequest is used to update a workflow execution
	UpdateWorkflowExecutionRequest struct {
		ExecutionInfo        *WorkflowExecutionInfo
		ReplicationState     *ReplicationState
		TransferTasks        []Task
		TimerTasks           []Task
		ReplicationTasks     []Task
		DeleteTimerTask      Task
		Condition            int64
		RangeID              int64
		ContinueAsNew        *CreateWorkflowExecutionRequest
		FinishExecution      bool
		FinishedExecutionTTL int32

		// Mutable state
		UpsertActivityInfos           []*ActivityInfo
		DeleteActivityInfos           []int64
		UpserTimerInfos               []*TimerInfo
		DeleteTimerInfos              []string
		UpsertChildExecutionInfos     []*ChildExecutionInfo
		DeleteChildExecutionInfo      *int64
		UpsertRequestCancelInfos      []*RequestCancelInfo
		DeleteRequestCancelInfo       *int64
		UpsertSignalInfos             []*SignalInfo
		DeleteSignalInfo              *int64
		UpsertSignalRequestedIDs      []string
		DeleteSignalRequestedID       string
		NewBufferedEvents             *SerializedHistoryEventBatch
		ClearBufferedEvents           bool
		NewBufferedReplicationTask    *BufferedReplicationTask
		DeleteBufferedReplicationTask *int64
	}

	// ResetMutableStateRequest is used to reset workflow execution state
	ResetMutableStateRequest struct {
		PrevRunID        string
		ExecutionInfo    *WorkflowExecutionInfo
		ReplicationState *ReplicationState
		Condition        int64
		RangeID          int64

		// Mutable state
		InsertActivityInfos       []*ActivityInfo
		InsertTimerInfos          []*TimerInfo
		InsertChildExecutionInfos []*ChildExecutionInfo
		InsertRequestCancelInfos  []*RequestCancelInfo
		InsertSignalInfos         []*SignalInfo
		InsertSignalRequestedIDs  []string
	}

	// DeleteWorkflowExecutionRequest is used to delete a workflow execution
	DeleteWorkflowExecutionRequest struct {
		DomainID   string
		WorkflowID string
		RunID      string
	}

	// GetTransferTasksRequest is used to read tasks from the transfer task queue
	GetTransferTasksRequest struct {
		ReadLevel     int64
		MaxReadLevel  int64
		BatchSize     int
		NextPageToken []byte
	}

	// GetTransferTasksResponse is the response to GetTransferTasksRequest
	GetTransferTasksResponse struct {
		Tasks         []*TransferTaskInfo
		NextPageToken []byte
	}

	// GetReplicationTasksRequest is used to read tasks from the replication task queue
	GetReplicationTasksRequest struct {
		ReadLevel     int64
		MaxReadLevel  int64
		BatchSize     int
		NextPageToken []byte
	}

	// GetReplicationTasksResponse is the response to GetReplicationTask
	GetReplicationTasksResponse struct {
		Tasks         []*ReplicationTaskInfo
		NextPageToken []byte
	}

	// CompleteTransferTaskRequest is used to complete a task in the transfer task queue
	CompleteTransferTaskRequest struct {
		TaskID int64
	}

	// RangeCompleteTransferTaskRequest is used to complete a range of tasks in the transfer task queue
	RangeCompleteTransferTaskRequest struct {
		ExclusiveBeginTaskID int64
		InclusiveEndTaskID   int64
	}

	// CompleteReplicationTaskRequest is used to complete a task in the replication task queue
	CompleteReplicationTaskRequest struct {
		TaskID int64
	}

	// RangeCompleteTimerTaskRequest is used to complete a range of tasks in the timer task queue
	RangeCompleteTimerTaskRequest struct {
		InclusiveBeginTimestamp time.Time
		ExclusiveEndTimestamp   time.Time
	}

	// CompleteTimerTaskRequest is used to complete a task in the timer task queue
	CompleteTimerTaskRequest struct {
		VisibilityTimestamp time.Time
		TaskID              int64
	}

	// LeaseTaskListRequest is used to request lease of a task list
	LeaseTaskListRequest struct {
		DomainID     string
		TaskList     string
		TaskType     int
		TaskListKind int
	}

	// LeaseTaskListResponse is response to LeaseTaskListRequest
	LeaseTaskListResponse struct {
		TaskListInfo *TaskListInfo
	}

	// UpdateTaskListRequest is used to update task list implementation information
	UpdateTaskListRequest struct {
		TaskListInfo *TaskListInfo
	}

	// UpdateTaskListResponse is the response to UpdateTaskList
	UpdateTaskListResponse struct {
	}

	// CreateTasksRequest is used to create a new task for a workflow exectution
	CreateTasksRequest struct {
		TaskListInfo *TaskListInfo
		Tasks        []*CreateTaskInfo
	}

	// CreateTaskInfo describes a task to be created in CreateTasksRequest
	CreateTaskInfo struct {
		Execution workflow.WorkflowExecution
		Data      *TaskInfo
		TaskID    int64
	}

	// CreateTasksResponse is the response to CreateTasksRequest
	CreateTasksResponse struct {
	}

	// GetTasksRequest is used to retrieve tasks of a task list
	GetTasksRequest struct {
		DomainID     string
		TaskList     string
		TaskType     int
		ReadLevel    int64
		MaxReadLevel int64 // inclusive
		BatchSize    int
		RangeID      int64
	}

	// GetTasksResponse is the response to GetTasksRequests
	GetTasksResponse struct {
		Tasks []*TaskInfo
	}

	// CompleteTaskRequest is used to complete a task
	CompleteTaskRequest struct {
		TaskList *TaskListInfo
		TaskID   int64
	}

	// GetTimerIndexTasksRequest is the request for GetTimerIndexTasks
	// TODO: replace this with an iterator that can configure min and max index.
	GetTimerIndexTasksRequest struct {
		MinTimestamp  time.Time
		MaxTimestamp  time.Time
		BatchSize     int
		NextPageToken []byte
	}

	// GetTimerIndexTasksResponse is the response for GetTimerIndexTasks
	GetTimerIndexTasksResponse struct {
		Timers        []*TimerTaskInfo
		NextPageToken []byte
	}

	// SerializedHistoryEventBatch represents a serialized batch of history events
	SerializedHistoryEventBatch struct {
		EncodingType common.EncodingType
		Version      int
		Data         []byte
	}

	// HistoryEventBatch represents a batch of history events
	HistoryEventBatch struct {
		Version int
		Events  []*workflow.HistoryEvent
	}

	// AppendHistoryEventsRequest is used to append new events to workflow execution history
	AppendHistoryEventsRequest struct {
		DomainID          string
		Execution         workflow.WorkflowExecution
		FirstEventID      int64
		EventBatchVersion int64
		RangeID           int64
		TransactionID     int64
		Events            *SerializedHistoryEventBatch
		Overwrite         bool
	}

	// GetWorkflowExecutionHistoryRequest is used to retrieve history of a workflow execution
	GetWorkflowExecutionHistoryRequest struct {
		DomainID  string
		Execution workflow.WorkflowExecution
		// Get the history events from FirstEventID. Inclusive.
		FirstEventID int64
		// Get the history events upto NextEventID.  Not Inclusive.
		NextEventID int64
		// Maximum number of history append transactions per page
		PageSize int
		// Token to continue reading next page of history append transactions.  Pass in empty slice for first page
		NextPageToken []byte
	}

	// GetWorkflowExecutionHistoryResponse is the response to GetWorkflowExecutionHistoryRequest
	GetWorkflowExecutionHistoryResponse struct {
		History *workflow.History
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on GetworkflowExecutionHistoryRequest to read the next page.
		NextPageToken []byte
		// the first_event_id of last loaded batch
		LastFirstEventID int64
	}

	// DeleteWorkflowExecutionHistoryRequest is used to delete workflow execution history
	DeleteWorkflowExecutionHistoryRequest struct {
		DomainID  string
		Execution workflow.WorkflowExecution
	}

	// DomainInfo describes the domain entity
	DomainInfo struct {
		ID          string
		Name        string
		Status      int
		Description string
		OwnerEmail  string
		Data        map[string]string
	}

	// DomainConfig describes the domain configuration
	DomainConfig struct {
		// NOTE: this retention is in days, not in seconds
		Retention  int32
		EmitMetric bool
	}

	// DomainReplicationConfig describes the cross DC domain replication configuration
	DomainReplicationConfig struct {
		ActiveClusterName string
		Clusters          []*ClusterReplicationConfig
	}

	// ClusterReplicationConfig describes the cross DC cluster replication configuration
	ClusterReplicationConfig struct {
		ClusterName string
	}

	// CreateDomainRequest is used to create the domain
	CreateDomainRequest struct {
		Info              *DomainInfo
		Config            *DomainConfig
		ReplicationConfig *DomainReplicationConfig
		IsGlobalDomain    bool
		ConfigVersion     int64
		FailoverVersion   int64
	}

	// CreateDomainResponse is the response for CreateDomain
	CreateDomainResponse struct {
		ID string
	}

	// GetDomainRequest is used to read domain
	GetDomainRequest struct {
		ID   string
		Name string
	}

	// GetDomainResponse is the response for GetDomain
	GetDomainResponse struct {
		Info                        *DomainInfo
		Config                      *DomainConfig
		ReplicationConfig           *DomainReplicationConfig
		IsGlobalDomain              bool
		ConfigVersion               int64
		FailoverVersion             int64
		FailoverNotificationVersion int64
		NotificationVersion         int64
		TableVersion                int
	}

	// UpdateDomainRequest is used to update domain
	UpdateDomainRequest struct {
		Info                        *DomainInfo
		Config                      *DomainConfig
		ReplicationConfig           *DomainReplicationConfig
		ConfigVersion               int64
		FailoverVersion             int64
		FailoverNotificationVersion int64
		NotificationVersion         int64
		TableVersion                int
	}

	// DeleteDomainRequest is used to delete domain entry from domains table
	DeleteDomainRequest struct {
		ID string
	}

	// DeleteDomainByNameRequest is used to delete domain entry from domains_by_name table
	DeleteDomainByNameRequest struct {
		Name string
	}

	// ListDomainsRequest is used to list domains
	ListDomainsRequest struct {
		PageSize      int
		NextPageToken []byte
	}

	// ListDomainsResponse is the response for GetDomain
	ListDomainsResponse struct {
		Domains       []*GetDomainResponse
		NextPageToken []byte
	}

	// GetMetadataResponse is the response for GetMetadata
	GetMetadataResponse struct {
		NotificationVersion int64
	}

	// Closeable is an interface for any entity that supports a close operation to release resources
	Closeable interface {
		Close()
	}

	// ShardManager is used to manage all shards
	ShardManager interface {
		Closeable
		CreateShard(request *CreateShardRequest) error
		GetShard(request *GetShardRequest) (*GetShardResponse, error)
		UpdateShard(request *UpdateShardRequest) error
	}

	// ExecutionManager is used to manage workflow executions
	ExecutionManager interface {
		Closeable
		CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error
		ResetMutableState(request *ResetMutableStateRequest) error
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error)

		// Transfer task related methods
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error
		RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error

		// Replication task related methods
		GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error)
		CompleteReplicationTask(request *CompleteReplicationTaskRequest) error

		// Timer related methods.
		GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error)
		CompleteTimerTask(request *CompleteTimerTaskRequest) error
		RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error
	}

	// ExecutionManagerFactory creates an instance of ExecutionManager for a given shard
	ExecutionManagerFactory interface {
		Closeable
		CreateExecutionManager(shardID int) (ExecutionManager, error)
	}

	// TaskManager is used to manage tasks
	TaskManager interface {
		Closeable
		LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error)
		UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error)
		CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error)
		GetTasks(request *GetTasksRequest) (*GetTasksResponse, error)
		CompleteTask(request *CompleteTaskRequest) error
	}

	// HistoryManager is used to manage Workflow Execution HistoryEventBatch
	HistoryManager interface {
		Closeable
		AppendHistoryEvents(request *AppendHistoryEventsRequest) error
		// GetWorkflowExecutionHistory retrieves the paginated list of history events for given execution
		GetWorkflowExecutionHistory(request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponse,
			error)
		DeleteWorkflowExecutionHistory(request *DeleteWorkflowExecutionHistoryRequest) error
	}

	// MetadataManager is used to manage metadata CRUD for domain entities
	MetadataManager interface {
		Closeable
		CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error)
		GetDomain(request *GetDomainRequest) (*GetDomainResponse, error)
		UpdateDomain(request *UpdateDomainRequest) error
		DeleteDomain(request *DeleteDomainRequest) error
		DeleteDomainByName(request *DeleteDomainByNameRequest) error
		ListDomains(request *ListDomainsRequest) (*ListDomainsResponse, error)
		GetMetadata() (*GetMetadataResponse, error)
	}
)

func (e *ConditionFailedError) Error() string {
	return e.Msg
}

func (e *ShardAlreadyExistError) Error() string {
	return e.Msg
}

func (e *ShardOwnershipLostError) Error() string {
	return e.Msg
}

func (e *WorkflowExecutionAlreadyStartedError) Error() string {
	return e.Msg
}

func (e *TimeoutError) Error() string {
	return e.Msg
}

// GetType returns the type of the activity task
func (a *ActivityTask) GetType() int {
	return TransferTaskTypeActivityTask
}

// GetVersion returns the version of the activity task
func (a *ActivityTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the activity task
func (a *ActivityTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the activity task
func (a *ActivityTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the activity task
func (a *ActivityTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *ActivityTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *ActivityTask) SetVisibilityTimestamp(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

// GetType returns the type of the decision task
func (d *DecisionTask) GetType() int {
	return TransferTaskTypeDecisionTask
}

// GetVersion returns the version of the decision task
func (d *DecisionTask) GetVersion() int64 {
	return d.Version
}

// SetVersion returns the version of the decision task
func (d *DecisionTask) SetVersion(version int64) {
	d.Version = version
}

// GetTaskID returns the sequence ID of the decision task.
func (d *DecisionTask) GetTaskID() int64 {
	return d.TaskID
}

// SetTaskID sets the sequence ID of the decision task
func (d *DecisionTask) SetTaskID(id int64) {
	d.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (d *DecisionTask) GetVisibilityTimestamp() time.Time {
	return d.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (d *DecisionTask) SetVisibilityTimestamp(timestamp time.Time) {
	d.VisibilityTimestamp = timestamp
}

// GetType returns the type of the close execution task
func (a *CloseExecutionTask) GetType() int {
	return TransferTaskTypeCloseExecution
}

// GetVersion returns the version of the close execution task
func (a *CloseExecutionTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the close execution task
func (a *CloseExecutionTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the close execution task
func (a *CloseExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the close execution task
func (a *CloseExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *CloseExecutionTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *CloseExecutionTask) SetVisibilityTimestamp(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

// GetType returns the type of the delete execution task
func (a *DeleteHistoryEventTask) GetType() int {
	return TaskTypeDeleteHistoryEvent
}

// GetVersion returns the version of the delete execution task
func (a *DeleteHistoryEventTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the delete execution task
func (a *DeleteHistoryEventTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the delete execution task
func (a *DeleteHistoryEventTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the delete execution task
func (a *DeleteHistoryEventTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *DeleteHistoryEventTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *DeleteHistoryEventTask) SetVisibilityTimestamp(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

// GetType returns the type of the timer task
func (d *DecisionTimeoutTask) GetType() int {
	return TaskTypeDecisionTimeout
}

// GetVersion returns the version of the timer task
func (d *DecisionTimeoutTask) GetVersion() int64 {
	return d.Version
}

// SetVersion returns the version of the timer task
func (d *DecisionTimeoutTask) SetVersion(version int64) {
	d.Version = version
}

// GetTaskID returns the sequence ID.
func (d *DecisionTimeoutTask) GetTaskID() int64 {
	return d.TaskID
}

// SetTaskID sets the sequence ID.
func (d *DecisionTimeoutTask) SetTaskID(id int64) {
	d.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (d *DecisionTimeoutTask) GetVisibilityTimestamp() time.Time {
	return d.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (d *DecisionTimeoutTask) SetVisibilityTimestamp(t time.Time) {
	d.VisibilityTimestamp = t
}

// GetType returns the type of the timer task
func (a *ActivityTimeoutTask) GetType() int {
	return TaskTypeActivityTimeout
}

// GetVersion returns the version of the timer task
func (a *ActivityTimeoutTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the timer task
func (a *ActivityTimeoutTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID.
func (a *ActivityTimeoutTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID.
func (a *ActivityTimeoutTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (a *ActivityTimeoutTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (a *ActivityTimeoutTask) SetVisibilityTimestamp(t time.Time) {
	a.VisibilityTimestamp = t
}

// GetType returns the type of the timer task
func (u *UserTimerTask) GetType() int {
	return TaskTypeUserTimer
}

// GetVersion returns the version of the timer task
func (u *UserTimerTask) GetVersion() int64 {
	return u.Version
}

// SetVersion returns the version of the timer task
func (u *UserTimerTask) SetVersion(version int64) {
	u.Version = version
}

// GetTaskID returns the sequence ID of the timer task.
func (u *UserTimerTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the timer task.
func (u *UserTimerTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (u *UserTimerTask) GetVisibilityTimestamp() time.Time {
	return u.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (u *UserTimerTask) SetVisibilityTimestamp(t time.Time) {
	u.VisibilityTimestamp = t
}

// GetType returns the type of the retry timer task
func (r *ActivityRetryTimerTask) GetType() int {
	return TaskTypeActivityRetryTimer
}

// GetVersion returns the version of the retry timer task
func (r *ActivityRetryTimerTask) GetVersion() int64 {
	return r.Version
}

// SetVersion returns the version of the retry timer task
func (r *ActivityRetryTimerTask) SetVersion(version int64) {
	r.Version = version
}

// GetTaskID returns the sequence ID.
func (r *ActivityRetryTimerTask) GetTaskID() int64 {
	return r.TaskID
}

// SetTaskID sets the sequence ID.
func (r *ActivityRetryTimerTask) SetTaskID(id int64) {
	r.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (r *ActivityRetryTimerTask) GetVisibilityTimestamp() time.Time {
	return r.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (r *ActivityRetryTimerTask) SetVisibilityTimestamp(t time.Time) {
	r.VisibilityTimestamp = t
}

// GetType returns the type of the retry timer task
func (r *WorkflowRetryTimerTask) GetType() int {
	return TaskTypeWorkflowRetryTimer
}

// GetVersion returns the version of the retry timer task
func (r *WorkflowRetryTimerTask) GetVersion() int64 {
	return r.Version
}

// SetVersion returns the version of the retry timer task
func (r *WorkflowRetryTimerTask) SetVersion(version int64) {
	r.Version = version
}

// GetTaskID returns the sequence ID.
func (r *WorkflowRetryTimerTask) GetTaskID() int64 {
	return r.TaskID
}

// SetTaskID sets the sequence ID.
func (r *WorkflowRetryTimerTask) SetTaskID(id int64) {
	r.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (r *WorkflowRetryTimerTask) GetVisibilityTimestamp() time.Time {
	return r.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (r *WorkflowRetryTimerTask) SetVisibilityTimestamp(t time.Time) {
	r.VisibilityTimestamp = t
}

// GetType returns the type of the timeout task.
func (u *WorkflowTimeoutTask) GetType() int {
	return TaskTypeWorkflowTimeout
}

// GetVersion returns the version of the timeout task
func (u *WorkflowTimeoutTask) GetVersion() int64 {
	return u.Version
}

// SetVersion returns the version of the timeout task
func (u *WorkflowTimeoutTask) SetVersion(version int64) {
	u.Version = version
}

// GetTaskID returns the sequence ID of the cancel transfer task.
func (u *WorkflowTimeoutTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the cancel transfer task.
func (u *WorkflowTimeoutTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (u *WorkflowTimeoutTask) GetVisibilityTimestamp() time.Time {
	return u.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (u *WorkflowTimeoutTask) SetVisibilityTimestamp(t time.Time) {
	u.VisibilityTimestamp = t
}

// GetType returns the type of the cancel transfer task
func (u *CancelExecutionTask) GetType() int {
	return TransferTaskTypeCancelExecution
}

// GetVersion returns the version of the cancel transfer task
func (u *CancelExecutionTask) GetVersion() int64 {
	return u.Version
}

// SetVersion returns the version of the cancel transfer task
func (u *CancelExecutionTask) SetVersion(version int64) {
	u.Version = version
}

// GetTaskID returns the sequence ID of the cancel transfer task.
func (u *CancelExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the cancel transfer task.
func (u *CancelExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (u *CancelExecutionTask) GetVisibilityTimestamp() time.Time {
	return u.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (u *CancelExecutionTask) SetVisibilityTimestamp(timestamp time.Time) {
	u.VisibilityTimestamp = timestamp
}

// GetType returns the type of the signal transfer task
func (u *SignalExecutionTask) GetType() int {
	return TransferTaskTypeSignalExecution
}

// GetVersion returns the version of the signal transfer task
func (u *SignalExecutionTask) GetVersion() int64 {
	return u.Version
}

// SetVersion returns the version of the signal transfer task
func (u *SignalExecutionTask) SetVersion(version int64) {
	u.Version = version
}

// GetTaskID returns the sequence ID of the signal transfer task.
func (u *SignalExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the signal transfer task.
func (u *SignalExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (u *SignalExecutionTask) GetVisibilityTimestamp() time.Time {
	return u.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (u *SignalExecutionTask) SetVisibilityTimestamp(timestamp time.Time) {
	u.VisibilityTimestamp = timestamp
}

// GetType returns the type of the start child transfer task
func (u *StartChildExecutionTask) GetType() int {
	return TransferTaskTypeStartChildExecution
}

// GetVersion returns the version of the start child transfer task
func (u *StartChildExecutionTask) GetVersion() int64 {
	return u.Version
}

// SetVersion returns the version of the start child transfer task
func (u *StartChildExecutionTask) SetVersion(version int64) {
	u.Version = version
}

// GetTaskID returns the sequence ID of the start child transfer task
func (u *StartChildExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the start child transfer task
func (u *StartChildExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (u *StartChildExecutionTask) GetVisibilityTimestamp() time.Time {
	return u.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (u *StartChildExecutionTask) SetVisibilityTimestamp(timestamp time.Time) {
	u.VisibilityTimestamp = timestamp
}

// GetType returns the type of the history replication task
func (a *HistoryReplicationTask) GetType() int {
	return ReplicationTaskTypeHistory
}

// GetVersion returns the version of the history replication task
func (a *HistoryReplicationTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the history replication task
func (a *HistoryReplicationTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the history replication task
func (a *HistoryReplicationTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the history replication task
func (a *HistoryReplicationTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *HistoryReplicationTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *HistoryReplicationTask) SetVisibilityTimestamp(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

// GetTaskID returns the task ID for transfer task
func (t *TransferTaskInfo) GetTaskID() int64 {
	return t.TaskID
}

// GetVersion returns the task version for transfer task
func (t *TransferTaskInfo) GetVersion() int64 {
	return t.Version
}

// GetTaskType returns the task type for transfer task
func (t *TransferTaskInfo) GetTaskType() int {
	return t.TaskType
}

// GetVisibilityTimestamp returns the task type for transfer task
func (t *TransferTaskInfo) GetVisibilityTimestamp() time.Time {
	return t.VisibilityTimestamp
}

// String returns string
func (t *TransferTaskInfo) String() string {
	return fmt.Sprintf(
		"{DomainID: %v, WorkflowID: %v, RunID: %v, TaskID: %v, TargetDomainID: %v, TargetWorkflowID %v, TargetRunID: %v, TargetChildWorkflowOnly: %v, TaskList: %v, TaskType: %v, ScheduleID: %v, Version: %v.}",
		t.DomainID, t.WorkflowID, t.RunID, t.TaskID, t.TargetDomainID, t.TargetWorkflowID, t.TargetRunID, t.TargetChildWorkflowOnly, t.TaskList, t.TaskType, t.ScheduleID, t.Version,
	)
}

// GetTaskID returns the task ID for replication task
func (t *ReplicationTaskInfo) GetTaskID() int64 {
	return t.TaskID
}

// GetVersion returns the task version for replication task
func (t *ReplicationTaskInfo) GetVersion() int64 {
	return t.Version
}

// GetTaskType returns the task type for replication task
func (t *ReplicationTaskInfo) GetTaskType() int {
	return t.TaskType
}

// GetVisibilityTimestamp returns the task type for transfer task
func (t *ReplicationTaskInfo) GetVisibilityTimestamp() time.Time {
	return time.Time{}
}

// GetTaskID returns the task ID for timer task
func (t *TimerTaskInfo) GetTaskID() int64 {
	return t.TaskID
}

// GetVersion returns the task version for timer task
func (t *TimerTaskInfo) GetVersion() int64 {
	return t.Version
}

// GetTaskType returns the task type for timer task
func (t *TimerTaskInfo) GetTaskType() int {
	return t.TaskType
}

// GetVisibilityTimestamp returns the task type for transfer task
func (t *TimerTaskInfo) GetVisibilityTimestamp() time.Time {
	return t.VisibilityTimestamp
}

// GetTaskType returns the task type for timer task
func (t *TimerTaskInfo) String() string {
	return fmt.Sprintf(
		"{DomainID: %v, WorkflowID: %v, RunID: %v, VisibilityTimestamp: %v, TaskID: %v, TaskType: %v, TimeoutType: %v, EventID: %v, ScheduleAttempt: %v, Version: %v.}",
		t.DomainID, t.WorkflowID, t.RunID, t.VisibilityTimestamp, t.TaskID, t.TaskType, t.TimeoutType, t.EventID, t.ScheduleAttempt, t.Version,
	)
}

// NewHistoryEventBatch returns a new instance of HistoryEventBatch
func NewHistoryEventBatch(version int, events []*workflow.HistoryEvent) *HistoryEventBatch {
	return &HistoryEventBatch{
		Version: version,
		Events:  events,
	}
}

func (b *HistoryEventBatch) String() string {
	return fmt.Sprintf("[version:%v, events:%v]", b.Version, b.Events)
}

// NewSerializedHistoryEventBatch constructs and returns a new instance of of SerializedHistoryEventBatch
func NewSerializedHistoryEventBatch(data []byte, encoding common.EncodingType, version int) *SerializedHistoryEventBatch {
	return &SerializedHistoryEventBatch{
		EncodingType: encoding,
		Version:      version,
		Data:         data,
	}
}

func (h *SerializedHistoryEventBatch) String() string {
	return fmt.Sprintf("[encodingType:%v,historyVersion:%v,history:%v]",
		h.EncodingType, h.Version, string(h.Data))
}

func (config *ClusterReplicationConfig) serialize() map[string]interface{} {
	output := make(map[string]interface{})
	output["cluster_name"] = config.ClusterName
	return output
}

func (config *ClusterReplicationConfig) deserialize(input map[string]interface{}) {
	config.ClusterName = input["cluster_name"].(string)
}

// SetSerializedHistoryDefaults  sets the version and encoding types to defaults if they
// are missing from persistence. This is purely for backwards compatibility
func SetSerializedHistoryDefaults(history *SerializedHistoryEventBatch) {
	if history.Version == 0 {
		history.Version = GetDefaultHistoryVersion()
	}
	if len(history.EncodingType) == 0 {
		history.EncodingType = DefaultEncodingType
	}
}
