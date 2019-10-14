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
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/replicator"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
)

// Domain status
const (
	DomainStatusRegistered = iota
	DomainStatusDeprecated
	DomainStatusDeleted
)

// CreateWorkflowMode workflow creation mode
type CreateWorkflowMode int

// Create Workflow Execution Mode
const (
	// Fail if current record exists
	// Only applicable for CreateWorkflowExecution
	CreateWorkflowModeBrandNew CreateWorkflowMode = iota
	// Update current record only if workflow is closed
	// Only applicable for CreateWorkflowExecution
	CreateWorkflowModeWorkflowIDReuse
	// Update current record only if workflow is open
	// Only applicable for UpdateWorkflowExecution
	CreateWorkflowModeContinueAsNew
	// Do not update current record since workflow to
	// applicable for CreateWorkflowExecution, UpdateWorkflowExecution
	CreateWorkflowModeZombie
)

// UpdateWorkflowMode update mode
type UpdateWorkflowMode int

// Update Workflow Execution Mode
const (
	// Update workflow, including current record
	// NOTE: update on current record is a condition update
	UpdateWorkflowModeUpdateCurrent UpdateWorkflowMode = iota
	// Update workflow, without current record
	// NOTE: current record CANNOT point to the workflow to be updated
	UpdateWorkflowModeBypassCurrent
)

// ConflictResolveWorkflowMode conflict resolve mode
type ConflictResolveWorkflowMode int

// Conflict Resolve Workflow Mode
const (
	// Conflict resolve workflow, including current record
	// NOTE: update on current record is a condition update
	ConflictResolveWorkflowModeUpdateCurrent ConflictResolveWorkflowMode = iota
	// Conflict resolve workflow, without current record
	// NOTE: current record CANNOT point to the workflow to be updated
	ConflictResolveWorkflowModeBypassCurrent
)

// Workflow execution states
const (
	WorkflowStateCreated = iota
	WorkflowStateRunning
	WorkflowStateCompleted
	WorkflowStateZombie
	WorkflowStateVoid
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
	TransferTaskTypeRecordWorkflowStarted
	TransferTaskTypeResetWorkflow
	TransferTaskTypeUpsertWorkflowSearchAttributes
)

// Types of replication tasks
const (
	ReplicationTaskTypeHistory = iota
	ReplicationTaskTypeSyncActivity
)

// Types of timers
const (
	TaskTypeDecisionTimeout = iota
	TaskTypeActivityTimeout
	TaskTypeUserTimer
	TaskTypeWorkflowTimeout
	TaskTypeDeleteHistoryEvent
	TaskTypeActivityRetryTimer
	TaskTypeWorkflowBackoffTimer
)

// UnknownNumRowsAffected is returned when the number of rows that an API affected cannot be determined
const UnknownNumRowsAffected = -1

// Types of workflow backoff timeout
const (
	WorkflowBackoffTimeoutTypeRetry = iota
	WorkflowBackoffTimeoutTypeCron
)

const (
	// InitialFailoverNotificationVersion is the initial failover version for a domain
	InitialFailoverNotificationVersion int64 = 0

	// TransferTaskTransferTargetWorkflowID is the the dummy workflow ID for transfer tasks of types
	// that do not have a target workflow
	TransferTaskTransferTargetWorkflowID = "20000000-0000-f000-f000-000000000001"
	// TransferTaskTransferTargetRunID is the the dummy run ID for transfer tasks of types
	// that do not have a target workflow
	TransferTaskTransferTargetRunID = "30000000-0000-f000-f000-000000000002"

	// indicate invalid workflow state transition
	invalidStateTransitionMsg = "unable to change workflow state from %v to %v, close status %v"
)

const numItemsInGarbageInfo = 3

type (
	// InvalidPersistenceRequestError represents invalid request to persistence
	InvalidPersistenceRequestError struct {
		Msg string
	}

	// CurrentWorkflowConditionFailedError represents a failed conditional update for current workflow record
	CurrentWorkflowConditionFailedError struct {
		Msg string
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
		ShardID int
		Msg     string
	}

	// WorkflowExecutionAlreadyStartedError is returned when creating a new workflow failed.
	WorkflowExecutionAlreadyStartedError struct {
		Msg              string
		StartRequestID   string
		RunID            string
		State            int
		CloseStatus      int
		LastWriteVersion int64
	}

	// TimeoutError is returned when a write operation fails due to a timeout
	TimeoutError struct {
		Msg string
	}

	// TransactionSizeLimitError is returned when the transaction size is too large
	TransactionSizeLimitError struct {
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
		ClusterReplicationLevel   map[string]int64                 // cluster -> last replicated taskID
		DomainNotificationVersion int64
	}

	// TransferFailoverLevel contains corresponding start / end level
	TransferFailoverLevel struct {
		StartTime    time.Time
		MinLevel     int64
		CurrentLevel int64
		MaxLevel     int64
		DomainIDs    map[string]struct{}
	}

	// TimerFailoverLevel contains domain IDs and corresponding start / end level
	TimerFailoverLevel struct {
		StartTime    time.Time
		MinLevel     time.Time
		CurrentLevel time.Time
		MaxLevel     time.Time
		DomainIDs    map[string]struct{}
	}

	// WorkflowExecutionInfo describes a workflow execution
	WorkflowExecutionInfo struct {
		DomainID                           string
		WorkflowID                         string
		RunID                              string
		ParentDomainID                     string
		ParentWorkflowID                   string
		ParentRunID                        string
		InitiatedID                        int64
		CompletionEventBatchID             int64
		CompletionEvent                    *workflow.HistoryEvent
		TaskList                           string
		WorkflowTypeName                   string
		WorkflowTimeout                    int32
		DecisionTimeoutValue               int32
		ExecutionContext                   []byte
		State                              int
		CloseStatus                        int
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
		StickyTaskList                     string
		StickyScheduleToStartTimeout       int32
		ClientLibraryVersion               string
		ClientFeatureVersion               string
		ClientImpl                         string
		AutoResetPoints                    *workflow.ResetPoints
		Memo                               map[string][]byte
		SearchAttributes                   map[string][]byte
		// for retry
		Attempt            int32
		HasRetryPolicy     bool
		InitialInterval    int32
		BackoffCoefficient float64
		MaximumInterval    int32
		ExpirationTime     time.Time
		MaximumAttempts    int32
		NonRetriableErrors []string
		BranchToken        []byte
		// Cron
		CronSchedule      string
		ExpirationSeconds int32
	}

	// ExecutionStats is the statistics about workflow execution
	ExecutionStats struct {
		HistorySize int64
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
		RecordVisibility        bool
	}

	// ReplicationTaskInfo describes the replication task created for replication of history events
	ReplicationTaskInfo struct {
		DomainID          string
		WorkflowID        string
		RunID             string
		TaskID            int64
		TaskType          int
		FirstEventID      int64
		NextEventID       int64
		Version           int64
		ScheduledID       int64
		BranchToken       []byte
		NewRunBranchToken []byte
		ResetWorkflow     bool

		// TODO deprecate when NDC is fully released && migrated
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
		DomainID    string
		Name        string
		TaskType    int
		RangeID     int64
		AckLevel    int64
		Kind        int
		Expiry      time.Time
		LastUpdated time.Time
	}

	// TaskInfo describes either activity or decision task
	TaskInfo struct {
		DomainID               string
		WorkflowID             string
		RunID                  string
		TaskID                 int64
		ScheduleID             int64
		ScheduleToStartTimeout int32
		Expiry                 time.Time
		CreatedTime            time.Time
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
		RecordVisibility    bool
	}

	// RecordWorkflowStartedTask identifites a transfer task for writing visibility open execution record
	RecordWorkflowStartedTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}

	// ResetWorkflowTask identifites a transfer task to reset workflow
	ResetWorkflowTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
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

	// UpsertWorkflowSearchAttributesTask identifies a transfer task for upsert search attributes
	UpsertWorkflowSearchAttributesTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		// this version is not used by task processing for validation,
		// instead, the version is used by elastic search
		Version int64
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

	// WorkflowBackoffTimerTask to schedule first decision task for retried workflow
	WorkflowBackoffTimerTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64 // TODO this attribute is not used?
		Version             int64
		TimeoutType         int // 0 for retry, 1 for cron.
	}

	// HistoryReplicationTask is the replication task created for shipping history replication events to other clusters
	HistoryReplicationTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		FirstEventID        int64
		NextEventID         int64
		Version             int64
		BranchToken         []byte
		NewRunBranchToken   []byte

		// TODO when 2DC is deprecated remove these 2 attributes
		ResetWorkflow       bool
		LastReplicationInfo map[string]*ReplicationInfo
	}

	// SyncActivityTask is the replication task created for shipping activity info to other clusters
	SyncActivityTask struct {
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		ScheduledID         int64
	}

	// ReplicationInfo represents the information stored for last replication event details per cluster
	ReplicationInfo struct {
		Version     int64
		LastEventID int64
	}

	// VersionHistoryItem contains the event id and the associated version
	VersionHistoryItem struct {
		EventID int64
		Version int64
	}

	// VersionHistory provides operations on version history
	VersionHistory struct {
		BranchToken []byte
		Items       []*VersionHistoryItem
	}

	// VersionHistories contains a set of VersionHistory
	VersionHistories struct {
		CurrentVersionHistoryIndex int
		Histories                  []*VersionHistory
	}

	// WorkflowMutableState indicates workflow related state
	WorkflowMutableState struct {
		ActivityInfos       map[int64]*ActivityInfo
		TimerInfos          map[string]*TimerInfo
		ChildExecutionInfos map[int64]*ChildExecutionInfo
		RequestCancelInfos  map[int64]*RequestCancelInfo
		SignalInfos         map[int64]*SignalInfo
		SignalRequestedIDs  map[string]struct{}
		ExecutionInfo       *WorkflowExecutionInfo
		ExecutionStats      *ExecutionStats
		ReplicationState    *ReplicationState
		BufferedEvents      []*workflow.HistoryEvent
		VersionHistories    *VersionHistories
	}

	// ActivityInfo details.
	ActivityInfo struct {
		Version                  int64
		ScheduleID               int64
		ScheduledEventBatchID    int64
		ScheduledEvent           *workflow.HistoryEvent
		ScheduledTime            time.Time
		StartedID                int64
		StartedEvent             *workflow.HistoryEvent
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
		LastFailureReason  string
		LastWorkerIdentity string
		LastFailureDetails []byte
		// Not written to database - This is used only for deduping heartbeat timer creation
		LastHeartbeatTimeoutVisibility int64
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
		Version               int64
		InitiatedID           int64
		InitiatedEventBatchID int64
		InitiatedEvent        *workflow.HistoryEvent
		StartedID             int64
		StartedWorkflowID     string
		StartedRunID          string
		StartedEvent          *workflow.HistoryEvent
		CreateRequestID       string
		DomainName            string
		WorkflowTypeName      string
		ParentClosePolicy     workflow.ParentClosePolicy
	}

	// RequestCancelInfo has details for pending external workflow cancellations
	RequestCancelInfo struct {
		Version               int64
		InitiatedEventBatchID int64
		InitiatedID           int64
		CancelRequestID       string
	}

	// SignalInfo has details for pending external workflow signal
	SignalInfo struct {
		Version               int64
		InitiatedEventBatchID int64
		InitiatedID           int64
		SignalRequestID       string
		SignalName            string
		Input                 []byte
		Control               []byte
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
		RangeID int64

		Mode CreateWorkflowMode

		PreviousRunID            string
		PreviousLastWriteVersion int64

		NewWorkflowSnapshot WorkflowSnapshot
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
		State             *WorkflowMutableState
		MutableStateStats *MutableStateStats
	}

	// GetCurrentExecutionRequest is used to retrieve the current RunId for an execution
	GetCurrentExecutionRequest struct {
		DomainID   string
		WorkflowID string
	}

	// GetCurrentExecutionResponse is the response to GetCurrentExecution
	GetCurrentExecutionResponse struct {
		StartRequestID   string
		RunID            string
		State            int
		CloseStatus      int
		LastWriteVersion int64
	}

	// UpdateWorkflowExecutionRequest is used to update a workflow execution
	UpdateWorkflowExecutionRequest struct {
		RangeID int64

		Mode UpdateWorkflowMode

		UpdateWorkflowMutation WorkflowMutation

		NewWorkflowSnapshot *WorkflowSnapshot

		Encoding common.EncodingType // optional binary encoding type
	}

	// ConflictResolveWorkflowExecutionRequest is used to reset workflow execution state for a single run
	ConflictResolveWorkflowExecutionRequest struct {
		RangeID int64

		Mode ConflictResolveWorkflowMode

		// workflow to be resetted
		ResetWorkflowSnapshot WorkflowSnapshot

		// maybe new workflow
		NewWorkflowSnapshot *WorkflowSnapshot

		// current workflow
		CurrentWorkflowMutation *WorkflowMutation

		// TODO deprecate this once nDC migration is completed
		//  basically should use CurrentWorkflowMutation instead
		CurrentWorkflowCAS *CurrentWorkflowCAS

		Encoding common.EncodingType // optional binary encoding type
	}

	// CurrentWorkflowCAS represent a compare and swap on current record
	// TODO deprecate this once nDC migration is completed
	CurrentWorkflowCAS struct {
		PrevRunID            string
		PrevLastWriteVersion int64
		PrevState            int
	}

	// ResetWorkflowExecutionRequest is used to reset workflow execution state for current run and create new run
	ResetWorkflowExecutionRequest struct {
		RangeID int64

		// for base run (we need to make sure the baseRun hasn't been deleted after forking)
		BaseRunID          string
		BaseRunNextEventID int64

		// for current workflow record
		CurrentRunID          string
		CurrentRunNextEventID int64

		// for current mutable state
		CurrentWorkflowMutation *WorkflowMutation

		// For new mutable state
		NewWorkflowSnapshot WorkflowSnapshot

		Encoding common.EncodingType // optional binary encoding type
	}

	// WorkflowEvents is used as generic workflow history events transaction container
	WorkflowEvents struct {
		DomainID    string
		WorkflowID  string
		RunID       string
		BranchToken []byte
		Events      []*workflow.HistoryEvent
	}

	// WorkflowMutation is used as generic workflow execution state mutation
	WorkflowMutation struct {
		ExecutionInfo    *WorkflowExecutionInfo
		ExecutionStats   *ExecutionStats
		ReplicationState *ReplicationState
		VersionHistories *VersionHistories

		UpsertActivityInfos       []*ActivityInfo
		DeleteActivityInfos       []int64
		UpsertTimerInfos          []*TimerInfo
		DeleteTimerInfos          []string
		UpsertChildExecutionInfos []*ChildExecutionInfo
		DeleteChildExecutionInfo  *int64
		UpsertRequestCancelInfos  []*RequestCancelInfo
		DeleteRequestCancelInfo   *int64
		UpsertSignalInfos         []*SignalInfo
		DeleteSignalInfo          *int64
		UpsertSignalRequestedIDs  []string
		DeleteSignalRequestedID   string
		NewBufferedEvents         []*workflow.HistoryEvent
		ClearBufferedEvents       bool

		TransferTasks    []Task
		ReplicationTasks []Task
		TimerTasks       []Task

		Condition int64
	}

	// WorkflowSnapshot is used as generic workflow execution state snapshot
	WorkflowSnapshot struct {
		ExecutionInfo    *WorkflowExecutionInfo
		ExecutionStats   *ExecutionStats
		ReplicationState *ReplicationState
		VersionHistories *VersionHistories

		ActivityInfos       []*ActivityInfo
		TimerInfos          []*TimerInfo
		ChildExecutionInfos []*ChildExecutionInfo
		RequestCancelInfos  []*RequestCancelInfo
		SignalInfos         []*SignalInfo
		SignalRequestedIDs  []string

		TransferTasks    []Task
		ReplicationTasks []Task
		TimerTasks       []Task

		Condition int64
	}

	// DeleteWorkflowExecutionRequest is used to delete a workflow execution
	DeleteWorkflowExecutionRequest struct {
		DomainID   string
		WorkflowID string
		RunID      string
	}

	// DeleteTaskRequest is used to detele a task that corrupted and need to be removed
	// 	e.g. corrupted history event batch, eventID is not continouous
	DeleteTaskRequest struct {
		TaskID  int64
		Type    int
		ShardID int
	}

	// DeleteCurrentWorkflowExecutionRequest is used to delete the current workflow execution
	DeleteCurrentWorkflowExecutionRequest struct {
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
		RangeID      int64
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

	// ListTaskListRequest contains the request params needed to invoke ListTaskList API
	ListTaskListRequest struct {
		PageSize  int
		PageToken []byte
	}

	// ListTaskListResponse is the response from ListTaskList API
	ListTaskListResponse struct {
		Items         []TaskListInfo
		NextPageToken []byte
	}

	// DeleteTaskListRequest contains the request params needed to invoke DeleteTaskList API
	DeleteTaskListRequest struct {
		DomainID     string
		TaskListName string
		TaskListType int
		RangeID      int64
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
		ReadLevel    int64  // range exclusive
		MaxReadLevel *int64 // optional: range inclusive when specified
		BatchSize    int
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

	// CompleteTasksLessThanRequest contains the request params needed to invoke CompleteTasksLessThan API
	CompleteTasksLessThanRequest struct {
		DomainID     string
		TaskListName string
		TaskType     int
		TaskID       int64 // Tasks less than or equal to this ID will be completed
		Limit        int   // Limit on the max number of tasks that can be completed. Required param
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

	// AppendHistoryEventsRequest is used to append new events to workflow execution history
	// Deprecated: use v2 API-AppendHistoryNodes() instead
	AppendHistoryEventsRequest struct {
		DomainID          string
		Execution         workflow.WorkflowExecution
		FirstEventID      int64
		EventBatchVersion int64
		RangeID           int64
		TransactionID     int64
		Events            []*workflow.HistoryEvent
		Overwrite         bool
		Encoding          common.EncodingType // optional binary encoding type
	}

	// GetWorkflowExecutionHistoryRequest is used to retrieve history of a workflow execution
	// Deprecated: use v2 API-AppendHistoryNodes() instead
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
	// Deprecated: use V2 API instead-ReadHistoryBranch()
	GetWorkflowExecutionHistoryResponse struct {
		History *workflow.History
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on GetWorkflowExecutionHistoryRequest to read the next page.
		NextPageToken []byte
		// the first_event_id of last loaded batch
		LastFirstEventID int64
		// Size of history read from store
		Size int
	}

	// GetWorkflowExecutionHistoryByBatchResponse is the response to GetWorkflowExecutionHistoryRequest
	// Deprecated: use V2 API instead-ReadHistoryBranchByBatch()
	GetWorkflowExecutionHistoryByBatchResponse struct {
		History []*workflow.History
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on GetWorkflowExecutionHistoryRequest to read the next page.
		NextPageToken []byte
		// the first_event_id of last loaded batch
		LastFirstEventID int64
		// Size of history read from store
		Size int
	}

	// DeleteWorkflowExecutionHistoryRequest is used to delete workflow execution history
	// Deprecated: use v2 API-AppendHistoryNodes() instead
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
		Retention                int32
		EmitMetric               bool
		HistoryArchivalStatus    workflow.ArchivalStatus
		HistoryArchivalURI       string
		VisibilityArchivalStatus workflow.ArchivalStatus
		VisibilityArchivalURI    string
		BadBinaries              workflow.BadBinaries
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

	// MutableStateStats is the size stats for MutableState
	MutableStateStats struct {
		// Total size of mutable state
		MutableStateSize int

		// Breakdown of size into more granular stats
		ExecutionInfoSize  int
		ActivityInfoSize   int
		TimerInfoSize      int
		ChildInfoSize      int
		SignalInfoSize     int
		BufferedEventsSize int

		// Item count for various information captured within mutable state
		ActivityInfoCount      int
		TimerInfoCount         int
		ChildInfoCount         int
		SignalInfoCount        int
		RequestCancelInfoCount int
		BufferedEventsCount    int
	}

	// MutableStateUpdateSessionStats is size stats for mutableState updating session
	MutableStateUpdateSessionStats struct {
		MutableStateSize int // Total size of mutable state update

		// Breakdown of mutable state size update for more granular stats
		ExecutionInfoSize  int
		ActivityInfoSize   int
		TimerInfoSize      int
		ChildInfoSize      int
		SignalInfoSize     int
		BufferedEventsSize int

		// Item counts in this session update
		ActivityInfoCount      int
		TimerInfoCount         int
		ChildInfoCount         int
		SignalInfoCount        int
		RequestCancelInfoCount int

		// Deleted item counts in this session update
		DeleteActivityInfoCount      int
		DeleteTimerInfoCount         int
		DeleteChildInfoCount         int
		DeleteSignalInfoCount        int
		DeleteRequestCancelInfoCount int
	}

	// UpdateWorkflowExecutionResponse is response for UpdateWorkflowExecutionRequest
	UpdateWorkflowExecutionResponse struct {
		MutableStateUpdateSessionStats *MutableStateUpdateSessionStats
	}

	// AppendHistoryNodesRequest is used to append a batch of history nodes
	AppendHistoryNodesRequest struct {
		// true if this is the first append request to the branch
		IsNewBranch bool
		// the info for clean up data in background
		Info string
		// The branch to be appended
		BranchToken []byte
		// The batch of events to be appended. The first eventID will become the nodeID of this batch
		Events []*workflow.HistoryEvent
		// requested TransactionID for this write operation. For the same eventID, the node with larger TransactionID always wins
		TransactionID int64
		// optional binary encoding type
		Encoding common.EncodingType
		// The shard to get history node data
		ShardID *int
	}

	// AppendHistoryNodesResponse is a response to AppendHistoryNodesRequest
	AppendHistoryNodesResponse struct {
		// the size of the event data that has been appended
		Size int
	}

	// ReadHistoryBranchRequest is used to read a history branch
	ReadHistoryBranchRequest struct {
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
		// The shard to get history branch data
		ShardID *int
	}

	// ReadHistoryBranchResponse is the response to ReadHistoryBranchRequest
	ReadHistoryBranchResponse struct {
		// History events
		HistoryEvents []*workflow.HistoryEvent
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on ReadHistoryBranchRequest to read the next page.
		// Empty means we have reached the last page, not need to continue
		NextPageToken []byte
		// Size of history read from store
		Size int
		// the first_event_id of last loaded batch
		LastFirstEventID int64
	}

	// ReadHistoryBranchByBatchResponse is the response to ReadHistoryBranchRequest
	ReadHistoryBranchByBatchResponse struct {
		// History events by batch
		History []*workflow.History
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on ReadHistoryBranchRequest to read the next page.
		// Empty means we have reached the last page, not need to continue
		NextPageToken []byte
		// Size of history read from store
		Size int
		// the first_event_id of last loaded batch
		LastFirstEventID int64
	}

	// ReadRawHistoryBranchResponse is the response to ReadHistoryBranchRequest
	ReadRawHistoryBranchResponse struct {
		// HistoryEventBlobs history event blobs
		HistoryEventBlobs []*DataBlob
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on ReadHistoryBranchRequest to read the next page.
		// Empty means we have reached the last page, not need to continue
		NextPageToken []byte
		// Size of history read from store
		Size int
	}

	// ForkHistoryBranchRequest is used to fork a history branch
	ForkHistoryBranchRequest struct {
		// The base branch to fork from
		ForkBranchToken []byte
		// The nodeID to fork from, the new branch will start from ( inclusive ), the base branch will stop at(exclusive)
		// Application must provide a void forking nodeID, it must be a valid nodeID in that branch. A valid nodeID is the firstEventID of a valid batch of events.
		// And ForkNodeID > 1 because forking from 1 doesn't make any sense.
		ForkNodeID int64
		// the info for clean up data in background
		Info string
		// The shard to get history branch data
		ShardID *int
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
		// branch to be deleted
		BranchToken []byte
		// The shard to delete history branch data
		ShardID *int
	}

	// GetHistoryTreeRequest is used to retrieve branch info of a history tree
	GetHistoryTreeRequest struct {
		// A UUID of a tree
		TreeID string
		// Get data from this shard
		ShardID *int
		// optional: can provide treeID via branchToken if treeID is empty
		BranchToken []byte
	}

	// HistoryBranchDetail contains detailed information of a branch
	HistoryBranchDetail struct {
		TreeID   string
		BranchID string
		ForkTime time.Time
		Info     string
	}

	// GetHistoryTreeResponse is a response to GetHistoryTreeRequest
	GetHistoryTreeResponse struct {
		// all branches of a tree
		Branches                  []*workflow.HistoryBranch
		ForkingInProgressBranches []HistoryBranchDetail
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

	// AppendHistoryEventsResponse is response for AppendHistoryEventsRequest
	// Deprecated: uses V2 API-AppendHistoryNodesRequest
	AppendHistoryEventsResponse struct {
		Size int
	}

	// Closeable is an interface for any entity that supports a close operation to release resources
	Closeable interface {
		Close()
	}

	// ShardManager is used to manage all shards
	ShardManager interface {
		Closeable
		GetName() string
		CreateShard(request *CreateShardRequest) error
		GetShard(request *GetShardRequest) (*GetShardResponse, error)
		UpdateShard(request *UpdateShardRequest) error
	}

	// ExecutionManager is used to manage workflow executions
	ExecutionManager interface {
		Closeable
		GetName() string
		GetShardID() int

		CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) (*UpdateWorkflowExecutionResponse, error)
		ConflictResolveWorkflowExecution(request *ConflictResolveWorkflowExecutionRequest) error
		ResetWorkflowExecution(request *ResetWorkflowExecutionRequest) error
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		DeleteCurrentWorkflowExecution(request *DeleteCurrentWorkflowExecutionRequest) error
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

		// Remove Task due to corrupted data
		DeleteTask(request *DeleteTaskRequest) error
	}

	// ExecutionManagerFactory creates an instance of ExecutionManager for a given shard
	ExecutionManagerFactory interface {
		Closeable
		NewExecutionManager(shardID int) (ExecutionManager, error)
	}

	// TaskManager is used to manage tasks
	TaskManager interface {
		Closeable
		GetName() string
		LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error)
		UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error)
		ListTaskList(request *ListTaskListRequest) (*ListTaskListResponse, error)
		DeleteTaskList(request *DeleteTaskListRequest) error
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

	// HistoryV2Manager is used to manager workflow history events
	HistoryV2Manager interface {
		Closeable
		GetName() string

		// The below are history V2 APIs
		// V2 regards history events growing as a tree, decoupled from workflow concepts
		// For Cadence, treeID is new runID, except for fork(reset), treeID will be the runID that it forks from.

		// AppendHistoryNodes add(or override) a batch of nodes to a history branch
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
		// CompleteForkBranch will complete the forking process after update mutableState, this is to help preventing data leakage
		CompleteForkBranch(request *CompleteForkBranchRequest) error
		// DeleteHistoryBranch removes a branch
		// If this is the last branch to delete, it will also remove the root node
		DeleteHistoryBranch(request *DeleteHistoryBranchRequest) error
		// GetHistoryTree returns all branch information of a tree
		GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error)
		// GetAllHistoryTreeBranches returns all branches of all trees
		GetAllHistoryTreeBranches(request *GetAllHistoryTreeBranchesRequest) (*GetAllHistoryTreeBranchesResponse, error)
	}

	// MetadataManager is used to manage metadata CRUD for domain entities
	MetadataManager interface {
		Closeable
		GetName() string
		CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error)
		GetDomain(request *GetDomainRequest) (*GetDomainResponse, error)
		UpdateDomain(request *UpdateDomainRequest) error
		DeleteDomain(request *DeleteDomainRequest) error
		DeleteDomainByName(request *DeleteDomainByNameRequest) error
		ListDomains(request *ListDomainsRequest) (*ListDomainsResponse, error)
		GetMetadata() (*GetMetadataResponse, error)
	}

	// DomainReplicationQueue is used to publish and list domain replication tasks
	DomainReplicationQueue interface {
		Closeable
		Publish(message interface{}) error
		GetReplicationMessages(lastMessageID int, maxCount int) ([]*replicator.ReplicationTask, int, error)
		UpdateAckLevel(lastProcessedMessageID int, clusterName string) error
		GetAckLevels() (map[string]int, error)
	}
)

func (e *InvalidPersistenceRequestError) Error() string {
	return e.Msg
}

func (e *CurrentWorkflowConditionFailedError) Error() string {
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

func (e *WorkflowExecutionAlreadyStartedError) Error() string {
	return e.Msg
}

func (e *TimeoutError) Error() string {
	return e.Msg
}

func (e *TransactionSizeLimitError) Error() string {
	return e.Msg
}

// IsTimeoutError check whether error is TimeoutError
func IsTimeoutError(err error) bool {
	_, ok := err.(*TimeoutError)
	return ok
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

// GetType returns the type of the record workflow started task
func (a *RecordWorkflowStartedTask) GetType() int {
	return TransferTaskTypeRecordWorkflowStarted
}

// GetVersion returns the version of the record workflow started task
func (a *RecordWorkflowStartedTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the record workflow started task
func (a *RecordWorkflowStartedTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the record workflow started task
func (a *RecordWorkflowStartedTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the record workflow started task
func (a *RecordWorkflowStartedTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *RecordWorkflowStartedTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *RecordWorkflowStartedTask) SetVisibilityTimestamp(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

// GetType returns the type of the ResetWorkflowTask
func (a *ResetWorkflowTask) GetType() int {
	return TransferTaskTypeResetWorkflow
}

// GetVersion returns the version of the ResetWorkflowTask
func (a *ResetWorkflowTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the ResetWorkflowTask
func (a *ResetWorkflowTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the ResetWorkflowTask
func (a *ResetWorkflowTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the ResetWorkflowTask
func (a *ResetWorkflowTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *ResetWorkflowTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *ResetWorkflowTask) SetVisibilityTimestamp(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
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
func (r *WorkflowBackoffTimerTask) GetType() int {
	return TaskTypeWorkflowBackoffTimer
}

// GetVersion returns the version of the retry timer task
func (r *WorkflowBackoffTimerTask) GetVersion() int64 {
	return r.Version
}

// SetVersion returns the version of the retry timer task
func (r *WorkflowBackoffTimerTask) SetVersion(version int64) {
	r.Version = version
}

// GetTaskID returns the sequence ID.
func (r *WorkflowBackoffTimerTask) GetTaskID() int64 {
	return r.TaskID
}

// SetTaskID sets the sequence ID.
func (r *WorkflowBackoffTimerTask) SetTaskID(id int64) {
	r.TaskID = id
}

// GetVisibilityTimestamp gets the visibility time stamp
func (r *WorkflowBackoffTimerTask) GetVisibilityTimestamp() time.Time {
	return r.VisibilityTimestamp
}

// SetVisibilityTimestamp gets the visibility time stamp
func (r *WorkflowBackoffTimerTask) SetVisibilityTimestamp(t time.Time) {
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

// GetType returns the type of the upsert search attributes transfer task
func (u *UpsertWorkflowSearchAttributesTask) GetType() int {
	return TransferTaskTypeUpsertWorkflowSearchAttributes
}

// GetVersion returns the version of the upsert search attributes transfer task
func (u *UpsertWorkflowSearchAttributesTask) GetVersion() int64 {
	return u.Version
}

// SetVersion returns the version of the upsert search attributes transfer task
func (u *UpsertWorkflowSearchAttributesTask) SetVersion(version int64) {
	u.Version = version
}

// GetTaskID returns the sequence ID of the signal transfer task.
func (u *UpsertWorkflowSearchAttributesTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the signal transfer task.
func (u *UpsertWorkflowSearchAttributesTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (u *UpsertWorkflowSearchAttributesTask) GetVisibilityTimestamp() time.Time {
	return u.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (u *UpsertWorkflowSearchAttributesTask) SetVisibilityTimestamp(timestamp time.Time) {
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

// GetType returns the type of the history replication task
func (a *SyncActivityTask) GetType() int {
	return ReplicationTaskTypeSyncActivity
}

// GetVersion returns the version of the history replication task
func (a *SyncActivityTask) GetVersion() int64 {
	return a.Version
}

// SetVersion returns the version of the history replication task
func (a *SyncActivityTask) SetVersion(version int64) {
	a.Version = version
}

// GetTaskID returns the sequence ID of the history replication task
func (a *SyncActivityTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the history replication task
func (a *SyncActivityTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetVisibilityTimestamp get the visibility timestamp
func (a *SyncActivityTask) GetVisibilityTimestamp() time.Time {
	return a.VisibilityTimestamp
}

// SetVisibilityTimestamp set the visibility timestamp
func (a *SyncActivityTask) SetVisibilityTimestamp(timestamp time.Time) {
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

// GetWorkflowID returns the workflow ID for transfer task
func (t *TransferTaskInfo) GetWorkflowID() string {
	return t.WorkflowID
}

// GetRunID returns the run ID for transfer task
func (t *TransferTaskInfo) GetRunID() string {
	return t.RunID
}

// GetDomainID returns the domain ID for transfer task
func (t *TransferTaskInfo) GetDomainID() string {
	return t.DomainID
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

// GetVisibilityTimestamp returns the task type for replication task
func (t *ReplicationTaskInfo) GetVisibilityTimestamp() time.Time {
	return time.Time{}
}

// GetWorkflowID returns the workflow ID for replication task
func (t *ReplicationTaskInfo) GetWorkflowID() string {
	return t.WorkflowID
}

// GetRunID returns the run ID for replication task
func (t *ReplicationTaskInfo) GetRunID() string {
	return t.RunID
}

// GetDomainID returns the domain ID for replication task
func (t *ReplicationTaskInfo) GetDomainID() string {
	return t.DomainID
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

// GetVisibilityTimestamp returns the task type for timer task
func (t *TimerTaskInfo) GetVisibilityTimestamp() time.Time {
	return t.VisibilityTimestamp
}

// GetWorkflowID returns the workflow ID for timer task
func (t *TimerTaskInfo) GetWorkflowID() string {
	return t.WorkflowID
}

// GetRunID returns the run ID for timer task
func (t *TimerTaskInfo) GetRunID() string {
	return t.RunID
}

// GetDomainID returns the domain ID for timer task
func (t *TimerTaskInfo) GetDomainID() string {
	return t.DomainID
}

// GetTaskType returns the task type for timer task
func (t *TimerTaskInfo) String() string {
	return fmt.Sprintf(
		"{DomainID: %v, WorkflowID: %v, RunID: %v, VisibilityTimestamp: %v, TaskID: %v, TaskType: %v, TimeoutType: %v, EventID: %v, ScheduleAttempt: %v, Version: %v.}",
		t.DomainID, t.WorkflowID, t.RunID, t.VisibilityTimestamp, t.TaskID, t.TaskType, t.TimeoutType, t.EventID, t.ScheduleAttempt, t.Version,
	)
}

// SerializeClusterConfigs makes an array of *ClusterReplicationConfig serializable
// by flattening them into map[string]interface{}
func SerializeClusterConfigs(replicationConfigs []*ClusterReplicationConfig) []map[string]interface{} {
	seriaizedReplicationConfigs := []map[string]interface{}{}
	for index := range replicationConfigs {
		seriaizedReplicationConfigs = append(seriaizedReplicationConfigs, replicationConfigs[index].serialize())
	}
	return seriaizedReplicationConfigs
}

// DeserializeClusterConfigs creates an array of ClusterReplicationConfigs from an array of map representations
func DeserializeClusterConfigs(replicationConfigs []map[string]interface{}) []*ClusterReplicationConfig {
	deseriaizedReplicationConfigs := []*ClusterReplicationConfig{}
	for index := range replicationConfigs {
		deseriaizedReplicationConfig := &ClusterReplicationConfig{}
		deseriaizedReplicationConfig.deserialize(replicationConfigs[index])
		deseriaizedReplicationConfigs = append(deseriaizedReplicationConfigs, deseriaizedReplicationConfig)
	}

	return deseriaizedReplicationConfigs
}

func (config *ClusterReplicationConfig) serialize() map[string]interface{} {
	output := make(map[string]interface{})
	output["cluster_name"] = config.ClusterName
	return output
}

func (config *ClusterReplicationConfig) deserialize(input map[string]interface{}) {
	config.ClusterName = input["cluster_name"].(string)
}

// DBTimestampToUnixNano converts CQL timestamp to UnixNano
func DBTimestampToUnixNano(milliseconds int64) int64 {
	return milliseconds * 1000 * 1000 // Milliseconds are 10⁻³, nanoseconds are 10⁻⁹, (-3) - (-9) = 6, so multiply by 10⁶
}

// UnixNanoToDBTimestamp converts UnixNano to CQL timestamp
func UnixNanoToDBTimestamp(timestamp int64) int64 {
	return timestamp / (1000 * 1000) // Milliseconds are 10⁻³, nanoseconds are 10⁻⁹, (-9) - (-3) = -6, so divide by 10⁶
}

var internalThriftEncoder = codec.NewThriftRWEncoder()

// NewHistoryBranchToken return a new branch token
func NewHistoryBranchToken(treeID string) ([]byte, error) {
	branchID := uuid.New()
	bi := &workflow.HistoryBranch{
		TreeID:    &treeID,
		BranchID:  &branchID,
		Ancestors: []*workflow.HistoryBranchRange{},
	}
	token, err := internalThriftEncoder.Encode(bi)
	if err != nil {
		return nil, err
	}
	return token, nil
}

// NewHistoryBranchTokenByBranchID return a new branch token with treeID/branchID
func NewHistoryBranchTokenByBranchID(treeID, branchID string) ([]byte, error) {
	bi := &workflow.HistoryBranch{
		TreeID:    &treeID,
		BranchID:  &branchID,
		Ancestors: []*workflow.HistoryBranchRange{},
	}
	token, err := internalThriftEncoder.Encode(bi)
	if err != nil {
		return nil, err
	}
	return token, nil
}

// NewHistoryBranchTokenFromAnother make up a branchToken
func NewHistoryBranchTokenFromAnother(branchID string, anotherToken []byte) ([]byte, error) {
	var branch workflow.HistoryBranch
	err := internalThriftEncoder.Decode(anotherToken, &branch)
	if err != nil {
		return nil, err
	}

	bi := &workflow.HistoryBranch{
		TreeID:    branch.TreeID,
		BranchID:  &branchID,
		Ancestors: []*workflow.HistoryBranchRange{},
	}
	token, err := internalThriftEncoder.Encode(bi)
	if err != nil {
		return nil, err
	}
	return token, nil
}

// BuildHistoryGarbageCleanupInfo combine the workflow identity information into a string
func BuildHistoryGarbageCleanupInfo(domainID, workflowID, runID string) string {
	return fmt.Sprintf("%v:%v:%v", domainID, workflowID, runID)
}

// SplitHistoryGarbageCleanupInfo returns workflow identity information
func SplitHistoryGarbageCleanupInfo(info string) (domainID, workflowID, runID string, err error) {
	ss := strings.Split(info, ":")
	// workflowID can contain ":" so len(ss) can be greater than 3
	if len(ss) < numItemsInGarbageInfo {
		return "", "", "", fmt.Errorf("not able to split info for  %s", info)
	}
	domainID = ss[0]
	runID = ss[len(ss)-1]
	workflowEnd := len(info) - len(runID) - 1
	workflowID = info[len(domainID)+1 : workflowEnd]
	return
}
