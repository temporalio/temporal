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

type (
	//////////////////////////////////////////////////////////////////////
	// Persistence interface is a lower layer of dataInterface.
	// The intention is to let different persistence implementation(SQL,Cassandra/etc) share some common logic
	// Right now the only common part is serialization/deserialization, and only ExecutionManager/HistoryManager need it.
	// ShardManager/TaskManager/MetadataManager are the same.
	//////////////////////////////////////////////////////////////////////

	// ShardStore is a lower level of ShardManager
	ShardStore = ShardManager
	// TaskStore is a lower level of TaskManager
	TaskStore = TaskManager
	// MetadataStore is a lower level of MetadataManager
	MetadataStore interface {
		Closeable
		GetName() string
		CreateDomain(request *InternalCreateDomainRequest) (*CreateDomainResponse, error)
		GetDomain(request *GetDomainRequest) (*InternalGetDomainResponse, error)
		UpdateDomain(request *InternalUpdateDomainRequest) error
		DeleteDomain(request *DeleteDomainRequest) error
		DeleteDomainByName(request *DeleteDomainByNameRequest) error
		ListDomains(request *ListDomainsRequest) (*InternalListDomainsResponse, error)
		GetMetadata() (*GetMetadataResponse, error)
	}

	// ExecutionStore is used to manage workflow executions for Persistence layer
	ExecutionStore interface {
		Closeable
		GetName() string
		GetShardID() int
		//The below three APIs are related to serialization/deserialization
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *InternalUpdateWorkflowExecutionRequest) error
		ConflictResolveWorkflowExecution(request *InternalConflictResolveWorkflowExecutionRequest) error
		ResetWorkflowExecution(request *InternalResetWorkflowExecutionRequest) error

		CreateWorkflowExecution(request *InternalCreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
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

		// Remove corrupted task
		DeleteTask(request *DeleteTaskRequest) error
	}

	// HistoryV2Store is to manager workflow history events
	HistoryV2Store interface {
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
		// UpdateHistoryBranch update a branch
		CompleteForkBranch(request *InternalCompleteForkBranchRequest) error
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
		ReadMessages(lastMessageID int, maxCount int) ([]*QueueMessage, error)
		DeleteMessagesBefore(messageID int) error
		UpdateAckLevel(messageID int, clusterName string) error
		GetAckLevels() (map[string]int, error)
	}

	// QueueMessage is the message that stores in the queue
	QueueMessage struct {
		ID      int
		Payload []byte
	}

	// DataBlob represents a blob for any binary data.
	// It contains raw data, and metadata(right now only encoding) in other field
	// Note that it should be only used for Persistence layer, below dataInterface and application(historyEngine/etc)
	DataBlob struct {
		Encoding common.EncodingType
		Data     []byte
	}

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
		DomainID                           string
		WorkflowID                         string
		RunID                              string
		ParentDomainID                     string
		ParentWorkflowID                   string
		ParentRunID                        string
		InitiatedID                        int64
		CompletionEventBatchID             int64
		CompletionEvent                    *DataBlob
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
		AutoResetPoints                    *DataBlob
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
		CronSchedule       string
		ExpirationSeconds  int32
		Memo               map[string][]byte
		SearchAttributes   map[string][]byte

		// attributes which are not related to mutable state at all
		HistorySize int64
	}

	// InternalWorkflowMutableState indicates workflow related state for Persistence Interface
	InternalWorkflowMutableState struct {
		ExecutionInfo    *InternalWorkflowExecutionInfo
		ReplicationState *ReplicationState
		VersionHistories *DataBlob
		ActivityInfos    map[int64]*InternalActivityInfo

		TimerInfos          map[string]*TimerInfo
		ChildExecutionInfos map[int64]*InternalChildExecutionInfo
		RequestCancelInfos  map[int64]*RequestCancelInfo
		SignalInfos         map[int64]*SignalInfo
		SignalRequestedIDs  map[string]struct{}
		BufferedEvents      []*DataBlob
	}

	// InternalActivityInfo details  for Persistence Interface
	InternalActivityInfo struct {
		Version                  int64
		ScheduleID               int64
		ScheduledEventBatchID    int64
		ScheduledEvent           *DataBlob
		ScheduledTime            time.Time
		StartedID                int64
		StartedEvent             *DataBlob
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

	// InternalChildExecutionInfo has details for pending child executions for Persistence Interface
	InternalChildExecutionInfo struct {
		Version               int64
		InitiatedID           int64
		InitiatedEventBatchID int64
		InitiatedEvent        *DataBlob
		StartedID             int64
		StartedWorkflowID     string
		StartedRunID          string
		StartedEvent          *DataBlob
		CreateRequestID       string
		DomainName            string
		WorkflowTypeName      string
		ParentClosePolicy     workflow.ParentClosePolicy
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
		VersionHistories *DataBlob
		StartVersion     int64
		LastWriteVersion int64

		UpsertActivityInfos       []*InternalActivityInfo
		DeleteActivityInfos       []int64
		UpsertTimerInfos          []*TimerInfo
		DeleteTimerInfos          []string
		UpsertChildExecutionInfos []*InternalChildExecutionInfo
		DeleteChildExecutionInfo  *int64
		UpsertRequestCancelInfos  []*RequestCancelInfo
		DeleteRequestCancelInfo   *int64
		UpsertSignalInfos         []*SignalInfo
		DeleteSignalInfo          *int64
		UpsertSignalRequestedIDs  []string
		DeleteSignalRequestedID   string
		NewBufferedEvents         *DataBlob
		ClearBufferedEvents       bool

		TransferTasks    []Task
		TimerTasks       []Task
		ReplicationTasks []Task

		Condition int64
	}

	// InternalWorkflowSnapshot is used as generic workflow execution state snapshot for Persistence Interface
	InternalWorkflowSnapshot struct {
		ExecutionInfo    *InternalWorkflowExecutionInfo
		ReplicationState *ReplicationState
		VersionHistories *DataBlob
		StartVersion     int64
		LastWriteVersion int64

		ActivityInfos       []*InternalActivityInfo
		TimerInfos          []*TimerInfo
		ChildExecutionInfos []*InternalChildExecutionInfo
		RequestCancelInfos  []*RequestCancelInfo
		SignalInfos         []*SignalInfo
		SignalRequestedIDs  []string

		TransferTasks    []Task
		TimerTasks       []Task
		ReplicationTasks []Task

		Condition int64
	}

	// InternalAppendHistoryEventsRequest is used to append new events to workflow execution history  for Persistence Interface
	InternalAppendHistoryEventsRequest struct {
		DomainID          string
		Execution         workflow.WorkflowExecution
		FirstEventID      int64
		EventBatchVersion int64
		RangeID           int64
		TransactionID     int64
		Events            *DataBlob
		Overwrite         bool
	}

	// InternalAppendHistoryNodesRequest is used to append a batch of history nodes
	InternalAppendHistoryNodesRequest struct {
		// True if it is the first append request to the branch
		IsNewBranch bool
		// The info for clean up data in background
		Info string
		// The branch to be appended
		BranchInfo workflow.HistoryBranch
		// The first eventID becomes the nodeID to be appended
		NodeID int64
		// The events to be appended
		Events *DataBlob
		// Requested TransactionID for conditional update
		TransactionID int64
		// Used in sharded data stores to identify which shard to use
		ShardID int
	}

	// InternalGetWorkflowExecutionResponse is the response to GetworkflowExecutionRequest for Persistence Interface
	InternalGetWorkflowExecutionResponse struct {
		State *InternalWorkflowMutableState
	}

	// InternalGetWorkflowExecutionHistoryRequest is used to retrieve history of a workflow execution
	InternalGetWorkflowExecutionHistoryRequest struct {
		// an extra field passing from GetWorkflowExecutionHistoryRequest
		LastEventBatchVersion int64

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

	// InternalGetWorkflowExecutionHistoryResponse is the response to GetWorkflowExecutionHistoryRequest for Persistence Interface
	InternalGetWorkflowExecutionHistoryResponse struct {
		History []*DataBlob
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on GetworkflowExecutionHistoryRequest to read the next page.
		NextPageToken []byte
		// an extra field passing to DataInterface
		LastEventBatchVersion int64
	}

	// InternalForkHistoryBranchRequest is used to fork a history branch
	InternalForkHistoryBranchRequest struct {
		// The base branch to fork from
		ForkBranchInfo workflow.HistoryBranch
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
		NewBranchInfo workflow.HistoryBranch
	}

	// InternalDeleteHistoryBranchRequest is used to remove a history branch
	InternalDeleteHistoryBranchRequest struct {
		// branch to be deleted
		BranchInfo workflow.HistoryBranch
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
		BranchInfo workflow.HistoryBranch
		// whether fork is successful
		Success bool
		// Used in sharded data stores to identify which shard to use
		ShardID int
	}

	// InternalReadHistoryBranchResponse is the response to ReadHistoryBranchRequest
	InternalReadHistoryBranchResponse struct {
		// History events
		History []*DataBlob
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
		Status           *workflow.WorkflowExecutionCloseStatus
		HistoryLength    int64
		Memo             *DataBlob
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
		DomainUUID         string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		WorkflowTimeout    int64
		TaskID             int64
		Memo               *DataBlob
		SearchAttributes   map[string][]byte
	}

	// InternalRecordWorkflowExecutionClosedRequest is request to RecordWorkflowExecutionClosed
	InternalRecordWorkflowExecutionClosedRequest struct {
		DomainUUID         string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		TaskID             int64
		Memo               *DataBlob
		SearchAttributes   map[string][]byte
		CloseTimestamp     int64
		Status             workflow.WorkflowExecutionCloseStatus
		HistoryLength      int64
		RetentionSeconds   int64
	}

	// InternalUpsertWorkflowExecutionRequest is request to UpsertWorkflowExecution
	InternalUpsertWorkflowExecutionRequest struct {
		DomainUUID         string
		WorkflowID         string
		RunID              string
		WorkflowTypeName   string
		StartTimestamp     int64
		ExecutionTimestamp int64
		WorkflowTimeout    int64
		TaskID             int64
		Memo               *DataBlob
		SearchAttributes   map[string][]byte
	}

	// InternalDomainConfig describes the domain configuration
	InternalDomainConfig struct {
		// NOTE: this retention is in days, not in seconds
		Retention                int32
		EmitMetric               bool
		ArchivalBucket           string                  // deprecated
		ArchivalStatus           workflow.ArchivalStatus // deprecated
		HistoryArchivalStatus    workflow.ArchivalStatus
		HistoryArchivalURI       string
		VisibilityArchivalStatus workflow.ArchivalStatus
		VisibilityArchivalURI    string
		BadBinaries              *DataBlob
	}

	// InternalCreateDomainRequest is used to create the domain
	InternalCreateDomainRequest struct {
		Info              *DomainInfo
		Config            *InternalDomainConfig
		ReplicationConfig *DomainReplicationConfig
		IsGlobalDomain    bool
		ConfigVersion     int64
		FailoverVersion   int64
	}

	// InternalGetDomainResponse is the response for GetDomain
	InternalGetDomainResponse struct {
		Info                        *DomainInfo
		Config                      *InternalDomainConfig
		ReplicationConfig           *DomainReplicationConfig
		IsGlobalDomain              bool
		ConfigVersion               int64
		FailoverVersion             int64
		FailoverNotificationVersion int64
		NotificationVersion         int64
	}

	// InternalUpdateDomainRequest is used to update domain
	InternalUpdateDomainRequest struct {
		Info                        *DomainInfo
		Config                      *InternalDomainConfig
		ReplicationConfig           *DomainReplicationConfig
		ConfigVersion               int64
		FailoverVersion             int64
		FailoverNotificationVersion int64
		NotificationVersion         int64
	}

	// InternalListDomainsResponse is the response for GetDomain
	InternalListDomainsResponse struct {
		Domains       []*InternalGetDomainResponse
		NextPageToken []byte
	}
)

// NewDataBlob returns a new DataBlob
func NewDataBlob(data []byte, encodingType common.EncodingType) *DataBlob {
	if data == nil || len(data) == 0 {
		return nil
	}
	if encodingType != "thriftrw" && data[0] == 'Y' {
		panic(fmt.Sprintf("Invalid incoding: \"%v\"", encodingType))
	}
	return &DataBlob{
		Data:     data,
		Encoding: encodingType,
	}
}

// FromDataBlob decodes a datablob into a (payload, encodingType) tuple
func FromDataBlob(blob *DataBlob) ([]byte, string) {
	if blob == nil || len(blob.Data) == 0 {
		return nil, ""
	}
	return blob.Data, string(blob.Encoding)
}

// GetEncoding returns encoding type
func (d *DataBlob) GetEncoding() common.EncodingType {
	encodingStr := string(d.Encoding)

	switch common.EncodingType(encodingStr) {
	case common.EncodingTypeGob:
		return common.EncodingTypeGob
	case common.EncodingTypeJSON:
		return common.EncodingTypeJSON
	case common.EncodingTypeThriftRW:
		return common.EncodingTypeThriftRW
	case common.EncodingTypeEmpty:
		return common.EncodingTypeEmpty
	default:
		return common.EncodingTypeUnknown
	}
}

// ToThrift convert data blob to thrift representation
func (d *DataBlob) ToThrift() *workflow.DataBlob {
	switch d.Encoding {
	case common.EncodingTypeJSON:
		return &workflow.DataBlob{
			EncodingType: workflow.EncodingTypeJSON.Ptr(),
			Data:         d.Data,
		}
	case common.EncodingTypeThriftRW:
		return &workflow.DataBlob{
			EncodingType: workflow.EncodingTypeThriftRW.Ptr(),
			Data:         d.Data,
		}
	default:
		panic(fmt.Sprintf("DataBlob seeing unsupported enconding type: %v", d.Encoding))
	}
}

// NewDataBlobFromThrift convert data blob from thrift representation
func NewDataBlobFromThrift(blob *workflow.DataBlob) *DataBlob {
	switch blob.GetEncodingType() {
	case workflow.EncodingTypeJSON:
		return &DataBlob{
			Encoding: common.EncodingTypeJSON,
			Data:     blob.Data,
		}
	case workflow.EncodingTypeThriftRW:
		return &DataBlob{
			Encoding: common.EncodingTypeThriftRW,
			Data:     blob.Data,
		}
	default:
		panic(fmt.Sprintf("NewDataBlobFromThrift seeing unsupported enconding type: %v", blob.GetEncodingType()))
	}
}
