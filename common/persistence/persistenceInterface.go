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
	MetadataStore = MetadataManager
	// VisibilityStore is the store interface for visibility
	VisibilityStore = VisibilityManager

	// ExecutionStore is used to manage workflow executions for Persistence layer
	ExecutionStore interface {
		Closeable
		GetName() string
		//The below three APIs are related to serialization/deserialization
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *InternalUpdateWorkflowExecutionRequest) error
		ResetMutableState(request *InternalResetMutableStateRequest) error

		CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
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

	// HistoryStore is used to manage Workflow Execution HistoryEventBatch for Persistence layer
	HistoryStore interface {
		Closeable
		GetName() string
		//The below two APIs are related to serialization/deserialization
		AppendHistoryEvents(request *InternalAppendHistoryEventsRequest) error
		GetWorkflowExecutionHistory(request *InternalGetWorkflowExecutionHistoryRequest) (*InternalGetWorkflowExecutionHistoryResponse, error)

		DeleteWorkflowExecutionHistory(request *DeleteWorkflowExecutionHistoryRequest) error
	}

	// DataBlob represents a blob for any binary data.
	// It contains raw data, and metadata(right now only encoding) in other field
	// Note that it should be only used for Persistence layer, below dataInterface and application(historyEngine/etc)
	DataBlob struct {
		Encoding common.EncodingType
		Data     []byte
	}

	// InternalWorkflowExecutionInfo describes a workflow execution for Persistence Interface
	InternalWorkflowExecutionInfo struct {
		DomainID                     string
		WorkflowID                   string
		RunID                        string
		ParentDomainID               string
		ParentWorkflowID             string
		ParentRunID                  string
		InitiatedID                  int64
		CompletionEvent              *DataBlob
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
		HistorySize                  int64
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

	// InternalWorkflowMutableState indicates workflow related state for Persistence Interface
	InternalWorkflowMutableState struct {
		ActivitInfos             map[int64]*InternalActivityInfo
		TimerInfos               map[string]*TimerInfo
		ChildExecutionInfos      map[int64]*InternalChildExecutionInfo
		RequestCancelInfos       map[int64]*RequestCancelInfo
		SignalInfos              map[int64]*SignalInfo
		SignalRequestedIDs       map[string]struct{}
		ExecutionInfo            *InternalWorkflowExecutionInfo
		ReplicationState         *ReplicationState
		BufferedEvents           []*DataBlob
		BufferedReplicationTasks map[int64]*InternalBufferedReplicationTask
	}

	// InternalActivityInfo details  for Persistence Interface
	InternalActivityInfo struct {
		Version                  int64
		ScheduleID               int64
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
		// Not written to database - This is used only for deduping heartbeat timer creation
		LastTimeoutVisibility int64
	}

	// InternalChildExecutionInfo has details for pending child executions  for Persistence Interface
	InternalChildExecutionInfo struct {
		Version         int64
		InitiatedID     int64
		InitiatedEvent  DataBlob
		StartedID       int64
		StartedEvent    *DataBlob
		CreateRequestID string
	}

	// InternalBufferedReplicationTask has details to handle out of order receive of history events  for Persistence Interface
	InternalBufferedReplicationTask struct {
		FirstEventID  int64
		NextEventID   int64
		Version       int64
		History       *DataBlob
		NewRunHistory *DataBlob
	}

	// InternalUpdateWorkflowExecutionRequest is used to update a workflow execution  for Persistence Interface
	InternalUpdateWorkflowExecutionRequest struct {
		ExecutionInfo        *InternalWorkflowExecutionInfo
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
		UpsertActivityInfos           []*InternalActivityInfo
		DeleteActivityInfos           []int64
		UpserTimerInfos               []*TimerInfo
		DeleteTimerInfos              []string
		UpsertChildExecutionInfos     []*InternalChildExecutionInfo
		DeleteChildExecutionInfo      *int64
		UpsertRequestCancelInfos      []*RequestCancelInfo
		DeleteRequestCancelInfo       *int64
		UpsertSignalInfos             []*SignalInfo
		DeleteSignalInfo              *int64
		UpsertSignalRequestedIDs      []string
		DeleteSignalRequestedID       string
		NewBufferedEvents             *DataBlob
		ClearBufferedEvents           bool
		NewBufferedReplicationTask    *InternalBufferedReplicationTask
		DeleteBufferedReplicationTask *int64
	}

	// InternalResetMutableStateRequest is used to reset workflow execution state  for Persistence Interface
	InternalResetMutableStateRequest struct {
		PrevRunID        string
		ExecutionInfo    *InternalWorkflowExecutionInfo
		ReplicationState *ReplicationState
		Condition        int64
		RangeID          int64

		// Mutable state
		InsertActivityInfos       []*InternalActivityInfo
		InsertTimerInfos          []*TimerInfo
		InsertChildExecutionInfos []*InternalChildExecutionInfo
		InsertRequestCancelInfos  []*RequestCancelInfo
		InsertSignalInfos         []*SignalInfo
		InsertSignalRequestedIDs  []string
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
)

// NewDataBlob returns a new DataBlob
func NewDataBlob(data []byte, encodingType common.EncodingType) *DataBlob {
	if data == nil || len(data) == 0 {
		return nil
	}
	if encodingType != "thriftrw" && data[0] == 'Y' {
		panic(fmt.Sprintf("Invlid incoding: \"%v\"", encodingType))
	}
	return &DataBlob{
		Data:     data,
		Encoding: encodingType,
	}
}

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
	default:
		return common.EncodingTypeUnknown
	}
}
