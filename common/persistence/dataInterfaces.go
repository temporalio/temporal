package persistence

import (
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
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

// Types of task lists
const (
	TaskListTypeDecision = iota
	TaskListTypeActivity
)

// Transfer task types
const (
	TransferTaskTypeDecisionTask = iota
	TransferTaskTypeActivityTask
	TransferTaskTypeDeleteExecution
	TransferTaskTypeCancelExecution
)

// Types of timers
const (
	TaskTypeDecisionTimeout = iota
	TaskTypeActivityTimeout
	TaskTypeUserTimer
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

	// TimeoutError is returned when a write operation fails due to a timeout
	TimeoutError struct {
		Msg string
	}

	// ShardInfo describes a shard
	ShardInfo struct {
		ShardID          int
		Owner            string
		RangeID          int64
		StolenSinceRenew int
		UpdatedAt        time.Time
		TransferAckLevel int64
	}

	// WorkflowExecutionInfo describes a workflow execution
	WorkflowExecutionInfo struct {
		DomainID             string
		WorkflowID           string
		RunID                string
		TaskList             string
		WorkflowTypeName     string
		DecisionTimeoutValue int32
		ExecutionContext     []byte
		State                int
		NextEventID          int64
		LastProcessedEvent   int64
		StartTimestamp       time.Time
		LastUpdatedTimestamp time.Time
		CreateRequestID      string
		DecisionScheduleID   int64
		DecisionStartedID    int64
		DecisionRequestID    string
		DecisionTimeout      int32
	}

	// TransferTaskInfo describes a transfer task
	TransferTaskInfo struct {
		DomainID         string
		WorkflowID       string
		RunID            string
		TaskID           int64
		TargetDomainID   string
		TargetWorkflowID string
		TargetRunID      string
		TaskList         string
		TaskType         int
		ScheduleID       int64
	}

	// TimerTaskInfo describes a timer task.
	TimerTaskInfo struct {
		DomainID    string
		WorkflowID  string
		RunID       string
		TaskID      int64
		TaskType    int
		TimeoutType int
		EventID     int64
	}

	// TaskListInfo describes a state of a task list implementation.
	TaskListInfo struct {
		DomainID string
		Name     string
		TaskType int
		RangeID  int64
		AckLevel int64
	}

	// TaskInfo describes either activity or decision task
	TaskInfo struct {
		DomainID   string
		WorkflowID string
		RunID      string
		TaskID     int64
		ScheduleID int64
	}

	// Task is the generic interface for workflow tasks
	Task interface {
		GetType() int
		GetTaskID() int64
		SetTaskID(id int64)
	}

	// ActivityTask identifies a transfer task for activity
	ActivityTask struct {
		TaskID     int64
		DomainID   string
		TaskList   string
		ScheduleID int64
	}

	// DecisionTask identifies a transfer task for decision
	DecisionTask struct {
		TaskID     int64
		DomainID   string
		TaskList   string
		ScheduleID int64
	}

	// DeleteExecutionTask identifies a transfer task for deletion of execution
	DeleteExecutionTask struct {
		TaskID int64
	}

	// DecisionTimeoutTask identifies a timeout task.
	DecisionTimeoutTask struct {
		TaskID  int64
		EventID int64
	}

	// CancelExecutionTask identifies a transfer task for cancel of execution
	CancelExecutionTask struct {
		TaskID           int64
		TargetDomainID   string
		TargetWorkflowID string
		TargetRunID      string
		ScheduleID       int64
	}

	// ActivityTimeoutTask identifies a timeout task.
	ActivityTimeoutTask struct {
		TaskID      int64
		TimeoutType int
		EventID     int64
	}

	// UserTimerTask identifies a timeout task.
	UserTimerTask struct {
		TaskID  int64
		EventID int64
	}

	// WorkflowMutableState indicates workflow related state
	WorkflowMutableState struct {
		ActivitInfos  map[int64]*ActivityInfo
		TimerInfos    map[string]*TimerInfo
		ExecutionInfo *WorkflowExecutionInfo
	}

	// ActivityInfo details.
	ActivityInfo struct {
		ScheduleID             int64
		ScheduledEvent         []byte
		StartedID              int64
		StartedEvent           []byte
		ActivityID             string
		RequestID              string
		Details                []byte
		ScheduleToStartTimeout int32
		ScheduleToCloseTimeout int32
		StartToCloseTimeout    int32
		HeartbeatTimeout       int32
		CancelRequested        bool
		CancelRequestID        int64
	}

	// TimerInfo details - metadata about user timer info.
	TimerInfo struct {
		TimerID    string
		StartedID  int64
		ExpiryTime time.Time
		TaskID     int64
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
		TaskList                    string
		WorkflowTypeName            string
		DecisionTimeoutValue        int32
		ExecutionContext            []byte
		NextEventID                 int64
		LastProcessedEvent          int64
		TransferTasks               []Task
		TimerTasks                  []Task
		RangeID                     int64
		DecisionScheduleID          int64
		DecisionStartedID           int64
		DecisionStartToCloseTimeout int32
	}

	// CreateWorkflowExecutionResponse is the response to CreateWorkflowExecutionRequest
	CreateWorkflowExecutionResponse struct {
		TaskID string
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
		DomainID  string
		WorkflowID string
	}

	// GetCurrentExecutionResponse is the response to GetCurrentExecution
	GetCurrentExecutionResponse struct {
		RunID string
	}

	// UpdateWorkflowExecutionRequest is used to update a workflow execution
	UpdateWorkflowExecutionRequest struct {
		ExecutionInfo   *WorkflowExecutionInfo
		TransferTasks   []Task
		TimerTasks      []Task
		DeleteTimerTask Task
		Condition       int64
		RangeID         int64

		// Mutable state
		UpsertActivityInfos []*ActivityInfo
		DeleteActivityInfo  *int64
		UpserTimerInfos     []*TimerInfo
		DeleteTimerInfos    []string
	}

	// DeleteWorkflowExecutionRequest is used to delete a workflow execution
	DeleteWorkflowExecutionRequest struct {
		ExecutionInfo *WorkflowExecutionInfo
	}

	// GetTransferTasksRequest is used to read tasks from the transfer task queue
	GetTransferTasksRequest struct {
		ReadLevel int64
		BatchSize int
	}

	// GetTransferTasksResponse is the response to GetTransferTasksRequest
	GetTransferTasksResponse struct {
		Tasks []*TransferTaskInfo
	}

	// CompleteTransferTaskRequest is used to complete a task in the transfer task queue
	CompleteTransferTaskRequest struct {
		TaskID int64
	}

	// CompleteTimerTaskRequest is used to complete a task in the timer task queue
	CompleteTimerTaskRequest struct {
		TaskID int64
	}

	// LeaseTaskListRequest is used to request lease of a task list
	LeaseTaskListRequest struct {
		DomainID string
		TaskList string
		TaskType int
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
		DomainID     string
		TaskList     string
		TaskListType int
		RangeID      int64
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
		MinKey    int64
		MaxKey    int64
		BatchSize int
	}

	// GetTimerIndexTasksResponse is the response for GetTimerIndexTasks
	GetTimerIndexTasksResponse struct {
		Timers []*TimerTaskInfo
	}

	// AppendHistoryEventsRequest is used to append new events to workflow execution history
	AppendHistoryEventsRequest struct {
		DomainID      string
		Execution     workflow.WorkflowExecution
		FirstEventID  int64
		RangeID       int64
		TransactionID int64
		Events        []byte
		Overwrite     bool
	}

	// GetWorkflowExecutionHistoryRequest is used to retrieve history of a workflow execution
	GetWorkflowExecutionHistoryRequest struct {
		DomainID  string
		Execution workflow.WorkflowExecution
		// Get the history events upto NextEventID.  Not Inclusive.
		NextEventID int64
		// Maximum number of history append transactions per page
		PageSize int
		// Token to continue reading next page of history append transactions.  Pass in empty slice for first page
		NextPageToken []byte
	}

	// GetWorkflowExecutionHistoryResponse is the response to GetWorkflowExecutionHistoryRequest
	GetWorkflowExecutionHistoryResponse struct {
		// Slice of history append transactioin payload
		Events [][]byte
		// Token to read next page if there are more events beyond page size.
		// Use this to set NextPageToken on GetworkflowExecutionHistoryRequest to read the next page.
		NextPageToken []byte
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
	}

	// DomainConfig describes the domain configuration
	DomainConfig struct {
		Retention  int32
		EmitMetric bool
	}

	// CreateDomainRequest is used to create the domain
	CreateDomainRequest struct {
		Name        string
		Status      int
		Description string
		OwnerEmail  string
		Retention   int32
		EmitMetric  bool
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
		Info   *DomainInfo
		Config *DomainConfig
	}

	// UpdateDomainRequest is used to update domain
	UpdateDomainRequest struct {
		Info   *DomainInfo
		Config *DomainConfig
	}

	// DeleteDomainRequest is used to delete domain entry from domains table
	DeleteDomainRequest struct {
		ID string
	}

	// DeleteDomainByNameRequest is used to delete domain entry from domains_by_name table
	DeleteDomainByNameRequest struct {
		Name string
	}

	// ShardManager is used to manage all shards
	ShardManager interface {
		CreateShard(request *CreateShardRequest) error
		GetShard(request *GetShardRequest) (*GetShardResponse, error)
		UpdateShard(request *UpdateShardRequest) error
	}

	// ExecutionManager is used to manage workflow executions
	ExecutionManager interface {
		CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error)
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error

		// Timer related methods.
		GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error)
		CompleteTimerTask(request *CompleteTimerTaskRequest) error
	}

	// ExecutionManagerFactory creates an instance of ExecutionManager for a given shard
	ExecutionManagerFactory interface {
		CreateExecutionManager(shardID int) (ExecutionManager, error)
	}

	// TaskManager is used to manage tasks
	TaskManager interface {
		LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error)
		UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error)
		CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error)
		GetTasks(request *GetTasksRequest) (*GetTasksResponse, error)
		CompleteTask(request *CompleteTaskRequest) error
	}

	// HistoryManager is used to manage Workflow Execution History
	HistoryManager interface {
		AppendHistoryEvents(request *AppendHistoryEventsRequest) error
		// GetWorkflowExecutionHistory retrieves the paginated list of history events for given execution
		GetWorkflowExecutionHistory(request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponse,
			error)
		DeleteWorkflowExecutionHistory(request *DeleteWorkflowExecutionHistoryRequest) error
	}

	// MetadataManager is used to manage metadata CRUD for various entities
	MetadataManager interface {
		CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error)
		GetDomain(request *GetDomainRequest) (*GetDomainResponse, error)
		UpdateDomain(request *UpdateDomainRequest) error
		DeleteDomain(request *DeleteDomainRequest) error
		DeleteDomainByName(request *DeleteDomainByNameRequest) error
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

func (e *TimeoutError) Error() string {
	return e.Msg
}

// GetType returns the type of the activity task
func (a *ActivityTask) GetType() int {
	return TransferTaskTypeActivityTask
}

// GetTaskID returns the sequence ID of the activity task
func (a *ActivityTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the activity task
func (a *ActivityTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetType returns the type of the decision task
func (d *DecisionTask) GetType() int {
	return TransferTaskTypeDecisionTask
}

// GetTaskID returns the sequence ID of the decision task.
func (d *DecisionTask) GetTaskID() int64 {
	return d.TaskID
}

// SetTaskID sets the sequence ID of the decision task
func (d *DecisionTask) SetTaskID(id int64) {
	d.TaskID = id
}

// GetType returns the type of the delete execution task
func (a *DeleteExecutionTask) GetType() int {
	return TransferTaskTypeDeleteExecution
}

// GetTaskID returns the sequence ID of the delete execution task
func (a *DeleteExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID of the delete execution task
func (a *DeleteExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetType returns the type of the timer task
func (d *DecisionTimeoutTask) GetType() int {
	return TaskTypeDecisionTimeout
}

// GetTaskID returns the sequence ID.
func (d *DecisionTimeoutTask) GetTaskID() int64 {
	return d.TaskID
}

// SetTaskID sets the sequence ID.
func (d *DecisionTimeoutTask) SetTaskID(id int64) {
	d.TaskID = id
}

// GetType returns the type of the timer task
func (a *ActivityTimeoutTask) GetType() int {
	return TaskTypeActivityTimeout
}

// GetTaskID returns the sequence ID.
func (a *ActivityTimeoutTask) GetTaskID() int64 {
	return a.TaskID
}

// SetTaskID sets the sequence ID.
func (a *ActivityTimeoutTask) SetTaskID(id int64) {
	a.TaskID = id
}

// GetType returns the type of the timer task
func (u *UserTimerTask) GetType() int {
	return TaskTypeUserTimer
}

// GetTaskID returns the sequence ID of the timer task.
func (u *UserTimerTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the timer task.
func (u *UserTimerTask) SetTaskID(id int64) {
	u.TaskID = id
}

// GetType returns the type of the cancel transfer task
func (u *CancelExecutionTask) GetType() int {
	return TransferTaskTypeCancelExecution
}

// GetTaskID returns the sequence ID of the cancel transfer task.
func (u *CancelExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

// SetTaskID sets the sequence ID of the cancel transfer task.
func (u *CancelExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}
