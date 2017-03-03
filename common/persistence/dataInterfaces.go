package persistence

import (
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

// Workflow execution states
const (
	WorkflowStateCreated   = iota
	WorkflowStateRunning
	WorkflowStateCompleted
)

// Types of task lists
const (
	TaskListTypeDecision = iota
	TaskListTypeActivity
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
		WorkflowID           string
		RunID                string
		TaskList             string
		History              []byte
		ExecutionContext     []byte
		State                int
		NextEventID          int64
		LastProcessedEvent   int64
		LastUpdatedTimestamp time.Time
		DecisionPending      bool
	}

	// TransferTaskInfo describes a transfer task
	TransferTaskInfo struct {
		WorkflowID string
		RunID      string
		TaskID     int64
		TaskList   string
		TaskType   int
		ScheduleID int64
	}

	// TimerTaskInfo describes a timer task.
	TimerTaskInfo struct {
		WorkflowID  string
		RunID       string
		TaskID      int64
		TaskType    int
		TimeoutType int
		EventID     int64
	}

	// TaskListInfo describes a state of a task list implementation.
	TaskListInfo struct {
		Name     string
		TaskType int
		RangeID  int64
		AckLevel int64
	}

	// TaskInfo describes either activity or decision task
	TaskInfo struct {
		WorkflowID string
		RunID      string
		TaskID     int64
		ScheduleID int64
	}

	// Task is the generic interface for workflow tasks
	Task interface {
		GetType() int
		GetTaskID() int64
	}

	// ActivityTask identifies an activity task
	ActivityTask struct {
		TaskID     int64
		TaskList   string
		ScheduleID int64
	}

	// DecisionTask identifies a decision task
	DecisionTask struct {
		TaskID     int64
		TaskList   string
		ScheduleID int64
	}

	// DecisionTimeoutTask identifies a timeout task.
	DecisionTimeoutTask struct {
		TaskID  int64
		EventID int64
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

	// WorkflowMutableState indicates worklow realted state
	WorkflowMutableState struct {
		ActivitInfos map[int64]*ActivityInfo
		TimerInfos   map[string]*TimerInfo
	}

	// ActivityInfo details.
	ActivityInfo struct {
		ScheduleID             int64
		StartedID              int64
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
		Execution          workflow.WorkflowExecution
		TaskList           string
		History            []byte
		ExecutionContext   []byte
		NextEventID        int64
		LastProcessedEvent int64
		TransferTasks      []Task
		TimerTasks         []Task
		RangeID            int64
	}

	// CreateWorkflowExecutionResponse is the response to CreateWorkflowExecutionRequest
	CreateWorkflowExecutionResponse struct {
		TaskID string
	}

	// GetWorkflowExecutionRequest is used to retrieve the info of a workflow execution
	GetWorkflowExecutionRequest struct {
		Execution workflow.WorkflowExecution
	}

	// GetWorkflowExecutionResponse is the response to GetworkflowExecutionRequest
	GetWorkflowExecutionResponse struct {
		ExecutionInfo *WorkflowExecutionInfo
	}

	// UpdateWorkflowExecutionRequest is used to update a workflow execution
	UpdateWorkflowExecutionRequest struct {
		ExecutionInfo       *WorkflowExecutionInfo
		TransferTasks       []Task
		TimerTasks          []Task
		DeleteTimerTask     Task
		Condition           int64
		RangeID             int64
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
		ReadLevel    int64
		MaxReadLevel int64
		BatchSize    int
	}

	// GetTransferTasksResponse is the response to GetTransferTasksRequest
	GetTransferTasksResponse struct {
		Tasks []*TransferTaskInfo
	}

	// CompleteTransferTaskRequest is used to complete a task in the transfer task queue
	CompleteTransferTaskRequest struct {
		Execution workflow.WorkflowExecution
		TaskID    int64
	}

	// LeaseTaskListRequest is used to request lease of a task list
	LeaseTaskListRequest struct {
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

	// CreateTaskRequest is used to create a new task for a workflow exectution
	CreateTaskRequest struct {
		Execution workflow.WorkflowExecution
		Data      Task
		TaskID    int64
		RangeID   int64
	}

	// CreateTaskResponse is the response to CreateTaskRequest
	CreateTaskResponse struct {
	}

	// GetTasksRequest is used to retrieve tasks of a task list
	GetTasksRequest struct {
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

	// GetWorkflowMutableStateRequest is used to retrieve the info of a workflow execution
	GetWorkflowMutableStateRequest struct {
		WorkflowID     string
		RunID          string
		IncludeDetails bool
	}

	// GetWorkflowMutableStateResponse is the response to GetWorkflowMutableStateRequest
	GetWorkflowMutableStateResponse struct {
		State *WorkflowMutableState
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
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error

		// Timer related methods.
		GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error)

		// Workflow mutable state operations.
		GetWorkflowMutableState(request *GetWorkflowMutableStateRequest) (*GetWorkflowMutableStateResponse, error)
	}

	// ExecutionManagerFactory creates an instance of ExecutionManager for a given shard
	ExecutionManagerFactory interface {
		CreateExecutionManager(shardID int) (ExecutionManager, error)
	}

	// TaskManager is used to manage tasks
	TaskManager interface {
		LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error)
		UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error)
		CreateTask(request *CreateTaskRequest) (*CreateTaskResponse, error)
		GetTasks(request *GetTasksRequest) (*GetTasksResponse, error)
		CompleteTask(request *CompleteTaskRequest) error
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

// GetType returns the type of the activity task
func (a *ActivityTask) GetType() int {
	return TaskListTypeActivity
}

// GetTaskID returns the sequence ID of the activity task
func (a *ActivityTask) GetTaskID() int64 {
	return a.TaskID
}

// GetType returns the type of the decision task
func (d *DecisionTask) GetType() int {
	return TaskListTypeDecision
}

// GetTaskID returns the sequence ID of the decision task.
func (d *DecisionTask) GetTaskID() int64 {
	return d.TaskID
}

// GetType returns the type of the timer task
func (d *DecisionTimeoutTask) GetType() int {
	return TaskTypeDecisionTimeout
}

// GetTaskID returns the sequence ID.
func (d *DecisionTimeoutTask) GetTaskID() int64 {
	return d.TaskID
}

// GetType returns the type of the timer task
func (a *ActivityTimeoutTask) GetType() int {
	return TaskTypeActivityTimeout
}

// GetTaskID returns the sequence ID.
func (a *ActivityTimeoutTask) GetTaskID() int64 {
	return a.TaskID
}

// GetType returns the type of the timer task
func (u *UserTimerTask) GetType() int {
	return TaskTypeUserTimer
}

// GetTaskID returns the sequence ID of the decision task.
func (u *UserTimerTask) GetTaskID() int64 {
	return u.TaskID
}
