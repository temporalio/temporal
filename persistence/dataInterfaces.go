package persistence

import (
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
)

// Workflow execution states
const (
	WorkflowStateCreated = iota
	WorkflowStateRunning
	WorkflowStateCompleted
)

// Types of tasks
const (
	TaskTypeDecision = iota
	TaskTypeActivity
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
		msg string
	}

	// ShardAlreadyExistError is returned when conditionally creating a shard fails
	ShardAlreadyExistError struct {
		msg string
	}

	// ShardInfo describes a shard
	ShardInfo struct {
		ShardID          int
		RangeID          int64
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

	// TaskInfo describes a task
	TaskInfo struct {
		WorkflowID     string
		RunID          string
		TaskID         int64
		TaskList       string
		TaskType       int
		ScheduleID     int64
		VisibilityTime time.Time
		LockToken      string
		DeliveryCount  int
	}

	// TimerInfo describes a timer.
	TimerInfo struct {
		WorkflowID  string
		RunID       string
		TaskID      int64
		TaskType    int
		TimeoutType int
		EventID     int64
	}

	// TaskInfoWithID describes a task from tasks table
	TaskInfoWithID struct {
		TaskUUID string
		Info     *TaskInfo
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
		TaskID   int64
		TaskList string
		EventID  int64
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
		ExecutionInfo   *WorkflowExecutionInfo
		TransferTasks   []Task
		TimerTasks      []Task
		DeleteTimerTask Task
		Condition       int64
		RangeID         int64
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
		RangeID      int64
	}

	// GetTransferTasksResponse is the response to GetTransferTasksRequest
	GetTransferTasksResponse struct {
		Tasks []*TaskInfo
	}

	// CompleteTransferTaskRequest is used to complete a task in the transfer task queue
	CompleteTransferTaskRequest struct {
		Execution workflow.WorkflowExecution
		TaskID    int64
	}

	// CreateTaskRequest is used to create a new task for a workflow exectution
	CreateTaskRequest struct {
		Execution workflow.WorkflowExecution
		TaskList  string
		Data      Task
	}

	// CreateTaskResponse is the response to CreateTaskRequest
	CreateTaskResponse struct {
		TaskID string
	}

	// GetTasksRequest is used to retrieve tasks of a task list
	GetTasksRequest struct {
		TaskList    string
		TaskType    int
		LockTimeout time.Duration
		BatchSize   int
	}

	// GetTasksResponse is the response to GetTasksRequests
	GetTasksResponse struct {
		Tasks []*TaskInfoWithID
	}

	// CompleteTaskRequest is used to complete a task
	CompleteTaskRequest struct {
		Execution workflow.WorkflowExecution
		TaskList  string
		TaskType  int
		TaskID    string
		LockToken string
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
		Timers []*TimerInfo
	}

	// ExecutionManager is the used to manage workflow executions
	ExecutionManager interface {
		CreateShard(request *CreateShardRequest) error
		GetShard(request *GetShardRequest) (*GetShardResponse, error)
		UpdateShard(request *UpdateShardRequest) error
		CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error)
		GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error
		DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error
		GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error)
		CompleteTransferTask(request *CompleteTransferTaskRequest) error

		// Timer related methods.
		GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error)
	}

	// TaskManager is used to manage tasks
	TaskManager interface {
		CreateTask(request *CreateTaskRequest) (*CreateTaskResponse, error)
		GetTasks(request *GetTasksRequest) (*GetTasksResponse, error)
		CompleteTask(request *CompleteTaskRequest) error
	}
)

func (e *ConditionFailedError) Error() string {
	return e.msg
}

func (e *ShardAlreadyExistError) Error() string {
	return e.msg
}

// GetType returns the type of the activity task
func (a *ActivityTask) GetType() int {
	return TaskTypeActivity
}

// GetTaskID returns the sequence ID of the activity task
func (a *ActivityTask) GetTaskID() int64 {
	return a.TaskID
}

// GetType returns the type of the decision task
func (d *DecisionTask) GetType() int {
	return TaskTypeDecision
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
