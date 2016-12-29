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

	// TaskInfoWithID describes a task from tasks table
	TaskInfoWithID struct {
		TaskUUID string
		Info     *TaskInfo
	}

	// Task is the generic interface for workflow tasks
	Task interface {
		GetType() int
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
		ExecutionInfo *WorkflowExecutionInfo
		TransferTasks []Task
		Condition     int64
		RangeID       int64
	}

	// DeleteWorkflowExecutionRequest is used to delete a workflow execution
	DeleteWorkflowExecutionRequest struct {
		ExecutionInfo *WorkflowExecutionInfo
	}

	// GetTransferTasksRequest is used to read tasks from the transfer task queue
	GetTransferTasksRequest struct {
		LockTimeout time.Duration
		BatchSize   int
	}

	// GetTransferTasksResponse is the response to GetTransferTasksRequest
	GetTransferTasksResponse struct {
		Tasks []*TaskInfo
	}

	// CompleteTransferTaskRequest is used to complete a task in the transfer task queue
	CompleteTransferTaskRequest struct {
		Execution workflow.WorkflowExecution
		TaskID    int64
		LockToken string
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

// GetType returns the type of the decision task
func (d *DecisionTask) GetType() int {
	return TaskTypeDecision
}
