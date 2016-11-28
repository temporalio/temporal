package workflow

import (
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
)

// Workflow execution states
const (
	workflowStateCreated = iota
	workflowStateRunning
	workflowStateCompleted
)

const (
	taskTypeDecision = iota
	taskTypeActivity
)

type (
	conditionFailedError struct {
		msg string
	}

	workflowExecutionInfo struct {
		workflowID           string
		runID                string
		taskList             string
		history              []byte
		executionContext     []byte
		state                int
		nextEventID          int64
		lastProcessedEvent   int64
		lastUpdatedTimestamp time.Time
		decisionPending      bool
	}

	taskInfo struct {
		workflowID     string
		runID          string
		taskID         string
		taskList       string
		taskType       int
		scheduleID     int64
		visibilityTime time.Time
		lockToken      string
		deliveryCount  int
	}

	task interface {
		GetType() int
	}

	activityTask struct {
		taskList   string
		scheduleID int64
	}

	decisionTask struct {
		taskList   string
		scheduleID int64
	}

	createWorkflowExecutionRequest struct {
		execution          workflow.WorkflowExecution
		taskList           string
		history            []byte
		executionContext   []byte
		nextEventID        int64
		lastProcessedEvent int64
		transferTasks      []task
	}

	createWorkflowExecutionResponse struct {
		taskID string
	}

	getWorkflowExecutionRequest struct {
		execution workflow.WorkflowExecution
	}

	getWorkflowExecutionResponse struct {
		executionInfo *workflowExecutionInfo
	}

	updateWorkflowExecutionRequest struct {
		executionInfo *workflowExecutionInfo
		transferTasks []task
		condition     int64
	}

	deleteWorkflowExecutionRequest struct {
		execution workflow.WorkflowExecution
		condition int64
	}

	getTransferTasksRequest struct {
		lockTimeout time.Duration
		batchSize   int
	}

	getTransferTasksResponse struct {
		tasks []*taskInfo
	}

	completeTransferTaskRequest struct {
		execution workflow.WorkflowExecution
		taskID    string
		lockToken string
	}

	createTaskRequest struct {
		execution workflow.WorkflowExecution
		taskList  string
		data      task
	}

	createTaskResponse struct {
		taskID string
	}

	getTasksRequest struct {
		taskList    string
		taskType    int
		lockTimeout time.Duration
		batchSize   int
	}

	getTasksResponse struct {
		tasks []*taskInfo
	}

	completeTaskRequest struct {
		execution workflow.WorkflowExecution
		taskList  string
		taskType  int
		taskID    string
		lockToken string
	}

	// ExecutionPersistence is the used to manage workflow executions
	ExecutionPersistence interface {
		CreateWorkflowExecution(request *createWorkflowExecutionRequest) (*createWorkflowExecutionResponse, error)
		GetWorkflowExecution(request *getWorkflowExecutionRequest) (*getWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *updateWorkflowExecutionRequest) error
		DeleteWorkflowExecution(request *deleteWorkflowExecutionRequest) error
		GetTransferTasks(request *getTransferTasksRequest) (*getTransferTasksResponse, error)
		CompleteTransferTask(request *completeTransferTaskRequest) error
	}

	// TaskPersistence is used to manage tasks
	TaskPersistence interface {
		CreateTask(request *createTaskRequest) (*createTaskResponse, error)
		GetTasks(request *getTasksRequest) (*getTasksResponse, error)
		CompleteTask(request *completeTaskRequest) error
	}
)

func (e *conditionFailedError) Error() string {
	return e.msg
}

func (a *activityTask) GetType() int {
	return taskTypeActivity
}

func (d *decisionTask) GetType() int {
	return taskTypeDecision
}
