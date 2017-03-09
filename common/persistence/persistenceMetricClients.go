package persistence

import (
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/metrics"
)

type (
	shardPersistenceClient struct {
		m3Client    metrics.Client
		persistence ShardManager
	}

	workflowExecutionPersistenceClient struct {
		m3Client    metrics.Client
		persistence ExecutionManager
	}

	taskPersistenceClient struct {
		m3Client    metrics.Client
		persistence TaskManager
	}
)

var _ ShardManager = (*shardPersistenceClient)(nil)
var _ ExecutionManager = (*workflowExecutionPersistenceClient)(nil)
var _ TaskManager = (*taskPersistenceClient)(nil)

// NewShardPersistenceClient creates a client to manage shards
func NewShardPersistenceClient(persistence ShardManager, m3Client metrics.Client) ShardManager {
	return &shardPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

// NewWorkflowExecutionPersistenceClient creates a client to manage executions
func NewWorkflowExecutionPersistenceClient(persistence ExecutionManager, m3Client metrics.Client) ExecutionManager {
	return &workflowExecutionPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

// NewTaskPersistenceClient creates a client to manage tasks
func NewTaskPersistenceClient(persistence TaskManager, m3Client metrics.Client) TaskManager {
	return &taskPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

func (p *shardPersistenceClient) CreateShard(request *CreateShardRequest) error {
	p.m3Client.IncCounter(metrics.CreateShardScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.CreateShardScope, metrics.PersistenceLatency)
	err := p.persistence.CreateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ShardAlreadyExistError); ok {
			p.m3Client.IncCounter(metrics.CreateShardScope, metrics.PersistenceErrShardExistsCounter)
		} else {
			p.m3Client.IncCounter(metrics.CreateShardScope, metrics.PersistenceFailures)
		}
	}

	return err
}

func (p *shardPersistenceClient) GetShard(
	request *GetShardRequest) (*GetShardResponse, error) {
	p.m3Client.IncCounter(metrics.GetShardScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.GetShardScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetShard(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			p.m3Client.IncCounter(metrics.GetShardScope, metrics.CadenceErrEntityNotExistsCounter)
		default:
			p.m3Client.IncCounter(metrics.GetShardScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}

func (p *shardPersistenceClient) UpdateShard(request *UpdateShardRequest) error {
	p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateShardScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ShardOwnershipLostError); ok {
			p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.PersistenceErrShardOwnershipLostCounter)
		} else {
			p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.PersistenceFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.CreateWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *workflow.WorkflowExecutionAlreadyStartedError:
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.CadenceErrExecutionAlreadyStartedCounter)
		case *ShardOwnershipLostError:
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.PersistenceErrShardOwnershipLostCounter)
		case *ConditionFailedError:
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.PersistenceErrConditionFailedCounter)
		case *TimeoutError:
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.PersistenceErrTimeoutCounter)
		default:
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.GetWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.CadenceErrEntityNotExistsCounter)
		default:
			p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.UpdateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *ShardOwnershipLostError:
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.PersistenceErrShardOwnershipLostCounter)
		case *ConditionFailedError:
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.PersistenceErrConditionFailedCounter)
		case *TimeoutError:
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.PersistenceErrTimeoutCounter)
		default:
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.PersistenceFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.DeleteWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.PersistenceFailures)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.GetTransferTasksScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetTransferTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.PersistenceFailures)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTransferTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTransferTask(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.CadenceErrEntityNotExistsCounter)
		default:
			p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.PersistenceFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTimerIndexTasksScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.GetTimerIndexTasksScope, metrics.PersistenceLatency)
	resonse, err := p.persistence.GetTimerIndexTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTimerIndexTasksScope, metrics.PersistenceFailures)
	}

	return resonse, err
}

func (p *workflowExecutionPersistenceClient) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTimerTaskScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTimerTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTimerTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CompleteTimerTaskScope, metrics.PersistenceFailures)
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetWorkflowMutableState(
	request *GetWorkflowMutableStateRequest) (*GetWorkflowMutableStateResponse, error) {
	p.m3Client.IncCounter(metrics.GetWorkflowMutableStateScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.GetWorkflowMutableStateScope, metrics.PersistenceLatency)
	resonse, err := p.persistence.GetWorkflowMutableState(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			p.m3Client.IncCounter(metrics.GetWorkflowMutableStateScope, metrics.CadenceErrEntityNotExistsCounter)
		default:
			p.m3Client.IncCounter(metrics.GetWorkflowMutableStateScope, metrics.PersistenceFailures)
		}
	}

	return resonse, err
}

func (p *taskPersistenceClient) CreateTask(request *CreateTaskRequest) (*CreateTaskResponse, error) {
	p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.CreateTaskScope, metrics.PersistenceLatency)
	response, err := p.persistence.CreateTask(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *ConditionFailedError:
			p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.PersistenceErrConditionFailedCounter)
		default:
			p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTasksScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.GetTasksScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTasksScope, metrics.PersistenceFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(request *CompleteTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTaskScope, metrics.PersistenceLatency)
	err := p.persistence.CompleteTask(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *ConditionFailedError:
			p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.PersistenceErrConditionFailedCounter)
		default:
			p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.PersistenceFailures)
		}
	}

	return err
}

func (p *taskPersistenceClient) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	p.m3Client.IncCounter(metrics.LeaseTaskListScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.LeaseTaskListScope, metrics.PersistenceLatency)
	response, err := p.persistence.LeaseTaskList(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *ConditionFailedError:
			p.m3Client.IncCounter(metrics.LeaseTaskListScope, metrics.PersistenceErrConditionFailedCounter)
		default:
			p.m3Client.IncCounter(metrics.LeaseTaskListScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}

func (p *taskPersistenceClient) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	p.m3Client.IncCounter(metrics.UpdateTaskListScope, metrics.PersistenceRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateTaskListScope, metrics.PersistenceLatency)
	response, err := p.persistence.UpdateTaskList(request)
	sw.Stop()

	if err != nil {
		switch err.(type) {
		case *ConditionFailedError:
			p.m3Client.IncCounter(metrics.UpdateTaskListScope, metrics.PersistenceErrConditionFailedCounter)
		default:
			p.m3Client.IncCounter(metrics.UpdateTaskListScope, metrics.PersistenceFailures)
		}
	}

	return response, err
}
