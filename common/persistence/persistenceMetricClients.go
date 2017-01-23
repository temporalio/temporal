package persistence

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/metrics"
)

type (
	workflowExecutionPersistenceClient struct {
		m3Client    metrics.Client
		persistence ExecutionManager
	}

	taskPersistenceClient struct {
		m3Client    metrics.Client
		persistence TaskManager
	}
)

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

func (p *workflowExecutionPersistenceClient) CreateShard(request *CreateShardRequest) error {
	p.m3Client.IncCounter(metrics.CreateShardScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateShardScope, metrics.WorkflowLatencyTimer)
	err := p.CreateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ShardAlreadyExistError); !ok {
			p.m3Client.IncCounter(metrics.CreateShardScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetShard(
	request *GetShardRequest) (*GetShardResponse, error) {
	p.m3Client.IncCounter(metrics.GetShardScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetShardScope, metrics.WorkflowLatencyTimer)
	response, err := p.GetShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			p.m3Client.IncCounter(metrics.GetShardScope, metrics.WorkflowFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) UpdateShard(request *UpdateShardRequest) error {
	p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateShardScope, metrics.WorkflowLatencyTimer)
	err := p.UpdateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ConditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	response, err := p.CreateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.WorkflowExecutionAlreadyStartedError); !ok {
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	response, err := p.GetWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	err := p.UpdateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ConditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	err := p.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*ConditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTransferTasksScope, metrics.WorkflowLatencyTimer)
	response, err := p.GetTransferTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTransferTaskScope, metrics.WorkflowLatencyTimer)
	err := p.CompleteTransferTask(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTimerIndexTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTimerIndexTasksScope, metrics.WorkflowLatencyTimer)
	resonse, err := p.GetTimerIndexTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTimerIndexTasksScope, metrics.WorkflowFailures)
	}

	return resonse, err
}

func (p *workflowExecutionPersistenceClient) GetWorkflowMutableState(request *GetWorkflowMutableStateRequest) (*GetWorkflowMutableStateResponse, error) {
	sw := p.m3Client.StartTimer(metrics.GetWorkflowMutableStateScope, metrics.WorkflowLatencyTimer)
	resonse, err := p.GetWorkflowMutableState(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetWorkflowMutableStateScope, metrics.WorkflowFailures)
	}

	return resonse, err
}

func (p *taskPersistenceClient) CreateTask(request *CreateTaskRequest) (*CreateTaskResponse, error) {
	p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateTaskScope, metrics.WorkflowLatencyTimer)
	response, err := p.CreateTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTasksScope, metrics.WorkflowLatencyTimer)
	response, err := p.GetTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTasksScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(request *CompleteTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTaskScope, metrics.WorkflowLatencyTimer)
	err := p.CompleteTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowFailures)
	}

	return err
}

func (p *taskPersistenceClient) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	p.m3Client.IncCounter(metrics.LeaseTaskListScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.LeaseTaskListScope, metrics.WorkflowLatencyTimer)
	response, err := p.LeaseTaskList(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowFailures)
	}

	return response, err
}
