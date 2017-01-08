package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/metrics"
	"code.uber.internal/devexp/minions/persistence"
)

type (
	workflowExecutionPersistenceClient struct {
		m3Client    metrics.Client
		persistence persistence.ExecutionManager
	}

	taskPersistenceClient struct {
		m3Client    metrics.Client
		persistence persistence.TaskManager
	}
)

// NewWorkflowExecutionPersistenceClient creates a client to manage executions
func NewWorkflowExecutionPersistenceClient(persistence persistence.ExecutionManager, m3Client metrics.Client) persistence.ExecutionManager {
	return &workflowExecutionPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

// NewTaskPersistenceClient creates a client to manage tasks
func NewTaskPersistenceClient(persistence persistence.TaskManager, m3Client metrics.Client) persistence.TaskManager {
	return &taskPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

func (p *workflowExecutionPersistenceClient) CreateShard(request *persistence.CreateShardRequest) error {
	p.m3Client.IncCounter(metrics.CreateShardScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateShardScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.CreateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*persistence.ShardAlreadyExistError); !ok {
			p.m3Client.IncCounter(metrics.CreateShardScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetShard(
	request *persistence.GetShardRequest) (*persistence.GetShardResponse, error) {
	p.m3Client.IncCounter(metrics.GetShardScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetShardScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.GetShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			p.m3Client.IncCounter(metrics.GetShardScope, metrics.WorkflowFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) UpdateShard(request *persistence.UpdateShardRequest) error {
	p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateShardScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.UpdateShard(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*persistence.ConditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.UpdateShardScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
	p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.CreateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.WorkflowExecutionAlreadyStartedError); !ok {
			p.m3Client.IncCounter(metrics.CreateWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) GetWorkflowExecution(request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.GetWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			p.m3Client.IncCounter(metrics.GetWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.UpdateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*persistence.ConditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteWorkflowExecution(request *persistence.DeleteWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*persistence.ConditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTransferTasks(request *persistence.GetTransferTasksRequest) (*persistence.GetTransferTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTransferTasksScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.GetTransferTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) CompleteTransferTask(request *persistence.CompleteTransferTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTransferTaskScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.CompleteTransferTask(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			p.m3Client.IncCounter(metrics.CompleteTransferTaskScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTimerIndexTasks(request *persistence.GetTimerIndexTasksRequest) (*persistence.GetTimerIndexTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTimerIndexTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTimerIndexTasksScope, metrics.WorkflowLatencyTimer)
	resonse, err := p.persistence.GetTimerIndexTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTimerIndexTasksScope, metrics.WorkflowFailures)
	}

	return resonse, err
}

func (p *taskPersistenceClient) CreateTask(request *persistence.CreateTaskRequest) (*persistence.CreateTaskResponse, error) {
	p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateTaskScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.CreateTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTasksScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.GetTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTasksScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(request *persistence.CompleteTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTaskScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.CompleteTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowFailures)
	}

	return err
}
