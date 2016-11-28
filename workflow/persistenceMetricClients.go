package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/workflow/metrics"
)

type (
	workflowExecutionPersistenceClient struct {
		m3Client    common.Client
		persistence ExecutionPersistence
	}

	taskPersistenceClient struct {
		m3Client    common.Client
		persistence TaskPersistence
	}
)

// NewWorkflowExecutionPersistenceClient creates a client to manage executions
func NewWorkflowExecutionPersistenceClient(persistence ExecutionPersistence, m3Client common.Client) ExecutionPersistence {
	return &workflowExecutionPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

// NewTaskPersistenceClient creates a client to manage tasks
func NewTaskPersistenceClient(persistence TaskPersistence, m3Client common.Client) TaskPersistence {
	return &taskPersistenceClient{
		persistence: persistence,
		m3Client:    m3Client,
	}
}

func (p *workflowExecutionPersistenceClient) CreateWorkflowExecution(request *createWorkflowExecutionRequest) (*createWorkflowExecutionResponse, error) {
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

func (p *workflowExecutionPersistenceClient) GetWorkflowExecution(request *getWorkflowExecutionRequest) (*getWorkflowExecutionResponse, error) {
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

func (p *workflowExecutionPersistenceClient) UpdateWorkflowExecution(request *updateWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.UpdateWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*conditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.UpdateWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) DeleteWorkflowExecution(request *deleteWorkflowExecutionRequest) error {
	p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*conditionFailedError); !ok {
			p.m3Client.IncCounter(metrics.DeleteWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}

	return err
}

func (p *workflowExecutionPersistenceClient) GetTransferTasks(request *getTransferTasksRequest) (*getTransferTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTransferTasksScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.GetTransferTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTransferTasksScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *workflowExecutionPersistenceClient) CompleteTransferTask(request *completeTransferTaskRequest) error {
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

func (p *taskPersistenceClient) CreateTask(request *createTaskRequest) (*createTaskResponse, error) {
	p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CreateTaskScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.CreateTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CreateTaskScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) GetTasks(request *getTasksRequest) (*getTasksResponse, error) {
	p.m3Client.IncCounter(metrics.GetTasksScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.GetTasksScope, metrics.WorkflowLatencyTimer)
	response, err := p.persistence.GetTasks(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.GetTasksScope, metrics.WorkflowFailures)
	}

	return response, err
}

func (p *taskPersistenceClient) CompleteTask(request *completeTaskRequest) error {
	p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowRequests)

	sw := p.m3Client.StartTimer(metrics.CompleteTaskScope, metrics.WorkflowLatencyTimer)
	err := p.persistence.CompleteTask(request)
	sw.Stop()

	if err != nil {
		p.m3Client.IncCounter(metrics.CompleteTaskScope, metrics.WorkflowFailures)
	}

	return err
}
