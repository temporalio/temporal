package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/metrics"
)

type (
	engineWithMetricsImpl struct {
		m3Client metrics.Client
		engine   Engine
	}
)

// Assert that structs do indeed implement the interfaces
var _ Engine = (*engineWithMetricsImpl)(nil)

// NewEngineWithMetricsImpl creates an engine with metrics
func NewEngineWithMetricsImpl(engine Engine, m3Client metrics.Client) Engine {
	return &engineWithMetricsImpl{
		engine:   engine,
		m3Client: m3Client,
	}
}

func (e *engineWithMetricsImpl) StartWorkflowExecution(
	request *workflow.StartWorkflowExecutionRequest) (workflow.WorkflowExecution, error) {
	e.m3Client.IncCounter(metrics.StartWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.StartWorkflowExecutionScope, metrics.WorkflowLatencyTimer)
	resp, err := e.engine.StartWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.WorkflowExecutionAlreadyStartedError); !ok {
			e.m3Client.IncCounter(metrics.StartWorkflowExecutionScope, metrics.WorkflowFailures)
		}
	}
	return resp, err
}

func (e *engineWithMetricsImpl) PollForDecisionTask(
	request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error) {
	e.m3Client.IncCounter(metrics.PollForDecisionTaskScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.PollForDecisionTaskScope, metrics.WorkflowLatencyTimer)
	resp, err := e.engine.PollForDecisionTask(request)
	sw.Stop()

	if err != nil {
		e.m3Client.IncCounter(metrics.PollForDecisionTaskScope, metrics.WorkflowFailures)
	}
	return resp, err
}

func (e *engineWithMetricsImpl) PollForActivityTask(
	request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error) {
	e.m3Client.IncCounter(metrics.PollForActivityTaskScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.PollForActivityTaskScope, metrics.WorkflowLatencyTimer)
	resp, err := e.engine.PollForActivityTask(request)
	sw.Stop()

	if err != nil {
		e.m3Client.IncCounter(metrics.PollForActivityTaskScope, metrics.WorkflowFailures)
	}
	return resp, err
}

func (e *engineWithMetricsImpl) RespondDecisionTaskCompleted(
	request *workflow.RespondDecisionTaskCompletedRequest) error {
	e.m3Client.IncCounter(metrics.RespondDecisionTaskCompletedScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.RespondDecisionTaskCompletedScope, metrics.WorkflowLatencyTimer)
	err := e.engine.RespondDecisionTaskCompleted(request)
	sw.Stop()

	if err != nil {
		e.m3Client.IncCounter(metrics.RespondDecisionTaskCompletedScope, metrics.WorkflowFailures)
	}
	return err
}

func (e *engineWithMetricsImpl) RespondActivityTaskCompleted(
	request *workflow.RespondActivityTaskCompletedRequest) error {
	e.m3Client.IncCounter(metrics.RespondActivityTaskCompletedScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.RespondActivityTaskCompletedScope, metrics.WorkflowLatencyTimer)
	err := e.engine.RespondActivityTaskCompleted(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			e.m3Client.IncCounter(metrics.RespondActivityTaskCompletedScope, metrics.WorkflowFailures)
		}
	}
	return err
}

func (e *engineWithMetricsImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	e.m3Client.IncCounter(metrics.RespondActivityTaskFailedScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.RespondActivityTaskFailedScope, metrics.WorkflowLatencyTimer)
	err := e.engine.RespondActivityTaskFailed(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			e.m3Client.IncCounter(metrics.RespondActivityTaskFailedScope, metrics.WorkflowFailures)
		}
	}
	return err
}

func (e *engineWithMetricsImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	e.m3Client.IncCounter(metrics.GetWorkflowExecutionHistoryScope, metrics.WorkflowRequests)

	sw := e.m3Client.StartTimer(metrics.GetWorkflowExecutionHistoryScope, metrics.WorkflowLatencyTimer)
	resp, err := e.engine.GetWorkflowExecutionHistory(request)
	sw.Stop()

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			e.m3Client.IncCounter(metrics.GetWorkflowExecutionHistoryScope, metrics.WorkflowFailures)
		}
	}

	return resp, err
}

func (e *engineWithMetricsImpl) Start() {
	e.engine.Start()
}

func (e *engineWithMetricsImpl) Stop() {
	e.engine.Stop()
}
