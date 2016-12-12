package flow

import (
	"fmt"
	"time"

	"github.com/uber/tchannel-go/thrift"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

type (
	// workflowTaskHandler is the implementation of WorkflowTaskHandler
	workflowTaskHandler struct {
		taskListName       string
		identity           string
		workflowDefFactory WorkflowDefinitionFactory
		contextLogger      *log.Entry
		reporter           common.Reporter
	}

	// activityTaskHandler is the implementation of ActivityTaskHandler
	activityTaskHandler struct {
		taskListName        string
		identity            string
		activityImplFactory ActivityImplementationFactory
		service             m.TChanWorkflowService
		contextLogger       *log.Entry
		reporter            common.Reporter
	}

	// eventsHelper wrapper method to help information about events.
	eventsHelper struct {
		workflowTask *WorkflowTask
	}

	// activityExecutionContext an implementation of ActivityExecutionContext represents a context for workflow execution.
	activityExecutionContext struct {
		taskToken []byte
		identity  string
		service   m.TChanWorkflowService
	}

	// ActivityTaskFailedError wraps the details of the failure of activity
	ActivityTaskFailedError struct {
		reason  string
		details []byte
	}

	// ActivityTaskTimeoutError wraps the details of the timeout of activity
	ActivityTaskTimeoutError struct {
		TimeoutType m.TimeoutType
	}
)

// Error from error.Error
func (e ActivityTaskFailedError) Error() string {
	return fmt.Sprintf("Reason: %s, Details: %s", e.reason, e.details)
}

// Details of the error
func (e ActivityTaskFailedError) Details() []byte {
	return e.details
}

// Reason of the error
func (e ActivityTaskFailedError) Reason() string {
	return e.reason
}

// Error from error.Error
func (e ActivityTaskTimeoutError) Error() string {
	return fmt.Sprintf("TimeoutType: %v", e.TimeoutType)
}

// Details of the error
func (e ActivityTaskTimeoutError) Details() []byte {
	return nil
}

// Reason of the error
func (e ActivityTaskTimeoutError) Reason() string {
	return e.Error()
}

// Get last non replayed event ID.
func (eh eventsHelper) LastNonReplayedID() int64 {
	if eh.workflowTask.task.PreviousStartedEventId == nil {
		return 0
	}
	return *eh.workflowTask.task.PreviousStartedEventId
}

// newWorkflowTaskHandler returns an implementation of workflow task handler.
func newWorkflowTaskHandler(taskListName string, identity string, factory WorkflowDefinitionFactory,
	contextLogger *log.Entry, reporter common.Reporter) *workflowTaskHandler {
	return &workflowTaskHandler{
		taskListName:       taskListName,
		identity:           identity,
		workflowDefFactory: factory,
		contextLogger:      contextLogger,
		reporter:           reporter}
}

// ProcessWorkflowTask processes each all the events of the workflow task.
func (wth *workflowTaskHandler) ProcessWorkflowTask(workflowTask *WorkflowTask) (*m.RespondDecisionTaskCompletedRequest, error) {
	if workflowTask == nil {
		return nil, fmt.Errorf("nil workflowtask provided")
	}

	// wth.reporter.IncCounter(common.DecisionsTotalCounter, nil, 1)
	// wth.contextLogger.Debugf("Processing New Workflow Task: Type=%s, PreviousStartedEventId=%d",
	// 	workflowTask.task.GetWorkflowType().GetName(), workflowTask.task.GetPreviousStartedEventId())

	// Setup workflow Info
	workflowInfo := &WorkflowInfo{
		workflowType: *workflowTask.task.WorkflowType,
		taskListName: wth.taskListName,
		// workflowExecution
	}

	isWorkflowCompleted := false
	var completionResult []byte
	var failure Error

	completionHandler := func(result []byte, err Error) {
		completionResult = result
		failure = err
		isWorkflowCompleted = true
	}

	eventHandler := newWorkflowExecutionEventHandler(
		workflowInfo, wth.workflowDefFactory, completionHandler, wth.contextLogger)
	helperEvents := &eventsHelper{workflowTask: workflowTask}
	history := workflowTask.task.History
	decisions := []*m.Decision{}

	startTime := time.Now()

	// Process events
	for _, event := range history.Events {
		// wth.contextLogger.Debugf("ProcessWorkflowTask: Id=%d, Event=%+v", event.GetEventId(), event)
		if event.GetEventType() == m.EventType_WorkflowExecutionStarted {
			startTime = time.Unix(0, event.GetTimestamp())
		}
		eventDecisions, err := eventHandler.ProcessEvent(event)
		if err != nil {
			return nil, err
		}
		if event.GetEventId() >= helperEvents.LastNonReplayedID() {
			if eventDecisions != nil {
				decisions = append(decisions, eventDecisions...)
				for _, d := range decisions {
					if d.GetDecisionType() == m.DecisionType_ScheduleActivityTask {
						wth.contextLogger.Infof("[WorkflowID: %s] Scheduling Activity: %s",
							workflowTask.task.GetWorkflowExecution().GetWorkflowId(),
							d.GetScheduleActivityTaskDecisionAttributes().GetActivityType().GetName())
					}
				}
			}
		}
	}

	eventDecisions := wth.completeWorkflow(isWorkflowCompleted, completionResult, failure)
	if len(eventDecisions) > 0 {
		decisions = append(decisions, eventDecisions...)

		wth.reporter.IncCounter(common.WorkflowsCompletionTotalCounter, nil, 1)
		elapsed := time.Now().Sub(startTime)
		wth.reporter.RecordTimer(common.WorkflowEndToEndLatency, nil, elapsed)
	}

	// Fill the response.
	taskCompletionRequest := &m.RespondDecisionTaskCompletedRequest{
		TaskToken: workflowTask.task.TaskToken,
		Decisions: decisions,
		Identity:  common.StringPtr(wth.identity),
		// ExecutionContext:
	}
	return taskCompletionRequest, nil
}

func (wth *workflowTaskHandler) completeWorkflow(isWorkflowCompleted bool, completionResult []byte,
	err Error) []*m.Decision {
	decisions := []*m.Decision{}
	if err != nil {
		// Workflow failures
		failDecision := createNewDecision(m.DecisionType_FailWorkflowExecution)
		failDecision.FailWorkflowExecutionDecisionAttributes = &m.FailWorkflowExecutionDecisionAttributes{
			Reason:  common.StringPtr(err.Reason()),
			Details: err.Details(),
		}
		decisions = append(decisions, failDecision)
	} else if isWorkflowCompleted {
		// Workflow completion
		completeDecision := createNewDecision(m.DecisionType_CompleteWorkflowExecution)
		completeDecision.CompleteWorkflowExecutionDecisionAttributes = &m.CompleteWorkflowExecutionDecisionAttributes{
			Result_: completionResult,
		}
		decisions = append(decisions, completeDecision)
	}
	return decisions
}

func newActivityTaskHandler(taskListName string, identity string, factory ActivityImplementationFactory,
	service m.TChanWorkflowService, contextLogger *log.Entry, reporter common.Reporter) ActivityTaskHandler {
	return &activityTaskHandler{
		taskListName:        taskListName,
		identity:            identity,
		activityImplFactory: factory,
		service:             service,
		contextLogger:       contextLogger,
		reporter:            reporter}
}

// Execute executes an implementation of the activity.
func (ath *activityTaskHandler) Execute(context context.Context, activityTask *ActivityTask) (interface{}, error) {
	ath.contextLogger.Infof("[WorkflowID: %s] Execute Activity: %s",
		activityTask.task.GetWorkflowExecution().GetWorkflowId(), activityTask.task.GetActivityType().GetName())
	//ath.reporter.IncCounter(common.ActivitiesTotalCounter, nil, 1)

	activityExecutionContext := &activityExecutionContext{
		taskToken: activityTask.task.TaskToken,
		identity:  ath.identity,
		service:   ath.service}
	activityImplementation, err := ath.activityImplFactory(*activityTask.task.GetActivityType())
	if err != nil {
		// Couldn't find the activity implementation.
		return nil, err
	}

	output, err := activityImplementation.Execute(activityExecutionContext, activityTask.task.GetInput())
	if err != nil {
		responseFailure := &m.RespondActivityTaskFailedRequest{
			TaskToken: activityTask.task.TaskToken,
			Reason:    common.StringPtr(err.Reason()),
			Details:   err.Details(),
			Identity:  common.StringPtr(ath.identity)}
		return responseFailure, nil
	}

	responseComplete := &m.RespondActivityTaskCompletedRequest{
		TaskToken: activityTask.task.TaskToken,
		Result_:   output,
		Identity:  common.StringPtr(ath.identity)}
	return responseComplete, nil
}

func (aec *activityExecutionContext) TaskToken() []byte {
	return aec.taskToken
}

func (aec *activityExecutionContext) RecordActivityHeartbeat(details []byte) error {
	request := &m.RecordActivityTaskHeartbeatRequest{
		TaskToken: aec.TaskToken(),
		Details:   details,
		Identity:  common.StringPtr(aec.identity)}

	err := backoff.Retry(
		func() error {
			ctx, cancel := thrift.NewContext(serviceTimeOut)
			defer cancel()

			// TODO: Handle the propagation of Cancel to activity.
			_, err2 := aec.service.RecordActivityTaskHeartbeat(ctx, request)
			return err2
		}, serviceOperationRetryPolicy, isServiceTransientError)
	return err
}

func createNewDecision(decisionType m.DecisionType) *m.Decision {
	return &m.Decision{
		DecisionType: common.DecisionTypePtr(decisionType),
	}
}
