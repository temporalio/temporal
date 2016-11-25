package flow

import (
	"fmt"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	log "github.com/Sirupsen/logrus"
)

type (
	// CompletionHandler Handler to indicate completion result
	CompletionHandler func(result []byte)

	// FailureHandler Handler to indicate failure of the events.
	FailureHandler func(reason string, details []byte)

	// workflowExecutionEventHandler handler to handle workflowExecutionEventHandler
	workflowExecutionEventHandler struct {
		*workflowContext
		contextLogger *log.Entry
	}

	// workflowContext an implementation of WorkflowContext represents a context for workflow execution.
	workflowContext struct {
		workflowInfo              *WorkflowInfo
		workflowDefinitionFactory WorkflowDefinitionFactory

		scheduledActivites           map[string]ResultHandler // Map of Activities(activity ID ->) and their response handlers
		scheduledEventIDToActivityID map[int64]string         // Mapping from scheduled event ID to activity ID
		counterID                    int32                    // To generate activity IDs
		executeStartDecisions        []*m.Decision            // Decisions made during the execute of the workflow
		completeHandler              CompletionHandler        // events completion handler
		failureHandler               FailureHandler           // events failure handler
	}
)

func newWorkflowExecutionEventHandler(workflowInfo *WorkflowInfo, workflowDefinitionFactory WorkflowDefinitionFactory,
	completionHandler CompletionHandler, failureHandler FailureHandler, logger *log.Entry) *workflowExecutionEventHandler {
	context := &workflowContext{
		workflowInfo:                 workflowInfo,
		workflowDefinitionFactory:    workflowDefinitionFactory,
		scheduledActivites:           make(map[string]ResultHandler),
		scheduledEventIDToActivityID: make(map[int64]string),
		executeStartDecisions:        make([]*m.Decision, 0),
		completeHandler:              completionHandler,
		failureHandler:               failureHandler}
	return &workflowExecutionEventHandler{context, logger}
}

func (wc *workflowContext) WorkflowInfo() *WorkflowInfo {
	return wc.workflowInfo
}

func (wc *workflowContext) Complete(result []byte) {
	wc.completeHandler(result)
}

func (wc *workflowContext) Fail(reason string, details []byte) {
	wc.failureHandler(reason, details)
}

func (wc *workflowContext) GenerateActivityID() string {
	activityID := wc.counterID
	wc.counterID++
	return fmt.Sprintf("%d", activityID)
}

func (wc *workflowContext) ExecutionStartDecisions() []*m.Decision {
	return wc.executeStartDecisions
}

func (wc *workflowContext) CreateNewDecision(decisionType m.DecisionType) *m.Decision {
	return &m.Decision{
		DecisionType: common.DecisionTypePtr(decisionType),
	}
}

func (wc *workflowContext) ScheduleActivityTask(parameters ExecuteActivityParameters, callback ResultHandler) {
	scheduleTaskAttr := &m.ScheduleActivityTaskDecisionAttributes{}
	if parameters.ActivityID == nil {
		scheduleTaskAttr.ActivityId = common.StringPtr(wc.GenerateActivityID())
	} else {
		scheduleTaskAttr.ActivityId = parameters.ActivityID
	}
	scheduleTaskAttr.ActivityType = common.ActivityTypePtr(parameters.ActivityType)
	scheduleTaskAttr.TaskList = common.TaskListPtr(m.TaskList{Name: common.StringPtr(parameters.TaskListName)})
	scheduleTaskAttr.Input = parameters.Input
	scheduleTaskAttr.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(parameters.ScheduleToCloseTimeoutSeconds)
	scheduleTaskAttr.StartToCloseTimeoutSeconds = common.Int32Ptr(parameters.StartToCloseTimeoutSeconds)
	scheduleTaskAttr.ScheduleToStartTimeoutSeconds = common.Int32Ptr(parameters.ScheduleToStartTimeoutSeconds)

	decision := wc.CreateNewDecision(m.DecisionType_ScheduleActivityTask)
	decision.ScheduleActivityTaskDecisionAttributes = scheduleTaskAttr

	wc.executeStartDecisions = append(wc.executeStartDecisions, decision)
	wc.scheduledActivites[scheduleTaskAttr.GetActivityId()] = callback

	fmt.Printf("Schedule Activity ID: %v \n", *scheduleTaskAttr.ActivityId)
}

func (weh *workflowExecutionEventHandler) ProcessEvent(event *m.HistoryEvent) ([]*m.Decision, error) {
	if event == nil {
		return nil, fmt.Errorf("Nil event provided.")
	}

	switch event.GetEventType() {
	case m.EventType_WorkflowExecutionStarted:
		return weh.handleWorkflowExecutionStarted(event.WorkflowExecutionStartedEventAttributes)

	case m.EventType_WorkflowExecutionCompleted:
		// No Operation
	case m.EventType_WorkflowExecutionFailed:
		// No Operation
	case m.EventType_WorkflowExecutionTimedOut:
		// TODO:
	case m.EventType_DecisionTaskScheduled:
		// No Operation
	case m.EventType_DecisionTaskStarted:
		// No Operation
	case m.EventType_DecisionTaskTimedOut:
		// TODO:
	case m.EventType_DecisionTaskCompleted:
		// TODO:
	case m.EventType_ActivityTaskScheduled:
		attributes := event.ActivityTaskScheduledEventAttributes
		weh.scheduledEventIDToActivityID[event.GetEventId()] = attributes.GetActivityId()

	case m.EventType_ActivityTaskStarted:
		// No Operation
	case m.EventType_ActivityTaskCompleted:
		return weh.handleActivityTaskCompleted(event.ActivityTaskCompletedEventAttributes)

	case m.EventType_ActivityTaskFailed:
		return weh.handleActivityTaskFailed(event.ActivityTaskFailedEventAttributes)

	case m.EventType_ActivityTaskTimedOut:
		return weh.handleActivityTaskTimedOut(event.ActivityTaskTimedOutEventAttributes)

	case m.EventType_TimerStarted:
		// TODO:
	case m.EventType_TimerFired:
		// TODO:
	default:
		return nil, fmt.Errorf("Missing event handler for event type: %v", event)
	}
	return nil, nil
}

func (weh *workflowExecutionEventHandler) Close() {
}

func (weh *workflowExecutionEventHandler) handleWorkflowExecutionStarted(
	attributes *m.WorkflowExecutionStartedEventAttributes) ([]*m.Decision, error) {
	workflowDefinition, err := weh.workflowDefinitionFactory(weh.workflowInfo.workflowType)
	if err != nil {
		return nil, err
	}

	// Invoke the workflow.
	workflowDefinition.Execute(weh, attributes.Input)
	return weh.ExecutionStartDecisions(), nil
}

func (weh *workflowExecutionEventHandler) handleActivityTaskCompleted(
	attributes *m.ActivityTaskCompletedEventAttributes) ([]*m.Decision, error) {

	activityID, ok := weh.scheduledEventIDToActivityID[attributes.GetScheduledEventId()]
	if !ok {
		return nil, fmt.Errorf("Unable to find activity ID for the event: %v", attributes)
	}
	handler, ok := weh.scheduledActivites[activityID]
	if !ok {
		return nil, fmt.Errorf("Unable to find callback handler for the event: %v with activity ID: %v", attributes, activityID)
	}

	if handler != nil {
		// Invoke the callback
		handler(nil, attributes.GetResult_())
	}
	return nil, nil
}

func (weh *workflowExecutionEventHandler) handleActivityTaskFailed(
	attributes *m.ActivityTaskFailedEventAttributes) ([]*m.Decision, error) {

	activityID, ok := weh.scheduledEventIDToActivityID[attributes.GetScheduledEventId()]
	if !ok {
		return nil, fmt.Errorf("Unable to find activity ID for the event: %v", attributes)
	}
	handler, ok := weh.scheduledActivites[activityID]
	if !ok {
		return nil, fmt.Errorf("Unable to find callback handler for the event: %v", attributes)
	}

	if handler != nil {
		err := &ActivityTaskFailedError{
			Reason:  *attributes.Reason,
			Details: attributes.Details}
		// Invoke the callback
		handler(err, nil)
	}
	return nil, nil
}

func (weh *workflowExecutionEventHandler) handleActivityTaskTimedOut(
	attributes *m.ActivityTaskTimedOutEventAttributes) ([]*m.Decision, error) {

	activityID, ok := weh.scheduledEventIDToActivityID[attributes.GetScheduledEventId()]
	if !ok {
		return nil, fmt.Errorf("Unable to find activity ID for the event: %v", attributes)
	}
	handler, ok := weh.scheduledActivites[activityID]
	if !ok {
		return nil, fmt.Errorf("Unable to find callback handler for the event: %v", attributes)
	}

	if handler != nil {
		err := &ActivityTaskTimeoutError{TimeoutType: attributes.GetTimeoutType()}
		// Invoke the callback
		handler(err, nil)
	}
	return nil, nil
}
