package flow

import (
	"fmt"

	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	log "github.com/Sirupsen/logrus"
)

type (
	// CompletionHandler Handler to indicate completion result
	CompletionHandler func(result []byte, err Error)

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
		executeDecisions             []*gen.Decision          // Decisions made during the execute of the workflow
		completeHandler              CompletionHandler        // events completion handler
		contextLogger                *log.Entry
	}
)

func newWorkflowExecutionEventHandler(workflowInfo *WorkflowInfo, workflowDefinitionFactory WorkflowDefinitionFactory,
	completionHandler CompletionHandler, logger *log.Entry) *workflowExecutionEventHandler {
	context := &workflowContext{
		workflowInfo:                 workflowInfo,
		workflowDefinitionFactory:    workflowDefinitionFactory,
		scheduledActivites:           make(map[string]ResultHandler),
		scheduledEventIDToActivityID: make(map[int64]string),
		executeDecisions:             make([]*gen.Decision, 0),
		completeHandler:              completionHandler,
		contextLogger:                logger}
	return &workflowExecutionEventHandler{context, logger}
}

func (wc *workflowContext) WorkflowInfo() *WorkflowInfo {
	return wc.workflowInfo
}

func (wc *workflowContext) Complete(result []byte, err Error) {
	wc.completeHandler(result, err)
}

func (wc *workflowContext) GenerateActivityID() string {
	activityID := wc.counterID
	wc.counterID++
	return fmt.Sprintf("%d", activityID)
}

func (wc *workflowContext) SwapExecuteDecisions(decisions []*gen.Decision) []*gen.Decision {
	oldDecisions := wc.executeDecisions
	wc.executeDecisions = decisions
	return oldDecisions
}

func (wc *workflowContext) CreateNewDecision(decisionType gen.DecisionType) *gen.Decision {
	return &gen.Decision{
		DecisionType: common.DecisionTypePtr(decisionType),
	}
}

func (wc *workflowContext) ExecuteActivity(parameters ExecuteActivityParameters, callback ResultHandler) {
	scheduleTaskAttr := &gen.ScheduleActivityTaskDecisionAttributes{}
	if parameters.ActivityID == nil {
		scheduleTaskAttr.ActivityId = common.StringPtr(wc.GenerateActivityID())
	} else {
		scheduleTaskAttr.ActivityId = parameters.ActivityID
	}
	scheduleTaskAttr.ActivityType = common.ActivityTypePtr(parameters.ActivityType)
	scheduleTaskAttr.TaskList = common.TaskListPtr(gen.TaskList{Name: common.StringPtr(parameters.TaskListName)})
	scheduleTaskAttr.Input = parameters.Input
	scheduleTaskAttr.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(parameters.ScheduleToCloseTimeoutSeconds)
	scheduleTaskAttr.StartToCloseTimeoutSeconds = common.Int32Ptr(parameters.StartToCloseTimeoutSeconds)
	scheduleTaskAttr.ScheduleToStartTimeoutSeconds = common.Int32Ptr(parameters.ScheduleToStartTimeoutSeconds)

	decision := wc.CreateNewDecision(gen.DecisionType_ScheduleActivityTask)
	decision.ScheduleActivityTaskDecisionAttributes = scheduleTaskAttr

	wc.executeDecisions = append(wc.executeDecisions, decision)
	wc.scheduledActivites[scheduleTaskAttr.GetActivityId()] = callback
	// wc.contextLogger.Infof("Scheduling Activity: %+v", parameters.ActivityType.GetName())
}

func (weh *workflowExecutionEventHandler) ProcessEvent(event *gen.HistoryEvent) ([]*gen.Decision, error) {
	if event == nil {
		return nil, fmt.Errorf("nil event provided")
	}

	switch event.GetEventType() {
	case gen.EventType_WorkflowExecutionStarted:
		return weh.handleWorkflowExecutionStarted(event.WorkflowExecutionStartedEventAttributes)

	case gen.EventType_WorkflowExecutionCompleted:
		// No Operation
	case gen.EventType_WorkflowExecutionFailed:
		// No Operation
	case gen.EventType_WorkflowExecutionTimedOut:
		// TODO:
	case gen.EventType_DecisionTaskScheduled:
		// No Operation
	case gen.EventType_DecisionTaskStarted:
		// No Operation
	case gen.EventType_DecisionTaskTimedOut:
		// TODO:
	case gen.EventType_DecisionTaskCompleted:
		// TODO:
	case gen.EventType_ActivityTaskScheduled:
		attributes := event.ActivityTaskScheduledEventAttributes
		weh.scheduledEventIDToActivityID[event.GetEventId()] = attributes.GetActivityId()

	case gen.EventType_ActivityTaskStarted:
		// No Operation
	case gen.EventType_ActivityTaskCompleted:
		return weh.handleActivityTaskCompleted(event.ActivityTaskCompletedEventAttributes)

	case gen.EventType_ActivityTaskFailed:
		return weh.handleActivityTaskFailed(event.ActivityTaskFailedEventAttributes)

	case gen.EventType_ActivityTaskTimedOut:
		return weh.handleActivityTaskTimedOut(event.ActivityTaskTimedOutEventAttributes)

	case gen.EventType_TimerStarted:
		// TODO:
	case gen.EventType_TimerFired:
		// TODO:
	default:
		return nil, fmt.Errorf("missing event handler for event type: %v", event)
	}
	return nil, nil
}

func (weh *workflowExecutionEventHandler) Close() {
}

func (weh *workflowExecutionEventHandler) handleWorkflowExecutionStarted(
	attributes *gen.WorkflowExecutionStartedEventAttributes) ([]*gen.Decision, error) {
	workflowDefinition, err := weh.workflowDefinitionFactory(weh.workflowInfo.workflowType)
	if err != nil {
		return nil, err
	}

	// Invoke the workflow.
	workflowDefinition.Execute(weh, attributes.Input)
	return weh.SwapExecuteDecisions([]*gen.Decision{}), nil
}

func (weh *workflowExecutionEventHandler) handleActivityTaskCompleted(
	attributes *gen.ActivityTaskCompletedEventAttributes) ([]*gen.Decision, error) {

	activityID, ok := weh.scheduledEventIDToActivityID[attributes.GetScheduledEventId()]
	if !ok {
		return nil, fmt.Errorf("unable to find activity ID for the event: %v", attributes)
	}
	handler, ok := weh.scheduledActivites[activityID]
	if !ok {
		return nil, fmt.Errorf("unable to find callback handler for the event: %v with activity ID: %v", attributes, activityID)
	}

	if handler != nil {
		// Invoke the callback
		handler(attributes.GetResult_(), nil)
	}
	return weh.SwapExecuteDecisions([]*gen.Decision{}), nil
}

func (weh *workflowExecutionEventHandler) handleActivityTaskFailed(
	attributes *gen.ActivityTaskFailedEventAttributes) ([]*gen.Decision, error) {

	activityID, ok := weh.scheduledEventIDToActivityID[attributes.GetScheduledEventId()]
	if !ok {
		return nil, fmt.Errorf("unable to find activity ID for the event: %v", attributes)
	}
	handler, ok := weh.scheduledActivites[activityID]
	if !ok {
		return nil, fmt.Errorf("unable to find callback handler for the event: %v", attributes)
	}

	if handler != nil {
		err := &ActivityTaskFailedError{
			reason:  *attributes.Reason,
			details: attributes.Details}
		// Invoke the callback
		handler(nil, err)
	}
	return weh.SwapExecuteDecisions([]*gen.Decision{}), nil
}

func (weh *workflowExecutionEventHandler) handleActivityTaskTimedOut(
	attributes *gen.ActivityTaskTimedOutEventAttributes) ([]*gen.Decision, error) {

	activityID, ok := weh.scheduledEventIDToActivityID[attributes.GetScheduledEventId()]
	if !ok {
		return nil, fmt.Errorf("unable to find activity ID for the event: %v", attributes)
	}
	handler, ok := weh.scheduledActivites[activityID]
	if !ok {
		return nil, fmt.Errorf("unable to find callback handler for the event: %v", attributes)
	}

	if handler != nil {
		err := &ActivityTaskTimeoutError{TimeoutType: attributes.GetTimeoutType()}
		// Invoke the callback
		handler(nil, err)
	}
	return weh.SwapExecuteDecisions([]*gen.Decision{}), nil
}
