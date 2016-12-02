package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/persistence"
)

type (
	// TestBaseOptions options to configure workflow test base.
	// For now just wraps the TestBaseOptions in the persistence package.
	TestBaseOptions struct {
		persistence.TestBaseOptions
	}

	// TestBase wraps the base setup needed to create workflows over engine layer.
	// For now just wraps the TestBase in persistence package.
	TestBase struct {
		persistence.TestBase
	}
)

func addWorkflowExecutionStartedEvent(builder *historyBuilder, workflowID, workflowType, taskList string, input []byte,
	executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) *workflow.HistoryEvent {
	e := builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(workflowID),
		WorkflowType: &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskScheduledEvent(builder *historyBuilder, taskList string,
	timeout int32) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskScheduledEvent(taskList, timeout)

	return e
}

func addDecisionTaskStartedEvent(builder *historyBuilder, scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskStartedEvent(scheduleID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskCompletedEvent(builder *historyBuilder, scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	})

	return e
}

func addActivityTaskScheduledEvent(builder *historyBuilder, decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) *workflow.HistoryEvent {
	e := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:   common.StringPtr(activityID),
		ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(hearbeatTimeout),
	})

	return e
}

func addActivityTaskStartedEvent(builder *historyBuilder, scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskStartedEvent(scheduleID, &workflow.PollForActivityTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityTaskCompletedEvent(builder *historyBuilder, scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result_:  result,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityTaskFailedEvent(builder *historyBuilder, scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return e
}
