package main

import (
	"encoding/json"
	"time"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/test/flow"
	"code.uber.internal/devexp/minions/test/workflow"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uber/tchannel-go/thrift"
)

type (
	workflowConfig struct {
		runActivities          bool
		runDeciders            bool
		startCount             int
		panicWorkflow          bool
		runGreeterActivity     bool
		runNameActivity        bool
		runSayGreetingActivity bool
		useWorkflowID          string
		useRunID               string
		replayWorkflow         bool
	}

	workflowError struct {
		reason  string
		details []byte
	}

	// Operations exposed
	Operations interface {
		Name() string
		Greeting() string
		sayGreeting(greeting string)
	}
)

func activityInfo(activityName string) flow.ExecuteActivityParameters {
	return serializeParams(activityName, nil)
}

func activityInfoWithInput(activityName string, request *sayGreetingActivityRequest) flow.ExecuteActivityParameters {
	sayGreetInput, err := json.Marshal(request)
	if err != nil {
		log.Panicf("Marshalling failed with error: %+v", err)
	}
	return serializeParams(activityName, sayGreetInput)
}

func serializeParams(activityName string, input []byte) flow.ExecuteActivityParameters {
	return flow.ExecuteActivityParameters{
		TaskListName: "testTaskList" + "-" + activityName,
		ActivityType: m.ActivityType{Name: common.StringPtr(activityName)},
		Input:        input}
}

func (we *workflowError) Reason() string {
	return we.reason
}

func (we *workflowError) Details() []byte {
	return we.details
}

func (we *workflowError) Error() string {
	return we.reason
}

func replayWorkflow(service m.TChanWorkflowService, reporter common.Reporter, config *workflowConfig) {

	// greetingsWorkflow := workflow.NewWorkflowDefinition(greetingsWorkflow{})
	// workflowFactory := func(wt m.WorkflowType) (flow.WorkflowDefinition, flow.Error) {
	// 	switch wt.GetName() {
	// 	case "greetingsWorkflow":
	// 		return greetingsWorkflow, nil
	// 	case "panicWorkflow":
	// 		return workflow.NewWorkflowDefinition(panicWorkflow{}), nil
	// 	}
	// 	panic("Invalid workflow type")
	// }

	if config.replayWorkflow {

		ctx, cancel := thrift.NewContext(10 * time.Second)
		defer cancel()

		executionInfo := &m.WorkflowExecution{
			WorkflowId: common.StringPtr(config.useWorkflowID),
			RunId:      common.StringPtr(config.useRunID)}

		request := &m.GetWorkflowExecutionHistoryRequest{
			Execution: executionInfo}
		_, err := service.GetWorkflowExecutionHistory(ctx, request)
		if err != nil {
			log.Panicf("GetWorkflowExecutionHistory failed with error: %s", err.Error())
		}

		// taskHandler := flow.NewWorkflowTaskHandlerImpl("testTask", "testID", workflowFactory, logger, reporter)
		// workflowTask := &flow.WorkflowTask{Task: &m.PollForDecisionTaskResponse{
		// 	TaskToken:         []byte("test-token"),
		// 	History:           historyResponse.GetHistory(),
		// 	WorkflowExecution: executionInfo,
		// 	WorkflowType:      &m.WorkflowType{Name: common.StringPtr("greetingsWorkflow")},
		// }}

		// _, stackTrace, err := taskHandler.ProcessWorkflowTask(workflowTask, true)
		// if err != nil {
		// 	log.Panicf("TaskHandler::ProcessWorkflowTask failed with error: %s", err.Error())
		// }
		// log.Infof("ProcessWorkflowTask: Stack Trace: %s", stackTrace)

		return
	}
}

func launchDemoWorkflow(service m.TChanWorkflowService, reporter common.Reporter, config *workflowConfig) {
	logger := log.WithFields(log.Fields{})

	// Workflow execution parameters.
	workflowExecutionParameters := flow.WorkerExecutionParameters{}
	workflowExecutionParameters.TaskListName = "testTaskList"
	workflowExecutionParameters.ConcurrentPollRoutineSize = 4

	workflowFactory := func(wt m.WorkflowType) (flow.WorkflowDefinition, flow.Error) {
		switch wt.GetName() {
		case "greetingsWorkflow":
			return workflow.NewWorkflowDefinition(greetingsWorkflow{}), nil
		case "panicWorkflow":
			return workflow.NewWorkflowDefinition(panicWorkflow{}), nil
		}
		panic("Invalid workflow type")
	}

	activityFactory := func(at m.ActivityType) (flow.ActivityImplementation, flow.Error) {
		switch at.GetName() {
		case "getGreetingActivity":
			return getGreetingActivity{}, nil
		case "getNameActivity":
			return getNameActivity{}, nil
		case "sayGreetingActivity":
			return sayGreetingActivity{}, nil
		case "simpleActivity":
			return simpleActivity{}, nil
		}
		panic("Invalid activity type")
	}

	if config.runDeciders {
		// Launch worker.
		workflowWorker := flow.NewWorkflowWorker(workflowExecutionParameters, workflowFactory, service, logger, reporter)
		workflowWorker.Start()
		log.Infoln("Started Deciders for worklows.")
	}

	taskListActivitySuffix := ""
	switch {
	case config.runGreeterActivity:
		taskListActivitySuffix = "-getGreetingActivity"
	case config.runNameActivity:
		taskListActivitySuffix = "-getNameActivity"
	case config.runSayGreetingActivity:
		taskListActivitySuffix = "-sayGreetingActivity"
	}

	// Create activity execution parameters.
	activityExecutionParameters := flow.WorkerExecutionParameters{}
	activityExecutionParameters.TaskListName = "testTaskList" + taskListActivitySuffix
	activityExecutionParameters.ConcurrentPollRoutineSize = 10

	if config.runActivities {
		// Register activity instances and launch the worker.
		activityWorker := flow.NewActivityWorker(activityExecutionParameters, activityFactory, service, logger, reporter)
		activityWorker.Start()
		log.Infoln("Started activities for workflows.")
	}

	startedWorkflowsCount := 0

	workflowName := "greetingsWorkflow"
	if config.panicWorkflow {
		workflowName = "panicWorkflow"
	}

	for i := 0; i < config.startCount; i++ {
		// Start a workflow.
		workflowID := config.useWorkflowID
		if i > 0 {
			workflowID = uuid.New()
		}
		workflowOptions := flow.StartWorkflowOptions{
			WorkflowID:                             workflowID,
			WorkflowType:                           m.WorkflowType{Name: common.StringPtr(workflowName)},
			TaskListName:                           "testTaskList",
			WorkflowInput:                          nil,
			ExecutionStartToCloseTimeoutSeconds:    10,
			DecisionTaskStartToCloseTimeoutSeconds: 10,
		}
		workflowClient := flow.NewWorkflowClient(workflowOptions, service, reporter)
		_, err := workflowClient.StartWorkflowExecution()
		if err != nil {
			panic("Failed to create workflow.")
		} else {
			startedWorkflowsCount++
		}
	}

	if startedWorkflowsCount > 0 {
		log.Infof("Started %d %s.\n", startedWorkflowsCount, workflowName)
	}
}
