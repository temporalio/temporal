package main

import (
	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/health/driver"
	"code.uber.internal/devexp/minions/test/flow"
	"code.uber.internal/devexp/minions/test/workflow"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
)

type (
	workflowConfig struct {
		runActivities bool
		runDeciders   bool
		startCount    int
		panicWorkflow bool
	}
)

func launchDemoWorkflow(service *driver.ServiceMockEngine, reporter common.Reporter, config *workflowConfig) {
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
		log.Infoln("Started Deciders greetingsWorkflow Worker.")
	}

	// Create activity execution parameters.
	activityExecutionParameters := flow.WorkerExecutionParameters{}
	activityExecutionParameters.TaskListName = "testTaskList"
	activityExecutionParameters.ConcurrentPollRoutineSize = 10

	if config.runActivities {
		// Register activity instances and launch the worker.
		activityWorker := flow.NewActivityWorker(activityExecutionParameters, activityFactory, service, logger, reporter)
		activityWorker.Start()
		log.Infoln("Started Activities Worker.")
	}

	startedWorkflowsCount := 0

	workflowName := "greetingsWorkflow"
	if config.panicWorkflow {
		workflowName = "panicWorkflow"
	}

	for i := 0; i < config.startCount; i++ {
		// Start a workflow.
		workflowOptions := flow.StartWorkflowOptions{
			WorkflowID:                             uuid.New(),
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
