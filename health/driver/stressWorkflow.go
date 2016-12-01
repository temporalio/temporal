package driver

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/test/flow"
	log "github.com/Sirupsen/logrus"
)

type (
	// WorkflowParams inputs to workflow.
	WorkflowParams struct {
		ChainSequence    int
		ActivitySleepMin time.Duration
		ActivitySleepMax time.Duration
	}

	// Stress workflow decider
	stressWorkflow struct {
	}

	// Sleep activity.
	stressSleepActivity struct {
	}

	sleepActivityParams struct {
		sleepMin time.Duration
		sleepMax time.Duration
	}
)

func logrusSettings() {
	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	log.SetFormatter(formatter)
	log.SetLevel(log.DebugLevel)
}

// Stress workflow decider
func (wf stressWorkflow) Execute(context flow.WorkflowContext, input []byte) {
	workflowInput := &WorkflowParams{}
	err := json.Unmarshal(input, workflowInput)
	if err != nil {
		context.Fail(err.Error(), []byte("Failed to parse the workflow input"))
		return
	}

	activityParams := &sleepActivityParams{sleepMin: workflowInput.ActivitySleepMin, sleepMax: workflowInput.ActivitySleepMax}
	activityInput, err := json.Marshal(activityParams)
	if err != nil {
		context.Fail(err.Error(), []byte("Failed to serialize sleep activity input"))
		return
	}

	wf.runActivityCount(workflowInput.ChainSequence, context, activityInput)
}

func (wf stressWorkflow) runActivityCount(count int, context flow.WorkflowContext, activityInput []byte) {
	if count > 0 {
		activityParameters := flow.ExecuteActivityParameters{
			TaskListName: "testTaskList",
			ActivityType: m.ActivityType{Name: common.StringPtr("sleepActivity")},
			Input:        activityInput,
		}
		context.ScheduleActivityTask(activityParameters, func(err error, result []byte) {
			if err != nil {
				taskFailure := err.(flow.ActivityTaskFailedError)
				context.Fail(taskFailure.Reason, taskFailure.Details)
				return
			}
			if count > 1 {
				wf.runActivityCount(count-1, context, activityInput)
			} else {
				context.Complete(nil)
			}
		})
	}
}

func (sa stressSleepActivity) Execute(context flow.ActivityExecutionContext, input []byte) ([]byte, error) {
	activityParams := &sleepActivityParams{}
	err := json.Unmarshal(input, activityParams)
	if err != nil {
		return nil, &flow.ActivityTaskFailedError{Reason: "failed to parse input with error:" + err.Error(), Details: input}
	}

	// log.Infof("Activity parameters: %+v", activityParams)
	// TODO: Input is getting nil input if it has beens stored properly

	// randomMultiple := rand.Intn(int((activityParams.sleepMax - activityParams.sleepMin) / time.Second))
	// sleepDuration := activityParams.sleepMin + time.Duration(randomMultiple)*time.Second

	// time.Sleep(time.Second)
	return nil, nil
}

// LaunchWorkflows starts workflows.
func LaunchWorkflows(countOfWorkflows int, goRoutineCount int, wp *WorkflowParams,
	service *ServiceMockEngine, reporter common.Reporter) error {
	logrusSettings()

	workerOverrides := &flow.WorkerOverrides{Reporter: reporter}
	// Workflow execution parameters.
	workflowExecutionParameters := flow.WorkerExecutionParameters{}
	workflowExecutionParameters.TaskListName = "testTaskList"
	workflowExecutionParameters.ConcurrentPollRoutineSize = 4

	workflowFactory := func(wt m.WorkflowType) (flow.WorkflowDefinition, error) {
		return &stressWorkflow{}, nil
	}
	activityFactory := func(at m.ActivityType) (flow.ActivityImplementation, error) {
		return &stressSleepActivity{}, nil
	}

	// Launch worker.
	workflowWorker := flow.NewWorkflowWorker(workflowExecutionParameters, workflowFactory, service, workerOverrides)
	workflowWorker.Start()

	// Create activity execution parameters.
	activityExecutionParameters := flow.WorkerExecutionParameters{}
	activityExecutionParameters.TaskListName = "testTaskList"
	activityExecutionParameters.ConcurrentPollRoutineSize = 10

	// Register activity instances and launch the worker.
	activityWorker := flow.NewActivityWorker(activityExecutionParameters, activityFactory, service, workerOverrides)
	activityWorker.Start()

	// Start a workflow.
	workflowInput, err := json.Marshal(wp)
	if err != nil {
		log.Error("Unable to marshal workflow input with")
		return err
	}
	workflowOptions := flow.StartWorkflowOptions{
		WorkflowType:                           m.WorkflowType{Name: common.StringPtr("stressWorkfow")},
		TaskListName:                           "testTaskList",
		WorkflowInput:                          workflowInput,
		ExecutionStartToCloseTimeoutSeconds:    10,
		DecisionTaskStartToCloseTimeoutSeconds: 10,
	}

	var totalWorkflowCount int32
	var goWaitGroup sync.WaitGroup

	workflowCreator := func(routineId int, createCount int, options flow.StartWorkflowOptions) {
		defer goWaitGroup.Done()

		for i := 0; i < createCount; i++ {
			options.WorkflowID = fmt.Sprintf("%s-%d-%d", uuid.New(), routineId, i)
			workflowClient := flow.NewWorkflowClient(options, service, reporter)
			_, err := workflowClient.StartWorkflowExecution()
			if err == nil {
				atomic.AddInt32(&totalWorkflowCount, 1)
				// log.Infof("Created Workflow - workflow Id: %s, run Id: %s \n", we.GetWorkflowId(), we.GetRunId())
			} else {
				log.Error(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	goWaitGroup.Add(goRoutineCount)
	for i := 0; i < goRoutineCount; i++ {
		go workflowCreator(i, countOfWorkflows/goRoutineCount, workflowOptions)
	}
	goWaitGroup.Wait()
	log.Infof("Total Created Workflow Count: %d \n", totalWorkflowCount)
	return nil
}
