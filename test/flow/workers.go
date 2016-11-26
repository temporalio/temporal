package flow

import (
	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	log "github.com/Sirupsen/logrus"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// WorkerExecutionParameters defines worker configure/execution options.
	WorkerExecutionParameters struct {
		// Task list name to poll.
		TaskListName string

		// Defines how many concurrent poll requests for the task list by this worker.
		ConcurrentPollRoutineSize int

		// Defines how many executions for task list by this worker.
		// TODO: In future we want to separate the activity executions as they take longer than polls.
		// ConcurrentExecutionRoutineSize int

		// User can provide an identity for the debuggability. If not provided the framework has
		// a default option.
		Identity string
	}

	// WorkflowWorker wraps the code for hosting workflow types.
	// And worker is mapped 1:1 with task list. If the user want's to poll multiple
	// task list names they might have to manage 'n' workers for 'n' task lists.
	WorkflowWorker struct {
		executionParameters WorkerExecutionParameters
		workflowDefFactory  WorkflowDefinitionFactory
		workflowService     m.TChanWorkflowService
		poller              TaskPoller // TaskPoller to poll the tasks.
		worker              *baseWorker
		identity            string
	}

	// ActivityRegistry collection of activity implementations
	ActivityRegistry map[string]ActivityImplementation

	// ActivityWorker wraps the code for hosting activity types.
	// TODO: Worker doing heartbeating automatically while activity task is running
	ActivityWorker struct {
		executionParameters WorkerExecutionParameters
		activityRegistry    ActivityRegistry
		workflowService     m.TChanWorkflowService
		poller              *activityTaskPoller
		worker              *baseWorker
		identity            string
	}

	// WorkerOverrides overrides.
	WorkerOverrides struct {
		workflowTaskHander  WorkflowTaskHandler
		activityTaskHandler ActivityTaskHandler
		Reporter            common.Reporter
	}
)

// NewWorkflowWorker returns an instance of the workflow worker.
func NewWorkflowWorker(params WorkerExecutionParameters, factory WorkflowDefinitionFactory,
	service m.TChanWorkflowService, overrides *WorkerOverrides) *WorkflowWorker {
	return newWorkflowWorkerInternal(params, factory, service, overrides)
}

func newWorkflowWorkerInternal(params WorkerExecutionParameters, factory WorkflowDefinitionFactory,
	service m.TChanWorkflowService, overrides *WorkerOverrides) *WorkflowWorker {
	// Get an identity.
	identity := params.Identity
	if identity == "" {
		identity = GetWorkerIdentity(params.TaskListName)
	}

	logger := log.WithFields(log.Fields{tagTaskListName: params.TaskListName})

	// Get a workflow task handler.
	var taskHandler WorkflowTaskHandler
	if overrides != nil && overrides.workflowTaskHander != nil {
		taskHandler = overrides.workflowTaskHander
	} else {
		taskHandler = newWorkflowTaskHandler(params.TaskListName, identity, factory, logger, overrides.Reporter)
	}

	poller := newWorkflowTaskPoller(
		service,
		params.TaskListName,
		identity,
		taskHandler,
		logger)
	worker := newBaseWorker(baseWorkerOptions{
		routineCount:    params.ConcurrentPollRoutineSize,
		taskPoller:      poller,
		workflowService: service,
		identity:        identity})

	return &WorkflowWorker{
		executionParameters: params,
		workflowDefFactory:  factory,
		workflowService:     service,
		poller:              poller,
		worker:              worker,
		identity:            identity,
	}
}

// Start the worker.
func (ww *WorkflowWorker) Start() {
	ww.worker.Start()
}

// Shutdown the worker.
func (ww *WorkflowWorker) Shutdown() {
	ww.worker.Shutdown()
}

// NewActivityWorker returns an instance of the activity worker.
func NewActivityWorker(executionParameters WorkerExecutionParameters, factory ActivityImplementationFactory,
	service m.TChanWorkflowService, overrides *WorkerOverrides) *ActivityWorker {
	return newActivityWorkerInternal(executionParameters, factory, service, overrides)
}

func newActivityWorkerInternal(executionParameters WorkerExecutionParameters, factory ActivityImplementationFactory,
	service m.TChanWorkflowService, overrides *WorkerOverrides) *ActivityWorker {
	// Get an identity.
	identity := executionParameters.Identity
	if identity == "" {
		identity = GetWorkerIdentity(executionParameters.TaskListName)
	}

	logger := log.WithFields(log.Fields{tagTaskListName: executionParameters.TaskListName})

	// Get a activity task handler.
	var taskHandler ActivityTaskHandler
	if overrides != nil && overrides.activityTaskHandler != nil {
		taskHandler = overrides.activityTaskHandler
	} else {
		taskHandler = newActivityTaskHandler(executionParameters.TaskListName, executionParameters.Identity,
			factory, service, logger, overrides.Reporter)
	}
	poller := newActivityTaskPoller(
		service,
		executionParameters.TaskListName,
		identity,
		taskHandler,
		logger)
	worker := newBaseWorker(baseWorkerOptions{
		routineCount:    executionParameters.ConcurrentPollRoutineSize,
		taskPoller:      poller,
		workflowService: service,
		identity:        identity})

	return &ActivityWorker{
		executionParameters: executionParameters,
		activityRegistry:    make(map[string]ActivityImplementation),
		workflowService:     service,
		worker:              worker,
		poller:              poller,
		identity:            identity,
	}
}

// Start the worker.
func (aw *ActivityWorker) Start() {
	aw.worker.Start()
}

// Shutdown the worker.
func (aw *ActivityWorker) Shutdown() {
	aw.worker.Shutdown()
}

// NewWorkflowClient creates an instance of workflow client that users can start a workflow
func NewWorkflowClient(options StartWorkflowOptions, service m.TChanWorkflowService, reporter common.Reporter) *WorkflowClient {
	// Get an identity.
	identity := options.Identity
	if identity == "" {
		identity = GetWorkerIdentity(options.TaskListName)
	}
	return &WorkflowClient{options: options, workflowService: service, Identity: identity, reporter: reporter}
}

// StartWorkflowExecution starts a workflow execution
func (wc *WorkflowClient) StartWorkflowExecution() (*m.WorkflowExecution, error) {

	startRequest := &m.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(wc.options.WorkflowID),
		WorkflowType: common.WorkflowTypePtr(wc.options.WorkflowType),
		TaskList:     common.TaskListPtr(m.TaskList{Name: common.StringPtr(wc.options.TaskListName)}),
		Input:        wc.options.WorkflowInput,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(wc.options.ExecutionStartToCloseTimeoutSeconds),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(wc.options.DecisionTaskStartToCloseTimeoutSeconds),
		Identity:                            common.StringPtr(wc.Identity)}

	var response *m.StartWorkflowExecutionResponse

	// Start creating workflow request.
	err := backoff.Retry(
		func() error {
			ctx, cancel := thrift.NewContext(serviceTimeOut)
			defer cancel()

			var err1 error
			response, err1 = wc.workflowService.StartWorkflowExecution(ctx, startRequest)
			return err1
		}, serviceOperationRetryPolicy, isServiceTransientError)

	if err != nil {
		return nil, err
	}

	wc.reporter.IncCounter(common.WorkflowsStartTotalCounter, nil, 1)
	executionInfo := &m.WorkflowExecution{
		// TODO: StartWorkflowExecution should return workflow ID as well along with run Id
		WorkflowId: common.StringPtr(wc.options.WorkflowID),
		RunId:      response.RunId}
	return executionInfo, nil
}
