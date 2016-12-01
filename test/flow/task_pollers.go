package flow

import (
	"fmt"
	"time"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	log "github.com/Sirupsen/logrus"
	"github.com/uber/tchannel-go/thrift"
)

const (
	serviceTimeOut  = 10 * time.Second
	tagTaskListName = "taskListName"

	retryServiceOperationInitialInterval    = time.Millisecond
	retryServiceOperationMaxInterval        = 10 * time.Second
	retryServiceOperationExpirationInterval = 60 * time.Second
)

var (
	serviceOperationRetryPolicy = createServiceRetryPolicy()
)

type (
	// TaskPoller interface to poll for a single task
	TaskPoller interface {
		PollAndProcessSingleTask(routineID int) error
	}

	// workflowTaskPoller implements polling/processing a workflow task
	workflowTaskPoller struct {
		taskListName  string
		identity      string
		service       m.TChanWorkflowService
		taskHandler   WorkflowTaskHandler
		contextLogger *log.Entry
		reporter      common.Reporter
	}

	// activityTaskPoller implements polling/processing a workflow task
	activityTaskPoller struct {
		taskListName  string
		identity      string
		service       m.TChanWorkflowService
		taskHandler   ActivityTaskHandler
		contextLogger *log.Entry
		reporter      common.Reporter
	}
)

func createServiceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryServiceOperationInitialInterval)
	policy.SetMaximumInterval(retryServiceOperationMaxInterval)
	policy.SetExpirationInterval(retryServiceOperationExpirationInterval)
	return policy
}

func isServiceTransientError(err error) bool {
	// Retrying by default so it covers all transport errors.
	switch err.(type) {
	case *m.BadRequestError:
		return false
	case *m.EntityNotExistsError:
		return false
	case *m.WorkflowExecutionAlreadyStartedError:
		return false
	}

	// m.InternalServiceError
	return true
}

func newWorkflowTaskPoller(service m.TChanWorkflowService, taskListName string, identity string,
	taskHandler WorkflowTaskHandler, logger *log.Entry, reporter common.Reporter) *workflowTaskPoller {
	return &workflowTaskPoller{
		service:       service,
		taskListName:  taskListName,
		identity:      identity,
		taskHandler:   taskHandler,
		contextLogger: logger,
		reporter:      reporter}
}

// PollAndProcessSingleTask process one single task
func (wtp *workflowTaskPoller) PollAndProcessSingleTask(routineID int) error {
	startTime := time.Now()
	defer func() {
		deltaTime := time.Now().Sub(startTime)
		wtp.reporter.IncCounter(common.DecisionsTotalCounter, nil, 1)
		wtp.reporter.RecordTimer(common.DecisionsEndToEndLatency, nil, deltaTime)
	}()

	// Get the task.
	workflowTask, err := wtp.poll(routineID)
	if err != nil {
		return err
	}
	if workflowTask.task == nil {
		// We didn't have task, poll might have time out.
		wtp.contextLogger.Debug("Workflow task unavailable")
		return nil
	}

	// Process the task.
	completedRequest, err := wtp.taskHandler.ProcessWorkflowTask(workflowTask)
	if err != nil {
		return err
	}

	// Respond task completion.
	err = backoff.Retry(
		func() error {
			ctx, cancel := thrift.NewContext(serviceTimeOut)
			defer cancel()
			return wtp.service.RespondDecisionTaskCompleted(ctx, completedRequest)
		}, serviceOperationRetryPolicy, isServiceTransientError)

	if err != nil {
		return err
	}
	return nil
}

// Poll for a single workflow task from the service
func (wtp *workflowTaskPoller) poll(routineID int) (*WorkflowTask, error) {
	wtp.contextLogger.Debugf("[%d]workflowTaskPoller::Poll", routineID)
	request := &m.PollForDecisionTaskRequest{
		TaskList: common.TaskListPtr(m.TaskList{Name: common.StringPtr(wtp.taskListName)}),
		Identity: common.StringPtr(wtp.identity),
	}

	ctx, cancel := thrift.NewContext(serviceTimeOut)
	defer cancel()

	response, err := wtp.service.PollForDecisionTask(ctx, request)
	if err != nil {
		return nil, err
	}
	if response == nil || len(response.GetTaskToken()) == 0 {
		return &WorkflowTask{}, nil
	}
	return &WorkflowTask{task: response}, nil
}

func newActivityTaskPoller(service m.TChanWorkflowService, taskListName string, identity string,
	taskHandler ActivityTaskHandler, logger *log.Entry, reporter common.Reporter) *activityTaskPoller {
	return &activityTaskPoller{
		service:       service,
		taskListName:  taskListName,
		identity:      identity,
		taskHandler:   taskHandler,
		contextLogger: logger,
		reporter:      reporter}
}

// Poll for a single activity task from the service
func (atp *activityTaskPoller) poll(routineID int) (*ActivityTask, error) {
	atp.contextLogger.Debugf("[%d]activityTaskPoller::Poll", routineID)
	request := &m.PollForActivityTaskRequest{
		TaskList: common.TaskListPtr(m.TaskList{Name: common.StringPtr(atp.taskListName)}),
		Identity: common.StringPtr(atp.identity),
	}

	ctx, cancel := thrift.NewContext(serviceTimeOut)
	defer cancel()

	response, err := atp.service.PollForActivityTask(ctx, request)
	if err != nil {
		return nil, err
	}
	if response == nil || len(response.GetTaskToken()) == 0 {
		return &ActivityTask{}, nil
	}
	return &ActivityTask{task: response}, nil
}

// PollAndProcessSingleTask process one single activity task
func (atp *activityTaskPoller) PollAndProcessSingleTask(routineID int) error {
	startTime := time.Now()
	defer func() {
		deltaTime := time.Now().Sub(startTime)
		atp.reporter.IncCounter(common.ActivitiesTotalCounter, nil, 1)
		atp.reporter.RecordTimer(common.ActivityEndToEndLatency, nil, deltaTime)
	}()

	// Get the task.
	activityTask, err := atp.poll(routineID)
	if err != nil {
		return err
	}
	if activityTask.task == nil {
		// We didn't have task, poll might have time out.
		atp.contextLogger.Debug("Activity task unavailable")
		return nil
	}

	// Process the activity task.
	ctx, cancel := thrift.NewContext(serviceTimeOut)
	defer cancel()
	result, err := atp.taskHandler.Execute(ctx, activityTask)
	if err != nil {
		return err
	}

	// TODO: Handle Cancel of the activity after the thrift method is introduced.
	switch result.(type) {
	// Report success untill we succeed
	case *m.RespondActivityTaskCompletedRequest:
		err = backoff.Retry(
			func() error {
				ctx, cancel := thrift.NewContext(serviceTimeOut)
				defer cancel()

				return atp.service.RespondActivityTaskCompleted(ctx, result.(*m.RespondActivityTaskCompletedRequest))
			}, serviceOperationRetryPolicy, isServiceTransientError)

		if err != nil {
			return err
		}
		// Report failure untill we succeed
	case *m.RespondActivityTaskFailedRequest:
		err = backoff.Retry(
			func() error {
				ctx, cancel := thrift.NewContext(serviceTimeOut)
				defer cancel()

				return atp.service.RespondActivityTaskFailed(ctx, result.(*m.RespondActivityTaskFailedRequest))
			}, serviceOperationRetryPolicy, isServiceTransientError)
		if err != nil {
			return err
		}
	default:
		panic(fmt.Errorf("Unhandled activity response type: %v", result))
	}
	return nil
}
