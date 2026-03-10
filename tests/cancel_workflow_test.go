package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CancelWorkflowSuite struct {
	testcore.FunctionalTestBase
}

func TestCancelWorkflowSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CancelWorkflowSuite))
}

func (s *CancelWorkflowSuite) TestExternalRequestCancelWorkflowExecution() {
	id := "functional-request-cancel-workflow-test"
	wt := "functional-request-cancel-workflow-test-type"
	tl := "functional-request-cancel-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	// cancellation to non exist workflow will lead to error
	_, err := s.FrontendClient().RequestCancelWorkflowExecution(testcore.NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.IsType(&serviceerror.NotFound{}, err)
	s.EqualError(err, "workflow not found for ID: "+id)

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{
				Details: payloads.EncodeString("Cancelled"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = s.FrontendClient().RequestCancelWorkflowExecution(testcore.NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	_, err = s.FrontendClient().RequestCancelWorkflowExecution(testcore.NewContext(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionCancelRequested
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCanceled {"Details":{"Payloads":[{"Data":"\"Cancelled\""}]}}`, historyEvents)
}

func (s *CancelWorkflowSuite) TestRequestCancelWorkflowCommandExecution_TargetRunning() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-cancel-workflow-command-target-running-test"
	wt := "functional-cancel-workflow-command-target-running-test-type"
	tl := "functional-cancel-workflow-command-target-running-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	externalRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.ExternalNamespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), externalRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on external namespace", tag.WorkflowNamespace(s.ExternalNamespace().String()), tag.WorkflowRunID(we2.RunId))

	cancellationSent := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !cancellationSent {
			cancellationSent = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					Namespace:  s.ExternalNamespace().String(),
					WorkflowId: id,
					RunId:      we2.RunId,
				}},
			}}, nil
		}

		// Find cancel requested event and verify it.
		var cancelRequestEvent *historypb.HistoryEvent
		for _, x := range task.History.Events {
			if x.EventType == enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				cancelRequestEvent = x
			}
		}
		s.NotNil(cancelRequestEvent)

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	externalWTFHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		// Find cancel requested event and verify it.
		var cancelRequestEvent *historypb.HistoryEvent
		for _, x := range task.History.Events {
			if x.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				cancelRequestEvent = x
			}
		}

		s.NotNil(cancelRequestEvent)
		cancelRequestEventAttributes := cancelRequestEvent.GetWorkflowExecutionCancelRequestedEventAttributes()
		s.Equal(int64(5), cancelRequestEventAttributes.ExternalInitiatedEventId)
		s.Equal(id, cancelRequestEventAttributes.ExternalWorkflowExecution.WorkflowId)
		s.Equal(we.RunId, cancelRequestEventAttributes.ExternalWorkflowExecution.RunId)

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{
				Details: payloads.EncodeString("Cancelled"),
			}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
	externalPoller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.ExternalNamespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: externalWTFHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Cancel the external workflow with this workflow task request.
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(cancellationSent)

	// Finish execution
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Accept cancellation.
	_, err = externalPoller.PollAndProcessWorkflowTask()
	s.Logger.Info("external PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
}

func (s *CancelWorkflowSuite) TestRequestCancelWorkflowCommandExecution_TargetFinished() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-cancel-workflow-command-target-finished-test"
	wt := "functional-cancel-workflow-command-target-finished-test-type"
	tl := "functional-cancel-workflow-command-target-finished-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	externalRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.ExternalNamespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), externalRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on external namespace", tag.WorkflowNamespace(s.ExternalNamespace().String()), tag.WorkflowRunID(we2.RunId))

	cancellationSent := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !cancellationSent {
			cancellationSent = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					Namespace:  s.ExternalNamespace().String(),
					WorkflowId: id,
					RunId:      we2.RunId,
				}},
			}}, nil
		}

		// Find cancel requested event and verify it.
		var cancelRequestEvent *historypb.HistoryEvent
		for _, x := range task.History.Events {
			if x.EventType == enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				cancelRequestEvent = x
			}
		}
		s.NotNil(cancelRequestEvent)

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	externalWTFHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		// Find cancel requested event not present
		var cancelRequestEvent *historypb.HistoryEvent
		for _, x := range task.History.Events {
			if x.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				cancelRequestEvent = x
			}
		}

		s.Nil(cancelRequestEvent)

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
	externalPoller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.ExternalNamespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: externalWTFHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Complete target workflow
	_, err := externalPoller.PollAndProcessWorkflowTask()
	s.Logger.Info("external PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Cancel the target workflow with this workflow task request.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(cancellationSent)

	// Finish execution
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
}

func (s *CancelWorkflowSuite) TestRequestCancelWorkflowCommandExecution_TargetNotFound() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-cancel-workflow-command-target-not-found-test"
	wt := "functional-cancel-workflow-command-target-not-found-test-type"
	tl := "functional-cancel-workflow-command-target-not-found-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	cancellationSent := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !cancellationSent {
			cancellationSent = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					Namespace:  s.ExternalNamespace().String(),
					WorkflowId: "some-random-non-existence-workflow-id",
				}},
			}}, nil
		}

		// Find cancel requested event and verify it.
		var cancelRequestEvent *historypb.HistoryEvent
		for _, x := range task.History.Events {
			if x.EventType == enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
				cancelRequestEvent = x
			}
		}
		s.NotNil(cancelRequestEvent)

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Cancel the target workflow with this workflow task request.
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(cancellationSent)

	// Finish execution
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
}

func (s *CancelWorkflowSuite) TestImmediateChildCancellation_WorkflowTaskFailed() {
	id := "functional-immediate-child-cancellation-workflow-task-failed-test"
	wt := "functional-immediate-child-cancellation-workflow-task-failed-test-type"
	tl := "functional-immediate-child-cancellation-workflow-task-failed-test-taskqueue"
	childWorkflowID := "functional-immediate-child-cancellation-workflow-task-failed-child-test"
	childTaskQueue := "functional-immediate-child-cancellation-workflow-task-failed-child-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	_, err := s.FrontendClient().RequestCancelWorkflowExecution(testcore.NewContext(),
		&workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Identity:  identity,
			RequestId: uuid.NewString(),
		})
	s.NoError(err)

	childCancelled := false
	var initiatedEvent *historypb.HistoryEvent
	var requestCancelEvent *historypb.HistoryEvent
	var workflowtaskFailedEvent *historypb.HistoryEvent
	workflowComplete := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !childCancelled {
			startEvent := task.History.Events[0]
			if startEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				return nil, errors.New("first event is not workflow execution started") //nolint:err113
			}

			workflowTaskScheduledEvent := task.History.Events[1]
			if workflowTaskScheduledEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				return nil, errors.New("second event is not workflow task scheduled") //nolint:err113
			}

			cancelRequestedEvent := task.History.Events[2]
			if cancelRequestedEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
				return nil, errors.New("third event is not cancel requested") //nolint:err113
			}

			// Schedule and cancel child workflow in the same decision
			childCancelled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:    s.Namespace().String(),
					WorkflowId:   childWorkflowID,
					WorkflowType: &commonpb.WorkflowType{Name: "childTypeA"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: childTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:        payloads.EncodeBytes([]byte{1}),
				}},
			}, {
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{RequestCancelExternalWorkflowExecutionCommandAttributes: &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
					Namespace:         s.Namespace().String(),
					WorkflowId:        childWorkflowID,
					ChildWorkflowOnly: true,
				}},
			}}, nil
		}

		if task.PreviousStartedEventId != 0 {
			return nil, errors.New("previous started decision moved unexpectedly after first failed workflow task") //nolint:err113
		}
		// Validate child workflow as cancelled
		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			s.Logger.Info(fmt.Sprintf("Processing EventID: %v, Event: %v", event.GetEventId(), event))
			switch event.GetEventType() { // nolint:exhaustive
			case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
				initiatedEvent = event
			case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
				requestCancelEvent = event
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
				workflowtaskFailedEvent = event
			}
		}

		if initiatedEvent != nil {
			return nil, errors.New("start child workflow command accepted from previous workflow task") //nolint:err113
		}

		if requestCancelEvent != nil {
			return nil, errors.New("request cancel command accepted from previous workflow task") //nolint:err113
		}

		if workflowtaskFailedEvent == nil {
			return nil, errors.New("workflow task failed event not found due to previous bad commands") //nolint:err113
		}

		taskFailure := workflowtaskFailedEvent.GetWorkflowTaskFailedEventAttributes().GetFailure()
		if taskFailure.GetMessage() != fmt.Sprintf("BadRequestCancelExternalWorkflowExecutionAttributes: Start and RequestCancel for child workflow is not allowed in same workflow task. WorkflowId=%s RunId= Namespace=%s", childWorkflowID, s.Namespace().String()) {
			return nil, errors.New("unexpected workflow task failure") //nolint:err113
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	s.Logger.Info("Process first workflow task which starts and request cancels child workflow")
	_, err = poller.PollAndProcessWorkflowTask()
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal(fmt.Sprintf("BadRequestCancelExternalWorkflowExecutionAttributes: Start and RequestCancel for child workflow is not allowed in same workflow task. WorkflowId=%s RunId= Namespace=%s", childWorkflowID, s.Namespace().String()), err.Error())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionCancelRequested
  4 WorkflowTaskStarted
  5 WorkflowTaskFailed
  6 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
	}))

	s.Logger.Info("Process second workflow task which observes child workflow is cancelled and completes")
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionCancelRequested
  4 WorkflowTaskStarted
  5 WorkflowTaskFailed
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
	}))

	_, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
		},
	})
	if err == nil {
		s.PrintHistoryEvents(s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
		}))
	}
	s.Logger.Error("Describe error", tag.Error(err))
	s.Error(err, "Child workflow execution started instead of getting cancelled")
	s.IsType(&serviceerror.NotFound{}, err, "Error is not of type 'NotFound'")

	s.True(workflowComplete)
}
