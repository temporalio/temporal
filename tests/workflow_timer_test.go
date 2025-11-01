package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowTimerTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowTimerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowTimerTestSuite))
}

func (s *WorkflowTimerTestSuite) TestCancelTimer() {
	id := "functional-cancel-timer-test"
	wt := "functional-cancel-timer-test-type"
	tl := "functional-cancel-timer-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1000 * time.Second),
		Identity:            identity,
	}

	creatResp, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	timer := 2000 * time.Second
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("%v", timerID),
					StartToFireTimeout: durationpb.New(timer),
				}},
			}}, nil
		}

		historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
		for _, event := range historyEvents {
			switch event.GetEventType() { // nolint:exhaustive
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
				signalDelivered = true
			case enumspb.EVENT_TYPE_TIMER_CANCELED:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
				Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
				}},
			}}, nil
		}

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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.SendSignal(s.Namespace().String(), workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.SendSignal(s.Namespace().String(), workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 TimerCanceled
 11 WorkflowExecutionSignaled
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskCompleted
 15 WorkflowExecutionCompleted
`, historyEvents)
}

func (s *WorkflowTimerTestSuite) TestCancelTimer_CancelFiredAndBuffered() {
	id := "functional-cancel-timer-fired-and-buffered-test"
	wt := "functional-cancel-timer-fired-and-buffered-test-type"
	tl := "functional-cancel-timer-fired-and-buffered-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1000 * time.Second),
		Identity:            identity,
	}

	creatResp, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      creatResp.GetRunId(),
	}

	timerID := 1
	timerScheduled := false
	signalDelivered := false
	timerCancelled := false
	timer := 4 * time.Second
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !timerScheduled {
			timerScheduled = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            fmt.Sprintf("%v", timerID),
					StartToFireTimeout: durationpb.New(timer),
				}},
			}}, nil
		}

		historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
		for _, event := range historyEvents {
			switch event.GetEventType() { // nolint:exhaustive
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
				signalDelivered = true
			case enumspb.EVENT_TYPE_TIMER_CANCELED:
				timerCancelled = true
			}
		}

		if !signalDelivered {
			s.Fail("should receive a signal")
		}

		if !timerCancelled {
			time.Sleep(2 * timer) //nolint:forbidigo
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
				Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
					TimerId: fmt.Sprintf("%v", timerID),
				}},
			}}, nil
		}

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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// schedule the timer
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.SendSignal(s.Namespace().String(), workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))

	// receive the signal & cancel the timer
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	s.Nil(s.SendSignal(s.Namespace().String(), workflowExecution, "random signal name", payloads.EncodeString("random signal payload"), identity))
	// complete the workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask: completed")
	s.NoError(err)

	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 TimerCanceled
 11 WorkflowExecutionSignaled
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskCompleted
 15 WorkflowExecutionCompleted
`, historyEvents)
}
