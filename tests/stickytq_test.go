package tests

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type StickyTqTestSuite struct {
	testcore.FunctionalTestBase
}

func TestStickyTqTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(StickyTqTestSuite))
}

func (s *StickyTqTestSuite) TestStickyTimeout_NonTransientWorkflowTask() {
	id := "functional-sticky-timeout-non-transient-workflow-task"
	wt := "functional-sticky-timeout-non-transient-command-type"
	tl := "functional-sticky-timeout-non-transient-workflow-taskqueue"
	stl := "functional-sticky-timeout-non-transient-workflow-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stl, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}}, nil
		}

		if failureCount > 0 {
			// send a signal on third failure to be buffered, forcing a non-transient workflow task when buffer is flushed
			/*
				if failureCount == 3 {
					err := s.FrontendClient().SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
						Namespace:            s.Namespace(),
						WorkflowExecution: workflowExecution,
						SignalName:        "signalB",
						Input:             codec.EncodeString("signal input"),
						Identity:          identity,
						RequestId:         uuid.NewString(),
					})
					s.NoError(err)
				}
			*/
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:                       s.FrontendClient(),
		Namespace:                    s.Namespace().String(),
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for range 10 {
		events := s.GetHistory(s.Namespace().String(), workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT {
				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut {"TimeoutType":2} // enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
  9 WorkflowTaskScheduled`, events)
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second) //nolint:forbidigo
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)

	for i := 1; i <= 2; i++ {
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	events := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed
 16 WorkflowTaskScheduled`, events)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(3))
	s.NoError(err)

	events = s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed  // Two WFTs have failed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed // Two WFTs have failed
 16 WorkflowTaskScheduled
 17 WorkflowTaskStarted
 18 WorkflowTaskCompleted
 19 WorkflowExecutionCompleted // Workflow has completed`, events)
}

func (s *StickyTqTestSuite) TestStickyTaskqueueResetThenTimeout() {
	id := "functional-reset-sticky-fire-schedule-to-start-timeout"
	wt := "functional-reset-sticky-fire-schedule-to-start-timeout-type"
	tl := "functional-reset-sticky-fire-schedule-to-start-timeout-taskqueue"
	stl := "functional-reset-sticky-fire-schedule-to-start-timeout-taskqueue-sticky"
	identity := "worker1"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stl, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}}, nil
		}

		if failureCount > 0 {
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:                       s.FrontendClient(),
		Namespace:                    s.Namespace().String(),
		TaskQueue:                    &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)

	// Reset sticky taskqueue before sticky workflow task starts
	_, err = s.FrontendClient().ResetStickyTaskQueue(testcore.NewContext(), &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: s.Namespace().String(),
		Execution: workflowExecution,
	})
	s.NoError(err)

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for range 10 {
		events := s.GetHistory(s.Namespace().String(), workflowExecution)
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT {
				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut {"TimeoutType":2} // enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START 
  9 WorkflowTaskScheduled`, events)
				stickyTimeout = true
				break WaitForStickyTimeoutLoop
			}
		}
		time.Sleep(time.Second) //nolint:forbidigo
	}
	s.True(stickyTimeout, "Workflow task not timed out")

	for i := 1; i <= 3; i++ {
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)

	for i := 1; i <= 2; i++ {
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	events := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed
 16 WorkflowTaskScheduled`, events)

	// Complete workflow execution
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(3))
	s.NoError(err)

	events = s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskTimedOut
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskFailed  // Two WFTs have failed
 12 WorkflowExecutionSignaled
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskFailed // Two WFTs have failed
 16 WorkflowTaskScheduled
 17 WorkflowTaskStarted
 18 WorkflowTaskCompleted
 19 WorkflowExecutionCompleted // Workflow has completed`, events)
}
