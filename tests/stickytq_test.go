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

	stickyTaskQueue := taskqueuepb.TaskQueue_builder{Name: stl, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}.Build()
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))
	workflowExecution := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}.Build()

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				RecordMarkerCommandAttributes: commandpb.RecordMarkerCommandAttributes_builder{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}.Build(),
			}.Build()}, nil
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

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:                       s.FrontendClient(),
		Namespace:                    s.Namespace().String(),
		TaskQueue:                    taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
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

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err)

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
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

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	}.Build())
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
 15 WorkflowTaskFailed`, events)

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

	stickyTaskQueue := taskqueuepb.TaskQueue_builder{Name: stl, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}.Build()
	stickyScheduleToStartTimeout := 2 * time.Second

	// Start workflow execution
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))
	workflowExecution := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}.Build()

	// workflow logic
	localActivityDone := false
	failureCount := 5
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !localActivityDone {
			localActivityDone = true

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				RecordMarkerCommandAttributes: commandpb.RecordMarkerCommandAttributes_builder{
					MarkerName: "local activity marker",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}.Build(),
			}.Build()}, nil
		}

		if failureCount > 0 {
			failureCount--
			return nil, errors.New("non deterministic error")
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:                       s.FrontendClient(),
		Namespace:                    s.Namespace().String(),
		TaskQueue:                    taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
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

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalA",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err)

	// Reset sticky taskqueue before sticky workflow task starts
	_, err = s.FrontendClient().ResetStickyTaskQueue(testcore.NewContext(), workflowservice.ResetStickyTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: workflowExecution,
	}.Build())
	s.NoError(err)

	// Wait for workflow task timeout
	stickyTimeout := false
WaitForStickyTimeoutLoop:
	for i := 0; i < 10; i++ {
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

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "signalB",
		Input:             payloads.EncodeString("signal input"),
		Identity:          identity,
		RequestId:         uuid.NewString(),
	}.Build())
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
 15 WorkflowTaskFailed`, events)

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
