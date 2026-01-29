package tests

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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

func TestStickyTq(t *testing.T) {
	t.Run("StickyTimeout_NonTransientWorkflowTask", func(t *testing.T) {
		s := testcore.NewEnv(t)

		id := s.Tv().WorkflowID()
		wt := s.Tv().WorkflowType().Name
		tl := s.Tv().TaskQueue().Name
		stl := s.Tv().TaskQueue().Name + "-sticky"
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
		require.NoError(t, err0)

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
						require.NoError(t, err)
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
			T:                            t,
			StickyTaskQueue:              stickyTaskQueue,
			StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
		}

		_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		require.NoError(t, err)

		_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "signalA",
			Input:             payloads.EncodeString("signal input"),
			Identity:          identity,
			RequestId:         uuid.NewString(),
		})
		require.NoError(t, err)

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
		require.True(t, stickyTimeout, "Workflow task not timed out")

		for i := 1; i <= 3; i++ {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			require.NoError(t, err)
		}

		_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "signalB",
			Input:             payloads.EncodeString("signal input"),
			Identity:          identity,
			RequestId:         uuid.NewString(),
		})
		require.NoError(t, err)

		for i := 1; i <= 2; i++ {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			require.NoError(t, err)
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
		require.NoError(t, err)

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
	})

	t.Run("StickyTaskqueueResetThenTimeout", func(t *testing.T) {
		s := testcore.NewEnv(t)

		id := s.Tv().WorkflowID()
		wt := s.Tv().WorkflowType().Name
		tl := s.Tv().TaskQueue().Name
		stl := s.Tv().TaskQueue().Name + "-sticky"
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
		require.NoError(t, err0)

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
			T:                            t,
			StickyTaskQueue:              stickyTaskQueue,
			StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
		}

		_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		require.NoError(t, err)

		_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "signalA",
			Input:             payloads.EncodeString("signal input"),
			Identity:          identity,
			RequestId:         uuid.NewString(),
		})
		require.NoError(t, err)

		// Reset sticky taskqueue before sticky workflow task starts
		_, err = s.FrontendClient().ResetStickyTaskQueue(testcore.NewContext(), &workflowservice.ResetStickyTaskQueueRequest{
			Namespace: s.Namespace().String(),
			Execution: workflowExecution,
		})
		require.NoError(t, err)

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
		require.True(t, stickyTimeout, "Workflow task not timed out")

		for i := 1; i <= 3; i++ {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			require.NoError(t, err)
		}

		_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "signalB",
			Input:             payloads.EncodeString("signal input"),
			Identity:          identity,
			RequestId:         uuid.NewString(),
		})
		require.NoError(t, err)

		for i := 1; i <= 2; i++ {
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithRespondSticky, testcore.WithExpectedAttemptCount(i))
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			require.NoError(t, err)
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
		require.NoError(t, err)

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
	})
}
