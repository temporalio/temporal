package tests

import (
	"bytes"
	"encoding/binary"
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
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowBufferedEventsTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowBufferedEventsTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowBufferedEventsTestSuite))
}

func (s *WorkflowBufferedEventsTestSuite) TestRateLimitBufferedEvents() {
	id := "functional-rate-limit-buffered-events-test"
	wt := "functional-rate-limit-buffered-events-test-type"
	tl := "functional-rate-limit-buffered-events-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
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
	workflowComplete := false
	signalsSent := false
	signalCount := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		// Count signals
		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
			}
		}

		if !signalsSent {
			signalsSent = true
			// Buffered Signals
			for i := 0; i < 100; i++ {
				buf := new(bytes.Buffer)
				err := binary.Write(buf, binary.LittleEndian, byte(i))
				s.NoError(err)
				s.Nil(s.SendSignal(s.Namespace().String(), workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity))
			}

			buf := new(bytes.Buffer)
			err := binary.Write(buf, binary.LittleEndian, byte(101))
			s.NoError(err)
			signalErr := s.SendSignal(s.Namespace().String(), workflowExecution, "SignalName", payloads.EncodeBytes(buf.Bytes()), identity)
			s.NoError(signalErr)

			// this command will be ignored as workflow task has already failed
			return []*commandpb.Command{}, nil
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task to send 101 signals, the last signal will force fail workflow task and flush buffered events.
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Equal("Workflow task not found.", err.Error())

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
	s.Equal(101, signalCount) // check that all 101 signals are received.
}

func (s *WorkflowBufferedEventsTestSuite) TestBufferedEvents() {
	id := "functional-buffered-events-test"
	wt := "functional-buffered-events-test-type"
	tl := "functional-buffered-events-test-taskqueue"
	identity := "worker1"
	signalName := "buffered-signal"

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

	// workflow logic
	workflowComplete := false
	signalSent := false
	var signalEvent *historypb.HistoryEvent
	newRequestIDAttached := false
	var optionsUpdatedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			// this will create new event when there is in-flight workflow task, and the new event will be buffered
			_, err := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace: s.Namespace().String(),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal",
					Input:      payloads.EncodeString("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 && signalEvent == nil {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
				}
			}
		}

		if !newRequestIDAttached {
			newRequestIDAttached = true

			// attach new request id
			newRequestID := uuid.NewString()
			newRequest := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:                newRequestID,
				Namespace:                s.Namespace().String(),
				WorkflowId:               id,
				WorkflowType:             &commonpb.WorkflowType{Name: wt},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:                    nil,
				WorkflowRunTimeout:       durationpb.New(100 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
				Identity:                 identity,
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
				OnConflictOptions: &workflowpb.OnConflictOptions{
					AttachRequestId: true,
				},
			}
			resp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), newRequest)
			s.NoError(err)
			s.False(resp.Started)

			// check describe workflow request id infos map
			descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
				},
			}
			descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
			s.NoError(err)
			requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
			s.NotNil(requestIDInfos)
			s.NotNil(requestIDInfos[newRequestID])
			s.ProtoEqual(
				&workflowpb.RequestIdInfo{
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					EventId:   0,
					Buffered:  true,
				},
				requestIDInfos[newRequestID],
			)

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "2",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if optionsUpdatedEvent == nil {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
					optionsUpdatedEvent = event
				}
			}
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task, which sends signal and the signal event should be buffered to append after first workflow task closed
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// check history, the signal event should be after the complete workflow task
	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	fmt.Printf("history events: %#v\n", historyEvents)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, historyEvents)

	// Process signal in workflow, and second workflow task tries to start workflow which will simply
	// attach the request ID and have a WorkflowExecutionOptionsUpdated event buffered and appended
	// after the workflow task is closed.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// check history, the WorkflowExecutionOptionsUpdated event should be after the complete workflow task
	historyEvents = s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskTimedOut
  10 WorkflowTaskScheduled
  11 WorkflowTaskStarted
  12 WorkflowTaskCompleted
  13 ActivityTaskScheduled
  14 WorkflowExecutionOptionsUpdated
  15 WorkflowTaskScheduled
  16 WorkflowTaskStarted`, historyEvents)

	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.NotNil(optionsUpdatedEvent)
	s.NotEmpty(optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().AttachedRequestId)
	s.True(workflowComplete)

	// Check after the buffered events are flushed, the request ID infos map has the correct event ID.
	newRequestID := optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().AttachedRequestId
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	s.NoError(err)
	requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
	s.NotNil(requestIDInfos)
	s.NotNil(requestIDInfos[newRequestID])
	s.ProtoEqual(
		&workflowpb.RequestIdInfo{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			EventId:   14,
			Buffered:  false,
		},
		requestIDInfos[newRequestID],
	)
}

func (s *WorkflowBufferedEventsTestSuite) TestBufferedEventsOutOfOrder() {
	id := "functional-buffered-events-out-of-order-test"
	wt := "functional-buffered-events-out-of-order-test-type"
	tl := "functional-buffered-events-out-of-order-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(20 * time.Second),
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
	firstWorkflowTask := false
	secondWorkflowTask := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !firstWorkflowTask {
			firstWorkflowTask = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "some random marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					}}},
			}, {
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "Activity-1",
					ActivityType:           &commonpb.ActivityType{Name: "ActivityType"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("some random activity input"),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(100 * time.Second),
					StartToCloseTimeout:    durationpb.New(100 * time.Second),
					HeartbeatTimeout:       durationpb.New(100 * time.Second),
				}},
			}}, nil
		}

		if !secondWorkflowTask {
			secondWorkflowTask = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "some random marker name",
					Details: map[string]*commonpb.Payloads{
						"data": payloads.EncodeString("some random data"),
					}}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}
	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task, which will schedule an activity and add marker
	res, err := poller.PollAndProcessWorkflowTask(
		testcore.WithDumpHistory,
		testcore.WithExpectedAttemptCount(0),
		testcore.WithRetries(1),
		testcore.WithForceNewWorkflowTask)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	task := res.NewTask
	s.NoError(err)

	// This will cause activity start and complete to be buffered
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("pollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// second workflow task, completes another local activity and forces flush of buffered activity events
	newWorkflowTask := task.GetWorkflowTask()
	s.NotNil(newWorkflowTask)
	task, err = poller.HandlePartialWorkflowTask(newWorkflowTask, true)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(task)

	// third workflow task, which will close workflow
	newWorkflowTask = task.GetWorkflowTask()
	s.NotNil(newWorkflowTask)
	task, err = poller.HandlePartialWorkflowTask(newWorkflowTask, true)
	s.Logger.Info("pollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Nil(task.WorkflowTask)

	events := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 MarkerRecorded
 11 ActivityTaskStarted {"ScheduledEventId":6}
 12 ActivityTaskCompleted {"ScheduledEventId":6,"StartedEventId":11}
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
 16 WorkflowExecutionCompleted`, events)
}
