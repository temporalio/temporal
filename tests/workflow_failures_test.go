package tests

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkflowFailuresTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowFailuresTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowFailuresTestSuite))
}

func (s *WorkflowFailuresTestSuite) TestWorkflowTimeout() {
	startTime := time.Now().UTC()

	id := "functional-workflow-timeout"
	wt := "functional-workflow-timeout-type"
	tl := "functional-workflow-timeout-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(1 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	time.Sleep(time.Second) //nolint:forbidigo

	var historyEvents []*historypb.HistoryEvent
GetHistoryLoop:
	for range 10 {
		historyEvents = s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
			s.Logger.Warn("Execution not timedout yet. Last event: " + lastEvent.GetEventType().String())
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
			continue GetHistoryLoop
		}

		break GetHistoryLoop
	}
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionTimedOut`, historyEvents)

	startFilter := &filterpb.StartTimeFilter{
		EarliestTime: timestamppb.New(startTime),
		LatestTime:   timestamppb.New(time.Now().UTC()),
	}

	closedCount := 0
ListClosedLoop:
	for range 10 {
		resp, err3 := s.FrontendClient().ListClosedWorkflowExecutions(testcore.NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err3)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.Logger.Info("Closed WorkflowExecution is not yet visibile")
			time.Sleep(1000 * time.Millisecond) //nolint:forbidigo
			continue ListClosedLoop
		}
		break ListClosedLoop
	}
	s.Equal(1, closedCount)
}

func (s *WorkflowFailuresTestSuite) TestWorkflowTaskFailed() {
	id := "functional-workflowtask-failed-test"
	wt := "functional-workflowtask-failed-test-type"
	tl := "functional-workflowtask-failed-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

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
	activityScheduled := false
	activityData := int32(1)
	failureCount := 10
	signalCount := 0
	sendSignal := false
	lastWorkflowTaskTime := time.Time{}
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		// Count signals
		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
			}
		}
		// Some signals received on this workflow task
		if signalCount == 1 {
			return []*commandpb.Command{}, nil
		}

		// Send signals during workflow task
		if sendSignal {
			s.NoError(s.SendSignal(s.Namespace().String(), workflowExecution, "signalC", nil, identity))
			s.NoError(s.SendSignal(s.Namespace().String(), workflowExecution, "signalD", nil, identity))
			s.NoError(s.SendSignal(s.Namespace().String(), workflowExecution, "signalE", nil, identity))
			sendSignal = false
		}

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if failureCount > 0 {
			// Otherwise decrement failureCount and keep failing workflow tasks
			failureCount--
			return nil, errors.New("workflow panic") //nolint:err113
		}

		workflowComplete = true
		time.Sleep(time.Second) //nolint:forbidigo
		s.Logger.Warn(fmt.Sprintf("PrevStarted: %v, StartedEventID: %v, Size: %v", task.PreviousStartedEventId, task.StartedEventId,
			len(task.History.Events)))
		lastWorkflowTaskEvent := task.History.Events[task.StartedEventId-1]
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lastWorkflowTaskEvent.GetEventType())
		lastWorkflowTaskTime = lastWorkflowTaskEvent.GetEventTime().AsTime()
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

	// Make first workflow task to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// process activity
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// fail workflow task 5 times
	for i := 1; i <= 5; i++ {
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(i))
		s.NoError(err)
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// process signal
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, signalCount)

	// send another signal to trigger workflow task
	err = s.SendSignal(s.Namespace().String(), workflowExecution, "signalB", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// fail workflow task 2 more times
	for i := 1; i <= 2; i++ {
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(i))
		s.NoError(err)
	}
	s.Equal(3, signalCount)

	// now send a signal during failed workflow task
	sendSignal = true
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(3))
	s.NoError(err)
	s.Equal(4, signalCount)

	// fail workflow task 1 more times
	for i := 1; i <= 2; i++ {
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(i))
		s.NoError(err)
	}
	s.Equal(12, signalCount)

	// Make complete workflow workflow task
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(3))
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(16, signalCount)

	events := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskFailed
 11 WorkflowExecutionSignaled
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskCompleted
 15 WorkflowExecutionSignaled
 16 WorkflowTaskScheduled
 17 WorkflowTaskStarted
 18 WorkflowTaskFailed
 19 WorkflowExecutionSignaled
 20 WorkflowExecutionSignaled
 21 WorkflowExecutionSignaled
 22 WorkflowTaskScheduled
 23 WorkflowTaskStarted
 24 WorkflowTaskFailed
 25 WorkflowTaskScheduled
 26 WorkflowTaskStarted //lastWorkflowTaskStartedEvent
 27 WorkflowTaskCompleted
 28 WorkflowExecutionCompleted //wfCompletedEvent`, events)

	lastWorkflowTaskStartedEvent := events[25]
	s.Equal(lastWorkflowTaskTime, lastWorkflowTaskStartedEvent.GetEventTime().AsTime())
	wfCompletedEvent := events[27]
	s.True(wfCompletedEvent.GetEventTime().AsTime().Sub(lastWorkflowTaskTime) >= time.Second)
}

func (s *WorkflowFailuresTestSuite) TestRespondWorkflowTaskCompleted_ReturnsErrorIfInvalidArgument() {
	id := "functional-respond-workflow-task-completed-test"
	wt := "functional-respond-workflow-task-completed-test-type"
	tq := "functional-respond-workflow-task-completed-test-taskqueue"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         id,
		WorkflowType:       &commonpb.WorkflowType{Name: wt},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           identity,
	}

	we0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.NotNil(we0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "", // Marker name is missing.
					Details:    nil,
					Header:     nil,
					Failure:    nil,
				}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("BadRecordMarkerAttributes: MarkerName is not set on RecordMarkerCommand.", err.Error())

	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we0.GetRunId(),
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled`, historyEvents)
}
