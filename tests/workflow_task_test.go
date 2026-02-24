package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkflowTaskTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowTaskTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowTaskTestSuite))
}

func (s *WorkflowTaskTestSuite) TestWorkflowTaskHeartbeatingWithEmptyResult() {
	id := uuid.NewString()
	wt := "functional-workflow-workflow-task-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	taskToken := resp1.GetTaskToken()
	hbTimeout := 0
	for range 12 {
		resp2, err2 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: taskToken,
			Commands:  []*commandpb.Command{},
			StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
				WorkerTaskQueue:        stickyTaskQueue,
				ScheduleToStartTimeout: durationpb.New(5 * time.Second),
			},
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: true,
		})
		if _, isNotFound := err2.(*serviceerror.NotFound); isNotFound {
			hbTimeout++
			s.IsType(&workflowservice.RespondWorkflowTaskCompletedResponse{}, resp2)

			resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.Namespace().String(),
				TaskQueue: taskQueue,
				Identity:  identity,
			})
			s.NoError(err)
			taskToken = resp.GetTaskToken()
		} else {
			s.NoError(err2)
			taskToken = resp2.WorkflowTask.GetTaskToken()
		}
		time.Sleep(time.Second) //nolint:forbidigo
	}

	s.Equal(2, hbTimeout)

	resp5, err5 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: taskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("efg"),
				},
				},
			}},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	})
	s.NoError(err5)
	s.Nil(resp5.WorkflowTask)

	s.EqualHistoryEvents(`
 1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowTaskScheduled
 18 WorkflowTaskStarted
 19 WorkflowTaskTimedOut
 20 WorkflowTaskScheduled
 21 WorkflowTaskStarted
 22 WorkflowTaskTimedOut
 23 WorkflowTaskScheduled
 24 WorkflowTaskStarted
 25 WorkflowTaskCompleted
 26 WorkflowTaskScheduled
 27 WorkflowTaskStarted
 28 WorkflowTaskCompleted
 29 WorkflowTaskScheduled
 30 WorkflowTaskStarted
 31 WorkflowTaskCompleted
 32 WorkflowTaskScheduled
 33 WorkflowTaskStarted
 34 WorkflowTaskCompleted
 35 WorkflowTaskScheduled
 36 WorkflowTaskStarted
 37 WorkflowTaskTimedOut
 38 WorkflowTaskScheduled
 39 WorkflowTaskStarted
 40 WorkflowTaskTimedOut
 41 WorkflowTaskScheduled
 42 WorkflowTaskStarted
 43 WorkflowTaskCompleted
 44 WorkflowTaskScheduled
 45 WorkflowTaskStarted
 46 WorkflowTaskCompleted
 47 WorkflowExecutionCompleted`, s.GetHistory(s.Namespace().String(), we))
}

func (s *WorkflowTaskTestSuite) TestWorkflowTaskHeartbeatingWithLocalActivitiesResult() {
	id := uuid.NewString()
	wt := "functional-workflow-workflow-task-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(5 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	resp2, err2 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Commands:  []*commandpb.Command{},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
	})
	s.NoError(err2)

	resp3, err3 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp2.WorkflowTask.GetTaskToken(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "localActivity1",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
	})
	s.NoError(err3)

	resp4, err4 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp3.WorkflowTask.GetTaskToken(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "localActivity2",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
			}},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
	})
	s.NoError(err4)

	resp5, err5 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp4.WorkflowTask.GetTaskToken(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("efg"),
				},
				},
			}},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	})
	s.NoError(err5)
	s.Nil(resp5.WorkflowTask)

	historyEvents := s.GetHistory(s.Namespace().String(), we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 MarkerRecorded
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 MarkerRecorded
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
 16 WorkflowExecutionCompleted`, historyEvents)
}

func (s *WorkflowTaskTestSuite) TestWorkflowTerminationSignalBeforeRegularWorkflowTaskStarted() {
	id := uuid.NewString()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
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
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled`, s.GetHistory(s.Namespace().String(), we))

	// start this transient workflow task, the attempt should be cleared and it becomes again a regular workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowTaskStarted
  5 WorkflowTaskFailed
  6 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace().String(), we))
}

func (s *WorkflowTaskTestSuite) TestWorkflowTerminationSignalAfterRegularWorkflowTaskStarted() {
	id := uuid.NewString()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
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
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task to make signals into bufferedEvents
	_, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	historyEvents := s.GetHistory(s.Namespace().String(), we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionTerminated`, historyEvents)
}

func (s *WorkflowTaskTestSuite) TestWorkflowTerminationSignalAfterRegularWorkflowTaskStartedAndFailWorkflowTask() {
	id := uuid.NewString()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
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
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE

	// start workflow task to make signals into bufferedEvents
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// fail this workflow task to flush buffer, and then another workflow task will be scheduled
	_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace().String(), we))
}

func (s *WorkflowTaskTestSuite) TestWorkflowTerminationSignalBeforeTransientWorkflowTaskStarted() {
	id := uuid.NewString()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
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
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := range 10 {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int32(i+1), resp1.GetAttempt())
		if i == 0 {
			// first time is regular workflow task
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient workflow task
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		})
		s.NoError(err2)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err0)
	histAfterSignal := s.GetHistory(s.Namespace().String(), we)
	s.GreaterOrEqual(len(histAfterSignal), 5, "Should have at least 5 events after signal")
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled`, histAfterSignal[:5])

	// start this transient workflow task, the attempt should be cleared and it becomes again a regular workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskFailed
  9 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace().String(), we))
}

func (s *WorkflowTaskTestSuite) TestWorkflowTerminationSignalAfterTransientWorkflowTaskStarted() {
	id := uuid.NewString()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
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
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := range 10 {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int32(i+1), resp1.GetAttempt())
		if i == 0 {
			// first time is regular workflow task
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient workflow task
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		})
		s.NoError(err2)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task to make signals into bufferedEvents
	_, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace().String(), we))
}

func (s *WorkflowTaskTestSuite) TestWorkflowTerminationSignalAfterTransientWorkflowTaskStartedAndFailWorkflowTask() {
	id := uuid.NewString()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
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
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := range 10 {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int32(i+1), resp1.GetAttempt())
		if i == 0 {
			// first time is regular workflow task
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient workflow task
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		})
		s.NoError(err2)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task to make signals into bufferedEvents
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// fail this workflow task to flush buffer
	_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace().String(), we))
}

// TestRawHistoryFlowWithSearchAttributes tests that workflows with search attributes
// work correctly when raw history is sent between internal services.
// This test verifies that:
// 1. Search attributes are properly serialized in the workflow started event
// 2. The history is correctly returned via PollWorkflowTaskQueue response
// 3. Search attributes are properly processed through the raw history path
// Note: SendRawHistoryBetweenInternalServices is enabled by default in functional tests
// (see tests/testcore/dynamic_config_overrides.go)
func (s *WorkflowTaskTestSuite) TestRawHistoryFlowWithSearchAttributes() {
	id := uuid.NewString()
	wt := "functional-workflow-raw-history-search-attributes"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Create search attributes with a custom keyword
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": {
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte(`"test-search-value"`),
			},
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(5 * time.Second),
		Identity:            identity,
		SearchAttributes:    searchAttr,
	}

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	// Poll for the workflow task - this exercises the raw history flow:
	// History Service -> Matching Service -> Frontend Service
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	// Verify that we received a valid response with history
	s.NotNil(resp1)
	s.NotEmpty(resp1.GetTaskToken())
	s.NotNil(resp1.GetHistory())
	s.NotEmpty(resp1.GetHistory().GetEvents())

	// Verify the workflow started event contains search attributes
	startedEvent := resp1.GetHistory().GetEvents()[0]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startedEvent.GetEventType())

	startedAttrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttrs)
	s.NotNil(startedAttrs.GetSearchAttributes())
	s.Contains(startedAttrs.GetSearchAttributes().GetIndexedFields(), "CustomKeywordField")

	// Complete the workflow task
	_, err2 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("done"),
					},
				},
			},
		},
		Identity: identity,
	})
	s.NoError(err2)

	// Verify the final history
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, s.GetHistory(s.Namespace().String(), we))
}
