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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	stickyTaskQueue := taskqueuepb.TaskQueue_builder{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	taskToken := resp1.GetTaskToken()
	hbTimeout := 0
	for i := 0; i < 12; i++ {
		resp2, err2 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
			Namespace: s.Namespace().String(),
			TaskToken: taskToken,
			Commands:  []*commandpb.Command{},
			StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
				WorkerTaskQueue:        stickyTaskQueue,
				ScheduleToStartTimeout: durationpb.New(5 * time.Second),
			}.Build(),
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: true,
		}.Build())
		if _, isNotFound := err2.(*serviceerror.NotFound); isNotFound {
			hbTimeout++
			s.IsType(&workflowservice.RespondWorkflowTaskCompletedResponse{}, resp2)

			resp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
				Namespace: s.Namespace().String(),
				TaskQueue: taskQueue,
				Identity:  identity,
			}.Build())
			s.NoError(err)
			taskToken = resp.GetTaskToken()
		} else {
			s.NoError(err2)
			taskToken = resp2.GetWorkflowTask().GetTaskToken()
		}
		time.Sleep(time.Second) //nolint:forbidigo
	}

	s.Equal(2, hbTimeout)

	resp5, err5 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: taskToken,
		Commands: []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
					Result: payloads.EncodeString("efg"),
				}.Build(),
			}.Build()},
		StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}.Build(),
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	}.Build())
	s.NoError(err5)
	s.Nil(resp5.GetWorkflowTask())

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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	stickyTaskQueue := taskqueuepb.TaskQueue_builder{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(5 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	resp2, err2 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Commands:  []*commandpb.Command{},
		StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}.Build(),
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
	}.Build())
	s.NoError(err2)

	resp3, err3 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: resp2.GetWorkflowTask().GetTaskToken(),
		Commands: []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				RecordMarkerCommandAttributes: commandpb.RecordMarkerCommandAttributes_builder{
					MarkerName: "localActivity1",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}.Build(),
			}.Build()},
		StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}.Build(),
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
	}.Build())
	s.NoError(err3)

	resp4, err4 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: resp3.GetWorkflowTask().GetTaskToken(),
		Commands: []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				RecordMarkerCommandAttributes: commandpb.RecordMarkerCommandAttributes_builder{
					MarkerName: "localActivity2",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}.Build(),
			}.Build()},
		StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}.Build(),
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: true,
	}.Build())
	s.NoError(err4)

	resp5, err5 := s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: resp4.GetWorkflowTask().GetTaskToken(),
		Commands: []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
					Result: payloads.EncodeString("efg"),
				}.Build(),
			}.Build()},
		StickyAttributes: taskqueuepb.StickyExecutionAttributes_builder{
			WorkerTaskQueue:        stickyTaskQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}.Build(),
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	}.Build())
	s.NoError(err5)
	s.Nil(resp5.GetWorkflowTask())

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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled`, s.GetHistory(s.Namespace().String(), we))

	// start this transient workflow task, the attempt should be cleared and it becomes again a regular workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	}.Build())
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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task to make signals into bufferedEvents
	_, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	}.Build())
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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE

	// start workflow task to make signals into bufferedEvents
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.GetHistory(s.Namespace().String(), we))

	// fail this workflow task to flush buffer, and then another workflow task will be scheduled
	_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	}.Build())
	s.NoError(err2)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	}.Build())
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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := 0; i < 10; i++ {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		}.Build())
		s.NoError(err1)
		s.Equal(int32(i+1), resp1.GetAttempt())
		if i == 0 {
			// first time is regular workflow task
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient workflow task
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
			Namespace: s.Namespace().String(),
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		}.Build())
		s.NoError(err2)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled`, s.GetHistory(s.Namespace().String(), we))

	// start this transient workflow task, the attempt should be cleared and it becomes again a regular workflow task
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
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
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	}.Build())
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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := 0; i < 10; i++ {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		}.Build())
		s.NoError(err1)
		s.Equal(int32(i+1), resp1.GetAttempt())
		if i == 0 {
			// first time is regular workflow task
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient workflow task
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
			Namespace: s.Namespace().String(),
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		}.Build())
		s.NoError(err2)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task to make signals into bufferedEvents
	_, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	}.Build())
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

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	resp0, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	we := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      resp0.GetRunId(),
	}.Build()

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := 0; i < 10; i++ {
		resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  identity,
		}.Build())
		s.NoError(err1)
		s.Equal(int32(i+1), resp1.GetAttempt())
		if i == 0 {
			// first time is regular workflow task
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient workflow task
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
			Namespace: s.Namespace().String(),
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		}.Build())
		s.NoError(err2)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	// start workflow task to make signals into bufferedEvents
	resp1, err1 := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	// this signal should be buffered
	_, err0 = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.NewString(),
	}.Build())
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.GetHistory(s.Namespace().String(), we))

	// fail this workflow task to flush buffer
	_, err2 := s.FrontendClient().RespondWorkflowTaskFailed(testcore.NewContext(), workflowservice.RespondWorkflowTaskFailedRequest_builder{
		Namespace: s.Namespace().String(),
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	}.Build())
	s.NoError(err2)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled`, s.GetHistory(s.Namespace().String(), we))

	// then terminate the workflow
	_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: we,
		Reason:            "test-reason",
	}.Build())
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
