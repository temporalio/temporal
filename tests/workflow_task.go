// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/payloads"
)

func (s *FunctionalSuite) TestWorkflowTaskHeartbeatingWithEmptyResult() {
	id := uuid.New()
	wt := "functional-workflow-workflow-task-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	// start workflow task
	resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	taskToken := resp1.GetTaskToken()
	hbTimeout := 0
	for i := 0; i < 12; i++ {
		resp2, err2 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.namespace,
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

			resp, err := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.namespace,
				TaskQueue: taskQueue,
				Identity:  identity,
			})
			s.NoError(err)
			taskToken = resp.GetTaskToken()
		} else {
			s.NoError(err2)
			taskToken = resp2.WorkflowTask.GetTaskToken()
		}
		time.Sleep(time.Second)
	}

	s.Equal(2, hbTimeout)

	resp5, err5 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
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
 47 WorkflowExecutionCompleted`, s.getHistory(s.namespace, we))
}

func (s *FunctionalSuite) TestWorkflowTaskHeartbeatingWithLocalActivitiesResult() {
	id := uuid.New()
	wt := "functional-workflow-workflow-task-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(5 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	// start workflow task
	resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	resp2, err2 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
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

	resp3, err3 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
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

	resp4, err4 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
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

	resp5, err5 := s.engine.RespondWorkflowTaskCompleted(NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
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

	historyEvents := s.getHistory(s.namespace, we)
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

func (s *FunctionalSuite) TestWorkflowTerminationSignalBeforeRegularWorkflowTaskStarted() {
	id := uuid.New()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled`, s.getHistory(s.namespace, we))

	// start this transient workflow task, the attempt should be cleared and it becomes again a regular workflow task
	resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int32(1), resp1.GetAttempt())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	// then terminate the workflow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
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
  6 WorkflowExecutionTerminated`, s.getHistory(s.namespace, we))
}

func (s *FunctionalSuite) TestWorkflowTerminationSignalAfterRegularWorkflowTaskStarted() {
	id := uuid.New()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	// start workflow task to make signals into bufferedEvents
	_, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	// then terminate the workflow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	historyEvents := s.getHistory(s.namespace, we)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionTerminated`, historyEvents)
}

func (s *FunctionalSuite) TestWorkflowTerminationSignalAfterRegularWorkflowTaskStartedAndFailWorkflowTask() {
	id := uuid.New()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE

	// start workflow task to make signals into bufferedEvents
	resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	// fail this workflow task to flush buffer, and then another workflow task will be scheduled
	_, err2 := s.engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.namespace,
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
  6 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	// then terminate the workflow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
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
  7 WorkflowExecutionTerminated`, s.getHistory(s.namespace, we))
}

func (s *FunctionalSuite) TestWorkflowTerminationSignalBeforeTransientWorkflowTaskStarted() {
	id := uuid.New()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
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

		_, err2 := s.engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: s.namespace,
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
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled`, s.getHistory(s.namespace, we))

	// start this transient workflow task, the attempt should be cleared and it becomes again a regular workflow task
	resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
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
  7 WorkflowTaskStarted`, s.getHistory(s.namespace, we))

	// then terminate the workflow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
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
  9 WorkflowExecutionTerminated`, s.getHistory(s.namespace, we))
}

func (s *FunctionalSuite) TestWorkflowTerminationSignalAfterTransientWorkflowTaskStarted() {
	id := uuid.New()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
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

		_, err2 := s.engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: s.namespace,
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
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	// start workflow task to make signals into bufferedEvents
	_, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	// then terminate the workflow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
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
  6 WorkflowExecutionTerminated`, s.getHistory(s.namespace, we))
}

func (s *FunctionalSuite) TestWorkflowTerminationSignalAfterTransientWorkflowTaskStartedAndFailWorkflowTask() {
	id := uuid.New()
	wt := "functional-workflow-transient-workflow-task-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(3 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	cause := enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.namespace,
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

		_, err2 := s.engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: s.namespace,
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
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	// start workflow task to make signals into bufferedEvents
	resp1, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err1)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed`, s.getHistory(s.namespace, we))

	// fail this workflow task to flush buffer
	_, err2 := s.engine.RespondWorkflowTaskFailed(NewContext(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.namespace,
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
  6 WorkflowTaskScheduled`, s.getHistory(s.namespace, we))

	// then terminate the workflow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
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
  7 WorkflowExecutionTerminated`, s.getHistory(s.namespace, we))
}
