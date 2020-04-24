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

package host

import (
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/temporal-proto/serviceerror"

	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/codec"
)

func (s *integrationSuite) TestDecisionHeartbeatingWithEmptyResult() {
	id := uuid.New()
	wt := "integration-workflow-decision-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{
		Name: tl,
		Kind: tasklistpb.TaskListKind_Normal,
	}
	stikyTaskList := &tasklistpb.TaskList{
		Name: "test-sticky-tasklist",
		Kind: tasklistpb.TaskListKind_Sticky,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 20,
		WorkflowTaskTimeoutSeconds:      3,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	// start decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 3, eventpb.EventType_DecisionTaskStarted)

	taskToken := resp1.GetTaskToken()
	hbTimeout := 0
	for i := 0; i < 12; i++ {
		resp2, err2 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Decisions: []*decisionpb.Decision{},
			StickyAttributes: &decisionpb.StickyExecutionAttributes{
				WorkerTaskList:                stikyTaskList,
				ScheduleToStartTimeoutSeconds: 5,
			},
			ReturnNewDecisionTask:      true,
			ForceCreateNewDecisionTask: true,
		})
		if _, ok := err2.(*serviceerror.NotFound); ok {
			hbTimeout++
			s.IsType(&workflowservice.RespondDecisionTaskCompletedResponse{}, resp2)

			resp, err := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
				Namespace: s.namespace,
				TaskList:  taskList,
				Identity:  identity,
			})
			s.NoError(err)
			taskToken = resp.GetTaskToken()
		} else {
			s.NoError(err2)
			taskToken = resp2.DecisionTask.GetTaskToken()
		}
		time.Sleep(time.Second)
	}

	s.Equal(2, hbTimeout)

	resp5, err5 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Decisions: []*decisionpb.Decision{
			{
				DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("efg"),
				},
				},
			}},
		StickyAttributes: &decisionpb.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: false,
	})
	s.NoError(err5)
	s.Nil(resp5.DecisionTask)

	s.assertLastHistoryEvent(we, 41, eventpb.EventType_WorkflowExecutionCompleted)
}

func (s *integrationSuite) TestDecisionHeartbeatingWithLocalActivitiesResult() {
	id := uuid.New()
	wt := "integration-workflow-decision-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{
		Name: tl,
		Kind: tasklistpb.TaskListKind_Normal,
	}
	stikyTaskList := &tasklistpb.TaskList{
		Name: "test-sticky-tasklist",
		Kind: tasklistpb.TaskListKind_Sticky,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 20,
		WorkflowTaskTimeoutSeconds:      5,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	// start decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 3, eventpb.EventType_DecisionTaskStarted)

	resp2, err2 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp1.GetTaskToken(),
		Decisions: []*decisionpb.Decision{},
		StickyAttributes: &decisionpb.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: true,
	})
	s.NoError(err2)

	resp3, err3 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp2.DecisionTask.GetTaskToken(),
		Decisions: []*decisionpb.Decision{
			{
				DecisionType: decisionpb.DecisionType_RecordMarker,
				Attributes: &decisionpb.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &decisionpb.RecordMarkerDecisionAttributes{
					MarkerName: "localActivity1",
					Details:    []byte("abc"),
				},
				},
			}},
		StickyAttributes: &decisionpb.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: true,
	})
	s.NoError(err3)

	resp4, err4 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp3.DecisionTask.GetTaskToken(),
		Decisions: []*decisionpb.Decision{
			{
				DecisionType: decisionpb.DecisionType_RecordMarker,
				Attributes: &decisionpb.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &decisionpb.RecordMarkerDecisionAttributes{
					MarkerName: "localActivity2",
					Details:    []byte("abc"),
				},
				},
			}},
		StickyAttributes: &decisionpb.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: true,
	})
	s.NoError(err4)

	resp5, err5 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp4.DecisionTask.GetTaskToken(),
		Decisions: []*decisionpb.Decision{
			{
				DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("efg"),
				},
				},
			}},
		StickyAttributes: &decisionpb.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: false,
	})
	s.NoError(err5)
	s.Nil(resp5.DecisionTask)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskCompleted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskCompleted,
		eventpb.EventType_MarkerRecorded,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskCompleted,
		eventpb.EventType_MarkerRecorded,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskCompleted,
		eventpb.EventType_WorkflowExecutionCompleted,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalBeforeRegularDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 3,
		WorkflowTaskTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, eventpb.EventType_WorkflowExecutionSignaled)

	// start this transient decision, the attempt should be cleared and it becomes again a regular decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskStarted)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_WorkflowExecutionSignaled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterRegularDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 3,
		WorkflowTaskTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	// start decision to make signals into bufferedEvents
	_, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 3, eventpb.EventType_DecisionTaskStarted)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, eventpb.EventType_DecisionTaskStarted)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionSignaled,
		eventpb.EventType_WorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterRegularDecisionStartedAndFailDecision() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 3,
		WorkflowTaskTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	cause := eventpb.DecisionTaskFailedCause_WorkflowWorkerUnhandledFailure

	// start decision to make signals into bufferedEvents
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 3, eventpb.EventType_DecisionTaskStarted)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, eventpb.EventType_DecisionTaskStarted)

	// fail this decision to flush buffer, and then another decision will be scheduled
	_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.assertLastHistoryEvent(we, 6, eventpb.EventType_DecisionTaskScheduled)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionSignaled,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_WorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalBeforeTransientDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 3,
		WorkflowTaskTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	cause := eventpb.DecisionTaskFailedCause_WorkflowWorkerUnhandledFailure
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
			Namespace: s.namespace,
			TaskList:  taskList,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int64(i), resp1.GetAttempt())
		if i == 0 {
			// first time is regular decision
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient decision
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		})
		s.NoError(err2)
	}

	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 5, eventpb.EventType_WorkflowExecutionSignaled)

	// start this transient decision, the attempt should be cleared and it becomes again a regular decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 7, eventpb.EventType_DecisionTaskStarted)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionSignaled,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterTransientDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 3,
		WorkflowTaskTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	cause := eventpb.DecisionTaskFailedCause_WorkflowWorkerUnhandledFailure
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
			Namespace: s.namespace,
			TaskList:  taskList,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int64(i), resp1.GetAttempt())
		if i == 0 {
			// first time is regular decision
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient decision
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		})
		s.NoError(err2)
	}

	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	// start decision to make signals into bufferedEvents
	_, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionSignaled,
		eventpb.EventType_WorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterTransientDecisionStartedAndFailDecision() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Namespace:                           s.namespace,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		WorkflowRunTimeoutSeconds: 3,
		WorkflowTaskTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, eventpb.EventType_DecisionTaskScheduled)

	cause := eventpb.DecisionTaskFailedCause_WorkflowWorkerUnhandledFailure
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
			Namespace: s.namespace,
			TaskList:  taskList,
			Identity:  identity,
		})
		s.NoError(err1)
		s.Equal(int64(i), resp1.GetAttempt())
		if i == 0 {
			// first time is regular decision
			s.Equal(int64(3), resp1.GetStartedEventId())
		} else {
			// the rest is transient decision
			s.Equal(int64(6), resp1.GetStartedEventId())
		}

		_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
			TaskToken: resp1.GetTaskToken(),
			Cause:     cause,
			Identity:  "integ test",
		})
		s.NoError(err2)
	}

	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	// start decision to make signals into bufferedEvents
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 4, eventpb.EventType_DecisionTaskFailed)

	// fail this decision to flush buffer
	_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.assertLastHistoryEvent(we, 6, eventpb.EventType_DecisionTaskScheduled)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []eventpb.EventType{
		eventpb.EventType_WorkflowExecutionStarted,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_DecisionTaskStarted,
		eventpb.EventType_DecisionTaskFailed,
		eventpb.EventType_WorkflowExecutionSignaled,
		eventpb.EventType_DecisionTaskScheduled,
		eventpb.EventType_WorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) assertHistory(we *executionpb.WorkflowExecution, expectedHistory []eventpb.EventType) {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: we,
	})
	s.NoError(err)
	history := historyResponse.History
	encoder := codec.NewJSONPBIndentEncoder("    ")
	data, err := encoder.Encode(history)
	s.NoError(err)
	s.Equal(len(expectedHistory), len(history.Events), string(data))
	for i, e := range history.Events {
		s.Equal(expectedHistory[i], e.GetEventType(), "%v, %v, %v", strconv.Itoa(i), e.GetEventType().String(), string(data))
	}
}

func (s *integrationSuite) assertLastHistoryEvent(we *executionpb.WorkflowExecution, count int, eventType eventpb.EventType) {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: we,
	})
	s.NoError(err)
	history := historyResponse.History
	encoder := codec.NewJSONPBIndentEncoder("    ")
	data, err := encoder.Encode(history)
	s.NoError(err)
	s.Equal(count, len(history.Events), string(data))
	s.Equal(eventType, history.Events[len(history.Events)-1].GetEventType(), string(data))
}
