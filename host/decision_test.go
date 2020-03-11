// Copyright (c) 2016 Uber Technologies, Inc.
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
	"encoding/json"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/temporal-proto/serviceerror"

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/codec"
)

func (s *integrationSuite) TestDecisionHeartbeatingWithEmptyResult() {
	id := uuid.New()
	wt := "integration-workflow-decision-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{
		Name: tl,
		Kind: enums.TaskListKindNormal,
	}
	stikyTaskList := &commonproto.TaskList{
		Name: "test-sticky-tasklist",
		Kind: enums.TaskListKindSticky,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 20,
		TaskStartToCloseTimeoutSeconds:      3,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	// start decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 3, enums.EventTypeDecisionTaskStarted)

	taskToken := resp1.GetTaskToken()
	hbTimeout := 0
	for i := 0; i < 12; i++ {
		resp2, err2 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Decisions: []*commonproto.Decision{},
			StickyAttributes: &commonproto.StickyExecutionAttributes{
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
				Domain:   s.domainName,
				TaskList: taskList,
				Identity: identity,
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
		Decisions: []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
				Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("efg"),
				},
				},
			}},
		StickyAttributes: &commonproto.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: false,
	})
	s.NoError(err5)
	s.Nil(resp5.DecisionTask)

	s.assertLastHistoryEvent(we, 41, enums.EventTypeWorkflowExecutionCompleted)
}

func (s *integrationSuite) TestDecisionHeartbeatingWithLocalActivitiesResult() {
	id := uuid.New()
	wt := "integration-workflow-decision-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{
		Name: tl,
		Kind: enums.TaskListKindNormal,
	}
	stikyTaskList := &commonproto.TaskList{
		Name: "test-sticky-tasklist",
		Kind: enums.TaskListKindSticky,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 20,
		TaskStartToCloseTimeoutSeconds:      5,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	// start decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 3, enums.EventTypeDecisionTaskStarted)

	resp2, err2 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp1.GetTaskToken(),
		Decisions: []*commonproto.Decision{},
		StickyAttributes: &commonproto.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: true,
	})
	s.NoError(err2)

	resp3, err3 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp2.DecisionTask.GetTaskToken(),
		Decisions: []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "localActivity1",
					Details:    []byte("abc"),
				},
				},
			}},
		StickyAttributes: &commonproto.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: true,
	})
	s.NoError(err3)

	resp4, err4 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp3.DecisionTask.GetTaskToken(),
		Decisions: []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeRecordMarker,
				Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
					MarkerName: "localActivity2",
					Details:    []byte("abc"),
				},
				},
			}},
		StickyAttributes: &commonproto.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: true,
	})
	s.NoError(err4)

	resp5, err5 := s.engine.RespondDecisionTaskCompleted(NewContext(), &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: resp4.DecisionTask.GetTaskToken(),
		Decisions: []*commonproto.Decision{
			{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
				Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("efg"),
				},
				},
			}},
		StickyAttributes: &commonproto.StickyExecutionAttributes{
			WorkerTaskList:                stikyTaskList,
			ScheduleToStartTimeoutSeconds: 5,
		},
		ReturnNewDecisionTask:      true,
		ForceCreateNewDecisionTask: false,
	})
	s.NoError(err5)
	s.Nil(resp5.DecisionTask)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskCompleted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskCompleted,
		enums.EventTypeMarkerRecorded,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskCompleted,
		enums.EventTypeMarkerRecorded,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskCompleted,
		enums.EventTypeWorkflowExecutionCompleted,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalBeforeRegularDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 3,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, enums.EventTypeWorkflowExecutionSignaled)

	// start this transient decision, the attempt should be cleared and it becomes again a regular decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskStarted)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeWorkflowExecutionSignaled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterRegularDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 3,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	// start decision to make signals into bufferedEvents
	_, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 3, enums.EventTypeDecisionTaskStarted)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, enums.EventTypeDecisionTaskStarted)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionSignaled,
		enums.EventTypeWorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterRegularDecisionStartedAndFailDecision() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 3,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	cause := enums.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure

	// start decision to make signals into bufferedEvents
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 3, enums.EventTypeDecisionTaskStarted)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, enums.EventTypeDecisionTaskStarted)

	// fail this decision to flush buffer, and then another decision will be scheduled
	_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.assertLastHistoryEvent(we, 6, enums.EventTypeDecisionTaskScheduled)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionSignaled,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeWorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalBeforeTransientDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 3,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	cause := enums.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
			Domain:   s.domainName,
			TaskList: taskList,
			Identity: identity,
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

	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 5, enums.EventTypeWorkflowExecutionSignaled)

	// start this transient decision, the attempt should be cleared and it becomes again a regular decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 7, enums.EventTypeDecisionTaskStarted)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionSignaled,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterTransientDecisionStarted() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 3,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	cause := enums.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
			Domain:   s.domainName,
			TaskList: taskList,
			Identity: identity,
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

	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	// start decision to make signals into bufferedEvents
	_, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionSignaled,
		enums.EventTypeWorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) TestWorkflowTerminationSignalAfterTransientDecisionStartedAndFailDecision() {
	id := uuid.New()
	wt := "integration-workflow-transient-decision-test-type"
	tl := id
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 3,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonproto.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enums.EventTypeDecisionTaskScheduled)

	cause := enums.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure
	for i := 0; i < 10; i++ {
		resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
			Domain:   s.domainName,
			TaskList: taskList,
			Identity: identity,
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

	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	// start decision to make signals into bufferedEvents
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Domain:   s.domainName,
		TaskList: taskList,
		Identity: identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	// this signal should be buffered
	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             []byte(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 4, enums.EventTypeDecisionTaskFailed)

	// fail this decision to flush buffer
	_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.assertLastHistoryEvent(we, 6, enums.EventTypeDecisionTaskScheduled)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Domain:            s.domainName,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enums.EventType{
		enums.EventTypeWorkflowExecutionStarted,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeDecisionTaskStarted,
		enums.EventTypeDecisionTaskFailed,
		enums.EventTypeWorkflowExecutionSignaled,
		enums.EventTypeDecisionTaskScheduled,
		enums.EventTypeWorkflowExecutionTerminated,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) assertHistory(we *commonproto.WorkflowExecution, expectedHistory []enums.EventType) {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain:    s.domainName,
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

func (s *integrationSuite) assertLastHistoryEvent(we *commonproto.WorkflowExecution, count int, eventType enums.EventType) {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain:    s.domainName,
		Execution: we,
	})
	s.NoError(err)
	history := historyResponse.History
	data, err := json.MarshalIndent(history, "", "    ")
	s.NoError(err)
	s.Equal(count, len(history.Events), string(data))
	s.Equal(eventType, history.Events[len(history.Events)-1].GetEventType(), string(data))
}
