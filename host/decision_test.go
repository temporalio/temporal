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
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	commonpb "go.temporal.io/temporal-proto/common/v1"
	decisionpb "go.temporal.io/temporal-proto/decision/v1"
	tasklistpb "go.temporal.io/temporal-proto/tasklist/v1"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	"github.com/temporalio/temporal/common/codec"
	"github.com/temporalio/temporal/common/payloads"
)

func (s *integrationSuite) TestDecisionHeartbeatingWithEmptyResult() {
	id := uuid.New()
	wt := "integration-workflow-decision-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{
		Name: tl,
		Kind: enumspb.TASK_LIST_KIND_NORMAL,
	}
	stikyTaskList := &tasklistpb.TaskList{
		Name: "test-sticky-tasklist",
		Kind: enumspb.TASK_LIST_KIND_STICKY,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  20,
		WorkflowTaskTimeoutSeconds: 3,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	// start decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

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
				DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: payloads.EncodeString("efg"),
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

	s.assertLastHistoryEvent(we, 41, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
}

func (s *integrationSuite) TestDecisionHeartbeatingWithLocalActivitiesResult() {
	id := uuid.New()
	wt := "integration-workflow-decision-heartbeating-local-activities"
	tl := id
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{
		Name: tl,
		Kind: enumspb.TASK_LIST_KIND_NORMAL,
	}
	stikyTaskList := &tasklistpb.TaskList{
		Name: "test-sticky-tasklist",
		Kind: enumspb.TASK_LIST_KIND_STICKY,
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  20,
		WorkflowTaskTimeoutSeconds: 5,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	// start decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

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
				DecisionType: enumspb.DECISION_TYPE_RECORD_MARKER,
				Attributes: &decisionpb.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &decisionpb.RecordMarkerDecisionAttributes{
					MarkerName: "localActivity1",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
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
				DecisionType: enumspb.DECISION_TYPE_RECORD_MARKER,
				Attributes: &decisionpb.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &decisionpb.RecordMarkerDecisionAttributes{
					MarkerName: "localActivity2",
					Details: map[string]*commonpb.Payloads{
						"data":   payloads.EncodeString("local activity marker"),
						"result": payloads.EncodeString("local activity result"),
					}}},
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
				DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: payloads.EncodeString("efg"),
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

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		enumspb.EVENT_TYPE_MARKER_RECORDED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		enumspb.EVENT_TYPE_MARKER_RECORDED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  3,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)

	// start this transient decision, the attempt should be cleared and it becomes again a regular decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  3,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	// start decision to make signals into bufferedEvents
	_, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

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
	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  3,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	cause := enumspb.DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE

	// start decision to make signals into bufferedEvents
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

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
	s.assertLastHistoryEvent(we, 3, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

	// fail this decision to flush buffer, and then another decision will be scheduled
	_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.assertLastHistoryEvent(we, 6, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  3,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	cause := enumspb.DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
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

	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

	_, err0 = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		SignalName:        "sig-for-integ-test",
		Input:             payloads.EncodeString(""),
		Identity:          "integ test",
		RequestId:         uuid.New(),
	})
	s.NoError(err0)
	s.assertLastHistoryEvent(we, 5, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)

	// start this transient decision, the attempt should be cleared and it becomes again a regular decision
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.Equal(int64(0), resp1.GetAttempt())
	s.assertLastHistoryEvent(we, 7, enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  3,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	cause := enumspb.DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
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

	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

	// start decision to make signals into bufferedEvents
	_, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

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
	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
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
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  3,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	resp0, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	we := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      resp0.RunId,
	}

	s.assertLastHistoryEvent(we, 2, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	cause := enumspb.DECISION_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
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

	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

	// start decision to make signals into bufferedEvents
	resp1, err1 := s.engine.PollForDecisionTask(NewContext(), &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
		TaskList:  taskList,
		Identity:  identity,
	})
	s.NoError(err1)

	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

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
	s.assertLastHistoryEvent(we, 4, enumspb.EVENT_TYPE_DECISION_TASK_FAILED)

	// fail this decision to flush buffer
	_, err2 := s.engine.RespondDecisionTaskFailed(NewContext(), &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: resp1.GetTaskToken(),
		Cause:     cause,
		Identity:  "integ test",
	})
	s.NoError(err2)
	s.assertLastHistoryEvent(we, 6, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	// then terminate the worklfow
	_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: we,
		Reason:            "test-reason",
	})
	s.NoError(err)

	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		enumspb.EVENT_TYPE_DECISION_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
	}
	s.assertHistory(we, expectedHistory)
}

func (s *integrationSuite) assertHistory(we *commonpb.WorkflowExecution, expectedHistory []enumspb.EventType) {
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

func (s *integrationSuite) assertLastHistoryEvent(we *commonpb.WorkflowExecution, count int, eventType enumspb.EventType) {
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
