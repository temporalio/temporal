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
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
)

func (s *FunctionalSuite) TestContinueAsNewWorkflow() {
	id := "functional-continue-as-new-workflow-test"
	wt := "functional-continue-as-new-workflow-test-type"
	tl := "functional-continue-as-new-workflow-test-taskqueue"
	identity := "worker1"
	saName := "CustomKeywordField"
	// Uncomment this line to test with mapper.
	// saName = "AliasForCustomKeywordField"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample payload")},
	}
	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{"memoKey": payload.EncodeString("memoVal")},
	}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			saName: payload.EncodeString("random"),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		Memo:                memo,
		SearchAttributes:    searchAttr,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = task.WorkflowExecution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						Input:               payloads.EncodeBytes(buf.Bytes()),
						Header:              header,
						Memo:                memo,
						SearchAttributes:    searchAttr,
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}}, nil
		}

		lastRunStartedEvent = task.History.Events[0]
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
	s.NotNil(lastRunStartedEvent)
	lastRunStartedEventAttrs := lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	lastRunStartedEventSearchAttrs := lastRunStartedEventAttrs.GetSearchAttributes()
	s.Equal(previousRunID, lastRunStartedEventAttrs.GetContinuedExecutionRunId())
	// top-level workflow doesn't have parent, and root is itself (nil in history event)
	s.Nil(lastRunStartedEventAttrs.GetParentWorkflowExecution())
	s.Nil(lastRunStartedEventAttrs.GetRootWorkflowExecution())
	s.ProtoEqual(header, lastRunStartedEventAttrs.Header)
	s.ProtoEqual(memo, lastRunStartedEventAttrs.Memo)
	s.Equal(
		searchAttr.GetIndexedFields()[saName].GetData(),
		lastRunStartedEventSearchAttrs.GetIndexedFields()[saName].GetData(),
	)
	s.Equal(
		"Keyword",
		string(lastRunStartedEventSearchAttrs.GetIndexedFields()[saName].GetMetadata()["type"]),
	)
}

func (s *FunctionalSuite) TestContinueAsNewRun_Timeout() {
	id := "functional-continue-as-new-workflow-timeout-test"
	wt := "functional-continue-as-new-workflow-timeout-test-type"
	tl := "functional-continue-as-new-workflow-timeout-test-taskqueue"
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
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(1)
	continueAsNewCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if continueAsNewCounter < continueAsNewCount {
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						Input:               payloads.EncodeBytes(buf.Bytes()),
						WorkflowRunTimeout:  durationpb.New(1 * time.Second), // set timeout to 1
						WorkflowTaskTimeout: durationpb.New(1 * time.Second),
					},
				},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// process the workflow task and continue as new
	_, err := poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)

	time.Sleep(1 * time.Second) // wait 1 second for timeout

	var historyEvents []*historypb.HistoryEvent
	for i := 0; i < 20; i++ {
		historyEvents = s.getHistory(s.namespace, &commonpb.WorkflowExecution{
			WorkflowId: id,
		})
		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
			s.Logger.Warn("Execution not timedout yet")
			time.Sleep(200 * time.Millisecond)
			continue
		}

		workflowComplete = true
		break
	}
	s.True(workflowComplete)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionTimedOut`, historyEvents)
}

func (s *FunctionalSuite) TestWorkflowContinueAsNew_TaskID() {
	id := "functional-wf-continue-as-new-task-id-test"
	wt := "functional-wf-continue-as-new-task-id-type"
	tl := "functional-wf-continue-as-new-task-id-taskqueue"
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
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution

	continueAsNewed := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)

		if !continueAsNewed {
			continueAsNewed = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						Input:               nil,
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(1 * time.Second),
					},
				},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("succeed"),
				},
			},
		}}, nil

	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	minTaskID := int64(0)
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.getHistory(s.namespace, executions[0])
	s.True(len(events) != 0)
	for _, event := range events {
		s.True(event.GetTaskId() > minTaskID)
		minTaskID = event.GetTaskId()
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[1])
	s.True(len(events) != 0)
	for _, event := range events {
		s.True(event.GetTaskId() > minTaskID)
		minTaskID = event.GetTaskId()
	}
}

type (
	ParentWithChildContinueAsNew struct {
		suite *FunctionalSuite

		parentID           string
		parentType         string
		childID            string
		childType          string
		parentWorkflowType *commonpb.WorkflowType
		childWorkflowType  *commonpb.WorkflowType
		closePolicy        enumspb.ParentClosePolicy

		childComplete         bool
		childExecutionStarted bool
		childData             int32
		continueAsNewCount    int32
		continueAsNewCounter  int32
		startedEvent          *historypb.HistoryEvent
		completedEvent        *historypb.HistoryEvent
		childStartedEvents    []*historypb.HistoryEvent
	}
)

func newParentWithChildContinueAsNew(
	s *FunctionalSuite,
	parentID, parentType, childID, childType string,
	closePolicy enumspb.ParentClosePolicy,
) *ParentWithChildContinueAsNew {
	workflow := &ParentWithChildContinueAsNew{
		suite:       s,
		parentID:    parentID,
		parentType:  parentType,
		childID:     childID,
		childType:   childType,
		closePolicy: closePolicy,

		childComplete:         false,
		childExecutionStarted: false,
		childData:             int32(1),
		continueAsNewCount:    int32(10),
		continueAsNewCounter:  int32(0),
	}
	workflow.parentWorkflowType = &commonpb.WorkflowType{}
	workflow.parentWorkflowType.Name = parentType

	workflow.childWorkflowType = &commonpb.WorkflowType{}
	workflow.childWorkflowType.Name = childType

	return workflow
}

func (w *ParentWithChildContinueAsNew) workflow(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
	w.suite.Logger.Info(
		"Processing workflow task for WorkflowId:",
		tag.WorkflowID(task.WorkflowExecution.GetWorkflowId()),
	)

	// Child workflow logic
	if task.WorkflowExecution.GetWorkflowId() == w.childID {
		if task.PreviousStartedEventId <= 0 {
			w.childStartedEvents = append(w.childStartedEvents, task.History.Events[0])
		}

		if w.continueAsNewCounter < w.continueAsNewCount {
			w.continueAsNewCounter++
			buf := new(bytes.Buffer)
			w.suite.Nil(binary.Write(buf, binary.LittleEndian, w.continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						Input: payloads.EncodeBytes(buf.Bytes()),
					},
				},
			}}, nil
		}

		w.childComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Child Done"),
				},
			},
		}}, nil
	}

	// Parent workflow logic
	if task.WorkflowExecution.GetWorkflowId() == w.parentID {
		if !w.childExecutionStarted {
			w.suite.Logger.Info("Starting child execution")
			w.childExecutionStarted = true
			buf := new(bytes.Buffer)
			w.suite.Nil(binary.Write(buf, binary.LittleEndian, w.childData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace:         w.suite.namespace,
						WorkflowId:        w.childID,
						WorkflowType:      w.childWorkflowType,
						Input:             payloads.EncodeBytes(buf.Bytes()),
						ParentClosePolicy: w.closePolicy,
					},
				},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					w.startedEvent = event
					return []*commandpb.Command{}, nil
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
					w.completedEvent = event
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("Done"),
							},
						},
					}}, nil
				}
			}
		}
	}

	return nil, nil
}

func (s *FunctionalSuite) TestChildWorkflowWithContinueAsNew() {
	parentID := "functional-child-workflow-with-continue-as-new-test-parent"
	childID := "functional-child-workflow-with-continue-as-new-test-child"
	wtParent := "functional-child-workflow-with-continue-as-new-test-parent-type"
	wtChild := "functional-child-workflow-with-continue-as-new-test-child-type"
	tl := "functional-child-workflow-with-continue-as-new-test-taskqueue"
	identity := "worker1"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	definition := newParentWithChildContinueAsNew(
		s,
		parentID,
		wtParent,
		childID,
		wtChild,
		enumspb.PARENT_CLOSE_POLICY_ABANDON,
	)

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        definition.parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: definition.workflow,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to start child execution
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(definition.childExecutionStarted)

	// Process ChildExecution Started event and all generations of child executions
	for i := 0; i < 11; i++ {
		s.Logger.Info("workflow task", tag.Counter(i))
		_, err = poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	s.Len(definition.childStartedEvents, 10)
	for _, childStartedEvent := range definition.childStartedEvents {
		s.NotNil(childStartedEvent)
		childStartedEventAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
		s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
		s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
		s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
	}

	s.False(definition.childComplete)
	s.NotNil(definition.startedEvent)

	// Process Child Execution final workflow task to complete it
	_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(definition.childComplete)

	// Process ChildExecution completed event and complete parent execution
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(definition.completedEvent)
	completedAttributes := definition.completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.namespace, completedAttributes.Namespace)
	// TODO: change to s.Equal(s.namespaceID) once it is available.
	s.NotEmpty(completedAttributes.Namespace)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.NotEqual(
		definition.startedEvent.GetChildWorkflowExecutionStartedEventAttributes().WorkflowExecution.RunId,
		completedAttributes.WorkflowExecution.RunId,
	)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	s.Equal("Child Done", s.decodePayloadsString(completedAttributes.GetResult()))

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 StartChildWorkflowExecutionInitiated
  6 ChildWorkflowExecutionStarted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 ChildWorkflowExecutionCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: parentID,
		RunId:      we.RunId,
	}))
}

func (s *FunctionalSuite) TestChildWorkflowWithContinueAsNewParentTerminate() {
	parentID := "functional-child-workflow-with-continue-as-new-parent-terminate-test-parent"
	childID := "functional-child-workflow-with-continue-as-new-parent-terminate-test-child"
	wtParent := "functional-child-workflow-with-continue-as-new-parent-terminate-test-parent-type"
	wtChild := "functional-child-workflow-with-continue-as-new-parent-terminate-test-child-type"
	tl := "functional-child-workflow-with-continue-as-new-parent-terminate-test-taskqueue"
	identity := "worker1"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	definition := newParentWithChildContinueAsNew(
		s,
		parentID,
		wtParent,
		childID,
		wtChild,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        definition.parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: definition.workflow,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to start child execution
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(definition.childExecutionStarted)

	// Process ChildExecution Started event and all generations of child executions
	for i := 0; i < 11; i++ {
		s.Logger.Info("workflow task", tag.Counter(i))
		_, err = poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	s.False(definition.childComplete)
	s.NotNil(definition.startedEvent)

	// Terminate parent workflow execution which should also trigger terminate of child due to parent close policy
	_, err = s.engine.TerminateWorkflowExecution(
		NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentID,
			},
		},
	)
	s.NoError(err)

	parentDescribeResp, err := s.engine.DescribeWorkflowExecution(
		NewContext(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: parentID,
			},
		},
	)
	s.NoError(err)
	s.NotNil(parentDescribeResp.WorkflowExecutionInfo.CloseTime)

	s.Logger.Info(fmt.Sprintf("Parent Status: %v", parentDescribeResp.WorkflowExecutionInfo.Status))

	var childDescribeResp *workflowservice.DescribeWorkflowExecutionResponse
	// Retry 10 times to wait for child to be terminated due to transfer task processing to enforce parent close policy
	for i := 0; i < 10; i++ {
		childDescribeResp, err = s.engine.DescribeWorkflowExecution(
			NewContext(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: childID,
				},
			},
		)
		s.NoError(err)

		// Check if child is terminated
		if childDescribeResp.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			break
		}

		// Wait for child to be terminated by back ground transfer task processing
		time.Sleep(time.Second)
	}
	s.Equal(
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		childDescribeResp.WorkflowExecutionInfo.Status,
		"expected child to be terminated",
	)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 StartChildWorkflowExecutionInitiated
  6 ChildWorkflowExecutionStarted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionTerminated`, s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: parentID,
		RunId:      we.RunId,
	}))

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowExecutionTerminated`, s.getHistory(s.namespace, &commonpb.WorkflowExecution{
		WorkflowId: childID,
	}))
}
