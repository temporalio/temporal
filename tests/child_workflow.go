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
	"fmt"
	"sort"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
)

func (s *FunctionalSuite) TestChildWorkflowExecution() {
	parentID := "functional-child-workflow-test-parent"
	childID := "functional-child-workflow-test-child"
	grandchildID := "functional-child-workflow-test-grandchild"
	wtParent := "functional-child-workflow-test-parent-type"
	wtChild := "functional-child-workflow-test-child-type"
	wtGrandchild := "functional-child-workflow-test-grandchild-type"
	tlParent := "functional-child-workflow-test-parent-taskqueue"
	tlChild := "functional-child-workflow-test-child-taskqueue"
	tlGrandchild := "functional-child-workflow-test-grandchild-taskqueue"
	identity := "worker1"
	saName := "CustomKeywordField"
	// Uncomment this line to test with mapper.
	// saName = "AliasForCustomKeywordField"

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	grandchildWorkflowType := &commonpb.WorkflowType{Name: wtGrandchild}

	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	taskQueueGrandchild := &taskqueuepb.TaskQueue{Name: tlGrandchild, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample payload")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childComplete := false
	childExecutionStarted := false
	grandchildComplete := false
	grandchildExecutionStarted := false
	var parentStartedEvent *historypb.HistoryEvent
	var childStartedEventFromParent *historypb.HistoryEvent
	var childCompletedEventFromParent *historypb.HistoryEvent

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString("memo"),
		},
	}
	attrValPayload := payload.EncodeString("attrVal")
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			saName: attrValPayload,
		},
	}

	// Parent workflow logic
	wtHandlerParent := func(
		task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for Parent", tag.WorkflowID(task.WorkflowExecution.WorkflowId))
		parentStartedEvent = task.History.Events[0]

		if task.WorkflowExecution.WorkflowId == parentID {
			if !childExecutionStarted {
				s.Logger.Info("Starting child execution")
				childExecutionStarted = true

				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
						StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
							WorkflowId:          childID,
							WorkflowType:        childWorkflowType,
							TaskQueue:           taskQueueChild,
							Input:               payloads.EncodeString("child-workflow-input"),
							Header:              header,
							WorkflowRunTimeout:  durationpb.New(200 * time.Second),
							WorkflowTaskTimeout: durationpb.New(2 * time.Second),
							Control:             "",
							Memo:                memo,
							SearchAttributes:    searchAttr,
						},
					},
				}}, nil
			} else if task.PreviousStartedEventId > 0 {
				for _, event := range task.History.Events[task.PreviousStartedEventId:] {
					if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
						childStartedEventFromParent = event
						return []*commandpb.Command{}, nil
					}

					if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
						childCompletedEventFromParent = event
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

	var childStartedEvent *historypb.HistoryEvent
	var childRunID string
	// Child workflow logic
	wtHandlerChild := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if task.PreviousStartedEventId <= 0 {
			childStartedEvent = task.History.Events[0]
			childRunID = task.WorkflowExecution.GetRunId()
		}

		s.Logger.Info("Processing workflow task for Child", tag.WorkflowID(task.WorkflowExecution.WorkflowId))
		if !grandchildExecutionStarted {
			s.Logger.Info("Starting grandchild execution")
			grandchildExecutionStarted = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          grandchildID,
						WorkflowType:        grandchildWorkflowType,
						TaskQueue:           taskQueueGrandchild,
						Input:               payloads.EncodeString("grandchild-workflow-input"),
						Header:              header,
						WorkflowRunTimeout:  durationpb.New(200 * time.Second),
						WorkflowTaskTimeout: durationpb.New(2 * time.Second),
						Control:             "",
						Memo:                memo,
						SearchAttributes:    searchAttr,
					},
				},
			}}, nil
		}
		if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					return []*commandpb.Command{}, nil
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
					childComplete = true
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("Child Done"),
							},
						},
					}}, nil
				}
			}
		}

		return nil, nil
	}

	var grandchildStartedEvent *historypb.HistoryEvent
	// Grandchild workflow logic to check root workflow execution is carried correctly
	wtHandlerGrandchild := func(
		task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if task.PreviousStartedEventId <= 0 {
			grandchildStartedEvent = task.History.Events[0]
		}

		s.Logger.Info("Processing workflow task for Grandchild", tag.WorkflowID(task.WorkflowExecution.WorkflowId))
		grandchildComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Grandchild Done"),
				},
			},
		}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueParent,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerParent,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueChild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerChild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerGrandchild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueGrandchild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerGrandchild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first workflow task to start child execution
	_, err := pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)
	s.NotNil(parentStartedEvent)
	parentStartedEventAttrs := parentStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	// top-level workflow doesn't have parent, and root is itself (nil in history event)
	s.Nil(parentStartedEventAttrs.GetParentWorkflowExecution())
	s.Nil(parentStartedEventAttrs.GetRootWorkflowExecution())

	// Process ChildExecution Started event and Process Child Execution and complete it
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process Child workflow to start grandchild execution
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(childStartedEventFromParent)
	s.NotNil(childStartedEvent)
	childStartedEventAttrsFromParent := childStartedEventFromParent.GetChildWorkflowExecutionStartedEventAttributes()
	childStartedEventAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	childStartedEventSearchAttrs := childStartedEventAttrs.GetSearchAttributes()
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
	// check parent of child workflow is the top-level workflow
	s.Equal(s.namespace, childStartedEventAttrs.GetParentWorkflowNamespace())
	s.Equal(parentID, childStartedEventAttrs.ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.ParentWorkflowExecution.GetRunId())
	s.Equal(
		childStartedEventAttrsFromParent.GetInitiatedEventId(),
		childStartedEventAttrs.GetParentInitiatedEventId(),
	)
	// check root of child workflow is the top-level workflow
	s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
	s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
	s.ProtoEqual(header, childStartedEventAttrsFromParent.Header)
	s.ProtoEqual(header, childStartedEventAttrs.Header)
	s.ProtoEqual(memo, childStartedEventAttrs.GetMemo())
	s.Equal(
		searchAttr.GetIndexedFields()[saName].GetData(),
		childStartedEventSearchAttrs.GetIndexedFields()[saName].GetData(),
	)
	s.Equal(
		"Keyword",
		string(childStartedEventSearchAttrs.GetIndexedFields()[saName].GetMetadata()["type"]),
	)
	s.Equal(time.Duration(0), childStartedEventAttrs.GetWorkflowExecutionTimeout().AsDuration())
	s.Equal(200*time.Second, childStartedEventAttrs.GetWorkflowRunTimeout().AsDuration())

	// Process GrandchildExecution Started event and Process Grandchild Execution and complete it
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process Grandchild workflow
	_, err = pollerGrandchild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(grandchildComplete)
	s.NotNil(grandchildStartedEvent)
	grandchildStartedEventAttrs := grandchildStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, grandchildStartedEvent.GetEventType())
	// check parent of grandchild workflow is the child workflow
	s.NotNil(grandchildStartedEventAttrs.GetParentWorkflowExecution())
	s.Equal(childID, grandchildStartedEventAttrs.ParentWorkflowExecution.GetWorkflowId())
	s.Equal(childRunID, grandchildStartedEventAttrs.ParentWorkflowExecution.GetRunId())
	// check root of grandchild workflow is the top-level workflow
	s.NotNil(grandchildStartedEventAttrs.GetRootWorkflowExecution())
	s.Equal(parentID, grandchildStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), grandchildStartedEventAttrs.RootWorkflowExecution.GetRunId())

	// Process GrandchildExecution completed event and complete child execution
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childComplete)

	// Process ChildExecution completed event and complete parent execution
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(childCompletedEventFromParent)
	completedAttributes := childCompletedEventFromParent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.namespace, completedAttributes.Namespace)
	// TODO: change to s.Equal(s.namespaceID) once it is available.
	s.NotEmpty(completedAttributes.NamespaceId)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	s.Equal("Child Done", s.decodePayloadsString(completedAttributes.GetResult()))
}

func (s *FunctionalSuite) TestCronChildWorkflowExecution() {
	parentID := "functional-cron-child-workflow-test-parent"
	childID := "functional-cron-child-workflow-test-child"
	wtParent := "functional-cron-child-workflow-test-parent-type"
	wtChild := "functional-cron-child-workflow-test-child-type"
	tlParent := "functional-cron-child-workflow-test-parent-taskqueue"
	tlChild := "functional-cron-child-workflow-test-child-taskqueue"
	identity := "worker1"

	cronSchedule := "@every 3s"
	targetBackoffDuration := time.Second * 3

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}

	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	// Because of rounding in GetBackoffForNextSchedule, we'll tend to stay aligned to whatever
	// phase we start in relative to second boundaries, but drift slightly later within the second
	// over time. If we cross a second boundary, one of our intervals will end up being 2s instead
	// of 3s. To avoid this, wait until we can start early in the second.
	for time.Now().Nanosecond()/int(time.Millisecond) > 150 {
		time.Sleep(50 * time.Millisecond)
	}

	startParentWorkflowTS := time.Now().UTC()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childExecutionStarted := false
	seenChildStarted := false
	var terminatedEvent *historypb.HistoryEvent
	// Parent workflow logic
	wtHandlerParent := func(
		task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for", tag.WorkflowID(task.WorkflowExecution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          childID,
						WorkflowType:        childWorkflowType,
						TaskQueue:           taskQueueChild,
						Input:               nil,
						WorkflowRunTimeout:  durationpb.New(200 * time.Second),
						WorkflowTaskTimeout: durationpb.New(2 * time.Second),
						Control:             "",
						CronSchedule:        cronSchedule,
					},
				},
			}}, nil
		}
		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
				seenChildStarted = true
			} else if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED {
				terminatedEvent = event
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
		return nil, nil
	}

	var childStartedEvent *historypb.HistoryEvent
	// Child workflow logic
	wtHandlerChild := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for Child", tag.WorkflowID(task.WorkflowExecution.WorkflowId))
		childStartedEvent = task.History.Events[0]
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueParent,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerParent,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueChild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerChild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first workflow task to start child execution
	_, err := pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(seenChildStarted)

	// Run through three executions of the child workflow
	for i := 0; i < 3; i++ {
		_, err = pollerChild.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err), tag.Counter(i))
		s.NoError(err)
		s.NotNil(childStartedEvent)
		childStartedEventAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
		s.Equal(s.namespace, childStartedEventAttrs.GetParentWorkflowNamespace())
		s.Equal(parentID, childStartedEventAttrs.ParentWorkflowExecution.GetWorkflowId())
		s.Equal(we.GetRunId(), childStartedEventAttrs.ParentWorkflowExecution.GetRunId())
		s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
		s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
		s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
		// clean up to make sure the next poll will update this var and assert correctly
		childStartedEvent = nil
	}

	// terminate the child workflow
	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childID,
		},
	})
	s.Nil(terminateErr)

	// Process ChildExecution terminated event and complete parent execution
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(terminatedEvent)
	terminatedAttributes := terminatedEvent.GetChildWorkflowExecutionTerminatedEventAttributes()
	s.Equal(childID, terminatedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, terminatedAttributes.WorkflowType.Name)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = timestamppb.New(startParentWorkflowTS)
	startFilter.LatestTime = timestamppb.New(time.Now().UTC())
	var closedExecutions []*workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == 5 {
			closedExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.NotNil(closedExecutions)
	sort.Slice(closedExecutions, func(i, j int) bool {
		return closedExecutions[i].GetStartTime().AsTime().Before(closedExecutions[j].GetStartTime().AsTime())
	})
	// Execution 0 is the parent, 1, 2, 3 are the child (cron) that completed, 4 is the child that was
	// terminated. Even though it was terminated, ExecutionTime will be set correctly (in the future).
	lastExecution := closedExecutions[1]
	for i := 2; i < 5; i++ {
		executionInfo := closedExecutions[i]
		// Round up the time precision to seconds
		expectedBackoff := executionInfo.GetExecutionTime().AsTime().Sub(lastExecution.GetExecutionTime().AsTime())
		// The execution time calculated based on last execution close time.
		// However, the current execution time is based on the current start time.
		// This code is to remove the diff between current start time and last execution close time.
		// TODO: Remove this line once we unify the time source.
		executionTimeDiff := executionInfo.GetStartTime().AsTime().Sub(lastExecution.GetCloseTime().AsTime())
		// The backoff between any two executions should be a multiplier of the target backoff duration which is 3 in this test
		s.Equal(0, int(expectedBackoff.Seconds()-executionTimeDiff.Seconds())%int(targetBackoffDuration.Seconds()))
		lastExecution = executionInfo
	}
}

func (s *FunctionalSuite) TestRetryChildWorkflowExecution() {
	parentID := "functional-retry-child-workflow-test-parent"
	childID := "functional-retry-child-workflow-test-child"
	wtParent := "functional-retry-child-workflow-test-parent-type"
	wtChild := "functional-retry-child-workflow-test-child-type"
	tlParent := "functional-retry-child-workflow-test-parent-taskqueue"
	tlChild := "functional-retry-child-workflow-test-child-taskqueue"
	identity := "worker1"

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childComplete := false
	childExecutionStarted := false
	var startedEvent *historypb.HistoryEvent
	var completedEvent *historypb.HistoryEvent

	// Parent workflow logic
	wtHandlerParent := func(
		task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for Parent", tag.WorkflowID(task.WorkflowExecution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          childID,
						WorkflowType:        childWorkflowType,
						TaskQueue:           taskQueueChild,
						Input:               payloads.EncodeString("child-workflow-input"),
						WorkflowRunTimeout:  durationpb.New(200 * time.Second),
						WorkflowTaskTimeout: durationpb.New(2 * time.Second),
						Control:             "",
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval:    durationpb.New(1 * time.Millisecond),
							BackoffCoefficient: 2.0,
						},
					},
				},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
					return []*commandpb.Command{}, nil
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
					completedEvent = event
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

		return nil, nil
	}

	var childStartedEvent *historypb.HistoryEvent
	// Child workflow logic
	wtHandlerChild := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info(
			"Processing workflow task for Child",
			tag.WorkflowID(task.WorkflowExecution.WorkflowId),
			tag.WorkflowRunID(task.WorkflowExecution.RunId),
		)

		childStartedEvent = task.History.Events[0]
		attempt := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes().Attempt
		// Fail twice, succeed on third attempt
		if attempt < 3 {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
					FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
						Failure: &failurepb.Failure{
							Message: fmt.Sprintf("Failed attempt %d", attempt),
						},
					}},
			}}, nil
		} else {
			childComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("Child Done"),
					},
				},
			}}, nil
		}
	}

	pollerParent := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueParent,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerParent,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueChild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerChild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first workflow task to start child execution
	_, err := pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)

	// Process Child Execution #1
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.False(childComplete)
	s.NotNil(childStartedEvent)
	childStartedEventAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
	s.Equal(s.namespace, childStartedEventAttrs.GetParentWorkflowNamespace())
	s.Equal(parentID, childStartedEventAttrs.ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.ParentWorkflowExecution.GetRunId())
	s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
	s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
	// clean up to make sure the next poll will update this var and assert correctly
	childStartedEvent = nil

	// Process Child Execution #2
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.False(childComplete)
	s.NotNil(childStartedEvent)
	childStartedEventAttrs = childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
	s.Equal(s.namespace, childStartedEventAttrs.GetParentWorkflowNamespace())
	s.Equal(parentID, childStartedEventAttrs.ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.ParentWorkflowExecution.GetRunId())
	s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
	s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
	// clean up to make sure the next poll will update this var and assert correctly
	childStartedEvent = nil

	// Process Child Execution #3
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childComplete)
	s.NotNil(childStartedEvent)
	childStartedEventAttrs = childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
	s.Equal(s.namespace, childStartedEventAttrs.GetParentWorkflowNamespace())
	s.Equal(parentID, childStartedEventAttrs.ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.ParentWorkflowExecution.GetRunId())
	s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
	s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
	// clean up to make sure the next poll will update this var and assert correctly
	childStartedEvent = nil

	// Parent should see child complete
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Child result should be present in completion event
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal("Child Done", s.decodePayloadsString(completedAttributes.GetResult()))
}

func (s *FunctionalSuite) TestRetryFailChildWorkflowExecution() {
	parentID := "functional-retry-fail-child-workflow-test-parent"
	childID := "functional-retry-fail-child-workflow-test-child"
	wtParent := "functional-retry-fail-child-workflow-test-parent-type"
	wtChild := "functional-retry-fail-child-workflow-test-child-type"
	tlParent := "functional-retry-fail-child-workflow-test-parent-taskqueue"
	tlChild := "functional-retry-fail-child-workflow-test-child-taskqueue"
	identity := "worker1"

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childExecutionStarted := false
	var startedEvent *historypb.HistoryEvent
	var completedEvent *historypb.HistoryEvent

	// Parent workflow logic
	wtHandlerParent := func(
		task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for Parent", tag.WorkflowID(task.WorkflowExecution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          childID,
						WorkflowType:        childWorkflowType,
						TaskQueue:           taskQueueChild,
						Input:               payloads.EncodeString("child-workflow-input"),
						WorkflowRunTimeout:  durationpb.New(200 * time.Second),
						WorkflowTaskTimeout: durationpb.New(2 * time.Second),
						Control:             "",
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval:    durationpb.New(1 * time.Millisecond),
							BackoffCoefficient: 2.0,
							MaximumAttempts:    3,
						},
					},
				},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
					return []*commandpb.Command{}, nil
				}
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED {
					completedEvent = event
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

		return nil, nil
	}

	// Child workflow logic
	wtHandlerChild := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		s.Logger.Info(
			"Processing workflow task for Child",
			tag.WorkflowID(task.WorkflowExecution.WorkflowId),
			tag.WorkflowRunID(task.WorkflowExecution.RunId),
		)

		attempt := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes().Attempt
		// We shouldn't see more than 3 attempts
		s.LessOrEqual(int(attempt), 3)

		// Always fail
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
				FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: &failurepb.Failure{
						Message: fmt.Sprintf("Failed attempt %d", attempt),
					},
				}},
		}}, nil
	}

	pollerParent := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueParent,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerParent,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	pollerChild := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueueChild,
		Identity:            identity,
		WorkflowTaskHandler: wtHandlerChild,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first workflow task to start child execution
	_, err := pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)

	// Process Child Execution #1
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process Child Execution #2
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process Child Execution #3
	_, err = pollerChild.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Parent should see child complete
	_, err = pollerParent.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Child failure should be present in completion event
	s.NotNil(completedEvent)
	attrs := completedEvent.GetChildWorkflowExecutionFailedEventAttributes()
	s.Equal(attrs.Failure.Message, "Failed attempt 3")
}
