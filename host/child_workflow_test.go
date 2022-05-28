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

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestChildWorkflowExecution() {
	parentID := "integration-child-workflow-test-parent"
	childID := "integration-child-workflow-test-child"
	wtParent := "integration-child-workflow-test-parent-type"
	wtChild := "integration-child-workflow-test-child-type"
	tlParent := "integration-child-workflow-test-parent-taskqueue"
	tlChild := "integration-child-workflow-test-child-taskqueue"
	identity := "worker1"
	saName := "CustomKeywordField"
	// Uncomment this line to test with mapper.
	// saName = "AliasForCustomKeywordField"

	parentWorkflowType := &commonpb.WorkflowType{}
	parentWorkflowType.Name = wtParent

	childWorkflowType := &commonpb.WorkflowType{}
	childWorkflowType.Name = wtChild

	taskQueueParent := &taskqueuepb.TaskQueue{}
	taskQueueParent.Name = tlParent
	taskQueueChild := &taskqueuepb.TaskQueue{}
	taskQueueChild.Name = tlChild

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
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for ", tag.WorkflowID(execution.WorkflowId))

		if execution.WorkflowId == parentID {
			if !childExecutionStarted {
				s.Logger.Info("Starting child execution")
				childExecutionStarted = true

				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId:          childID,
						WorkflowType:        childWorkflowType,
						TaskQueue:           taskQueueChild,
						Input:               payloads.EncodeString("child-workflow-input"),
						Header:              header,
						WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
						WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
						Control:             "",
						Memo:                memo,
						SearchAttributes:    searchAttr,
					}},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
						startedEvent = event
						return []*commandpb.Command{}, nil
					}

					if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
						completedEvent = event
						return []*commandpb.Command{{
							CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
							Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("Done"),
							}},
						}}, nil
					}
				}
			}
		}

		return nil, nil
	}

	var childStartedEvent *historypb.HistoryEvent
	// Child workflow logic
	wtHandlerChild := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if previousStartedEventID <= 0 {
			childStartedEvent = history.Events[0]
		}

		s.Logger.Info("Processing workflow task for Child ", tag.WorkflowID(execution.WorkflowId))
		childComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Child Done"),
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
	_, err := pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event and Process Child Execution and complete it
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)
	s.True(childComplete)
	s.NotNil(childStartedEvent)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, childStartedEvent.GetEventType())
	s.Equal(s.namespace, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowNamespace())
	s.Equal(parentID, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.GetWorkflowId())
	s.Equal(we.GetRunId(), childStartedEvent.GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.GetRunId())
	s.Equal(startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
		childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetParentInitiatedEventId())
	s.Equal(header, startedEvent.GetChildWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(header, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(memo, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetMemo())
	s.Equal(searchAttr.GetIndexedFields()[saName].GetData(), childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()[saName].GetData())
	s.Equal("Keyword", string(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()[saName].GetMetadata()["type"]))
	s.Equal(time.Duration(0), timestamp.DurationValue(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowExecutionTimeout()))
	s.Equal(200*time.Second, timestamp.DurationValue(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout()))

	// Process ChildExecution completed event and complete parent execution
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.namespace, completedAttributes.Namespace)
	// TODO: change to s.Equal(s.namespaceID) once it is available.
	s.NotEmpty(completedAttributes.NamespaceId)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	s.Equal("Child Done", s.decodePayloadsString(completedAttributes.GetResult()))
}

func (s *integrationSuite) TestCronChildWorkflowExecution() {
	parentID := "integration-cron-child-workflow-test-parent"
	childID := "integration-cron-child-workflow-test-child"
	wtParent := "integration-cron-child-workflow-test-parent-type"
	wtChild := "integration-cron-child-workflow-test-child-type"
	tlParent := "integration-cron-child-workflow-test-parent-taskqueue"
	tlChild := "integration-cron-child-workflow-test-child-taskqueue"
	identity := "worker1"

	cronSchedule := "@every 3s"
	targetBackoffDuration := time.Second * 3

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}

	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for", tag.WorkflowID(execution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId:          childID,
					WorkflowType:        childWorkflowType,
					TaskQueue:           taskQueueChild,
					Input:               nil,
					WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
					Control:             "",
					CronSchedule:        cronSchedule,
				}},
			}}, nil
		}
		for _, event := range history.Events[previousStartedEventID:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
				seenChildStarted = true
			} else if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED {
				terminatedEvent = event
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("Done"),
					}},
				}}, nil
			}
		}
		return nil, nil
	}

	// Child workflow logic
	wtHandlerChild := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		s.Logger.Info("Processing workflow task for Child", tag.WorkflowID(execution.WorkflowId))
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}}, nil
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
	_, err := pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(seenChildStarted)

	// Run through three executions of the child workflow
	for i := 0; i < 3; i++ {
		_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err), tag.Counter(i))
		s.NoError(err)
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
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(terminatedEvent)
	terminatedAttributes := terminatedEvent.GetChildWorkflowExecutionTerminatedEventAttributes()
	s.Equal(childID, terminatedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, terminatedAttributes.WorkflowType.Name)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startParentWorkflowTS
	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
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
		return closedExecutions[i].GetStartTime().Before(timestamp.TimeValue(closedExecutions[j].GetStartTime()))
	})
	// Execution 0 is the parent, 1, 2, 3 are the child (cron) that completed, 4 is the child that was
	// terminated. Even though it was terminated, ExecutionTime will be set correctly (in the future).
	lastExecution := closedExecutions[1]
	for i := 2; i < 5; i++ {
		executionInfo := closedExecutions[i]
		// Round up the time precision to seconds
		expectedBackoff := executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(lastExecution.GetExecutionTime()))
		// The execution time calculated based on last execution close time.
		// However, the current execution time is based on the current start time.
		// This code is to remove the diff between current start time and last execution close time.
		// TODO: Remove this line once we unify the time source.
		executionTimeDiff := executionInfo.GetStartTime().Sub(timestamp.TimeValue(lastExecution.GetCloseTime()))
		// The backoff between any two executions should be a multiplier of the target backoff duration which is 3 in this test
		s.Equal(0, int(expectedBackoff.Seconds()-executionTimeDiff.Seconds())%int(targetBackoffDuration.Seconds()))
		lastExecution = executionInfo
	}
}

func (s *integrationSuite) TestRetryChildWorkflowExecution() {
	parentID := "integration-retry-child-workflow-test-parent"
	childID := "integration-retry-child-workflow-test-child"
	wtParent := "integration-retry-child-workflow-test-parent-type"
	wtChild := "integration-retry-child-workflow-test-child-type"
	tlParent := "integration-retry-child-workflow-test-parent-taskqueue"
	tlChild := "integration-retry-child-workflow-test-child-taskqueue"
	identity := "worker1"

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for ", tag.WorkflowID(execution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId:          childID,
					WorkflowType:        childWorkflowType,
					TaskQueue:           taskQueueChild,
					Input:               payloads.EncodeString("child-workflow-input"),
					WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
					Control:             "",
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval:    timestamp.DurationPtr(1 * time.Millisecond),
						BackoffCoefficient: 2.0,
					},
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
					return []*commandpb.Command{}, nil
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
					completedEvent = event
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("Done"),
						}},
					}}, nil
				}
			}
		}

		return nil, nil
	}

	// Child workflow logic
	wtHandlerChild := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		s.Logger.Info("Processing workflow task for Child ", tag.WorkflowID(execution.WorkflowId), tag.WorkflowRunID(execution.RunId))

		attempt := history.Events[0].GetWorkflowExecutionStartedEventAttributes().Attempt
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
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Child Done"),
				}},
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
	_, err := pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)

	// Process Child Execution #1
	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.False(childComplete)

	// Process Child Execution #2
	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.False(childComplete)

	// Process Child Execution #3
	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childComplete)

	// Parent should see child complete
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Child result should be present in completion event
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal("Child Done", s.decodePayloadsString(completedAttributes.GetResult()))
}

func (s *integrationSuite) TestRetryFailChildWorkflowExecution() {
	parentID := "integration-retry-fail-child-workflow-test-parent"
	childID := "integration-retry-fail-child-workflow-test-child"
	wtParent := "integration-retry-fail-child-workflow-test-parent-type"
	wtChild := "integration-retry-fail-child-workflow-test-child-type"
	tlParent := "integration-retry-fail-child-workflow-test-parent-taskqueue"
	tlChild := "integration-retry-fail-child-workflow-test-child-taskqueue"
	identity := "worker1"

	parentWorkflowType := &commonpb.WorkflowType{Name: wtParent}
	childWorkflowType := &commonpb.WorkflowType{Name: wtChild}
	taskQueueParent := &taskqueuepb.TaskQueue{Name: tlParent}
	taskQueueChild := &taskqueuepb.TaskQueue{Name: tlChild}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          parentID,
		WorkflowType:        parentWorkflowType,
		TaskQueue:           taskQueueParent,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
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
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for ", tag.WorkflowID(execution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId:          childID,
					WorkflowType:        childWorkflowType,
					TaskQueue:           taskQueueChild,
					Input:               payloads.EncodeString("child-workflow-input"),
					WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(2 * time.Second),
					Control:             "",
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval:    timestamp.DurationPtr(1 * time.Millisecond),
						BackoffCoefficient: 2.0,
						MaximumAttempts:    3,
					},
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
					return []*commandpb.Command{}, nil
				}
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED {
					completedEvent = event
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("Done"),
						}},
					}}, nil
				}
			}
		}

		return nil, nil
	}

	// Child workflow logic
	wtHandlerChild := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		s.Logger.Info("Processing workflow task for Child ", tag.WorkflowID(execution.WorkflowId), tag.WorkflowRunID(execution.RunId))

		attempt := history.Events[0].GetWorkflowExecutionStartedEventAttributes().Attempt
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
	_, err := pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(startedEvent)

	// Process Child Execution #1
	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process Child Execution #2
	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process Child Execution #3
	_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Parent should see child complete
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Child failure should be present in completion event
	s.NotNil(completedEvent)
	attrs := completedEvent.GetChildWorkflowExecutionFailedEventAttributes()
	s.Equal(attrs.Failure.Message, "Failed attempt 3")
}
