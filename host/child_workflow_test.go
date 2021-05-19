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
	"sort"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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
			"CustomKeywordField": attrValPayload,
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
	s.Equal(searchAttr, childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes())
	s.Equal(time.Duration(0), timestamp.DurationValue(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowExecutionTimeout()))
	s.Equal(200*time.Second, timestamp.DurationValue(childStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout()))

	// Process ChildExecution completed event and complete parent execution
	_, err = pollerParent.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.namespace, completedAttributes.Namespace)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	var r string
	err = payloads.Decode(completedAttributes.GetResult(), &r)
	s.NoError(err)
	s.Equal("Child Done", r)
}

func (s *integrationSuite) TestCronChildWorkflowExecution() {
	s.T().Skip(`
	    integration_test.go:2046:
	        	Error Trace:	integration_test.go:2046
	        	Error:      	Expected value not to be nil.
	        	Test:       	TestIntegrationSuite/TestCronChildWorkflowExecution
	`)

	parentID := "integration-cron-child-workflow-test-parent"
	childID := "integration-cron-child-workflow-test-child"
	wtParent := "integration-cron-child-workflow-test-parent-type"
	wtChild := "integration-cron-child-workflow-test-child-type"
	tlParent := "integration-cron-child-workflow-test-parent-taskqueue"
	tlChild := "integration-cron-child-workflow-test-child-taskqueue"
	identity := "worker1"

	cronSchedule := "@every 3s"
	targetBackoffDuration := time.Second * 3
	backoffDurationTolerance := time.Second

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

	startParentWorkflowTS := time.Now().UTC()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	childExecutionStarted := false
	var terminatedEvent *historypb.HistoryEvent
	var startChildWorkflowTS time.Time
	// Parent workflow logic
	wtHandlerParent := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		s.Logger.Info("Processing workflow task for ", tag.WorkflowID(execution.WorkflowId))

		if !childExecutionStarted {
			s.Logger.Info("Starting child execution")
			childExecutionStarted = true
			startChildWorkflowTS = time.Now().UTC()
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
			if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED {
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

		s.Logger.Info("Processing workflow task for Child ", tag.WorkflowID(execution.WorkflowId))
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

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startChildWorkflowTS
	for i := 0; i < 2; i++ {
		// Sleep some time before checking the open executions.
		// This will not cost extra time as the polling for first workflow task will be blocked for 3 seconds.
		time.Sleep(2 * time.Second)
		startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
		resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: childID,
			}},
		})
		s.NoError(err)
		s.Equal(1, len(resp.GetExecutions()))

		_, err = pollerChild.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)

		backoffDuration := time.Now().UTC().Sub(startChildWorkflowTS)
		s.True(backoffDuration < targetBackoffDuration+backoffDurationTolerance)
		startChildWorkflowTS = time.Now().UTC()
	}

	// terminate the childworkflow
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
	s.Empty(terminatedAttributes.Namespace)
	s.Equal(childID, terminatedAttributes.WorkflowExecution.WorkflowId)
	s.Equal(wtChild, terminatedAttributes.WorkflowType.Name)

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
		if len(resp.GetExecutions()) == 4 {
			closedExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.NotNil(closedExecutions)
	sort.Slice(closedExecutions, func(i, j int) bool {
		return closedExecutions[i].GetStartTime().Before(timestamp.TimeValue(closedExecutions[j].GetStartTime()))
	})
	// The first parent is not the cron workflow, only verify child workflow with cron schedule
	lastExecution := closedExecutions[1]
	for i := 2; i != 4; i++ {
		executionInfo := closedExecutions[i]
		// Round up the time precision to seconds
		expectedBackoff := executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(lastExecution.GetExecutionTime()))
		// The execution time calculate based on last execution close time
		// However, the current execution time is based on the current start time
		// This code is to remove the diff between current start time and last execution close time
		// TODO: Remove this line once we unify the time source.
		executionTimeDiff := executionInfo.GetStartTime().Sub(timestamp.TimeValue(lastExecution.GetCloseTime()))
		// The backoff between any two executions should be multiplier of the target backoff duration which is 3 in this test
		s.Equal(0, int(expectedBackoff.Seconds()-executionTimeDiff.Seconds())%int(targetBackoffDuration.Seconds()))
		lastExecution = executionInfo
	}
}
