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
	"context"
	"errors"
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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"

	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestCronWorkflow_Failed_Infinite() {
	id := "integration-wf-cron-failed-infinite-test"
	wt := "integration-wf-cron-failed-infinite-type"
	tl := "integration-wf-cron-failed-infinite-taskqueue"
	identity := "worker1"
	cronSchedule := "@every 5s"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(5 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		CronSchedule:        cronSchedule, // minimum interval by standard spec is 1m (* * * * *, use non-standard descriptor for short interval for test
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumAttempts:    2,
			MaximumInterval:    timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient: 1.2,
		},
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	respondFailed := false
	seeRetry := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !respondFailed {
			respondFailed = true

			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
						FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
							Failure: failure.NewServerFailure("cron error for retry", false),
						}},
				}}, nil
		}

		startEvent := history.Events[0]
		seeRetry = startEvent.GetWorkflowExecutionStartedEventAttributes().Initiator == enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: nil,
					}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	s.Logger.Info("Process first cron run which fails")
	_, err := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	s.Logger.Info("Process first cron run which completes")
	_, err = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(err)

	s.True(seeRetry)
}

func (s *integrationSuite) TestCronWorkflow() {
	id := "integration-wf-cron-test"
	wt := "integration-wf-cron-type"
	tl := "integration-wf-cron-taskqueue"
	identity := "worker1"
	cronSchedule := "@every 3s"

	targetBackoffDuration := time.Second * 3
	backoffDurationTolerance := time.Millisecond * 500

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{"memoKey": payload.EncodeString("memoVal")},
	}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": payload.EncodeString(`"1"`),
		},
	}

	// can't do simply s.Equal because "type" is added
	checkSearchAttrs := func(sa *commonpb.SearchAttributes) {
		field := sa.IndexedFields["CustomKeywordField"]
		s.Equal(searchAttr.IndexedFields["CustomKeywordField"].Data, field.Data)
		s.Equal([]byte("Keyword"), field.Metadata["type"])
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		CronSchedule:        cronSchedule, // minimum interval by standard spec is 1m (* * * * *, use non-standard descriptor for short interval for test
		Memo:                memo,
		SearchAttributes:    searchAttr,
	}

	// Because of rounding in GetBackoffForNextSchedule, we'll tend to stay aligned to whatever
	// phase we start in relative to second boundaries, but drift slightly later within the second
	// over time. If we cross a second boundary, one of our intervals will end up being 2s instead
	// of 3s. To avoid this, wait until we can start early in the second.
	for time.Now().Nanosecond()/int(time.Millisecond) > 150 {
		time.Sleep(50 * time.Millisecond)
	}

	startWorkflowTS := time.Now().UTC()
	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if previousStartedEventID == common.EmptyEventID {
			startedEvent := history.Events[0]
			if startedEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
							Failure: failure.NewServerFailure("incorrect first event", true),
						}},
					}}, nil
			}

			// Just check that it can be decoded
			s.decodePayloadsInt(startedEvent.GetWorkflowExecutionStartedEventAttributes().GetLastCompletionResult())
		}

		executions = append(executions, execution)
		if len(executions) >= 3 {
			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("cron-test-result"),
					}},
				}}, nil
		}
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("cron-test-error", false),
				}},
			}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startWorkflowTS
	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())

	// Sleep some time before checking the open executions.
	// This will not cost extra time as the polling for first workflow task will be blocked for 3 seconds.
	time.Sleep(2 * time.Second)
	resp, err := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace:       s.namespace,
		MaximumPageSize: 100,
		StartTimeFilter: startFilter,
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	})
	s.NoError(err)
	s.Equal(1, len(resp.GetExecutions()))
	executionInfo := resp.GetExecutions()[0]
	s.Equal(targetBackoffDuration, executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(executionInfo.GetStartTime())))

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	// Make sure the cron workflow start running at a proper time, in this case 3 seconds after the
	// startWorkflowExecution request
	backoffDuration := time.Now().UTC().Sub(startWorkflowTS)
	s.True(backoffDuration > targetBackoffDuration)
	s.True(backoffDuration < targetBackoffDuration+backoffDurationTolerance)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	s.Equal(3, len(executions))

	_, terminateErr := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(terminateErr)

	// first two should be failures
	for i := 0; i < 2; i++ {
		events := s.getHistory(s.namespace, executions[i])

		startAttrs := events[0].GetWorkflowExecutionStartedEventAttributes()
		s.Equal(memo, startAttrs.Memo)
		checkSearchAttrs(startAttrs.SearchAttributes)

		lastEvent := events[len(events)-1]
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, lastEvent.GetEventType())
		failAttrs := lastEvent.GetWorkflowExecutionFailedEventAttributes()
		s.Equal("cron-test-error", failAttrs.GetFailure().GetMessage())
		s.Equal(executions[i+1].RunId, failAttrs.GetNewExecutionRunId())
	}

	// third should be completed
	events := s.getHistory(s.namespace, executions[2])

	startAttrs := events[0].GetWorkflowExecutionStartedEventAttributes()
	s.Equal(memo, startAttrs.Memo)
	checkSearchAttrs(startAttrs.SearchAttributes)

	lastEvent := events[len(events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())

	completedAttrs := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
	s.Equal("cron-test-result", s.decodePayloadsString(completedAttrs.Result))

	startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())
	var closedExecutions []*workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: startFilter,
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == 4 {
			closedExecutions = resp.GetExecutions()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.NotNil(closedExecutions)
	dweResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)
	expectedExecutionTime := dweResponse.WorkflowExecutionInfo.GetStartTime().Add(3 * time.Second)
	s.Equal(expectedExecutionTime, timestamp.TimeValue(dweResponse.WorkflowExecutionInfo.GetExecutionTime()))

	sort.Slice(closedExecutions, func(i, j int) bool {
		return closedExecutions[i].GetStartTime().Before(timestamp.TimeValue(closedExecutions[j].GetStartTime()))
	})
	lastExecution := closedExecutions[0]
	for i := 1; i < 4; i++ {
		executionInfo := closedExecutions[i]
		expectedBackoff := executionInfo.GetExecutionTime().Sub(timestamp.TimeValue(lastExecution.GetExecutionTime()))
		// The execution time calculated based on last execution close time.
		// However, the current execution time is based on the current start time.
		// This code is to remove the diff between current start time and last execution close time.
		// TODO: Remove this line once we unify the time source
		executionTimeDiff := executionInfo.GetStartTime().Sub(timestamp.TimeValue(lastExecution.GetCloseTime()))
		// The backoff between any two executions should be a multiplier of the target backoff duration which is 3 in this test
		s.Equal(
			0,
			int((expectedBackoff-executionTimeDiff).Round(time.Second).Seconds())%int(targetBackoffDuration.Seconds()),
			"expected backoff %v-%v=%v should be multiplier of target backoff %v",
			expectedBackoff.Seconds(),
			executionTimeDiff.Seconds(),
			(expectedBackoff - executionTimeDiff).Round(time.Second).Seconds(),
			targetBackoffDuration.Seconds())
		lastExecution = executionInfo
	}
}

func (s *clientIntegrationSuite) TestCronWorkflowCompletionStates() {
	// Run a cron workflow that completes in (almost) all the possible ways:
	// Run 1: succeeds
	// Run 2: fails
	// Run 3: times out
	// Run 4: succeeds
	// Run 5: succeeds
	// Run 6: terminated before it runs

	// Continue-as-new is not tested (behavior is currently not correct)

	id := "integration-wf-cron-failed-test"
	cronSchedule := "@every 3s"

	targetBackoffDuration := 3 * time.Second
	workflowRunTimeout := 5 * time.Second
	tolerance := 500 * time.Millisecond

	runIDs := make(map[string]bool)
	wfCh := make(chan int)

	workflowFn := func(ctx workflow.Context) (string, error) {
		runIDs[workflow.GetInfo(ctx).WorkflowExecution.RunID] = true
		iteration := len(runIDs)
		wfCh <- iteration

		var lcr string
		switch iteration {
		case 1:
			s.False(workflow.HasLastCompletionResult(ctx))
			s.Nil(workflow.GetLastError(ctx))
			return "pass", nil

		case 2:
			s.True(workflow.HasLastCompletionResult(ctx))
			s.NoError(workflow.GetLastCompletionResult(ctx, &lcr))
			s.Equal(lcr, "pass")
			s.Nil(workflow.GetLastError(ctx))
			return "", errors.New("second error")

		case 3:
			s.True(workflow.HasLastCompletionResult(ctx))
			s.NoError(workflow.GetLastCompletionResult(ctx, &lcr))
			s.Equal(lcr, "pass")
			s.NotNil(workflow.GetLastError(ctx))
			s.Equal(workflow.GetLastError(ctx).Error(), "second error")
			workflow.Sleep(ctx, 10*time.Second) // cause wft timeout
			panic("should have been timed out on server already")

		case 4:
			s.True(workflow.HasLastCompletionResult(ctx))
			s.NoError(workflow.GetLastCompletionResult(ctx, &lcr))
			s.Equal(lcr, "pass")
			s.NotNil(workflow.GetLastError(ctx))
			s.Equal(workflow.GetLastError(ctx).Error(), "workflow timeout (type: StartToClose)")
			return "pass again", nil

		case 5:
			s.True(workflow.HasLastCompletionResult(ctx))
			s.NoError(workflow.GetLastCompletionResult(ctx, &lcr))
			s.Equal(lcr, "pass again")
			s.Nil(workflow.GetLastError(ctx))
			return "final pass", nil
		}

		panic("shouldn't get here")
	}

	s.worker.RegisterWorkflow(workflowFn)

	// Because of rounding in GetBackoffForNextSchedule, we'll tend to stay aligned to whatever
	// phase we start in relative to second boundaries, but drift slightly later within the second
	// over time. If we cross a second boundary, one of our intervals will end up being 2s instead
	// of 3s. To avoid this, wait until we can start early in the second.
	for time.Now().Nanosecond()/int(time.Millisecond) > 150 {
		time.Sleep(50 * time.Millisecond)
	}

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: workflowRunTimeout,
		CronSchedule:       cronSchedule,
	}
	ts := time.Now()
	startTs := ts
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// check execution and history of first run
	exec := s.listOpenWorkflowExecutions(startTs, time.Now(), id, 1)[0]
	firstRunID := exec.GetExecution().RunId
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, exec.GetStatus())
	lastEvent := s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, lastEvent.GetEventType())
	attrs0 := lastEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(cronSchedule, attrs0.CronSchedule)
	s.DurationNear(timestamp.DurationValue(attrs0.FirstWorkflowTaskBackoff), targetBackoffDuration, tolerance)
	s.Equal(firstRunID, attrs0.FirstExecutionRunId)
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, attrs0.Initiator)
	s.Equal("", attrs0.ContinuedExecutionRunId)

	// wait for first run
	s.Equal(<-wfCh, 1)
	s.DurationNear(time.Since(ts), targetBackoffDuration, tolerance)
	ts = time.Now()

	// let first run finish, then check execution and history of second run
	time.Sleep(500 * time.Millisecond)
	exec = s.listOpenWorkflowExecutions(startTs, time.Now(), id, 1)[0]
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, exec.GetStatus())
	lastEvent = s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, lastEvent.GetEventType())
	attrs0 = lastEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(cronSchedule, attrs0.CronSchedule)
	s.DurationNear(timestamp.DurationValue(attrs0.FirstWorkflowTaskBackoff), targetBackoffDuration, tolerance)
	s.Equal(firstRunID, attrs0.FirstExecutionRunId)
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, attrs0.Initiator)
	s.Equal(firstRunID, attrs0.ContinuedExecutionRunId)

	// wait for second run
	s.Equal(<-wfCh, 2)
	s.DurationNear(time.Since(ts), targetBackoffDuration, tolerance)
	ts = time.Now()

	// don't bother checking started events for subsequent runs, we covered the important parts already

	// wait for third run
	s.Equal(<-wfCh, 3)
	s.DurationNear(time.Since(ts), targetBackoffDuration, tolerance)
	ts = time.Now()

	// wait for fourth run (third one waits for timeout after 5s, so will run after 6s)
	s.Equal(<-wfCh, 4)
	s.DurationNear(time.Since(ts), 2*targetBackoffDuration, tolerance)
	ts = time.Now()

	// wait for fifth run
	s.Equal(<-wfCh, 5)
	s.DurationNear(time.Since(ts), targetBackoffDuration, tolerance)

	// let fifth run finish and sixth get scheduled
	time.Sleep(500 * time.Millisecond)
	// then terminate
	s.NoError(s.sdkClient.TerminateWorkflow(ctx, id, "", "test is over"))

	closedExecutions := s.listClosedWorkflowExecutions(startTs, time.Now(), id, 6)

	exec = closedExecutions[5] // first: success
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, exec.GetStatus())
	lastEvent = s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())
	attrs1 := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
	s.Equal("pass", s.decodePayloadsString(attrs1.GetResult()))

	exec = closedExecutions[4] // second: fail
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, exec.GetStatus())
	lastEvent = s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, lastEvent.GetEventType())
	attrs2 := lastEvent.GetWorkflowExecutionFailedEventAttributes()
	s.Equal("second error", attrs2.GetFailure().GetMessage())

	exec = closedExecutions[3] // third: timed out
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, exec.GetStatus())
	lastEvent = s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, lastEvent.GetEventType())
	attrs3 := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
	s.Equal(attrs3.GetRetryState(), enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET)

	exec = closedExecutions[2] // fourth: success
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, exec.GetStatus())
	lastEvent = s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())
	attrs1 = lastEvent.GetWorkflowExecutionCompletedEventAttributes()
	s.Equal("pass again", s.decodePayloadsString(attrs1.GetResult()))

	exec = closedExecutions[1] // fifth: success
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, exec.GetStatus())
	lastEvent = s.getLastEvent(s.namespace, exec.GetExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())
	attrs1 = lastEvent.GetWorkflowExecutionCompletedEventAttributes()
	s.Equal("final pass", s.decodePayloadsString(attrs1.GetResult()))

	exec = closedExecutions[0] // sixth: terminated
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, exec.GetStatus())
	events := s.getHistory(s.namespace, exec.GetExecution())
	s.Equal(2, len(events)) // only started and terminated
	lastEvent = events[len(events)-1]
	attrs4 := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
	s.Equal("test is over", attrs4.GetReason())
}

func (s *clientIntegrationSuite) listOpenWorkflowExecutions(start, end time.Time, id string, expectedNumber int) []*workflowpb.WorkflowExecutionInfo {
	s.T().Helper()
	for i := 0; i < 20; i++ {
		resp, err := s.sdkClient.ListOpenWorkflow(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: int32(2 * expectedNumber),
			StartTimeFilter: &filterpb.StartTimeFilter{EarliestTime: &start, LatestTime: &end},
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == expectedNumber {
			return resp.GetExecutions()
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.FailNow("didn't get expected number")
	panic("unreached")
}

func (s *clientIntegrationSuite) listClosedWorkflowExecutions(start, end time.Time, id string, expectedNumber int) []*workflowpb.WorkflowExecutionInfo {
	s.T().Helper()
	for i := 0; i < 20; i++ {
		resp, err := s.sdkClient.ListClosedWorkflow(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: int32(2 * expectedNumber),
			StartTimeFilter: &filterpb.StartTimeFilter{EarliestTime: &start, LatestTime: &end},
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err)
		if len(resp.GetExecutions()) == expectedNumber {
			return resp.GetExecutions()
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.FailNow("didn't get expected number")
	panic("unreached")
}
