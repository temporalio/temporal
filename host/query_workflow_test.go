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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/atomic"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
)

func (s *integrationSuite) TestQueryWorkflow_Sticky() {
	id := "interation-query-workflow-test-sticky"
	wt := "interation-query-workflow-test-sticky-type"
	tl := "interation-query-workflow-test-sticky-taskqueue"
	stl := "interation-query-workflow-test-sticky-taskqueue-sticky"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stl}
	stickyScheduleToStartTimeout := 10 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryArgs := payloads.EncodeString("my query input")
	queryHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"my query header key": payload.EncodeString("my query header value")},
	}
	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		s.Equal(queryArgs, task.Query.QueryArgs)
		s.Equal(queryHeader, task.Query.Header)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    taskQueue,
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		ActivityTaskHandler:          atHandler,
		QueryHandler:                 queryHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string) {
		queryResp, err := s.engine.QueryWorkflow(NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
				QueryArgs: queryArgs,
				Header:    queryHeader,
			},
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTaskWithSticky(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	go queryWorkflowFn("invalid-query-type")
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTaskWithSticky(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	queryResult = <-queryResultCh
	s.NotNil(queryResult.Err)
	s.IsType(&serviceerror.QueryFailed{}, queryResult.Err)
	s.Equal("unknown-query-type", queryResult.Err.Error())
}

func (s *integrationSuite) TestQueryWorkflow_StickyTimeout() {
	id := "interation-query-workflow-test-sticky-timeout"
	wt := "interation-query-workflow-test-sticky-timeout-type"
	tl := "interation-query-workflow-test-sticky-timeout-taskqueue"
	stl := "interation-query-workflow-test-sticky-timeout-taskqueue-sticky"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stl}
	stickyScheduleToStartTimeout := 10 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    taskQueue,
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		ActivityTaskHandler:          atHandler,
		QueryHandler:                 queryHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string) {
		queryResp, err := s.engine.QueryWorkflow(NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
			},
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		// here we poll on normal taskqueue, to simulate a worker crash and restart
		// on the server side, server will first try the sticky taskqueue and then the normal taskqueue
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))
}

func (s *integrationSuite) TestQueryWorkflow_NonSticky() {
	id := "integration-query-workflow-test-non-sticky"
	wt := "integration-query-workflow-test-non-sticky-type"
	tl := "integration-query-workflow-test-non-sticky-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryArgs := payloads.EncodeString("my query input")
	queryHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"my query header key": payload.EncodeString("my query header value")},
	}
	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		s.Equal(queryArgs, task.Query.QueryArgs)
		s.Equal(queryHeader, task.Query.Header)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string, rejectCondition enumspb.QueryRejectCondition) {
		queryResp, err := s.engine.QueryWorkflow(NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
				QueryArgs: queryArgs,
				Header:    queryHeader,
			},
			QueryRejectCondition: rejectCondition,
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(queryType, enumspb.QUERY_REJECT_CONDITION_NONE)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	} // wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Nil(queryResult.Resp.QueryRejected)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	go queryWorkflowFn("invalid-query-type", enumspb.QUERY_REJECT_CONDITION_NONE)
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	queryResult = <-queryResultCh
	s.NotNil(queryResult.Err)
	s.IsType(&serviceerror.QueryFailed{}, queryResult.Err)
	s.Equal("unknown-query-type", queryResult.Err.Error())

	// advance the state of the workflow
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	go queryWorkflowFn(queryType, enumspb.QUERY_REJECT_CONDITION_NONE)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	} // wait until query result is ready
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Nil(queryResult.Resp.QueryRejected)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	rejectCondition := enumspb.QUERY_REJECT_CONDITION_NOT_OPEN
	go queryWorkflowFn(queryType, rejectCondition)
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.Nil(queryResult.Resp.QueryResult)
	s.NotNil(queryResult.Resp.QueryRejected.GetStatus())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, queryResult.Resp.QueryRejected.GetStatus())

	rejectCondition = enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY
	go queryWorkflowFn(queryType, rejectCondition)
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	} // wait until query result is ready
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Nil(queryResult.Resp.QueryRejected)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))
}

func (s *integrationSuite) TestQueryWorkflow_Consistent_PiggybackQuery() {
	id := "integration-query-workflow-test-consistent-piggyback-query"
	wt := "integration-query-workflow-test-consistent-piggyback-query-type"
	tl := "integration-query-workflow-test-consistent-piggyback-query-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	handledSignal := &atomic.Bool{}
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(2 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					handledSignal.Store(true)
					return []*commandpb.Command{}, nil
				}
			}
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string, rejectCondition enumspb.QueryRejectCondition) {
		// before the query is answer the signal is not handled because the workflow task is not dispatched
		// to the worker yet
		s.False(handledSignal.Load())
		queryResp, err := s.engine.QueryWorkflow(NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
			},
			QueryRejectCondition: rejectCondition,
		})
		// after the query is answered the signal is handled because query is consistent and since
		// signal came before query signal must be handled by the time query returns
		s.True(handledSignal.Load())
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// send signal to ensure there is an outstanding workflow task to dispatch query on
	// otherwise query will just go through matching
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// call QueryWorkflow in separate goroutine (because it is blocking). That will generate a query task
	// notice that the query comes after signal here but is consistent so it should reflect the state of the signal having been applied
	go queryWorkflowFn(queryType, enumspb.QUERY_REJECT_CONDITION_NONE)
	// ensure query has had enough time to at least start before a workflow task is polled
	// if the workflow task containing the signal is polled before query is started it will not impact
	// correctness but it will mean query will be able to be dispatched directly after signal
	// without being attached to the workflow task signal is on
	<-time.After(time.Second)

	isQueryTask, _, errInner := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		false,
		false,
		0,
		5,
		false,
		&querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeString("consistent query result"),
		})

	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(errInner)
	s.False(isQueryTask)

	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Nil(queryResult.Resp.QueryRejected)
	s.Equal("consistent query result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))
}

func (s *integrationSuite) TestQueryWorkflow_Consistent_Timeout() {
	id := "integration-query-workflow-test-consistent-timeout"
	wt := "integration-query-workflow-test-consistent-timeout-type"
	tl := "integration-query-workflow-test-consistent-timeout-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second), // ensure longer than time takes to handle signal
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second), // ensure longer than time it takes to handle signal
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second), // ensure longer than time takes to handle signal
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					<-time.After(3 * time.Second) // take longer to respond to the workflow task than the query waits for
					return []*commandpb.Command{}, nil
				}
			}
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string, rejectCondition enumspb.QueryRejectCondition) {
		shortCtx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Second)
		queryResp, err := s.engine.QueryWorkflow(shortCtx, &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
			},
			QueryRejectCondition: rejectCondition,
		})
		cancel()
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// call QueryWorkflow in separate goroutine (because it is blocking). That will generate a query task
	// notice that the query comes after signal here but is consistent so it should reflect the state of the signal having been applied
	go queryWorkflowFn(queryType, enumspb.QUERY_REJECT_CONDITION_NONE)
	// ensure query has had enough time to at least start before a workflow task is polled
	// if the workflow task containing the signal is polled before query is started it will not impact
	// correctness but it will mean query will be able to be dispatched directly after signal
	// without being attached to the workflow task signal is on
	<-time.After(time.Second)

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	// wait for query to timeout
	queryResult := <-queryResultCh
	s.Error(queryResult.Err) // got a timeout error
	s.Nil(queryResult.Resp)
}

func (s *integrationSuite) TestQueryWorkflow_Consistent_BlockedByStarted_NonSticky() {
	id := "integration-query-workflow-test-consistent-blocked-by-started-non-sticky"
	wt := "integration-query-workflow-test-consistent-blocked-by-started-non-sticky-type"
	tl := "integration-query-workflow-test-consistent-blocked-by-started-non-sticky-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second), // ensure longer than time takes to handle signal
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	handledSignal := &atomic.Bool{}
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second), // ensure longer than time it takes to handle signal
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second), // ensure longer than time takes to handle signal
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					// wait for some time to force workflow task to stay in started state while query is issued
					<-time.After(5 * time.Second)
					handledSignal.Store(true)
					return []*commandpb.Command{}, nil
				}
			}
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string, rejectCondition enumspb.QueryRejectCondition) {
		s.False(handledSignal.Load())
		queryResp, err := s.engine.QueryWorkflow(NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
			},
			QueryRejectCondition: rejectCondition,
		})
		s.True(handledSignal.Load())
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// send a signal that will take 5 seconds to handle
	// this causes the signal to still be outstanding at the time query arrives
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	go func() {
		// wait for workflow task for signal to get started before querying workflow
		// since signal processing takes 5 seconds and we wait only one second to issue the query
		// at the time the query comes in there will be a started workflow task
		// only once signal completes can queryWorkflow unblock
		<-time.After(time.Second)
		queryWorkflowFn(queryType, enumspb.QUERY_REJECT_CONDITION_NONE)
	}()

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	<-time.After(time.Second)

	// query should not have been dispatched on the workflow task which contains signal
	// because signal was already outstanding by the time query arrived
	select {
	case <-queryResultCh:
		s.Fail("query should not be ready yet")
	default:
	}

	// now that started workflow task completes poll for next task which will be a query task
	// containing the buffered query
	isQueryTask, err := poller.PollAndProcessWorkflowTask(false, false)
	s.True(isQueryTask)
	s.NoError(err)

	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Nil(queryResult.Resp.QueryRejected)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))
}

func (s *integrationSuite) TestQueryWorkflow_Consistent_NewWorkflowTask_Sticky() {
	id := "integration-query-workflow-test-consistent-new-workflow-task-sticky"
	wt := "integration-query-workflow-test-consistent-new-workflow-task-sticky-type"
	tl := "integration-query-workflow-test-consistent-new-workflow-task-sticky-taskqueue"
	stl := "integration-query-workflow-test-consistent-new-workflow-task-sticky-taskqueue-sticky"
	identity := "worker1"
	activityName := "activity_type1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	stickyTaskQueue := &taskqueuepb.TaskQueue{}
	stickyTaskQueue.Name = stl
	stickyScheduleToStartTimeout := 10 * time.Second

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second), // ensure longer than time takes to handle signal
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	handledSignal := &atomic.Bool{}
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second), // ensure longer than time it takes to handle signal
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second), // ensure longer than time takes to handle signal
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		} else if previousStartedEventID > 0 {
			for _, event := range history.Events {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					// wait for some time to force workflow task to stay in started state while query is issued
					<-time.After(5 * time.Second)
					handledSignal.Store(true)
					return []*commandpb.Command{}, nil
				}
			}
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := &TaskPoller{
		Engine:                       s.engine,
		Namespace:                    s.namespace,
		TaskQueue:                    taskQueue,
		Identity:                     identity,
		WorkflowTaskHandler:          wtHandler,
		ActivityTaskHandler:          atHandler,
		QueryHandler:                 queryHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
		StickyTaskQueue:              stickyTaskQueue,
		StickyScheduleToStartTimeout: stickyScheduleToStartTimeout,
	}

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTaskWithAttempt(false, false, false, true, 1)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(queryType string, rejectCondition enumspb.QueryRejectCondition) {
		s.False(handledSignal.Load())
		queryResp, err := s.engine.QueryWorkflow(NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
			},
			QueryRejectCondition: rejectCondition,
		})
		s.True(handledSignal.Load())
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// send a signal that will take 5 seconds to handle
	// this causes the signal to still be outstanding at the time query arrives
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	go func() {
		// wait for workflow task for signal to get started before querying workflow
		// since signal processing takes 5 seconds and we wait only one second to issue the query
		// at the time the query comes in there will be a started workflow task
		// only once signal completes can queryWorkflow unblock
		<-time.After(time.Second)

		// at this point there is a workflow task started on the worker so this second signal will become buffered
		signalName := "my signal"
		signalInput := payloads.EncodeString("my signal input")
		_, err = s.engine.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			SignalName: signalName,
			Input:      signalInput,
			Identity:   identity,
		})
		s.NoError(err)

		queryWorkflowFn(queryType, enumspb.QUERY_REJECT_CONDITION_NONE)
	}()

	_, err = poller.PollAndProcessWorkflowTaskWithSticky(false, false)
	s.NoError(err)
	<-time.After(time.Second)

	// query should not have been dispatched on the workflow task which contains signal
	// because signal was already outstanding by the time query arrived
	select {
	case <-queryResultCh:
		s.Fail("query should not be ready yet")
	default:
	}

	// now poll for workflow task which contains the query which was buffered
	isQueryTask, _, errInner := poller.PollAndProcessWorkflowTaskWithAttemptAndRetryAndForceNewWorkflowTask(
		false,
		false,
		true,
		true,
		0,
		5,
		false,
		&querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeString("consistent query result"),
		})

	// the task should not be a query task because at the time outstanding workflow task completed
	// there existed a buffered event which triggered the creation of a new workflow task which query was dispatched on
	s.False(isQueryTask)
	s.NoError(errInner)

	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Nil(queryResult.Resp.QueryRejected)
	s.Equal("consistent query result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))
}

func (s *integrationSuite) TestQueryWorkflow_BeforeFirstWorkflowTask() {
	id := "integration-test-query-workflow-before-first-workflow-task"
	wt := "integration-test-query-workflow-before-first-command-type"
	tl := "integration-test-query-workflow-before-first-workflow-taskqueue"
	identity := "worker1"
	queryType := "test-query"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// query workflow without any workflow task should timeout
	ctx, cancel := context.WithTimeout(NewContext(), 1*time.Second)
	defer cancel()
	queryResp, err := s.engine.QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
		Namespace: s.namespace,
		Execution: workflowExecution,
		Query: &querypb.WorkflowQuery{
			QueryType: queryType,
		},
	})
	s.IsType(&workflowservice.QueryWorkflowResponse{}, queryResp)
	s.IsType(&serviceerror.DeadlineExceeded{}, err)
}
