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
	"time"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/persistence/serialization"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestGetWorkflowExecutionHistory_All() {
	workflowID := "integration-get-workflow-history-events-long-poll-test-all"
	workflowTypeName := "integration-get-workflow-history-events-long-poll-test-all-type"
	taskqueueName := "integration-get-workflow-history-events-long-poll-test-all-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	taskQueue := &taskqueuepb.TaskQueue{Name: taskqueueName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
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
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              taskQueue,
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(25 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(25 * time.Second),
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	getHistory := func(namespace string, workflowID string, token []byte, isLongPoll bool) ([]*historypb.HistoryEvent, []byte) {
		responseInner, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize: 100,
			WaitNewEvent:    isLongPoll,
			NextPageToken:   token,
		})
		s.NoError(err)

		return responseInner.History.Events, responseInner.NextPageToken
	}

	var allEvents []*historypb.HistoryEvent
	var events []*historypb.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now().UTC()
	events, token = getHistory(s.namespace, workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// Make first command to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask1 := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask1))
	})
	start = time.Now().UTC()
	events, token = getHistory(s.namespace, workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().After(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask2 := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask2))
	})
	for token != nil {
		events, token = getHistory(s.namespace, workflowID, token, true)
		allEvents = append(allEvents, events...)
	}

	// there are total 11 events
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowTaskStarted
	//  4. WorkflowTaskCompleted
	//  5. ActivityTaskScheduled
	//  6. ActivityTaskStarted
	//  7. ActivityTaskCompleted
	//  8. WorkflowTaskScheduled
	//  9. WorkflowTaskStarted
	// 10. WorkflowTaskCompleted
	// 11. WorkflowExecutionCompleted
	s.Equal(11, len(allEvents))

	// test non long poll
	allEvents = nil
	token = nil
	for {
		events, token = getHistory(s.namespace, workflowID, token, false)
		allEvents = append(allEvents, events...)
		if token == nil {
			break
		}
	}
	s.Equal(11, len(allEvents))
}

func (s *integrationSuite) TestGetWorkflowExecutionHistory_Close() {
	workflowID := "integration-get-workflow-history-events-long-poll-test-close"
	workflowTypeName := "integration-get-workflow-history-events-long-poll-test-close-type"
	taskqueueName := "integration-get-workflow-history-events-long-poll-test-close-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	taskQueue := &taskqueuepb.TaskQueue{Name: taskqueueName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          workflowID,
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
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              taskQueue,
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(25 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(25 * time.Second),
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

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	getHistory := func(namespace string, workflowID string, token []byte, isLongPoll bool) ([]*historypb.HistoryEvent, []byte) {
		closeEventOnly := enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
		responseInner, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize:        100,
			WaitNewEvent:           isLongPoll,
			NextPageToken:          token,
			HistoryEventFilterType: closeEventOnly,
		})

		s.NoError(err)
		return responseInner.History.Events, responseInner.NextPageToken
	}

	var events []*historypb.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now().UTC()
	events, token = getHistory(s.namespace, workflowID, token, true)
	s.True(time.Now().UTC().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// Make first command to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask1 := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask1))
	})
	start = time.Now().UTC()
	events, token = getHistory(s.namespace, workflowID, token, true)
	s.True(time.Now().UTC().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask2 := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask2))
	})
	for token != nil {
		events, token = getHistory(s.namespace, workflowID, token, true)

		// since we are only interested in close event
		if token == nil {
			s.Equal(1, len(events))
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[0].EventType)
		} else {
			s.Empty(events)
		}
	}

	// test non long poll for only closed events
	token = nil
	for {
		events, token = getHistory(s.namespace, workflowID, token, false)
		if token == nil {
			break
		}
	}
	s.Equal(1, len(events))
	s.Logger.Info("Done TestGetWorkflowExecutionHistory_Close")
}

func (s *integrationSuite) TestGetWorkflowExecutionHistory_GetRawHistoryData() {
	workflowID := "integration-poll-for-workflow-raw-history-events-long-poll-test-all"
	workflowTypeName := "integration-poll-for-workflow-raw-history-events-long-poll-test-all-type"
	taskqueueName := "integration-poll-for-workflow-raw-history-events-long-poll-test-all-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	taskQueue := &taskqueuepb.TaskQueue{Name: taskqueueName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.testRawHistoryNamespaceName,
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *workflow.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "1",
						ActivityType:           &commonpb.ActivityType{Name: activityName},
						TaskQueue:              taskQueue,
						Input:                  payloads.EncodeBytes(buf.Bytes()),
						ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						ScheduleToStartTimeout: timestamp.DurationPtr(25 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(25 * time.Second),
					},
				},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.testRawHistoryNamespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	getHistoryWithLongPoll := func(namespace string, workflowID string, token []byte, isLongPoll bool) ([]*commonpb.DataBlob, []byte) {
		responseInner, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize: 100,
			WaitNewEvent:    isLongPoll,
			NextPageToken:   token,
		})
		s.Nil(err)
		return responseInner.RawHistory, responseInner.NextPageToken
	}

	getHistory := func(namespace string, workflowID string, token []byte) ([]*commonpb.DataBlob, []byte) {
		responseInner, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			MaximumPageSize: int32(100),
			NextPageToken:   token,
		})
		s.Nil(err)
		return responseInner.RawHistory, responseInner.NextPageToken
	}

	serializer := serialization.NewSerializer()
	convertBlob := func(blobs []*commonpb.DataBlob) []*historypb.HistoryEvent {
		events := []*historypb.HistoryEvent{}
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enumspb.ENCODING_TYPE_PROTO3)
			blobEvents, err := serializer.DeserializeEvents(&commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	var blobs []*commonpb.DataBlob
	var token []byte

	var allEvents []*historypb.HistoryEvent
	var events []*historypb.HistoryEvent

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now().UTC()
	blobs, token = getHistoryWithLongPoll(s.testRawHistoryNamespaceName, workflowID, token, true)
	events = convertBlob(blobs)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// Make first command to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask1 := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask1))
	})
	start = time.Now().UTC()
	blobs, token = getHistoryWithLongPoll(s.testRawHistoryNamespaceName, workflowID, token, true)
	events = convertBlob(blobs)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().After(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask2 := poller.PollAndProcessWorkflowTask(false, false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask2))
	})
	for token != nil {
		blobs, token = getHistoryWithLongPoll(s.testRawHistoryNamespaceName, workflowID, token, true)
		events = convertBlob(blobs)
		allEvents = append(allEvents, events...)
	}

	// there are total 11 events
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowTaskStarted
	//  4. WorkflowTaskCompleted
	//  5. ActivityTaskScheduled
	//  6. ActivityTaskStarted
	//  7. ActivityTaskCompleted
	//  8. WorkflowTaskScheduled
	//  9. WorkflowTaskStarted
	// 10. WorkflowTaskCompleted
	// 11. WorkflowExecutionCompleted
	s.Equal(11, len(allEvents))

	// test non long poll
	allEvents = nil
	token = nil
	for {
		blobs, token = getHistory(s.testRawHistoryNamespaceName, workflowID, token)
		events = convertBlob(blobs)
		allEvents = append(allEvents, events...)
		if token == nil {
			break
		}
	}
	s.Equal(11, len(allEvents))
}

func (s *clientIntegrationSuite) TestGetHistoryReverse() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	activityId := "heartbeat_retry"
	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})
		f1 := workflow.ExecuteActivity(ctx1, activityFn)
		err1 := f1.Get(ctx1, nil)
		s.NoError(err1)

		return nil
	}

	s.worker.RegisterActivity(activityFn)
	s.worker.RegisterWorkflow(workflowFn)

	wfId := "integration-test-gethistoryreverse"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	wfeResponse, err := s.sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.Nil(err)

	eventDefaultOrder := s.getHistory(s.namespace, wfeResponse.WorkflowExecutionInfo.Execution)
	eventDefaultOrder = reverseSlice(eventDefaultOrder)

	events := s.getHistoryReverse(s.namespace, wfeResponse.WorkflowExecutionInfo.Execution, 100)
	s.Equal(len(eventDefaultOrder), len(events))
	s.Equal(eventDefaultOrder, events)

	events = s.getHistoryReverse(s.namespace, wfeResponse.WorkflowExecutionInfo.Execution, 3)
	s.Equal(len(eventDefaultOrder), len(events))
	s.Equal(eventDefaultOrder, events)

	events = s.getHistoryReverse(s.namespace, wfeResponse.WorkflowExecutionInfo.Execution, 1)
	s.Equal(len(eventDefaultOrder), len(events))
	s.Equal(eventDefaultOrder, events)
}

func (s *clientIntegrationSuite) TestGetHistoryReverse_MultipleBranches() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	activityId := "integration-test-activity-gethistory-reverse-multiple-branches"
	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})

		var err1, err2 error

		f1 := workflow.ExecuteActivity(ctx1, activityFn)
		err1 = f1.Get(ctx1, nil)
		s.NoError(err1)

		workflow.Sleep(ctx, time.Second*2)

		f2 := workflow.ExecuteActivity(ctx1, activityFn)
		err2 = f2.Get(ctx1, nil)
		s.NoError(err2)

		return nil
	}

	s.worker.RegisterActivity(activityFn)
	s.worker.RegisterWorkflow(workflowFn)

	wfId := "integration-test-wf-gethistory-reverse-multiple-branches"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	// we want to reset workflow in the middle of execution
	time.Sleep(time.Second)

	wfeResponse, err := s.sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)

	rweResponse, err := s.sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.namespace,
		WorkflowExecution:         wfeResponse.WorkflowExecutionInfo.Execution,
		Reason:                    "TestGetHistoryReverseBranch",
		WorkflowTaskFinishEventId: 10,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	resetRunId := rweResponse.GetRunId()
	resetWorkflowRun := s.sdkClient.GetWorkflow(ctx, wfId, resetRunId)
	err = resetWorkflowRun.Get(ctx, nil)
	s.NoError(err)

	resetWfeResponse, err := s.sdkClient.DescribeWorkflowExecution(ctx, resetWorkflowRun.GetID(), resetWorkflowRun.GetRunID())
	s.NoError(err)

	eventsDefaultOrder := s.getHistory(s.namespace, resetWfeResponse.WorkflowExecutionInfo.Execution)
	eventsDefaultOrder = reverseSlice(eventsDefaultOrder)

	events := s.getHistoryReverse(s.namespace, resetWfeResponse.WorkflowExecutionInfo.Execution, 100)
	s.Equal(len(eventsDefaultOrder), len(events))
	s.Equal(eventsDefaultOrder, events)

	events = s.getHistoryReverse(s.namespace, resetWfeResponse.WorkflowExecutionInfo.Execution, 3)
	s.Equal(len(eventsDefaultOrder), len(events))
	s.Equal(eventsDefaultOrder, events)

	events = s.getHistoryReverse(s.namespace, resetWfeResponse.WorkflowExecutionInfo.Execution, 1)
	s.Equal(len(eventsDefaultOrder), len(events))
	s.Equal(eventsDefaultOrder, events)
}

func reverseSlice(events []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	return events
}

func (s *IntegrationBase) getHistoryReverse(namespace string, execution *commonpb.WorkflowExecution, pageSize int32) []*historypb.HistoryEvent {
	historyResponse, err := s.engine.GetWorkflowExecutionHistoryReverse(NewContext(), &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace:       namespace,
		Execution:       execution,
		NextPageToken:   nil,
		MaximumPageSize: pageSize,
	})
	s.Require().NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistoryReverse(NewContext(), &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
			Namespace:       namespace,
			Execution:       execution,
			NextPageToken:   historyResponse.NextPageToken,
			MaximumPageSize: pageSize,
		})
		s.Require().NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}
