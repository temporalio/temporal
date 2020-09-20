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
	"encoding/binary"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
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
					ActivityId:             strconv.Itoa(int(1)),
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
					ActivityId:             strconv.Itoa(int(1)),
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

	serializer := persistence.NewPayloadSerializer()
	convertBlob := func(blobs []*commonpb.DataBlob) []*historypb.HistoryEvent {
		events := []*historypb.HistoryEvent{}
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enumspb.ENCODING_TYPE_PROTO3)
			blobEvents, err := serializer.DeserializeBatchEvents(&serialization.DataBlob{
				Encoding: enumspb.ENCODING_TYPE_PROTO3,
				Data:     blob.Data,
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

func (s *integrationSuite) TestAdminGetWorkflowExecutionRawHistory_All() {
	workflowID := "integration-admin-get-workflow-history-raw-events-all"
	workflowTypeName := "integration-admin-get-workflow-history-raw-events-all-type"
	taskqueueName := "integration-admin-get-workflow-history-raw-events-all-taskqueue"
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

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      we.GetRunId(),
	}

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
					ActivityId:             strconv.Itoa(int(1)),
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
	pageSize := 1
	getHistory := func(namespace string, execution *commonpb.WorkflowExecution, firstEventID int64, nextEventID int64,
		token []byte) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

		return s.adminClient.GetWorkflowExecutionRawHistoryV2(NewContext(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace:       namespace,
			Execution:       execution,
			StartEventId:    firstEventID,
			EndEventId:      nextEventID,
			MaximumPageSize: int32(pageSize),
			NextPageToken:   token,
		})
	}

	serializer := persistence.NewPayloadSerializer()
	convertBlob := func(blobs []*commonpb.DataBlob) []*historypb.HistoryEvent {
		var events []*historypb.HistoryEvent
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enumspb.ENCODING_TYPE_PROTO3)
			blobEvents, err := serializer.DeserializeBatchEvents(&serialization.DataBlob{
				Encoding: enumspb.ENCODING_TYPE_PROTO3,
				Data:     blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	var blobs []*commonpb.DataBlob
	var token []byte

	resp, err := getHistory(s.namespace, execution, common.FirstEventID, common.EndEventID, token)
	s.NoError(err)
	s.True(len(resp.HistoryBatches) == pageSize)
	blobs = append(blobs, resp.HistoryBatches...)
	token = resp.NextPageToken
	if token != nil {
		resp, err := getHistory(s.namespace, execution, common.FirstEventID, common.EndEventID, token)
		s.NoError(err)
		s.Equal(0, len(resp.HistoryBatches))
		s.Nil(resp.NextPageToken)
	}
	// until now, only start event and workflow task scheduled should be in the history
	events := convertBlob(blobs)
	s.True(len(events) == 2)

	// poll so workflow will make progress, and get history from the very begining
	poller.PollAndProcessWorkflowTask(false, false)
	blobs = nil
	token = nil
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.namespace, execution, common.FirstEventID, common.EndEventID, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// now, there shall be 3 batches of events:
	// 1. start event and workflow task scheduled;
	// 2. workflow task started
	// 3. workflow task completed and activity task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 3)
	s.True(len(events) == 5)

	// continue the workflow by processing activity
	poller.PollAndProcessActivityTask(false)
	// continue to get the history
	token = nil
	beginingEventID := events[len(events)-1].GetEventId() + 1
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.namespace, execution, beginingEventID, common.EndEventID, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// now, there shall be 4 batches of events:
	// 1. start event and workflow task scheduled;
	// 2. workflow task started
	// 3. workflow task completed, activity task scheduled
	// 4. activity task started, activity task completed and workflow task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 4)
	s.True(len(events) == 8)

	// continue the workflow by processing command, after this, workflow shall end
	poller.PollAndProcessWorkflowTask(false, false)
	// continue to get the history
	token = nil
	beginingEventID = events[len(events)-1].GetEventId() + 1
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.namespace, execution, beginingEventID, common.EndEventID, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// now, there shall be 6 batches of events:
	// 1. start event and workflow task scheduled;
	// 2. workflow task started
	// 3. workflow task completed, activity task scheduled
	// 4. activity task started, activity task completed and workflow task scheduled
	// 5. workflow task started
	// 6. workflow task completed, workflow execution completed
	events = convertBlob(blobs)
	s.True(len(blobs) == 6)
	s.True(len(events) == 11)

	// get history in between
	blobs = nil // clear existing blobs
	token = nil
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.namespace, execution, 4, 7, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// should get the following events
	// 1. workflow task completed and activity task scheduled
	// 2. activity task started, activity task completed, workflow task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 2)
	s.True(len(events) == 5)
}

func (s *integrationSuite) TestAdminGetWorkflowExecutionRawHistory_InTheMiddle() {
	workflowID := "integration-admin-get-workflow-history-raw-events-in-the-middle"
	workflowTypeName := "integration-admin-get-workflow-history-raw-events-in-the-middle-type"
	taskqueueName := "integration-admin-get-workflow-history-raw-events-in-the-middle-taskqueue"
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

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      we.GetRunId(),
	}

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
					ActivityId:             strconv.Itoa(int(1)),
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

	getHistory := func(namespace string, execution *commonpb.WorkflowExecution, firstEventID int64, nextEventID int64,
		token []byte) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

		return s.adminClient.GetWorkflowExecutionRawHistoryV2(NewContext(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace:       namespace,
			Execution:       execution,
			StartEventId:    firstEventID,
			EndEventId:      nextEventID,
			MaximumPageSize: 1,
			NextPageToken:   token,
		})
	}

	// poll so workflow will make progress
	poller.PollAndProcessWorkflowTask(false, false)
	// continue the workflow by processing activity
	poller.PollAndProcessActivityTask(false)
	// poll so workflow will make progress
	poller.PollAndProcessWorkflowTask(false, false)

	// now, there shall be 6 batches of events:
	// 1. start event and workflow task scheduled;
	// 2. workflow task started
	// 3. workflow task completed, activity task scheduled
	// 4. activity task started, activity task completed and workflow task scheduled
	// 5. workflow task started
	// 6. workflow task completed, workflow execution completed

	// trying getting history from the middle to the end

	serializer := persistence.NewPayloadSerializer()
	convertBlob := func(blobs []*commonpb.DataBlob) []*historypb.HistoryEvent {
		var events []*historypb.HistoryEvent
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enumspb.ENCODING_TYPE_PROTO3)
			blobEvents, err := serializer.DeserializeBatchEvents(&serialization.DataBlob{
				Encoding: enumspb.ENCODING_TYPE_PROTO3,
				Data:     blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	firstEventID := int64(6)
	var token []byte
	// this should get the #4 batch, activity task started
	resp, err := getHistory(s.namespace, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
	events := convertBlob(resp.HistoryBatches)
	s.Equal(3, len(events))
	token = resp.NextPageToken
	s.NotEmpty(token)

	// this should get the #5 batch, workflow task started
	resp, err = getHistory(s.namespace, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
	events = convertBlob(resp.HistoryBatches)
	s.Equal(1, len(events))
	token = resp.NextPageToken
	s.NotEmpty(token)

	// this should get the #6 batch, workflow task completed, workflow execution completed
	resp, err = getHistory(s.namespace, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
	events = convertBlob(resp.HistoryBatches)
	s.Equal(2, len(events))
	token = resp.NextPageToken
	s.NotEmpty(token)

	// this should get no further batches
	resp, err = getHistory(s.namespace, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(0, len(resp.HistoryBatches))
	token = resp.NextPageToken
	s.Empty(token)
}
