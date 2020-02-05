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
	"bytes"
	"encoding/binary"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
)

func (s *integrationSuite) TestGetWorkflowExecutionHistory_All() {
	workflowID := "integration-get-workflow-history-events-long-poll-test-all"
	workflowTypeName := "integration-get-workflow-history-events-long-poll-test-all-type"
	tasklistName := "integration-get-workflow-history-events-long-poll-test-all-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: workflowTypeName}

	taskList := &commonproto.TaskList{Name: tasklistName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          workflowID,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(1)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      taskList,
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 25,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       25,
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// this function poll events from history side
	getHistory := func(domain string, workflowID string, token []byte, isLongPoll bool) ([]*commonproto.HistoryEvent, []byte) {
		responseInner, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: domain,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize: 100,
			WaitForNewEvent: isLongPoll,
			NextPageToken:   token,
		})
		s.NoError(err)

		return responseInner.History.Events, responseInner.NextPageToken
	}

	var allEvents []*commonproto.HistoryEvent
	var events []*commonproto.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// make first decision to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errDecision1 := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(errDecision1))
	})
	start = time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().After(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errDecision2 := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(errDecision2))
	})
	for token != nil {
		events, token = getHistory(s.domainName, workflowID, token, true)
		allEvents = append(allEvents, events...)
	}

	// there are total 11 events
	//  1. WorkflowExecutionStarted
	//  2. DecisionTaskScheduled
	//  3. DecisionTaskStarted
	//  4. DecisionTaskCompleted
	//  5. ActivityTaskScheduled
	//  6. ActivityTaskStarted
	//  7. ActivityTaskCompleted
	//  8. DecisionTaskScheduled
	//  9. DecisionTaskStarted
	// 10. DecisionTaskCompleted
	// 11. WorkflowExecutionCompleted
	s.Equal(11, len(allEvents))

	// test non long poll
	allEvents = nil
	token = nil
	for {
		events, token = getHistory(s.domainName, workflowID, token, false)
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
	tasklistName := "integration-get-workflow-history-events-long-poll-test-close-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: workflowTypeName}

	taskList := &commonproto.TaskList{Name: tasklistName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          workflowID,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(1)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      taskList,
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 25,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       25,
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// this function poll events from history side
	getHistory := func(domain string, workflowID string, token []byte, isLongPoll bool) ([]*commonproto.HistoryEvent, []byte) {
		closeEventOnly := enums.HistoryEventFilterTypeCloseEvent
		responseInner, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain: domain,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize:        100,
			WaitForNewEvent:        isLongPoll,
			NextPageToken:          token,
			HistoryEventFilterType: closeEventOnly,
		})

		s.NoError(err)
		return responseInner.History.Events, responseInner.NextPageToken
	}

	var events []*commonproto.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	s.True(time.Now().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// make first decision to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errDecision1 := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(errDecision1))
	})
	start = time.Now()
	events, token = getHistory(s.domainName, workflowID, token, true)
	s.True(time.Now().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errDecision2 := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(errDecision2))
	})
	for token != nil {
		events, token = getHistory(s.domainName, workflowID, token, true)

		// since we are only interested in close event
		if token == nil {
			s.Equal(1, len(events))
			s.Equal(enums.EventTypeWorkflowExecutionCompleted, events[0].EventType)
		} else {
			s.Empty(events)
		}
	}

	// test non long poll for only closed events
	token = nil
	for {
		events, token = getHistory(s.domainName, workflowID, token, false)
		if token == nil {
			break
		}
	}
	s.Equal(1, len(events))
}

func (s *integrationSuite) TestAdminGetWorkflowExecutionRawHistory_All() {
	workflowID := "integration-admin-get-workflow-history-raw-events-all"
	workflowTypeName := "integration-admin-get-workflow-history-raw-events-all-type"
	tasklistName := "integration-admin-get-workflow-history-raw-events-all-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: workflowTypeName}

	taskList := &commonproto.TaskList{Name: tasklistName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          workflowID,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	execution := &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      we.GetRunId(),
	}

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(1)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      taskList,
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 25,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       25,
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// this function poll events from history side
	pageSize := 1
	getHistory := func(domain string, execution *commonproto.WorkflowExecution, firstEventID int64, nextEventID int64,
		token []byte) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

		return s.adminClient.GetWorkflowExecutionRawHistory(NewContext(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
			Domain:          domain,
			Execution:       execution,
			FirstEventId:    firstEventID,
			NextEventId:     nextEventID,
			MaximumPageSize: int32(pageSize),
			NextPageToken:   token,
		})
	}

	serializer := persistence.NewPayloadSerializer()
	convertBlob := func(blobs []*commonproto.DataBlob) []*shared.HistoryEvent {
		var events []*shared.HistoryEvent
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enums.EncodingTypeThriftRW)
			blobEvents, err := serializer.DeserializeBatchEvents(&persistence.DataBlob{
				Encoding: common.EncodingTypeThriftRW,
				Data:     blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	var blobs []*commonproto.DataBlob
	var token []byte

	resp, err := getHistory(s.domainName, execution, common.FirstEventID, common.EndEventID, token)
	s.NoError(err)
	s.True(len(resp.HistoryBatches) == pageSize)
	blobs = append(blobs, resp.HistoryBatches...)
	token = resp.NextPageToken
	if token != nil {
		resp, err := getHistory(s.domainName, execution, common.FirstEventID, common.EndEventID, token)
		s.NoError(err)
		s.Equal(0, len(resp.HistoryBatches))
		s.Nil(resp.NextPageToken)
	}
	// until now, only start event and decision task scheduled should be in the history
	events := convertBlob(blobs)
	s.True(len(events) == 2)

	// poll so workflow will make progress, and get history from the very begining
	poller.PollAndProcessDecisionTask(false, false)
	blobs = nil
	token = nil
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, common.FirstEventID, common.EndEventID, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// now, there shall be 3 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 3)
	s.True(len(events) == 5)

	// continue the workflow by processing activity
	poller.PollAndProcessActivityTask(false)
	// continue to get the history
	token = nil
	beginingEventID := events[len(events)-1].GetEventId() + 1
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, beginingEventID, common.EndEventID, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// now, there shall be 5 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	// 4. activity task started
	// 5. activity task completed and decision task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 5)
	s.True(len(events) == 8)

	// continue the workflow by processing decision, after this, workflow shall end
	poller.PollAndProcessDecisionTask(false, false)
	// continue to get the history
	token = nil
	beginingEventID = events[len(events)-1].GetEventId() + 1
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, beginingEventID, common.EndEventID, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// now, there shall be 7 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	// 4. activity task started
	// 5. activity task completed and decision task scheduled
	// 6. decision task started
	// 7. decision task completed and workflow execution completed
	events = convertBlob(blobs)
	s.True(len(blobs) == 7)
	s.True(len(events) == 11)

	// get history in between
	blobs = nil // clear existing blobs
	token = nil
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, 4, 7, token)
		s.NoError(err)
		s.True(len(resp.HistoryBatches) <= pageSize)
		blobs = append(blobs, resp.HistoryBatches...)
		token = resp.NextPageToken
	}
	// should get the following events
	// 1. decision task completed and activity task scheduled
	// 2. activity task started
	events = convertBlob(blobs)
	s.True(len(blobs) == 2)
	s.True(len(events) == 3)
}

func (s *integrationSuite) TestAdminGetWorkflowExecutionRawHistory_InTheMiddle() {
	workflowID := "integration-admin-get-workflow-history-raw-events-in-the-middle"
	workflowTypeName := "integration-admin-get-workflow-history-raw-events-in-the-middle-type"
	tasklistName := "integration-admin-get-workflow-history-raw-events-in-the-middle-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: workflowTypeName}

	taskList := &commonproto.TaskList{Name: tasklistName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          workflowID,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	execution := &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      we.GetRunId(),
	}

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(1)),
					ActivityType:                  &commonproto.ActivityType{Name: activityName},
					TaskList:                      taskList,
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 25,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       25,
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	getHistory := func(domain string, execution *commonproto.WorkflowExecution, firstEventID int64, nextEventID int64,
		token []byte) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

		return s.adminClient.GetWorkflowExecutionRawHistory(NewContext(), &adminservice.GetWorkflowExecutionRawHistoryRequest{
			Domain:          domain,
			Execution:       execution,
			FirstEventId:    firstEventID,
			NextEventId:     nextEventID,
			MaximumPageSize: 1,
			NextPageToken:   token,
		})
	}

	// poll so workflow will make progress
	poller.PollAndProcessDecisionTask(false, false)
	// continue the workflow by processing activity
	poller.PollAndProcessActivityTask(false)
	// poll so workflow will make progress
	poller.PollAndProcessDecisionTask(false, false)

	// now, there shall be 5 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	// 4. activity task started
	// 5. activity task completed and decision task scheduled
	// 6. decision task started
	// 7. decision task completed and workflow execution completed

	// trying getting history from the middle to the end
	firstEventID := int64(5)
	var token []byte
	// this should get the #4 batch, activity task started
	resp, err := getHistory(s.domainName, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
	token = resp.NextPageToken
	s.NotEmpty(token)

	// this should get the #5 batch, activity task completed and decision task scheduled
	resp, err = getHistory(s.domainName, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
	token = resp.NextPageToken
	s.NotEmpty(token)

	// this should get the #6 batch, decision task started
	resp, err = getHistory(s.domainName, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
	token = resp.NextPageToken
	s.NotEmpty(token)

	// this should get the #7 batch, decision task completed and workflow execution completed
	resp, err = getHistory(s.domainName, execution, firstEventID, common.EndEventID, token)
	s.NoError(err)
	s.Equal(1, len(resp.HistoryBatches))
}

func (s *integrationSuite) TestGetWorkflowExecutionRawHistory_All() {
	workflowID := "integration-get-workflow-history-raw-events-all"
	workflowTypeName := "integration-get-workflow-history-raw-events-all-type"
	tasklistName := "integration-get-workflow-history-raw-events-all-tasklist"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonproto.WorkflowType{Name: workflowTypeName}

	taskList := &commonproto.TaskList{Name: tasklistName}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          workflowID,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      1,
		Identity:                            identity,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	execution := &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      we.GetRunId(),
	}

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *workflow.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeScheduleActivityTask,
				Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{
					ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    "1",
						ActivityType:                  &commonproto.ActivityType{Name: activityName},
						TaskList:                      taskList,
						Input:                         buf.Bytes(),
						ScheduleToCloseTimeoutSeconds: 100,
						ScheduleToStartTimeoutSeconds: 25,
						StartToCloseTimeoutSeconds:    50,
						HeartbeatTimeoutSeconds:       25,
					},
				}}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{
				CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Done"),
				}},
		}}, nil
	}

	// activity handler
	atHandler := func(execution *commonproto.WorkflowExecution, activityType *commonproto.ActivityType,
		activityID string, input []byte, taskToken []byte) ([]byte, bool, error) {

		return []byte("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// this function poll events from history side
	pageSize := 1
	getHistory := func(domain string, execution *commonproto.WorkflowExecution,
		token []byte) (*workflowservice.GetWorkflowExecutionRawHistoryResponse, error) {

		return s.engine.GetWorkflowExecutionRawHistory(NewContext(), &workflowservice.GetWorkflowExecutionRawHistoryRequest{
			Domain:          domain,
			Execution:       execution,
			MaximumPageSize: int32(pageSize),
			NextPageToken:   token,
		})
	}

	serializer := persistence.NewPayloadSerializer()

	convertBlob := func(blobs []*commonproto.DataBlob) []*shared.HistoryEvent {
		var events []*shared.HistoryEvent
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enums.EncodingTypeThriftRW)
			blobEvents, err := serializer.DeserializeBatchEvents(&persistence.DataBlob{
				Encoding: common.EncodingTypeThriftRW,
				Data:     blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	var blobs []*commonproto.DataBlob
	var token []byte

	resp, err := getHistory(s.domainName, execution, token)
	s.NoError(err)
	s.True(len(resp.RawHistory) == pageSize)
	blobs = append(blobs, resp.RawHistory...)
	token = resp.NextPageToken
	if token != nil {
		resp, err := getHistory(s.domainName, execution, token)
		s.NoError(err)
		s.Equal(0, len(resp.RawHistory))
		s.Nil(resp.NextPageToken)
	}
	// until now, only start event and decision task scheduled should be in the history
	events := convertBlob(blobs)
	s.True(len(events) == 2)

	// poll so workflow will make progress, and get history from the very begining
	poller.PollAndProcessDecisionTask(false, false)
	blobs = nil
	token = nil

	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, token)
		s.NoError(err)
		s.True(len(resp.RawHistory) <= pageSize)
		blobs = append(blobs, resp.RawHistory...)
		token = resp.NextPageToken
	}
	// now, there shall be 3 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 3)
	s.True(len(events) == 5)

	blobs = nil
	token = nil
	// continue the workflow by processing activity
	poller.PollAndProcessActivityTask(false)
	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, token)
		s.NoError(err)
		s.True(len(resp.RawHistory) <= pageSize)
		blobs = append(blobs, resp.RawHistory...)
		token = resp.NextPageToken
	}

	// now, there shall be 5 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	// 4. activity task started
	// 5. activity task completed and decision task scheduled
	events = convertBlob(blobs)
	s.True(len(blobs) == 5)
	s.True(len(events) == 8)

	blobs = nil
	token = nil
	// continue the workflow by processing decision, after this, workflow shall end
	poller.PollAndProcessDecisionTask(false, false)

	for continuePaging := true; continuePaging; continuePaging = len(token) != 0 {
		resp, err = getHistory(s.domainName, execution, token)
		s.NoError(err)
		s.True(len(resp.RawHistory) <= pageSize)
		blobs = append(blobs, resp.RawHistory...)
		token = resp.NextPageToken
	}
	// now, there shall be 7 batches of events:
	// 1. start event and decision task scheduled;
	// 2. decision task started
	// 3. decision task completed and activity task scheduled
	// 4. activity task started
	// 5. activity task completed and decision task scheduled
	// 6. decision task started
	// 7. decision task completed and workflow execution completed
	events = convertBlob(blobs)
	s.True(len(blobs) == 7)
	s.True(len(events) == 11)
}
