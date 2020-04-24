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
	"fmt"
	"strconv"
	"time"

	"github.com/temporalio/temporal/service/matching"

	"github.com/pborman/uuid"

	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/log/tag"
)

func (s *integrationSuite) TestContinueAsNewWorkflow() {
	id := "integration-continue-as-new-workflow-test"
	wt := "integration-continue-as-new-workflow-test-type"
	tl := "integration-continue-as-new-workflow-test-tasklist"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	header := &commonpb.Header{
		Fields: map[string][]byte{"tracing": []byte("sample payload")},
	}
	memo := &commonpb.Memo{
		Fields: map[string][]byte{"memoKey": []byte("memoVal")},
	}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string][]byte{"CustomKeywordField": []byte(`"1"`)},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskList:                   taskList,
		Input:                      nil,
		Header:                     header,
		Memo:                       memo,
		SearchAttributes:           searchAttr,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *eventpb.HistoryEvent
	dtHandler := func(execution *executionpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]byte, []*decisionpb.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = execution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ContinueAsNewWorkflowExecution,
				Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:               workflowType,
					TaskList:                   &tasklistpb.TaskList{Name: tl},
					Input:                      buf.Bytes(),
					Header:                     header,
					Memo:                       memo,
					SearchAttributes:           searchAttr,
					WorkflowRunTimeoutSeconds:  100,
					WorkflowTaskTimeoutSeconds: 10,
				}},
			}}, nil
		}

		lastRunStartedEvent = history.Events[0]
		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	for i := 0; i < 10; i++ {
		_, err := poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(previousRunID, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId())
	s.Equal(header, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().Header)
	s.Equal(memo, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().Memo)
	s.Equal(searchAttr, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().SearchAttributes)
}

func (s *integrationSuite) TestContinueAsNewRun_Timeout() {
	id := "integration-continue-as-new-workflow-timeout-test"
	wt := "integration-continue-as-new-workflow-timeout-test-type"
	tl := "integration-continue-as-new-workflow-timeout-test-tasklist"
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
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(1)
	continueAsNewCounter := int32(0)
	dtHandler := func(execution *executionpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]byte, []*decisionpb.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ContinueAsNewWorkflowExecution,
				Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:               workflowType,
					TaskList:                   &tasklistpb.TaskList{Name: tl},
					Input:                      buf.Bytes(),
					WorkflowRunTimeoutSeconds:  1, // set timeout to 1
					WorkflowTaskTimeoutSeconds: 1,
				}},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// process the decision and continue as new
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)

	time.Sleep(1 * time.Second) // wait 1 second for timeout

GetHistoryLoop:
	for i := 0; i < 20; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &executionpb.WorkflowExecution{
				WorkflowId: id,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.GetEventType() != eventpb.EventType_WorkflowExecutionTimedOut {
			s.Logger.Warn("Execution not timedout yet")
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		timeoutEventAttributes := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		s.Equal(eventpb.TimeoutType_StartToClose, timeoutEventAttributes.TimeoutType)
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)
}

func (s *integrationSuite) TestContinueAsNewWorkflow_Timeout() {
	id := "integration-continue-as-new-workflow-timeout-test"
	wt := "integration-continue-as-new-workflow-timeout-test-type"
	tl := "integration-continue-as-new-workflow-timeout-test-tasklist"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                       uuid.New(),
		Namespace:                       s.namespace,
		WorkflowId:                      id,
		WorkflowType:                    workflowType,
		TaskList:                        taskList,
		Input:                           nil,
		WorkflowExecutionTimeoutSeconds: 5,
		Identity:                        identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCounter := int32(0)
	dtHandler := func(execution *executionpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]byte, []*decisionpb.Decision, error) {
		continueAsNewCounter++
		buf := new(bytes.Buffer)
		s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_ContinueAsNewWorkflowExecution,
			Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
				WorkflowType: workflowType,
				TaskList:     &tasklistpb.TaskList{Name: tl},
				Input:        buf.Bytes(),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// process the decision and continue as new
	_, err := poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)

GetHistoryLoop:
	for i := 0; i < 200; i++ {
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &executionpb.WorkflowExecution{
				WorkflowId: id,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		firstEvent := history.Events[0]
		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.GetEventType() != eventpb.EventType_WorkflowExecutionTimedOut {
			if lastEvent.GetEventType() == eventpb.EventType_WorkflowExecutionContinuedAsNew {
				// Ensure that timeout is not caused by runTimeout
				s.True(time.Duration(lastEvent.Timestamp-firstEvent.Timestamp) < 5*time.Second)
			}
			s.Logger.Warn(fmt.Sprintf("Execution not timed out yet. Last event is %v", lastEvent))
			time.Sleep(200 * time.Millisecond)
			_, err := poller.PollAndProcessDecisionTask(true, false)
			s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
			if err != matching.ErrNoTasks {
				s.NoError(err)
			}
			continue GetHistoryLoop
		}

		s.True(firstEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeoutSeconds() < 5)
		timeoutEventAttributes := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		s.Equal(eventpb.TimeoutType_StartToClose, timeoutEventAttributes.TimeoutType)
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)
	s.True(continueAsNewCounter > 1)
}

func (s *integrationSuite) TestWorkflowContinueAsNew_TaskID() {
	id := "integration-wf-continue-as-new-task-id-test"
	wt := "integration-wf-continue-as-new-task-id-type"
	tl := "integration-wf-continue-as-new-task-id-tasklist"
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
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*executionpb.WorkflowExecution

	continueAsNewed := false
	dtHandler := func(execution *executionpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]byte, []*decisionpb.Decision, error) {

		executions = append(executions, execution)

		if !continueAsNewed {
			continueAsNewed = true
			return nil, []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_ContinueAsNewWorkflowExecution,
				Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:               workflowType,
					TaskList:                   taskList,
					Input:                      nil,
					WorkflowRunTimeoutSeconds:  100,
					WorkflowTaskTimeoutSeconds: 1,
				}},
			}}, nil
		}

		return nil, []*decisionpb.Decision{{
			DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("succeed"),
			}},
		}}, nil

	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	minTaskID := int64(0)
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.NoError(err)
	events := s.getHistory(s.namespace, executions[0])
	s.True(len(events) != 0)
	for _, event := range events {
		s.True(event.GetTaskId() > minTaskID)
		minTaskID = event.GetTaskId()
	}

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.NoError(err)
	events = s.getHistory(s.namespace, executions[1])
	s.True(len(events) != 0)
	for _, event := range events {
		s.True(event.GetTaskId() > minTaskID)
		minTaskID = event.GetTaskId()
	}
}

func (s *integrationSuite) TestChildWorkflowWithContinueAsNew() {
	parentID := "integration-child-workflow-with-continue-as-new-test-parent"
	childID := "integration-child-workflow-with-continue-as-new-test-child"
	wtParent := "integration-child-workflow-with-continue-as-new-test-parent-type"
	wtChild := "integration-child-workflow-with-continue-as-new-test-child-type"
	tl := "integration-child-workflow-with-continue-as-new-test-tasklist"
	identity := "worker1"

	parentWorkflowType := &commonpb.WorkflowType{}
	parentWorkflowType.Name = wtParent

	childWorkflowType := &commonpb.WorkflowType{}
	childWorkflowType.Name = wtChild

	taskList := &tasklistpb.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  s.namespace,
		WorkflowId:                 parentID,
		WorkflowType:               parentWorkflowType,
		TaskList:                   taskList,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// decider logic
	childComplete := false
	childExecutionStarted := false
	childData := int32(1)
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var startedEvent *eventpb.HistoryEvent
	var completedEvent *eventpb.HistoryEvent
	dtHandler := func(execution *executionpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *eventpb.History) ([]byte, []*decisionpb.Decision, error) {
		s.Logger.Info("Processing decision task for WorkflowId:", tag.WorkflowID(execution.GetWorkflowId()))

		// Child Decider Logic
		if execution.GetWorkflowId() == childID {
			if continueAsNewCounter < continueAsNewCount {
				continueAsNewCounter++
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

				return []byte(strconv.Itoa(int(continueAsNewCounter))), []*decisionpb.Decision{{
					DecisionType: decisionpb.DecisionType_ContinueAsNewWorkflowExecution,
					Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
						Input: buf.Bytes(),
					}},
				}}, nil
			}

			childComplete = true
			return nil, []*decisionpb.Decision{{
				DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: []byte("Child Done"),
				}},
			}}, nil
		}

		// Parent Decider Logic
		if execution.GetWorkflowId() == parentID {
			if !childExecutionStarted {
				s.Logger.Info("Starting child execution")
				childExecutionStarted = true
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, childData))

				return nil, []*decisionpb.Decision{{
					DecisionType: decisionpb.DecisionType_StartChildWorkflowExecution,
					Attributes: &decisionpb.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
						Namespace:    s.namespace,
						WorkflowId:   childID,
						WorkflowType: childWorkflowType,
						Input:        buf.Bytes(),
					}},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if event.GetEventType() == eventpb.EventType_ChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*decisionpb.Decision{}, nil
					}

					if event.GetEventType() == eventpb.EventType_ChildWorkflowExecutionCompleted {
						completedEvent = event
						return nil, []*decisionpb.Decision{{
							DecisionType: decisionpb.DecisionType_CompleteWorkflowExecution,
							Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
								Result: []byte("Done"),
							}},
						}}, nil
					}
				}
			}
		}

		return nil, nil, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       s.namespace,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	// Make first decision to start child execution
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(childExecutionStarted)

	// Process ChildExecution Started event and all generations of child executions
	for i := 0; i < 11; i++ {
		s.Logger.Warn("decision", tag.Counter(i))
		_, err = poller.PollAndProcessDecisionTask(false, false)
		s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
		s.NoError(err)
	}

	s.False(childComplete)
	s.NotNil(startedEvent)

	// Process Child Execution final decision to complete it
	_, err = poller.PollAndProcessDecisionTask(true, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.True(childComplete)

	// Process ChildExecution completed event and complete parent execution
	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(completedEvent)
	completedAttributes := completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(s.namespace, completedAttributes.Namespace)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.NotEqual(startedEvent.GetChildWorkflowExecutionStartedEventAttributes().WorkflowExecution.RunId,
		completedAttributes.WorkflowExecution.RunId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	s.Equal([]byte("Child Done"), completedAttributes.Result)

	s.Logger.Info("Parent Workflow Execution History: ")
}
