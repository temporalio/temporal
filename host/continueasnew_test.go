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

	"github.com/temporalio/temporal/common/log/tag"
)

func (s *integrationSuite) TestContinueAsNewWorkflow() {
	id := "integration-continue-as-new-workflow-test"
	wt := "integration-continue-as-new-workflow-test-type"
	tl := "integration-continue-as-new-workflow-test-tasklist"
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	header := &commonproto.Header{
		Fields: map[string][]byte{"tracing": []byte("sample payload")},
	}
	memo := &commonproto.Memo{
		Fields: map[string][]byte{"memoKey": []byte("memoVal")},
	}
	searchAttr := &commonproto.SearchAttributes{
		IndexedFields: map[string][]byte{"CustomKeywordField": []byte("1")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		Header:                              header,
		Memo:                                memo,
		SearchAttributes:                    searchAttr,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = execution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeContinueAsNewWorkflowExecution,
				Attributes: &commonproto.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:                        workflowType,
					TaskList:                            &commonproto.TaskList{Name: tl},
					Input:                               buf.Bytes(),
					Header:                              header,
					Memo:                                memo,
					SearchAttributes:                    searchAttr,
					ExecutionStartToCloseTimeoutSeconds: 100,
					TaskStartToCloseTimeoutSeconds:      10,
				}},
			}}, nil
		}

		lastRunStartedEvent = history.Events[0]
		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
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

func (s *integrationSuite) TestContinueAsNewWorkflow_Timeout() {
	id := "integration-continue-as-new-workflow-timeout-test"
	wt := "integration-continue-as-new-workflow-timeout-test-type"
	tl := "integration-continue-as-new-workflow-timeout-test-tasklist"
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: 100,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            identity,
	}

	we, err0 := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(1)
	continueAsNewCounter := int32(0)
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []byte(strconv.Itoa(int(continueAsNewCounter))), []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeContinueAsNewWorkflowExecution,
				Attributes: &commonproto.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:                        workflowType,
					TaskList:                            &commonproto.TaskList{Name: tl},
					Input:                               buf.Bytes(),
					ExecutionStartToCloseTimeoutSeconds: 1, // set timeout to 1
					TaskStartToCloseTimeoutSeconds:      1,
				}},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(continueAsNewCounter))), []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
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
			Domain: s.domainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: id,
			},
		})
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.GetEventType() != enums.EventTypeWorkflowExecutionTimedOut {
			s.Logger.Warn("Execution not timedout yet")
			time.Sleep(200 * time.Millisecond)
			continue GetHistoryLoop
		}

		timeoutEventAttributes := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		s.Equal(enums.TimeoutTypeStartToClose, timeoutEventAttributes.TimeoutType)
		workflowComplete = true
		break GetHistoryLoop
	}
	s.True(workflowComplete)
}

func (s *integrationSuite) TestWorkflowContinueAsNew_TaskID() {
	id := "integration-wf-continue-as-new-task-id-test"
	wt := "integration-wf-continue-as-new-task-id-type"
	tl := "integration-wf-continue-as-new-task-id-tasklist"
	identity := "worker1"

	workflowType := &commonproto.WorkflowType{Name: wt}

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          id,
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

	var executions []*commonproto.WorkflowExecution

	continueAsNewed := false
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {

		executions = append(executions, execution)

		if !continueAsNewed {
			continueAsNewed = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeContinueAsNewWorkflowExecution,
				Attributes: &commonproto.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:                        workflowType,
					TaskList:                            taskList,
					Input:                               nil,
					ExecutionStartToCloseTimeoutSeconds: 100,
					TaskStartToCloseTimeoutSeconds:      1,
				}},
			}}, nil
		}

		return nil, []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("succeed"),
			}},
		}}, nil

	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          s.domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}

	minTaskID := int64(0)
	_, err := poller.PollAndProcessDecisionTask(false, false)
	s.NoError(err)
	events := s.getHistory(s.domainName, executions[0])
	s.True(len(events) != 0)
	for _, event := range events {
		s.True(event.GetTaskId() > minTaskID)
		minTaskID = event.GetTaskId()
	}

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.NoError(err)
	events = s.getHistory(s.domainName, executions[1])
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

	parentWorkflowType := &commonproto.WorkflowType{}
	parentWorkflowType.Name = wtParent

	childWorkflowType := &commonproto.WorkflowType{}
	childWorkflowType.Name = wtChild

	taskList := &commonproto.TaskList{Name: tl}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                           uuid.New(),
		Domain:                              s.domainName,
		WorkflowId:                          parentID,
		WorkflowType:                        parentWorkflowType,
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
	childComplete := false
	childExecutionStarted := false
	childData := int32(1)
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var startedEvent *commonproto.HistoryEvent
	var completedEvent *commonproto.HistoryEvent
	dtHandler := func(execution *commonproto.WorkflowExecution, wt *commonproto.WorkflowType,
		previousStartedEventID, startedEventID int64, history *commonproto.History) ([]byte, []*commonproto.Decision, error) {
		s.Logger.Info("Processing decision task for WorkflowID:", tag.WorkflowID(execution.GetWorkflowId()))

		// Child Decider Logic
		if execution.GetWorkflowId() == childID {
			if continueAsNewCounter < continueAsNewCount {
				continueAsNewCounter++
				buf := new(bytes.Buffer)
				s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

				return []byte(strconv.Itoa(int(continueAsNewCounter))), []*commonproto.Decision{{
					DecisionType: enums.DecisionTypeContinueAsNewWorkflowExecution,
					Attributes: &commonproto.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes{
						Input: buf.Bytes(),
					}},
				}}, nil
			}

			childComplete = true
			return nil, []*commonproto.Decision{{
				DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
				Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
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

				return nil, []*commonproto.Decision{{
					DecisionType: enums.DecisionTypeStartChildWorkflowExecution,
					Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
						Domain:       s.domainName,
						WorkflowId:   childID,
						WorkflowType: childWorkflowType,
						Input:        buf.Bytes(),
					}},
				}}, nil
			} else if previousStartedEventID > 0 {
				for _, event := range history.Events[previousStartedEventID:] {
					if event.GetEventType() == enums.EventTypeChildWorkflowExecutionStarted {
						startedEvent = event
						return nil, []*commonproto.Decision{}, nil
					}

					if event.GetEventType() == enums.EventTypeChildWorkflowExecutionCompleted {
						completedEvent = event
						return nil, []*commonproto.Decision{{
							DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
							Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
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
		Domain:          s.domainName,
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
	s.Equal(s.domainName, completedAttributes.Domain)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.NotEqual(startedEvent.GetChildWorkflowExecutionStartedEventAttributes().WorkflowExecution.RunId,
		completedAttributes.WorkflowExecution.RunId)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	s.Equal([]byte("Child Done"), completedAttributes.Result)

	s.Logger.Info("Parent Workflow Execution History: ")
}
