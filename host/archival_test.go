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
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

const (
	retryLimit       = 20
	retryBackoffTime = 200 * time.Millisecond
)

func (s *integrationSuite) TestArchival_TimerQueueProcessor() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	domainID := s.getDomainID(s.archivalDomainName)
	workflowID := "archival-timer-queue-processor-workflow-id"
	workflowType := "archival-timer-queue-processor-type"
	taskList := "archival-timer-queue-processor-task-list"
	numActivities := 1
	numRuns := 1
	runID := s.startAndFinishWorkflow(workflowID, workflowType, taskList, s.archivalDomainName, domainID, numActivities, numRuns)[0]

	execution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	s.True(s.isHistoryArchived(s.archivalDomainName, execution))
	s.True(s.isHistoryDeleted(domainID, execution))
	s.True(s.isMutableStateDeleted(domainID, execution))
}

func (s *integrationSuite) TestArchival_ContinueAsNew() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	domainID := s.getDomainID(s.archivalDomainName)
	workflowID := "archival-continueAsNew-workflow-id"
	workflowType := "archival-continueAsNew-workflow-type"
	taskList := "archival-continueAsNew-task-list"
	numActivities := 1
	numRuns := 5
	runIDs := s.startAndFinishWorkflow(workflowID, workflowType, taskList, s.archivalDomainName, domainID, numActivities, numRuns)

	for _, runID := range runIDs {
		execution := &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		}
		s.True(s.isHistoryArchived(s.archivalDomainName, execution))
		s.True(s.isHistoryDeleted(domainID, execution))
		s.True(s.isMutableStateDeleted(domainID, execution))
	}
}

func (s *integrationSuite) TestArchival_ArchiverWorker() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	domainID := s.getDomainID(s.archivalDomainName)
	workflowID := "archival-archiver-worker-workflow-id"
	workflowType := "archival-archiver-worker-workflow-type"
	taskList := "archival-archiver-worker-task-list"
	numActivities := 10
	runID := s.startAndFinishWorkflow(workflowID, workflowType, taskList, s.archivalDomainName, domainID, numActivities, 1)[0]

	execution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	s.True(s.isHistoryArchived(s.archivalDomainName, execution))
	s.True(s.isHistoryDeleted(domainID, execution))
	s.True(s.isMutableStateDeleted(domainID, execution))
}

func (s *integrationSuite) TestVisibilityArchival() {
	s.True(s.testCluster.archiverBase.metadata.GetVisibilityConfig().ClusterConfiguredForArchival())

	domainID := s.getDomainID(s.archivalDomainName)
	workflowID := "archival-visibility-workflow-id"
	workflowType := "archival-visibility-workflow-type"
	taskList := "archival-visibility-task-list"
	numActivities := 3
	numRuns := 5
	startTime := time.Now().UnixNano()
	s.startAndFinishWorkflow(workflowID, workflowType, taskList, s.archivalDomainName, domainID, numActivities, numRuns)
	s.startAndFinishWorkflow("some other workflowID", "some other workflow type", taskList, s.archivalDomainName, domainID, numActivities, numRuns)
	endTime := time.Now().UnixNano()

	var executions []*workflow.WorkflowExecutionInfo

	for i := 0; i != retryLimit; i++ {
		executions = []*workflow.WorkflowExecutionInfo{}
		request := &workflow.ListArchivedWorkflowExecutionsRequest{
			Domain:   common.StringPtr(s.archivalDomainName),
			PageSize: common.Int32Ptr(2),
			Query:    common.StringPtr(fmt.Sprintf("CloseTime >= %v and CloseTime <= %v and WorkflowType = '%s'", startTime, endTime, workflowType)),
		}
		for len(executions) == 0 || request.NextPageToken != nil {
			response, err := s.engine.ListArchivedWorkflowExecutions(createContext(), request)
			s.NoError(err)
			s.NotNil(response)
			executions = append(executions, response.GetExecutions()...)
			request.NextPageToken = response.NextPageToken
		}
		if len(executions) == numRuns {
			break
		}
		time.Sleep(retryBackoffTime)
	}

	for _, execution := range executions {
		s.Equal(workflowID, execution.GetExecution().GetWorkflowId())
		s.Equal(workflowType, execution.GetType().GetName())
		s.NotZero(execution.StartTime)
		s.NotZero(execution.ExecutionTime)
		s.NotZero(execution.CloseTime)
	}
}

func (s *integrationSuite) getDomainID(domain string) string {
	domainResp, err := s.engine.DescribeDomain(createContext(), &workflow.DescribeDomainRequest{
		Name: common.StringPtr(s.archivalDomainName),
	})
	s.Nil(err)
	return domainResp.DomainInfo.GetUUID()
}

func (s *integrationSuite) isHistoryArchived(domain string, execution *workflow.WorkflowExecution) bool {
	request := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain:    common.StringPtr(s.archivalDomainName),
		Execution: execution,
	}

	for i := 0; i < retryLimit; i++ {
		getHistoryResp, err := s.engine.GetWorkflowExecutionHistory(createContext(), request)
		if err == nil && getHistoryResp != nil && getHistoryResp.GetArchived() {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) isHistoryDeleted(domainID string, execution *workflow.WorkflowExecution) bool {
	shardID := common.WorkflowIDToHistoryShard(*execution.WorkflowId, s.testClusterConfig.HistoryConfig.NumHistoryShards)
	request := &persistence.GetHistoryTreeRequest{
		TreeID:  execution.GetRunId(),
		ShardID: common.IntPtr(shardID),
	}
	for i := 0; i < retryLimit; i++ {
		resp, err := s.testCluster.testBase.HistoryV2Mgr.GetHistoryTree(request)
		s.Nil(err)
		if len(resp.Branches) == 0 && len(resp.ForkingInProgressBranches) == 0 {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) isMutableStateDeleted(domainID string, execution *workflow.WorkflowExecution) bool {
	request := &persistence.GetWorkflowExecutionRequest{
		DomainID:  domainID,
		Execution: *execution,
	}

	for i := 0; i < retryLimit; i++ {
		_, err := s.testCluster.testBase.ExecutionManager.GetWorkflowExecution(request)
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) startAndFinishWorkflow(id, wt, tl, domain, domainID string, numActivities, numRuns int) []string {
	identity := "worker1"
	activityName := "activity_type1"
	workflowType := &workflow.WorkflowType{
		Name: common.StringPtr(wt),
	}
	taskList := &workflow.TaskList{
		Name: common.StringPtr(tl),
	}
	request := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domain),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}
	we, err := s.engine.StartWorkflowExecution(createContext(), request)
	s.Nil(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(*we.RunId))
	runIDs := make([]string, numRuns)

	workflowComplete := false
	activityCount := int32(numActivities)
	activityCounter := int32(0)
	expectedActivityID := int32(1)
	runCounter := 1

	dtHandler := func(
		execution *workflow.WorkflowExecution,
		wt *workflow.WorkflowType,
		previousStartedEventID int64,
		startedEventID int64,
		history *workflow.History,
	) ([]byte, []*workflow.Decision, error) {
		runIDs[runCounter-1] = execution.GetRunId()
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
				ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    common.StringPtr(strconv.Itoa(int(activityCounter))),
					ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityName)},
					TaskList:                      &workflow.TaskList{Name: &tl},
					Input:                         buf.Bytes(),
					ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
					StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
					HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
				},
			}}, nil
		}

		if runCounter < numRuns {
			activityCounter = int32(0)
			expectedActivityID = int32(1)
			runCounter++
			return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
				DecisionType: common.DecisionTypePtr(workflow.DecisionTypeContinueAsNewWorkflowExecution),
				ContinueAsNewWorkflowExecutionDecisionAttributes: &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:                        workflowType,
					TaskList:                            &workflow.TaskList{Name: &tl},
					Input:                               nil,
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
				},
			}}, nil
		}

		workflowComplete = true
		return []byte(strconv.Itoa(int(activityCounter))), []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: []byte("Done."),
			},
		}}, nil
	}

	atHandler := func(
		execution *workflow.WorkflowExecution,
		activityType *workflow.ActivityType,
		activityID string,
		input []byte,
		taskToken []byte,
	) ([]byte, bool, error) {
		s.Equal(id, *execution.WorkflowId)
		s.Equal(activityName, *activityType.Name)
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivityID), id)
		buf := bytes.NewReader(input)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivityID, in)
		expectedActivityID++
		return []byte("Activity Result."), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Domain:          domain,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.Logger,
		T:               s.T(),
	}
	for run := 0; run < numRuns; run++ {
		for i := 0; i < numActivities; i++ {
			_, err := poller.PollAndProcessDecisionTask(false, false)
			s.Logger.Info("PollAndProcessDecisionTask", tag.Error(err))
			s.Nil(err)
			if i%2 == 0 {
				err = poller.PollAndProcessActivityTask(false)
			} else { // just for testing respondActivityTaskCompleteByID
				err = poller.PollAndProcessActivityTaskWithID(false)
			}
			s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
			s.Nil(err)
		}

		_, err = poller.PollAndProcessDecisionTask(true, false)
		s.Nil(err)
	}

	s.True(workflowComplete)
	for run := 1; run < numRuns; run++ {
		s.NotEqual(runIDs[run-1], runIDs[run])
	}
	return runIDs
}
