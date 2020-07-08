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

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	decisionpb "go.temporal.io/temporal-proto/decision/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	"go.temporal.io/temporal-proto/serviceerror"
	taskqueuepb "go.temporal.io/temporal-proto/taskqueue/v1"
	workflowpb "go.temporal.io/temporal-proto/workflow/v1"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
)

const (
	retryLimit       = 20
	retryBackoffTime = 200 * time.Millisecond
)

func (s *integrationSuite) TestArchival_TimerQueueProcessor() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-timer-queue-processor-workflow-id"
	workflowType := "archival-timer-queue-processor-type"
	taskQueue := "archival-timer-queue-processor-task-queue"
	numActivities := 1
	numRuns := 1
	runID := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)[0]

	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	s.True(s.isHistoryArchived(s.archivalNamespace, execution))
	s.True(s.isHistoryDeleted(execution))
	s.True(s.isMutableStateDeleted(namespaceID, execution))
}

func (s *integrationSuite) TestArchival_ContinueAsNew() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-continueAsNew-workflow-id"
	workflowType := "archival-continueAsNew-workflow-type"
	taskQueue := "archival-continueAsNew-task-queue"
	numActivities := 1
	numRuns := 5
	runIDs := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)

	for _, runID := range runIDs {
		execution := &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		}
		s.True(s.isHistoryArchived(s.archivalNamespace, execution))
		s.True(s.isHistoryDeleted(execution))
		s.True(s.isMutableStateDeleted(namespaceID, execution))
	}
}

func (s *integrationSuite) TestArchival_ArchiverWorker() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-archiver-worker-workflow-id"
	workflowType := "archival-archiver-worker-workflow-type"
	taskQueue := "archival-archiver-worker-task-queue"
	numActivities := 10
	runID := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, 1)[0]

	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	s.True(s.isHistoryArchived(s.archivalNamespace, execution))
	s.True(s.isHistoryDeleted(execution))
	s.True(s.isMutableStateDeleted(namespaceID, execution))
}

func (s *integrationSuite) TestVisibilityArchival() {
	s.True(s.testCluster.archiverBase.metadata.GetVisibilityConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-visibility-workflow-id"
	workflowType := "archival-visibility-workflow-type"
	taskQueue := "archival-visibility-task-queue"
	numActivities := 3
	numRuns := 5
	startTime := time.Now().UnixNano()
	s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)
	s.startAndFinishWorkflow("some other workflowID", "some other workflow type", taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)
	endTime := time.Now().UnixNano()

	var executions []*workflowpb.WorkflowExecutionInfo

	for i := 0; i != retryLimit; i++ {
		executions = []*workflowpb.WorkflowExecutionInfo{}
		request := &workflowservice.ListArchivedWorkflowExecutionsRequest{
			Namespace: s.archivalNamespace,
			PageSize:  2,
			Query:     fmt.Sprintf("CloseTime >= %v and CloseTime <= %v and WorkflowType = '%s'", startTime, endTime, workflowType),
		}
		for len(executions) == 0 || request.NextPageToken != nil {
			response, err := s.engine.ListArchivedWorkflowExecutions(NewContext(), request)
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
		s.NotZero(execution.StartTime.Value)
		s.NotZero(execution.ExecutionTime)
		s.NotZero(execution.CloseTime.Value)
	}
}

func (s *integrationSuite) getNamespaceID(namespace string) string {
	namespaceResp, err := s.engine.DescribeNamespace(NewContext(), &workflowservice.DescribeNamespaceRequest{
		Name: s.archivalNamespace,
	})
	s.NoError(err)
	return namespaceResp.NamespaceInfo.GetId()
}

func (s *integrationSuite) isHistoryArchived(namespace string, execution *commonpb.WorkflowExecution) bool {
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.archivalNamespace,
		Execution: execution,
	}

	for i := 0; i < retryLimit; i++ {
		getHistoryResp, err := s.engine.GetWorkflowExecutionHistory(NewContext(), request)
		if err == nil && getHistoryResp != nil && getHistoryResp.GetArchived() {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) isHistoryDeleted(execution *commonpb.WorkflowExecution) bool {
	shardID := common.WorkflowIDToHistoryShard(execution.GetWorkflowId(), s.testClusterConfig.HistoryConfig.NumHistoryShards)
	request := &persistence.GetHistoryTreeRequest{
		TreeID:  execution.GetRunId(),
		ShardID: convert.IntPtr(shardID),
	}
	for i := 0; i < retryLimit; i++ {
		resp, err := s.testCluster.testBase.HistoryV2Mgr.GetHistoryTree(request)
		s.NoError(err)
		if len(resp.Branches) == 0 {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) isMutableStateDeleted(namespaceID string, execution *commonpb.WorkflowExecution) bool {
	request := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
	}

	for i := 0; i < retryLimit; i++ {
		_, err := s.testCluster.testBase.ExecutionManager.GetWorkflowExecution(request)
		if _, ok := err.(*serviceerror.NotFound); ok {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) startAndFinishWorkflow(id, wt, tq, namespace, namespaceID string, numActivities, numRuns int) []string {
	identity := "worker1"
	activityName := "activity_type1"
	workflowType := &commonpb.WorkflowType{
		Name: wt,
	}
	taskQueue := &taskqueuepb.TaskQueue{
		Name: tq,
	}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	runIDs := make([]string, numRuns)

	workflowComplete := false
	activityCount := int32(numActivities)
	activityCounter := int32(0)
	expectedActivityID := int32(1)
	runCounter := 1

	dtHandler := func(
		execution *commonpb.WorkflowExecution,
		wt *commonpb.WorkflowType,
		previousStartedEventID int64,
		startedEventID int64,
		history *historypb.History,
	) ([]*decisionpb.Decision, error) {
		runIDs[runCounter-1] = execution.GetRunId()
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tq},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		if runCounter < numRuns {
			activityCounter = int32(0)
			expectedActivityID = int32(1)
			runCounter++
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:               workflowType,
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: tq},
					Input:                      nil,
					WorkflowRunTimeoutSeconds:  100,
					WorkflowTaskTimeoutSeconds: 1,
				}},
			}}, nil
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(
		execution *commonpb.WorkflowExecution,
		activityType *commonpb.ActivityType,
		activityID string,
		input *commonpb.Payloads,
		taskToken []byte,
	) (*commonpb.Payloads, bool, error) {
		s.Equal(id, execution.GetWorkflowId())
		s.Equal(activityName, activityType.Name)
		id, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivityID), id)
		var b []byte
		err := payloads.Decode(input, &b)
		s.NoError(err)
		buf := bytes.NewReader(b)
		var in int32
		binary.Read(buf, binary.LittleEndian, &in)
		s.Equal(expectedActivityID, in)
		expectedActivityID++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:          s.engine,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
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
			s.NoError(err)
			if i%2 == 0 {
				err = poller.PollAndProcessActivityTask(false)
			} else { // just for testing respondActivityTaskCompleteByID
				err = poller.PollAndProcessActivityTaskWithID(false)
			}
			s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
			s.NoError(err)
		}

		_, err = poller.PollAndProcessDecisionTask(true, false)
		s.NoError(err)
	}

	s.True(workflowComplete)
	for run := 1; run < numRuns; run++ {
		s.NotEqual(runIDs[run-1], runIDs[run])
	}
	return runIDs
}
