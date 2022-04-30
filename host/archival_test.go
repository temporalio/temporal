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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	retryLimit       = 20
	retryBackoffTime = 500 * time.Millisecond
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
		s.NotZero(execution.StartTime)
		s.NotZero(execution.ExecutionTime)
		s.NotZero(execution.CloseTime)
	}
}

func (s *integrationSuite) getNamespaceID(namespace string) string {
	namespaceResp, err := s.engine.DescribeNamespace(NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: s.archivalNamespace,
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
	namespaceID := s.getNamespaceID(s.archivalNamespace)
	shardID := common.WorkflowIDToHistoryShard(namespaceID, execution.GetWorkflowId(),
		s.testClusterConfig.HistoryConfig.NumHistoryShards)
	request := &persistence.GetHistoryTreeRequest{
		TreeID:  execution.GetRunId(),
		ShardID: convert.Int32Ptr(shardID),
	}
	for i := 0; i < retryLimit; i++ {
		resp, err := s.testCluster.testBase.ExecutionManager.GetHistoryTree(NewContext(), request)
		s.NoError(err)
		if len(resp.Branches) == 0 {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *integrationSuite) isMutableStateDeleted(namespaceID string, execution *commonpb.WorkflowExecution) bool {
	shardID := common.WorkflowIDToHistoryShard(namespaceID, execution.GetWorkflowId(),
		s.testClusterConfig.HistoryConfig.NumHistoryShards)
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}

	for i := 0; i < retryLimit; i++ {
		_, err := s.testCluster.testBase.ExecutionManager.GetWorkflowExecution(NewContext(), request)
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
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
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
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

	wtHandler := func(
		execution *commonpb.WorkflowExecution,
		wt *commonpb.WorkflowType,
		previousStartedEventID int64,
		startedEventID int64,
		history *historypb.History,
	) ([]*commandpb.Command, error) {
		runIDs[runCounter-1] = execution.GetRunId()
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		if runCounter < numRuns {
			activityCounter = int32(0)
			expectedActivityID = int32(1)
			runCounter++
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tq},
					Input:               nil,
					WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
				}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
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
		s.Equal(runIDs[runCounter-1], execution.GetRunId())
		s.Equal(activityName, activityType.Name)
		currentActivityId, _ := strconv.Atoi(activityID)
		s.Equal(int(expectedActivityID), currentActivityId)
		s.Equal(expectedActivityID, s.decodePayloadsByteSliceInt32(input))
		expectedActivityID++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}
	for run := 0; run < numRuns; run++ {
		for i := 0; i < numActivities; i++ {
			_, err := poller.PollAndProcessWorkflowTask(false, false)
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			s.NoError(err)
			if i%2 == 0 {
				err = poller.PollAndProcessActivityTask(false)
			} else { // just for testing respondActivityTaskCompleteByID
				err = poller.PollAndProcessActivityTaskWithID(false)
			}
			s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
			s.NoError(err)
		}

		_, err = poller.PollAndProcessWorkflowTask(true, false)
		s.NoError(err)
	}

	s.True(workflowComplete)
	for run := 1; run < numRuns; run++ {
		s.NotEqual(runIDs[run-1], runIDs[run])
	}
	return runIDs
}
